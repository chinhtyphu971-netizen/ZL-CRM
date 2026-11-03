/**
 * zalo-history-backfill.ts — initial history seeding for fresh accounts.
 *
 * On first connect (or via manual sync endpoint) fetches:
 *   - All friends → upsert contacts
 *   - All groups → recent history via REST getGroupChatHistory (when Zalo still serves it)
 *   - DM + group history via websocket requestOldMessages (cmd 510/511), same as Zalo Web
 *
 * Note: Zalo only returns history its server keeps for web sessions. Messages that
 * live only on the phone (never synced to Zalo PC/Web) cannot be fetched here.
 *
 * Idempotent: re-running is safe — message-handler dedup guards prevent duplicates.
 * Fire-and-forget callable: errors are logged, not propagated.
 */
import { randomUUID } from 'node:crypto';
import { prisma } from '../../shared/database/prisma-client.js';
import { logger } from '../../shared/utils/logger.js';
import { handleIncomingMessage } from '../chat/message-handler.js';
import { detectContentType, extractAlbumInfo } from './zalo-message-helpers.js';

const MAX_GROUPS = 50;
const MESSAGES_PER_GROUP = 50;
const DM_MAX_PAGES = 50;
const DM_PAGE_TIMEOUT_MS = 15_000;
const GROUP_INFO_BATCH = 50;
// Right after QR login the websocket may not be open / keyed yet, and Zalo can
// answer the very first request with an empty page. Retry the first page a few times.
const FIRST_PAGE_RETRIES = 3;
const FIRST_PAGE_RETRY_DELAY_MS = 4_000;
const LISTENER_READY_TIMEOUT_MS = 30_000;

/**
 * Wait until the zca-js listener websocket is open AND has received its cipher key
 * (needed to decode old_messages payloads). Resolves false on timeout.
 */
async function waitForListenerReady(api: any, accountId: string): Promise<boolean> {
  const deadline = Date.now() + LISTENER_READY_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const listener = api?.listener;
    if (listener?.ws?.readyState === 1 && listener?.cipherKey) return true;
    await new Promise((r) => setTimeout(r, 250));
  }
  logger.warn(`[backfill:${accountId}] Listener not ready after ${LISTENER_READY_TIMEOUT_MS}ms — requesting history anyway`);
  return false;
}

interface GroupMeta { name?: string; avatar?: string; membersCount?: number; }

/**
 * Multi-cursor strategy ported from openzca CLI `getRecentPageCursors`.
 * Tries oldest-by-ts / last-in-array / first-in-array — different cursors
 * sometimes unlock different next-page slices from Zalo.
 */
function pickNextCursors(messages: any[]): string[] {
  const cursors: string[] = [];
  const seen = new Set<string>();
  const add = (c: string) => {
    const v = c.trim();
    if (!v || seen.has(v)) return;
    seen.add(v); cursors.push(v);
  };
  const cursorOf = (m: any): string => String(m?.data?.msgId || m?.data?.actionId || m?.data?.cliMsgId || '');

  // Oldest by timestamp
  let oldest: any = null;
  for (const m of messages) {
    const ts = parseInt(m?.data?.ts || '0');
    if (!oldest || ts < parseInt(oldest?.data?.ts || '0')) oldest = m;
  }
  if (oldest) add(cursorOf(oldest));
  add(cursorOf(messages[messages.length - 1]));
  add(cursorOf(messages[0]));
  return cursors;
}

// ThreadType from zca-js: 0 = User (DM), 1 = Group
const THREAD_TYPE_USER = 0;
const THREAD_TYPE_GROUP = 1;

export interface BackfillResult {
  friendsSynced: number;
  groupsSynced: number;
  messagesBackfilled: number;
  dmPagesRequested: number;
  groupPagesRequested: number;
  errors: number;
}

interface PumpStats { pagesRequested: number; messagesInserted: number; messagesReceived: number; }

/**
 * Drives Zalo's `requestOldMessages` pagination AND directly persists each
 * incoming batch via `handleIncomingMessage`. This bypasses the main listener's
 * `old_messages` handler so insertion is deterministic and counted.
 */
async function pumpOldMessages(
  api: any,
  threadType: number,
  accountId: string,
  groupMeta?: Map<string, GroupMeta>,
): Promise<PumpStats> {
  return new Promise((resolve) => {
    const stats: PumpStats = { pagesRequested: 0, messagesInserted: 0, messagesReceived: 0 };
    const requestedCursors = new Set<string>();
    const threadTypeLabel = threadType === THREAD_TYPE_USER ? 'user' : 'group';
    let timer: ReturnType<typeof setTimeout> | null = null;
    let finished = false;
    let emptyFirstPages = 0;

    const finish = () => {
      if (finished) return;
      finished = true;
      try { api.listener.off?.('old_messages', onPage); } catch {}
      if (timer) clearTimeout(timer);
      resolve(stats);
    };

    const resetIdleTimer = () => {
      if (timer) clearTimeout(timer);
      timer = setTimeout(finish, DM_PAGE_TIMEOUT_MS);
    };

    const requestPage = (cursor: string | null): boolean => {
      const c = (cursor ?? '').trim();
      if (c && requestedCursors.has(c)) return false;
      if (c) requestedCursors.add(c);
      try {
        api.listener.requestOldMessages(threadType, c || null);
        stats.pagesRequested++;
        resetIdleTimer();
        return true;
      } catch (err) {
        logger.warn(`[backfill:${accountId}] requestOldMessages failed:`, err);
        return false;
      }
    };

    const onPage = async (messages: any[], type: number) => {
      if (finished) return;
      if (type !== threadType) return;
      if (!Array.isArray(messages) || messages.length === 0) {
        // Empty first page right after login is often a timing artefact — retry it.
        if (stats.messagesReceived === 0 && emptyFirstPages < FIRST_PAGE_RETRIES) {
          emptyFirstPages++;
          logger.info(`[backfill:${accountId}] Empty first ${threadTypeLabel} page — retry ${emptyFirstPages}/${FIRST_PAGE_RETRIES}`);
          if (timer) clearTimeout(timer);
          setTimeout(() => { if (!finished && !requestPage(null)) finish(); }, FIRST_PAGE_RETRY_DELAY_MS);
          return;
        }
        finish();
        return;
      }

      stats.messagesReceived += messages.length;
      logger.info(`[backfill:${accountId}] ${threadTypeLabel} page received: ${messages.length} message(s) (received total=${stats.messagesReceived})`);

      // Persist each message directly. Use senderUid as fallback threadId for
      // self messages, since Zalo's payload puts the peer in idTo.
      for (const m of messages) {
        try {
          const isSelf = Boolean(m?.isSelf);
          const senderUid = String(m?.data?.uidFrom || '');
          const threadId = String(m?.threadId || (isSelf ? m?.data?.idTo : senderUid) || '');
          if (!threadId) continue;

          const rawContent = m?.data?.content;
          const content =
            typeof rawContent === 'string' ? rawContent : JSON.stringify(rawContent || '');
          const contentType = detectContentType(m?.data?.msgType, rawContent);
          const album = extractAlbumInfo(contentType, rawContent);
          const meta = threadType === THREAD_TYPE_GROUP ? groupMeta?.get(threadId) : undefined;

          const inserted = await handleIncomingMessage({
            accountId,
            senderUid,
            senderName: m?.data?.dName || '',
            content,
            contentType,
            msgId: String(m?.data?.msgId || m?.data?.cliMsgId || ''),
            timestamp: parseInt(m?.data?.ts || String(Date.now())),
            isSelf,
            threadId,
            threadType: threadTypeLabel,
            groupName: meta?.name,
            groupAvatarUrl: meta?.avatar,
            groupMembersCount: meta?.membersCount,
            attachments: [],
            quote: m?.data?.quote,
            albumKey: album.albumKey,
            albumIndex: album.albumIndex,
            albumTotal: album.albumTotal,
            isBackfill: true,
          });
          if (inserted) stats.messagesInserted++;
        } catch (err) {
          logger.warn(`[backfill:${accountId}] ${threadTypeLabel} insert failed:`, err);
        }
      }

      if (stats.pagesRequested >= DM_MAX_PAGES) { finish(); return; }

      // Try multi-cursor candidates — different cursors may unlock different
      // page slices (oldest-by-ts / last / first).
      const candidates = pickNextCursors(messages);
      let requested = false;
      for (const c of candidates) {
        if (requestPage(c)) { requested = true; break; }
      }
      if (!requested) finish();
    };

    api.listener.on('old_messages', onPage);
    if (!requestPage(null)) finish();
  });
}

export async function backfillAccountHistory(api: any, accountId: string): Promise<BackfillResult> {
  const result: BackfillResult = {
    friendsSynced: 0,
    groupsSynced: 0,
    messagesBackfilled: 0,
    dmPagesRequested: 0,
    groupPagesRequested: 0,
    errors: 0,
  };

  const account = await prisma.zaloAccount.findUnique({
    where: { id: accountId },
    select: { orgId: true },
  });
  if (!account) {
    logger.warn(`[backfill:${accountId}] Account not found`);
    return result;
  }

  // ── 1. Sync friends → contacts ─────────────────────────────────────────
  try {
    const friendsRaw = await api.getAllFriends();
    const friends = Array.isArray(friendsRaw) ? friendsRaw : Object.values(friendsRaw || {});
    for (const friend of friends as any[]) {
      const uid = String(friend?.userId || friend?.uid || '');
      if (!uid) continue;

      const zaloName = friend?.zaloName || friend?.zalo_name || friend?.displayName || friend?.display_name || '';
      const avatar = friend?.avatar || '';
      const phone = friend?.phoneNumber || '';

      try {
        const existing = await prisma.contact.findFirst({
          where: { zaloUid: uid, orgId: account.orgId },
          select: { id: true, fullName: true },
        });
        if (existing) {
          await prisma.contact.update({
            where: { id: existing.id },
            data: {
              fullName: zaloName || existing.fullName,
              avatarUrl: avatar || undefined,
              phone: phone || undefined,
            },
          });
        } else {
          await prisma.contact.create({
            data: {
              id: randomUUID(),
              orgId: account.orgId,
              zaloUid: uid,
              fullName: zaloName || 'Unknown',
              avatarUrl: avatar || null,
              phone: phone || null,
            },
          });
        }
        result.friendsSynced++;
      } catch (err) {
        result.errors++;
        logger.warn(`[backfill:${accountId}] Friend ${uid} upsert failed:`, err);
      }
    }
  } catch (err) {
    result.errors++;
    logger.warn(`[backfill:${accountId}] getAllFriends failed:`, err);
  }

  // ── 2. Sync groups → conversations + history ───────────────────────────
  // zca-js 2.x getAllGroups returns { version, gridVerMap: { [groupId]: version } } —
  // only ids. Names/avatars come from getGroupInfo (max 50 ids per call).
  let groupIds: string[] = [];
  const groupMeta = new Map<string, GroupMeta>();
  try {
    const groupsRaw = await api.getAllGroups();
    groupIds = Object.keys(groupsRaw?.gridVerMap || {});
  } catch (err) {
    result.errors++;
    logger.warn(`[backfill:${accountId}] getAllGroups failed:`, err);
  }

  for (let i = 0; i < groupIds.length; i += GROUP_INFO_BATCH) {
    const batch = groupIds.slice(i, i + GROUP_INFO_BATCH);
    try {
      const info = await api.getGroupInfo(batch);
      for (const [id, g] of Object.entries<any>(info?.gridInfoMap || {})) {
        groupMeta.set(id, {
          name: g?.name || undefined,
          avatar: g?.avt || g?.fullAvt || undefined,
          membersCount: typeof g?.totalMember === 'number' ? g.totalMember : undefined,
        });
      }
    } catch (err) {
      logger.warn(`[backfill:${accountId}] getGroupInfo batch failed:`, err);
    }
  }
  logger.info(`[backfill:${accountId}] Found ${groupIds.length} group(s), metadata for ${groupMeta.size}`);

  // REST group history: stop calling it after a 404 (endpoint retired for this
  // session) — the websocket pump in step 4 covers group history instead.
  let groupRestAvailable = true;
  for (const groupId of groupIds.slice(0, MAX_GROUPS)) {
    if (!groupRestAvailable) break;
    const meta = groupMeta.get(groupId);

    try {
      const groupName = meta?.name || 'Nhóm';
      const groupAvatar = meta?.avatar || null;
      const membersCount = meta?.membersCount ?? null;

      const history = await api.getGroupChatHistory(groupId, MESSAGES_PER_GROUP);
      const messages = history?.groupMsgs || history?.data?.groupMsgs || [];

      for (const msg of messages as any[]) {
        try {
          const zaloMsgId = String(msg?.data?.msgId || msg?.data?.cliMsgId || '');
          if (!zaloMsgId) continue;

          const rawContent = msg?.data?.content;
          const content =
            typeof rawContent === 'string' ? rawContent : JSON.stringify(rawContent || '');
          const contentType = detectContentType(msg?.data?.msgType, rawContent);
          const album = extractAlbumInfo(contentType, rawContent);

          const inserted = await handleIncomingMessage({
            accountId,
            senderUid: String(msg?.data?.uidFrom || ''),
            senderName: msg?.data?.dName || '',
            content,
            contentType,
            msgId: zaloMsgId,
            timestamp: parseInt(msg?.data?.ts || String(Date.now())),
            isSelf: Boolean(msg?.isSelf),
            threadId: groupId,
            threadType: 'group',
            groupName,
            groupAvatarUrl: groupAvatar || undefined,
            groupMembersCount: typeof membersCount === 'number' ? membersCount : undefined,
            attachments: [],
            quote: msg?.data?.quote,
            albumKey: album.albumKey,
            albumIndex: album.albumIndex,
            albumTotal: album.albumTotal,
            isBackfill: true,
          });
          if (inserted) result.messagesBackfilled++;
        } catch (err) {
          result.errors++;
          logger.warn(`[backfill:${accountId}] Group ${groupId} message insert failed:`, err);
        }
      }
      result.groupsSynced++;
    } catch (err) {
      result.errors++;
      const status = (err as any)?.code ?? (err as any)?.response?.status;
      if (status === 404 || /status code 404/.test(String((err as any)?.message || err))) {
        groupRestAvailable = false;
        logger.warn(`[backfill:${accountId}] getGroupChatHistory returned 404 — skipping REST group history, using websocket instead`);
      } else {
        logger.warn(`[backfill:${accountId}] Group ${groupId} history fetch failed:`, err);
      }
    }
  }

  // ── 3+4. DM + group history via requestOldMessages pagination ──────────
  // Zalo pushes batches via `old_messages` event (cmd 510 = DM, 511 = group).
  // Wait for the websocket to be open and keyed first — requesting too early
  // (right after QR login) gets an empty page.
  let dmReceived = 0;
  let groupReceived = 0;
  if (api?.listener?.requestOldMessages) {
    await waitForListenerReady(api, accountId);
    try {
      const stats = await pumpOldMessages(api, THREAD_TYPE_USER, accountId);
      result.dmPagesRequested = stats.pagesRequested;
      dmReceived = stats.messagesReceived;
      // Count by raw received — main listener may win the insert race, but
      // the message still ends up in DB. messagesInserted alone undercounts.
      result.messagesBackfilled += stats.messagesReceived;
    } catch (err) {
      result.errors++;
      logger.warn(`[backfill:${accountId}] DM pump failed:`, err);
    }
    try {
      const stats = await pumpOldMessages(api, THREAD_TYPE_GROUP, accountId, groupMeta);
      result.groupPagesRequested = stats.pagesRequested;
      groupReceived = stats.messagesReceived;
      result.messagesBackfilled += stats.messagesReceived;
    } catch (err) {
      result.errors++;
      logger.warn(`[backfill:${accountId}] Group pump failed:`, err);
    }
  } else {
    logger.warn(`[backfill:${accountId}] api.listener.requestOldMessages unavailable — skipping websocket history`);
  }

  // Sanity check: verify what actually landed in DB for this account
  const dbCounts = await prisma.$transaction([
    prisma.conversation.count({ where: { zaloAccountId: accountId } }),
    prisma.message.count({
      where: { conversation: { zaloAccountId: accountId } },
    }),
  ]).catch(() => [0, 0] as [number, number]);

  logger.info(
    `[backfill:${accountId}] Done — friends=${result.friendsSynced} groups=${result.groupsSynced} ` +
    `dmReceived=${dmReceived} dmPages=${result.dmPagesRequested} ` +
    `groupReceived=${groupReceived} groupPages=${result.groupPagesRequested} errors=${result.errors} ` +
    `| DB now has ${dbCounts[0]} conversation(s), ${dbCounts[1]} message(s) for this account`,
  );
  return result;
}

/**
 * Backfill only if account has no conversations yet (first-time login).
 * Returns true if backfill was triggered.
 */
export async function backfillIfEmpty(api: any, accountId: string): Promise<boolean> {
  const existing = await prisma.conversation.count({ where: { zaloAccountId: accountId } });
  if (existing > 0) return false;

  logger.info(`[backfill:${accountId}] Empty conversation set detected — starting initial backfill`);
  await backfillAccountHistory(api, accountId);
  return true;
}
