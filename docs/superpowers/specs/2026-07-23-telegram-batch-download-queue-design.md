# Telegram Batch Download Queue and Control UI Design

Date: 2026-07-23
Status: Approved design
Scope: qBT Orchestrator Telegram Bot, checked download enrollment, warning inbox, and user-facing status on `ssh.paff-67.top`

## 1. Outcome

`/start` opens a mobile-friendly Telegram control panel implemented with ordinary `InlineKeyboardMarkup`. The welcome message uses clear natural language, shows active downloads and their progress, disk space, scheduling condition, processed-task totals, add-queue status, and unread warnings. Buttons edit the same panel message instead of flooding the chat with command output.

The add flow accepts batches of qBittorrent-supported link formats:

- `magnet:` links;
- HTTP or HTTPS URLs that resolve to BitTorrent metainfo or a supported redirect;
- `bc://bt/` links.

Telegram `.torrent` document uploads are explicitly out of scope. One message may contain multiple links, and a user may send several messages into one draft before pressing `提交本批`. Every item is normalized, deduplicated, checked against qBittorrent and the remote media index, and processed through a durable queue that resumes after service restart.

Confidently unique items enter normal orchestrator management. Definite duplicates are not added. A same-ID/different-size result remains stopped and requires an inline confirmation. Confirmation adds it while retaining a manual hold; a later `允许调度` action is required before Planner may start it.

## 2. Chosen architecture

Use the existing orchestrator process and SQLite database rather than an in-memory conversation or a new Redis/broker service:

```text
Telegram getUpdates
  -> TelegramUpdateRouter
  -> AddBatchIngress
  -> SQLite add batch/items
  -> AddValidationWorker
       -> LinkResolver
       -> qBT stopped metadata precheck
       -> DuplicateMatcher
  -> ConfirmationGate
  -> EnrollmentService
  -> Planner

SQLite notification/inbox state
  -> TelegramNotificationSender
  -> sendMessage / editMessageText / answerCallbackQuery
```

SQLite is the source of truth for drafts, item states, approvals, warnings, and notification delivery. Telegram messages are a projection of persisted state, not the state itself. Restarting the service may replay a step, but idempotency keys prevent duplicate torrents, callbacks, or notifications.

The existing standalone `/opt/qbt-orchestrator/qbt_add_checked.py` behavior is migrated into testable modules under `src/qbt_orchestrator`; the Bot does not shell out to a legacy script or maintain a second database truth.

## 3. Authorization and Telegram API surface

Only configured operator/admin Telegram identities may create batches or change download state. Viewer roles may open status, processed-history, queue, and warning pages but cannot add or approve downloads. Unknown chats receive no operational details.

Extend the Telegram API adapter with:

- `editMessageText` for panel navigation;
- `answerCallbackQuery` for immediate button acknowledgement;
- existing `sendMessage` for proactive alerts and summaries;
- `sendDocument` only for an on-demand, generated, redacted `.txt` warning export.

Telegram's native `copy_text` button accepts at most 256 characters. The warning page therefore provides `复制摘要` for a compact native one-tap clipboard payload and `导出全部日志` for a complete redacted `.txt` generated on demand. The latter is a Bot output, not a user-uploaded `.torrent`, and is deleted from local temporary storage immediately after Telegram accepts it. Long on-screen warning collections remain paginated. See the official [`CopyTextButton`](https://core.telegram.org/bots/api#copytextbutton) definition.

Callback data carries only a compact action, object ID, and random generation token. The handler rechecks chat, user role, object state, expiry, and token inside one SQLite transaction. Replayed, expired, or already-consumed callbacks return the current result without executing the action again.

## 4. Durable data model

### 4.1 Add batches

Create `bot_add_batches`:

```text
id
batch_key                 unique
chat_id
user_id
state                     draft | queued | processing | awaiting_confirmation |
                          complete | cancelled | draft_expired
panel_message_id
received_count
valid_count
enrolled_count
duplicate_count
confirmation_count
failed_count
created_at
submitted_at
completed_at
updated_at
```

One chat/user may have only one open draft. An inactive draft expires after 30 minutes into `draft_expired`, remains visible in queue history, and may be reopened into a new batch. Expiry never silently submits links.

### 4.2 Add items

Create `bot_add_items`:

```text
id
batch_id
source_message_id
source_index
input_kind                magnet | http_url | https_url | bc_link
raw_input                 temporary, root-only SQLite storage
redacted_input
input_sha256
canonical_identity
infohash_v1
infohash_v2
display_name
normalized_media_id
total_size
primary_video_size
state
decision
decision_reason
qbt_hash
qbt_precheck_tag
remote_match_json
approval_generation
approved_by
approved_at
attempts
next_run_at
last_error
created_at
updated_at
```

The unique ingress key is `(chat_id, source_message_id, source_index)` through the parent batch. Repeated Telegram updates return the existing item. `input_sha256` deduplicates repeated input in a draft. Canonical torrent identity deduplicates equivalent links after resolution.

Raw input is stored only while resolution or enrollment may need it. On every terminal state it is cleared, while the redacted form, SHA-256, canonical identity, decision, evidence, and timestamps remain. Bot messages and general logs never contain credentials or full private URL query strings.

### 4.3 Item state machine

```text
received
  -> invalid
  -> resolving
  -> duplicate_local
  -> prechecking
       -> duplicate_remote
       -> needs_confirmation
       -> ready
  -> enrolling
       -> enrolled
       -> enrolled_hold
       -> failed
  -> cancelled
```

`needs_confirmation` does not block validation of later items or other batches. Batch completion waits until every item reaches a terminal result, including an approval or cancellation for each pending confirmation.

Create `bot_add_events` as an append-only audit of state changes, callback actor, reason code, and safe evidence. Existing `checked_add_requests/events` data remains readable and may be imported into history, but new writes use the new tables.

## 5. Batch ingress and parsing

Pressing `添加下载` edits the panel into an open-draft view. While a draft is open:

- every authorized text message is scanned line by line;
- blank lines and surrounding whitespace are ignored;
- a single line must begin with one supported scheme;
- unrelated chat text is rejected with a concise explanation and is not added to the batch;
- the Bot updates the draft count after each message;
- `继续添加`, `提交本批`, and `取消整个批次` remain available.

`提交本批` atomically closes ingress and queues the valid items. New links after submission create or join a new draft; they never mutate a processing batch. `取消整个批次` clears temporary raw inputs and cancels only items that have not already been enrolled.

The Bot does not send user-supplied links to qBT in one bulk API request. Items are processed individually so each has an exact result, audit trail, retry policy, and correlation identity.

## 6. Link resolution

### 6.1 Magnet

Parse the URI structurally rather than with substring matching. Require at least one supported exact-topic identity. Normalize 40-character hexadecimal and 32-character Base32 BTIH to lowercase hexadecimal; preserve supported BTMH identity when present. Trackers and display names do not participate in torrent identity.

Known identities are checked against active qBT torrents and prior terminal add records before metadata precheck.

### 6.2 HTTP and HTTPS

The resolver follows bounded redirects and downloads metainfo into memory with connection/read timeouts and a 10 MiB response limit. It rejects embedded URL credentials, unsupported redirect schemes, malformed bencode, missing `info`, and ordinary HTTP file responses that are not BitTorrent metainfo. It computes the canonical v1/v2 identity before qBT enrollment.

The fetcher resolves each redirect target and rejects loopback, link-local, metadata-service, and unrelated private-network addresses unless an explicit deployment allowlist permits the destination. This prevents an authorized Bot command from becoming an accidental unrestricted server-side request primitive.

The internally resolved metainfo may be submitted to qBT as multipart data. This is an implementation detail and does not enable Telegram `.torrent` document upload.

### 6.3 `bc://bt/`

Decode and validate the BitComet link structure, extract its torrent identity, and normalize it into the same identity model. Invalid encoding or missing identity is rejected before qBT. The original redacted link type remains in audit history.

## 7. Stopped metadata precheck

Every non-duplicate item enters qBT stopped with category/tag isolation:

```text
category: precheck
tags: precheck, add-item-<opaque token>, hold
```

No payload file receives download priority during precheck. The worker waits for metadata with a bounded timeout, stops the torrent again when metadata becomes available, and reads its file list, total size, main-video candidate, and qBT hash.

Video candidates use the existing checked-add policy: recognized media extensions with a preference for files at least 100 MiB. Metadata timeout, invalid file inventory, missing primary video, or qBT identity mismatch moves the item to `failed` and generates an immediate notification.

Validation concurrency is one item per worker. This avoids ambiguous qBT correlation, limits tracker/metadata bursts, and is sufficient for the expected personal queue. FIFO order is used across submitted item IDs, but `needs_confirmation` items are skipped so they cannot starve later work.

## 8. Duplicate decision

Duplicate checks are layered and deterministic:

1. **Canonical torrent identity** against active qBT and prior successful requests.
2. **Normalized media ID** extracted from the main video name using the existing normalizer.
3. **Remote media index** exact normalized ID match.
4. **Primary video size** comparison using the existing 15% tolerance.
5. Optional live remote probe only when the persisted index is stale or an indexed path requires confirmation.

Decisions are:

- same torrent identity: `duplicate_local`, do not add;
- same normalized ID and size within tolerance: `duplicate_remote`, do not add;
- same normalized ID but size differs or is unknown: `needs_confirmation`;
- no identity match: `ready`;
- fuzzy title similarity only: warning evidence, never an automatic rejection.

Temporary precheck torrents for definite duplicates or cancelled items are removed from qBT without deleting payload data. They have downloaded no selected content. Existing user torrents are never modified or removed by duplicate handling.

## 9. Confirmation and enrollment

A `needs_confirmation` item immediately sends or edits a message such as:

```text
检查完成，需要你确认

BBAN-582
待添加版本：18.6 GiB
远端版本：6.1 GiB

两个版本大小差异明显，当前任务保持暂停。
```

Buttons are:

```text
仍然添加（保持暂停）
查看比对详情
取消添加
```

Confirming transitions the item exactly once to `enrolled_hold`, changes the qBT category to managed `auto`, retains `hold`, and records the approving user and generation token. It does not start the torrent. The queue/status detail page exposes a separate `允许调度` action that removes `hold`; only then may Planner allocate download budget.

A confidently unique item removes its precheck tag/hold, enters managed `auto`, and becomes `enrolled`, waiting for normal Planner capacity allocation. Enrollment never bypasses capacity mode or directly resumes a torrent.

Cancelling removes only the precheck torrent created for that item and clears temporary raw input. Repeated approval/cancel callbacks return the persisted result.

## 10. Home panel and navigation

The selected layout is the layered mobile control panel. `/start` and `刷新面板` render at most three active tasks with numeric and graphical progress, followed by:

- available disk space;
- a natural-language scheduling condition;
- add-queue counts;
- completed-download, completed-ingest, unresolved-error, and automatic-reclaim totals;
- unread warning count.

Example capacity text:

```text
空间不足，已暂停启动新任务；系统正在寻找能够安全释放的文件。
```

The welcome panel never exposes `DRAIN`, `capacity_deadlock`, PID, plan-generation, raw budget codes, or lease internals. Detailed technical evidence remains available from a secondary diagnostic view.

Inline navigation:

```text
运行状态 | 添加下载
添加队列 | 警告
刷新面板
```

`添加队列` shows `正在检查`, `等待确认`, `等待调度`, and `处理失败` counts plus paginated item details. Processed-history pages show processing time and media ID/torrent name for downloads, ingests, errors, and automatic reclaims.

## 11. Notifications and warning inbox

Notification behavior is:

- `needs_confirmation`: immediate item notification;
- validation/enrollment failure: immediate item notification;
- normal success: no per-item chat message;
- batch terminal completion: one proactive summary with success, duplicate, confirmed-hold, cancelled, and failure counts;
- existing automatic reclaim: immediate completion notification with name, magnet, and released bytes.

All messages have durable deduplication keys. Delivery retries use the existing `bot_notifications` queue and do not rerun the underlying add action.

Create a durable warning inbox separate from outbound delivery state. Warning records have severity, topic, safe message, related hash/job/batch/item IDs, occurrence/update timestamps, and resolved status. Per-chat read state determines the unread badge. Opening the warning page marks only displayed records as read; it does not resolve them.

The warning page supports pagination, acknowledgement where applicable, `复制摘要`, and `导出全部日志`. The export contains every related persisted warning/event selected by the current filter, is capped by a configured row/byte limit, and states explicitly when truncation occurred. Raw secrets, full private URLs, WebUI credentials, and Bot tokens never appear.

## 12. Recovery and failure handling

On daemon startup:

- recover submitted batches whose items are non-terminal;
- reset expired worker leases with generation fencing;
- inspect qBT for persisted `qbt_hash` or unique precheck tags before retrying an add;
- never create a second torrent when an earlier call succeeded but its response was lost;
- recreate missing Telegram panel projections from SQLite state;
- retry queued notifications independently of item execution.

HTTP resolution uses bounded retries only for transient network errors. Invalid formats, definite duplicates, authorization failures, and explicit cancellations are terminal. qBT/WebUI authentication failure pauses the validation worker, raises one deduplicated warning, and leaves queued items recoverable.

## 13. Tests

Focused tests cover:

1. Multi-line and multi-message draft ingestion, ordering, and batch submission.
2. Mixed `magnet`, HTTP, HTTPS, and `bc://bt/` parsing; Telegram documents are ignored/rejected.
3. BTIH hexadecimal/Base32 normalization and supported v2 identity handling.
4. HTTP timeout, redirect, size, bencode, credential, and restricted-address guards.
5. Batch/item ingress and callback idempotency under repeated Telegram updates.
6. Restart recovery at every non-terminal item state.
7. qBT precheck remains stopped and downloads no selected payload.
8. Active qBT identity, remote ID/size, same-ID/different-size, and fuzzy-only duplicate decisions.
9. Same-ID/different-size buttons keep the torrent held after confirmation.
10. `允许调度` is a separate authorized action and is the only action that removes the manual hold.
11. A waiting confirmation does not block later queue items.
12. Immediate confirmation/failure notices and one final batch summary are deduplicated.
13. Welcome and queue pages show natural language, correct counts, and no internal operational codes.
14. Warning unread/read state is separate from outbound notification delivery state.
15. Native copy summaries respect the 256-character Bot API limit, and full warning exports are redacted, bounded, and remove their temporary file after send.
16. Authorization rejects unknown users, stale callback generations, and role-inappropriate actions.

The full orchestrator suite must pass in addition to focused Telegram and checked-add tests.

## 14. Live rollout

This feature is deployed only after the shared-capacity-assessment release is accepted.

1. Run focused and full local tests.
2. Commit and push an immutable release.
3. Back up the live environment, state database including WAL/SHM, active release pointer, unit definition, legacy checked-add script/config/database, and recent logs.
4. Deploy schema and code with Bot add callbacks disabled.
5. Verify `/start`, panel editing, role authorization, warning inbox counts, and notification retries using read-only views.
6. Enable batch ingress for the configured admin chat.
7. Submit a canary batch containing one valid unique magnet, one exact duplicate, one same-ID/different-size case if available, and one invalid URL. Keep every canary stopped during validation.
8. Verify per-item database states, qBT tags/categories, immediate confirmation/failure messages, final batch summary, and restart recovery.
9. Confirm that an approved ambiguous item remains held and that `允许调度` is required to remove the hold.
10. Leave the feature enabled, monitor one hour of Bot polling, qBT authentication, queue latency, notification delivery, SQLite errors, and service restarts, then continue normal persistent operation if healthy.

Only the orchestrator daemon is restarted. qBittorrent remains online.

## 15. Rollback

Disable the Bot batch-add feature flag to stop new drafts while leaving status and existing notification delivery available. Non-terminal batches remain persisted for replay after correction. Code rollback repoints the live release and restores the prior daemon environment; additive SQLite tables remain backward compatible.

Rollback never removes already enrolled torrents. Precheck torrents are individually listed before any manual cleanup, and no payload deletion is performed by the Bot rollback path.

## 16. Acceptance criteria

The Telegram change is complete only when all of the following are demonstrated:

1. `/start` shows the approved natural-language panel and inline navigation.
2. A batch can contain multiple mixed supported links across multiple Telegram messages.
3. Telegram `.torrent` documents are not accepted.
4. Each submitted item has a durable, restart-safe, idempotent result.
5. Definite local/remote duplicates are not enrolled.
6. Same-ID/different-size items expose confirmation buttons and remain stopped and held after approval.
7. Unique items enter managed scheduling without bypassing capacity allocation.
8. Waiting confirmations do not block other queue work.
9. Confirmation and failure messages arrive immediately; normal successes are combined into one proactive terminal batch summary.
10. The homepage and queue detail show correct live queue counts.
11. Warnings are persisted with per-chat unread state and do not flood the chat with raw logs.
12. `复制摘要` copies a bounded native payload, while `导出全部日志` sends a complete redacted text export within configured safety limits.
13. Unknown users and replayed/stale callbacks cannot inspect or mutate operational state.
14. The orchestrator remains active with zero unexpected restarts, qBT authentication remains healthy, and live logs contain no unhandled Telegram, queue, or SQLite errors during the one-hour observation.
