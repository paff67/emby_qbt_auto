# Telegram Batch Download Queue and Control UI Design

Date: 2026-07-23
Status: Approved design
Scope: qBT Orchestrator Telegram Bot, checked download enrollment, warning inbox, and user-facing status on `ssh.paff-67.top`

## 0. 2026-08-02 P1 override

This section supersedes the WarningInbox, Telegram transport, panel routing, and duplicate presentation portions of the 2026-07-23 plan where they conflict. The existing link parser, 500/50/1000 queue limits, metadata probe state machine, and qBT isolation remain in force.

1. There is exactly one `getUpdates` consumer: `TelegramPollingService` under the existing `TelegramSupervisor`.
2. `bot_warning_inbox` is reused. `bot_warning_reads` is dropped and never read or recreated by the new release.
3. Warning read state is global for the configured administrator: `resolved=0` means unread and `resolved=1` means read.
4. Every warning is committed to `bot_warning_inbox` before any `bot_notifications` projection is attempted.
5. `remote_media_index` remains a current-presence cache. It is not history and cannot represent a manual deletion tombstone.
6. A manual deletion creates `download_policy='block_permanent'`. No Telegram callback, ordinary CLI option, remote-index refresh, or file restoration may clear it.
7. Checked-add returns `blocked_manual_deleted`, not `duplicate_remote`, for a tombstoned normalized ID.
8. Telegram does not support user-uploaded `.torrent` documents. `sendDocument` is used only for generated warning exports.
9. All user-facing pages are natural-language Chinese. Internal terms such as `DRAIN`, `capacity_deadlock`, PID, generation, lease, and raw reason codes are forbidden in normal pages.
10. Page size is 8 rows, body limit is 3,500 characters, callback data is at most 64 UTF-8 bytes, copy text is at most 256 characters, and exports are at most 1,000 lines/512,000 UTF-8 bytes.

Initial production tombstone seed IDs: `BBAN-574`, `BBAN-576`, `BBAN-580`, `BBAN-582`, `BBAN-586`.

## 1. Outcome

`/start` opens a mobile-friendly Telegram control panel implemented with ordinary `InlineKeyboardMarkup`. The welcome message uses clear natural language, shows active downloads and their progress, disk space, scheduling condition, processed-task totals, add-queue status, and unread warnings. Buttons edit the same panel message instead of flooding the chat with command output.

The add flow accepts batches of qBittorrent-supported link formats:

- `magnet:` links;
- HTTP or HTTPS URLs that resolve to BitTorrent metainfo or a supported redirect;
- `bc://bt/` links.

Telegram `.torrent` document uploads are explicitly out of scope. One message may contain multiple links, and a user may send several messages into one draft before pressing `提交本批`. Every item is normalized, deduplicated, checked against qBittorrent and the remote media index, and processed through a durable queue that resumes after service restart.

One logical submission accepts at most 500 links and is internally divided into shards of 50 items. The global submitted, non-terminal backlog is capped at 1,000 items. These are validated configuration defaults, not unbounded in-memory collections.

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
       -> MetadataProbeCoordinator
            -> bounded qBT metadata slots
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

Create `bot_add_shards` so a large logical submission is processed and summarized in bounded groups:

```text
id
batch_id
shard_index
state                     queued | processing | complete | cancelled
item_count
processed_count
created_at
completed_at
updated_at
```

`(batch_id, shard_index)` is unique. Shards are an internal scheduling boundary; the Telegram user continues to see one logical submission and one final submission summary.

### 4.2 Add items

Create `bot_add_items`:

```text
id
batch_id
source_message_id
source_index
input_kind                magnet | http_url | https_url | bc_link
raw_input                 temporary, root-only SQLite storage
raw_input_expires_at
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
metadata_probe_attempt
metadata_probe_started_at
metadata_probe_deadline
metadata_next_poll_at
metadata_retry_at
metadata_lease_owner
metadata_lease_generation
approved_by
approved_at
attempts
next_run_at
last_error
created_at
updated_at
```

The unique ingress key is `(chat_id, source_message_id, source_index)` through the parent batch. Repeated Telegram updates return the existing item. `input_sha256` deduplicates repeated input in a draft. Canonical torrent identity deduplicates equivalent links after resolution.

Raw input is stored only while resolution, scheduled retry, or an explicit manual-retry window may need it. It expires no later than seven days after receipt and is cleared immediately on enrollment, duplicate, cancellation, permanent failure, or retry expiry. `metadata_unavailable` may retain it only until that deadline so the displayed retry actions remain functional. The state database and backups must be root-readable only. The redacted form, SHA-256, canonical identity, decision, evidence, and timestamps remain after clearing. Bot messages and general logs never contain credentials or full private URL query strings.

### 4.3 Item state machine

```text
received
  -> invalid
  -> resolving
  -> duplicate_local
  -> waiting_probe_slot
  -> metadata_wait
       -> metadata_retry_wait
       -> metadata_unavailable
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

`metadata_retry_wait` is deferred, not worker-blocking. `metadata_unavailable` is terminal for automatic processing but exposes manual retry actions.

Create `bot_add_events` as an append-only audit of state changes, callback actor, reason code, and safe evidence. Existing `checked_add_requests/events` data remains readable and may be imported into history, but new writes use the new tables.

## 5. Batch ingress and parsing

Pressing `添加下载` edits the panel into an open-draft view. While a draft is open:

- every authorized text message is scanned line by line;
- blank lines and surrounding whitespace are ignored;
- a single line must begin with one supported scheme;
- unrelated chat text is rejected with a concise explanation and is not added to the batch;
- the Bot updates the draft count after each message;
- `继续添加`, `提交本批`, and `取消整个批次` remain available.

Effective ingress limits are:

```text
maximum links per logical submission: 500
internal shard size:                   50
maximum submitted non-terminal items: 1,000
maximum encoded link length:           8 KiB
maximum raw draft input:                2 MiB
```

The Bot never silently truncates input. If one incoming Telegram message would push a draft above 500 links or 2 MiB, that whole message is rejected and the existing draft remains unchanged. If the global queue is full when `提交本批` is pressed, the batch remains a visible draft and the Bot reports the current capacity instead of partially submitting it. The user may submit the draft after backlog falls or cancel it.

`提交本批` atomically closes ingress and queues the valid items. New links after submission create or join a new draft; they never mutate a processing batch. `取消整个批次` clears temporary raw inputs and cancels only items that have not already been enrolled.

The Bot does not send user-supplied links to qBT in one bulk API request. Items are processed individually so each has an exact result, audit trail, retry policy, and correlation identity. Shards bound counters and summaries; they do not cause 50 simultaneous qBT additions.

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

## 7. Bounded, non-blocking metadata precheck

### 7.1 Coordinator and slots

Metadata acquisition is a persisted state machine, not a worker call that sleeps until a long timeout. `MetadataProbeCoordinator` has three global active slots. Starting or mutating a qBT precheck remains serialized, but up to three already-correlated magnet probes may acquire metadata concurrently.

The coordinator claims due items with a lease generation, starts a probe, persists `metadata_probe_deadline` and `metadata_next_poll_at`, and returns immediately. During an active window the poll interval is five seconds, and a poller examines only due rows. No worker thread or scheduler tick waits synchronously for metadata.

Slots are assigned round-robin across logical batches, with at most one slot per batch while at least three batches have eligible work. A 500-link submission therefore cannot monopolize all probe capacity. `needs_confirmation`, retry backoff, and unavailable items never occupy a slot.

HTTP/HTTPS metainfo successfully resolved in memory and valid decoded `bc://bt/` identities normally bypass magnet metadata acquisition. They still pass through the same stopped qBT inventory and duplicate checks.

### 7.2 qBT isolation

Every item that requires qBT metadata enters with category/tag isolation:

```text
category: precheck
tags: precheck, metadata-probe, add-item-a1b2c3d4, hold
```

The `a1b2c3d4` suffix is an example; production generates a random opaque eight-character token persisted on the item row.

The probe uses a 1 KiB/s temporary payload download limit so metadata extension traffic can proceed while payload transfer remains negligible. As soon as metadata appears, the next due poll stops the torrent, sets every file priority to zero, verifies the opaque item tag/hash correlation, and reads the file list, total size, main-video candidate, and qBT hash. It never changes the torrent to managed `auto` or removes `hold` during precheck.

Video candidates use the existing checked-add policy: recognized media extensions with a preference for files at least 100 MiB.

### 7.3 Probe windows and backoff

Automatic probe policy is:

```text
attempt 1: active for 5 minutes, then retry after 30 minutes
attempt 2: active for 10 minutes, then retry after 6 hours
attempt 3: active for 15 minutes, then metadata_unavailable
```

On each timed-out attempt the coordinator fences the lease, stops and revalidates the temporary torrent, removes only that temporary qBT registration with payload deletion disabled, persists the next retry time, and releases the slot. The original link remains temporarily available for the scheduled retry. There is no indefinite `observe` task.

After the third timeout, the item becomes `metadata_unavailable` and immediately exposes:

```text
重新尝试
24 小时后再试一次
取消并移除
```

`重新尝试` resets the approved three-window automatic policy. `24 小时后再试一次` creates a new approval generation and schedules one 15-minute window at `now + 24 hours`; it occupies no slot during the wait and does not resume an unbounded background torrent. Invalid file inventory, missing primary video, qBT identity mismatch, or a fenced-operation conflict moves the item to `failed` and generates an immediate notification.

### 7.4 Submission progress

When all immediately runnable items have either completed validation or entered a persisted retry/confirmation state, the Bot sends an initial processing summary. Deferred metadata items continue in the background without blocking later shards or batches. A final summary is sent after all deferred items become terminal.

The queue panel shows `正在获取元数据`, `等待元数据重试`, and `长时间无元数据` counts. Routine progress edits the existing panel; it does not produce one chat message per poll or per successful item.

## 8. Duplicate decision

Duplicate checks are layered and deterministic:

1. **Canonical torrent identity** against active qBT and prior successful requests.
2. **Normalized media ID** extracted from the main video name using the existing normalizer.
3. **Processed-media tombstone ledger** for `download_policy='block_permanent'` (checked immediately after confident normalization and before remote-index scan).
4. **Remote media index** exact normalized ID match (current presence only; never a deletion tombstone).
5. **Primary video size** comparison using the existing 15% tolerance.
6. Optional live remote probe only when the persisted index is stale or an indexed path requires confirmation.

Decisions are:

- same torrent identity: `duplicate_local`, do not add;
- tombstoned normalized ID: `blocked_manual_deleted` / `previously_ingested_then_manually_deleted`, terminal `cancelled`, do not add;
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

For large or slow submissions it additionally shows shard progress and metadata state, for example:

```text
本次提交 327 条 · 分为 7 个处理分片
已完成 83 · 正在获取元数据 3
等待重试 6 · 长时间无元数据 2
```

## 11. Notifications and warning inbox

Notification behavior is:

- `needs_confirmation`: immediate item notification;
- validation/enrollment failure: immediate item notification;
- final metadata exhaustion: immediate item notification with retry/cancel buttons;
- normal success: no per-item chat message;
- all immediately runnable work exhausted while deferred probes remain: one initial processing summary;
- batch terminal completion: one proactive summary with success, duplicate, confirmed-hold, cancelled, and failure counts;
- existing automatic reclaim: immediate completion notification with name, magnet, and released bytes.

All messages have durable deduplication keys. Delivery retries use the existing `bot_notifications` queue and do not rerun the underlying add action.

Create a durable warning inbox separate from outbound delivery state. Warning records have severity, topic, safe message, related hash/job/batch/item IDs, occurrence/update timestamps, and resolved status. Read state is global for the configured administrator (`resolved=0` unread, `resolved=1` read); the legacy `bot_warning_reads` per-chat table is removed. Opening a warning detail marks that one occurrence read; merely opening the list, copying a summary, or exporting text does not.

The warning page supports pagination, acknowledgement where applicable, `复制摘要`, and `导出全部日志`. The export contains every related persisted warning/event selected by the current filter, is capped by a configured row/byte limit, and states explicitly when truncation occurred. Raw secrets, full private URLs, WebUI credentials, and Bot tokens never appear.

## 12. Recovery and failure handling

On daemon startup:

- recover submitted batches whose items are non-terminal;
- reset expired worker leases with generation fencing;
- reclaim expired metadata-probe leases, then reconcile the persisted item with its opaque qBT precheck tag before allocating a slot;
- inspect qBT for persisted `qbt_hash` or unique precheck tags before retrying an add;
- never create a second torrent when an earlier call succeeded but its response was lost;
- recreate missing Telegram panel projections from SQLite state;
- retry queued notifications independently of item execution.

HTTP resolution uses bounded retries only for transient network errors. Invalid formats, definite duplicates, authorization failures, and explicit cancellations are terminal. qBT/WebUI authentication failure pauses the validation worker, raises one deduplicated warning, and leaves queued items recoverable.

## 13. Tests

Focused tests cover:

1. Multi-line and multi-message draft ingestion, ordering, and batch submission.
2. The 500-link, 50-item-shard, 1,000-global-item, 8-KiB-link, and 2-MiB-draft limits reject atomically without truncation.
3. Mixed `magnet`, HTTP, HTTPS, and `bc://bt/` parsing; Telegram documents are ignored/rejected.
4. BTIH hexadecimal/Base32 normalization and supported v2 identity handling.
5. HTTP timeout, redirect, size, bencode, credential, and restricted-address guards.
6. Batch/item ingress and callback idempotency under repeated Telegram updates.
7. Restart recovery at every non-terminal item state.
8. Three active metadata slots, round-robin batch fairness, due-only polling, and lease-generation fencing.
9. The 5/10/15-minute windows and 30-minute/6-hour backoffs release slots without blocking later items.
10. Metadata timeout removes the temporary qBT registration without payload deletion and never leaves an indefinite observer running.
11. qBT precheck remains held, payload-limited, and stops with all file priorities zero when metadata arrives.
12. Initial deferred-work and final terminal summaries are independently deduplicated.
13. Active qBT identity, remote ID/size, same-ID/different-size, and fuzzy-only duplicate decisions.
14. Same-ID/different-size buttons keep the torrent held after confirmation.
15. `允许调度` is a separate authorized action and is the only action that removes the manual hold.
16. A waiting confirmation or metadata backoff does not block later queue items.
17. Immediate confirmation/failure notices and final batch summaries are deduplicated.
18. Welcome and queue pages show natural language, correct counts, and no internal operational codes.
19. Warning unread/read state is separate from outbound notification delivery state.
20. Native copy summaries respect the 256-character Bot API limit, and full warning exports are redacted, bounded, and remove their temporary file after send.
21. Authorization rejects unknown users, stale callback generations, and role-inappropriate actions.

The full orchestrator suite must pass in addition to focused Telegram and checked-add tests.

## 14. Live rollout

This feature is deployed only after the shared-capacity-assessment release is accepted.

1. Run focused and full local tests.
2. Commit and push an immutable release.
3. Back up the live environment, state database including WAL/SHM, active release pointer, unit definition, legacy checked-add script/config/database, and recent logs.
4. Deploy schema and code with Bot add callbacks disabled.
5. Verify `/start`, panel editing, role authorization, warning inbox counts, and notification retries using read-only views.
6. Enable batch ingress for the configured admin chat.
7. Submit a canary batch containing one valid unique magnet, one exact duplicate, one same-ID/different-size case if available, one invalid URL, and one magnet that does not return metadata within the first probe window. Keep every canary isolated during validation.
8. Verify per-item database states, qBT tags/categories, slot release/backoff, immediate confirmation/failure messages, initial/final batch summaries, and restart recovery.
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
3. A logical submission accepts up to 500 links, shards at 50, applies a 1,000-item global backlog cap, and never silently truncates overflow.
4. Telegram `.torrent` documents are not accepted.
5. Each submitted item has a durable, restart-safe, idempotent result.
6. At most three metadata probes run concurrently, with fair batch rotation and no blocking worker wait.
7. Timed-out probes release their slot, follow the approved retry schedule, and never remain as indefinite running observers.
8. Definite local/remote duplicates are not enrolled.
9. Same-ID/different-size items expose confirmation buttons and remain stopped and held after approval.
10. Unique items enter managed scheduling without bypassing capacity allocation.
11. Waiting confirmations and metadata backoffs do not block other queue work.
12. Confirmation and failure messages arrive immediately; deferred work gets an initial summary and all terminal results produce one final summary.
13. The homepage and queue detail show correct live queue and metadata counts.
14. Warnings are persisted with single-admin unread state (`resolved`) and do not flood the chat with raw logs.
15. `复制摘要` copies a bounded native payload, while `导出全部日志` sends a complete redacted text export within configured safety limits.
16. Unknown users and replayed/stale callbacks cannot inspect or mutate operational state.
17. The orchestrator remains active with zero unexpected restarts, qBT authentication remains healthy, and live logs contain no unhandled Telegram, queue, or SQLite errors during the one-hour observation.
