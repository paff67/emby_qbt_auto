# Telegram persistent console and failure warnings

## `/start` console lifecycle

Each authorized `/start` creates a **new** console message at the bottom of the
chat and binds `telegram_panel_session` to that `message_id`.

Flow:

1. Read the previous session (if any)
2. Render the home view
3. `sendMessage` (always; never reuse the old message for `/start`)
4. `bind_new` increments `panel_generation` and stores the new message
5. Best-effort `deleteMessage` on the old console; on failure, edit it to
   “此控制台已刷新，请使用最新控制台消息。” with no buttons

`/help`, pagination, and status refresh continue to **edit** the current bound
message. Duplicate Telegram `update_id` values for `/start` are ignored via
`last_start_update_id`.

If `sendMessage` fails, the previous session binding is left unchanged.

## Stale callback fencing

When a console is bound for the chat:

- `n:*` / `a:*` / `w:*` callbacks must target the current `message_id`
- `i:*` callbacks from `last_retired_message_id` are rejected
- Confirmation buttons on separate notification messages remain allowed

Rejected callbacks answer with “控制台已刷新，请使用最新消息” and do not mutate
SQLite.

## Failure item WarningInbox projection

`BatchFailureWarningProjector` (daemon loop `batch_failure_warnings`, 15s)
upserts one WarningInbox row per item in:

- `failed`
- `invalid`
- `metadata_unavailable`

Key: `checked_add:batch_failed:{batch_id}:{item_id}`

Message format includes 番号/`display_name` (from magnet `dn` when available)
or `第 N 条 · hash…` fallback. Magnet URIs and tokens are never written into
`safe_message`.

When the item recovers into enrolled/cancelled/ready/… the projector resolves
the matching warning. Metadata-unavailable notifications keep the historical
key `checked_add:metadata_unavailable:{item_id}` and now include the item label.

## Migration 25

```text
panel_generation
last_start_update_id
last_retired_message_id
```

Idempotent via duplicate-column handling; existing panel and warning rows are
preserved.

## VPS checklist

1. Backup `/var/lib/qbt-orchestrator/state.sqlite`
2. Deploy and restart the daemon
3. Send `/start` twice → second message is current; old buttons toast only
4. Submit a failing link → Warning center shows 番号/fallback label
5. Refresh home → edits the latest console only
