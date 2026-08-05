# Transient qBT `add-item-*` tags

Opaque `add-item-<32hex>` tags are temporary ownership fences used while
CheckedAdd enrolls a torrent. They must not linger as empty global definitions
in the qBT WebUI sidebar, and torrents that still carry them must not be
started, uploaded, or cleaned by background workers.

## Runtime knobs

| Variable | Default | Meaning |
| --- | --- | --- |
| `QBT_ORCH_ADD_TAG_GC_ENABLED` | off | Build and schedule `qbt_tag_janitor` |
| `QBT_ORCH_ADD_TAG_GC_DRY_RUN` | `1` | Report candidates without calling `deleteTags` |
| `QBT_ORCH_ADD_TAG_GC_INTERVAL_SEC` | `300` | Janitor loop interval |
| `QBT_ORCH_ADD_TAG_GC_BATCH_LIMIT` | `25` | Max tag definitions deleted per tick |

Daemon global dry-run also forces the janitor into dry-run. Live deletion
requires a live daemon **and** `QBT_ORCH_ADD_TAG_GC_DRY_RUN=0`.

MetadataProbe, CheckedAdd, and TagJanitor share one `QbtPrecheckGateway`
instance when any of them is enabled.

## Reading dry-run candidates

Janitor ticks write `events_v2` rows with `component=qbt_tag_janitor` and
`event_type=dry_run_candidates`. The payload includes:

- `candidate_count`
- `sample_tags` (up to five tag names)
- `dry_run: true`

Daemon tick logs also include the janitor result:

```json
{
  "status": "dry_run",
  "global_count": 14,
  "referenced_count": 2,
  "assigned_count": 2,
  "candidate_count": 12,
  "deleted": [],
  "fenced": [],
  "errors": 0
}
```

## Dual-evidence delete rule

A tag is deleted only when **all** of the following hold:

1. Name matches `^add-item-[a-f0-9]{32}$` (current generator format only).
2. No `bot_add_items.qbt_precheck_tag` row references it (any item state).
3. No torrent currently carries the tag in qBT snapshots.
4. Pre-delete guard re-checks SQLite refs and `torrents/info?tag=<tag> == []`.

Live GC calls only `/api/v2/torrents/deleteTags`. It never calls torrent delete
APIs and never passes `deleteFiles`.

## Disable / rollback

1. Set `QBT_ORCH_ADD_TAG_GC_ENABLED=0` (or keep `DRY_RUN=1`) and restart.
2. Roll `/opt/emby_qbt_auto/current` back to the previous release if enrollment
   recovery misbehaves.
3. This change does not add SQLite schema. Restore the state DB only if data
   corruption is observed. Empty tag definitions removed by GC do not need
   restoration; new adds recreate tags as needed.

State DB backup location (typical VPS):

`/var/lib/qbt-orchestrator/state.sqlite`

## Stuck enrollment

CheckedAdd enrollment is crash-replayable. Failures bump `attempts`, set
`last_error` to a safe code (for example `qbt_stop_not_observed`), and schedule
`next_run_at = now + min(300, 5 * 2**min(attempts, 6))`.

After three consecutive failures the worker upserts WarningInbox key:

`checked_add:enrollment_stuck:<item_id>`

Successful enrollment resolves that warning. While the opaque tag remains,
Planner / Engine / Soak / upload / cleanup / reclaim / path reconcile treat
the torrent as unmanaged.

## Rollout checklist

1. Enable GC with dry-run; confirm candidate counts match empty sidebar tags.
2. Confirm each candidate has zero SQLite refs and zero qBT users.
3. Flip `QBT_ORCH_ADD_TAG_GC_DRY_RUN=0` for one GC tick.
4. Verify torrent count, downloaded bytes, and local files are unchanged.
5. Watch `enrolling` age, WarningInbox, and journal for repeated
   `qbt_precheck_not_stopped` / `qbt_write_fenced` storms.

## Local verification (2026-08-05)

- Branch: `cursor/qbt-transient-add-tag-c210`
- `python -m pytest -q`: **1141 passed, 3 skipped**
- `python -m compileall -q src` and `git diff --check`: clean
- VPS dry-run/live GC rollout remains an operator step using the checklist above
