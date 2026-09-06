# Scheduler v3 rollout and rollback

## Safety contract

- Rollout never edits or removes `hold`.
- Capacity deadlock blocks new exploratory work. Viability-aware detection excludes stale tasks without a complete source. Optional dead-partial reclaim is deployed dry-run first and may reset only validated `dead` payloads while retaining the qBT torrent record.
- SQLite migrations are additive. Rollback keeps `state.sqlite`; there is no downgrade migration.
- Full-torrent delete is disabled by default and requires remote verification, healthy qBT sync, source presence, and seed policy approval.

## Preflight

1. Record the release commit and current symlink target.
2. Back up `/etc/qbt-orchestrator/daemon.env`, config, systemd units, and SQLite with WAL/SHM while the service is stopped or via SQLite backup API.
3. Run `deploy/scripts/run-dry-run.sh <release-dir>`.
4. Require the full test suite, additive migration preview, finite daemon dry-run, status output, and zero destructive dry-run actions.

## Staged rollout

| Stage | Change | Minimum observation / gate |
|---|---|---|
| 0 | Deploy code; `SCHEDULER_ENGINE=legacy`; batch/soak/full-cleanup off or dry-run | Migration and dry-run only |
| 1 | Transition metrics/logging | No repeated unchanged decisions |
| 2 | `BACKGROUND_PERIODIC_WORKERS=1` | Safety P99 <3s and max <5s for 24h |
| 3 | Host/delta qBT session | Delta ratio >=99% for 24h; degraded mode explicit |
| 4 | Resource ledger shadow marker | Zero unsafe budget divergence for 24h |
| 5 | `SCHEDULER_ENGINE=shadow` | 48h; no hold/mode/budget invariant violation |
| 6 | Scheduler live on operational allowlist | Small canary; compare plan generations/actions |
| 7 | Soak then batch canary | Inventory <=8 hashes/min; no unowned claims |
| 8 | Upload phases live | Verify retry copy count remains one |
| 9 | Optional full cleanup dry-run, then live | Explicit approval; seed/ratio policy and sync gate verified |
| 10 | `CAPACITY_RECLAIM=1`, dry-run first | Review exact paths/allocated bytes; then enable live one item per interval |

Advance one stage at a time. Reset the observation window after any config or code change.

## Stop/rollback triggers

- Safety P99 >=3s or max >=5s.
- Planner P95 >=500ms, stale generation execution, or budget overrun.
- Delta ratio <99% without an explicit degraded signal.
- More than 8 files/properties inventory calls per minute.
- Any automatic action against `hold`/`seed-long`, any reclaim outside the configured incomplete root, any overlapping/shared torrent path, any torrent-record deletion, or any unowned active batch claim.
- Verify retry repeats copy, cleanup runs without remote verification/seed approval, or attempts exceed `max_attempts`.

Run `deploy/scripts/rollback.sh [previous-release-dir]`. It stops the daemon, backs up and rewrites the env to legacy/dry-run/off switches, optionally changes the release symlink, and enables the legacy timer. It does not remove schema or state.

## Post-rollback checks

```bash
systemctl status qbt-orchestrator.timer --no-pager -l
readlink -f /opt/emby_qbt_auto/current
grep -E 'QBT_ORCH_(SCHEDULER_ENGINE|BATCH_PIPELINE|SOAK_ENABLED|FULL_CLEANUP|UPLOAD_DRY_RUN)' /etc/qbt-orchestrator/daemon.env
python3 -m qbt_orchestrator.cli status --json --state-db /var/lib/qbt-orchestrator/state.sqlite
```

Confirm `SCHEDULER_ENGINE=legacy`, exploratory workers are off, uploads are dry-run, the SQLite DB remains readable, and no new destructive action was emitted.
