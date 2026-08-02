# Processed Media Ledger Operations

The `processed_media` ledger tracks observed → downloaded → uploaded → ingested titles and permanent manual-deletion tombstones (`download_policy='block_permanent'`). Tombstones are checked during checked-add before the remote media index and surface as `blocked_manual_deleted`.

## Backfill

Dry-run (default):

```bash
python3 -m qbt_orchestrator.cli processed-media backfill --state-db /var/lib/qbt-orchestrator/state.sqlite --json
```

Apply:

```bash
python3 -m qbt_orchestrator.cli processed-media backfill --state-db /var/lib/qbt-orchestrator/state.sqlite --apply --json
```

Counts: `inserted` / `updated` / `skipped` / `conflict`. Rows with only `normalize_failed` / `missing_remote` / unnormalized names are skipped and never treated as successfully ingested.

## Register a tombstone from an audit manifest

```bash
python3 -m qbt_orchestrator.cli processed-media tombstone \
  --state-db /var/lib/qbt-orchestrator/state.sqlite \
  --id BBAN-582 \
  --deleted-at 1754116704 \
  --actor ops \
  --manifest /opt/qbt/manual-deletions/bban-remove-20260802T143824+0800/manifest.json \
  --reason "manual_deletion_audit" \
  --create-from-audit \
  --apply --json
```

`--deleted-at` is a Unix epoch. The command prints inserted/updated/skipped/conflict counts and the SHA-256 of the input manifest.

## Production seed for the five BBAN titles

Deletion time: `2026-08-02T14:38:24+08:00` (`1754116704` epoch).

Manifest: `/opt/qbt/manual-deletions/bban-remove-20260802T143824+0800/manifest.json`

IDs: `BBAN-574`, `BBAN-576`, `BBAN-580`, `BBAN-582`, `BBAN-586`.

Example (do not run from this document during code landing):

```bash
for id in BBAN-574 BBAN-576 BBAN-580 BBAN-582 BBAN-586; do
  python3 -m qbt_orchestrator.cli processed-media tombstone \
    --state-db /var/lib/qbt-orchestrator/state.sqlite \
    --id "$id" \
    --deleted-at 1754116704 \
    --actor ops \
    --manifest /opt/qbt/manual-deletions/bban-remove-20260802T143824+0800/manifest.json \
    --reason "bban-remove-20260802T143824+0800" \
    --batch-key "bban-remove-20260802T143824+0800" \
    --create-from-audit \
    --apply --json
done
```

## Enforcement flag

Checked-add tombstone enforcement is gated by `QBT_ORCH_PROCESSED_MEDIA_ENFORCE` (default `0`). The ledger may be backfilled and seeded while enforcement remains off.

There is no untombstone API. Clearing `block_permanent` is rejected by database triggers.
