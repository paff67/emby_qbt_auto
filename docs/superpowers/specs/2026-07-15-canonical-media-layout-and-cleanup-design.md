# Canonical Media Layout, Naming, Migration, and Cleanup Design

Date: 2026-07-15  
Status: Approved design (方案 A)  
Scope: qBT Orchestrator, Javinizer/Google Drive backfill, Emby refresh, and live cleanup on `ssh.paff-67.top`

## 1. Outcome

All recognized media use one canonical remote directory, one canonical display label, and one final verification barrier before local cleanup.

For a movie whose normalized ID is `BBAN-582` and whose Javinizer/JavDB title is `いじられキャラはもっとえっちないじりを期待している`, the canonical result is:

```text
gcrypt:/BBAN-582/
  BBAN-582 いじられキャラはもっとえっちないじりを期待している.mp4
  BBAN-582 いじられキャラはもっとえっちないじりを期待している.nfo
  BBAN-582 いじられキャラはもっとえっちないじりを期待している-poster.jpg
  BBAN-582 いじられキャラはもっとえっちないじりを期待している-fanart.jpg
  poster.jpg
  fanart.jpg
  thumb.jpg
  extrafanart/
```

The top-level directory remains the pure normalized ID. This preserves the existing dominant remote folder structure and prevents path churn when metadata titles change.

## 2. Canonical naming policy

### 2.1 Metadata source and precedence

The normalized ID comes from `jav_name_normalize.py`. The title comes from the Javinizer database using this precedence:

1. `movies.display_title`
2. `movies.title`
3. `movies.original_title`
4. normalized ID as a last-resort display value

Only normalization confidence at or above `0.95` is eligible for automatic destructive migration. FC2/HEYZO matches above that threshold are included. Lower-confidence and unmatched media are preserved in place and reported for manual review.

### 2.2 Display and filesystem values

The full Emby display title is:

```text
<NORMALIZED_ID><single ASCII space><JAVDB_TITLE>
```

NFO fields are:

```xml
<title>BBAN-582 影片名称</title>
<originaltitle>影片名称</originaltitle>
<sorttitle>BBAN-582</sorttitle>
<id>BBAN-582</id>
```

The full NFO title is not truncated. Filesystem basenames are NFKC-normalized, have control characters and `\/:*?"<>|` replaced with `_`, collapse whitespace, trim trailing spaces/dots, and are limited to 120 Unicode code points before the extension. The normalized ID prefix is never truncated.

For multi-part media, a detected stable part suffix is appended after the canonical label, for example `BBAN-582 影片名称-CD1.mp4`. When two distinct source videos still collide, the second target receives a deterministic eight-character source-path digest and the collision is recorded.

### 2.3 Sidecar co-location

File-specific NFO, poster, fanart, thumb, and subtitle names use the same canonical video basename. Generic Emby aliases (`poster.jpg`, `fanart.jpg`, `thumb.jpg`) and `extrafanart/` remain supported inside the same ID directory.

No sidecar may be uploaded to a different top-level directory from its video. Emby precise refresh always targets `/media/gcrypt/<NORMALIZED_ID>`.

## 3. Future upload and promotion flow

The existing hash-suffixed upload directory remains an ingest/staging location, not a final media location.

```text
qBT complete
  -> upload to hash-scoped ingest path
  -> verify ingest manifest
  -> normalize ID and resolve/scrape title
  -> enqueue durable media promotion
  -> move video and associated payload to gcrypt:/<ID>/<canonical label>.*
  -> verify final path/size (and compatible hashes when available)
  -> generate canonical NFO and sidecars
  -> upload and verify all required sidecars in gcrypt:/<ID>/
  -> queue Emby refresh for /media/gcrypt/<ID>
  -> create cleanup_full_torrent job
```

The upload job no longer creates a destructive cleanup child immediately after ingest verification. Cleanup creation moves behind a finalization barrier that requires:

- final canonical video verified;
- the media promotion journal committed;
- required sidecars verified, or an explicit passthrough state for unsupported/unrecognized content;
- a canonical Emby directory recorded.

The barrier is idempotent. Restarts may replay any phase without duplicate remote files, duplicate cleanup jobs, or repeated Emby refresh jobs.

## 4. Durable media promotion

A durable promotion record stores:

- source and destination remote paths;
- expected size and compatible hashes;
- normalized ID, metadata title, canonical basename, and confidence;
- phase (`planned`, `moving`, `verifying`, `verified`, `rollback_wait`, `done`, `failed`);
- attempts, lease, retry time, error, and timestamps.

Promotion uses same-remote `rclone moveto`. Before moving, it verifies that the source exists and that the destination is absent or exactly identical. After moving, it verifies the destination. If verification fails and the source disappeared, it attempts the journaled reverse move. A conflicting destination is never overwritten.

Successful promotion updates the persisted upload manifest to the final path so all later verification, media processing, and cleanup evidence refers to the canonical object.

## 5. Existing remote migration

### 5.1 Scope

The current remote inventory has 351 videos. At the 2026-07-15 audit, 130 are recognized and 216 are low-confidence/unmatched. Automatic migration covers only recognized high-confidence media with a usable Javinizer title. Unmatched, non-JAV, and user-created collection directories remain untouched and appear in a review report.

### 5.2 Plan and execution

Migration is deterministic and resumable:

1. Take one recursive `rclone lsjson` inventory and a read-only Javinizer title snapshot.
2. Generate JSON and CSV plans containing every source, destination, expected size, decision, and reason.
3. Reject plans containing overwrites, conflicting IDs, missing titles, or ambiguous multi-video grouping.
4. Apply in bounded batches with a durable journal.
5. Verify every destination after each move; automatically reverse a failed move when possible.
6. Regenerate and upload the canonical NFO/sidecars.
7. Verify video and required sidecars in the ID directory.
8. Remove only empty hash-wrapper directories; preserve unknown residual files.
9. Refresh the affected Emby ID directories and then request one library-level reconciliation.
10. Re-scan the remote and require the second plan to contain zero eligible moves.

Existing split pairs such as `gcrypt:/BBAN-582/` plus `gcrypt:/BBAN-582.torrent-238df97834d4/` are merged into the pure-ID directory. The video is renamed to the canonical display label and duplicate metadata is resolved by exact size/hash and generation provenance; conflicts are retained and reported rather than deleted.

## 6. NFO regeneration

`javinizer_db_to_sidecar.py` becomes the single NFO title policy implementation. It generates the canonical display title while preserving the original title separately. The backfill normalizer exposes the additional values needed by all callers:

- `metadata_title`
- `display_title`
- `canonical_basename`
- `canonical_remote_dir`

The pure filename parser remains responsible only for ID extraction. Metadata lookup and canonical-name construction are separate, testable units so normalization does not silently depend on network access.

Existing recognized NFO files are regenerated from the Javinizer DB during migration. NFOs without a matching database row are not overwritten.

## 7. Disk-adaptive cleanup policy

### 7.1 Hard gates

Automatic full-torrent cleanup always requires:

- final canonical remote video verification;
- healthy qBT delta synchronization;
- an existing qBT source torrent;
- no `seed-long` or `hold` tag;
- no unresolved promotion or destination conflict.

### 7.2 Release gates

After hard gates pass, cleanup is allowed when any release condition is true:

1. free space is below the cleanup pressure threshold;
2. torrent ratio reaches the configured target;
3. seeding time reaches the configured target;
4. qBT has stopped the torrent after reaching its share limit;
5. completion age reaches the maximum local retention time.

This replaces the current impossible `minimum seed time AND minimum ratio` conjunction. It prevents torrents such as `BBAN-582` (`ratio=3.13`, `seeding_time=0`, `stoppedUP`) from occupying disk forever.

### 7.3 Effective production defaults

Production uses one effective configuration source with environment overrides validated at startup:

```text
cleanup pressure free space: 5 GiB
normal ratio target:          1.0
normal seeding target:        900 seconds
maximum local retention:      7200 seconds
cleanup retry interval:       300 seconds
```

Below 5 GiB, verified canonical media release immediately. Above 5 GiB, the first ratio/time/share-limit/retention condition releases it. This maximizes reusable local disk while preserving optional longer seeding through `seed-long`.

Cleanup jobs are prioritized by reclaimable bytes descending after emergency priority, so the largest verified payload frees space first.

## 8. Live rollout

Rollout is performed in this order:

1. Run focused and full local tests.
2. Commit and push an immutable release.
3. Back up the daemon environment, state database, unit metadata, active release pointer, backfill scripts, and migration reports.
4. Deploy code with cleanup still disabled while schema migration and read-only plan generation run.
5. Migrate and verify `WAAA-614` and `BBAN-582` as canaries.
6. Refresh Emby and verify their displayed titles and media paths.
7. Enable `QBT_ORCH_FULL_CLEANUP=1` and set `QBT_ORCH_FULL_CLEANUP_DRY_RUN=0`.
8. Confirm the canary cleanup jobs finish and local payloads disappear only after remote final verification.
9. Apply the remaining recognized remote migration in bounded batches.
10. Leave the daemon in live, non-dry-run cleanup mode and monitor service restarts, failed jobs, free space, qBT sync health, remote conflicts, and Emby refresh outcomes.
11. Commit the accepted implementation and operational documentation, push the branch to the configured GitHub origin, and verify zero local/origin divergence at the deployed revision.

The qBittorrent container is not restarted. Only the orchestrator daemon is restarted during release activation; active downloads remain managed by qBT.

## 9. Rollback

Code rollback repoints `/opt/emby_qbt_auto/current` to the previous immutable release and restores the prior daemon environment.

Data rollback uses the promotion journal in reverse order. Only entries whose destination still matches the recorded size/hash are eligible for automatic reverse moves. NFO regeneration keeps pre-migration copies in the report bundle until final acceptance. Cleanup cannot be undone after local data deletion, so it is enabled only after canary remote and Emby verification succeeds.

## 10. Acceptance criteria

The change is complete only when all of the following are proven from live state:

1. All newly completed recognized media land in `gcrypt:/<ID>/` with canonical video and NFO basenames.
2. NFO `<title>` is `ID + single space + title`; `<originaltitle>` remains the source title; `<id>/<sorttitle>` remain the ID.
3. No recognized media pipeline run points sidecars or Emby refresh at a directory different from the final video directory.
4. The existing recognized remote inventory is migrated, verified, and produces zero remaining eligible migration moves.
5. Low-confidence/unmatched media are unchanged and listed in the review report.
6. `WAAA-614` and `BBAN-582` exist remotely in canonical form, display correctly in Emby, and their verified local payloads are released.
7. Cleanup is live with `QBT_ORCH_FULL_CLEANUP=1` and `QBT_ORCH_FULL_CLEANUP_DRY_RUN=0`.
8. Cleanup never acts on unverified, conflicted, `hold`, `seed-long`, or sync-unhealthy media.
9. The daemon and qBT container remain healthy, delta sync remains healthy, no cleanup/promotion jobs are unexpectedly stuck, and free space increases by the expected reclaimed bytes.
10. Local, deployed, and GitHub origin revisions match the same immutable release with zero divergence; all temporary staging files are removed, and rollback artifacts remain available.
