# Canonical Media Layout and Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put every recognized movie, canonical sidecar, and Emby refresh under `gcrypt:/<ID>/`, display and name it as `ID + single space + JavDB title`, migrate the recognized existing remote inventory, and run disk-adaptive automatic cleanup live rather than in dry-run.

**Architecture:** Add one pure naming policy, a durable promotion state machine between ingest verification and cleanup, and a resumable remote migration planner that shares the same naming policy. Move destructive cleanup behind final canonical verification and replace the seed-time/ratio conjunction with hard safety gates plus disk-adaptive release conditions.

**Tech Stack:** Python 3.12, pytest, SQLite/WAL, qBittorrent Web API, rclone crypt/Google Drive, Javinizer-Go/JavDB, Emby API, systemd, PowerShell/SSH deployment.

---

## File map

- Create `src/qbt_orchestrator/naming.py`: pure canonical ID/title/path policy.
- Create `src/qbt_orchestrator/promotion.py`: promotion repository, runner, final verification barrier.
- Create `src/qbt_orchestrator/remote_migration.py`: read-only plan, journaled apply, verify, rollback, and audit.
- Create `tools/gdrive_backfill/jav_name_normalize.py`: version-controlled copy of the VPS filename parser with canonical-result enrichment.
- Create `tools/gdrive_backfill/javinizer_db_to_sidecar.py`: version-controlled canonical NFO/sidecar generator.
- Create `tools/gdrive_backfill/repair_emby_layout.py`: thin version-controlled CLI around `remote_migration`.
- Create `tests/test_canonical_naming.py`, `tests/test_media_promotion.py`, `tests/test_remote_media_migration.py`, and `tests/test_backfill_naming_tools.py`.
- Modify `src/qbt_orchestrator/db.py`: promotion table, media-run canonical columns, unique finalization indexes.
- Modify `src/qbt_orchestrator/runtime.py`: ingest verification waits for promotion; cleanup creation occurs only after finalization.
- Modify `src/qbt_orchestrator/media.py`: resolve canonical names, retarget sidecars, enqueue promotion, refresh final directory.
- Modify `src/qbt_orchestrator/integrations/rclone.py`: `moveto`, object stat, exact object verification, rollback-safe errors.
- Modify `src/qbt_orchestrator/integrations/gdrive_backfill.py`: read `media_metadata.json` and expose canonical metadata.
- Modify `src/qbt_orchestrator/cleanup_policy.py`: hard gates and disk-adaptive OR release conditions.
- Modify `src/qbt_orchestrator/service.py` and `src/qbt_orchestrator/cli.py`: promotion worker, cleanup configuration, health/status.
- Modify `tests/fakes.py` and focused existing tests to support promotion/move/stat behavior.
- Modify `deploy/systemd/qbt-orchestrator-daemon.env.example`, `README.md`, and operations docs.

### Task 1: Canonical naming policy

**Files:**
- Create: `src/qbt_orchestrator/naming.py`
- Create: `tests/test_canonical_naming.py`

- [ ] **Step 1: Write failing canonical-name tests**

```python
from qbt_orchestrator.naming import canonical_file_basename, canonical_media_name


def test_canonical_media_name_keeps_id_directory_and_prefixes_title():
    value = canonical_media_name(
        "bban-582",
        "いじられキャラはもっとえっちないじりを期待している",
    )
    assert value.normalized_id == "BBAN-582"
    assert value.metadata_title == "いじられキャラはもっとえっちないじりを期待している"
    assert value.display_title == "BBAN-582 いじられキャラはもっとえっちないじりを期待している"
    assert value.canonical_basename == value.display_title
    assert value.remote_dir("gcrypt:") == "gcrypt:/BBAN-582"


def test_canonical_media_name_sanitizes_only_filesystem_value():
    value = canonical_media_name("ABF-017", '标题 / A:*?"<>|  ') 
    assert value.display_title == 'ABF-017 标题 / A:*?"<>|'
    assert value.canonical_basename == "ABF-017 标题 _ A_"


def test_canonical_media_name_never_truncates_id_prefix():
    value = canonical_media_name("FC2-PPV-4684796", "名" * 300, max_basename_chars=40)
    assert value.canonical_basename.startswith("FC2-PPV-4684796 ")
    assert len(value.canonical_basename) == 40


def test_canonical_file_basename_preserves_multi_part_suffix():
    value = canonical_media_name("BBAN-582", "影片名称")
    assert canonical_file_basename(value, "raw-name-CD2.mp4") == "BBAN-582 影片名称-CD2"
    assert canonical_file_basename(value, "raw-name.mp4", collision_digest="a1b2c3d4") == "BBAN-582 影片名称-a1b2c3d4"
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```powershell
python -m pytest tests/test_canonical_naming.py -q
```

Expected: collection fails with `ModuleNotFoundError: qbt_orchestrator.naming`.

- [ ] **Step 3: Implement the pure naming policy**

```python
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

_INVALID = re.compile(r'[\\/:*?"<>|\x00-\x1f]+')
_SPACE = re.compile(r"\s+")
_PART = re.compile(r"(?i)(?:[._ -]?((?:cd|disc|disk|part|pt)[._ -]?[0-9]{1,2}|上|下|前編|後編))$")


@dataclass(frozen=True)
class CanonicalMediaName:
    normalized_id: str
    metadata_title: str
    display_title: str
    canonical_basename: str

    def remote_dir(self, remote: str = "gcrypt:") -> str:
        return f"{remote.rstrip('/')}/{self.normalized_id}"


def canonical_media_name(normalized_id: str, metadata_title: str, *, max_basename_chars: int = 120) -> CanonicalMediaName:
    media_id = _SPACE.sub("-", unicodedata.normalize("NFKC", normalized_id).strip()).upper()
    title = _SPACE.sub(" ", unicodedata.normalize("NFKC", metadata_title).strip()) or media_id
    display = f"{media_id} {title}" if title != media_id else media_id
    safe = _SPACE.sub(" ", _INVALID.sub("_", display)).strip(" .")
    limit = max(len(media_id) + 1, int(max_basename_chars))
    safe = safe[:limit].rstrip(" .")
    return CanonicalMediaName(media_id, title, display, safe)


def canonical_file_basename(name: CanonicalMediaName, source_filename: str, *, collision_digest: str = "") -> str:
    stem = source_filename.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    match = _PART.search(stem)
    suffix = re.sub(r"[._ ]+", "", match.group(1)).upper() if match else ""
    if collision_digest:
        suffix = collision_digest.lower()[:8]
    return f"{name.canonical_basename}-{suffix}" if suffix else name.canonical_basename
```

- [ ] **Step 4: Run focused tests and verify GREEN**

Run `python -m pytest tests/test_canonical_naming.py -q`.

Expected: `4 passed`.

- [ ] **Step 5: Commit**

```powershell
git add src/qbt_orchestrator/naming.py tests/test_canonical_naming.py
git commit -m "feat: define canonical media naming"
```

### Task 2: Version-controlled NFO and filename tools

**Files:**
- Create: `tools/gdrive_backfill/jav_name_normalize.py`
- Create: `tools/gdrive_backfill/javinizer_db_to_sidecar.py`
- Create: `tests/test_backfill_naming_tools.py`

- [ ] **Step 1: Copy the currently served VPS scripts as the reviewable baseline**

Run:

```powershell
scp root@ssh.paff-67.top:/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py tools/gdrive_backfill/jav_name_normalize.py
scp root@ssh.paff-67.top:/opt/qbt/gdrive-backfill/bin/javinizer_db_to_sidecar.py tools/gdrive_backfill/javinizer_db_to_sidecar.py
```

Expected: both files exist locally and `git diff --no-index` against a fresh `scp` copy is empty.

- [ ] **Step 2: Write failing tests for enriched normalization and canonical NFO**

```python
def test_normalizer_enriches_result_with_title(tool_module):
    row = tool_module.enrich_with_title(tool_module.normalize("489155.com@BBAN-582.mp4"), "影片名称")
    assert row["normalized_id"] == "BBAN-582"
    assert row["metadata_title"] == "影片名称"
    assert row["display_title"] == "BBAN-582 影片名称"
    assert row["canonical_basename"] == "BBAN-582 影片名称"
    assert row["canonical_remote_dir"] == "gcrypt:/BBAN-582"


def test_nfo_title_is_id_plus_title(nfo_module, movie_row):
    xml = nfo_module.render_nfo(movie_row, [], [])
    assert "<title>BBAN-582 影片名称</title>" in xml
    assert "<originaltitle>影片名称</originaltitle>" in xml
    assert "<sorttitle>BBAN-582</sorttitle>" in xml
    assert "<id>BBAN-582</id>" in xml
```

- [ ] **Step 3: Run and verify RED**

Run `python -m pytest tests/test_backfill_naming_tools.py -q`.

Expected: failures show missing `enrich_with_title` and an unprefixed `<title>`.

- [ ] **Step 4: Add canonical output to both tools**

Implement `enrich_with_title(result, title, remote="gcrypt:")` by calling `canonical_media_name`. In `render_nfo`, replace the title assignment with:

Both standalone tools import the one policy from the active immutable release, with a test-only/local checkout fallback:

```python
current = Path(os.environ.get("QBT_ORCH_CURRENT", "/opt/emby_qbt_auto/current"))
source_root = current / "src"
if source_root.exists() and str(source_root) not in sys.path:
    sys.path.insert(0, str(source_root))
from qbt_orchestrator.naming import canonical_media_name
```

```python
name = canonical_media_name(content_id, source_title)
title = name.display_title
original_title = source_title
```

In `write_sidecar_from_db`, write NFO/poster/fanart using `name.canonical_basename`, and create `media_metadata.json` containing:

```python
metadata = {
    "normalized_id": name.normalized_id,
    "metadata_title": name.metadata_title,
    "display_title": name.display_title,
    "canonical_basename": name.canonical_basename,
    "canonical_remote_dir": name.remote_dir("gcrypt:"),
}
```

- [ ] **Step 5: Run tests and commit**

Run `python -m pytest tests/test_canonical_naming.py tests/test_backfill_naming_tools.py -q`.

Expected: all focused tests pass.

```powershell
git add tools/gdrive_backfill tests/test_backfill_naming_tools.py
git commit -m "feat: generate canonical jav sidecars"
```

### Task 3: Persist durable media promotions

**Files:**
- Modify: `src/qbt_orchestrator/db.py`
- Create: `src/qbt_orchestrator/promotion.py`
- Create: `tests/test_media_promotion.py`

- [ ] **Step 1: Write failing migration/repository tests**

```python
def test_media_promotion_schema_and_idempotent_enqueue(db):
    repo = MediaPromotionRepository(db, now=lambda: 1000)
    first = repo.enqueue(upload_job_id=7, hash="abc", media_group_id=3,
        normalized_id="BBAN-582", metadata_title="影片名称",
        display_title="BBAN-582 影片名称",
        source_remote="gcrypt:/ingest/raw.mp4",
        target_remote="gcrypt:/BBAN-582/BBAN-582 影片名称.mp4", expected_size=123)
    second = repo.enqueue(upload_job_id=7, hash="abc", media_group_id=3,
        normalized_id="BBAN-582", metadata_title="影片名称",
        display_title="BBAN-582 影片名称",
        source_remote="gcrypt:/ingest/raw.mp4",
        target_remote="gcrypt:/BBAN-582/BBAN-582 影片名称.mp4", expected_size=123)
    assert first == second
    assert repo.get(first)["state"] == "planned"
```

- [ ] **Step 2: Run and verify RED**

Run `python -m pytest tests/test_media_promotion.py::test_media_promotion_schema_and_idempotent_enqueue -q`.

Expected: import/schema failure because the repository/table does not exist.

- [ ] **Step 3: Add the migration and repository**

Add a migration creating `media_promotions` with the exact columns from the design: IDs, source/target, names, expected size, state, verification, attempts/leases/retry/error/timestamps, and a unique index on `(upload_job_id,source_remote,target_remote)`. Add canonical columns to `media_pipeline_runs`.

```sql
create table if not exists media_promotions(
  id integer primary key autoincrement,
  upload_job_id integer not null,
  hash text,
  media_group_id integer,
  normalized_id text not null,
  metadata_title text not null,
  display_title text not null,
  source_remote text not null,
  target_remote text not null,
  expected_size integer not null,
  expected_hashes_json text not null default '{}',
  state text not null default 'planned',
  verification_method text,
  verification_result_json text,
  attempts integer not null default 0,
  max_attempts integer not null default 6,
  lease_owner text,
  lease_until integer,
  next_run_at integer,
  last_error text,
  created_at integer not null,
  updated_at integer not null,
  verified_at integer
);
create unique index if not exists idx_media_promotions_identity
  on media_promotions(upload_job_id,source_remote,target_remote);
create index if not exists idx_media_promotions_claim
  on media_promotions(state,next_run_at,id);
```

Implement repository methods `enqueue`, `claim_next`, `record_verified`, `schedule_retry`, `record_failed`, `get`, and `pending_for_upload`. All writes use `write_transaction`; claims require `attempts < max_attempts` and reclaim expired leases only through maintenance.

- [ ] **Step 4: Run repository tests and commit**

Run `python -m pytest tests/test_media_promotion.py -q`.

Expected: repository tests pass.

```powershell
git add src/qbt_orchestrator/db.py src/qbt_orchestrator/promotion.py tests/test_media_promotion.py
git commit -m "feat: persist remote media promotions"
```

### Task 4: Add rollback-safe rclone promotion primitives

**Files:**
- Modify: `src/qbt_orchestrator/integrations/rclone.py`
- Modify: `tests/fakes.py`
- Modify: `tests/test_media_promotion.py`

- [ ] **Step 1: Write failing move/stat/rollback tests**

```python
def test_promotion_moves_and_verifies_exact_destination(db):
    rclone = FakeRclone(remote_sizes={"gcrypt:/staging/raw.mp4": 123})
    runner = MediaPromotionRunner(repo, rclone)
    job_id = enqueue_promotion(repo, expected_size=123)
    assert runner.run_next() == job_id
    assert rclone.movetos == [("gcrypt:/staging/raw.mp4", "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4")]
    assert repo.get(job_id)["state"] == "verified"


def test_promotion_reverses_move_when_destination_verification_fails(db):
    rclone = FakeRclone(remote_sizes={"gcrypt:/staging/raw.mp4": 123}, moved_size=122)
    runner = MediaPromotionRunner(repo, rclone)
    job_id = enqueue_promotion(repo, expected_size=123)
    runner.run_next()
    assert rclone.movetos[-1] == ("gcrypt:/BBAN-582/BBAN-582 影片名称.mp4", "gcrypt:/staging/raw.mp4")
    assert repo.get(job_id)["state"] == "retry_wait"
```

- [ ] **Step 2: Run and verify RED**

Run the two tests; expect missing `moveto`/`stat` behavior.

- [ ] **Step 3: Implement `RcloneClient.stat`, `moveto`, and promotion runner**

`stat(remote)` uses `rclone lsjson --stat`; `moveto(source,target)` runs same-remote `rclone moveto`. `MediaPromotionRunner` checks source size, rejects a conflicting target, accepts an already-identical target idempotently, moves, verifies, and reverse-moves on verification failure.

```python
def stat(self, remote: str) -> dict | None:
    rc, out, err = self.runner(self._base() + ["lsjson", "--stat", remote], None, 300)
    if rc != 0:
        if "not found" in err.lower():
            return None
        raise RuntimeError(f"rclone stat failed rc={rc}: {err[-400:]}")
    row = json.loads(out or "null")
    return dict(row) if isinstance(row, dict) else None

def moveto(self, source: str, target: str) -> None:
    rc, _out, err = self.runner(self._base() + ["moveto", source, target], None, self.timeout)
    if rc != 0:
        raise RuntimeError(f"rclone moveto failed rc={rc}: {err[-400:]}")
```

Runner transition table:

```text
source exact + target absent       -> moveto -> target exact -> verified
source absent + target exact       -> verified (idempotent replay)
source exact + target exact        -> conflict unless hashes/size prove identity
source exact + target different    -> failed:target_conflict, never overwrite
move done + target verification bad -> reverse moveto -> retry_wait
```

- [ ] **Step 4: Run focused tests and commit**

Run `python -m pytest tests/test_media_promotion.py tests/test_new_system_behaviors.py -q`.

Expected: all pass.

```powershell
git add src/qbt_orchestrator/integrations/rclone.py src/qbt_orchestrator/promotion.py tests/fakes.py tests/test_media_promotion.py
git commit -m "feat: promote remote media safely"
```

### Task 5: Make ingest verification wait for canonical finalization

**Files:**
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `tests/test_runtime_repositories.py`
- Modify: `tests/test_daemon_runtime.py`

- [ ] **Step 1: Write failing barrier tests**

```python
def test_full_upload_verification_waits_for_promotion_without_cleanup(db):
    repo = TorrentJobRepository(db, now=lambda: 1000)
    upload_id = repo.enqueue("h", None, "upload", full_upload_payload(), priority=10)
    row = repo.get(upload_id)
    state = repo.finalize_verified(row, full_upload_payload(), verified_result())
    assert state == "promotion_wait"
    assert rows(db, "select * from torrent_jobs where job_type='cleanup_full_torrent'") == []


def test_finalization_barrier_creates_one_cleanup_after_promotion_and_sidecars(db):
    mark_upload_promotion_and_required_sidecars_verified(db, upload_id=7)
    assert finalize_canonical_upload(db, upload_id=7) is True
    assert finalize_canonical_upload(db, upload_id=7) is False
    cleanup = rows(db, "select * from torrent_jobs where job_type='cleanup_full_torrent'")
    assert len(cleanup) == 1
    assert json.loads(cleanup[0]["payload_json"])["canonical_remote_verified"] is True
```

- [ ] **Step 2: Run and verify RED**

Run both tests. Expected: current state is `cleanup_wait` and a cleanup child already exists.

- [ ] **Step 3: Move cleanup creation behind `finalize_canonical_upload`**

Change full upload verification to `state='promotion_wait', phase='promotion_wait'`. Keep media pipeline enqueue. Add one transactional finalizer that checks every promotion is verified, required sidecars are verified or passthrough is explicit, persists the final manifest, creates one cleanup child, and changes the parent to `cleanup_wait`.

```python
def finalize_canonical_upload(state_db, upload_job_id: int, now: int) -> bool:
    def txn(con):
        upload = con.execute("select * from torrent_jobs where id=? and job_type='upload'", (upload_job_id,)).fetchone()
        promotions = con.execute("select * from media_promotions where upload_job_id=?", (upload_job_id,)).fetchall()
        run = con.execute("select * from media_pipeline_runs where upload_manifest_id=?", (f"upload-job-{upload_job_id}",)).fetchone()
        if not upload or not promotions or any(row["state"] != "verified" for row in promotions):
            return False
        if not run or run["state"] not in {"SidecarVerified", "PassthroughAllowed"}:
            return False
        if not run["canonical_remote_dir"] or not run["canonical_video_manifest_json"]:
            return False
        payload = json.loads(upload["payload_json"] or "{}")
        cleanup_payload = {
            "upload_job_id": upload_job_id,
            "hash": upload["hash"],
            "remote": run["canonical_remote_dir"],
            "canonical_remote_verified": True,
            "final_manifest": json.loads(run["canonical_video_manifest_json"]),
            "cleanup_policy_snapshot": payload.get("cleanup_policy_snapshot", {}),
        }
        con.execute(
            "insert or ignore into torrent_jobs(hash,batch_id,job_type,state,priority,payload_json,parent_job_id,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
            (upload["hash"], upload["batch_id"], "cleanup_full_torrent", "queued", 10,
             json.dumps(cleanup_payload, ensure_ascii=False), upload_job_id, now, now),
        )
        changed = con.execute(
            "update torrent_jobs set state='cleanup_wait',phase='cleanup_wait',updated_at=? where id=? and state='promotion_wait'",
            (now, upload_job_id),
        ).rowcount
        return bool(changed)
    return bool(write_transaction(state_db, txn))
```

No qBT or rclone call occurs inside that transaction.

- [ ] **Step 4: Run tests and commit**

Run `python -m pytest tests/test_runtime_repositories.py tests/test_daemon_runtime.py -q`.

Expected: all pass.

```powershell
git add src/qbt_orchestrator/runtime.py tests/test_runtime_repositories.py tests/test_daemon_runtime.py
git commit -m "fix: gate cleanup on canonical verification"
```

### Task 6: Canonicalize the media pipeline and sidecar destinations

**Files:**
- Modify: `src/qbt_orchestrator/media.py`
- Modify: `src/qbt_orchestrator/integrations/gdrive_backfill.py`
- Modify: `tests/test_media_pipeline_persistence.py`
- Modify: `tests/test_gdrive_backfill_live_adapter.py`

- [ ] **Step 1: Write failing end-to-end layout tests**

```python
def test_verified_ingest_enqueues_canonical_promotion_and_colocated_sidecars(service, db):
    result = service.handle_upload_verified("upload-job-7", [UploadedFile(
        "gcrypt:/WAAA-614-8cfce204ec0e/489155.com@WAAA-614.mp4", 5_542_877_598)])
    assert result.media_group_key == "WAAA-614"
    promotion = rows(db, "select * from media_promotions")[0]
    assert promotion["target_remote"] == "gcrypt:/WAAA-614/WAAA-614 影片名称.mp4"
    artifacts = queued_sidecar_payloads(db)
    assert all(x["remote"].startswith("gcrypt:/WAAA-614/WAAA-614 影片名称") for x in artifacts)
    assert rows(db, "select * from emby_refresh_tasks") == []
```

- [ ] **Step 2: Run and verify RED**

Expected: no promotion row; sidecars currently target pure-ID basenames and refresh may be queued before promotion.

- [ ] **Step 3: Parse `media_metadata.json` and retarget the pipeline**

`GDriveBackfillScraper` reads the local metadata JSON and includes its canonical fields in the result. `MediaPipelineService` validates ID/title/confidence, creates the media group at `/media/gcrypt/<ID>`, retargets local artifacts to the canonical basename, enqueues promotions, and withholds refresh until promotion plus sidecars pass the finalizer.

```python
metadata_path = work_dir / "media_metadata.json"
metadata = json.loads(metadata_path.read_text("utf-8")) if metadata_path.exists() else {}
out.update({key: metadata.get(key) for key in (
    "normalized_id", "metadata_title", "display_title",
    "canonical_basename", "canonical_remote_dir",
)})
```

For every uploaded media file, the target is constructed as:

```python
suffix = PurePosixPath(primary.remote_path).suffix.lower() or ".mp4"
target_remote = f"{canonical.remote_dir(self.remote)}/{canonical.canonical_basename}{suffix}"
```

Sidecar artifacts are mapped to the same remote directory and canonical basename before enqueue; generic aliases keep `poster.jpg`, `fanart.jpg`, and `thumb.jpg`.

- [ ] **Step 4: Run tests and commit**

Run:

```powershell
python -m pytest tests/test_media_pipeline_persistence.py tests/test_gdrive_backfill_live_adapter.py tests/test_media_promotion.py -q
```

Expected: all pass.

```powershell
git add src/qbt_orchestrator/media.py src/qbt_orchestrator/integrations/gdrive_backfill.py tests/test_media_pipeline_persistence.py tests/test_gdrive_backfill_live_adapter.py
git commit -m "fix: colocate videos sidecars and emby paths"
```

### Task 7: Wire the promotion worker and observability

**Files:**
- Modify: `src/qbt_orchestrator/service.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Modify: `src/qbt_orchestrator/observability.py`
- Modify: `tests/test_daemon_runtime.py`
- Modify: `tests/test_cli_observability.py`

- [ ] **Step 1: Write failing daemon/CLI tests**

```python
def test_background_workers_include_live_promotion(runtime):
    assert "promotion" in dict(runtime._background_event_worker_specs())


def test_status_queue_reports_promotion_backlog(cli_db):
    seed_promotion(cli_db, state="retry_wait")
    status = json.loads(run_cli(["status", "queue", "--state-db", str(cli_db), "--json"])[1])
    assert status["promotions_by_state"]["retry_wait"] == 1
```

- [ ] **Step 2: Run and verify RED**

Expected: promotion worker/status key absent.

- [ ] **Step 3: Construct runner and add the event worker**

Wire one `MediaPromotionRunner`, add `process_media_promotions(max_jobs=1)`, record move/verify/rollback actions, and expose promotion counts/oldest age. The worker runs only in live mode and stops claiming when qBT sync is unhealthy.

```python
def _background_event_worker_specs(self):
    return [
        ("telegram", self.process_bot_notifications),
        ("upload", self.process_upload_jobs),
        ("promotion", self.process_media_promotions),
        ("cleanup", self.process_cleanup_requests),
        ("full_cleanup", self.process_full_cleanup_jobs),
        ("media_pipeline", self.process_media_pipeline_jobs),
        ("emby", self.process_emby_refresh_tasks),
    ]
```

The promotion callback runs canonical finalization and queues the precise Emby refresh only after the last required sidecar upload becomes verified.

- [ ] **Step 4: Run tests and commit**

Run `python -m pytest tests/test_daemon_runtime.py tests/test_cli_observability.py -q`.

Expected: all pass.

```powershell
git add src/qbt_orchestrator/service.py src/qbt_orchestrator/cli.py src/qbt_orchestrator/observability.py tests/test_daemon_runtime.py tests/test_cli_observability.py
git commit -m "feat: run and observe media promotions"
```

### Task 8: Implement disk-adaptive cleanup

**Files:**
- Modify: `src/qbt_orchestrator/cleanup_policy.py`
- Modify: `src/qbt_orchestrator/runtime.py`
- Modify: `src/qbt_orchestrator/cli.py`
- Modify: `tests/test_runtime_repositories.py`
- Modify: `tests/test_daemon_runtime.py`

- [ ] **Step 1: Write failing policy tests**

```python
def test_cleanup_releases_verified_media_under_disk_pressure():
    d = cleanup_eligibility(torrent(), canonical_remote_verified=True,
        free_bytes=4 * GiB, pressure_free_bytes=5 * GiB,
        min_seed_sec=900, min_ratio=1.0, max_retention_sec=7200, now=10_000)
    assert (d.allowed, d.reason) == (True, "disk_pressure")


def test_cleanup_uses_ratio_or_time_instead_of_and():
    d = cleanup_eligibility(torrent(ratio=3.13, seeding_time=0, state="stoppedUP"),
        canonical_remote_verified=True, free_bytes=10 * GiB, pressure_free_bytes=5 * GiB,
        min_seed_sec=900, min_ratio=1.0, max_retention_sec=7200, now=10_000)
    assert (d.allowed, d.reason) == (True, "ratio")


def test_cleanup_hard_gates_hold_seed_long_conflict_and_unverified():
    for tags, verified, conflict in [("hold", True, False), ("seed-long", True, False), ("auto", False, False), ("auto", True, True)]:
        assert cleanup_eligibility(torrent(tags=tags), canonical_remote_verified=verified,
            promotion_conflict=conflict, free_bytes=0, pressure_free_bytes=5 * GiB,
            min_seed_sec=0, min_ratio=0, max_retention_sec=0).allowed is False
```

- [ ] **Step 2: Run and verify RED**

Expected: signature mismatch and current `seed_time` block for the ratio-only case.

- [ ] **Step 3: Implement hard gates plus release-condition OR**

Add canonical verification, hold, conflict, free-space, completion age, and qBT state inputs. Return stable reasons: `remote_not_canonical`, `hold`, `seed_long`, `promotion_conflict`, `disk_pressure`, `ratio`, `seed_time`, `share_limit`, `retention`, or `policy_wait`.

```python
if not canonical_remote_verified:
    return CleanupEligibility(False, "remote_not_canonical", None)
if {"hold", "seed-long"} & _tags(torrent):
    return CleanupEligibility(False, "hold" if "hold" in _tags(torrent) else "seed_long", None)
if promotion_conflict:
    return CleanupEligibility(False, "promotion_conflict", None)
if free_bytes < pressure_free_bytes:
    return CleanupEligibility(True, "disk_pressure", None)
if float(torrent.get("ratio") or 0) >= min_ratio:
    return CleanupEligibility(True, "ratio", None)
if int(torrent.get("seeding_time") or 0) >= min_seed_sec:
    return CleanupEligibility(True, "seed_time", None)
if str(torrent.get("state") or "") == "stoppedUP" and bool(torrent.get("share_limit_reached")):
    return CleanupEligibility(True, "share_limit", None)
if completion_age_sec >= max_retention_sec:
    return CleanupEligibility(True, "retention", None)
return CleanupEligibility(False, "policy_wait", observed_at + 300)
```

Pass live free space into `FullTorrentCleanupRunner`; order queued cleanup jobs by pressure priority and reclaimable bytes descending. Load `QBT_ORCH_CLEANUP_PRESSURE_FREE_GB=5`, `QBT_ORCH_CLEANUP_MIN_SEED_SEC=900`, `QBT_ORCH_CLEANUP_MIN_RATIO=1.0`, and `QBT_ORCH_CLEANUP_MAX_RETENTION_SEC=7200`, with environment overrides taking precedence and a startup event recording the effective values.

- [ ] **Step 4: Run focused tests and commit**

Run `python -m pytest tests/test_runtime_repositories.py tests/test_daemon_runtime.py tests/test_production_invariants.py -q`.

Expected: all pass and no automatic delete occurs without canonical verification.

```powershell
git add src/qbt_orchestrator/cleanup_policy.py src/qbt_orchestrator/runtime.py src/qbt_orchestrator/cli.py tests/test_runtime_repositories.py tests/test_daemon_runtime.py
git commit -m "fix: release verified media under disk pressure"
```

### Task 9: Build the idempotent existing-remote migration CLI

**Files:**
- Create: `src/qbt_orchestrator/remote_migration.py`
- Create: `tools/gdrive_backfill/repair_emby_layout.py`
- Create: `tests/test_remote_media_migration.py`

- [ ] **Step 1: Write failing plan/apply/audit tests**

```python
def test_migration_merges_hash_wrapper_into_existing_id_directory(inventory, titles):
    plan = build_migration_plan(inventory, titles, min_confidence=0.95)
    move = next(x for x in plan.actions if x.kind == "video")
    assert move.source == "gcrypt:/BBAN-582.torrent-238df97834d4/489155.com@BBAN-582.mp4"
    assert move.target == "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4"


def test_migration_preserves_unmatched_and_conflicting_objects(inventory, titles):
    plan = build_migration_plan(inventory, titles, min_confidence=0.95)
    assert plan.review[0].reason in {"low_confidence", "missing_title", "target_conflict"}
    assert not any(x.source.endswith("unknown.mp4") for x in plan.actions)


def test_second_plan_is_empty_after_apply_and_verify(fake_remote, titles):
    first = build_migration_plan(fake_remote.inventory(), titles, min_confidence=0.95)
    apply_migration(first, fake_remote, journal_path=fake_remote.journal)
    second = build_migration_plan(fake_remote.inventory(), titles, min_confidence=0.95)
    assert second.actions == []
```

- [ ] **Step 2: Run and verify RED**

Expected: missing migration module.

- [ ] **Step 3: Implement deterministic plan/journal/apply/rollback/audit**

Use one inventory snapshot, one read-only title snapshot, and the shared naming policy. Refuse overwrites. Persist every transition to JSONL before and after `moveto`. Verify size/hash after each move and reverse failed moves. Generate JSON, CSV, review, and summary files. Add `plan`, `apply --batch-size`, `rollback --journal`, and `audit` subcommands.

Action records use this stable schema:

```json
{
  "action_id": "sha256(source,target)",
  "kind": "video|nfo|poster|fanart|thumb|subtitle|extrafanart",
  "normalized_id": "BBAN-582",
  "source": "gcrypt:/wrapper/raw.mp4",
  "target": "gcrypt:/BBAN-582/BBAN-582 影片名称.mp4",
  "expected_size": 6334240229,
  "expected_hashes": {},
  "state": "planned|moving|verified|rollback_wait|rolled_back|failed",
  "reason": "canonicalize"
}
```

NFO replacement first copies the old NFO into the timestamped report bundle, renders a new local NFO from the title snapshot, uploads it with `copyto`, and verifies its exact destination size. Existing pending upload/cleanup jobs are reconciled only after the migrated video and new NFO are verified: their payload receives `canonical_remote_verified=true`, the final manifest, and canonical directory; no unrelated job is mutated.

- [ ] **Step 4: Run tests and commit**

Run `python -m pytest tests/test_remote_media_migration.py -q`.

Expected: all pass, including empty second plan.

```powershell
git add src/qbt_orchestrator/remote_migration.py tools/gdrive_backfill/repair_emby_layout.py tests/test_remote_media_migration.py
git commit -m "feat: migrate remote media to canonical layout"
```

### Task 10: Documentation, env defaults, and deployment assets

**Files:**
- Modify: `deploy/systemd/qbt-orchestrator-daemon.env.example`
- Modify: `README.md`
- Create: `docs/operations/canonical-media-migration.md`
- Modify: `docs/traceability/requirements-map.md`

- [ ] **Step 1: Write the exact live/rollback procedure**

Document backup commands, release install, read-only plan, canary batch, destination verification, Emby verification, cleanup enablement, full migration batching, rollback journal, and final audit. Include these final live values:

```text
QBT_ORCH_FULL_CLEANUP=1
QBT_ORCH_FULL_CLEANUP_DRY_RUN=0
QBT_ORCH_CLEANUP_PRESSURE_FREE_GB=5
QBT_ORCH_CLEANUP_MIN_SEED_SEC=900
QBT_ORCH_CLEANUP_MIN_RATIO=1.0
QBT_ORCH_CLEANUP_MAX_RETENTION_SEC=7200
```

- [ ] **Step 2: Verify docs and commit**

Run:

```powershell
rg -n "FULL_CLEANUP|canonical|promotion|rollback|GitHub" README.md docs deploy/systemd/qbt-orchestrator-daemon.env.example
git diff --check
```

Expected: every rollout and rollback control is discoverable; no whitespace errors.

```powershell
git add README.md docs deploy/systemd/qbt-orchestrator-daemon.env.example
git commit -m "docs: add canonical media live rollout"
```

### Task 11: Full local verification and immutable release

**Files:** all changed files.

- [ ] **Step 1: Run the complete test suite**

Run:

```powershell
python -m pytest -q
```

Expected: zero failures; any warning must be identified as pre-existing or fixed before deployment.

- [ ] **Step 2: Run static repository checks**

```powershell
python -m compileall -q src tools
git diff --check
git status --short
```

Expected: compile and diff checks return 0; status contains only intentional changes.

- [ ] **Step 3: Commit remaining integration adjustments and push GitHub**

```powershell
git add -A
git commit -m "feat: canonicalize media and enable adaptive cleanup"
git push origin codex/orchestrator-v3-hardening
git rev-list --left-right --count HEAD...origin/codex/orchestrator-v3-hardening
```

Expected: divergence is `0 0`.

### Task 12: Deploy, migrate canaries, and enable non-dry-run cleanup

**Files/services affected:**
- `/opt/emby_qbt_auto/releases/<revision>` and `/opt/emby_qbt_auto/current`
- `/etc/qbt-orchestrator/daemon.env`
- `/var/lib/qbt-orchestrator/state.sqlite`
- `/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py`
- `/opt/qbt/gdrive-backfill/bin/javinizer_db_to_sidecar.py`
- `/opt/qbt/gdrive-backfill/bin/repair_emby_layout.py`
- `qbt-orchestrator-daemon.service`

- [ ] **Step 1: Announce and take backups before root writes**

Record purpose, exact paths, expected daemon-only restart, rollback release, and that qBittorrent itself will not restart. Back up the env, SQLite DB including WAL/SHM through SQLite backup API, live scripts, current release pointer, and remote plan reports.

- [ ] **Step 2: Install the immutable release with cleanup still disabled**

Deploy the exact Git revision, update script symlinks/copies atomically, run migrations, point `current` to the new release, and restart only `qbt-orchestrator-daemon.service`.

Expected: service active, qBT container PID unchanged, delta sync healthy, no unexpected jobs claimed.

- [ ] **Step 3: Generate and inspect the live migration plan**

Run the new CLI `plan` command against `gcrypt:` and Javinizer DB. Require no overwrites/conflicts in the two canaries and preserve the complete reports under `/var/lib/qbt-orchestrator/reports/`.

- [ ] **Step 4: Apply `WAAA-614` and `BBAN-582` canaries**

Apply only those IDs, verify canonical video and NFO sizes, inspect NFO title/originaltitle/id/sorttitle, confirm hash wrapper residuals contain no unknown files, and refresh `/media/gcrypt/WAAA-614` and `/media/gcrypt/BBAN-582`.

Expected canonical video paths:

```text
gcrypt:/WAAA-614/WAAA-614 <JavDB title>.mp4
gcrypt:/BBAN-582/BBAN-582 <JavDB title>.mp4
```

- [ ] **Step 5: Enable live cleanup**

Atomically edit the backed-up env to the six production values from Task 10 and restart only the orchestrator daemon.

Expected: jobs `108` and `114` or their reconciled replacements reach `done`; qBT removes the two torrents with `deleteFiles=true`; the exact verified local payloads disappear; free space increases by approximately 11.1 GiB.

- [ ] **Step 6: Apply remaining recognized migration in bounded batches**

Run batches, stop on any failed/rollback-wait/conflict result, verify each batch, and re-run plan after completion.

Expected: the second plan has zero eligible moves; all low-confidence/unmatched items remain unchanged in the review report.

- [ ] **Step 7: Monitor live operation**

Monitor service restarts, qBT sync metrics, promotion/cleanup backlog, free space, remote verification errors, and Emby refresh results long enough to cover one upload/promotion/cleanup cycle. Do not leave the service in dry-run.

### Task 13: Final completion audit and GitHub/deployment convergence

- [ ] **Step 1: Verify every acceptance criterion from the design spec**

Audit canonical paths and NFO fields, zero eligible migration moves, unchanged review items, canary local cleanup, live flags, hard-gate negative cases, daemon/qBT health, delta ratio, no stuck jobs, expected free-space gain, and rollback artifacts.

- [ ] **Step 2: Verify revision convergence and repository cleanliness**

Run:

```powershell
git status --short
git rev-parse HEAD
git rev-parse origin/codex/orchestrator-v3-hardening
git rev-list --left-right --count HEAD...origin/codex/orchestrator-v3-hardening
ssh root@ssh.paff-67.top 'cat /opt/emby_qbt_auto/current/REVISION'
```

Expected: clean status, identical local/origin/deployed revision, divergence `0 0`.

- [ ] **Step 3: Remove temporary deployment staging and retain only named rollback artifacts**

Verify no tarballs, transient scripts, or untracked migration scratch files remain. Keep immutable prior releases, timestamped env/DB/script backups, and signed migration reports.

- [ ] **Step 4: Record final evidence**

Summarize exact commits, tests, deployed revision, migration counts, review counts, canary paths/titles, cleanup job outcomes, reclaimed bytes, live flags, health metrics, and GitHub convergence without exposing credentials.
