#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


def _rows(db: Path, sql: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(sql)]
    con.close()
    return rows


def _job_count(db: Path, job_type: str) -> int:
    return int(_rows(db, f"select count(*) as count from torrent_jobs where job_type='{job_type}'")[0]["count"])


def test_observability_store_persists_redacted_events_actions_and_trace():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import ObservabilityStore

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        obs = ObservabilityStore(db)
        obs.event("warning", "telegram", "bot_rejected", "token " + "123456:" + "secret-token", {"magnet": "mag" + "net:?xt=urn:btih:" + "A" * 40}, hash="h1")
        obs.action(hash="h1", job_id=7, action_type="qbt_post", path="/api/v2/torrents/stop", payload={"hashes": "h1"}, status="succeeded", dry_run=False)
        trace = obs.trace("h1")
        dumped = json.dumps(trace)
        assert "secret-token" not in dumped
        assert "magnet:?" not in dumped
        assert trace["actions"][0]["path"] == "/api/v2/torrents/stop"
        assert trace["events"][0]["event_type"] == "bot_rejected"


def test_upload_job_runner_claims_job_updates_promotion_wait_and_verify_pending():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db)
        good = repo.enqueue("h1", 1, "upload", {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True}, priority=1)
        bad = repo.enqueue("h2", 2, "upload", {"local": "/tmp/b.mp4", "remote": "gcrypt:/B/b.mp4", "size": 100, "full_torrent": True}, priority=2)

        runner = UploadJobRunner(repo, FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/A/a.mp4": 100, "gcrypt:/B/b.mp4": 99}), FakeExecutor())
        assert runner.run_next() == good
        assert repo.get(good)["state"] == "promotion_wait"
        assert _job_count(db, "cleanup_full_torrent") == 0
        assert runner.run_next() == bad
        assert repo.get(bad)["state"] == "verify_pending"
        assert repo.get(bad)["last_stderr_tail"] == "remote size mismatch"


def test_upload_heartbeat_renews_lease_and_success_clears_retry_diagnostics():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor

    class SlowRclone:
        def copyto(self, local, remote):
            time.sleep(0.06)
            return True

        def lsjson_size(self, remote):
            return 100

    class CountingRepo(TorrentJobRepository):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.renew_count = 0

        def renew_lease(self, *args, **kwargs):
            renewed = super().renew_lease(*args, **kwargs)
            if renewed:
                self.renew_count += 1
            return renewed

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = CountingRepo(db, now=lambda: 100, lease_duration_sec=2)
        job_id = repo.enqueue(
            "h1",
            None,
            "upload",
            {
                "local": "/tmp/a.mp4",
                "remote": "gcrypt:/A/a.mp4",
                "size": 100,
                "full_torrent": False,
            },
            priority=1,
        )
        con = sqlite3.connect(db)
        con.execute(
            "update torrent_jobs set last_stderr_tail='old failure',next_run_at=99 where id=?",
            (job_id,),
        )
        con.commit()
        con.close()
        runner = UploadJobRunner(
            repo,
            SlowRclone(),
            FakeExecutor(),
            lease_heartbeat_interval_sec=0.01,
        )

        assert runner.run_next() == job_id

        row = repo.get(job_id)
        assert repo.renew_count >= 1
        assert row["state"] == "cleanup_deferred"
        assert row["lease_owner"] is None
        assert row["lease_until"] is None
        assert row["last_stderr_tail"] is None
        assert row["next_run_at"] is None


def test_upload_completion_is_fenced_by_lease_owner_and_generation():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.rclone import VerifyResult
    from qbt_orchestrator.runtime import LeaseLostError, TorrentJobRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 100, lease_duration_sec=30)
        payload = {
            "local": "/tmp/a.mp4",
            "remote": "gcrypt:/A/a.mp4",
            "size": 100,
            "full_torrent": False,
        }
        job_id = repo.enqueue("h1", None, "upload", payload, priority=1)
        first = repo.claim_next("upload")
        assert first is not None
        assert first["lease_generation"] == 1

        con = sqlite3.connect(db)
        con.execute(
            "update torrent_jobs set state='retry_wait',lease_owner=null,lease_until=null,next_run_at=100 where id=?",
            (job_id,),
        )
        con.commit()
        con.close()
        second = repo.claim_next("upload")
        assert second is not None
        assert second["lease_generation"] == 2
        assert second["lease_owner"] != first["lease_owner"]

        try:
            repo.finalize_verified(
                first, payload, VerifyResult(True, "path_size", [])
            )
        except LeaseLostError:
            pass
        else:
            raise AssertionError("stale lease generation finalized the upload")

        current = repo.get(job_id)
        assert current["state"] == "running"
        assert current["lease_owner"] == second["lease_owner"]
        assert current["lease_generation"] == 2


def test_full_upload_verification_waits_for_promotion_without_cleanup():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.rclone import VerifyResult
    from qbt_orchestrator.runtime import TorrentJobRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1_000)
        payload = {
            "local": "/tmp/BBAN-582",
            "remote": "gcrypt:/BBAN-582.torrent-hash",
            "size": 123,
            "full_torrent": True,
            "media_files": [
                {
                    "remote_path": "gcrypt:/BBAN-582.torrent-hash/raw.mp4",
                    "size": 123,
                }
            ],
        }
        upload_id = repo.enqueue("h", None, "upload", payload, priority=10)

        state = repo.finalize_verified(
            repo.get(upload_id),
            payload,
            VerifyResult(True, "path_size", []),
        )

        assert state == "promotion_wait"
        assert repo.get(upload_id)["phase"] == "promotion_wait"
        assert _job_count(db, "cleanup_full_torrent") == 0


def test_finalization_barrier_creates_one_cleanup_after_promotion_and_sidecars():
    from qbt_orchestrator.db import migrate, write_transaction
    from qbt_orchestrator.integrations.rclone import VerifyResult
    from qbt_orchestrator.promotion import (
        MediaPromotionRepository,
        finalize_canonical_upload,
    )
    from qbt_orchestrator.runtime import TorrentJobRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        now = 1_000
        repo = TorrentJobRepository(db, now=lambda: now)
        payload = {
            "local": "/tmp/BBAN-582",
            "remote": "gcrypt:/BBAN-582.torrent-hash",
            "size": 123,
            "full_torrent": True,
            "cleanup_policy_snapshot": {"tags": "auto"},
        }
        upload_id = repo.enqueue("h", None, "upload", payload, priority=10)
        repo.finalize_verified(
            repo.get(upload_id),
            payload,
            VerifyResult(True, "path_size", []),
        )

        def seed(con):
            group_id = con.execute(
                "insert into media_groups(media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?)",
                ("BBAN-582", "BBAN-582", "/media/gcrypt/BBAN-582", now, now),
            ).lastrowid
            con.execute(
                "insert into media_pipeline_runs(upload_manifest_id,media_group_id,state,created_at,updated_at,canonical_remote_dir,canonical_basename,canonical_video_manifest_json) values(?,?,?,?,?,?,?,?)",
                (
                    f"upload-job-{upload_id}",
                    group_id,
                    "SidecarVerified",
                    now,
                    now,
                    "gcrypt:/BBAN-582",
                    "BBAN-582 影片名称",
                    '[{"remote_path":"gcrypt:/BBAN-582/BBAN-582 影片名称.mp4","size":123}]',
                ),
            )
            return int(group_id)

        group_id = write_transaction(db, seed)
        promotions = MediaPromotionRepository(db, now=lambda: now)
        promotion_id = promotions.enqueue(
            upload_job_id=upload_id,
            hash="h",
            media_group_id=group_id,
            normalized_id="BBAN-582",
            metadata_title="影片名称",
            display_title="BBAN-582 影片名称",
            source_remote="gcrypt:/BBAN-582.torrent-hash/raw.mp4",
            target_remote="gcrypt:/BBAN-582/BBAN-582 影片名称.mp4",
            expected_size=123,
        )
        promotions.record_verified(
            promotion_id,
            method="path_size",
            details={"verified": True, "mismatches": []},
        )

        assert finalize_canonical_upload(db, upload_id=upload_id, now=now) is True
        assert finalize_canonical_upload(db, upload_id=upload_id, now=now) is False
        cleanup = _rows(
            db,
            "select * from torrent_jobs where job_type='cleanup_full_torrent'",
        )
        assert len(cleanup) == 1
        cleanup_payload = json.loads(cleanup[0]["payload_json"])
        assert cleanup_payload["canonical_remote_verified"] is True
        assert cleanup_payload["remote"] == "gcrypt:/BBAN-582"
        assert repo.get(upload_id)["state"] == "cleanup_wait"
        refresh = _rows(
            db,
            "select emby_media_dir,state,earliest_run_at,max_run_at,payload_json from emby_refresh_tasks",
        )
        assert len(refresh) == 1
        assert refresh[0]["emby_media_dir"] == "/media/gcrypt/BBAN-582"
        assert refresh[0]["state"] == "queued"
        assert refresh[0]["earliest_run_at"] == now + 300
        assert refresh[0]["max_run_at"] == now + 900
        assert json.loads(refresh[0]["payload_json"])["trigger_state"] == "CanonicalRemoteVerified"


def test_cleanup_policy_blocks_unverified_seed_long_and_transient_seed_wait():
    from qbt_orchestrator.io_governor import JobPriority
    from qbt_orchestrator.cleanup_policy import cleanup_eligibility

    assert [int(value) for value in JobPriority] == [0, 10, 15, 50, 70, 80]

    assert cleanup_eligibility(
        {"tags": "auto", "seeding_time": 99_999, "ratio": 9.0},
        canonical_remote_verified=False,
        free_bytes=10 * 1024**3,
        pressure_free_bytes=5 * 1024**3,
        min_seed_sec=900,
        min_ratio=1.0,
        max_retention_sec=7200,
        now=1_000,
    ).reason == "remote_not_canonical"
    seed_long = cleanup_eligibility(
        {"tags": "auto,seed-long", "seeding_time": 99_999, "ratio": 9.0},
        canonical_remote_verified=True,
        free_bytes=10 * 1024**3,
        pressure_free_bytes=5 * 1024**3,
        min_seed_sec=900,
        min_ratio=1.0,
        max_retention_sec=7200,
        now=1_000,
    )
    assert seed_long.allowed is False
    assert seed_long.reason == "seed_long"
    assert seed_long.next_check_at is None
    waiting = cleanup_eligibility(
        {"tags": "auto", "seeding_time": 899, "ratio": 9.0},
        canonical_remote_verified=True,
        free_bytes=10 * 1024**3,
        pressure_free_bytes=5 * 1024**3,
        min_seed_sec=900,
        min_ratio=10.0,
        max_retention_sec=7200,
        now=1_000,
    )
    assert waiting.reason == "policy_wait"
    assert waiting.next_check_at == 1_300


def test_cleanup_releases_canonical_media_under_pressure_or_ratio_or_retention():
    from qbt_orchestrator.cleanup_policy import cleanup_eligibility

    base = {
        "tags": "auto",
        "seeding_time": 0,
        "ratio": 0.0,
        "completion_on": 9_900,
        "state": "stoppedUP",
    }
    common = {
        "canonical_remote_verified": True,
        "pressure_free_bytes": 5 * 1024**3,
        "min_seed_sec": 900,
        "min_ratio": 1.0,
        "max_retention_sec": 7200,
        "now": 10_000,
    }
    pressure = cleanup_eligibility(
        base, free_bytes=4 * 1024**3, **common
    )
    ratio = cleanup_eligibility(
        {**base, "ratio": 3.13}, free_bytes=10 * 1024**3, **common
    )
    retention = cleanup_eligibility(
        {**base, "completion_on": 1_000}, free_bytes=10 * 1024**3, **common
    )

    assert (pressure.allowed, pressure.reason) == (True, "disk_pressure")
    assert (ratio.allowed, ratio.reason) == (True, "ratio")
    assert (retention.allowed, retention.reason) == (True, "retention")


def test_cleanup_hard_gates_override_disk_pressure():
    from qbt_orchestrator.cleanup_policy import cleanup_eligibility

    common = {
        "free_bytes": 0,
        "pressure_free_bytes": 5 * 1024**3,
        "min_seed_sec": 0,
        "min_ratio": 0,
        "max_retention_sec": 0,
        "now": 10_000,
    }
    cases = [
        ({"tags": "hold"}, True, False, "hold"),
        ({"tags": "seed-long"}, True, False, "seed_long"),
        ({"tags": "auto"}, False, False, "remote_not_canonical"),
        ({"tags": "auto"}, True, True, "promotion_conflict"),
    ]
    for torrent, verified, conflict, reason in cases:
        decision = cleanup_eligibility(
            torrent,
            canonical_remote_verified=verified,
            promotion_conflict=conflict,
            **common,
        )
        assert decision.allowed is False
        assert decision.reason == reason


def test_full_cleanup_runner_never_deletes_seed_long_and_retries_transient_seed_policy():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import FullTorrentCleanupRunner, TorrentJobRepository
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = [1_000]
        repo = TorrentJobRepository(db, now=lambda: clock[0])
        executor = FakeExecutor()

        held_parent = repo.enqueue("held", None, "upload", {}, priority=10)
        repo.update_state(held_parent, "cleanup_wait")
        held_cleanup = repo.enqueue(
            "held",
            None,
            "cleanup_full_torrent",
            {"canonical_remote_verified": True, "cleanup_policy_snapshot": {"tags": "auto,seed-long", "seeding_time": 99_999, "ratio": 9.0}},
            priority=10,
            parent_job_id=held_parent,
        )
        held_runner = FullTorrentCleanupRunner(repo, executor, min_seed_sec=900, min_ratio=1.0)
        assert held_runner.run_next() == held_cleanup
        assert repo.get(held_cleanup)["state"] == "blocked"
        assert repo.get(held_parent)["state"] == "cleanup_wait"
        assert executor.posts == []

        parent = repo.enqueue("ready-later", None, "upload", {}, priority=10)
        repo.update_state(parent, "cleanup_wait")
        cleanup = repo.enqueue(
            "ready-later",
            None,
            "cleanup_full_torrent",
            {"canonical_remote_verified": True, "cleanup_policy_snapshot": {}},
            priority=10,
            parent_job_id=parent,
        )
        live = {"tags": "auto", "seeding_time": 899, "ratio": 0.5}
        runner = FullTorrentCleanupRunner(
            repo,
            executor,
            torrent_provider=lambda _hash: dict(live),
            min_seed_sec=900,
            min_ratio=1.0,
        )

        assert runner.run_next() == cleanup
        assert repo.get(cleanup)["state"] == "retry_wait"
        assert repo.get(cleanup)["next_run_at"] == 1_300
        assert executor.posts == []

        clock[0] = 1_301
        live.update({"seeding_time": 1_000, "ratio": 1.0})
        assert runner.run_next() == cleanup
        assert repo.get(cleanup)["state"] == "done"
        assert repo.get(parent)["state"] == "done"
        assert executor.posts == [
            ("/api/v2/torrents/delete", {"hashes": "ready-later", "deleteFiles": "true"})
        ]


def test_full_cleanup_runner_reclaims_largest_canonical_job_first_under_pressure():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import FullTorrentCleanupRunner, TorrentJobRepository
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1_000)
        executor = FakeExecutor()
        for torrent_hash, size in [("small", 10), ("large", 100)]:
            parent = repo.enqueue(torrent_hash, None, "upload", {}, priority=10)
            repo.update_state(parent, "cleanup_wait")
            repo.enqueue(
                torrent_hash,
                None,
                "cleanup_full_torrent",
                {
                    "canonical_remote_verified": True,
                    "final_manifest": [
                        {"remote_path": f"gcrypt:/{torrent_hash}.mp4", "size": size}
                    ],
                },
                priority=10,
                parent_job_id=parent,
            )
        runner = FullTorrentCleanupRunner(
            repo,
            executor,
            torrent_provider=lambda h: {
                "hash": h,
                "tags": "auto",
                "seeding_time": 0,
                "ratio": 0,
            },
            free_bytes_provider=lambda: 4 * 1024**3,
            pressure_free_bytes=5 * 1024**3,
            max_retention_sec=7200,
        )

        assert runner.run_next() is not None

        assert executor.posts == [
            ("/api/v2/torrents/delete", {"hashes": "large", "deleteFiles": "true"})
        ]


def test_upload_phase_schema_and_verify_retry_does_not_repeat_copy_or_delete():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor

    class RetryVerifyRclone:
        def __init__(self):
            self.copy_calls = 0
            self.verify_calls = 0
            self.verify_sizes = [99, 100]

        def copyto(self, local, remote):
            self.copy_calls += 1
            return True

        def lsjson_size(self, remote):
            self.verify_calls += 1
            return self.verify_sizes.pop(0)

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        columns = {row[1] for row in con.execute("pragma table_info(torrent_jobs)")}
        con.close()
        assert {"phase", "copy_completed_at", "verification_method", "verification_result_json", "verified_at", "parent_job_id"} <= columns

        repo = TorrentJobRepository(db, now=lambda: 1_000)
        upload_id = repo.enqueue(
            "h1",
            None,
            "upload",
            {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True},
            priority=1,
        )
        rclone = RetryVerifyRclone()
        executor = FakeExecutor()
        runner = UploadJobRunner(repo, rclone, executor)

        assert repo.get(upload_id)["phase"] == "queued_copy"
        assert runner.run_next() == upload_id
        first = repo.get(upload_id)
        assert first["state"] == "verify_pending"
        assert first["phase"] == "verifying"
        assert first["copy_completed_at"] == 1_000
        assert _job_count(db, "cleanup_full_torrent") == 0

        assert runner.run_next() == upload_id
        second = repo.get(upload_id)
        assert second["state"] == "promotion_wait"
        assert second["phase"] == "promotion_wait"
        assert second["verification_method"] == "single_size"
        assert json.loads(second["verification_result_json"]) == {"mismatches": [], "verified": True}
        assert second["verified_at"] == 1_000
        assert rclone.copy_calls == 1
        assert rclone.verify_calls == 2
        assert executor.posts == []
        assert _job_count(db, "cleanup_full_torrent") == 0
        from qbt_orchestrator.service import DaemonRuntime

        runtime = object.__new__(DaemonRuntime)
        runtime.state_db = db
        assert runtime._disk_releasing_job_count() == 1


def test_migration_backfills_legacy_verify_pending_phase_without_recopying():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_jobs(hash,job_type,state,phase,payload_json,created_at,updated_at) "
            "values('legacy','upload','verify_pending',null,'{}',1,1)"
        )
        con.commit()
        con.close()

        migrate(db, dry_run=False)

        assert _rows(db, "select state,phase from torrent_jobs where hash='legacy'") == [
            {"state": "verify_pending", "phase": "verifying"}
        ]


def test_upload_verified_enqueues_media_pipeline_and_sidecar_upload_uses_upload_worker():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 100)
        upload_id = repo.enqueue(
            "h1",
            1,
            "upload",
            {
                "local": "/tmp/ABC-123.mp4",
                "remote": "gcrypt:/ABC-123/ABC-123.mp4",
                "size": 100,
                "full_torrent": True,
                "upload_manifest_id": "manifest-h1",
                "media_files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 100, "duration_sec": 120}],
            },
            priority=1,
        )
        sidecar_id = repo.enqueue(
            None,
            None,
            "sidecar_upload",
            {"local": "/staging/ABC-123.nfo", "remote": "gcrypt:/ABC-123/ABC-123.nfo", "size": 10, "full_torrent": False},
            priority=2,
        )
        runner = UploadJobRunner(
            repo,
            FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/ABC-123/ABC-123.mp4": 100, "gcrypt:/ABC-123/ABC-123.nfo": 10}),
            FakeExecutor(),
        )

        assert runner.run_next() == upload_id
        assert repo.get(upload_id)["state"] == "promotion_wait"
        assert _job_count(db, "cleanup_full_torrent") == 0
        media_job = repo.claim_next("media_pipeline")
        assert media_job is not None
        media_payload = json.loads(media_job["payload_json"])
        assert media_payload["upload_manifest_id"] == "manifest-h1"
        assert media_payload["files"][0]["remote_path"] == "gcrypt:/ABC-123/ABC-123.mp4"

        assert runner.run_next() == sidecar_id
        assert repo.get(sidecar_id)["state"] == "done"


def test_sidecar_upload_verified_marks_manifest_and_only_then_queues_emby_refresh():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into media_groups(id,media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?,?)",
            (1, "ABC-123", "ABC-123", "/media/gcrypt/ABC-123", 100, 100),
        )
        con.execute(
            "insert into media_pipeline_runs(id,upload_manifest_id,media_group_id,state,metadata_policy,metadata_quality,created_at,updated_at) values(?,?,?,?,?,?,?,?)",
            (1, "manifest-1", 1, "SidecarUploadQueued", "sidecar", "normalized", 100, 100),
        )
        con.execute(
            "insert into sidecar_manifests(id,media_group_id,staging_dir,artifacts_json,state,created_at,updated_at) values(?,?,?,?,?,?,?)",
            (1, 1, "/staging/ABC-123", "[]", "local_sidecar_validated", 100, 100),
        )
        con.commit(); con.close()
        repo = TorrentJobRepository(db, now=lambda: 1000)
        nfo_job = repo.enqueue(
            None,
            None,
            "sidecar_upload",
            {"local": "/staging/ABC-123/movie.nfo", "remote": "gcrypt:/ABC-123/movie.nfo", "size": 10, "full_torrent": False, "sidecar_manifest_id": 1},
            priority=1,
        )
        poster_job = repo.enqueue(
            None,
            None,
            "sidecar_upload",
            {"local": "/staging/ABC-123/poster.jpg", "remote": "gcrypt:/ABC-123/poster.jpg", "size": 20, "full_torrent": False, "sidecar_manifest_id": 1},
            priority=2,
        )
        runner = UploadJobRunner(
            repo,
            FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/ABC-123/movie.nfo": 10, "gcrypt:/ABC-123/poster.jpg": 20}),
            FakeExecutor(),
        )

        assert runner.run_next() == nfo_job
        assert _rows(db, "select state from sidecar_manifests where id=1") == [{"state": "sidecar_uploading"}]
        assert _rows(db, "select state from media_pipeline_runs where id=1") == [{"state": "SidecarUploading"}]
        assert _rows(db, "select count(*) as n from emby_refresh_tasks") == [{"n": 0}]

        assert runner.run_next() == poster_job
        assert _rows(db, "select state from sidecar_manifests where id=1") == [{"state": "sidecar_verified"}]
        assert _rows(db, "select state from media_pipeline_runs where id=1") == [{"state": "SidecarVerified"}]
        refresh = _rows(db, "select emby_media_dir,state,earliest_run_at,max_run_at,payload_json from emby_refresh_tasks")
        assert len(refresh) == 1
        assert refresh[0]["emby_media_dir"] == "/media/gcrypt/ABC-123"
        assert refresh[0]["state"] == "queued"
        assert refresh[0]["earliest_run_at"] == 1300
        assert refresh[0]["max_run_at"] == 1900
        assert json.loads(refresh[0]["payload_json"])["trigger_state"] == "SidecarVerified"


def test_sidecar_completion_finalizes_already_promoted_upload_without_early_refresh():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.integrations.rclone import VerifyResult
    from qbt_orchestrator.promotion import MediaPromotionRepository
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        upload_payload = {
            "local": "/tmp/ABC-123",
            "remote": "gcrypt:/ABC-123-hash",
            "size": 10,
            "full_torrent": True,
        }
        upload_id = repo.enqueue("h1", None, "upload", upload_payload, priority=1)
        repo.finalize_verified(
            repo.get(upload_id), upload_payload, VerifyResult(True, "path_size", [])
        )
        con = sqlite3.connect(db)
        con.execute(
            "insert into media_groups(id,media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?,?)",
            (1, "ABC-123", "ABC-123", "/media/gcrypt/ABC-123", 100, 100),
        )
        con.execute(
            "insert into media_pipeline_runs(id,upload_manifest_id,media_group_id,state,metadata_policy,metadata_quality,created_at,updated_at,canonical_remote_dir,canonical_basename,canonical_video_manifest_json) "
            "values(?,?,?,?,?,?,?,?,?,?,?)",
            (
                1,
                f"upload-job-{upload_id}",
                1,
                "SidecarUploadQueued",
                "sidecar",
                "normalized",
                100,
                100,
                "gcrypt:/ABC-123",
                "ABC-123 Title",
                '[{"remote_path":"gcrypt:/ABC-123/ABC-123 Title.mp4","size":10}]',
            ),
        )
        con.execute(
            "insert into sidecar_manifests(id,media_group_id,staging_dir,artifacts_json,state,created_at,updated_at) values(?,?,?,?,?,?,?)",
            (1, 1, "/staging/ABC-123", "[]", "local_sidecar_validated", 100, 100),
        )
        con.commit()
        con.close()
        promotions = MediaPromotionRepository(db, now=lambda: 1000)
        promotion_id = promotions.enqueue(
            upload_job_id=upload_id,
            hash="h1",
            media_group_id=1,
            normalized_id="ABC-123",
            metadata_title="Title",
            display_title="ABC-123 Title",
            source_remote="gcrypt:/ABC-123-hash/raw.mp4",
            target_remote="gcrypt:/ABC-123/ABC-123 Title.mp4",
            expected_size=10,
        )
        promotions.record_verified(
            promotion_id, method="path_size", details={"verified": True}
        )
        sidecar_job = repo.enqueue(
            None,
            None,
            "sidecar_upload",
            {
                "local": "/staging/ABC-123/movie.nfo",
                "remote": "gcrypt:/ABC-123/ABC-123 Title.nfo",
                "size": 5,
                "full_torrent": False,
                "sidecar_manifest_id": 1,
            },
            priority=1,
        )
        runner = UploadJobRunner(
            repo,
            FakeRclone(
                copy_ok=True,
                remote_sizes={"gcrypt:/ABC-123/ABC-123 Title.nfo": 5},
            ),
            FakeExecutor(),
        )

        assert runner.run_next() == sidecar_job

        assert repo.get(upload_id)["state"] == "cleanup_wait"
        assert _job_count(db, "cleanup_full_torrent") == 1
        refresh = _rows(db, "select payload_json from emby_refresh_tasks")
        assert len(refresh) == 1
        assert json.loads(refresh[0]["payload_json"])["trigger_state"] == "CanonicalRemoteVerified"


def test_sidecar_upload_verify_failure_at_max_attempts_uses_passthrough_not_verified():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into media_groups(id,media_group_key,normalized_id,emby_media_dir,created_at,updated_at) values(?,?,?,?,?,?)",
            (1, "ABC-123", "ABC-123", "/media/gcrypt/ABC-123", 100, 100),
        )
        con.execute(
            "insert into media_pipeline_runs(id,upload_manifest_id,media_group_id,state,metadata_policy,metadata_quality,created_at,updated_at) values(?,?,?,?,?,?,?,?)",
            (1, "manifest-1", 1, "SidecarUploadQueued", "sidecar", "normalized", 100, 100),
        )
        con.execute(
            "insert into sidecar_manifests(id,media_group_id,staging_dir,artifacts_json,state,created_at,updated_at) values(?,?,?,?,?,?,?)",
            (1, 1, "/staging/ABC-123", "[]", "local_sidecar_validated", 100, 100),
        )
        con.commit()
        con.close()

        repo = TorrentJobRepository(db, now=lambda: 2000)
        job_id = repo.enqueue(
            None,
            None,
            "sidecar_upload",
            {
                "local": "/staging/ABC-123/poster.jpg",
                "remote": "gcrypt:/ABC-123/poster.jpg",
                "size": 20,
                "full_torrent": False,
                "sidecar_manifest_id": 1,
                "allow_unrecognized_passthrough": True,
            },
            priority=1,
        )
        con = sqlite3.connect(db)
        con.execute("update torrent_jobs set max_attempts=1 where id=?", (job_id,))
        con.commit()
        con.close()

        runner = UploadJobRunner(
            repo,
            FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/ABC-123/poster.jpg": 19}),
            FakeExecutor(),
        )

        assert runner.run_next() == job_id

        assert repo.get(job_id)["state"] == "failed"
        assert _rows(db, "select state from sidecar_manifests where id=1") == [{"state": "sidecar_upload_failed"}]
        run = _rows(db, "select state,metadata_policy,passthrough_reason from media_pipeline_runs where id=1")[0]
        assert run == {
            "state": "PassthroughAllowed",
            "metadata_policy": "passthrough",
            "passthrough_reason": "sidecar_upload_failed",
        }
        refresh = _rows(db, "select emby_media_dir,state,earliest_run_at,max_run_at,payload_json from emby_refresh_tasks")
        assert len(refresh) == 1
        assert refresh[0]["emby_media_dir"] == "/media/gcrypt/ABC-123"
        assert refresh[0]["state"] == "queued"
        assert refresh[0]["earliest_run_at"] == 2300
        assert refresh[0]["max_run_at"] == 2900
        payload = json.loads(refresh[0]["payload_json"])
        assert payload["trigger_state"] == "PassthroughAllowed"
        assert payload["passthrough_reason"] == "sidecar_upload_failed"


def test_upload_job_runner_marks_pipeline_batch_cleanup_deferred_and_counts_pending_cleanup():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor, FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,downloaded_bytes,reserved_bytes,upload_job_id,local_pinned_bytes,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "h1", 1, "upload_queued", "pipeline", "[0]", 10, 10, 100, None, 10, 50, 50),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
            ("h1", 1, "cleanup_pending", 10, "active", 50, None, "batch_upload_queued"),
        )
        con.commit(); con.close()
        repo = TorrentJobRepository(db, now=lambda: 100)
        job_id = repo.enqueue(
            "h1",
            1,
            "upload",
            {
                "local": "/tmp/Big",
                "remote": "gcrypt:/Big",
                "size": 10,
                "full_torrent": False,
                "copy_mode": "copy_files",
                "files": [{"relative_path": "A.mp4", "local_path": "/tmp/Big/A.mp4", "remote_path": "gcrypt:/Big/A.mp4", "size": 10}],
                "media_files": [{"remote_path": "gcrypt:/Big/A.mp4", "size": 10, "duration_sec": 120}],
            },
            priority=1,
        )
        rclone = FakeRclone(copy_ok=True, remote_listing=[{"Path": "A.mp4", "Size": 10}])
        runner = UploadJobRunner(repo, rclone, FakeExecutor())

        assert runner.run_next() == job_id

        assert repo.get(job_id)["state"] == "cleanup_deferred"
        assert rclone.copies == [("/tmp/Big/A.mp4", "gcrypt:/Big/A.mp4")]
        assert rclone.dir_copies == []
        batch = _rows(db, "select state,upload_job_id,local_pinned_bytes,cleanup_deferred_at from torrent_batches where id=1")[0]
        assert batch == {"state": "cleanup_deferred", "upload_job_id": job_id, "local_pinned_bytes": 10, "cleanup_deferred_at": 100}
        reservation = _rows(
            db,
            "select kind,accounting_class,owner,last_observed_at,bytes,state,reason "
            "from resource_reservations where batch_id=1 and kind='cleanup_pending'",
        )[0]
        assert reservation == {
            "kind": "cleanup_pending",
            "accounting_class": "current_pinned",
            "owner": "upload_job_runner",
            "last_observed_at": 100,
            "bytes": 10,
            "state": "active",
            "reason": "batch_cleanup_deferred",
        }
        media_job = repo.claim_next("media_pipeline")
        assert media_job is not None
        assert media_job["batch_id"] == 1


def test_command_processor_executes_safe_commands_and_requires_cleanup_approval():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, BotNotificationRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db)
        notifications = BotNotificationRepository(db)
        commands.insert_command("c1", 100, 2, "pause", {"args": ["h1"]})
        commands.insert_command("c2", 100, 2, "resume", {"args": ["h1"]})
        commands.insert_command("c3", 100, 3, "cleanup", {"args": ["h2"]})
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor, notifications=notifications)

        assert processor.run_next() == "c1"
        assert processor.run_next() == "c2"
        assert processor.run_next() == "c3"
        assert executor.posts == [
            ("/api/v2/torrents/stop", {"hashes": "h1"}),
            ("/api/v2/torrents/start", {"hashes": "h1"}),
        ]
        assert commands.get("c3")["state"] == "approval_required"
        assert commands.pending_approvals()[0]["action"] == "cleanup"
        approval_notice = notifications.list_all()[0]
        assert approval_notice["topic"] == "approval"
        assert "approval required: /cleanup h2" in approval_notice["message"]
        notice_payload = json.loads(approval_notice["payload_json"])
        assert notice_payload["approval_id"] == "approval-c3"
        assert notice_payload["reply_markup"] == {
            "inline_keyboard": [[
                {"text": "Approve", "callback_data": "approve:approval-c3"},
                {"text": "Deny", "callback_data": "deny:approval-c3"},
            ]]
        }


def test_approved_dangerous_command_executes_once_after_approval():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db, now=lambda: 100)
        commands.insert_command("c4", 100, 3, "preempt", {"args": ["h9"]})
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor)

        assert processor.run_next() == "c4"
        assert commands.get("c4")["state"] == "approval_required"
        assert commands.pending_approvals()[0]["approval_id"] == "approval-c4"

        assert commands.approve_once("approval-c4", user_id=3) is True
        assert commands.approve_once("approval-c4", user_id=3) is False
        assert commands.get("c4")["state"] == "approved"

        assert processor.run_next() == "c4"
        assert processor.run_next() is None
        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h9"})]
        assert commands.get("c4")["state"] == "done"
        assert commands.pending_approvals()[0]["state"] == "approved"


def test_approved_preempt_command_uses_preemption_service_when_configured():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    class FakePreemptionService:
        def __init__(self):
            self.forced = []

        def force_preempt_hash(self, seeding_hash, *, target_hash=None, reason="telegram"):
            self.forced.append((seeding_hash, target_hash, reason))
            return {"accepted": True, "seeding_hash": seeding_hash}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db, now=lambda: 100)
        commands.insert_command("c-preempt", 100, 3, "preempt", {"args": ["seed1", "newhot"]})
        executor = FakeExecutor()
        preemption = FakePreemptionService()
        processor = CommandProcessor(commands, executor, preemption_service=preemption)

        assert processor.run_next() == "c-preempt"
        assert commands.get("c-preempt")["state"] == "approval_required"
        assert commands.approve_once("approval-c-preempt", user_id=3) is True
        assert processor.run_next() == "c-preempt"

        assert preemption.forced == [("seed1", "newhot", "telegram")]
        assert executor.posts == []
        assert commands.get("c-preempt")["state"] == "done"


def test_command_processor_queue_and_approved_force_upload_create_durable_jobs():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db, now=lambda: 200)
        queue_payload = {
            "hash": "h-queue",
            "batch_id": 7,
            "priority": 5,
            "job_payload": {"local": "/tmp/q.mp4", "remote": "gcrypt:/Q/q.mp4", "size": 10, "full_torrent": True},
        }
        force_payload = {
            "args": ["h-force"],
            "job_payload": {"local": "/tmp/f.mp4", "remote": "gcrypt:/F/f.mp4", "size": 20, "full_torrent": True},
        }
        commands.insert_command("queue-1", 100, 2, "queue", queue_payload)
        commands.insert_command("force-1", 100, 3, "force_upload", force_payload)
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor)

        assert processor.run_next() == "queue-1"
        assert commands.get("queue-1")["state"] == "done"
        assert processor.run_next() == "force-1"
        assert commands.get("force-1")["state"] == "approval_required"
        assert commands.approve_once("approval-force-1", user_id=3) is True
        assert processor.run_next() == "force-1"

        assert executor.posts == []
        rows = _rows(db, "select hash,batch_id,job_type,state,priority,payload_json from torrent_jobs order by id")
        assert len(rows) == 2
        assert rows[0]["hash"] == "h-queue"
        assert rows[0]["batch_id"] == 7
        assert rows[0]["job_type"] == "upload"
        assert rows[0]["priority"] == 5
        assert json.loads(rows[0]["payload_json"])["remote"] == "gcrypt:/Q/q.mp4"
        assert rows[1]["hash"] == "h-force"
        assert rows[1]["job_type"] == "upload"
        assert rows[1]["priority"] == 0
        assert json.loads(rows[1]["payload_json"])["force_upload"] is True
        assert commands.get("force-1")["state"] == "done"


def test_approved_cleanup_enqueues_request_without_deleting_files_and_config_is_audited():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db, now=lambda: 300)
        commands.insert_command("cleanup-1", 100, 3, "cleanup", {"args": ["h-clean"]})
        commands.insert_command("config-1", 100, 3, "config", {"args": ["set", "batch.enabled", "false"]})
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor)

        assert processor.run_next() == "cleanup-1"
        assert commands.approve_once("approval-cleanup-1", user_id=3) is True
        assert processor.run_next() == "cleanup-1"
        assert processor.run_next() == "config-1"
        assert commands.approve_once("approval-config-1", user_id=3) is True
        assert processor.run_next() == "config-1"

        assert executor.posts == []
        jobs = _rows(db, "select hash,job_type,state,priority,payload_json from torrent_jobs")
        assert len(jobs) == 1
        assert jobs[0]["hash"] == "h-clean"
        assert jobs[0]["job_type"] == "cleanup_request"
        assert jobs[0]["state"] == "queued"
        assert json.loads(jobs[0]["payload_json"]) == {"target": "h-clean", "args": ["h-clean"], "source": "telegram"}
        actions = _rows(db, "select action_type,path,status,dry_run,payload_json from action_log where action_type='bot_config'")
        assert len(actions) == 1
        assert actions[0]["path"] == "config"
        assert actions[0]["status"] == "queued"
        assert json.loads(actions[0]["payload_json"])["args"] == ["set", "batch.enabled", "false"]
        assert commands.get("cleanup-1")["state"] == "done"
        assert commands.get("config-1")["state"] == "done"


def test_cleanup_request_runner_logically_marks_pipeline_batch_without_physical_delete():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import CleanupRequestRunner, TorrentJobRepository
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,total_bytes,downloaded_bytes,reserved_bytes,local_pinned_bytes,cleanup_deferred_at,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (9, "h-clean", 1, "cleanup_deferred", "pipeline", "[0]", 10, 10, 12, 10, 100, 100, 100),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
            ("h-clean", 9, "cleanup_pending", 10, "active", 100, None, "batch_cleanup_deferred"),
        )
        con.commit()
        con.close()
        repo = TorrentJobRepository(db, now=lambda: 500)
        job_id = repo.enqueue("h-clean", 9, "cleanup_request", {"target": "h-clean", "source": "telegram"}, priority=10)
        executor = FakeExecutor()
        runner = CleanupRequestRunner(repo, executor)

        assert runner.run_next() == job_id

        assert executor.posts == []
        assert repo.get(job_id)["state"] == "done"
        batch = _rows(db, "select state,cleanup_deferred_at,updated_at from torrent_batches where id=9")[0]
        assert batch == {"state": "cleanup_requested", "cleanup_deferred_at": 100, "updated_at": 500}
        reservation = _rows(db, "select state,reason from resource_reservations where batch_id=9 and kind='cleanup_pending'")[0]
        assert reservation == {"state": "active", "reason": "cleanup_requested_logical_only"}
        action = _rows(db, "select action_type,path,status,dry_run,payload_json from action_log where action_type='cleanup_request'")[0]
        assert action["path"] == "torrent_batches/9"
        assert action["status"] == "logical_only"
        assert action["dry_run"] == 0
        assert json.loads(action["payload_json"])["physical_delete"] is False


def test_cleanup_request_runner_blocks_when_no_piece_safe_target_exists():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import CleanupRequestRunner, TorrentJobRepository
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 600)
        job_id = repo.enqueue("missing", None, "cleanup_request", {"target": "missing", "source": "telegram"}, priority=10)
        runner = CleanupRequestRunner(repo, FakeExecutor())

        assert runner.run_next() == job_id

        row = repo.get(job_id)
        assert row["state"] == "blocked"
        assert row["last_stderr_tail"] == "no cleanup_deferred batch matched request"
        action = _rows(db, "select action_type,path,status,dry_run from action_log where action_type='cleanup_request'")[0]
        assert action == {"action_type": "cleanup_request", "path": "cleanup_request", "status": "blocked", "dry_run": 0}


def test_bot_notification_repository_redacts_dedupes_and_retries():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = BotNotificationRepository(db, now=lambda: 100)

        first = repo.enqueue(
            chat_id=100,
            topic="status",
            message="token " + "123456:" + "secret-token" + " mag" + "net:?xt=urn:btih:" + "A" * 40,
            payload={"api_key": "abc123"},
            dedupe_key="status-c1",
        )
        second = repo.enqueue(chat_id=100, topic="status", message="duplicate", dedupe_key="status-c1")

        assert second == first
        claimed = repo.claim_next()
        assert claimed is not None
        assert claimed["id"] == first
        assert "secret-token" not in claimed["message"]
        assert "mag" + "net:?" not in claimed["message"]
        assert "<redacted-token>" in claimed["message"]

        repo.schedule_retry(first, error="telegram token " + "123456:" + "secret-token", delay_sec=60)
        assert repo.claim_next() is None

        due_repo = BotNotificationRepository(db, now=lambda: 161)
        claimed_retry = due_repo.claim_next()
        assert claimed_retry is not None
        assert claimed_retry["attempts"] == 2
        due_repo.mark_sent(first)
        assert due_repo.get(first)["state"] == "sent"
        assert "secret-token" not in due_repo.get(first)["last_error"]


def test_command_processor_status_trace_perf_enqueue_readonly_notifications_without_qbt_writes():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, BotNotificationRepository, CommandProcessor, ObservabilityStore
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert or replace into disk_state(id,sampled_at,free_bytes,pressure_state,resume_allowed) values(1,100,?,?,1)",
            (6 * 1024**3, "ok"),
        )
        con.execute(
            "insert into torrent_jobs(hash,job_type,state,priority,payload_json,created_at,updated_at) values('h1','upload','queued',1,'{}',100,100)"
        )
        con.commit()
        con.close()
        obs = ObservabilityStore(db, now=lambda: 101)
        obs.event("info", "qbt", "sync_ok", "hash h1", {"rid": 7}, hash="h1", correlation_id="corr-1")
        obs.action(hash="h1", job_id=7, action_type="qbt_post", path="/api/v2/torrents/stop", payload={"hashes": "h1"}, status="succeeded")

        commands = BotCommandRepository(db, now=lambda: 102)
        notifications = BotNotificationRepository(db, now=lambda: 102)
        commands.insert_command("s1", 100, 1, "status", {"args": ["disk"]})
        commands.insert_command("t1", 100, 1, "trace", {"args": ["h1"]})
        commands.insert_command("p1", 100, 1, "perf", {"args": []})
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor, notifications=notifications)

        assert processor.run_next() == "s1"
        assert processor.run_next() == "t1"
        assert processor.run_next() == "p1"
        assert executor.posts == []
        assert commands.get("s1")["state"] == "done"
        assert commands.get("t1")["state"] == "done"
        assert commands.get("p1")["state"] == "done"

        messages = [row["message"] for row in notifications.list_all()]
        assert any("disk=ok" in msg and "free=6.00GiB" in msg for msg in messages)
        assert any("trace h1" in msg and "sync_ok" in msg and "qbt_post" in msg for msg in messages)
        assert any("perf" in msg and "events=" in msg and "actions=" in msg for msg in messages)


class ExplodingRclone:
    def copyto(self, local, remote):
        raise RuntimeError("backend rate limit token " + "123456:" + "secret-token")
    def lsjson_size(self, remote):
        raise AssertionError("verify must not run after failed copy")


def test_torrent_job_repository_skips_retry_wait_until_next_run_at():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 100)
        job_id = repo.enqueue("h1", None, "upload", {"local": "a", "remote": "b", "size": 1}, priority=1)
        repo.schedule_retry(job_id, stderr_tail="later", exit_code=5, delay_sec=60)

        assert repo.claim_next("upload") is None

        due_repo = TorrentJobRepository(db, now=lambda: 161)
        claimed = due_repo.claim_next("upload")
        assert claimed is not None
        assert claimed["id"] == job_id
        assert due_repo.get(job_id)["state"] == "running"


def test_upload_job_runner_schedules_retry_wait_on_rclone_exception_with_redaction():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 100)
        job_id = repo.enqueue("h1", None, "upload", {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True}, priority=1)
        runner = UploadJobRunner(repo, ExplodingRclone(), FakeExecutor(), backoff_schedule=(60, 180))

        assert runner.run_next() == job_id

        row = repo.get(job_id)
        assert row["state"] == "retry_wait"
        assert row["next_run_at"] == 160
        assert row["last_exit_code"] == 1
        assert "secret-token" not in row["last_stderr_tail"]
        assert "<redacted-token>" in row["last_stderr_tail"]


def test_reconcile_expired_running_upload_job_to_retry_wait():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, reconcile_jobs

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 100)
        job_id = repo.enqueue("h1", None, "upload", {"local": "a", "remote": "b", "size": 1}, priority=1)
        claimed = repo.claim_next("upload")
        assert claimed is not None

        report = reconcile_jobs(db, now=2000, dry_run=False)

        assert report["expired_running"] == 1
        row = repo.get(job_id)
        assert row["state"] == "retry_wait"
        assert row["next_run_at"] == 2060
        assert "lease expired" in row["last_stderr_tail"]


def test_reconcile_exhausted_retry_wait_upload_job_to_failed():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, reconcile_jobs

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        job_id = repo.enqueue("gone", None, "upload", {"local": "/missing", "remote": "gcrypt:/gone", "size": 1}, priority=1)
        con = sqlite3.connect(db)
        con.execute(
            "update torrent_jobs set state='retry_wait', attempts=6, max_attempts=6, next_run_at=?, last_stderr_tail='directory not found' where id=?",
            (999999, job_id),
        )
        con.commit(); con.close()

        dry_report = reconcile_jobs(db, now=2000, dry_run=True)
        assert dry_report["exhausted_retry_wait"] == 1
        assert repo.get(job_id)["state"] == "retry_wait"

        report = reconcile_jobs(db, now=2000, dry_run=False)
        assert report["exhausted_retry_wait"] == 1
        row = repo.get(job_id)
        assert row["state"] == "failed"
        assert row["last_exit_code"] == 1
        assert "retry attempts exhausted" in row["last_stderr_tail"]


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
