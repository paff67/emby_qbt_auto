#!/usr/bin/env python3
import asyncio
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT))


def test_config_merges_legacy_with_vps_runtime_defaults():
    from qbt_orchestrator.config import load_config_from_dict

    cfg = load_config_from_dict(
        {
            "qbt": {"container": "qbittorrent", "api_base": "http://127.0.0.1:8080"},
            "paths": {"state_db": "/var/lib/qbt-orchestrator/state.sqlite"},
            "rclone": {"config": "/root/.config/rclone/rclone.conf", "remote": "gcrypt:"},
        }
    )

    assert cfg.qbt.container == "qbittorrent"
    assert cfg.qbt.api_base == "http://127.0.0.1:8080"
    assert cfg.emby.container_media_prefix == "/media/gcrypt"
    assert cfg.disk.emergency_free_bytes == 2 * 1024**3
    assert cfg.qbt_preferences.preallocate_all is False
    assert cfg.qbt_preferences.incomplete_files_ext_desired is None
    assert "incomplete_files_ext" in cfg.runtime_warnings[0]


def test_config_reads_explicit_qbt_preferences_guard_values():
    from qbt_orchestrator.config import load_config_from_dict

    cfg = load_config_from_dict(
        {
            "qbt_preferences": {
                "preallocate_all": True,
                "incomplete_files_ext_desired": True,
            }
        }
    )

    assert cfg.qbt_preferences.preallocate_all is True
    assert cfg.qbt_preferences.incomplete_files_ext_desired is True


def test_redaction_masks_tokens_magnets_and_rclone_config_paths():
    from qbt_orchestrator.observability import redact

    data = {
        "telegram_token": "123456:" + "secret-token",
        "magnet": "mag" + "net:?xt=urn:btih:" + "A" * 40 + "&dn=name",
        "rclone_config": "/root/.config/rclone/rclone.conf",
        "safe": "gcrypt:/Media/ABC-123/",
    }

    redacted = redact(data)
    dumped = json.dumps(redacted)
    assert "secret-token" not in dumped
    assert "magnet:?" not in dumped
    assert "/root/.config/rclone" not in dumped
    assert redacted["safe"] == "gcrypt:/Media/ABC-123/"


def test_qbt_sync_full_update_rebuilds_and_unhealthy_preserves_cache():
    from qbt_orchestrator.qbt_sync import QbtSyncCache, SyncHealth
    from tests.fakes import FakeQbtClient

    client = FakeQbtClient(
        maindata=[
            {"rid": 1, "full_update": True, "torrents": {"h1": {"hash": "h1", "name": "one"}}},
            {"rid": 2, "full_update": False, "torrents": {"h2": {"hash": "h2", "name": "two"}}, "torrents_removed": ["h1"]},
            RuntimeError("timeout"),
            {"rid": 3, "full_update": True, "torrents": {}},
        ]
    )
    cache = QbtSyncCache(client, managed_count_provider=lambda: 2)

    assert cache.poll_once().health == SyncHealth.HEALTHY_FULL
    assert set(cache.snapshots) == {"h1"}
    assert cache.poll_once().health == SyncHealth.HEALTHY_DELTA
    assert set(cache.snapshots) == {"h2"}
    assert cache.poll_once().health == SyncHealth.UNHEALTHY
    assert set(cache.snapshots) == {"h2"}
    assert cache.poll_once().health == SyncHealth.SUSPECT_EMPTY_FULL
    assert set(cache.snapshots) == {"h2"}
    assert cache.high_risk_actions_allowed is False


def test_disk_pressure_and_emergency_action_do_not_need_db():
    from qbt_orchestrator.models import DiskPressureState
    from qbt_orchestrator.policies.disk import classify_disk, emergency_pause_action

    assert classify_disk(6 * 1024**3).state == DiskPressureState.OK
    assert classify_disk(4 * 1024**3).state == DiskPressureState.WATCH
    assert classify_disk(3 * 1024**3).state == DiskPressureState.GUARD
    assert classify_disk(2 * 1024**3).state == DiskPressureState.CRITICAL
    assert classify_disk(2 * 1024**3 - 1).state == DiskPressureState.EMERGENCY

    action = emergency_pause_action([{"hash": "a", "category": "auto", "state": "downloading"}, {"hash": "b", "category": "", "state": "downloading"}])
    assert action is not None
    assert action.path == "/api/v2/torrents/stop"
    assert action.payload == {"hashes": "a"}


def test_health_state_machine_and_seq_dl_policy():
    from qbt_orchestrator.models import LifecycleState
    from qbt_orchestrator.policies.health import HealthPolicy, TorrentHealthSample
    from qbt_orchestrator.policies.download_mode import desired_seq_dl

    policy = HealthPolicy(now=lambda: 1000)
    current = LifecycleState.ACTIVE
    sample = TorrentHealthSample(dlspeed_bps=50 * 1024, upspeed_bps=0, completed_bytes=100, progress=0.2, num_seeds=1, num_peers=1, active_since=0, low_speed_since=600, no_progress_since=900)
    assert policy.next_state(current, sample) == LifecycleState.SOAK

    soak_fast = TorrentHealthSample(dlspeed_bps=512000, upspeed_bps=0, completed_bytes=200, progress=0.21, num_seeds=2, num_peers=3, active_since=0, low_speed_since=None, no_progress_since=None, promote_ticks=2)
    assert policy.next_state(LifecycleState.SOAK, soak_fast, disk_budget_allows=True, active_slot_available=True) == LifecycleState.ACTIVE

    dead_sample = TorrentHealthSample(dlspeed_bps=0, upspeed_bps=0, completed_bytes=200, progress=0.21, num_seeds=0, num_peers=0, last_swarm_seen_at=-3000, no_progress_since=-3000)
    assert policy.next_state(LifecycleState.SOAK, dead_sample) == LifecycleState.DEAD

    for state in [LifecycleState.SOAK, LifecycleState.DEAD, LifecycleState.CAROUSEL_PROBE]:
        assert desired_seq_dl(state, seeds=99, peers=99, stalled_seconds=0) is False
    assert desired_seq_dl(LifecycleState.ACTIVE, seeds=3, peers=5, stalled_seconds=0) is True
    assert desired_seq_dl(LifecycleState.ACTIVE, seeds=1, peers=5, stalled_seconds=0) is False


def test_batch_reservation_and_cleanup_gate_rules():
    from qbt_orchestrator.policies.batching import compute_batch_reservation, cleanup_decision

    reservation = compute_batch_reservation([{"index": 0, "size": 1024**3, "first_piece": 10, "last_piece": 20}], piece_size=16 * 1024**2, filesystem_slack=128 * 1024**2)
    assert reservation.payload_bytes == 1024**3
    assert reservation.piece_spill_overhead_bytes == 32 * 1024**2
    assert reservation.reserved_bytes == 1024**3 + 32 * 1024**2 + 128 * 1024**2

    assert cleanup_decision(full_torrent=False, remote_verified=True).allow_delete_files is False
    assert cleanup_decision(full_torrent=False, remote_verified=True).state == "CleanupDeferred"
    assert cleanup_decision(full_torrent=True, remote_verified=False).allow_delete_files is False
    assert cleanup_decision(full_torrent=True, remote_verified=True).allow_delete_files is True


def test_sqlite_migration_db_actor_readonly_and_job_recovery():
    from qbt_orchestrator.db import DbActor, migrate, recover_jobs, readonly_counts

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        con = sqlite3.connect(db)
        con.execute("create table torrent_state(hash text primary key, name text)")
        con.execute("insert into torrent_state values('h1','legacy')")
        con.commit(); con.close()

        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        tables = {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
        for table in ["torrent_jobs", "action_log", "events_v2", "decision_log", "metrics_snapshots", "media_groups", "bot_commands", "bot_approvals"]:
            assert table in tables
        assert con.execute("select name from torrent_state where hash='h1'").fetchone()[0] == "legacy"
        con.close()

        async def run_actor():
            actor = DbActor(db)
            await actor.start()
            job_id = await actor.enqueue_job("h1", None, "upload", {"path": "x"}, priority=10)
            await actor.flush()
            await actor.stop()
            return job_id

        job_id = asyncio.run(run_actor())
        assert job_id >= 1
        assert readonly_counts(db)["torrent_jobs"] == 1
        recovered = recover_jobs(db)
        assert recovered[0]["id"] == job_id
        assert recovered[0]["state"] == "queued"


def test_executor_dry_run_action_log_and_seq_toggle_idempotency():
    from qbt_orchestrator.executor import Executor
    from tests.fakes import FakeQbtClient

    qbt = FakeQbtClient(info={"h1": {"hash": "h1", "seq_dl": True}})
    ex = Executor(qbt, dry_run=True)
    assert ex.set_seq_dl("h1", True) is False
    assert qbt.posts == []
    assert ex.set_seq_dl("h1", False) is True
    assert qbt.posts == []
    assert ex.action_log[-1].dry_run is True
    assert ex.action_log[-1].path == "/api/v2/torrents/toggleSequentialDownload"

    live = Executor(FakeQbtClient(info={"h2": {"hash": "h2", "seq_dl": False}}), dry_run=False)
    assert live.set_seq_dl("h2", True) is True
    assert live.qbt.posts[-1][0] == "/api/v2/torrents/toggleSequentialDownload"


def test_daemon_safety_loop_only_uses_allowed_operations_and_pauses_below_floor():
    from qbt_orchestrator.daemon import SafetyMonitor
    from tests.fakes import FakeExecutor, FakeQbtClient

    qbt = FakeQbtClient(maindata=[{"rid": 1, "full_update": True, "torrents": {"h1": {"hash": "h1", "category": "auto", "state": "downloading"}}}])
    executor = FakeExecutor()
    monitor = SafetyMonitor(qbt, executor, free_bytes_provider=lambda: 1024**3)
    result = monitor.tick()

    assert result.disk_state == "emergency"
    assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h1"})]
    assert qbt.heavy_calls == []


def test_rclone_upload_worker_verify_failure_does_not_cleanup_and_success_full_allows_delete():
    from qbt_orchestrator.upload import RcloneUploadWorker, UploadJob
    from tests.fakes import FakeExecutor, FakeRclone

    failed = RcloneUploadWorker(FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/A/a.mp4": 99}), FakeExecutor())
    result = failed.run_once(UploadJob(hash="h1", batch_id=1, local="/tmp/a.mp4", remote="gcrypt:/A/a.mp4", size=100, full_torrent=True))
    assert result.state == "verify_pending"
    assert failed.executor.posts == []

    executor = FakeExecutor()
    ok = RcloneUploadWorker(FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/A/a.mp4": 100}), executor)
    result = ok.run_once(UploadJob(hash="h1", batch_id=1, local="/tmp/a.mp4", remote="gcrypt:/A/a.mp4", size=100, full_torrent=True))
    assert result.state == "done"
    assert executor.posts == [("/api/v2/torrents/delete", {"hashes": "h1", "deleteFiles": "true"})]


def test_rclone_upload_worker_directory_manifest_verify_gates_cleanup():
    from qbt_orchestrator.upload import RcloneUploadWorker, UploadJob
    from tests.fakes import FakeExecutor

    class ManifestRclone:
        def __init__(self, listing):
            self.listing = listing
            self.copies = []
            self.copytos = []
        def copy(self, local, remote):
            self.copies.append((local, remote))
            return True
        def copyto(self, local, remote):
            self.copytos.append((local, remote))
            return True
        def lsjson(self, remote, recursive=False):
            return self.listing

    files = [
        {"relative_path": "A.mp4", "size": 100},
        {"relative_path": "extras/B.nfo", "size": 10},
    ]
    executor = FakeExecutor()
    ok = RcloneUploadWorker(
        ManifestRclone([{"Path": "A.mp4", "Size": 100}, {"Path": "extras/B.nfo", "Size": 10}]),
        executor,
    )

    result = ok.run_once(UploadJob(hash="h1", batch_id=1, local="/tmp/ABC", remote="gcrypt:/ABC", size=110, full_torrent=True, files=files))

    assert result.state == "done"
    assert ok.rclone.copies == [("/tmp/ABC", "gcrypt:/ABC")]
    assert ok.rclone.copytos == []
    assert executor.posts == [("/api/v2/torrents/delete", {"hashes": "h1", "deleteFiles": "true"})]

    bad_executor = FakeExecutor()
    bad = RcloneUploadWorker(
        ManifestRclone([{"Path": "A.mp4", "Size": 99}, {"Path": "extras/B.nfo", "Size": 10}]),
        bad_executor,
    )
    bad_result = bad.run_once(UploadJob(hash="h2", batch_id=2, local="/tmp/ABC", remote="gcrypt:/ABC", size=110, full_torrent=True, files=files))

    assert bad_result.state == "verify_pending"
    assert bad_executor.posts == []


def test_rclone_upload_worker_single_file_manifest_uses_copyto():
    from qbt_orchestrator.upload import RcloneUploadWorker, UploadJob
    from tests.fakes import FakeExecutor

    class ManifestRclone:
        def __init__(self):
            self.copytos = []
            self.copies = []
        def copy(self, local, remote):
            self.copies.append((local, remote)); return True
        def copyto(self, local, remote):
            self.copytos.append((local, remote)); return True
        def lsjson(self, remote, recursive=False):
            return [{"Name": "A.mp4", "Size": 100}]

    executor = FakeExecutor()
    worker = RcloneUploadWorker(ManifestRclone(), executor)
    result = worker.run_once(
        UploadJob(
            hash="h1",
            batch_id=1,
            local="/tmp/A.mp4",
            remote="gcrypt:/A/A.mp4",
            size=100,
            full_torrent=True,
            files=[{"relative_path": "A.mp4", "size": 100}],
            copy_mode="copyto",
        )
    )

    assert result.state == "done"
    assert worker.rclone.copytos == [("/tmp/A.mp4", "gcrypt:/A/A.mp4")]
    assert worker.rclone.copies == []
    assert executor.posts == [("/api/v2/torrents/delete", {"hashes": "h1", "deleteFiles": "true"})]


def test_media_pipeline_groups_multi_cd_passthrough_and_emby_precise_refresh():
    from qbt_orchestrator.media import MediaPipeline, UploadedFile
    from tests.fakes import FakeBackfill, FakeEmby, FakeUploadQueue

    emby = FakeEmby()
    pipeline = MediaPipeline(backfill=FakeBackfill(), upload_queue=FakeUploadQueue(), emby=emby, emby_prefix="/media/gcrypt")
    files = [
        UploadedFile(remote_path="gcrypt:/ABC-123/ABC-123-CD1.mp4", size=1024**3, duration_sec=120),
        UploadedFile(remote_path="gcrypt:/ABC-123/ABC-123-CD2.mp4", size=1024**3, duration_sec=120),
    ]
    run = pipeline.handle_upload_verified("manifest-1", files)

    assert run.media_group_key == "ABC-123"
    assert pipeline.backfill.calls == [("ABC-123", "manifest-1")]
    assert pipeline.upload_queue.jobs[0]["job_type"] == "sidecar_upload"
    assert emby.refreshes == [{"Updates": [{"Path": "/media/gcrypt/ABC-123", "UpdateType": "Created"}]}]


def test_sidecar_scraper_guard_blocks_remote_writes():
    from qbt_orchestrator.integrations.gdrive_backfill import ScrapeCommandGuard

    guard = ScrapeCommandGuard(staging_dir="/var/lib/qbt-orchestrator/sidecar-staging")
    assert guard.validate(["/opt/qbt/gdrive-backfill/bin/javinizer_scrape_one.sh", "ABC-123"]).allowed is True
    blocked = guard.validate(["rclone", "copy", "/tmp/a.nfo", "gcrypt:/ABC-123/"])
    assert blocked.allowed is False
    assert blocked.reason == "scraper_io_bypass_blocked"


def test_telegram_auth_approval_and_duplicate_click_idempotency():
    from qbt_orchestrator.telegram_control import ApprovalStore, TelegramAuthorizer

    auth = TelegramAuthorizer(viewers={1}, operators={2}, admins={3})
    assert auth.role_for(1) == "viewer"
    assert auth.allowed(1, "trace") is True
    assert auth.allowed(1, "cleanup") is False
    assert auth.allowed(3, "cleanup") is True
    assert auth.allowed(99, "status") is False

    store = ApprovalStore(now=lambda: 100)
    approval_id = store.create("cleanup", {"hash": "h1"}, ttl=300)
    assert store.approve_once(approval_id, user_id=3) is True
    assert store.approve_once(approval_id, user_id=3) is False


def test_cli_status_trace_migrate_and_events_json():
    from qbt_orchestrator.cli import main
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        assert main(["migrate", "--dry-run", "--state-db", str(db)]) == 0
        assert main(["status", "--state-db", str(db), "--json"]) == 0
        assert main(["events", "--state-db", str(db), "--json"]) == 0
        assert main(["trace", "missing", "--state-db", str(db), "--json"]) == 0
        assert main(["once", "--dry-run", "--state-db", str(db)]) == 0
        assert main(["reconcile", "--dry-run", "--state-db", str(db)]) == 0


def test_deploy_assets_and_traceability_docs_exist():
    root = Path(__file__).resolve().parents[1]
    for rel in [
        "deploy/systemd/qbt-orchestrator-daemon.service",
        "deploy/systemd/qbt-orchestrator-daemon.env.example",
        "deploy/scripts/install-release.sh",
        "deploy/scripts/backup-live.sh",
        "deploy/scripts/rollback.sh",
        "deploy/scripts/run-dry-run.sh",
        "docs/traceability/requirements-map.md",
    ]:
        assert (root / rel).exists(), rel


def test_no_secret_literals_in_tracked_text():
    root = Path(__file__).resolve().parents[1]
    forbidden = ["123456:" + "secret-token", "mag" + "net:?xt=urn:btih:", "Emby" + "ApiKey=", "password=" + "plain"]
    for path in root.rglob("*"):
        if any(part in {".git", ".pytest_cache", "__pycache__", "legacy"} for part in path.parts) or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for needle in forbidden:
            assert needle not in text, f"{needle} leaked in {path}"


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")





