#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


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


def test_upload_job_runner_claims_job_updates_done_and_verify_pending():
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
        assert repo.get(good)["state"] == "done"
        assert runner.run_next() == bad
        assert repo.get(bad)["state"] == "verify_pending"
        assert repo.get(bad)["last_stderr_tail"] == "remote size mismatch"


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
        assert repo.get(upload_id)["state"] == "done"
        media_job = repo.claim_next("media_pipeline")
        assert media_job is not None
        media_payload = json.loads(media_job["payload_json"])
        assert media_payload["upload_manifest_id"] == "manifest-h1"
        assert media_payload["files"][0]["remote_path"] == "gcrypt:/ABC-123/ABC-123.mp4"

        assert runner.run_next() == sidecar_id
        assert repo.get(sidecar_id)["state"] == "done"


def test_command_processor_executes_safe_commands_and_requires_cleanup_approval():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db)
        commands.insert_command("c1", 100, 2, "pause", {"args": ["h1"]})
        commands.insert_command("c2", 100, 2, "resume", {"args": ["h1"]})
        commands.insert_command("c3", 100, 3, "cleanup", {"args": ["h2"]})
        executor = FakeExecutor()
        processor = CommandProcessor(commands, executor)

        assert processor.run_next() == "c1"
        assert processor.run_next() == "c2"
        assert processor.run_next() == "c3"
        assert executor.posts == [
            ("/api/v2/torrents/stop", {"hashes": "h1"}),
            ("/api/v2/torrents/start", {"hashes": "h1"}),
        ]
        assert commands.get("c3")["state"] == "approval_required"
        assert commands.pending_approvals()[0]["action"] == "cleanup"


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


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")

