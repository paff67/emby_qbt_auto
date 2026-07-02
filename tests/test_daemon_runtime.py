#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


class FakeQbt:
    def __init__(self):
        self.rids = []

    def get_maindata(self, rid):
        self.rids.append(rid)
        return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}


class FakeExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


def test_daemon_runtime_runs_safety_ticks_and_persists_disk_state():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = FakeQbt()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
        )
        daemon.run(max_safety_ticks=2)

        assert qbt.rids == [0, 1]
        con = sqlite3.connect(db)
        row = con.execute("select pressure_state, free_bytes from disk_state where id=1").fetchone()
        event_count = con.execute("select count(*) from events_v2 where component='daemon' and event_type='safety_tick'").fetchone()[0]
        con.close()
        assert row == ("ok", 6 * 1024**3)
        assert event_count == 2


def test_daemon_runtime_emergency_tick_pauses_managed_downloads():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class ManagedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {"rid": rid + 1, "full_update": True, "torrents": {"h1": {"name": "a", "category": "auto", "state": "downloading"}}, "server_state": {}}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=ManagedQbt(),
            executor=executor,
            free_bytes_provider=lambda: 1 * 1024**3,
            dry_run=False,
            safety_interval=0,
        )
        daemon.run(max_safety_ticks=1)

        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h1"})]


class FakeTelegramService:
    def __init__(self, fail_once=False):
        self.calls = 0
        self.fail_once = fail_once

    def poll_once(self):
        self.calls += 1
        if self.fail_once and self.calls == 1:
            raise RuntimeError("telegram boom")
        return 1


def test_telegram_supervisor_polls_and_survives_crash():
    from qbt_orchestrator.service import TelegramSupervisor

    service = FakeTelegramService(fail_once=True)
    supervisor = TelegramSupervisor(service, interval=0, max_backoff=0)

    assert supervisor.poll_once_supervised() == 0
    assert supervisor.poll_once_supervised() == 1
    assert service.calls == 2
    assert supervisor.consecutive_failures == 0


def test_daemon_runtime_starts_optional_telegram_supervisor():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, TelegramSupervisor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        telegram_service = FakeTelegramService()
        supervisor = TelegramSupervisor(telegram_service, interval=0, max_backoff=0)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            telegram_supervisor=supervisor,
        )

        daemon.run(max_safety_ticks=2)

        assert telegram_service.calls >= 1


def test_build_telegram_supervisor_from_env_requires_token_and_parses_roles():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import build_telegram_supervisor_from_env

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        assert build_telegram_supervisor_from_env(db, env={}) is None

        made = {}

        class FakeApi:
            def __init__(self, token):
                made["token"] = token
            def get_updates(self, offset, timeout):
                return []
            def send_message(self, chat_id, text, reply_markup=None):
                return {"ok": True}

        supervisor = build_telegram_supervisor_from_env(
            db,
            env={
                "QBT_ORCH_TELEGRAM_TOKEN": "123456:abc",
                "QBT_ORCH_TG_VIEWERS": "10",
                "QBT_ORCH_TG_OPERATORS": "20,21",
                "QBT_ORCH_TG_ADMINS": "30",
            },
            api_factory=FakeApi,
        )
        assert supervisor is not None
        assert made["token"] == "123456:abc"
        assert supervisor.service.authorizer.allowed(10, "status")
        assert supervisor.service.authorizer.allowed(20, "pause")
        assert supervisor.service.authorizer.allowed(30, "cleanup")
        assert not supervisor.service.authorizer.allowed(10, "cleanup")


def test_daemon_runtime_processes_queued_bot_commands_after_safety_tick():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotCommandRepository, CommandProcessor
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        commands = BotCommandRepository(db)
        commands.insert_command("c1", 100, 30, "pause", {"args": ["h1"]})
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            command_processor=CommandProcessor(commands, executor),
        )

        daemon.run(max_safety_ticks=1)

        assert executor.posts == [("/api/v2/torrents/stop", {"hashes": "h1"})]
        assert commands.get("c1")["state"] == "done"


def test_daemon_notification_sender_dry_run_does_not_claim_or_send():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository
    from qbt_orchestrator.service import DaemonRuntime

    class Sender:
        def __init__(self):
            self.calls = 0
        def has_pending(self):
            return True
        def send_next(self):
            self.calls += 1

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = BotNotificationRepository(db)
        note_id = repo.enqueue(100, "status", "hello")
        sender = Sender()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            telegram_notification_sender=sender,
            notification_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert sender.calls == 0
        assert repo.get(note_id)["state"] == "queued"
        con = sqlite3.connect(db)
        action = con.execute("select action_type,path,status,dry_run from action_log where action_type='telegram_notify'").fetchone()
        con.close()
        assert action == ("telegram_notify", "bot_notifications", "dry_run", 1)


def test_daemon_notification_sender_live_sends_one_message():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import BotNotificationRepository
    from qbt_orchestrator.service import DaemonRuntime

    class Sender:
        def __init__(self, repo):
            self.repo = repo
            self.calls = 0
        def has_pending(self):
            return self.repo.peek_next() is not None
        def send_next(self):
            self.calls += 1
            row = self.repo.claim_next()
            self.repo.mark_sent(row["id"])
            return row["id"]

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = BotNotificationRepository(db)
        note_id = repo.enqueue(100, "status", "hello")
        sender = Sender(repo)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            telegram_notification_sender=sender,
            notification_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        assert sender.calls == 1
        assert repo.get(note_id)["state"] == "sent"


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []
    def monotonic(self):
        return self.now
    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class CountingLoop:
    def __init__(self, name):
        self.name = name
        self.calls = []
    def __call__(self):
        self.calls.append(self.name)
        return {"loop": self.name, "calls": len(self.calls)}


def test_daemon_runtime_runs_design_multirate_loops_and_records_events():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime, LoopTask

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = FakeClock()
        planner = CountingLoop("planner")
        file_batch = CountingLoop("file_batch")
        maintenance = CountingLoop("maintenance")
        carousel = CountingLoop("carousel")
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=2,
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
            loop_tasks=[
                LoopTask("planner", 15, planner),
                LoopTask("file_batch", 60, file_batch),
                LoopTask("maintenance", 300, maintenance),
                LoopTask("carousel", 1800, carousel),
            ],
        )

        daemon.run(max_safety_ticks=31)

        assert len(planner.calls) == 4  # t=0,16,32,48 with 2s ticks
        assert len(file_batch.calls) == 2  # t=0,60
        assert len(maintenance.calls) == 1
        assert len(carousel.calls) == 1
        con = sqlite3.connect(db)
        events = con.execute("select event_type,count(*) from events_v2 group by event_type").fetchall()
        con.close()
        assert ("loop_tick", 8) in events


def test_daemon_default_planner_loop_uses_sync_cache_and_records_allocations():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        alloc = con.execute("select hash,desired_state,slot_kind from scheduler_allocations").fetchone()
        action = con.execute("select path,status,dry_run from action_log").fetchone()
        loop = con.execute("select data_json from events_v2 where component='planner' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert alloc == ("h1", "active", "stable")
        assert action == ("/api/v2/torrents/start", "dry_run", 1)
        assert "not_configured" not in loop


def test_daemon_live_safety_keeps_planner_dry_run_until_explicitly_enabled():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        assert executor.posts == []
        con = sqlite3.connect(db)
        action = con.execute("select path,status,dry_run from action_log").fetchone()
        con.close()
        assert action == ("/api/v2/torrents/start", "dry_run", 1)


def test_daemon_planner_can_be_explicitly_enabled_for_live_apply():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class PlannerQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=PlannerQbt(),
            executor=executor,
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            planner_dry_run=False,
            safety_interval=0,
        )

        daemon.run(max_safety_ticks=1)

        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "h1"})]


def test_daemon_upload_worker_dry_run_does_not_claim_or_call_rclone():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db)
        job_id = repo.enqueue("h1", 1, "upload", {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True}, priority=1)
        rclone = FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/A/a.mp4": 100})
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            upload_runner=UploadJobRunner(repo, rclone, FakeExecutor()),
            upload_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert rclone.copies == []
        assert repo.get(job_id)["state"] == "queued"
        con = sqlite3.connect(db)
        action = con.execute("select action_type,path,status,dry_run from action_log where action_type='upload_job'").fetchone()
        event = con.execute("select event_type from events_v2 where component='upload' order by id desc limit 1").fetchone()[0]
        con.close()
        assert action == ("upload_job", "torrent_jobs/upload", "dry_run", 1)
        assert event == "upload_dry_run"


def test_daemon_upload_worker_live_processes_job_and_verify_pending():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository, UploadJobRunner
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeRclone

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db)
        job_id = repo.enqueue("h2", 2, "upload", {"local": "/tmp/b.mp4", "remote": "gcrypt:/B/b.mp4", "size": 100, "full_torrent": True}, priority=1)
        rclone = FakeRclone(copy_ok=True, remote_sizes={"gcrypt:/B/b.mp4": 99})
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            upload_runner=UploadJobRunner(repo, rclone, FakeExecutor()),
            upload_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        assert rclone.copies == [("/tmp/b.mp4", "gcrypt:/B/b.mp4")]
        assert repo.get(job_id)["state"] == "verify_pending"
        con = sqlite3.connect(db)
        event = con.execute("select event_type from events_v2 where component='upload' order by id desc limit 1").fetchone()[0]
        con.close()
        assert event == "upload_job_processed"


class RecordingBackfill:
    def __init__(self, result=None):
        self.result = result or {"status": "not_found", "artifacts": []}
        self.calls = []

    def scrape_one(self, media_group_key, manifest_id):
        self.calls.append((media_group_key, manifest_id))
        return self.result


def test_daemon_media_and_emby_workers_dry_run_do_not_claim_or_call_emby():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import EmbyRefreshWorker, MediaPipelineJobRunner, MediaPipelineService
    from qbt_orchestrator.runtime import TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeEmby

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        job_id = repo.enqueue(
            "h1",
            None,
            "media_pipeline",
            {"upload_manifest_id": "m1", "files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 1024**3, "duration_sec": 120}]},
            priority=1,
        )
        con = sqlite3.connect(db)
        con.execute(
            "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
            ("/media/gcrypt/ABC-123", "queued", 900, 1200, "{}", 800, 800),
        )
        con.commit()
        con.close()
        emby = FakeEmby()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_pipeline_runner=MediaPipelineJobRunner(repo, MediaPipelineService(db, RecordingBackfill(), now=lambda: 1000)),
            media_pipeline_dry_run=True,
            emby_refresh_worker=EmbyRefreshWorker(db, emby=emby, now=lambda: 1000),
            emby_refresh_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        assert repo.get(job_id)["state"] == "queued"
        assert emby.refreshes == []
        con = sqlite3.connect(db)
        task_state = con.execute("select state from emby_refresh_tasks").fetchone()[0]
        actions = con.execute("select action_type,path,status,dry_run from action_log where action_type in ('media_pipeline_job','emby_refresh') order by id").fetchall()
        con.close()
        assert task_state == "queued"
        assert actions == [
            ("media_pipeline_job", "torrent_jobs/media_pipeline", "dry_run", 1),
            ("emby_refresh", "/media/gcrypt/ABC-123", "dry_run", 1),
        ]


def test_daemon_media_pipeline_then_emby_refresh_live_path():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.media import EmbyRefreshWorker, MediaPipelineJobRunner, MediaPipelineService
    from qbt_orchestrator.runtime import TorrentJobRepository
    from qbt_orchestrator.service import DaemonRuntime
    from tests.fakes import FakeEmby

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 1000)
        job_id = repo.enqueue(
            "h1",
            None,
            "media_pipeline",
            {"upload_manifest_id": "m1", "files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 1024**3, "duration_sec": 120}]},
            priority=1,
        )
        emby = FakeEmby()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            media_pipeline_runner=MediaPipelineJobRunner(repo, MediaPipelineService(db, RecordingBackfill(), now=lambda: 1000)),
            media_pipeline_dry_run=False,
            emby_refresh_worker=EmbyRefreshWorker(db, emby=emby, now=lambda: 1400),
            emby_refresh_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        assert repo.get(job_id)["state"] == "done"
        assert emby.refreshes == [{"Updates": [{"Path": "/media/gcrypt/ABC-123", "UpdateType": "Created"}]}]
        con = sqlite3.connect(db)
        refresh_state = con.execute("select state from emby_refresh_tasks").fetchone()[0]
        events = con.execute("select component,event_type from events_v2 where component in ('media_pipeline','emby') order by id").fetchall()
        con.close()
        assert refresh_state == "done"
        assert ("media_pipeline", "media_pipeline_job_processed") in events
        assert ("emby", "emby_refresh_processed") in events


def test_daemon_default_file_batch_loop_uses_sync_cache_and_dry_run_records_upload():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class CompletedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "Done", "category": "auto", "tags": "auto", "state": "uploading", "amount_left": 0, "size": 100, "progress": 1.0, "content_path": "/downloads/active/Done"}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=CompletedQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=True,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        jobs = con.execute("select count(*) from torrent_jobs").fetchone()[0]
        action = con.execute("select action_type,status,dry_run from action_log where action_type='enqueue_upload'").fetchone()
        loop = con.execute("select data_json from events_v2 where component='file_batch' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        assert jobs == 0
        assert action == ("enqueue_upload", "dry_run", 1)
        assert "not_configured" not in loop


def test_daemon_file_batch_can_enqueue_when_explicitly_enabled():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    class CompletedQbt(FakeQbt):
        def get_maindata(self, rid):
            self.rids.append(rid)
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h2": {"name": "Done2", "category": "auto", "tags": "auto", "state": "uploading", "amount_left": 0, "size": 100, "progress": 1.0, "content_path": "/downloads/active/Done2"}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=CompletedQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=False,
            safety_interval=0,
            file_batch_dry_run=False,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        job = con.execute("select hash,job_type,state from torrent_jobs").fetchone()
        con.close()
        assert job == ("h2", "upload", "queued")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
