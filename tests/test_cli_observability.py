#!/usr/bin/env python3
from __future__ import annotations

import contextlib
import io
import json
import os
import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


def run_cli(args):
    from qbt_orchestrator.cli import main
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = main(args)
    return rc, buf.getvalue()


def test_cli_trace_reads_events_actions_and_decisions_from_sqlite():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import ObservabilityStore

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        obs = ObservabilityStore(db)
        obs.event("info", "planner", "selected", "selected h1", {"budget": 1}, hash="h1", correlation_id="corr-1")
        obs.action("h1", 42, "qbt_post", "/api/v2/torrents/start", {"hashes": "h1"}, "succeeded", False, correlation_id="corr-1")
        con = sqlite3.connect(db)
        con.execute("insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(1,'planner','h1','active','budget_fit','{}')")
        con.commit(); con.close()

        rc, out = run_cli(["trace", "h1", "--state-db", str(db), "--json"])

        assert rc == 0
        payload = json.loads(out)
        assert payload["target"] == "h1"
        assert payload["events"][0]["event_type"] == "selected"
        assert payload["actions"][0]["path"] == "/api/v2/torrents/start"
        assert payload["decisions"][0]["decision"] == "active"


def test_cli_status_subcommands_are_readonly_views():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into disk_state(id,sampled_at,free_bytes,pressure_state,resume_allowed) values(1,1,123,'watch',1)")
        con.execute("insert into torrent_jobs(hash,job_type,state,priority,payload_json,created_at,updated_at) values('h1','upload','queued',1,'{}',1,1)")
        con.execute("insert into events_v2(ts,level,component,event_type,message,data_json) values(1,'info','daemon','safety_tick','ok','{}')")
        con.commit(); con.close()

        assert json.loads(run_cli(["status", "disk", "--state-db", str(db), "--json"])[1])["pressure_state"] == "watch"
        assert json.loads(run_cli(["status", "queue", "--state-db", str(db), "--json"])[1])["by_state"]["queued"] == 1
        assert json.loads(run_cli(["status", "db", "--state-db", str(db), "--json"])[1])["counts"]["torrent_jobs"] == 1
        assert json.loads(run_cli(["status", "perf", "--state-db", str(db), "--json"])[1])["recent_events"] >= 1


def test_cli_once_dry_run_executes_one_safety_and_planner_tick_without_writes():
    from qbt_orchestrator import cli

    class FakeQbt:
        def get_maindata(self, rid):
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "small", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1}},
                "server_state": {},
            }
        def post(self, path, payload):
            raise AssertionError("dry-run must not post to qBT")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        old_qbt = cli.QbtDockerClient
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        os.environ["QBT_ORCH_DISK_PATH"] = td
        try:
            rc, out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            if old_disk is None:
                os.environ.pop("QBT_ORCH_DISK_PATH", None)
            else:
                os.environ["QBT_ORCH_DISK_PATH"] = old_disk

        assert rc == 0
        assert "once dry-run completed" in out
        con = sqlite3.connect(db)
        alloc = con.execute("select hash,desired_state from scheduler_allocations").fetchone()
        action = con.execute("select path,status,dry_run from action_log").fetchone()
        con.close()
        assert alloc == ("h1", "active")
        assert action == ("/api/v2/torrents/start", "dry_run", 1)


def test_cli_once_wires_upload_worker_in_dry_run_by_default():
    from qbt_orchestrator import cli
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository

    class FakeQbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("dry-run must not post to qBT")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    class FakeRcloneClient:
        calls = []
        def __init__(self, *args, **kwargs):
            pass
        def copyto(self, local, remote):
            self.calls.append((local, remote))
            raise AssertionError("upload dry-run must not call rclone")
        def lsjson_size(self, remote):
            raise AssertionError("upload dry-run must not verify remote")

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        TorrentJobRepository(db).enqueue("h1", 1, "upload", {"local": "/tmp/a.mp4", "remote": "gcrypt:/A/a.mp4", "size": 100, "full_torrent": True}, priority=1)
        old_qbt = cli.QbtDockerClient
        old_rclone = getattr(cli, "RcloneClient", None)
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_upload = os.environ.get("QBT_ORCH_UPLOAD_DRY_RUN")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        cli.RcloneClient = FakeRcloneClient
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ.pop("QBT_ORCH_UPLOAD_DRY_RUN", None)
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            if old_rclone is not None:
                cli.RcloneClient = old_rclone
            if old_disk is None:
                os.environ.pop("QBT_ORCH_DISK_PATH", None)
            else:
                os.environ["QBT_ORCH_DISK_PATH"] = old_disk
            if old_upload is None:
                os.environ.pop("QBT_ORCH_UPLOAD_DRY_RUN", None)
            else:
                os.environ["QBT_ORCH_UPLOAD_DRY_RUN"] = old_upload

        assert rc == 0
        assert FakeRcloneClient.calls == []
        con = sqlite3.connect(db)
        action = con.execute("select action_type,status,dry_run from action_log where action_type='upload_job'").fetchone()
        state = con.execute("select state from torrent_jobs where job_type='upload'").fetchone()[0]
        con.close()
        assert action == ("upload_job", "dry_run", 1)
        assert state == "queued"


def test_cli_wires_io_governor_full_speed_limits_provider_to_rclone_client_by_default():
    from qbt_orchestrator import cli

    class FakeQbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("dry-run must not post to qBT")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    class FakeRcloneClient:
        kwargs = None
        def __init__(self, *args, **kwargs):
            FakeRcloneClient.kwargs = kwargs
        def copyto(self, local, remote):
            raise AssertionError("upload dry-run must not call rclone")
        def lsjson_size(self, remote):
            raise AssertionError("upload dry-run must not verify remote")

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        old_qbt = cli.QbtDockerClient
        old_rclone = getattr(cli, "RcloneClient", None)
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_iowait = os.environ.get("QBT_ORCH_IOWAIT_PERCENT")
        old_io_enabled = os.environ.get("QBT_ORCH_IO_GOVERNOR_ENABLED")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        cli.RcloneClient = FakeRcloneClient
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ["QBT_ORCH_IOWAIT_PERCENT"] = "40"
        os.environ.pop("QBT_ORCH_IO_GOVERNOR_ENABLED", None)
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            if old_rclone is not None:
                cli.RcloneClient = old_rclone
            if old_disk is None:
                os.environ.pop("QBT_ORCH_DISK_PATH", None)
            else:
                os.environ["QBT_ORCH_DISK_PATH"] = old_disk
            if old_iowait is None:
                os.environ.pop("QBT_ORCH_IOWAIT_PERCENT", None)
            else:
                os.environ["QBT_ORCH_IOWAIT_PERCENT"] = old_iowait
            if old_io_enabled is None:
                os.environ.pop("QBT_ORCH_IO_GOVERNOR_ENABLED", None)
            else:
                os.environ["QBT_ORCH_IO_GOVERNOR_ENABLED"] = old_io_enabled

        assert rc == 0
        provider = FakeRcloneClient.kwargs["limits_provider"]
        limits = provider()
        assert limits.transfers == 4
        assert limits.checkers == 8
        assert limits.bwlimit is None
        assert limits.state == "disabled"


def test_cli_wires_sqlite_maintenance_retention_env_to_runtime():
    from qbt_orchestrator import cli

    class FakeQbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("dry-run must not post to qBT")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        old_qbt = cli.QbtDockerClient
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_retention = os.environ.get("QBT_ORCH_RETENTION_DAYS")
        old_batch = os.environ.get("QBT_ORCH_RETENTION_DELETE_BATCH_SIZE")
        old_limit = os.environ.get("QBT_ORCH_SQLITE_JOURNAL_SIZE_LIMIT_BYTES")
        old_guard = os.environ.get("QBT_ORCH_QBT_PREFERENCES_GUARD")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ["QBT_ORCH_RETENTION_DAYS"] = "0"
        os.environ["QBT_ORCH_RETENTION_DELETE_BATCH_SIZE"] = "7"
        os.environ["QBT_ORCH_SQLITE_JOURNAL_SIZE_LIMIT_BYTES"] = "123456"
        os.environ["QBT_ORCH_QBT_PREFERENCES_GUARD"] = "0"
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            for key, old in [
                ("QBT_ORCH_DISK_PATH", old_disk),
                ("QBT_ORCH_RETENTION_DAYS", old_retention),
                ("QBT_ORCH_RETENTION_DELETE_BATCH_SIZE", old_batch),
                ("QBT_ORCH_SQLITE_JOURNAL_SIZE_LIMIT_BYTES", old_limit),
                ("QBT_ORCH_QBT_PREFERENCES_GUARD", old_guard),
            ]:
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old

        assert rc == 0
        con = sqlite3.connect(db)
        row = con.execute(
            "select data_json from events_v2 where component='maintenance' and event_type='loop_tick' order by id desc limit 1"
        ).fetchone()
        con.close()
        assert row is not None
        loop_json = row[0]
        result = json.loads(loop_json)["result"]
        assert result["retention_days"] == 0
        assert result["retention_delete_batch_size"] == 7
        assert result["journal_size_limit_bytes"] == 123456


def test_cli_wires_qbt_preferences_guard_into_maintenance_loop():
    from qbt_orchestrator import cli

    class FakeQbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("dry-run must not post to qBT")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}
        def get_preferences(self):
            return {"preallocate_all": True, "incomplete_files_ext": False}
        def set_preferences(self, prefs):
            raise AssertionError("preference guard default must dry-run")

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        old_qbt = cli.QbtDockerClient
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_guard = os.environ.get("QBT_ORCH_QBT_PREFERENCES_GUARD")
        old_guard_dry = os.environ.get("QBT_ORCH_QBT_PREFERENCES_DRY_RUN")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ["QBT_ORCH_QBT_PREFERENCES_GUARD"] = "1"
        os.environ.pop("QBT_ORCH_QBT_PREFERENCES_DRY_RUN", None)
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            for key, old in [
                ("QBT_ORCH_DISK_PATH", old_disk),
                ("QBT_ORCH_QBT_PREFERENCES_GUARD", old_guard),
                ("QBT_ORCH_QBT_PREFERENCES_DRY_RUN", old_guard_dry),
            ]:
                if old is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old

        assert rc == 0
        con = sqlite3.connect(db)
        loop_json = con.execute(
            "select data_json from events_v2 where component='maintenance' and event_type='loop_tick' order by id desc limit 1"
        ).fetchone()[0]
        action = con.execute("select action_type,status,dry_run from action_log where action_type='qbt_preferences'").fetchone()
        con.close()
        result = json.loads(loop_json)["result"]
        assert result["qbt_preferences"]["would_set"] == {"preallocate_all": False}
        assert result["qbt_preferences"]["drift"]["incomplete_files_ext"]["desired"] is None
        assert action == ("qbt_preferences", "dry_run", 1)


def test_cli_once_wires_file_batch_dry_run_by_default():
    from qbt_orchestrator import cli

    class FakeQbt:
        def get_maindata(self, rid):
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "Done", "category": "auto", "tags": "auto", "state": "uploading", "amount_left": 0, "size": 100, "progress": 1.0, "content_path": "/downloads/active/Done"}},
                "server_state": {},
            }
        def post(self, path, payload):
            raise AssertionError("dry-run must not post")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        old_qbt = cli.QbtDockerClient
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_file_batch = os.environ.get("QBT_ORCH_FILE_BATCH_DRY_RUN")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ.pop("QBT_ORCH_FILE_BATCH_DRY_RUN", None)
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            if old_disk is None:
                os.environ.pop("QBT_ORCH_DISK_PATH", None)
            else:
                os.environ["QBT_ORCH_DISK_PATH"] = old_disk
            if old_file_batch is None:
                os.environ.pop("QBT_ORCH_FILE_BATCH_DRY_RUN", None)
            else:
                os.environ["QBT_ORCH_FILE_BATCH_DRY_RUN"] = old_file_batch

        assert rc == 0
        con = sqlite3.connect(db)
        jobs = con.execute("select count(*) from torrent_jobs").fetchone()[0]
        action = con.execute("select action_type,status,dry_run from action_log where action_type='enqueue_upload'").fetchone()
        con.close()
        assert jobs == 0
        assert action == ("enqueue_upload", "dry_run", 1)


def test_cli_runtime_disables_batch_pipeline_by_default_and_enables_with_env(monkeypatch):
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate

    class Ns:
        config = None
        dry_run = False
        safety_interval = 0

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        monkeypatch.setenv("QBT_ORCH_STATE_DB", str(db))
        monkeypatch.setenv("QBT_ORCH_DRY_RUN", "0")
        monkeypatch.delenv("QBT_ORCH_BATCH_PIPELINE", raising=False)

        runtime, _ = _build_runtime(Ns(), db)

        assert runtime.batch_pipeline_enabled is False

        monkeypatch.setenv("QBT_ORCH_BATCH_PIPELINE", "1")
        runtime, _ = _build_runtime(Ns(), db)

        assert runtime.batch_pipeline_enabled is True


def test_cli_runtime_wires_batch_live_canary_env(monkeypatch):
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate

    class Ns:
        config = None
        dry_run = False
        safety_interval = 0

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        monkeypatch.setenv("QBT_ORCH_STATE_DB", str(db))
        monkeypatch.setenv("QBT_ORCH_DRY_RUN", "0")
        monkeypatch.setenv("QBT_ORCH_BATCH_PIPELINE", "1")
        monkeypatch.setenv("QBT_ORCH_BATCH_LIVE_VERIFY", "1")
        monkeypatch.setenv("QBT_ORCH_BATCH_ALLOW_HASHES", "ABCDEF, 123456")
        monkeypatch.setenv("QBT_ORCH_BATCH_ALLOW_TAG", "batch-canary")
        monkeypatch.setenv("QBT_ORCH_BATCH_MAX_LIVE_BATCH_BYTES_GB", "1.5")
        monkeypatch.setenv("QBT_ORCH_BATCH_MAX_NEW_PER_TICK", "1")

        runtime, _ = _build_runtime(Ns(), db)

        assert runtime.batch_pipeline_enabled is True
        assert runtime.batch_live_verify is True
        assert runtime.batch_allow_hashes == {"abcdef", "123456"}
        assert runtime.batch_allow_tag == "batch-canary"
        assert runtime.batch_max_live_batch_bytes == int(1.5 * 1024**3)
        assert runtime.batch_max_new_per_tick == 1


def test_cli_runtime_enables_background_event_workers_only_for_live_daemon(monkeypatch):
    from qbt_orchestrator.cli import _build_runtime
    from qbt_orchestrator.db import migrate

    class FakeQbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("test should not post to qBT")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    class Ns:
        config = None
        dry_run = False
        safety_interval = 0
        max_safety_ticks = None
        cmd = "daemon"

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        from qbt_orchestrator import cli
        old_qbt = cli.QbtDockerClient
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        monkeypatch.setenv("QBT_ORCH_STATE_DB", str(db))
        monkeypatch.setenv("QBT_ORCH_DRY_RUN", "0")
        monkeypatch.setenv("QBT_ORCH_DISK_PATH", td)
        monkeypatch.delenv("QBT_ORCH_BACKGROUND_EVENT_WORKERS", raising=False)
        try:
            runtime, _ = _build_runtime(Ns(), db)
            assert runtime.background_event_workers is True

            Ns.cmd = "once"
            runtime, _ = _build_runtime(Ns(), db)
            assert runtime.background_event_workers is False

            Ns.cmd = "daemon"
            monkeypatch.setenv("QBT_ORCH_BACKGROUND_EVENT_WORKERS", "0")
            runtime, _ = _build_runtime(Ns(), db)
            assert runtime.background_event_workers is False
        finally:
            cli.QbtDockerClient = old_qbt


def test_cli_once_wires_media_pipeline_and_emby_refresh_dry_run_by_default():
    from qbt_orchestrator import cli
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository

    class FakeQbt:
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("dry-run must not post")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        TorrentJobRepository(db).enqueue(
            "h1",
            None,
            "media_pipeline",
            {"upload_manifest_id": "m1", "files": [{"remote_path": "gcrypt:/ABC-123/ABC-123.mp4", "size": 1024**3, "duration_sec": 120}]},
            priority=1,
        )
        con = sqlite3.connect(db)
        con.execute(
            "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
            ("/media/gcrypt/ABC-123", "queued", 1, 1, "{}", 1, 1),
        )
        con.commit()
        con.close()
        old_qbt = cli.QbtDockerClient
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_media = os.environ.get("QBT_ORCH_MEDIA_PIPELINE_DRY_RUN")
        old_emby = os.environ.get("QBT_ORCH_EMBY_REFRESH_DRY_RUN")
        cli.QbtDockerClient = lambda *a, **kw: FakeQbt()
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ.pop("QBT_ORCH_MEDIA_PIPELINE_DRY_RUN", None)
        os.environ.pop("QBT_ORCH_EMBY_REFRESH_DRY_RUN", None)
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            if old_disk is None:
                os.environ.pop("QBT_ORCH_DISK_PATH", None)
            else:
                os.environ["QBT_ORCH_DISK_PATH"] = old_disk
            if old_media is None:
                os.environ.pop("QBT_ORCH_MEDIA_PIPELINE_DRY_RUN", None)
            else:
                os.environ["QBT_ORCH_MEDIA_PIPELINE_DRY_RUN"] = old_media
            if old_emby is None:
                os.environ.pop("QBT_ORCH_EMBY_REFRESH_DRY_RUN", None)
            else:
                os.environ["QBT_ORCH_EMBY_REFRESH_DRY_RUN"] = old_emby

        assert rc == 0
        con = sqlite3.connect(db)
        actions = con.execute("select action_type,status,dry_run from action_log where action_type in ('media_pipeline_job','emby_refresh') order by id").fetchall()
        media_state = con.execute("select state from torrent_jobs where job_type='media_pipeline'").fetchone()[0]
        refresh_state = con.execute("select state from emby_refresh_tasks").fetchone()[0]
        con.close()
        assert actions == [("media_pipeline_job", "dry_run", 1), ("emby_refresh", "dry_run", 1)]
        assert media_state == "queued"
        assert refresh_state == "queued"


def test_cli_reconcile_dry_run_and_apply_reports_expired_running_jobs():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.runtime import TorrentJobRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = TorrentJobRepository(db, now=lambda: 100)
        job_id = repo.enqueue("h1", None, "upload", {"local": "a", "remote": "b", "size": 1}, priority=1)
        assert repo.claim_next("upload") is not None

        rc1, out1 = run_cli(["reconcile", "--dry-run", "--state-db", str(db), "--json", "--now", "2000"])
        assert rc1 == 0
        assert json.loads(out1)["expired_running"] == 1
        assert repo.get(job_id)["state"] == "running"

        rc2, out2 = run_cli(["reconcile", "--apply", "--state-db", str(db), "--json", "--now", "2000"])
        assert rc2 == 0
        payload = json.loads(out2)
        assert payload["expired_running"] == 1
        assert payload["dry_run"] == 0
        row = repo.get(job_id)
        assert row["state"] == "retry_wait"
        assert row["next_run_at"] == 2060


def test_cli_wires_qbt_api_rate_limit_env_to_client():
    from qbt_orchestrator import cli

    class FakeQbtClient:
        kwargs = None
        def __init__(self, *args, **kwargs):
            FakeQbtClient.kwargs = kwargs
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {}, "server_state": {}}
        def post(self, path, payload):
            raise AssertionError("dry-run must not post")
        def torrent_info(self, hash):
            return {"hash": hash, "seq_dl": False}

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        old_qbt = cli.QbtDockerClient
        old_disk = os.environ.get("QBT_ORCH_DISK_PATH")
        old_rps = os.environ.get("QBT_ORCH_QBT_API_MAX_RPS")
        cli.QbtDockerClient = FakeQbtClient
        os.environ["QBT_ORCH_DISK_PATH"] = td
        os.environ["QBT_ORCH_QBT_API_MAX_RPS"] = "2"
        try:
            rc, _out = run_cli(["once", "--dry-run", "--state-db", str(db)])
        finally:
            cli.QbtDockerClient = old_qbt
            if old_disk is None:
                os.environ.pop("QBT_ORCH_DISK_PATH", None)
            else:
                os.environ["QBT_ORCH_DISK_PATH"] = old_disk
            if old_rps is None:
                os.environ.pop("QBT_ORCH_QBT_API_MAX_RPS", None)
            else:
                os.environ["QBT_ORCH_QBT_API_MAX_RPS"] = old_rps

        assert rc == 0
        assert FakeQbtClient.kwargs["api_max_requests_per_sec"] == 2.0



def test_status_queue_includes_soak_counts():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) values('s1','soak_resident','soak_resident','soak_resident',128,1,'budget_fit')")
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) values('s2','soak_hot','soak_hot','soak_hot',256,1,'hot_promoted')")
        con.execute("insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values('s1','soak_probe',128,'active',1,999,'soak_resident')")
        con.execute("insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values('s2','soak_probe',256,'active',1,999,'soak_resident')")
        con.commit(); con.close()

        payload = json.loads(run_cli(["status", "queue", "--state-db", str(db), "--json"])[1])

        assert payload["scheduler_by_state"]["soak_resident"] == 1
        assert payload["scheduler_by_state"]["soak_hot"] == 1
        assert payload["soak_probe_reserved_bytes"] == 384


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
