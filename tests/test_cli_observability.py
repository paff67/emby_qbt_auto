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


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
