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


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
