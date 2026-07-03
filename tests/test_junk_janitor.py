#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
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


class RecordingExecutor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


def test_junk_janitor_sets_file_priority_zero_before_quarantine():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.junk_janitor import JunkJanitorService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        movie = root / "ABC-123"
        junk = movie / "最新地址 收藏不迷路.html"
        junk.parent.mkdir(parents=True)
        junk.write_text("ad", encoding="utf-8")
        os.utime(junk, (900, 900))
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = RecordingExecutor()
        janitor = JunkJanitorService(db, executor, managed_root=root, trash_dir=Path(td) / ".trash", dry_run=False, stable_mtime_sec=60, now=lambda: 1000)
        snapshots = {"h1": {"hash": "h1", "name": "ABC-123", "category": "auto", "content_path": str(movie), "dlspeed_bps": 0}}
        files = {"h1": [{"index": 7, "name": "最新地址 收藏不迷路.html", "size": 2, "priority": 1}]}

        result = janitor.reconcile(snapshots, files, sync_healthy=True)

        assert result["set_prio_zero"] == ["h1:7"]
        assert result["quarantined"] == []
        assert junk.exists()
        assert executor.posts == [("/api/v2/torrents/filePrio", {"hash": "h1", "id": "7", "priority": "0"})]
        event = _rows(db, "select hash,file_index,action,reason,qbt_priority from junk_janitor_events")[-1]
        assert event == {"hash": "h1", "file_index": 7, "action": "set_prio_zero", "reason": "hard_junk_priority_not_zero", "qbt_priority": 1}


def test_junk_janitor_quarantines_only_priority_zero_stable_small_hard_junk():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.junk_janitor import JunkJanitorService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        movie = root / "ABC-123"
        junk = movie / "聚 合 全 網 H 直 播.html"
        clean = movie / "readme.txt"
        junk.parent.mkdir(parents=True)
        junk.write_text("ad", encoding="utf-8")
        clean.write_text("notes", encoding="utf-8")
        os.utime(junk, (900, 900))
        os.utime(clean, (900, 900))
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = RecordingExecutor()
        janitor = JunkJanitorService(db, executor, managed_root=root, trash_dir=Path(td) / ".trash", dry_run=False, stable_mtime_sec=60, now=lambda: 1000)
        snapshots = {"h1": {"hash": "h1", "name": "ABC-123", "category": "auto", "content_path": str(movie), "dlspeed_bps": 0}}
        files = {"h1": [
            {"index": 1, "name": "聚 合 全 網 H 直 播.html", "size": 2, "priority": 0},
            {"index": 2, "name": "readme.txt", "size": 5, "priority": 0},
        ]}

        result = janitor.reconcile(snapshots, files, sync_healthy=True)

        moved = Path(td) / ".trash" / "h1" / "聚 合 全 網 H 直 播.html"
        assert result["quarantined"] == [{"hash": "h1", "index": 1, "from": str(junk), "to": str(moved)}]
        assert not junk.exists()
        assert moved.read_text(encoding="utf-8") == "ad"
        assert clean.exists()
        assert executor.posts == []
        events = _rows(db, "select action,reason,path from junk_janitor_events order by id")
        assert events[-1]["action"] == "quarantined"
        rules = _rows(db, "select pattern,pattern_type,confidence,source,hits,enabled from dynamic_junk_rules")
        assert rules and rules[0]["confidence"] == "hard" and rules[0]["source"] == "janitor" and rules[0]["hits"] == 1


def test_junk_janitor_skips_unhealthy_current_batch_large_unstable_and_fast_active():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.junk_janitor import JunkJanitorService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        movie = root / "ABC-123"
        names = ["最新地址.html", "直播.url", "博彩.html", "telegram.html"]
        movie.mkdir(parents=True)
        for n in names:
            p = movie / n
            p.write_text("ad", encoding="utf-8")
            os.utime(p, (990, 990))
        os.utime(movie / "telegram.html", (900, 900))
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into torrent_batches(hash,batch_no,state,indices_json,created_at,updated_at) values('h1',1,'active','[1]',1,1)")
        con.commit(); con.close()
        executor = RecordingExecutor()
        janitor = JunkJanitorService(db, executor, managed_root=root, trash_dir=Path(td) / ".trash", dry_run=False, stable_mtime_sec=60, max_auto_quarantine_bytes=10, now=lambda: 1000)
        snapshots = {"h1": {"hash": "h1", "name": "ABC-123", "category": "auto", "content_path": str(movie), "dlspeed_bps": 3 * 1024**2}}
        files = {"h1": [
            {"index": 1, "name": "最新地址.html", "size": 2, "priority": 0},
            {"index": 2, "name": "直播.url", "size": 20, "priority": 0},
            {"index": 3, "name": "博彩.html", "size": 2, "priority": 0},
            {"index": 4, "name": "telegram.html", "size": 2, "priority": 0},
        ]}

        unhealthy = janitor.reconcile(snapshots, files, sync_healthy=False)
        assert unhealthy["suspended"] is True
        assert executor.posts == []

        healthy = janitor.reconcile(snapshots, files, sync_healthy=True)
        assert healthy["quarantined"] == []
        assert all((movie / n).exists() for n in names)
        reasons = [r["reason"] for r in _rows(db, "select reason from junk_janitor_events where action='skipped' order by id")]
        assert "current_batch" in reasons
        assert "size_over_limit" in reasons
        assert "mtime_unstable" in reasons
        assert "active_fast_download" in reasons


def test_daemon_file_batch_runs_junk_janitor_with_qbt_file_lists():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.junk_janitor import JunkJanitorService
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor

    class Qbt:
        def __init__(self, movie):
            self.movie = str(movie)
            self.file_calls = []
        def get_maindata(self, rid):
            return {"rid": rid + 1, "full_update": True, "torrents": {"h1": {"name": "ABC-123", "category": "auto", "content_path": self.movie, "amount_left": 1, "size": 100, "progress": 0.1}}, "server_state": {}}
        def torrent_files(self, h):
            self.file_calls.append(h)
            return [{"index": 1, "name": "最新地址.html", "size": 2, "priority": 1}]

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        movie = root / "ABC-123"
        junk = movie / "最新地址.html"
        junk.parent.mkdir(parents=True)
        junk.write_text("ad", encoding="utf-8")
        os.utime(junk, (900, 900))
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = Qbt(movie)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=qbt,
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            junk_janitor=JunkJanitorService(db, FakeExecutor(), managed_root=root, trash_dir=Path(td) / ".trash", dry_run=True, stable_mtime_sec=60, now=lambda: 1000),
        )

        daemon.run(max_safety_ticks=1)

        assert qbt.file_calls == ["h1"]
        con = sqlite3.connect(db)
        loop_json = con.execute("select data_json from events_v2 where component='file_batch' and event_type='loop_tick' order by id desc limit 1").fetchone()[0]
        con.close()
        result = json.loads(loop_json)["result"]
        assert result["junk_janitor"]["set_prio_zero"] == ["h1:1"]
        assert result["junk_janitor"]["dry_run"] is True


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
