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


def _rows(db: Path, sql: str):
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(sql)]
    con.close()
    return rows


def test_torrent_snapshot_preserves_paths_needed_by_orphan_janitor():
    from qbt_orchestrator.models import TorrentSnapshot

    snap = TorrentSnapshot.from_qbt(
        {
            "hash": "h1",
            "name": "Keep",
            "content_path": "/downloads/active/Keep",
            "save_path": "/downloads/active",
        }
    )

    assert snap.content_path == "/downloads/active/Keep"
    assert snap.save_path == "/downloads/active"


def test_orphan_janitor_requires_healthy_sync_and_two_confirmations_before_quarantine():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.orphan_janitor import OrphanJanitorService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        trash = Path(td) / ".trash"
        keep = root / "Keep"
        orphan = root / "Orphan"
        keep.mkdir(parents=True)
        orphan.mkdir(parents=True)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        janitor = OrphanJanitorService(
            state_db=db,
            managed_root=root,
            trash_dir=trash,
            dry_run=True,
            min_age_sec=0,
            min_confirmations=2,
            now=lambda: 100,
        )
        snapshots = {"h1": {"hash": "h1", "content_path": str(keep), "name": "Keep"}}

        first = janitor.reconcile(snapshots, sync_healthy=True)
        second = janitor.reconcile(snapshots, sync_healthy=True)

        assert first["confirmed_orphans"] == []
        assert second["confirmed_orphans"] == [str(orphan)]
        assert orphan.exists()
        rows = _rows(db, "select path,confirmations,state from orphan_candidates order by path")
        assert rows == [{"path": str(orphan), "confirmations": 2, "state": "confirmed"}]
        action = _rows(db, "select action_type,path,status,dry_run from action_log where action_type='orphan_quarantine'")[-1]
        assert action == {"action_type": "orphan_quarantine", "path": str(orphan), "status": "dry_run", "dry_run": 1}


def test_orphan_janitor_live_moves_only_confirmed_orphans_to_trash():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.orphan_janitor import OrphanJanitorService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        trash = Path(td) / ".trash"
        keep = root / "Keep"
        orphan = root / "Orphan"
        keep.mkdir(parents=True)
        orphan.mkdir(parents=True)
        (orphan / "file.txt").write_text("orphan", encoding="utf-8")
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        janitor = OrphanJanitorService(
            state_db=db,
            managed_root=root,
            trash_dir=trash,
            dry_run=False,
            min_age_sec=0,
            min_confirmations=2,
            now=lambda: 100,
        )
        snapshots = {"h1": {"hash": "h1", "content_path": str(keep), "name": "Keep"}}

        janitor.reconcile(snapshots, sync_healthy=True)
        result = janitor.reconcile(snapshots, sync_healthy=True)

        moved = trash / "Orphan"
        assert result["quarantined"] == [{"from": str(orphan), "to": str(moved)}]
        assert keep.exists()
        assert not orphan.exists()
        assert (moved / "file.txt").read_text(encoding="utf-8") == "orphan"


def test_orphan_janitor_suspends_when_sync_unhealthy_without_recording_candidates():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.orphan_janitor import OrphanJanitorService

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        orphan = root / "Orphan"
        orphan.mkdir(parents=True)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        janitor = OrphanJanitorService(db, root, Path(td) / ".trash", dry_run=False, min_age_sec=0, now=lambda: 100)

        result = janitor.reconcile({}, sync_healthy=False)

        assert result["suspended"] is True
        assert orphan.exists()
        assert _rows(db, "select * from orphan_candidates") == []
        event = _rows(db, "select component,event_type from events_v2 order by id desc limit 1")[0]
        assert event == {"component": "orphan_janitor", "event_type": "suspended_unhealthy_sync"}


def test_daemon_maintenance_runs_orphan_janitor_with_current_sync_cache():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.orphan_janitor import OrphanJanitorService
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor

    class Qbt:
        def __init__(self, keep_path):
            self.keep_path = str(keep_path)
        def get_maindata(self, rid):
            return {
                "rid": rid + 1,
                "full_update": True,
                "torrents": {"h1": {"name": "Keep", "category": "auto", "content_path": self.keep_path}},
                "server_state": {},
            }

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "active"
        keep = root / "Keep"
        orphan = root / "Orphan"
        keep.mkdir(parents=True)
        orphan.mkdir(parents=True)
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        janitor = OrphanJanitorService(db, root, Path(td) / ".trash", dry_run=True, min_age_sec=0, min_confirmations=1, now=lambda: 100)
        daemon = DaemonRuntime(
            state_db=db,
            qbt=Qbt(keep),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            orphan_janitor=janitor,
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        loop_json = con.execute(
            "select data_json from events_v2 where component='maintenance' and event_type='loop_tick' order by id desc limit 1"
        ).fetchone()[0]
        con.close()
        result = json.loads(loop_json)["result"]
        assert result["orphan_janitor"]["confirmed_orphans"] == [str(orphan)]


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
