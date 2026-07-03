#!/usr/bin/env python3
from __future__ import annotations

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


def test_planner_selects_budget_fit_active_and_pauses_unplanned_managed_downloads():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=2, disk_floor_bytes=2 * 1024**3)
        snapshots = {
            "h1": {"hash": "h1", "name": "small", "category": "auto", "tags": "", "state": "pausedDL", "amount_left": 1 * 1024**3, "size": 2 * 1024**3, "progress": 0.5, "num_seeds": 5, "num_peers": 8},
            "h2": {"hash": "h2", "name": "medium", "category": "auto", "tags": "", "state": "stoppedDL", "amount_left": 2 * 1024**3, "size": 4 * 1024**3, "progress": 0.5, "num_seeds": 4, "num_peers": 7},
            "h3": {"hash": "h3", "name": "too-big-running", "category": "auto", "tags": "", "state": "downloading", "amount_left": 3 * 1024**3, "size": 6 * 1024**3, "progress": 0.5, "num_seeds": 4, "num_peers": 7},
            "h4": {"hash": "h4", "name": "hold", "category": "auto", "tags": "hold", "state": "downloading", "amount_left": 1, "size": 1, "progress": 0.1},
        }

        result = planner.plan_and_apply(snapshots, free_bytes=5 * 1024**3, sync_healthy=True)

        assert result.selected_hashes == ["h1", "h2"]
        assert ("/api/v2/torrents/start", {"hashes": "h1|h2"}) in executor.posts
        assert ("/api/v2/torrents/stop", {"hashes": "h3"}) in executor.posts
        assert all("h4" not in payload.get("hashes", "") for _path, payload in executor.posts)
        allocations = _rows(db, "select hash,desired_state,slot_kind,reserved_bytes,desired_seq_dl from scheduler_allocations order by hash")
        assert [(r["hash"], r["desired_state"], r["slot_kind"]) for r in allocations] == [("h1", "active", "stable"), ("h2", "active", "stable"), ("h3", "soak", "soak")]
        assert all(r["desired_seq_dl"] == 0 for r in allocations if r["hash"] == "h3")
        decisions = _rows(db, "select hash,decision,reason_code from decision_log order by id")
        assert ("h1", "active", "budget_fit") in [(r["hash"], r["decision"], r["reason_code"]) for r in decisions]
        assert ("h3", "soak", "budget_or_slot_exhausted") in [(r["hash"], r["decision"], r["reason_code"]) for r in decisions]


def test_planner_only_starts_selected_torrents_that_are_stopped_or_paused():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=3, disk_floor_bytes=0)
        snapshots = {
            "running": {"hash": "running", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 1, "size": 2, "progress": 0.5},
            "stalled": {"hash": "stalled", "category": "auto", "tags": "auto", "state": "stalledDL", "amount_left": 2, "size": 3, "progress": 0.5},
            "paused": {"hash": "paused", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 3, "size": 4, "progress": 0.5},
        }

        result = planner.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert result.selected_hashes == ["running", "stalled", "paused"]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "paused"})]


def test_planner_conservative_mode_does_not_start_or_cleanup_when_sync_unhealthy():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False)
        snapshots = {"h1": {"hash": "h1", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2}}

        result = planner.plan_and_apply(snapshots, free_bytes=10 * 1024**3, sync_healthy=False)

        assert result.conservative is True
        assert result.selected_hashes == []
        assert executor.posts == []
        decisions = _rows(db, "select hash,decision,reason_code from decision_log")
        assert decisions == [{"hash": "h1", "decision": "hold", "reason_code": "sync_unhealthy"}]


def test_planner_dry_run_records_actions_without_qbt_writes():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=True)
        snapshots = {"h1": {"hash": "h1", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2}}

        result = planner.plan_and_apply(snapshots, free_bytes=10 * 1024**3, sync_healthy=True)

        assert result.selected_hashes == ["h1"]
        assert executor.posts == []
        actions = _rows(db, "select action_type,path,status,dry_run from action_log")
        assert actions == [{"action_type": "qbt_post", "path": "/api/v2/torrents/start", "status": "dry_run", "dry_run": 1}]


def test_planner_preserves_existing_dead_allocations_for_carousel():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) values('dead1','dead','dead','dead',1,'no_swarm')"
        )
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=2)
        snapshots = {
            "dead1": {"hash": "dead1", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1, "num_seeds": 0, "num_peers": 0},
            "fresh": {"hash": "fresh", "category": "auto", "state": "stoppedDL", "amount_left": 1, "size": 2, "progress": 0.1, "num_seeds": 5, "num_peers": 5},
        }

        result = planner.plan_and_apply(snapshots, free_bytes=6 * 1024**3, sync_healthy=True)

        assert result.selected_hashes == ["fresh"]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "fresh"})]
        alloc = _rows(db, "select hash,desired_state,slot_kind from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["slot_kind"]) for r in alloc} == {
            "dead1": ("dead", "dead"),
            "fresh": ("active", "stable"),
        }


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("ok")
