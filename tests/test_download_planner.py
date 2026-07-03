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


def test_planner_resets_active_and_low_speed_timers_when_promoting_from_soak():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,desired_seq_dl,allocated_at,reason) values('again','soak','soak','soak',0,900,'active_slow_5min')")
        con.execute("insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,low_speed_since,active_since,soak_since,updated_at) values('again',900,0,100,100,0.5,2,2,100,100,900,900)")
        con.commit(); con.close()
        executor = FakeExecutor()
        snapshots = {"again": {"hash": "again", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1, "size": 2, "progress": 0.5, "dlspeed": 0, "completed": 100, "num_seeds": 2, "num_peers": 2}}

        first = DownloadPlanner(db, executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 1000)
        assert first.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True).selected_hashes == ["again"]
        health = _rows(db, "select active_since,low_speed_since from torrent_health where hash='again'")[0]
        assert health == {"active_since": 1000, "low_speed_since": 1000}

        snapshots["again"]["state"] = "downloading"
        second = DownloadPlanner(db, executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 1016)
        assert second.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True).selected_hashes == ["again"]
        alloc = _rows(db, "select desired_state,reason from scheduler_allocations where hash='again'")[0]
        assert alloc == {"desired_state": "active", "reason": "budget_fit"}


def test_planner_updates_health_samples_so_slow_active_demotes_on_later_tick():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        snapshots = {
            "slow": {"hash": "slow", "name": "slow", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 1, "size": 2, "progress": 0.2, "dlspeed": 1024, "completed": 100, "num_seeds": 1, "num_peers": 1},
        }
        first = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 1000)
        assert first.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True).selected_hashes == ["slow"]
        health1 = _rows(db, "select hash,low_speed_since,active_since from torrent_health")
        assert health1 == [{"hash": "slow", "low_speed_since": 1000, "active_since": 1000}]

        second = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 1301)
        snapshots["fresh"] = {"hash": "fresh", "name": "fresh", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 2, "size": 3, "progress": 0.1, "dlspeed": 0, "completed": 0, "num_seeds": 5, "num_peers": 5}
        result = second.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert result.selected_hashes == ["fresh"]
        alloc = _rows(db, "select hash,desired_state,reason from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["reason"]) for r in alloc}["slow"] == ("soak", "active_slow_5min")


def test_planner_demotes_slow_active_after_five_minutes_and_selects_replacement():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) values('slow','active','active','stable',900,'budget_fit')")
        con.execute("insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,low_speed_since,active_since,updated_at) values('slow',1190,1024,100,100,0.2,1,1,890,800,1190)")
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 1200)
        snapshots = {
            "slow": {"hash": "slow", "name": "slow", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 1, "size": 2, "progress": 0.2, "dlspeed": 1024, "num_seeds": 1, "num_peers": 1},
            "fresh": {"hash": "fresh", "name": "fresh", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 2, "size": 3, "progress": 0.1, "dlspeed": 0, "num_seeds": 5, "num_peers": 5},
        }

        result = planner.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert result.selected_hashes == ["fresh"]
        assert ("/api/v2/torrents/stop", {"hashes": "slow"}) in executor.posts
        assert ("/api/v2/torrents/start", {"hashes": "fresh"}) in executor.posts
        alloc = _rows(db, "select hash,desired_state,reason from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["reason"]) for r in alloc} == {
            "fresh": ("active", "budget_fit"),
            "slow": ("soak", "active_slow_5min"),
        }


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


def test_planner_marks_soak_dead_after_one_hour_without_swarm_or_progress():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,desired_seq_dl,allocated_at,reason) values('deadish','soak','soak','soak',0,100,'budget_or_slot_exhausted')")
        con.execute("insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,last_swarm_seen_at,no_progress_since,soak_since,updated_at) values('deadish',1000,0,100,100,0.2,0,0,1000,1000,1000,1000)")
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 4601)
        snapshots = {
            "deadish": {"hash": "deadish", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 10, "size": 20, "progress": 0.2, "dlspeed": 0, "completed": 100, "num_seeds": 0, "num_peers": 0},
            "fresh": {"hash": "fresh", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1, "size": 2, "progress": 0.1, "dlspeed": 0, "completed": 0, "num_seeds": 2, "num_peers": 2},
        }

        result = planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)

        assert result.selected_hashes == ["fresh"]
        assert ("/api/v2/torrents/stop", {"hashes": "deadish"}) in executor.posts
        alloc = _rows(db, "select hash,desired_state,slot_kind,desired_seq_dl,reason from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["slot_kind"], r["desired_seq_dl"], r["reason"]) for r in alloc} == {
            "deadish": ("dead", "dead", 0, "health_no_swarm_no_progress"),
            "fresh": ("active", "stable", 0, "budget_fit"),
        }
        decisions = _rows(db, "select hash,decision,reason_code from decision_log where hash='deadish'")
        assert decisions[-1] == {"hash": "deadish", "decision": "dead", "reason_code": "health_no_swarm_no_progress"}


def test_planner_forces_sequential_download_off_for_soak_torrents():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner

    class SeqExecutor:
        def __init__(self):
            self.posts = []
            self.seq = []
        def qbt_post(self, path, payload):
            self.posts.append((path, payload))
        def set_seq_dl(self, hash, desired):
            self.seq.append((hash, desired))
            return True

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = SeqExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=0)
        snapshots = {
            "active": {"hash": "active", "category": "auto", "state": "pausedDL", "amount_left": 1, "size": 2, "progress": 0.1},
            "soak": {"hash": "soak", "category": "auto", "state": "downloading", "amount_left": 2, "size": 3, "progress": 0.1},
        }

        planner.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert executor.seq == [("soak", False)]


def test_planner_does_not_repeat_seq_false_when_allocation_already_records_false():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner

    class SeqExecutor:
        def __init__(self): self.seq = []
        def qbt_post(self, path, payload): pass
        def set_seq_dl(self, hash, desired): self.seq.append((hash, desired)); return True

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,desired_seq_dl,allocated_at,reason) values('soak','soak','soak','soak',0,1,'budget_or_slot_exhausted')")
        con.commit(); con.close()
        executor = SeqExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=0, disk_floor_bytes=0)

        planner.plan_and_apply({"soak": {"hash": "soak", "category": "auto", "state": "downloading", "amount_left": 1, "size": 2, "progress": 0.1}}, free_bytes=10, sync_healthy=True)

        assert executor.seq == []


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
