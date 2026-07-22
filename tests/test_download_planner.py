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


def test_planner_consumes_active_intents_and_owns_one_plan_generation():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from qbt_orchestrator.scheduler_intents import SchedulerIntent, SchedulerIntentRepository
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        intents = SchedulerIntentRepository(db)
        intents.upsert(SchedulerIntent("soak", "probe", "probe", 30, 1_120, {"exposure_bytes": 1}))
        intents.upsert(SchedulerIntent("batch", "protected", "protect_batch", 20, 1_120, {"batch_id": 7}))
        intents.upsert(SchedulerIntent("soak", "expired", "probe", 99, 999, {"exposure_bytes": 1}))
        executor = FakeExecutor()
        planner = DownloadPlanner(
            state_db=db,
            executor=executor,
            dry_run=False,
            active_slots=1,
            disk_floor_bytes=0,
            now=lambda: 1_000,
        )
        snapshots = {
            "regular": {"hash": "regular", "category": "auto", "state": "pausedDL", "amount_left": 1, "size": 2},
            "probe": {"hash": "probe", "category": "auto", "state": "pausedDL", "amount_left": 3, "size": 4},
            "protected": {"hash": "protected", "category": "auto", "state": "downloading", "amount_left": 5, "size": 6},
            "expired": {"hash": "expired", "category": "auto", "state": "pausedDL", "amount_left": 2, "size": 3},
        }

        first = planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)

        assert first.selected_hashes == ["probe"]
        assert ("/api/v2/torrents/start", {"hashes": "probe"}) in executor.posts
        assert all(
            not (path == "/api/v2/torrents/stop" and "protected" in payload["hashes"])
            for path, payload in executor.posts
        )
        allocations = _rows(
            db,
            "select hash,owner,plan_generation from scheduler_allocations order by hash",
        )
        assert allocations
        assert {row["owner"] for row in allocations} == {"central"}
        assert {row["plan_generation"] for row in allocations} == {first.plan_generation}
        assert first.plan_generation == 1

        second = planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=False)
        state = _rows(db, "select current_generation from scheduler_plan_state where id=1")
        assert second.plan_generation == 2
        assert state == [{"current_generation": 2}]


def test_planner_marks_never_seen_swarm_dead_after_no_progress_threshold():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        clock = [1_000]
        planner = DownloadPlanner(
            db,
            FakeExecutor(),
            dry_run=False,
            active_slots=0,
            disk_floor_bytes=0,
            now=lambda: clock[0],
        )
        snapshots = {
            "never": {
                "hash": "never",
                "category": "auto",
                "state": "stoppedDL",
                "amount_left": 10,
                "size": 20,
                "completed": 10,
                "progress": 0.5,
                "num_seeds": 0,
                "num_peers": 0,
            }
        }

        planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)
        first_health = _rows(
            db,
            "select last_swarm_seen_at,no_swarm_since,no_progress_since from torrent_health where hash='never'",
        )[0]
        assert first_health == {
            "last_swarm_seen_at": None,
            "no_swarm_since": 1_000,
            "no_progress_since": None,
        }

        clock[0] = 1_001
        planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)
        clock[0] = 4_602
        planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)

        allocation = _rows(
            db,
            "select desired_state,reason from scheduler_allocations where hash='never'",
        )[0]
        assert allocation == {
            "desired_state": "dead",
            "reason": "health_no_swarm_no_progress",
        }


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


def test_planner_default_full_active_slots_is_five():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, disk_floor_bytes=0)
        snapshots = {
            f"h{i}": {"hash": f"h{i}", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": i + 1, "size": 10, "progress": 0.1}
            for i in range(6)
        }

        result = planner.plan_and_apply(snapshots, free_bytes=100, sync_healthy=True)

        assert result.selected_hashes == ["h0", "h1", "h2", "h3", "h4"]
        assert ("/api/v2/torrents/start", {"hashes": "h0|h1|h2|h3|h4"}) in executor.posts


def test_planner_recovery_mode_uses_emergency_floor_and_prioritizes_nearly_done_small_remaining():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    mib = 1024**2
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(
            state_db=db,
            executor=executor,
            dry_run=False,
            active_slots=5,
            disk_floor_bytes=3 * gib,
            recovery_enabled=True,
            recovery_enter_bytes=3 * gib + 512 * mib,
            emergency_floor_bytes=1 * gib + 512 * mib,
            recovery_margin_bytes=256 * mib,
            recovery_active_slots=4,
            recovery_max_remaining_bytes=1 * gib + 512 * mib,
        )
        snapshots = {
            "near-a": {"hash": "near-a", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 220 * mib, "size": 6 * gib, "completed": 5 * gib, "progress": 0.98, "num_seeds": 2, "num_peers": 4},
            "near-b": {"hash": "near-b", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 380 * mib, "size": 5 * gib, "completed": 4 * gib, "progress": 0.93, "num_seeds": 2, "num_peers": 4},
            "tiny-low-progress": {"hash": "tiny-low-progress", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 64 * mib, "size": 3 * gib, "completed": 128 * mib, "progress": 0.04, "num_seeds": 10, "num_peers": 10},
            "too-big": {"hash": "too-big", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 2 * gib, "size": 8 * gib, "completed": 6 * gib, "progress": 0.99, "num_seeds": 99, "num_peers": 99},
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=2 * gib + 400 * mib,
            sync_healthy=True,
        )

        assert result.mode == "recovery"
        assert result.selected_hashes == ["near-a", "near-b"]
        assert result.budget_bytes == 656 * mib
        assert ("/api/v2/torrents/start", {"hashes": "near-a|near-b"}) in executor.posts
        allocations = _rows(db, "select hash,desired_state,reason,reserved_bytes from scheduler_allocations order by hash")
        by_hash = {r["hash"]: r for r in allocations}
        assert by_hash["near-a"]["reason"] == "recovery_budget_fit"
        assert by_hash["near-b"]["reason"] == "recovery_budget_fit"
        assert by_hash["tiny-low-progress"]["desired_state"] == "soak"
        assert by_hash["too-big"]["reason"] == "recovery_remaining_too_large"


def test_planner_reconciles_scheduler_allocations_for_hashes_absent_from_qbt_sync():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) values('absent','active','active','stable',123,1,'old')"
        )
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,reason) values('absent','active_download',123,'active',1,'old')"
        )
        con.commit(); con.close()

        snapshots = {
            "present": {"hash": "present", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1, "size": 2, "progress": 0.5}
        }
        result = DownloadPlanner(db, FakeExecutor(), dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 100).plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert result.selected_hashes == ["present"]
        assert _rows(db, "select hash from scheduler_allocations where hash='absent'") == []
        released = _rows(db, "select state,released_at,reason from resource_reservations where hash='absent'")[0]
        assert released == {"state": "released", "released_at": 100, "reason": "qbt_absent_reconciled"}
        decisions = _rows(db, "select hash,decision,reason_code from decision_log where hash='absent'")
        assert decisions == [{"hash": "absent", "decision": "allocation_reconciled", "reason_code": "qbt_absent"}]


def test_planner_recovery_demotes_slow_active_into_running_soak_without_stopping_it():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    mib = 1024**2
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,reserved_bytes,allocated_at,reason) "
            "values('slow','active','active','stable',?,?,?)",
            (120 * mib, 900, "budget_fit"),
        )
        con.execute(
            "insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,low_speed_since,active_since,updated_at) "
            "values('slow',900,0,100,100,0.98,1,1,900,900,900)"
        )
        con.commit(); con.close()
        executor = FakeExecutor()
        snapshots = {
            "slow": {"hash": "slow", "category": "auto", "tags": "auto", "state": "stalledDL", "amount_left": 120 * mib, "size": 6 * gib, "completed": 5 * gib, "progress": 0.98, "dlspeed": 0, "num_seeds": 1, "num_peers": 1},
            "next": {"hash": "next", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 300 * mib, "size": 5 * gib, "completed": 4 * gib, "progress": 0.95, "dlspeed": 0, "num_seeds": 2, "num_peers": 2},
        }
        planner = DownloadPlanner(
            db,
            executor,
            dry_run=False,
            active_slots=5,
            disk_floor_bytes=3 * gib,
            recovery_enabled=True,
            recovery_enter_bytes=3 * gib + 512 * mib,
            emergency_floor_bytes=int(1.5 * gib),
            recovery_margin_bytes=256 * mib,
            recovery_active_slots=4,
            recovery_max_remaining_bytes=int(1.5 * gib),
            slow_active_demote_sec=180,
            now=lambda: 1200,
        )

        result = planner.plan_and_apply(snapshots, free_bytes=2 * gib + 400 * mib, sync_healthy=True)

        assert result.mode == "recovery"
        assert result.selected_hashes == ["next"]
        assert ("/api/v2/torrents/stop", {"hashes": "slow"}) not in executor.posts
        assert ("/api/v2/torrents/start", {"hashes": "next"}) in executor.posts
        alloc = _rows(db, "select desired_state,reason from scheduler_allocations where hash='slow'")[0]
        assert alloc == {"desired_state": "soak", "reason": "active_slow_3min_recovery_soak"}
        soak = _rows(db, "select state,cooldown_until,reason from soak_state where hash='slow'")[0]
        assert soak == {"state": "soak_resident", "cooldown_until": None, "reason": "recovery_active_slow"}


def test_planner_resets_active_and_low_speed_timers_when_promoting_from_soak():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,desired_seq_dl,allocated_at,reason) values('again','soak','soak','soak',0,900,'active_slow_3min')")
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
        assert {r["hash"]: (r["desired_state"], r["reason"]) for r in alloc}["slow"] == ("soak", "active_slow_3min")


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
            "slow": ("soak", "active_slow_3min"),
        }

        # The demotion persists its cooldown in soak_state.  A new planner
        # instance must honour it even when SoakQueue is disabled and no
        # cooldown_hashes argument is supplied by the caller.
        executor.posts.clear()
        snapshots["slow"]["state"] = "stoppedDL"
        snapshots["fresh"]["state"] = "downloading"
        next_tick = DownloadPlanner(
            state_db=db,
            executor=executor,
            dry_run=False,
            active_slots=2,
            disk_floor_bytes=0,
            now=lambda: 1215,
        )
        follow_up = next_tick.plan_and_apply(
            snapshots, free_bytes=10, sync_healthy=True
        )

        assert "slow" not in follow_up.selected_hashes
        assert all(
            not (path == "/api/v2/torrents/start" and "slow" in payload["hashes"])
            for path, payload in executor.posts
        )


def test_planner_reads_unexpired_soak_state_cooldown_without_queue_service():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into soak_state(hash,state,cooldown_until,updated_at,reason) "
            "values('cool','soak_cooldown',1300,1000,'test')"
        )
        con.commit()
        con.close()
        executor = FakeExecutor()
        snapshots = {
            "cool": {
                "hash": "cool",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 1,
                "size": 2,
                "progress": 0.5,
            }
        }

        result = DownloadPlanner(
            db,
            executor,
            dry_run=False,
            active_slots=1,
            disk_floor_bytes=0,
            now=lambda: 1200,
        ).plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert result.selected_hashes == []
        assert executor.posts == []
        allocation = _rows(
            db,
            "select desired_state,reason from scheduler_allocations where hash='cool'",
        )[0]
        assert allocation == {"desired_state": "soak_cooldown", "reason": "cooldown"}


def test_planner_demotes_stalled_zero_speed_active_after_three_minutes():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute("insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) values('stalled','active','active','stable',900,'budget_fit')")
        con.execute("insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,low_speed_since,active_since,updated_at) values('stalled',1170,0,100,100,0.9,0,0,1000,1000,1170)")
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=0, now=lambda: 1181)
        snapshots = {
            "stalled": {"hash": "stalled", "name": "stalled", "category": "auto", "tags": "auto", "state": "stalledDL", "amount_left": 1, "size": 2, "progress": 0.9, "dlspeed": 0, "num_seeds": 0, "num_peers": 0},
            "fresh": {"hash": "fresh", "name": "fresh", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 2, "size": 3, "progress": 0.1, "dlspeed": 0, "num_seeds": 5, "num_peers": 5},
        }

        result = planner.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert result.selected_hashes == ["fresh"]
        assert ("/api/v2/torrents/stop", {"hashes": "stalled"}) in executor.posts
        alloc = _rows(db, "select hash,desired_state,reason from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["reason"]) for r in alloc}["stalled"] == ("soak", "active_slow_3min")


def test_planner_keeps_engine_selected_nearly_finished_torrent_resident_when_slow():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    mib = 1024**2
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) "
            "values('nearly-done','active','active','stable',900,'budget_fit')"
        )
        con.execute(
            "insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,"
            "num_seeds,num_peers,low_speed_since,active_since,updated_at) "
            "values('nearly-done',1170,0,6000,6000,0.98,0,1,900,900,1170)"
        )
        con.commit()
        con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(
            state_db=db,
            executor=executor,
            dry_run=False,
            active_slots=1,
            disk_floor_bytes=0,
            slow_active_demote_sec=180,
            finish_resident_max_remaining_bytes=256 * mib,
            now=lambda: 1200,
        )
        snapshots = {
            "nearly-done": {
                "hash": "nearly-done",
                "name": "nearly-done",
                "category": "auto",
                "tags": "auto",
                "state": "stalledDL",
                "amount_left": 80 * mib,
                "size": 6 * 1024**3,
                "progress": 0.98,
                "dlspeed": 0,
                "num_seeds": 0,
                "num_peers": 1,
            }
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=1024**3,
            sync_healthy=True,
            allowed_active_hashes={"nearly-done"},
        )

        assert result.selected_hashes == ["nearly-done"]
        assert executor.posts == []
        assert _rows(
            db,
            "select desired_state,reason from scheduler_allocations where hash='nearly-done'",
        ) == [{"desired_state": "active", "reason": "budget_fit"}]


def test_planner_demotes_nearly_finished_torrent_after_resident_stall_limit():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    mib = 1024**2
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) "
            "values('stuck-finish','active','active','stable',100,'budget_fit')"
        )
        con.execute(
            "insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,"
            "num_seeds,num_peers,low_speed_since,no_progress_since,active_since,updated_at) "
            "values('stuck-finish',3900,0,6000,6000,0.98,0,1,100,100,100,3900)"
        )
        con.commit()
        con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(
            state_db=db,
            executor=executor,
            dry_run=False,
            active_slots=1,
            disk_floor_bytes=0,
            slow_active_demote_sec=180,
            finish_resident_max_remaining_bytes=256 * mib,
            finish_resident_max_stall_sec=1_800,
            now=lambda: 4_000,
        )
        snapshots = {
            "stuck-finish": {
                "hash": "stuck-finish",
                "name": "stuck-finish",
                "category": "auto",
                "tags": "auto",
                "state": "stalledDL",
                "amount_left": 8 * mib,
                "size": 6 * 1024**3,
                "progress": 0.999,
                "availability": 0.996,
                "dlspeed": 0,
                "num_seeds": 0,
                "num_peers": 1,
            }
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=1024**3,
            sync_healthy=True,
            allowed_active_hashes={"stuck-finish"},
        )

        assert result.selected_hashes == []
        assert ("/api/v2/torrents/stop", {"hashes": "stuck-finish"}) in executor.posts


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
        con.execute("insert into torrent_health(hash,sampled_at,dlspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,last_swarm_seen_at,no_swarm_since,no_progress_since,soak_since,updated_at) values('deadish',1000,0,100,100,0.2,0,0,1000,1000,1000,1000,1000)")
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


def test_planner_applies_sequential_download_true_for_eligible_active_torrents():
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
            "hot": {
                "hash": "hot",
                "category": "auto",
                "state": "pausedDL",
                "amount_left": 1,
                "size": 2,
                "progress": 0.1,
                "num_seeds": 3,
                "num_peers": 5,
                "stalled_seconds": 0,
            },
        }

        planner.plan_and_apply(snapshots, free_bytes=10, sync_healthy=True)

        assert executor.seq == [("hot", True)]
        actions = _rows(db, "select path,status,dry_run,payload_json from action_log where path='/api/v2/torrents/toggleSequentialDownload'")
        assert len(actions) == 1
        assert actions[0]["status"] == "succeeded"
        assert actions[0]["dry_run"] == 0
        assert json.loads(actions[0]["payload_json"]) == {"hashes": "hot", "desired": True}


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


def test_planner_subtracts_active_resource_reservations_from_budget():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
            ("other", "active_download", 3 * gib, "active", 900, 2000, "existing_download_budget"),
        )
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=2 * gib, now=lambda: 1000)
        snapshots = {
            "newbig": {
                "hash": "newbig",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 4 * gib,
                "size": 8 * gib,
                "progress": 0.5,
            }
        }

        result = planner.plan_and_apply(snapshots, free_bytes=8 * gib, sync_healthy=True)

        assert result.budget_bytes == 3 * gib
        assert result.selected_hashes == []
        assert executor.posts == []
        decision = _rows(db, "select hash,decision,reason_code,data_json from decision_log where component='planner' order by id desc limit 1")[0]
        assert decision["hash"] == "newbig"
        assert decision["decision"] == "soak"
        assert decision["reason_code"] == "budget_or_slot_exhausted"


def test_planner_does_not_subtract_current_pinned_inventory_from_df_free():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state,created_at,reason) "
            "values('pinned','cleanup_pending','current_pinned',?,'active',900,'cleanup_wait')",
            (5 * gib,),
        )
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state,created_at,expires_at,reason) "
            "values('future','active_download','future_growth',?,'active',900,2000,'existing')",
            (1 * gib,),
        )
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(db, executor, dry_run=False, active_slots=1, disk_floor_bytes=2 * gib, now=lambda: 1000)
        snapshots = {
            "new": {
                "hash": "new",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 4 * gib,
                "size": 8 * gib,
                "progress": 0.5,
            }
        }

        result = planner.plan_and_apply(snapshots, free_bytes=7 * gib, sync_healthy=True)

        assert result.budget_bytes == 4 * gib
        assert result.selected_hashes == ["new"]
        claim = _rows(
            db,
            "select accounting_class,owner,lease_generation,last_observed_at "
            "from resource_reservations where hash='new' and kind='active_download'",
        )[0]
        assert claim == {
            "accounting_class": "future_growth",
            "owner": "planner",
            "lease_generation": 0,
            "last_observed_at": 1000,
        }


def test_planner_deduplicates_batch_and_active_download_reservations_for_same_hash():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
            ("same", "active_download", 2 * gib, "active", 900, 2000, "existing_download_budget"),
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
            ("same", 1, "batch", 2 * gib, "active", 900, 2000, "batch_pipeline_reserved"),
        )
        con.commit(); con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=2 * gib, now=lambda: 1000)
        snapshots = {
            "new": {
                "hash": "new",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 3 * gib,
                "size": 6 * gib,
                "progress": 0.5,
            },
        }

        result = planner.plan_and_apply(snapshots, free_bytes=7 * gib, sync_healthy=True)

        assert result.budget_bytes == 3 * gib
        assert result.selected_hashes == ["new"]
        assert ("/api/v2/torrents/start", {"hashes": "new"}) in executor.posts


def test_planner_creates_refreshes_and_releases_active_download_reservations():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        snapshots = {
            "h1": {
                "hash": "h1",
                "category": "auto",
                "tags": "auto",
                "state": "pausedDL",
                "amount_left": gib,
                "size": 2 * gib,
                "progress": 0.5,
            }
        }

        first = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=2 * gib, now=lambda: 1000)
        assert first.plan_and_apply(snapshots, free_bytes=5 * gib, sync_healthy=True).selected_hashes == ["h1"]
        rows = _rows(db, "select hash,kind,bytes,state,expires_at,released_at,reason from resource_reservations")
        assert rows == [{
            "hash": "h1",
            "kind": "active_download",
            "bytes": gib,
            "state": "active",
            "expires_at": 1120,
            "released_at": None,
            "reason": "planner_active_download",
        }]

        snapshots["h1"]["amount_left"] = gib // 2
        snapshots["h1"]["state"] = "downloading"
        second = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=1, disk_floor_bytes=2 * gib, now=lambda: 1010)
        assert second.plan_and_apply(snapshots, free_bytes=5 * gib, sync_healthy=True).selected_hashes == ["h1"]
        rows = _rows(db, "select hash,kind,bytes,state,expires_at,released_at,reason from resource_reservations")
        assert rows == [{
            "hash": "h1",
            "kind": "active_download",
            "bytes": gib // 2,
            "state": "active",
            "expires_at": 1130,
            "released_at": None,
            "reason": "planner_active_download",
        }]

        third = DownloadPlanner(state_db=db, executor=executor, dry_run=False, active_slots=0, disk_floor_bytes=2 * gib, now=lambda: 1020)
        assert third.plan_and_apply(snapshots, free_bytes=5 * gib, sync_healthy=True).selected_hashes == []
        rows = _rows(db, "select hash,kind,bytes,state,released_at,reason from resource_reservations")
        assert rows == [{
            "hash": "h1",
            "kind": "active_download",
            "bytes": gib // 2,
            "state": "released",
            "released_at": 1020,
            "reason": "planner_reallocated",
        }]



def test_planner_does_not_pause_protected_resident_soak():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(db, executor, dry_run=False, active_slots=1, disk_floor_bytes=0)
        snapshots = {
            "active": {"hash": "active", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1, "size": 10, "progress": 0.1},
            "soak": {"hash": "soak", "category": "auto", "tags": "auto", "state": "downloading", "amount_left": 9, "size": 10, "progress": 0.1},
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=100,
            sync_healthy=True,
            protected_running_hashes={"soak"},
        )

        assert result.selected_hashes == ["active"]
        assert ("/api/v2/torrents/stop", {"hashes": "soak"}) not in executor.posts
        assert ("/api/v2/torrents/start", {"hashes": "active"}) in executor.posts


def test_planner_excludes_cooldown_hash_from_full_active_selection():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(db, executor, dry_run=False, active_slots=1, disk_floor_bytes=0)
        snapshots = {
            "cool": {"hash": "cool", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 1, "size": 10, "progress": 0.1},
            "fresh": {"hash": "fresh", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": 2, "size": 10, "progress": 0.1},
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=100,
            sync_healthy=True,
            cooldown_hashes={"cool"},
        )

        assert result.selected_hashes == ["fresh"]
        alloc = _rows(db, "select hash,desired_state,reason from scheduler_allocations order by hash")
        assert {r["hash"]: (r["desired_state"], r["reason"]) for r in alloc}["cool"] == ("soak_cooldown", "cooldown")


def test_planner_subtracts_external_soak_reservations_from_full_active_budget():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    gib = 1024**3
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(db, executor, dry_run=False, active_slots=1, disk_floor_bytes=2 * gib)
        snapshots = {
            "h1": {"hash": "h1", "category": "auto", "tags": "auto", "state": "pausedDL", "amount_left": gib, "size": 2 * gib, "progress": 0.5},
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=5 * gib,
            sync_healthy=True,
            external_reserved_bytes=3 * gib,
        )

        assert result.selected_hashes == []
        assert executor.posts == []
        decision = _rows(db, "select decision,reason_code,data_json from decision_log where hash='h1' order by id desc limit 1")[0]
        assert decision["decision"] == "soak"
        assert decision["reason_code"] == "budget_or_slot_exhausted"
        assert json.loads(decision["data_json"])["external_reserved_bytes"] == 3 * gib


def test_planner_uses_at_most_two_write_transactions_for_one_tick():
    from qbt_orchestrator.db import db_actor_metrics, migrate, stop_write_actors
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        stop_write_actors()
        planner = DownloadPlanner(db, FakeExecutor(), dry_run=False, active_slots=0, disk_floor_bytes=0, now=lambda: 1000)
        snapshots = {
            f"h{index:03d}": {
                "hash": f"h{index:03d}",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": index + 1,
                "size": 1000,
                "progress": 0.1,
            }
            for index in range(100)
        }
        before = db_actor_metrics(db)["writes_completed"]

        planner.plan_and_apply(snapshots, free_bytes=100_000, sync_healthy=True)

        written = db_actor_metrics(db)["writes_completed"] - before
        assert written <= 2
        assert len(_rows(db, "select hash from scheduler_allocations")) == 100
        assert len(_rows(db, "select hash from torrent_health")) == 100
        assert len(_rows(db, "select hash from decision_log where component='planner'")) == 100


def test_planner_allowed_active_hashes_prevents_legacy_slot_backfill():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = FakeExecutor()
        planner = DownloadPlanner(db, executor, dry_run=False, active_slots=2, disk_floor_bytes=0, now=lambda: 100)
        snapshots = {
            "a": {"hash": "a", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 1, "size": 10, "progress": 0.1},
            "b": {"hash": "b", "category": "auto", "tags": "auto", "state": "stoppedDL", "amount_left": 2, "size": 10, "progress": 0.1},
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=100,
            sync_healthy=True,
            allowed_active_hashes={"b"},
        )

        assert result.selected_hashes == ["b"]
        assert executor.posts == [("/api/v2/torrents/start", {"hashes": "b"})]
        rows = _rows(db, "select hash,desired_state,reason from scheduler_allocations order by hash")
        assert rows == [
            {"hash": "a", "desired_state": "soak", "reason": "global_scheduler_not_selected"},
            {"hash": "b", "desired_state": "active", "reason": "budget_fit"},
        ]


def test_planner_does_not_let_expired_carousel_dead_override_live_selection():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,allocated_at,reason) "
            "values('nearly-done','soak_cooldown','soak_cooldown','soak_cooldown',10,'legacy_cooldown')"
        )
        con.execute(
            "insert into carousel_state(hash,state,last_probe_at,backoff_until,backoff_level,updated_at) "
            "values('nearly-done','dead',10,20,1,10)"
        )
        con.commit()
        con.close()
        executor = FakeExecutor()
        planner = DownloadPlanner(
            db,
            executor,
            dry_run=False,
            active_slots=1,
            disk_floor_bytes=0,
            now=lambda: 100,
        )
        snapshots = {
            "nearly-done": {
                "hash": "nearly-done",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 8 * 1024**2,
                "size": 6 * 1024**3,
                "progress": 0.99,
                "num_seeds": 0,
                "num_peers": 1,
            }
        }

        result = planner.plan_and_apply(
            snapshots,
            free_bytes=1024**3,
            sync_healthy=True,
            allowed_active_hashes={"nearly-done"},
        )

        assert result.selected_hashes == ["nearly-done"]
        assert executor.posts == [
            ("/api/v2/torrents/start", {"hashes": "nearly-done"})
        ]
        assert _rows(
            db,
            "select desired_state,owner,reason from scheduler_allocations where hash='nearly-done'",
        ) == [
            {"desired_state": "active", "owner": "central", "reason": "budget_fit"}
        ]


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
