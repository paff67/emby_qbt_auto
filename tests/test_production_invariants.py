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


def _count(db: Path, table: str, where: str = "1=1") -> int:
    con = sqlite3.connect(db)
    try:
        return int(con.execute(f"select count(*) from {table} where {where}").fetchone()[0])
    finally:
        con.close()


def _latest_metrics(db: Path, component: str) -> dict:
    con = sqlite3.connect(db)
    try:
        row = con.execute(
            "select metrics_json from metrics_snapshots where component=? order by id desc limit 1",
            (component,),
        ).fetchone()
        assert row is not None
        return json.loads(row[0])
    finally:
        con.close()


def test_same_decision_is_logged_once_until_transition():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.decision_recorder import DecisionRecorder

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        recorder = DecisionRecorder(db, now=lambda: 100)

        assert recorder.record("planner", "h", "soak", "budget", {"free_bytes": 10}) is True
        assert recorder.record("planner", "h", "soak", "budget", {"free_bytes": 20}) is False
        assert recorder.record("planner", "h", "active", "budget_fit", {"free_bytes": 20}) is True

        assert _count(db, "decision_log") == 2
        assert _count(db, "decision_state") == 1
        con = sqlite3.connect(db)
        try:
            payloads = [json.loads(row[0]) for row in con.execute("select data_json from decision_log order by id")]
        finally:
            con.close()
        assert [payload["free_bytes"] for payload in payloads] == [10, 20]


def test_stable_decision_fingerprint_ignores_only_declared_volatile_fields():
    from qbt_orchestrator.decision_recorder import stable_fingerprint

    first = {
        "mode": "normal",
        "progress": 0.1,
        "free_bytes": 100,
        "budget_bytes": 80,
        "nested": {"b": 2, "a": 1},
    }
    second = {
        "nested": {"a": 1, "b": 2},
        "budget_bytes": 10,
        "free_bytes": 20,
        "progress": 0.9,
        "mode": "normal",
    }
    assert stable_fingerprint(first) == stable_fingerprint(second)
    assert stable_fingerprint({**second, "mode": "drain"}) != stable_fingerprint(first)


def test_unchanged_planner_tick_does_not_append_repeated_decisions():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.planner import DownloadPlanner
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        planner = DownloadPlanner(db, FakeExecutor(), dry_run=True, active_slots=0, disk_floor_bytes=0, now=lambda: 100)
        snapshots = {
            "h1": {
                "hash": "h1",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "amount_left": 100,
                "size": 1000,
                "progress": 0.1,
            }
        }

        planner.plan_and_apply(snapshots, free_bytes=1_000, sync_healthy=True)
        planner.plan_and_apply(snapshots, free_bytes=2_000, sync_healthy=True)

        assert _count(db, "decision_log", "component='planner' and hash='h1'") == 1


def test_file_batch_emits_one_summary_per_loop_and_bounds_sample_hashes():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService

    class InventoryMustNotRun:
        def torrent_files(self, _hash):
            raise AssertionError("global budget gate must run before file inventory")

    gib = 1024**3
    mib = 1024**2
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = FileBatchService(
            db,
            dry_run=True,
            qbt=InventoryMustNotRun(),
            batch_pipeline_enabled=True,
            disk_floor_bytes=3 * gib,
            filesystem_slack_bytes=128 * mib,
            now=lambda: 100,
        )
        snapshots = {
            f"h{index:03d}": {
                "hash": f"h{index:03d}",
                "category": "auto",
                "tags": "auto",
                "state": "stoppedDL",
                "progress": 0.1,
                "amount_left": 1,
            }
            for index in range(100)
        }

        service.sync_completed(snapshots, free_bytes=3 * gib + 120 * mib, sync_healthy=True, scheduler_mode="normal")
        service.sync_completed(snapshots, free_bytes=3 * gib + 121 * mib, sync_healthy=True, scheduler_mode="normal")

        assert _count(db, "decision_log", "component='file_batch'") == 100
        assert _count(db, "metrics_snapshots", "component='file_batch'") == 2
        metrics = _latest_metrics(db, "file_batch")
        assert metrics["global_batch_budget_below_minimum"] == 100
        assert len(metrics["sample_hashes"]) == 3


def test_observe_promotion_logs_unchanged_skips_once_and_aggregates_each_loop():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.observe_promotion import ObservePromotionConfig, ObservePromotionService
    from tests.test_observe_promotion import FakeExecutor, FakeQbt

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = ObservePromotionService(
            db,
            FakeQbt({}),
            FakeExecutor(),
            dry_run=False,
            config=ObservePromotionConfig(max_per_tick=100),
            now=lambda: 100,
        )
        snapshots = {
            f"h{index:03d}": {
                "hash": f"h{index:03d}",
                "tags": "observe",
                "state": "stoppedDL",
                "has_metadata": False,
            }
            for index in range(100)
        }

        service.promote_ready(snapshots, sync_healthy=True)
        service.promote_ready(snapshots, sync_healthy=True)

        assert _count(db, "decision_log", "component='observe_promotion' and hash<>''") == 100
        assert _count(db, "events_v2", "component='observe_promotion' and event_type='skipped'") == 100
        assert _count(db, "metrics_snapshots", "component='observe_promotion'") == 2
        metrics = _latest_metrics(db, "observe_promotion")
        assert metrics["metadata_not_ready"] == 100
        assert len(metrics["sample_hashes"]) == 3


def test_virtual_unchanged_day_keeps_decision_rows_bounded():
    """Exercise the transition algorithm at production tick counts in virtual time."""

    from qbt_orchestrator.db import migrate, write_transaction
    from qbt_orchestrator.decision_recorder import DecisionEntry, DecisionRecorder

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        recorder = DecisionRecorder(db, now=lambda: 0)
        planner_entries = [
            DecisionEntry("planner", f"h{index:03d}", "soak", "budget", {"free_bytes": 10})
            for index in range(100)
        ]
        batch_entry = [DecisionEntry("file_batch", "", "blocked", "budget", {"free_bytes": 10})]

        def simulate(con: sqlite3.Connection) -> None:
            for tick in range(5_760):
                recorder.record_many_in_transaction(con, planner_entries, ts=tick * 15)
            for tick in range(1_440):
                ts = tick * 60
                recorder.record_many_in_transaction(con, batch_entry, ts=ts)
                con.execute(
                    "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                    (ts, "file_batch", '{"budget":100,"sample_hashes":["h000","h001","h002"]}'),
                )

        write_transaction(db, simulate)

        assert _count(db, "decision_log") <= 500
        assert _count(db, "metrics_snapshots") <= 8_000


def test_scheduler_engine_hard_invariants_across_modes_and_orderings():
    import random

    from qbt_orchestrator.scheduler_engine import SchedulerEngine
    from qbt_orchestrator.work_items import WorkItem, WorkKind

    mib = 1024**2
    items = [
        WorkItem(
            id=f"item-{index:03d}",
            hash=f"h{index:03d}",
            kind=[WorkKind.FULL_FINISH, WorkKind.BATCH_DELIVERY, WorkKind.SOAK_PROBE][index % 3],
            incremental_growth_bytes=(1 + index % 8) * 64 * mib,
            releasable_bytes=(index % 5) * 512 * mib if index % 3 == 0 else 0,
            pinned_after_success_bytes=256 * mib if index % 3 == 1 else 0,
            completion_probability=0.1 + (index % 9) / 10,
            throughput_bps=index * 1024,
            wait_age_sec=index * 60,
            operator_priority=index % 4,
            hold=index % 17 == 0,
        )
        for index in range(100)
    ]
    engine = SchedulerEngine(unit_bytes=64 * mib)
    budget = 2 * 1024**3
    for mode in ["emergency", "drain", "normal", "explore"]:
        shuffled = list(items)
        random.Random(100 + len(mode)).shuffle(shuffled)
        first = engine.select(items, mode, budget, 5)
        second = engine.select(shuffled, mode, budget, 5)
        assert [item.id for item in first.selected] == [item.id for item in second.selected]
        assert sum(item.incremental_growth_bytes for item in first.selected) <= budget
        assert all(not item.hold for item in first.selected)
        if mode == "drain":
            assert all(item.kind is WorkKind.FULL_FINISH for item in first.selected)
        if mode == "emergency":
            assert first.selected == []


def test_steady_state_100_torrent_inventory_and_delta_calls_stay_in_budget():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.file_batch import FileBatchService
    from tests.fakes import BudgetedQbtFake

    gib = 1024**3
    clock = [0]
    snapshots = {
        f"h{index:03d}": {
            "hash": f"h{index:03d}",
            "category": "auto",
            "tags": "auto",
            "state": "stoppedDL",
            "amount_left": gib,
            "size": gib,
            "progress": 0.0,
        }
        for index in range(100)
    }
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        qbt = BudgetedQbtFake(snapshots, now=lambda: clock[0])
        rid = 0
        for _ in range(100):
            response = qbt.get_maindata(rid)
            rid = int(response["rid"])
        for minute in range(3):
            clock[0] = minute * 60
            FileBatchService(
                db,
                dry_run=True,
                qbt=qbt,
                batch_inventory_limit=8,
                batch_max_new_per_tick=0,
                disk_floor_bytes=2 * gib,
                now=lambda: clock[0],
            ).sync_completed(snapshots, free_bytes=8 * gib, sync_healthy=True)

        assert qbt.calls_per_minute("torrents/files") <= 8
        assert qbt.calls_per_minute("torrents/properties") <= 8
        assert qbt.delta_ratio >= 0.99
        assert _count(db, "batch_file_claims", "state='active'") == 0
        assert _count(db, "action_log", "action_type like '%delete%'") == 0
