#!/usr/bin/env python3
from __future__ import annotations

import random
import json
import importlib.util
import sqlite3
import tempfile
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

MIB = 1024**2
GIB = 1024**3


def test_scheduler_schema_tracks_intents_and_plan_generation():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)

        con = sqlite3.connect(db)
        allocation_columns = {
            row[1] for row in con.execute("pragma table_info(scheduler_allocations)")
        }
        health_columns = {
            row[1] for row in con.execute("pragma table_info(torrent_health)")
        }
        intent_columns = [
            row[1] for row in con.execute("pragma table_info(scheduler_intents)")
        ]
        plan_state_columns = [
            row[1] for row in con.execute("pragma table_info(scheduler_plan_state)")
        ]
        con.close()

        assert {"owner", "plan_generation"} <= allocation_columns
        assert "no_swarm_since" in health_columns
        assert intent_columns == [
            "component",
            "hash",
            "intent",
            "priority",
            "expires_at",
            "data_json",
        ]
        assert plan_state_columns == ["id", "current_generation", "updated_at"]


def test_scheduler_intent_repository_refreshes_payload_and_filters_expired_rows():
    assert importlib.util.find_spec("qbt_orchestrator.scheduler_intents") is not None
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.scheduler_intents import SchedulerIntent, SchedulerIntentRepository

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        repo = SchedulerIntentRepository(db)

        repo.upsert(
            SchedulerIntent(
                component="soak",
                hash="h1",
                intent="probe",
                priority=30,
                expires_at=200,
                data={"exposure_bytes": 128 * MIB},
            )
        )
        repo.upsert(
            SchedulerIntent(
                component="soak",
                hash="h1",
                intent="probe",
                priority=35,
                expires_at=220,
                data={"exposure_bytes": 256 * MIB},
            )
        )
        repo.upsert(
            SchedulerIntent(
                component="batch",
                hash="h2",
                intent="protect_batch",
                priority=20,
                expires_at=150,
                data={"batch_id": 7},
            )
        )

        active = repo.active(now=151)

        assert [(item.component, item.hash, item.priority) for item in active] == [
            ("soak", "h1", 35)
        ]
        assert active[0].expires_at == 220
        assert active[0].data == {"exposure_bytes": 256 * MIB}
        assert repo.active(now=220) == []


def test_migration_backfills_active_batch_lease_as_protect_intent():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into torrent_batches(id,hash,batch_no,state,mode,indices_json,created_at,updated_at) "
            "values(1,'legacy-batch',1,'downloading','pipeline','[0]',1,1)"
        )
        con.execute(
            "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) "
            "values('legacy-batch',1,'batch',100,'active',1,null,'legacy')"
        )
        con.execute("delete from scheduler_intents")
        con.commit()
        con.close()

        migrate(db, dry_run=False)

        con = sqlite3.connect(db)
        row = con.execute(
            "select component,hash,intent,priority,expires_at,data_json from scheduler_intents"
        ).fetchone()
        con.close()
        assert row == (
            "batch",
            "legacy-batch",
            "protect_batch",
            20,
            None,
            '{"batch_id":1}',
        )


def _item(item_id, torrent_hash, kind, growth, *, release=0, priority=0, hold=False, probability=0.5):
    from qbt_orchestrator.work_items import WorkItem

    return WorkItem(
        id=item_id,
        hash=torrent_hash,
        kind=kind,
        incremental_growth_bytes=growth,
        releasable_bytes=release,
        pinned_after_success_bytes=0,
        completion_probability=probability,
        throughput_bps=0,
        wait_age_sec=0,
        operator_priority=priority,
        hold=hold,
    )


def test_drain_selects_finish_and_release_work_not_probe_or_batch():
    from qbt_orchestrator.scheduler_engine import SchedulerEngine
    from qbt_orchestrator.work_items import WorkKind

    items = [
        _item("finish", "a", WorkKind.FULL_FINISH, 300 * MIB, release=6 * GIB, probability=0.8),
        _item("probe", "b", WorkKind.SOAK_PROBE, 128 * MIB, probability=0.9),
        _item("batch", "c", WorkKind.BATCH_DELIVERY, 200 * MIB, probability=0.9),
    ]

    plan = SchedulerEngine(unit_bytes=64 * MIB).select(
        items,
        mode="drain",
        available_growth_bytes=400 * MIB,
        max_slots=2,
    )

    assert [item.id for item in plan.selected] == ["finish"]
    assert plan.rejection_counts == {"mode_disallowed": 2}


def test_hold_is_never_selected_automatically_and_emergency_selects_nothing():
    from qbt_orchestrator.scheduler_engine import SchedulerEngine
    from qbt_orchestrator.work_items import WorkKind

    held = _item("held", "h", WorkKind.FULL_FINISH, 1, release=10 * GIB, priority=100, hold=True)
    normal = SchedulerEngine().select([held], "normal", 10 * GIB, 5)
    emergency = SchedulerEngine().select(
        [_item("safe", "s", WorkKind.FULL_FINISH, 1, release=10 * GIB)],
        "emergency",
        10 * GIB,
        5,
    )

    assert normal.selected == []
    assert normal.rejection_counts == {"hold": 1}
    assert emergency.selected == []
    assert emergency.rejection_counts == {"mode_disallowed": 1}


def test_capacity_rounding_is_conservative_and_never_exceeds_real_budget():
    from qbt_orchestrator.scheduler_engine import SchedulerEngine
    from qbt_orchestrator.work_items import WorkKind

    items = [
        _item("a", "a", WorkKind.FULL_FINISH, 65 * MIB, priority=1),
        _item("b", "b", WorkKind.FULL_FINISH, 65 * MIB, priority=1),
    ]

    plan = SchedulerEngine(unit_bytes=64 * MIB).select(items, "normal", 129 * MIB, 2)

    assert sum(item.incremental_growth_bytes for item in plan.selected) <= 129 * MIB
    assert len(plan.selected) == 1


def test_bounded_dp_prefers_higher_combined_utility_over_single_greedy_item():
    from qbt_orchestrator.scheduler_engine import SchedulerEngine
    from qbt_orchestrator.work_items import WorkKind

    items = [
        _item("large", "z", WorkKind.FULL_FINISH, 2 * 64 * MIB, priority=3),
        _item("small-a", "a", WorkKind.FULL_FINISH, 64 * MIB, priority=2),
        _item("small-b", "b", WorkKind.FULL_FINISH, 64 * MIB, priority=2),
    ]

    plan = SchedulerEngine(unit_bytes=64 * MIB).select(items, "normal", 2 * 64 * MIB, 2)

    assert [item.id for item in plan.selected] == ["small-a", "small-b"]


def test_selection_is_independent_of_input_order_and_ties_use_stable_hash():
    from qbt_orchestrator.scheduler_engine import SchedulerEngine
    from qbt_orchestrator.work_items import WorkKind

    items = [
        _item("one", "b", WorkKind.FULL_FINISH, 64 * MIB),
        _item("two", "a", WorkKind.FULL_FINISH, 64 * MIB),
        _item("three", "c", WorkKind.FULL_FINISH, 64 * MIB),
    ]
    shuffled = list(items)
    random.Random(7).shuffle(shuffled)
    engine = SchedulerEngine(unit_bytes=64 * MIB)

    first = engine.select(items, "normal", 64 * MIB, 1)
    second = engine.select(shuffled, "normal", 64 * MIB, 1)

    assert [item.hash for item in first.selected] == ["a"]
    assert [item.id for item in second.selected] == [item.id for item in first.selected]


def test_full_finish_candidate_uses_piece_uncertainty_and_conservative_release_evidence():
    from qbt_orchestrator.work_items import build_full_finish_work_items

    items = build_full_finish_work_items(
        {
            "releasable": {
                "hash": "releasable",
                "category": "auto",
                "tags": "auto",
                "size": 10 * GIB,
                "amount_left": 3 * GIB,
                "completed": 7 * GIB,
                "piece_size": 16 * MIB,
                "remote_verified": True,
                "cleanup_eventually_permitted": True,
            },
            "unverified": {
                "hash": "unverified",
                "category": "auto",
                "tags": "auto",
                "size": 5 * GIB,
                "amount_left": 2 * GIB,
                "completed": 3 * GIB,
                "piece_size": 8 * MIB,
                "remote_verified": False,
                "cleanup_eventually_permitted": True,
            },
            "held": {
                "hash": "held",
                "category": "auto",
                "tags": "auto,hold",
                "amount_left": GIB,
                "piece_size": 4 * MIB,
            },
        },
        now=1_000,
    )
    by_hash = {item.hash: item for item in items}

    assert by_hash["releasable"].incremental_growth_bytes == 3 * GIB + 32 * MIB
    assert by_hash["releasable"].releasable_bytes == 10 * GIB
    assert by_hash["unverified"].incremental_growth_bytes == 2 * GIB + 16 * MIB
    assert by_hash["unverified"].releasable_bytes == 0
    assert by_hash["held"].hold is True


class _SchedulerQbt:
    def __init__(self):
        self.rids = []

    def get_maindata(self, rid):
        self.rids.append(rid)
        return {
            "rid": rid + 1,
            "full_update": True,
            "torrents": {
                "small": {
                    "hash": "small",
                    "name": "small",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": GIB,
                    "size": 4 * GIB,
                    "progress": 0.75,
                    "dlspeed": 0,
                },
                "fast": {
                    "hash": "fast",
                    "name": "fast",
                    "category": "auto",
                    "tags": "auto",
                    "state": "stoppedDL",
                    "amount_left": 2 * GIB,
                    "size": 5 * GIB,
                    "progress": 0.6,
                    "dlspeed": 50 * MIB,
                },
            },
            "server_state": {},
        }


class _Executor:
    def __init__(self):
        self.posts = []

    def qbt_post(self, path, payload):
        self.posts.append((path, payload))


def test_scheduler_shadow_records_comparison_but_applies_legacy_plan():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = _Executor()
        daemon = DaemonRuntime(
            db,
            _SchedulerQbt(),
            executor,
            free_bytes_provider=lambda: 10 * GIB,
            dry_run=False,
            safety_interval=0,
            planner_dry_run=False,
            planner_active_slots=1,
            disk_floor_bytes=2 * GIB,
            scheduler_engine_mode="shadow",
        )

        daemon.tick_safety()
        result = daemon.planner_tick()

        assert result["planner"]["selected_hashes"] == ["small"]
        assert result["scheduler_engine"]["applied_plan"] == "legacy"
        assert ("/api/v2/torrents/start", {"hashes": "small"}) in executor.posts
        assert ("/api/v2/torrents/start", {"hashes": "fast"}) not in executor.posts
        con = sqlite3.connect(db)
        try:
            row = con.execute(
                "select metrics_json from metrics_snapshots where component='scheduler_engine_shadow' order by id desc limit 1"
            ).fetchone()
        finally:
            con.close()
        assert row is not None
        metrics = json.loads(row[0])
        assert metrics["engine_selected_hashes"] == ["fast"]
        assert metrics["legacy_selected_hashes"] == ["small"]


def test_scheduler_live_applies_engine_selection_once():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        executor = _Executor()
        daemon = DaemonRuntime(
            db,
            _SchedulerQbt(),
            executor,
            free_bytes_provider=lambda: 10 * GIB,
            dry_run=False,
            safety_interval=0,
            planner_dry_run=False,
            planner_active_slots=1,
            disk_floor_bytes=2 * GIB,
            scheduler_engine_mode="live",
        )

        daemon.tick_safety()
        result = daemon.planner_tick()

        assert result["scheduler_engine"]["selected_hashes"] == ["fast"]
        assert result["scheduler_engine"]["applied_plan"] == "engine"
        assert result["planner"]["selected_hashes"] == ["fast"]
        assert executor.posts.count(("/api/v2/torrents/start", {"hashes": "fast"})) == 1
        assert ("/api/v2/torrents/start", {"hashes": "small"}) not in executor.posts


def test_scheduler_budget_subtracts_external_future_claims_but_only_reports_pinned():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.service import DaemonRuntime

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state) "
            "values('future','batch','future_growth',?,'active')",
            (GIB,),
        )
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state) "
            "values('pinned','cleanup_pending','current_pinned',?,'active')",
            (5 * GIB,),
        )
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state) "
            "values('held','active_download','future_growth',?,'active')",
            (2 * GIB,),
        )
        con.commit(); con.close()
        daemon = DaemonRuntime(
            db,
            _SchedulerQbt(),
            _Executor(),
            free_bytes_provider=lambda: 10 * GIB,
            dry_run=True,
            disk_floor_bytes=2 * GIB,
            emergency_floor_bytes=int(1.5 * GIB),
            scheduler_engine_mode="shadow",
        )

        budget = daemon._scheduler_growth_budget(10 * GIB, reallocatable_hashes={"other"})

        assert budget.available_growth_bytes == 5 * GIB
        assert budget.future_growth_reserved_bytes == 3 * GIB
        assert budget.current_pinned_bytes == 5 * GIB
