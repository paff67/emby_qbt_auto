#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

GIB = 1024**3
MIB = 1024**2


def test_current_pinned_inventory_is_not_subtracted_from_df_free():
    from qbt_orchestrator.budget import (
        AccountingClass,
        ResourceClaim,
        calculate_growth_budget,
    )

    claims = [
        ResourceClaim("h1", "cleanup_pending", AccountingClass.CURRENT_PINNED, 5 * GIB),
        ResourceClaim("h2", "active_download", AccountingClass.FUTURE_GROWTH, 1 * GIB),
    ]
    budget = calculate_growth_budget(
        free_bytes=10 * GIB,
        emergency_floor_bytes=2 * GIB,
        dynamic_guard_bytes=1 * GIB,
        claims=claims,
    )

    assert budget.available_growth_bytes == 6 * GIB
    assert budget.future_growth_reserved_bytes == 1 * GIB
    assert budget.current_pinned_bytes == 5 * GIB


def test_only_future_active_and_batch_claims_overlap_for_the_same_hash():
    from qbt_orchestrator.budget import (
        AccountingClass,
        ResourceClaim,
        calculate_growth_budget,
        future_growth_by_hash,
    )

    claims = [
        ResourceClaim("same", "active_download", AccountingClass.FUTURE_GROWTH, 4 * GIB),
        ResourceClaim("same", "batch", AccountingClass.FUTURE_GROWTH, 6 * GIB),
        ResourceClaim("same", "soak_probe", AccountingClass.FUTURE_GROWTH, 2 * GIB),
        ResourceClaim("same", "cleanup_pending", AccountingClass.CURRENT_PINNED, 5 * GIB),
        ResourceClaim("same", "batch", AccountingClass.CURRENT_PINNED, 3 * GIB),
    ]

    grouped = future_growth_by_hash(claims)
    budget = calculate_growth_budget(20 * GIB, 1 * GIB, 1 * GIB, claims)

    assert grouped == {"same": 8 * GIB}
    assert budget.future_growth_reserved_bytes == 8 * GIB
    assert budget.current_pinned_bytes == 8 * GIB
    assert budget.available_growth_bytes == 10 * GIB


def test_unscoped_future_claims_never_overlap_each_other():
    from qbt_orchestrator.budget import AccountingClass, ResourceClaim, future_growth_by_hash

    claims = [
        ResourceClaim("", "active_download", AccountingClass.FUTURE_GROWTH, 2 * GIB),
        ResourceClaim("", "batch", AccountingClass.FUTURE_GROWTH, 3 * GIB),
    ]

    assert sum(future_growth_by_hash(claims).values()) == 5 * GIB


def test_dynamic_guard_uses_rate_latency_piece_and_filesystem_slack():
    from qbt_orchestrator.budget import dynamic_guard_bytes

    guard = dynamic_guard_bytes(
        min_guard_bytes=100 * MIB,
        ingress_p99_bps=10 * MIB,
        control_p99_sec=2.5,
        stop_grace_sec=1.5,
        max_piece_size=16 * MIB,
        filesystem_slack_bytes=64 * MIB,
    )

    assert guard == 136 * MIB


def test_dynamic_guard_uses_conservative_rate_when_metrics_are_absent():
    from qbt_orchestrator.budget import dynamic_guard_bytes

    guard = dynamic_guard_bytes(
        min_guard_bytes=1,
        ingress_p99_bps=None,
        control_p99_sec=2,
        stop_grace_sec=1,
        max_piece_size=0,
        filesystem_slack_bytes=0,
        conservative_ingress_bps=7 * MIB,
    )

    assert guard == 21 * MIB


def test_migration_backfills_old_cleanup_pending_claims_as_current_pinned():
    from qbt_orchestrator.db import migrate

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        con = sqlite3.connect(db)
        con.execute(
            "create table resource_reservations("
            "id integer primary key autoincrement, hash text, batch_id integer, kind text, "
            "bytes integer not null, state text default 'active', created_at integer, "
            "expires_at integer, released_at integer, reason text)"
        )
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state) values('pinned','cleanup_pending',100,'active')"
        )
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state) values('future','batch',200,'active')"
        )
        con.commit()
        con.close()

        migrate(db, dry_run=False)

        con = sqlite3.connect(db)
        try:
            columns = {row[1] for row in con.execute("pragma table_info(resource_reservations)")}
            rows = dict(con.execute("select kind,accounting_class from resource_reservations"))
        finally:
            con.close()
        assert {"accounting_class", "owner", "lease_generation", "last_observed_at"} <= columns
        assert rows == {"cleanup_pending": "current_pinned", "batch": "future_growth"}


def test_soak_budget_reader_counts_only_future_growth_claims():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueService
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state,created_at,reason) "
            "values('pinned','cleanup_pending','current_pinned',500,'active',1,'cleanup_wait')"
        )
        con.execute(
            "insert into resource_reservations(hash,kind,accounting_class,bytes,state,created_at,reason) "
            "values('future','batch','future_growth',200,'active',1,'batch')"
        )
        con.commit(); con.close()

        service = SoakQueueService(db, FakeExecutor(), now=lambda: 100)

        assert service._non_soak_reservation_bytes(100) == 200


def test_soak_claim_refresh_increments_generation_and_observation_time():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.soak_queue import SoakQueueService
    from tests.fakes import FakeExecutor

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        service = SoakQueueService(db, FakeExecutor(), now=lambda: 100)

        service._sync_reservations({"soak": 123}, 100)
        service._sync_reservations({"soak": 456}, 110)

        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        try:
            row = dict(
                con.execute(
                    "select accounting_class,owner,bytes,lease_generation,last_observed_at "
                    "from resource_reservations where hash='soak' and kind='soak_probe'"
                ).fetchone()
            )
        finally:
            con.close()
        assert row == {
            "accounting_class": "future_growth",
            "owner": "soak_queue",
            "bytes": 456,
            "lease_generation": 1,
            "last_observed_at": 110,
        }
