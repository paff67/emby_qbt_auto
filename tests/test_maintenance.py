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


def test_sqlite_maintenance_retention_deletes_old_rows_and_checkpoints_wal():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.maintenance import SQLiteMaintenanceService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        old_ts = 1_000
        new_ts = 20_000
        for table, columns, values in [
            ("events_v2", "ts,level,component,event_type,message,data_json", (old_ts, "info", "old", "old", "old", "{}")),
            ("events_v2", "ts,level,component,event_type,message,data_json", (new_ts, "info", "new", "new", "new", "{}")),
            ("action_log", "ts,action_type,path,payload_json,status,dry_run", (old_ts, "old", "old", "{}", "done", 0)),
            ("action_log", "ts,action_type,path,payload_json,status,dry_run", (new_ts, "new", "new", "{}", "done", 0)),
            ("decision_log", "ts,component,hash,decision,reason_code,data_json", (old_ts, "old", "h", "d", "r", "{}")),
            ("decision_log", "ts,component,hash,decision,reason_code,data_json", (new_ts, "new", "h", "d", "r", "{}")),
            ("metrics_snapshots", "ts,component,metrics_json", (old_ts, "old", "{}")),
            ("metrics_snapshots", "ts,component,metrics_json", (new_ts, "new", "{}")),
        ]:
            placeholders = ",".join("?" for _ in values)
            con.execute(f"insert into {table}({columns}) values({placeholders})", values)
        con.commit()
        con.close()

        service = SQLiteMaintenanceService(
            db,
            now=lambda: new_ts,
            retention_days=0,
            retention_delete_batch_size=2,
            journal_size_limit_bytes=123456,
        )
        result = service.run_once()

        assert result["retention_deleted"] == {
            "events_v2": 1,
            "action_log": 1,
            "decision_log": 1,
            "metrics_snapshots": 1,
        }
        assert result["wal_checkpoint"][0] in {0, 1}
        con = sqlite3.connect(db)
        old_counts = {
            table: con.execute(f"select count(*) from {table} where ts=?", (old_ts,)).fetchone()[0]
            for table in ("events_v2", "action_log", "decision_log", "metrics_snapshots")
        }
        new_counts = {
            table: con.execute(f"select count(*) from {table} where ts=?", (new_ts,)).fetchone()[0]
            for table in ("events_v2", "action_log", "decision_log", "metrics_snapshots")
        }
        con.close()
        assert old_counts == {table: 0 for table in old_counts}
        assert new_counts == {table: 1 for table in new_counts}
        assert result["journal_size_limit_bytes"] == 123456


def test_sqlite_maintenance_expires_resource_reservations_without_deleting_audit_rows():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.maintenance import SQLiteMaintenanceService

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
            ("expired", "active_download", 100, "active", 1000, 1099, "test"),
        )
        con.execute(
            "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
            ("live", "active_download", 200, "active", 1000, 1200, "test"),
        )
        con.commit(); con.close()

        result = SQLiteMaintenanceService(db, now=lambda: 1100).run_once()

        assert result["reservations_expired"] == 1
        rows = []
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        try:
            rows = [dict(r) for r in con.execute("select hash,state,released_at,reason from resource_reservations order by hash")]
            event = con.execute("select component,event_type,message from events_v2 where component='reservation'").fetchone()
        finally:
            con.close()
        assert rows == [
            {"hash": "expired", "state": "expired", "released_at": 1100, "reason": "reservation_expired"},
            {"hash": "live", "state": "active", "released_at": None, "reason": "test"},
        ]
        assert tuple(event) == ("reservation", "reservation_expired", "expired 1 resource reservations")


def test_daemon_default_maintenance_loop_runs_retention_not_not_configured():
    from qbt_orchestrator.db import migrate
    from qbt_orchestrator.maintenance import SQLiteMaintenanceService
    from qbt_orchestrator.service import DaemonRuntime
    from tests.test_daemon_runtime import FakeExecutor, FakeQbt

    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "state.sqlite"
        migrate(db, dry_run=False)
        con = sqlite3.connect(db)
        con.execute(
            "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
            (1_000, "info", "old", "old", "old", "{}"),
        )
        con.commit()
        con.close()
        daemon = DaemonRuntime(
            state_db=db,
            qbt=FakeQbt(),
            executor=FakeExecutor(),
            free_bytes_provider=lambda: 6 * 1024**3,
            dry_run=True,
            safety_interval=0,
            maintenance_service=SQLiteMaintenanceService(db, now=lambda: 20_000, retention_days=0),
        )

        daemon.run(max_safety_ticks=1)

        con = sqlite3.connect(db)
        old_count = con.execute("select count(*) from events_v2 where ts=1000").fetchone()[0]
        loop_json = con.execute(
            "select data_json from events_v2 where component='maintenance' and event_type='loop_tick' order by id desc limit 1"
        ).fetchone()[0]
        con.close()
        loop = json.loads(loop_json)
        assert old_count == 0
        assert "not_configured" not in json.dumps(loop)
        assert loop["result"]["retention_deleted"]["events_v2"] >= 1


if __name__ == "__main__":
    inspect = __import__("inspect")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn) and not inspect.signature(fn).parameters:
            fn()
    print("ok")
