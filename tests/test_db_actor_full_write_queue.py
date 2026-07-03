#!/usr/bin/env python3
from __future__ import annotations

import ast
import sqlite3
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


def test_runtime_modules_do_not_commit_sqlite_directly_outside_db_actor():
    """All runtime writes must go through qbt_orchestrator.db single-writer helpers.

    Read-only sqlite connections are still allowed, but direct commit calls in
    feature modules bypass the DbActor queue and violate the v2 design.
    """

    src_root = ROOT / "src" / "qbt_orchestrator"
    offenders: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel == "src/qbt_orchestrator/db.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "commit":
                offenders.append(f"{rel}:{node.lineno}")

    assert offenders == []


def test_sync_db_actor_helpers_serialize_writes_and_readonly_connection_is_readonly():
    from qbt_orchestrator.db import migrate, readonly_connect, stop_write_actors, write_execute, write_transaction

    with tempfile.TemporaryDirectory() as td:
        try:
            db = Path(td) / "state.sqlite"
            migrate(db)

            def insert_event(i: int) -> int:
                return write_execute(
                    db,
                    "insert into events_v2(ts,level,component,event_type,message) values(?,?,?,?,?)",
                    (i, "info", "db_actor_test", "write", f"event-{i}"),
                ).lastrowid

            with ThreadPoolExecutor(max_workers=8) as pool:
                ids = list(pool.map(insert_event, range(40)))

            assert sorted(ids) == list(range(1, 41))

            def txn(con):
                con.execute(
                    "insert into action_log(ts,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?)",
                    (999, "db_actor_txn", "test", "{}", "succeeded", 0),
                )
                return con.execute("select count(*) from action_log").fetchone()[0]

            assert write_transaction(db, txn) == 1

            ro = readonly_connect(db)
            try:
                assert ro.execute("select count(*) from events_v2").fetchone()[0] == 40
                try:
                    ro.execute("insert into events_v2(ts,level,component,event_type,message) values(1,'x','x','x','x')")
                except sqlite3.OperationalError as exc:
                    assert "readonly" in str(exc).lower() or "read-only" in str(exc).lower()
                else:  # pragma: no cover - failure path
                    raise AssertionError("readonly connection accepted a write")
            finally:
                ro.close()
        finally:
            stop_write_actors()
