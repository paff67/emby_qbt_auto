from __future__ import annotations

import sqlite3

import pytest

from qbt_orchestrator.db import migrate, readonly_connect, write_execute


def _insert_warning(con: sqlite3.Connection, warning_key: str = "warning-1") -> int:
    cursor = con.execute(
        "insert into bot_warning_inbox("
        "warning_key,severity,topic,safe_message,occurrence_count,first_occurred_at,"
        "last_occurred_at,updated_at,resolved) values(?,?,?,?,?,?,?,?,?)",
        (warning_key, "warning", "capacity", "safe message", 1, 100, 100, 100, 0),
    )
    return int(cursor.lastrowid)


def test_warning_schema_covers_identity_occurrence_relations_and_resolution(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        columns = {
            str(row[1]) for row in con.execute("pragma table_info(bot_warning_inbox)")
        }
        assert {
            "id",
            "warning_key",
            "severity",
            "topic",
            "safe_message",
            "related_hash",
            "related_job_id",
            "related_batch_id",
            "related_item_id",
            "occurrence_count",
            "first_occurred_at",
            "last_occurred_at",
            "updated_at",
            "resolved",
            "resolved_at",
            "resolved_by",
        } <= columns
        index_columns = {
            tuple(str(column[2]) for column in con.execute(f'pragma index_info("{row[1]}")'))
            for row in con.execute("pragma index_list(bot_warning_inbox)")
        }
        assert ("resolved", "severity", "last_occurred_at", "id") in index_columns
        assert ("topic", "last_occurred_at", "id") in index_columns
    finally:
        con.close()


def test_warning_reads_table_absent_after_migration(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        tables = {
            str(row[0]) for row in con.execute("select name from sqlite_master where type='table'")
        }
        assert "bot_warning_inbox" in tables
        assert "bot_warning_reads" not in tables
        indexes = {
            str(row[0]) for row in con.execute("select name from sqlite_master where type='index'")
        }
        assert "idx_bot_warning_reads_actor" not in indexes
        versions = {
            int(row[0])
            for row in con.execute("select version from schema_migrations where version in (20,21)")
        }
        assert versions == {20, 21}
    finally:
        con.close()


def test_warning_severity_resolution_and_occurrence_integrity_checks(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    try:
        for index, severity in enumerate(("info", "warning", "error", "critical")):
            con.execute(
                "insert into bot_warning_inbox("
                "warning_key,severity,topic,safe_message,first_occurred_at,"
                "last_occurred_at,updated_at) values(?,?,?,?,?,?,?)",
                (f"severity-{index}", severity, "test", "safe", 100, 100, 100),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_warning_inbox("
                "warning_key,severity,topic,safe_message,first_occurred_at,"
                "last_occurred_at,updated_at) values(?,?,?,?,?,?,?)",
                ("bad-severity", "debug", "test", "safe", 100, 100, 100),
            )
        warning_id = _insert_warning(con, "bad-occurrence")
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update bot_warning_inbox set occurrence_count=0 where id=?", (warning_id,)
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update bot_warning_inbox set resolved=2 where id=?", (warning_id,)
            )
    finally:
        con.close()


def test_runtime_writer_cannot_insert_legacy_warning_reads(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)

    with pytest.raises(sqlite3.OperationalError):
        write_execute(
            db,
            "insert into bot_warning_reads(warning_id,chat_id,user_id,read_at) "
            "values(?,?,?,?)",
            (1, "chat", "user", 100),
        )
