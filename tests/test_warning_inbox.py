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


def test_warning_read_primary_key_is_per_chat_user_and_enforces_warning_fk(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    con.execute("pragma foreign_keys=ON")
    try:
        warning_id = _insert_warning(con)
        con.execute(
            "insert into bot_warning_reads(warning_id,chat_id,user_id,read_at) "
            "values(?,?,?,?)",
            (warning_id, "chat", "user-a", 100),
        )
        con.execute(
            "insert into bot_warning_reads(warning_id,chat_id,user_id,read_at) "
            "values(?,?,?,?)",
            (warning_id, "chat", "user-b", 100),
        )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_warning_reads(warning_id,chat_id,user_id,read_at) "
                "values(?,?,?,?)",
                (warning_id, "chat", "user-a", 101),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_warning_reads(warning_id,chat_id,user_id,read_at) "
                "values(?,?,?,?)",
                (warning_id + 999, "chat", "user-a", 100),
            )

        primary_key = {
            str(row[1]): int(row[5])
            for row in con.execute("pragma table_info(bot_warning_reads)")
            if row[5]
        }
        assert primary_key == {"warning_id": 1, "chat_id": 2, "user_id": 3}
        assert {
            (str(row[2]), str(row[3]), str(row[4]))
            for row in con.execute("pragma foreign_key_list(bot_warning_reads)")
        } == {("bot_warning_inbox", "warning_id", "id")}
    finally:
        con.close()


def test_runtime_writer_enforces_warning_read_foreign_key(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)

    with pytest.raises(sqlite3.IntegrityError):
        write_execute(
            db,
            "insert into bot_warning_reads(warning_id,chat_id,user_id,read_at) "
            "values(?,?,?,?)",
            (999, "chat", "user", 100),
        )


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
