from __future__ import annotations

import sqlite3

import pytest

from qbt_orchestrator.db import migrate, readonly_connect


def test_processed_media_schema_and_triggers(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = sqlite3.connect(db)
    con.execute("pragma foreign_keys=ON")
    try:
        tables = {
            str(row[0]) for row in con.execute("select name from sqlite_master where type='table'")
        }
        assert {
            "processed_media",
            "processed_media_aliases",
            "processed_media_events",
        } <= tables

        media_columns = {
            str(row[1]) for row in con.execute("pragma table_info(processed_media)")
        }
        assert {
            "normalized_id",
            "lifecycle_state",
            "download_policy",
            "ingestion_count",
            "manual_delete_requested_at",
            "manually_deleted_at",
            "deletion_manifest",
            "row_version",
        } <= media_columns

        batch_columns = {
            str(row[1]) for row in con.execute("pragma table_info(bot_add_batches)")
        }
        assert "blocked_history_count" in batch_columns

        index_columns = {
            tuple(str(column[2]) for column in con.execute(f'pragma index_info("{row[1]}")'))
            for row in con.execute("pragma index_list(processed_media)")
        }
        assert ("download_policy", "manually_deleted_at", "id") in index_columns
        assert ("lifecycle_state", "updated_at", "id") in index_columns

        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into processed_media("
                "normalized_id,origin,lifecycle_state,download_policy,"
                "first_seen_at,created_at,updated_at) values(?,?,?,?,?,?,?)",
                ("BAD-1", "test", "bogus", "normal", 1, 1, 1),
            )

        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into processed_media("
                "normalized_id,origin,lifecycle_state,download_policy,"
                "first_seen_at,created_at,updated_at) values(?,?,?,?,?,?,?)",
                ("BAD-2", "test", "observed", "block_permanent", 1, 1, 1),
            )

        cur = con.execute(
            "insert into processed_media("
            "normalized_id,origin,lifecycle_state,download_policy,"
            "first_seen_at,manual_delete_requested_at,created_at,updated_at) "
            "values(?,?,?,?,?,?,?,?)",
            ("BBAN-582", "test", "manual_deleted", "block_permanent", 1, 2, 1, 1),
        )
        media_id = int(cur.lastrowid)
        con.execute(
            "insert into processed_media_events("
            "processed_media_id,event_type,event_at,actor_type,payload_json) "
            "values(?,?,?,?,?)",
            (media_id, "manual_deleted", 3, "cli", "{}"),
        )
        event_id = int(con.execute("select id from processed_media_events").fetchone()[0])

        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update processed_media set download_policy='normal' where id=?",
                (media_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("delete from processed_media where id=?", (media_id,))
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update processed_media_events set event_type='mutated' where id=?",
                (event_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("delete from processed_media_events where id=?", (event_id,))

        con.execute(
            "insert into processed_media_aliases("
            "processed_media_id,alias_type,alias_value,alias_sha256,created_at) "
            "values(?,?,?,?,?)",
            (media_id, "normalized_id", "BBAN-582", "a" * 64, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into processed_media_aliases("
                "processed_media_id,alias_type,alias_value,alias_sha256,created_at) "
                "values(?,?,?,?,?)",
                (media_id, "normalized_id", "BBAN-582", "a" * 64, 1),
            )
    finally:
        con.close()


def test_processed_media_migration_idempotent_and_readable(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        count = con.execute("select count(*) from processed_media").fetchone()[0]
        assert int(count) == 0
        version = con.execute(
            "select name from schema_migrations where version=21"
        ).fetchone()
        assert version is not None
        assert version[0] == "processed_media_tombstone_ledger_v1"
    finally:
        con.close()
