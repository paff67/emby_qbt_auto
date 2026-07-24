from __future__ import annotations

import sqlite3

import pytest

from qbt_orchestrator.db import migrate, readonly_connect


EXPECTED_TABLES = {
    "bot_add_batches",
    "bot_add_shards",
    "bot_add_items",
    "bot_add_events",
    "remote_media_index",
    "bot_warning_inbox",
    "bot_warning_reads",
}

ITEM_COLUMNS = {
    "id",
    "batch_id",
    "source_message_id",
    "source_index",
    "input_kind",
    "raw_input",
    "raw_input_expires_at",
    "redacted_input",
    "input_sha256",
    "canonical_identity",
    "infohash_v1",
    "infohash_v2",
    "display_name",
    "normalized_media_id",
    "total_size",
    "primary_video_size",
    "state",
    "decision",
    "decision_reason",
    "qbt_hash",
    "qbt_precheck_tag",
    "remote_match_json",
    "approval_generation",
    "metadata_probe_attempt",
    "metadata_probe_started_at",
    "metadata_probe_deadline",
    "metadata_next_poll_at",
    "metadata_retry_at",
    "metadata_lease_owner",
    "metadata_lease_generation",
    "approved_by",
    "approved_at",
    "attempts",
    "next_run_at",
    "last_error",
    "created_at",
    "updated_at",
}

ITEM_STATES = {
    "received",
    "invalid",
    "resolving",
    "duplicate_local",
    "waiting_probe_slot",
    "metadata_wait",
    "metadata_retry_wait",
    "metadata_unavailable",
    "prechecking",
    "duplicate_remote",
    "needs_confirmation",
    "ready",
    "enrolling",
    "enrolled",
    "enrolled_hold",
    "failed",
    "cancelled",
}


def _table_names(con: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in con.execute("select name from sqlite_master where type='table'")
    }


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"pragma table_info({table})")}


def _index_columns(con: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    indexes: dict[str, tuple[str, ...]] = {}
    for row in con.execute(f"pragma index_list({table})"):
        name = str(row[1])
        indexes[name] = tuple(
            str(column[2]) for column in con.execute(f'pragma index_info("{name}")')
        )
    return indexes


def _insert_batch(
    con: sqlite3.Connection,
    *,
    batch_key: str,
    state: str = "draft",
    chat_id: str = "chat",
    user_id: str = "user",
) -> int:
    cursor = con.execute(
        "insert into bot_add_batches(batch_key,chat_id,user_id,state,created_at,updated_at) "
        "values(?,?,?,?,?,?)",
        (batch_key, chat_id, user_id, state, 100, 100),
    )
    return int(cursor.lastrowid)


def _insert_item(
    con: sqlite3.Connection,
    batch_id: int,
    *,
    source_message_id: int,
    source_index: int = 0,
    state: str = "received",
    input_kind: str = "magnet",
) -> int:
    cursor = con.execute(
        "insert into bot_add_items("
        "batch_id,source_message_id,source_index,input_kind,redacted_input,input_sha256,"
        "state,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
        (
            batch_id,
            source_message_id,
            source_index,
            input_kind,
            "magnet:[redacted]",
            f"sha-{source_message_id}-{source_index}",
            state,
            100,
            100,
        ),
    )
    return int(cursor.lastrowid)


def test_bot_queue_schema_contains_all_durable_state(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        assert EXPECTED_TABLES <= _table_names(con)
        assert ITEM_COLUMNS <= _columns(con, "bot_add_items")
    finally:
        con.close()


def test_migration_16_is_recorded_once_and_is_idempotent(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        assert [
            str(row[0])
            for row in con.execute(
                "select name from schema_migrations where version=16"
            )
        ] == ["telegram_add_queue_v1"]
        assert con.execute(
            "select count(*) from schema_migrations where version=16"
        ).fetchone()[0] == 1
        assert EXPECTED_TABLES <= _table_names(con)
    finally:
        con.close()


def test_migration_16_upgrades_a_database_marked_at_version_15(tmp_path):
    db = tmp_path / "state.sqlite"
    con = sqlite3.connect(db)
    con.execute(
        "create table schema_migrations("
        "version integer primary key,name text not null,applied_at integer not null)"
    )
    con.execute(
        "insert into schema_migrations(version,name,applied_at) values(15,?,?)",
        ("shared_capacity_assessment_v1", 100),
    )
    con.commit()
    con.close()

    migrate(db)
    con = readonly_connect(db)
    try:
        assert EXPECTED_TABLES <= _table_names(con)
        assert con.execute(
            "select count(*) from schema_migrations where version=16"
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_batch_and_shard_state_checks_and_open_draft_uniqueness(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    try:
        for index, state in enumerate(
            (
                "draft",
                "queued",
                "processing",
                "awaiting_confirmation",
                "complete",
                "cancelled",
                "draft_expired",
            )
        ):
            _insert_batch(
                con,
                batch_key=f"batch-{state}",
                state=state,
                chat_id=f"chat-{index}",
            )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_batch(con, batch_key="batch-illegal", state="unknown")

        with pytest.raises(sqlite3.IntegrityError):
            _insert_batch(
                con,
                batch_key="batch-draft",
                state="queued",
                chat_id="another-chat",
            )

        first = _insert_batch(
            con, batch_key="open-one", chat_id="same-chat", user_id="same-user"
        )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_batch(
                con, batch_key="open-two", chat_id="same-chat", user_id="same-user"
            )

        for index, state in enumerate(("queued", "processing", "complete", "cancelled")):
            con.execute(
                "insert into bot_add_shards("
                "batch_id,shard_index,state,created_at,updated_at) values(?,?,?,?,?)",
                (first, index, state, 100, 100),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_add_shards("
                "batch_id,shard_index,state,created_at,updated_at) values(?,?,?,?,?)",
                (first, 0, "queued", 100, 100),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_add_shards("
                "batch_id,shard_index,state,created_at,updated_at) values(?,?,?,?,?)",
                (first, 100, "unknown", 100, 100),
            )
    finally:
        con.close()


def test_item_state_input_kind_unique_ingress_and_integrity_checks(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    try:
        batch_id = _insert_batch(con, batch_key="items", state="queued")
        for index, state in enumerate(sorted(ITEM_STATES)):
            _insert_item(
                con,
                batch_id,
                source_message_id=1000 + index,
                state=state,
                input_kind=("magnet", "http_url", "https_url", "bc_link")[index % 4],
            )

        with pytest.raises(sqlite3.IntegrityError):
            _insert_item(
                con,
                batch_id,
                source_message_id=2000,
                state="unknown",
            )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_item(
                con,
                batch_id,
                source_message_id=2001,
                input_kind="torrent_file",
            )

        _insert_item(con, batch_id, source_message_id=3000, source_index=4)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_item(con, batch_id, source_message_id=3000, source_index=4)
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_add_items("
                "batch_id,source_message_id,source_index,input_kind,raw_input,"
                "redacted_input,input_sha256,state,created_at,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?)",
                (
                    batch_id,
                    4000,
                    0,
                    "magnet",
                    "secret-link",
                    "redacted",
                    "sha-4000",
                    "received",
                    100,
                    100,
                ),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update bot_add_items set attempts=-1 where batch_id=?",
                (batch_id,),
            )
    finally:
        con.close()


def test_queue_indexes_cover_claim_identity_and_lookup_paths(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        item_indexes = _index_columns(con, "bot_add_items")
        assert ("state", "metadata_retry_at", "id") in item_indexes.values()
        assert ("metadata_probe_deadline", "state") in item_indexes.values()
        assert ("canonical_identity",) in item_indexes.values()
        assert ("qbt_hash",) in item_indexes.values()
        batch_indexes = _index_columns(con, "bot_add_batches")
        assert ("chat_id", "user_id") in batch_indexes.values()
        open_draft = next(
            row
            for row in con.execute("pragma index_list(bot_add_batches)")
            if str(row[1]) == "idx_bot_add_open_draft"
        )
        assert int(open_draft[2]) == 1
        assert int(open_draft[4]) == 1
        shard_indexes = _index_columns(con, "bot_add_shards")
        assert ("batch_id", "shard_index") in shard_indexes.values()
    finally:
        con.close()


def test_bot_add_events_are_append_only_and_retain_safe_audit_fields(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    con.execute("pragma foreign_keys=ON")
    try:
        batch_id = _insert_batch(con, batch_key="audit", state="queued")
        item_id = _insert_item(con, batch_id, source_message_id=1)
        event_id = con.execute(
            "insert into bot_add_events("
            "batch_id,item_id,event_type,from_state,to_state,actor_chat_id,actor_user_id,"
            "actor_role,reason_code,safe_evidence_json,created_at) "
            "values(?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id,
                item_id,
                "state_transition",
                "received",
                "resolving",
                "chat",
                "user",
                "operator",
                "accepted",
                '{"input_sha256":"safe"}',
                100,
            ),
        ).lastrowid
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            con.execute(
                "update bot_add_events set reason_code='changed' where id=?", (event_id,)
            )
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            con.execute("delete from bot_add_events where id=?", (event_id,))
    finally:
        con.close()


def test_remote_media_index_has_primary_key_and_normalized_id_index(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        assert {
            "video_path",
            "normalized_id",
            "size",
            "raw_basename",
            "status",
            "source",
            "updated_at",
        } <= _columns(con, "remote_media_index")
        primary_key = {
            str(row[1]) for row in con.execute("pragma table_info(remote_media_index)") if row[5]
        }
        assert primary_key == {"video_path"}
        assert ("normalized_id",) in _index_columns(
            con, "remote_media_index"
        ).values()
    finally:
        con.close()
