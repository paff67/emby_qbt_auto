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


def test_warning_inbox_upsert_and_read_fencing(tmp_path):
    from qbt_orchestrator.warning_inbox import WarningInboxRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = WarningInboxRepository(db, now=lambda: 200)
    first = repo.upsert(
        warning_key="capacity:no_safe_reclaim",
        severity="warning",
        topic="capacity",
        safe_message="no safe reclaim",
    )
    assert first["occurrence_count"] == 1
    assert first["resolved"] == 0
    second = repo.upsert(
        warning_key="capacity:no_safe_reclaim",
        severity="error",
        topic="capacity",
        safe_message="still no reclaim",
    )
    assert second["occurrence_count"] == 2
    assert second["severity"] == "error"
    assert repo.unread_count() == 1
    assert not repo.mark_read(int(second["id"]), expected_occurrence=1, admin_id="admin")
    assert repo.mark_read(int(second["id"]), expected_occurrence=2, admin_id="admin")
    assert repo.unread_count() == 0
    reopened = repo.upsert(
        warning_key="capacity:no_safe_reclaim",
        severity="warning",
        topic="capacity",
        safe_message="again",
    )
    assert reopened["resolved"] == 0
    assert reopened["occurrence_count"] == 3
    summary = repo.copy_summary(int(reopened["id"]))
    assert len(summary.encode("utf-8")) <= 256
    exported = repo.export_text(int(reopened["id"]))
    assert b"capacity" in exported
    assert len(exported) <= 512_000


def test_warning_service_projects_after_inbox_commit(tmp_path):
    from qbt_orchestrator.warning_inbox import WarningService
    from qbt_orchestrator.runtime import BotNotificationRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    service = WarningService(db, admin_chat_id="1001", now=lambda: 300)
    row = service.report(
        warning_key="qbt:authentication",
        severity="error",
        topic="qbt",
        safe_message="auth failed",
    )
    con = readonly_connect(db)
    try:
        inbox = con.execute(
            "select warning_key,resolved from bot_warning_inbox where id=?",
            (int(row["id"]),),
        ).fetchone()
        note = con.execute(
            "select dedupe_key,chat_id from bot_notifications where dedupe_key=?",
            (f"warn:{int(row['id'])}:{int(row['occurrence_count'])}",),
        ).fetchone()
    finally:
        con.close()
    assert inbox["warning_key"] == "qbt:authentication"
    assert note["chat_id"] == "1001"
    # Drop projection and reconcile.
    write_execute(db, "delete from bot_notifications")
    assert service.reconcile_projections() == 1


def test_warning_service_projection_payload_and_failed_projection_keeps_inbox(tmp_path):
    import json

    from qbt_orchestrator.warning_inbox import WarningService

    db = tmp_path / "state.sqlite"
    migrate(db)
    write_execute(
        db,
        "insert into bot_add_batches("
        "id,batch_key,chat_id,user_id,state,created_at,updated_at) "
        "values(38,'b-38','1','1','queued',1,1)",
    )
    write_execute(
        db,
        "insert into bot_add_items("
        "id,batch_id,source_message_id,source_index,input_kind,redacted_input,"
        "input_sha256,state,created_at,updated_at) "
        "values(9,38,1,0,'magnet','r','s','metadata_unavailable',1,1)",
    )

    class BoomNotifications:
        def __init__(self):
            self.calls = 0

        def enqueue_with_status(self, *args, **kwargs):
            self.calls += 1
            raise RuntimeError("notify_failed")

    boom = BoomNotifications()
    service = WarningService(
        db, admin_chat_id="1001", notifications=boom, now=lambda: 310
    )
    row = service.report(
        warning_key="checked_add:metadata_unavailable:9",
        severity="warning",
        topic="metadata_probe",
        safe_message="暂时无法获取元数据",
        related_batch_id=38,
        related_item_id=9,
        projection_payload={
            "item_id": 9,
            "batch_id": 38,
            "reply_markup": {
                "inline_keyboard": [[{"text": "x", "callback_data": "i:r:9:1"}]]
            },
        },
    )
    assert boom.calls == 1
    con = readonly_connect(db)
    try:
        inbox = con.execute(
            "select related_batch_id,related_item_id from bot_warning_inbox where id=?",
            (int(row["id"]),),
        ).fetchone()
        notes = con.execute("select count(*) from bot_notifications").fetchone()[0]
    finally:
        con.close()
    assert int(inbox["related_batch_id"]) == 38
    assert int(inbox["related_item_id"]) == 9
    assert int(notes) == 0

    from qbt_orchestrator.runtime import BotNotificationRepository

    recovered = WarningService(
        db,
        admin_chat_id="1001",
        notifications=BotNotificationRepository(db, now=lambda: 311),
        now=lambda: 311,
    )
    assert recovered.reconcile_projections() == 1
    con = readonly_connect(db)
    try:
        note = con.execute(
            "select payload_json from bot_notifications where dedupe_key=?",
            (f"warn:{int(row['id'])}:{int(row['occurrence_count'])}",),
        ).fetchone()
    finally:
        con.close()
    payload = json.loads(note["payload_json"])
    assert "reply_markup" not in payload
    assert int(payload["warning_id"]) == int(row["id"])


def test_required_warning_keys_via_service(tmp_path):
    from qbt_orchestrator.warning_inbox import WarningService

    db = tmp_path / "state.sqlite"
    migrate(db)
    service = WarningService(db, admin_chat_id="9", now=lambda: 400)
    keys = [
        "capacity:no_safe_reclaim",
        "capacity:reclaim_failed:abc",
        "qbt:authentication",
        "checked_add:blocked_manual_deleted:1",
        "upload:verify:2",
        "path_drift:abc",
        "daemon_task:planner:RuntimeError",
    ]
    for key in keys:
        service.report(
            warning_key=key,
            severity="warning",
            topic=key.split(":", 1)[0],
            safe_message=f"safe {key}",
        )
    assert WarningService(db, admin_chat_id="9").inbox.unread_count() == len(keys)


def test_path_reconcile_reports_through_warning_service(tmp_path):
    from qbt_orchestrator.path_reconcile import QbtPathReconciler
    from qbt_orchestrator.warning_inbox import WarningService

    db = tmp_path / "state.sqlite"
    migrate(db)
    service = WarningService(db, admin_chat_id="9", now=lambda: 500)
    reconciler = QbtPathReconciler(
        db,
        expected_save_path="/downloads/active",
        allowed_temp_path="/downloads/incomplete",
        warning_service=service,
    )
    reconciler.reconcile(
        {
            "abc": {
                "hash": "abc",
                "name": "x",
                "category": "auto",
                "tags": "auto",
                "save_path": "/downloads/active",
                "content_path": "/downloads/BBAN-582",
                "progress": 0.5,
            }
        }
    )
    assert service.inbox.unread_count() >= 1
    keys = {
        row["warning_key"]
        for row in service.inbox.list_recent(limit=10)
    }
    assert any(key.startswith("path_drift:") for key in keys)
