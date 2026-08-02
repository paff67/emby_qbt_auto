from __future__ import annotations

from qbt_orchestrator.db import migrate, readonly_connect, write_execute
from qbt_orchestrator.telegram_batch_summary import BatchSummaryProjector


def _seed_batch(db, *, batch_id=1, state="processing", submitted_at=1000, **counts):
    write_execute(
        db,
        "insert into bot_add_batches("
        "id,batch_key,chat_id,user_id,state,received_count,enrolled_count,"
        "duplicate_count,confirmation_count,failed_count,blocked_history_count,"
        "created_at,submitted_at,updated_at,completed_at) "
        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            batch_id,
            f"b-{batch_id}",
            "7",
            "42",
            state,
            counts.get("received_count", 50),
            counts.get("enrolled_count", 31),
            counts.get("duplicate_count", 8),
            counts.get("confirmation_count", 3),
            counts.get("failed_count", 0),
            counts.get("blocked_history_count", 2),
            1,
            submitted_at,
            submitted_at,
            counts.get("completed_at"),
        ),
    )


def _seed_item(db, *, item_id, batch_id, state, source_index):
    write_execute(
        db,
        "insert into bot_add_items("
        "id,batch_id,source_message_id,source_index,input_kind,redacted_input,"
        "input_sha256,state,created_at,updated_at) "
        "values(?,?,1,?,?,?,?,?,?,?)",
        (
            item_id,
            batch_id,
            source_index,
            "magnet",
            f"r-{item_id}",
            f"s-{item_id}",
            state,
            1,
            1,
        ),
    )


def test_initial_summary_once_when_delayed_items_present(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    # Clear migration stamp so this new logical batch can notify.
    write_execute(db, "delete from bot_add_batches")
    _seed_batch(db, submitted_at=1000)
    _seed_item(db, item_id=1, batch_id=1, state="enrolled", source_index=0)
    _seed_item(db, item_id=2, batch_id=1, state="needs_confirmation", source_index=1)
    _seed_item(db, item_id=3, batch_id=1, state="metadata_wait", source_index=2)
    projector = BatchSummaryProjector(db, now=lambda: 1200, initial_delay_sec=120)
    first = projector.tick()
    second = projector.tick()
    assert first["initial"] == 1
    assert second["initial"] == 0
    con = readonly_connect(db)
    try:
        notes = con.execute(
            "select dedupe_key,message from bot_notifications "
            "where dedupe_key='tg:add-batch:1:initial'"
        ).fetchall()
        sent = con.execute(
            "select initial_summary_sent_at from bot_add_batches where id=1"
        ).fetchone()[0]
    finally:
        con.close()
    assert len(notes) == 1
    assert "等待确认：1 条" in notes[0]["message"]
    assert int(sent) == 1200


def test_fast_complete_skips_initial_and_emits_final(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    write_execute(db, "delete from bot_add_batches")
    _seed_batch(
        db,
        state="complete",
        submitted_at=1000,
        completed_at=1010,
        enrolled_count=36,
        duplicate_count=9,
        failed_count=3,
    )
    for index, state in enumerate(
        ["enrolled", "duplicate_local", "failed", "enrolled_hold"]
    ):
        _seed_item(db, item_id=index + 1, batch_id=1, state=state, source_index=index)
    projector = BatchSummaryProjector(db, now=lambda: 1100, initial_delay_sec=120)
    result = projector.tick()
    assert result["initial"] == 0
    assert result["final"] == 1
    con = readonly_connect(db)
    try:
        keys = [
            row[0]
            for row in con.execute("select dedupe_key from bot_notifications").fetchall()
        ]
    finally:
        con.close()
    assert keys == ["tg:add-batch:1:final"]


def test_repeat_tick_does_not_duplicate_notifications(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    write_execute(db, "delete from bot_add_batches")
    _seed_batch(db, state="complete", submitted_at=1, completed_at=2)
    _seed_item(db, item_id=1, batch_id=1, state="enrolled", source_index=0)
    projector = BatchSummaryProjector(db, now=lambda: 50)
    assert projector.tick()["final"] == 1
    assert projector.tick()["final"] == 0
    con = readonly_connect(db)
    try:
        count = con.execute("select count(*) from bot_notifications").fetchone()[0]
    finally:
        con.close()
    assert int(count) == 1


def test_migrated_historical_batches_do_not_storm(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    # Simulate upgrade: historical terminal batch exists before version 22 applies.
    write_execute(db, "delete from schema_migrations where version=22")
    write_execute(
        db,
        "insert into bot_add_batches("
        "batch_key,chat_id,user_id,state,received_count,created_at,submitted_at,"
        "completed_at,updated_at,initial_summary_sent_at,final_summary_sent_at) "
        "values(?,?,?,?,?,?,?,?,?,null,null)",
        ("old", "7", "42", "complete", 1, 1, 1, 2, 2),
    )
    migrate(db)
    projector = BatchSummaryProjector(db, now=lambda: 999999)
    result = projector.tick()
    assert result["initial"] == 0
    assert result["final"] == 0
    con = readonly_connect(db)
    try:
        notes = con.execute("select count(*) from bot_notifications").fetchone()[0]
        stamped = con.execute(
            "select initial_summary_sent_at is not null, final_summary_sent_at is not null "
            "from bot_add_batches where batch_key='old'"
        ).fetchone()
    finally:
        con.close()
    assert int(notes) == 0
    assert stamped[0] == 1
    assert stamped[1] == 1
