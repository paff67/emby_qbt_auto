from __future__ import annotations

from qbt_orchestrator.batch_failure_warnings import (
    BatchFailureWarningProjector,
    item_label,
    safe_failure_reason,
)
from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
from qbt_orchestrator.db import migrate, write_transaction
from qbt_orchestrator.warning_inbox import WarningInboxRepository, WarningService


def _seed_item(
    db,
    *,
    state: str,
    display_name=None,
    media_id=None,
    source_index=0,
    last_error=None,
):
    queue = BotAddQueueRepository(db, now=lambda: 2_000_000_000)
    batch = queue.open_draft("1", "1")
    magnet = "magnet:?" + "xt=urn:btih:" + "a" * 40
    if display_name:
        magnet += f"&dn={display_name}"
    queue.append_message(batch["id"], 1, [magnet])
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    write_transaction(
        db,
        lambda con: con.execute(
            "update bot_add_items set state=?, normalized_media_id=?, last_error=?,"
            " source_index=?, decision_reason=? where id=?",
            (
                state,
                media_id,
                last_error,
                source_index,
                last_error or state,
                item["id"],
            ),
        ),
    )
    return queue.get_item(item["id"])


def test_item_label_prefers_media_id_then_display_name_then_fallback():
    assert item_label({"normalized_media_id": "BBAN-523", "display_name": "x"}) == "BBAN-523"
    assert item_label({"display_name": "BBAN-523", "source_index": 1}) == "BBAN-523"
    assert (
        item_label({"source_index": 1, "canonical_identity": "abcdef1234567890"})
        == "第 2 条 · abcdef123456"
    )


def test_projector_creates_one_warning_per_failure_state(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(db, now=lambda: 2_000_000_000)
    batch = queue.open_draft("1", "1")
    magnets = [
        "magnet:?" + "xt=urn:btih:" + ("a" * 40) + "&dn=BBAN-523",
        "magnet:?" + "xt=urn:btih:" + ("b" * 40) + "&dn=BBAN-524",
        "magnet:?" + "xt=urn:btih:" + ("c" * 40) + "&dn=BBAN-525",
    ]
    queue.append_message(batch["id"], 1, magnets)
    queue.submit(batch["id"])
    items = queue.list_items(batch["id"])
    states = ("failed", "invalid", "metadata_unavailable")
    for item, state in zip(items, states):
        write_transaction(
            db,
            lambda con, item_id=item["id"], st=state: con.execute(
                "update bot_add_items set state=?, last_error=?, decision_reason=? where id=?",
                (st, f"{st}_code", f"{st}_code", item_id),
            ),
        )

    service = WarningService(db, now=lambda: 2_000_000_100)
    service.report(
        warning_key=f"checked_add:metadata_unavailable:{items[2]['id']}",
        severity="warning",
        topic="metadata_probe",
        safe_message="暂时无法获取元数据：BBAN-525",
        related_batch_id=int(batch["id"]),
        related_item_id=int(items[2]["id"]),
    )
    projector = BatchFailureWarningProjector(db, service, now=lambda: 2_000_000_100)
    first = projector.tick()
    assert first["projected"] == 2
    inbox = WarningInboxRepository(db)
    rows = inbox.list_unread(limit=20)
    assert len(rows) == 3
    messages = " ".join(row["safe_message"] for row in rows)
    assert "BBAN-523" in messages
    assert "failed_code" in messages or "invalid_code" in messages

    second = projector.tick()
    assert second["projected"] == 0
    rows = inbox.list_unread(limit=20)
    assert len(rows) == 3
    assert all(int(row["occurrence_count"]) == 1 for row in rows)


def test_projector_uses_source_index_fallback_without_media_id(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    item = _seed_item(
        db,
        state="invalid",
        source_index=3,
        last_error="bad_link",
    )
    write_transaction(
        db,
        lambda con: con.execute(
            "update bot_add_items set display_name=null, normalized_media_id=null, "
            "canonical_identity=? where id=?",
            ("deadbeefcafe0001", item["id"]),
        ),
    )
    service = WarningService(db)
    BatchFailureWarningProjector(db, service).tick()
    row = WarningInboxRepository(db).list_unread(limit=1)[0]
    assert "第 4 条 · deadbeefcafe" in row["safe_message"]
    assert "magnet" not in row["safe_message"].lower()


def test_projector_resolves_after_recovery_and_reopens_on_repeat_failure(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    item = _seed_item(db, state="failed", display_name="IPX-001", last_error="boom")
    service = WarningService(db)
    projector = BatchFailureWarningProjector(db, service)
    projector.tick()
    inbox = WarningInboxRepository(db)
    assert len(inbox.list_unread(limit=10)) == 1

    write_transaction(
        db,
        lambda con: con.execute(
            "update bot_add_items set state='enrolled',updated_at=2000000001 where id=?",
            (item["id"],),
        ),
    )
    projector.tick()
    assert inbox.list_unread(limit=10) == []

    write_transaction(
        db,
        lambda con: con.execute(
            "update bot_add_items set state='failed', last_error='again',"
            "updated_at=2000000002 where id=?",
            (item["id"],),
        ),
    )
    projector.tick()
    rows = inbox.list_unread(limit=10)
    assert len(rows) == 1
    assert int(rows[0]["resolved"] or 0) == 0
    assert "again" in rows[0]["safe_message"]


def test_safe_failure_reason_redacts_magnets():
    reason = safe_failure_reason(
        {
            "last_error": "bad magnet:?" + "xt=urn:btih:" + "a" * 40,
            "state": "failed",
        }
    )
    assert "magnet:?xt=" not in reason.lower()
    assert "a" * 40 not in reason
