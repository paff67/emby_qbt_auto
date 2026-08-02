from __future__ import annotations

from qbt_orchestrator.bot_add_queue import BotAddQueueRepository
from qbt_orchestrator.db import migrate, readonly_connect
from qbt_orchestrator.telegram_control import TelegramAuthorizer
from qbt_orchestrator.telegram_router import TelegramUpdateRouter, extract_links_from_text


class FakeApi:
    def __init__(self):
        self.messages: list[tuple] = []
        self.edits: list[tuple] = []
        self.callbacks: list[tuple] = []
        self.documents: list[tuple] = []
        self._next_id = 10

    def get_updates(self, offset, timeout):
        return []

    def send_message(self, chat_id, text, reply_markup=None):
        self._next_id += 1
        self.messages.append((chat_id, text, reply_markup))
        return {"ok": True, "result": {"message_id": self._next_id}}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append((chat_id, message_id, text, reply_markup))
        return {"ok": True}

    def answer_callback_query(self, callback_query_id, text=None):
        self.callbacks.append((callback_query_id, text))
        return {"ok": True}

    def send_document(self, chat_id, path, *, filename=None, caption=None):
        self.documents.append((chat_id, path, filename, caption))
        return {"ok": True}


def test_extract_links_from_text():
    text = "magnet:?" + "xt=urn:btih:" + ("a" * 40) + "\nhttps://example.test/x.torrent"
    links = extract_links_from_text(text)
    assert len(links) == 2


def test_telegram_router_appends_magnet_to_real_queue(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    authorizer = TelegramAuthorizer(admins={42}, single_admin_id=42)
    queue = BotAddQueueRepository(db)
    router = TelegramUpdateRouter(
        api=api,
        authorizer=authorizer,
        state_db=db,
        add_queue=queue,
        panel_enabled=True,
        admin_user_id="42",
    )
    magnet = "magnet:?" + "xt=urn:btih:" + ("b" * 40)
    router.handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 99,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": magnet,
            },
        }
    )
    con = readonly_connect(db)
    try:
        batches = con.execute("select count(*) from bot_add_batches").fetchone()[0]
        items = con.execute("select count(*) from bot_add_items").fetchone()[0]
    finally:
        con.close()
    assert int(batches) == 1
    assert int(items) == 1
    assert any("添加下载" in text and "已接收" in text for _, text, _ in api.messages)
    assert any(
        "提交本批" in str(btn.get("text") or "")
        for _, _, markup in api.messages
        if markup
        for row in markup.get("inline_keyboard", [])
        for btn in row
    )


def test_telegram_router_submit_and_cancel_callbacks(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    authorizer = TelegramAuthorizer(admins={42}, single_admin_id=42)
    queue = BotAddQueueRepository(db)
    router = TelegramUpdateRouter(
        api=api,
        authorizer=authorizer,
        state_db=db,
        add_queue=queue,
        panel_enabled=True,
        admin_user_id="42",
    )
    draft = queue.open_draft("7", "42")
    magnet = "magnet:?" + "xt=urn:btih:" + ("c" * 40)
    queue.append_message(int(draft["id"]), 11, [magnet])
    draft = queue.get_batch(int(draft["id"]))
    gen = int(draft["updated_at"])
    router.handle_update(
        {
            "update_id": 2,
            "callback_query": {
                "id": "cb1",
                "from": {"id": 42},
                "message": {"message_id": 5, "chat": {"id": 7}},
                "data": f"a:s:{draft['id']}:{gen}",
            },
        }
    )
    assert api.callbacks
    submitted = queue.get_batch(int(draft["id"]))
    assert submitted["state"] in {
        "queued",
        "processing",
        "complete",
        "awaiting_confirmation",
    }
    # Submitted draft navigates persistent panel to batch detail.
    assert any("批次" in text for _, _, text, _ in api.edits) or any(
        "批次" in text for _, text, _ in api.messages
    )


def test_stale_draft_cancel_after_submit_is_rejected(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(db)
    draft = queue.open_draft("7", "42")
    magnet = "magnet:?" + "xt=urn:btih:" + ("d" * 40)
    queue.append_message(int(draft["id"]), 11, [magnet])
    draft = queue.get_batch(int(draft["id"]))
    stale_gen = int(draft["updated_at"])
    submitted = queue.submit_draft(int(draft["id"]), stale_gen)
    assert submitted["state"] == "queued"

    import pytest

    with pytest.raises(ValueError, match="^draft_generation_conflict$"):
        queue.cancel_draft(int(draft["id"]), stale_gen, actor="42")
    assert queue.get_batch(int(draft["id"]))["state"] == "queued"


def test_start_uses_persistent_panel(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    assert len(api.messages) == 1
    assert "编排器控制台" in api.messages[0][1]
    router.handle_update(
        {
            "update_id": 2,
            "callback_query": {
                "id": "cb",
                "from": {"id": 42},
                "message": {"message_id": api._next_id, "chat": {"id": 7}},
                "data": "n:q:0",
            },
        }
    )
    assert api.edits
    assert "添加队列" in api.edits[-1][2]


def test_confirmation_callback_edits_source_message(tmp_path):
    from qbt_orchestrator.checked_add import CheckedAddService

    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    queue = BotAddQueueRepository(db)
    draft = queue.open_draft("7", "42")
    magnet = "magnet:?" + "xt=urn:btih:" + ("e" * 40)
    queue.append_message(int(draft["id"]), 11, [magnet])
    draft = queue.get_batch(int(draft["id"]))
    queue.submit_draft(int(draft["id"]), int(draft["updated_at"]))
    item = queue.get_item(1)
    # Force needs_confirmation for button path without full checked-add.
    from qbt_orchestrator.db import write_execute

    write_execute(
        db,
        "update bot_add_items set state='needs_confirmation', approval_generation=3 "
        "where id=1",
    )

    class FakeChecked:
        def approve_hold(self, item_id, actor, generation):
            write_execute(
                db,
                "update bot_add_items set state='enrolled_hold' where id=?",
                (item_id,),
            )

        def cancel(self, item_id, actor, generation):
            write_execute(
                db,
                "update bot_add_items set state='cancelled' where id=?",
                (item_id,),
            )

    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        add_queue=queue,
        checked_add=FakeChecked(),
        panel_enabled=True,
        admin_user_id="42",
    )
    # Independent confirmation message (not the panel message_id).
    router.handle_update(
        {
            "update_id": 9,
            "callback_query": {
                "id": "cby",
                "from": {"id": 42},
                "message": {"message_id": 999, "chat": {"id": 7}},
                "data": "i:y:1:3",
            },
        }
    )
    assert any(mid == 999 and "已确认" in text for _, mid, text, _ in api.edits)
    del item
    del CheckedAddService


def test_legacy_batch_detail_callback_opens_first_page(tmp_path):
    from qbt_orchestrator.db import write_execute

    db = tmp_path / "state.sqlite"
    migrate(db)
    write_execute(
        db,
        "insert into bot_add_batches("
        "batch_key,chat_id,user_id,state,received_count,created_at,updated_at) "
        "values(?,?,?,?,?,?,?)",
        ("b-legacy", "7", "42", "queued", 0, 1, 1),
    )
    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        add_queue=BotAddQueueRepository(db),
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    panel_mid = int(router.panel.sessions.get()["message_id"])
    router.handle_update(
        {
            "update_id": 9,
            "callback_query": {
                "id": "cb-legacy",
                "from": {"id": 42},
                "message": {"message_id": panel_mid, "chat": {"id": 7}},
                "data": "n:b:1",
            },
        }
    )
    assert api.edits
    assert "条目第 1 页" in api.edits[-1][2]


def test_item_cancel_qbt_write_fenced_keeps_needs_confirmation(tmp_path):
    from qbt_orchestrator.db import write_execute

    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(db)
    draft = queue.open_draft("7", "42")
    magnet = "magnet:?" + "xt=urn:btih:" + ("e" * 40)
    queue.append_message(int(draft["id"]), 1, [magnet])
    batch = queue.submit(int(draft["id"]))
    item = queue.list_items(int(batch["id"]))[0]
    write_execute(
        db,
        "update bot_add_items set state='needs_confirmation',qbt_hash=?,"
        "qbt_precheck_tag='tag-e',approval_generation=1,updated_at=updated_at+1 "
        "where id=?",
        ("a" * 40, int(item["id"])),
    )
    item = queue.get_item(int(item["id"]))

    class FencedCheckedAdd:
        def cancel(self, item_id, actor, approval_generation):
            raise ValueError("qbt_write_fenced")

        def approve_hold(self, *args, **kwargs):
            raise AssertionError("unused")

    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        add_queue=queue,
        checked_add=FencedCheckedAdd(),
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 4,
            "callback_query": {
                "id": "cb-x",
                "from": {"id": 42},
                "message": {"message_id": 7, "chat": {"id": 7}},
                "data": f"i:x:{item['id']}:{item['approval_generation']}",
            },
        }
    )
    assert queue.get_item(int(item["id"]))["state"] == "needs_confirmation"
    assert any(
        text and "写入被保护" in text for _, text in api.callbacks
    )


def test_telegram_router_rejects_torrent_documents(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    authorizer = TelegramAuthorizer(admins={42}, single_admin_id=42)
    router = TelegramUpdateRouter(
        api=api,
        authorizer=authorizer,
        state_db=db,
        add_queue=BotAddQueueRepository(db),
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 2,
            "message": {
                "message_id": 10,
                "chat": {"id": 7},
                "from": {"id": 42},
                "document": {
                    "file_name": "movie.torrent",
                    "mime_type": "application/x-bittorrent",
                },
            },
        }
    )
    assert any(".torrent" in text for _, text, _ in api.messages)


def test_static_guard_single_get_updates_path():
    from pathlib import Path

    root = Path("src/qbt_orchestrator")
    hits = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if '"getUpdates"' in text or "'getUpdates'" in text:
            hits.append(str(path))
    assert hits == ["src/qbt_orchestrator/integrations/telegram.py"]
    checked = Path("src/qbt_orchestrator/checked_add.py").read_text(encoding="utf-8")
    assert checked.index("is_permanently_blocked") < checked.index(
        "_scan_remote_matches("
    )
    db_sql = Path("src/qbt_orchestrator/db.py").read_text(encoding="utf-8")
    assert "create table if not exists bot_warning_reads" not in db_sql
    assert "drop table if exists bot_warning_reads" in db_sql


def test_unauthorized_chat_cannot_touch_bound_panel(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    assert len(api.messages) == 1
    bound = router.panel.sessions.get()
    assert str(bound["chat_id"]) == "7"
    message_id = int(bound["message_id"])

    before_messages = list(api.messages)
    before_edits = list(api.edits)
    router.handle_update(
        {
            "update_id": 2,
            "message": {
                "message_id": 2,
                "chat": {"id": 999},
                "from": {"id": 999},
                "text": "/status",
            },
        }
    )
    router.handle_update(
        {
            "update_id": 3,
            "message": {
                "message_id": 3,
                "chat": {"id": 999},
                "from": {"id": 999},
                "text": "/cleanup h1",
            },
        }
    )
    assert api.messages == before_messages
    assert api.edits == before_edits
    still = router.panel.sessions.get()
    assert str(still["chat_id"]) == "7"
    assert int(still["message_id"]) == message_id


def test_authorized_admin_can_rebind_panel_via_start_in_new_chat(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 1,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    old_id = int(router.panel.sessions.get()["message_id"])
    router.handle_update(
        {
            "update_id": 2,
            "message": {
                "message_id": 2,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/status",
            },
        }
    )
    assert api.edits
    assert int(router.panel.sessions.get()["message_id"]) == old_id

    router.handle_update(
        {
            "update_id": 3,
            "message": {
                "message_id": 3,
                "chat": {"id": 8},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    session = router.panel.sessions.get()
    assert str(session["chat_id"]) == "8"
    assert int(session["message_id"]) != old_id
    assert any(chat_id == 8 for chat_id, _text, _markup in api.messages)
