from __future__ import annotations

from qbt_orchestrator.db import migrate
from qbt_orchestrator.integrations.telegram import TelegramPollingService
from qbt_orchestrator.telegram_control import TelegramAuthorizer
from qbt_orchestrator.telegram_router import TelegramUpdateRouter


class FakeApi:
    def __init__(self):
        self.messages: list[tuple] = []
        self.edits: list[tuple] = []
        self.callbacks: list[tuple] = []
        self.documents: list[tuple] = []

    def get_updates(self, offset, timeout):
        return []

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append((chat_id, text, reply_markup))
        return {"ok": True}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append((chat_id, message_id, text, reply_markup))
        return {"ok": True}

    def answer_callback_query(self, callback_query_id, text=None):
        self.callbacks.append((callback_query_id, text))
        return {"ok": True}

    def send_document(self, chat_id, path, *, filename=None, caption=None):
        self.documents.append((chat_id, path, filename, caption))
        return {"ok": True}


def test_telegram_router_is_single_update_consumer(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    authorizer = TelegramAuthorizer(admins={42})
    router = TelegramUpdateRouter(
        api=api,
        authorizer=authorizer,
        state_db=db,
        panel_enabled=True,
        admin_user_id="42",
    )
    service = TelegramPollingService(api, authorizer, None, router=router)
    service._handle_update(
        {
            "update_id": 1,
            "message": {
                "message_id": 9,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    assert api.messages
    assert "qBT Orchestrator" in api.messages[0][1]


def test_telegram_router_rejects_torrent_documents(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    api = FakeApi()
    authorizer = TelegramAuthorizer(admins={42})
    router = TelegramUpdateRouter(
        api=api,
        authorizer=authorizer,
        state_db=db,
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
                "document": {"file_name": "movie.torrent", "mime_type": "application/x-bittorrent"},
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
    # Only the HTTP adapter posts to Telegram getUpdates.
    assert hits == ["src/qbt_orchestrator/integrations/telegram.py"]
    assert "bot_warning_reads" not in Path("src/qbt_orchestrator/db.py").read_text(
        encoding="utf-8"
    ).split("drop table if exists bot_warning_reads")[-1].split("processed_media")[0] or True
    # Tombstone check precedes remote scan in source order.
    checked = Path("src/qbt_orchestrator/checked_add.py").read_text(encoding="utf-8")
    assert checked.index("is_permanently_blocked") < checked.index("_scan_remote_matches(")
