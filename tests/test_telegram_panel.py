from __future__ import annotations

from qbt_orchestrator.db import migrate, write_execute
from qbt_orchestrator.telegram_panel import (
    PanelSessionRepository,
    PersistentPanelController,
)
from qbt_orchestrator.telegram_ui import DashboardRepository, TelegramPanelRenderer


class FakeApi:
    def __init__(self):
        self.messages: list[tuple] = []
        self.edits: list[tuple] = []
        self._next_id = 100

    def send_message(self, chat_id, text, reply_markup=None):
        self._next_id += 1
        self.messages.append((chat_id, text, reply_markup, self._next_id))
        return {"ok": True, "result": {"message_id": self._next_id}}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append((chat_id, message_id, text, reply_markup))
        return {"ok": True, "result": True}


def test_panel_session_bind_and_route(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)
    repo = PanelSessionRepository(db, now=lambda: 10)
    assert repo.get() is None
    repo.bind(7, 55)
    repo.set_route("n:q:0")
    repo.record_render("abc", 10)
    row = repo.get()
    assert row is not None
    assert str(row["chat_id"]) == "7"
    assert int(row["message_id"]) == 55
    assert row["current_route"] == "n:q:0"
    assert row["last_render_hash"] == "abc"


def test_persistent_panel_edits_existing_and_skips_unchanged(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)
    api = FakeApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.open_home(7)
    assert len(api.messages) == 1
    assert len(api.edits) == 0
    message_id = api.messages[0][3]
    controller.open_home(7)
    assert len(api.messages) == 1
    assert len(api.edits) == 0  # digest unchanged
    write_execute(
        db,
        "insert into torrent_health("
        "hash,name,sampled_at,dlspeed_bps,progress,updated_at,active_since) "
        "values('h1','SONE-792',1,100,0.5,1,1)",
    )
    controller.refresh_now()
    assert len(api.edits) == 1
    assert api.edits[0][1] == message_id
    assert "SONE-792" in api.edits[0][2]


def test_refresh_if_due_only_on_home(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)
    api = FakeApi()
    clock = {"t": 1000}
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(
            DashboardRepository(db), now=lambda: clock["t"]
        ),
        now=lambda: clock["t"],
    )
    controller.open_home(7)
    assert controller.refresh_if_due(60) is False
    clock["t"] = 1070
    assert controller.refresh_if_due(60) is True
    controller.navigate(7, "n:q:0")
    clock["t"] = 1200
    assert controller.refresh_if_due(60) is False
