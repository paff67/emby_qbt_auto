from __future__ import annotations

import pytest

from qbt_orchestrator.db import migrate, write_execute
from qbt_orchestrator.integrations.telegram import TelegramApiError
from qbt_orchestrator.telegram_panel import (
    PanelSessionRepository,
    PersistentPanelController,
)
from qbt_orchestrator.telegram_ui import DashboardRepository, TelegramPanelRenderer


class FakeApi:
    def __init__(self):
        self.messages: list[tuple] = []
        self.edits: list[tuple] = []
        self.deletes: list[tuple] = []
        self._next_id = 100

    def send_message(self, chat_id, text, reply_markup=None):
        self._next_id += 1
        self.messages.append((chat_id, text, reply_markup, self._next_id))
        return {"ok": True, "result": {"message_id": self._next_id}}

    def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append((chat_id, message_id, text, reply_markup))
        return {"ok": True, "result": True}

    def delete_message(self, chat_id, message_id):
        self.deletes.append((chat_id, message_id))
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


def test_start_creates_new_console_and_retires_old(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)
    api = FakeApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.open_home(7, update_id=1)
    assert len(api.messages) == 1
    first_id = api.messages[0][3]
    controller.open_home(7, update_id=2)
    assert len(api.messages) == 2
    second_id = api.messages[1][3]
    assert second_id != first_id
    session = controller.sessions.get()
    assert int(session["message_id"]) == second_id
    assert api.deletes == [(7, first_id)]
    # Duplicate update id is a no-op.
    controller.open_home(7, update_id=2)
    assert len(api.messages) == 2


def test_persistent_panel_refresh_edits_current_and_skips_unchanged(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)
    api = FakeApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.open_home(7, update_id=1)
    assert len(api.messages) == 1
    message_id = api.messages[0][3]
    controller.refresh_now()
    assert len(api.edits) == 1
    assert api.edits[0][1] == message_id
    write_execute(
        db,
        "insert into torrent_health("
        "hash,name,sampled_at,dlspeed_bps,progress,updated_at,active_since) "
        "values('h1','SONE-792',1,100,0.5,1,1)",
    )
    controller.refresh_now()
    assert len(api.edits) == 2
    assert api.edits[-1][1] == message_id
    assert "SONE-792" in api.edits[-1][2]


def test_open_home_keeps_old_session_when_send_fails(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)

    class FailSendApi(FakeApi):
        def send_message(self, chat_id, text, reply_markup=None):
            if self.messages:
                raise RuntimeError("send failed")
            return super().send_message(chat_id, text, reply_markup)

    api = FailSendApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.open_home(7, update_id=1)
    first_id = api.messages[0][3]
    with pytest.raises(RuntimeError):
        controller.open_home(7, update_id=2)
    session = controller.sessions.get()
    assert int(session["message_id"]) == first_id
    assert api.deletes == []


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


def test_refresh_if_due_backs_off_after_failed_attempt(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)

    class BoomApi(FakeApi):
        def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
            raise TelegramApiError(
                "editMessageText", http_status=500, description="upstream boom"
            )

        def send_message(self, chat_id, text, reply_markup=None):
            raise AssertionError("sendMessage must not run on transient edit errors")

    clock = {"t": 1000}
    controller = PersistentPanelController(
        db,
        api=BoomApi(),
        renderer=TelegramPanelRenderer(
            DashboardRepository(db), now=lambda: clock["t"]
        ),
        now=lambda: clock["t"],
    )
    # Seed a bound session without going through publish.
    controller.sessions.bind(7, 55)
    controller.sessions.set_route("n:h")
    controller.sessions.record_render("seed", 900)

    with pytest.raises(TelegramApiError):
        controller.refresh_if_due(60)
    session = controller.sessions.get()
    assert session is not None
    assert int(session["last_refresh_attempt_at"]) == 1000
    assert int(session["message_id"]) == 55

    clock["t"] = 1059
    assert controller.refresh_if_due(60) is False
    clock["t"] = 1060
    with pytest.raises(TelegramApiError):
        controller.refresh_if_due(60)
    assert int(controller.sessions.get()["last_refresh_attempt_at"]) == 1060


def test_publish_network_error_does_not_create_duplicate_console(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)

    class NetworkApi(FakeApi):
        def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
            raise TelegramApiError("editMessageText", description="timed out")

        def send_message(self, chat_id, text, reply_markup=None):
            raise AssertionError("sendMessage forbidden on network errors")

    api = NetworkApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.sessions.bind(7, 55)
    controller.sessions.set_route("n:h")
    controller.sessions.record_render("seed", 900)
    with pytest.raises(TelegramApiError):
        controller.refresh_now()
    assert api.messages == []
    session = controller.sessions.get()
    assert str(session["chat_id"]) == "7"
    assert int(session["message_id"]) == 55


def test_publish_rate_limit_does_not_create_duplicate_console(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)

    class RateLimitApi(FakeApi):
        def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
            raise TelegramApiError(
                "editMessageText",
                http_status=429,
                error_code=429,
                description="Too Many Requests",
                retry_after=3,
            )

        def send_message(self, chat_id, text, reply_markup=None):
            raise AssertionError("sendMessage forbidden on 429")

    api = RateLimitApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.sessions.bind(7, 55)
    controller.sessions.set_route("n:h")
    controller.sessions.record_render("seed", 900)
    with pytest.raises(TelegramApiError):
        controller.refresh_now()
    assert api.messages == []
    session = controller.sessions.get()
    assert str(session["chat_id"]) == "7"
    assert int(session["message_id"]) == 55


def test_publish_rebuilds_when_message_to_edit_not_found(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)

    class MissingMessageApi(FakeApi):
        def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
            self.edits.append((chat_id, message_id, text, reply_markup))
            raise TelegramApiError(
                "editMessageText",
                http_status=400,
                error_code=400,
                description="Bad Request: message to edit not found",
            )

    api = MissingMessageApi()
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        now=lambda: 1000,
    )
    controller.sessions.bind(7, 55)
    controller.sessions.set_route("n:h")
    controller.sessions.record_render("seed", 900)
    controller.refresh_now()
    assert len(api.edits) == 1
    assert len(api.messages) == 1
    session = controller.sessions.get()
    assert str(session["chat_id"]) == "7"
    assert int(session["message_id"]) == api.messages[0][3]
    assert int(session["message_id"]) != 55


def test_publish_successful_edit_with_record_render_failure_does_not_send(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)

    class FailingSessions(PanelSessionRepository):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.fail_record = False

        def record_render(self, digest: str, refreshed_at: int) -> None:
            if self.fail_record:
                raise RuntimeError("persist render failed")
            return super().record_render(digest, refreshed_at)

    api = FakeApi()
    sessions = FailingSessions(db, now=lambda: 1000)
    controller = PersistentPanelController(
        db,
        api=api,
        renderer=TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000),
        sessions=sessions,
        now=lambda: 1000,
    )
    sessions.bind(7, 55)
    sessions.set_route("n:h")
    sessions.record_render("seed", 900)
    sessions.fail_record = True
    with pytest.raises(RuntimeError, match="persist render failed"):
        controller.refresh_now()
    assert len(api.edits) == 1
    assert api.messages == []
    session = PanelSessionRepository(db).get()
    assert str(session["chat_id"]) == "7"
    assert int(session["message_id"]) == 55


def test_home_active_total_counts_beyond_display_limit(tmp_path):
    db = tmp_path / "panel.sqlite"
    migrate(db)
    for index in range(4):
        write_execute(
            db,
            "insert into torrent_health("
            "hash,name,sampled_at,dlspeed_bps,progress,updated_at,active_since) "
            "values(?,?,1,?,?,1,1)",
            (f"h{index}", f"T-{index}", 100 - index, 0.1 * (index + 1)),
        )
    snap = DashboardRepository(db).home_snapshot()
    assert snap["active_total"] == 4
    assert len(snap["active"]) == 3
    view = TelegramPanelRenderer(DashboardRepository(db), now=lambda: 1000).render_home()
    assert "当前任务（4）" in view.text
