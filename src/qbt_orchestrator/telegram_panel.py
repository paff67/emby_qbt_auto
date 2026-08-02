from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable

from .db import readonly_connect, write_transaction
from .observability import redact
from .telegram_ui import PanelView, TelegramPanelRenderer


def _view_digest(view: PanelView) -> str:
    payload = {
        "text": str(view.text or ""),
        "reply_markup": view.reply_markup or {},
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _extract_message_id(response: Any) -> int | None:
    if not isinstance(response, dict):
        return None
    result = response.get("result")
    if isinstance(result, dict) and result.get("message_id") is not None:
        try:
            return int(result["message_id"])
        except (TypeError, ValueError):
            return None
    if response.get("message_id") is not None:
        try:
            return int(response["message_id"])
        except (TypeError, ValueError):
            return None
    return None


class PanelSessionRepository:
    """Singleton Telegram control-panel session (one admin, one console)."""

    def __init__(self, state_db: str | Path, *, now: Callable[[], int] | None = None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def get(self) -> dict[str, Any] | None:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select * from telegram_panel_session where id=1"
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            con.close()

    def bind(self, chat_id: int | str, message_id: int) -> None:
        now = int(self.now())

        def txn(con) -> None:
            existing = con.execute(
                "select id from telegram_panel_session where id=1"
            ).fetchone()
            if existing is None:
                con.execute(
                    "insert into telegram_panel_session("
                    "id,chat_id,message_id,current_route,updated_at) "
                    "values(1,?,?, 'n:h',?)",
                    (str(chat_id), int(message_id), now),
                )
            else:
                con.execute(
                    "update telegram_panel_session set chat_id=?, message_id=?, "
                    "updated_at=? where id=1",
                    (str(chat_id), int(message_id), now),
                )

        write_transaction(self.state_db, txn)

    def set_route(self, route: str) -> None:
        now = int(self.now())
        route_text = str(route or "n:h")[:64]

        def txn(con) -> None:
            existing = con.execute(
                "select id from telegram_panel_session where id=1"
            ).fetchone()
            if existing is None:
                con.execute(
                    "insert into telegram_panel_session("
                    "id,chat_id,message_id,current_route,updated_at) "
                    "values(1,'',null,?,?)",
                    (route_text, now),
                )
            else:
                con.execute(
                    "update telegram_panel_session set current_route=?, updated_at=? "
                    "where id=1",
                    (route_text, now),
                )

        write_transaction(self.state_db, txn)

    def record_render(self, digest: str, refreshed_at: int) -> None:
        now = int(self.now())

        def txn(con) -> None:
            con.execute(
                "update telegram_panel_session set last_render_hash=?, "
                "last_refreshed_at=?, updated_at=? where id=1",
                (str(digest)[:64], int(refreshed_at), now),
            )

        write_transaction(self.state_db, txn)


class PersistentPanelController:
    """Edit one durable Telegram console message for all panel navigation."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        api,
        renderer: TelegramPanelRenderer,
        sessions: PanelSessionRepository | None = None,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.api = api
        self.renderer = renderer
        self.now = now or (lambda: int(time.time()))
        self.sessions = sessions or PanelSessionRepository(self.state_db, now=self.now)

    def open_home(self, chat_id: int) -> None:
        self.navigate(chat_id, "n:h")

    def navigate(self, chat_id: int, route: str) -> None:
        route_text = str(route or "n:h")
        self.sessions.set_route(route_text)
        view = self.render_route(route_text)
        self._publish(chat_id, view, route=route_text)

    def show_view(self, chat_id: int, route: str, view: PanelView) -> None:
        """Publish a pre-built view and persist the route."""
        route_text = str(route or "n:h")
        self.sessions.set_route(route_text)
        self._publish(chat_id, view, route=route_text)

    def refresh_now(self) -> None:
        session = self.sessions.get()
        if session is None or not session.get("chat_id"):
            return
        route = str(session.get("current_route") or "n:h")
        view = self.render_route(route)
        self._publish(int(session["chat_id"]), view, route=route, force=True)

    def refresh_if_due(self, interval_sec: int = 60) -> bool:
        session = self.sessions.get()
        if session is None or not session.get("chat_id"):
            return False
        route = str(session.get("current_route") or "n:h")
        if route != "n:h":
            return False
        last = session.get("last_refreshed_at")
        now = int(self.now())
        if last is not None and now - int(last) < max(1, int(interval_sec)):
            return False
        view = self.render_route("n:h")
        self._publish(int(session["chat_id"]), view, route="n:h")
        return True

    def render_route(self, route: str) -> PanelView:
        parts = str(route or "n:h").split(":")
        try:
            if parts[0] == "n" and parts[1] == "h":
                return self.renderer.render_home()
            if parts[0] == "n" and parts[1] == "s":
                if len(parts) >= 4 and parts[2] in {"d", "i", "e", "r", "m"}:
                    return self.renderer.render_status_history(
                        parts[2], int(parts[3])
                    )
                return self.renderer.render_status_home()
            if parts[0] == "n" and parts[1] == "q":
                page = int(parts[2]) if len(parts) > 2 else 0
                return self.renderer.render_queue(page)
            if parts[0] == "n" and parts[1] == "b":
                batch_id = int(parts[2])
                page = int(parts[3]) if len(parts) > 3 else 0
                return self.renderer.render_queue_detail(batch_id, page)
            if parts[0] == "n" and parts[1] == "w":
                page = int(parts[2]) if len(parts) > 2 else 0
                return self.renderer.render_warnings(page)
            if parts[0] == "a" and parts[1] == "d":
                batch_id = int(parts[2])
                return self.renderer.render_add_draft(batch_id)
            if parts[0] == "w" and parts[1] == "d":
                warning_id = int(parts[2])
                return self.renderer.render_warning_detail_by_id(warning_id)
        except Exception as exc:
            return PanelView(
                text=f"面板渲染失败：{str(redact(str(exc)))[:160]}",
                reply_markup={
                    "inline_keyboard": [
                        [{"text": "返回首页", "callback_data": "n:h"}]
                    ]
                },
            )
        return PanelView(
            text="未知页面。",
            reply_markup={
                "inline_keyboard": [[{"text": "返回首页", "callback_data": "n:h"}]]
            },
        )

    def _publish(
        self,
        chat_id: int,
        view: PanelView,
        *,
        route: str,
        force: bool = False,
    ) -> None:
        digest = _view_digest(view)
        session = self.sessions.get()
        if (
            not force
            and session is not None
            and session.get("message_id") is not None
            and str(session.get("last_render_hash") or "") == digest
            and str(session.get("current_route") or "") == str(route)
        ):
            return
        message_id = (
            int(session["message_id"])
            if session is not None and session.get("message_id") is not None
            else None
        )
        now = int(self.now())
        if message_id is not None:
            try:
                self.api.edit_message_text(
                    chat_id,
                    message_id,
                    view.text,
                    reply_markup=view.reply_markup,
                )
                self.sessions.record_render(digest, now)
                return
            except Exception:
                message_id = None
        response = self.api.send_message(
            chat_id, view.text, reply_markup=view.reply_markup
        )
        new_id = _extract_message_id(response)
        if new_id is None:
            # Tests/fakes may return {"ok": True} without message_id; keep prior.
            if session is not None and session.get("message_id") is not None:
                new_id = int(session["message_id"])
            else:
                new_id = 1
        self.sessions.bind(chat_id, new_id)
        self.sessions.set_route(route)
        self.sessions.record_render(digest, now)
