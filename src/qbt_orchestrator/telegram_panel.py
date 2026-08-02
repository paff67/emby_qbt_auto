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


def _is_panel_message_unavailable(exc: BaseException) -> bool:
    """True only for explicit Telegram 400s that mean the panel message is gone."""
    from .integrations.telegram import TelegramApiError

    if not isinstance(exc, TelegramApiError):
        return False
    status = exc.http_status if exc.http_status is not None else exc.error_code
    if status != 400:
        return False
    text = str(exc.description or "").lower()
    return (
        "message to edit not found" in text
        or "message can't be edited" in text
    )


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
                "last_refreshed_at=?, last_refresh_attempt_at=?, updated_at=? "
                "where id=1",
                (str(digest)[:64], int(refreshed_at), int(refreshed_at), now),
            )

        write_transaction(self.state_db, txn)

    def record_refresh_attempt(self, attempted_at: int) -> None:
        now = int(self.now())

        def txn(con) -> None:
            con.execute(
                "update telegram_panel_session set last_refresh_attempt_at=?, "
                "updated_at=? where id=1",
                (int(attempted_at), now),
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
        # /start is the only path allowed to rebind the singleton to a new chat.
        self.navigate(chat_id, "n:h", allow_rebind=True)

    def navigate(
        self, chat_id: int, route: str, *, allow_rebind: bool = False
    ) -> None:
        if not self._may_publish_to_chat(chat_id, allow_rebind=allow_rebind):
            return
        route_text = str(route or "n:h")
        self.sessions.set_route(route_text)
        view = self.render_route(route_text)
        self._publish(
            chat_id, view, route=route_text, allow_rebind=allow_rebind
        )

    def show_view(
        self,
        chat_id: int,
        route: str,
        view: PanelView,
        *,
        allow_rebind: bool = False,
    ) -> None:
        """Publish a pre-built view and persist the route."""
        if not self._may_publish_to_chat(chat_id, allow_rebind=allow_rebind):
            return
        route_text = str(route or "n:h")
        self.sessions.set_route(route_text)
        self._publish(
            chat_id, view, route=route_text, allow_rebind=allow_rebind
        )

    def _may_publish_to_chat(self, chat_id: int, *, allow_rebind: bool) -> bool:
        session = self.sessions.get()
        if session is None or session.get("message_id") is None:
            return True
        if str(session.get("chat_id") or "") == str(chat_id):
            return True
        return bool(allow_rebind)

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
        now = int(self.now())
        interval = max(1, int(interval_sec))
        gate: int | None = None
        last_refreshed = session.get("last_refreshed_at")
        last_attempt = session.get("last_refresh_attempt_at")
        if last_refreshed is not None:
            gate = int(last_refreshed)
        if last_attempt is not None:
            attempt = int(last_attempt)
            gate = attempt if gate is None else max(gate, attempt)
        if gate is not None and now - gate < interval:
            return False
        # Persist attempt before publish so failures still enforce backoff.
        self.sessions.record_refresh_attempt(now)
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
        allow_rebind: bool = False,
    ) -> None:
        digest = _view_digest(view)
        session = self.sessions.get()
        same_chat = (
            session is not None
            and str(session.get("chat_id") or "") == str(chat_id)
        )
        if (
            not force
            and same_chat
            and session.get("message_id") is not None
            and str(session.get("last_render_hash") or "") == digest
            and str(session.get("current_route") or "") == str(route)
        ):
            return
        message_id = (
            int(session["message_id"])
            if same_chat and session.get("message_id") is not None
            else None
        )
        now = int(self.now())
        if message_id is not None:
            from .integrations.telegram import TelegramApiError

            try:
                self.api.edit_message_text(
                    chat_id,
                    message_id,
                    view.text,
                    reply_markup=view.reply_markup,
                )
            except TelegramApiError as exc:
                if not _is_panel_message_unavailable(exc):
                    raise
            else:
                # Keep persistence outside the edit except-block so a DB failure
                # after a successful edit never falls through to sendMessage.
                self.sessions.record_render(digest, now)
                return
        elif (
            session is not None
            and session.get("message_id") is not None
            and not allow_rebind
        ):
            # Bound to another chat; refuse silent cross-chat create/rebind.
            return
        response = self.api.send_message(
            chat_id, view.text, reply_markup=view.reply_markup
        )
        new_id = _extract_message_id(response)
        if new_id is None:
            # Tests/fakes may return {"ok": True} without message_id; keep prior.
            if same_chat and session is not None and session.get("message_id") is not None:
                new_id = int(session["message_id"])
            else:
                new_id = 1
        self.sessions.bind(chat_id, new_id)
        self.sessions.set_route(route)
        self.sessions.record_render(digest, now)
