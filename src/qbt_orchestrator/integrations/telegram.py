from __future__ import annotations

import json
from typing import Any, Protocol
from urllib import parse, request

from ..observability import redact
from ..runtime import BotNotificationRepository
from ..telegram_control import TelegramAuthorizer


class TelegramApiProtocol(Protocol):
    def get_updates(self, offset: int | None, timeout: int) -> list[dict[str, Any]]: ...
    def send_message(self, chat_id: int, text: str, reply_markup: dict | None = None) -> Any: ...


class TelegramHttpApi:
    def __init__(self, token: str, timeout: int = 30):
        self.base = f"https://api.telegram.org/bot{token}"
        self.timeout = timeout

    def _post(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        data = parse.urlencode(payload).encode("utf-8")
        req = request.Request(f"{self.base}/{method}", data=data, method="POST")
        with request.urlopen(req, timeout=self.timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if not body.get("ok"):
            raise RuntimeError(f"telegram {method} failed")
        return body

    def get_updates(self, offset: int | None, timeout: int) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"timeout": timeout, "allowed_updates": json.dumps(["message", "callback_query"])}
        if offset is not None:
            payload["offset"] = offset
        return list(self._post("getUpdates", payload).get("result", []))

    def send_message(self, chat_id: int, text: str, reply_markup: dict | None = None) -> Any:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)
        return self._post("sendMessage", payload)


class TelegramPollingService:
    def __init__(self, api: TelegramApiProtocol, authorizer: TelegramAuthorizer, command_store, poll_timeout: int = 30):
        self.api = api
        self.authorizer = authorizer
        self.command_store = command_store
        self.poll_timeout = poll_timeout
        self.next_offset: int | None = None
        self.consecutive_failures = 0

    def poll_once(self) -> int:
        try:
            updates = self.api.get_updates(self.next_offset, self.poll_timeout)
        except Exception:
            self.consecutive_failures += 1
            return 0
        self.consecutive_failures = 0
        for update in updates:
            self.next_offset = max(self.next_offset or 0, int(update.get("update_id", 0)) + 1)
            self._handle_update(update)
        return len(updates)

    def _handle_update(self, update: dict[str, Any]) -> None:
        if update.get("callback_query"):
            self._handle_callback(update)
            return
        msg = update.get("message") or {}
        if not msg:
            return
        text = str(msg.get("text") or "")
        if not text.startswith("/"):
            return
        chat_id = int(msg.get("chat", {}).get("id"))
        user_id = int(msg.get("from", {}).get("id"))
        parts = text[1:].split()
        command = parts[0].replace("-", "_") if parts else ""
        args = parts[1:]
        if not self.authorizer.allowed(user_id, command):
            self.api.send_message(chat_id, "unauthorized")
            return
        if self.command_store is not None:
            self.command_store.insert_command(f"tg-{update.get('update_id')}", chat_id, user_id, command, {"args": args, "text": text})

    def _handle_callback(self, update: dict[str, Any]) -> None:
        callback = update.get("callback_query") or {}
        data = str(callback.get("data") or "")
        if ":" not in data:
            return
        action, approval_id = data.split(":", 1)
        action = action.replace("-", "_")
        chat_id = int((callback.get("message") or {}).get("chat", {}).get("id"))
        user_id = int((callback.get("from") or {}).get("id"))
        if action not in {"approve", "deny"}:
            return
        if not self.authorizer.allowed(user_id, action):
            self.api.send_message(chat_id, "unauthorized")
            return
        ok = False
        if self.command_store is not None:
            if action == "approve" and hasattr(self.command_store, "approve_once"):
                ok = bool(self.command_store.approve_once(approval_id, user_id))
            elif action == "deny" and hasattr(self.command_store, "deny_once"):
                ok = bool(self.command_store.deny_once(approval_id, user_id))
        if ok:
            self.api.send_message(chat_id, "approved" if action == "approve" else "denied")
        else:
            self.api.send_message(chat_id, "approval unavailable")


class TelegramNotificationSender:
    """Drain persistent bot_notifications to Telegram sendMessage.

    Polling only records commands/approvals in SQLite.  This sender is the
    opposite direction and is intentionally queue-backed so daemon restarts do
    not lose status/trace/perf replies.
    """

    def __init__(self, repo: BotNotificationRepository, api: TelegramApiProtocol, retry_delay: int = 60):
        self.repo = repo
        self.api = api
        self.retry_delay = retry_delay

    def has_pending(self) -> bool:
        return self.repo.peek_next() is not None

    def send_next(self) -> int | None:
        row = self.repo.claim_next()
        if row is None:
            return None
        notification_id = int(row["id"])
        try:
            self.api.send_message(int(row["chat_id"]), str(row["message"]), reply_markup=self._reply_markup(row))
        except Exception as exc:
            self.repo.schedule_retry(notification_id, error=str(redact(str(exc))), delay_sec=self.retry_delay)
            return notification_id
        self.repo.mark_sent(notification_id)
        return notification_id

    def _reply_markup(self, row: dict[str, Any]) -> dict[str, Any] | None:
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except Exception:
            return None
        reply_markup = payload.get("reply_markup")
        return reply_markup if isinstance(reply_markup, dict) else None

