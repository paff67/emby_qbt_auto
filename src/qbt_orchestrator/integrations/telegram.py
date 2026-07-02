from __future__ import annotations

import json
from typing import Any, Protocol
from urllib import parse, request

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

