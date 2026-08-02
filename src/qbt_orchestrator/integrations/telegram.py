from __future__ import annotations

import json
import mimetypes
import time
import uuid
from typing import Any, Protocol
from urllib import error, parse, request

from ..observability import redact
from ..runtime import BotNotificationRepository
from ..telegram_control import RETIRED_TELEGRAM_COMMANDS, TelegramAuthorizer


class TelegramApiError(RuntimeError):
    def __init__(
        self,
        method: str,
        *,
        http_status: int | None = None,
        error_code: int | None = None,
        description: str = "",
        retry_after: int | None = None,
    ):
        self.method = str(method)
        self.http_status = http_status
        self.error_code = error_code
        self.description = str(redact(description or ""))[:300]
        self.retry_after = retry_after
        super().__init__(
            f"telegram {self.method} failed"
            + (f" http={self.http_status}" if self.http_status is not None else "")
            + (f" code={self.error_code}" if self.error_code is not None else "")
            + (f": {self.description}" if self.description else "")
        )

    def __str__(self) -> str:
        text = super().__str__()
        return str(redact(text))


class TelegramApiProtocol(Protocol):
    def get_updates(self, offset: int | None, timeout: int) -> list[dict[str, Any]]: ...
    def send_message(self, chat_id: int, text: str, reply_markup: dict | None = None) -> Any: ...
    def edit_message_text(
        self, chat_id: int, message_id: int, text: str, reply_markup: dict | None = None
    ) -> Any: ...
    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> Any: ...
    def send_document(
        self, chat_id: int, path: str, *, filename: str | None = None, caption: str | None = None
    ) -> Any: ...


class TelegramHttpApi:
    MAX_DOCUMENT_BYTES = 20 * 1024 * 1024

    def __init__(self, token: str, timeout: int = 30, sleeper=None):
        # Keep token off exception strings; only use it to build the base URL.
        self._token = str(token)
        self.base = f"https://api.telegram.org/bot{self._token}"
        self.timeout = timeout
        self.sleeper = sleeper or time.sleep

    def _post_form(self, method: str, payload: dict[str, Any], *, retry_on_429: bool = False) -> dict[str, Any]:
        data = parse.urlencode(payload).encode("utf-8")
        req = request.Request(
            f"{self.base}/{method}",
            data=data,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        return self._send(method, req, retry_on_429=retry_on_429)

    def _post_multipart(
        self,
        method: str,
        fields: dict[str, str],
        *,
        file_field: str,
        filename: str,
        content: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        boundary = f"----qbtBoundary{uuid.uuid4().hex}"
        body = bytearray()
        for key, value in fields.items():
            body.extend(
                (
                    f"--{boundary}\r\n"
                    f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
                    f"{value}\r\n"
                ).encode("utf-8")
            )
        safe_name = filename.replace('"', "")
        body.extend(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{file_field}"; filename="{safe_name}"\r\n'
                f"Content-Type: {content_type}\r\n\r\n"
            ).encode("utf-8")
        )
        body.extend(content)
        body.extend(f"\r\n--{boundary}--\r\n".encode("utf-8"))
        req = request.Request(
            f"{self.base}/{method}",
            data=bytes(body),
            method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        return self._send(method, req, retry_on_429=False)

    def _send(self, method: str, req: request.Request, *, retry_on_429: bool) -> dict[str, Any]:
        attempts = 0
        while True:
            attempts += 1
            try:
                with request.urlopen(req, timeout=self.timeout) as resp:
                    raw = resp.read().decode("utf-8")
                    http_status = int(getattr(resp, "status", 200) or 200)
            except error.HTTPError as exc:
                raw = exc.read().decode("utf-8", errors="replace")
                http_status = int(exc.code)
                body = self._parse_body(raw, method, http_status)
                description = str(body.get("description") or "")
                if (
                    method == "editMessageText"
                    and http_status == 400
                    and "message is not modified" in description.lower()
                ):
                    return {"ok": True, "result": True, "description": description}
                retry_after = self._retry_after(body)
                if retry_on_429 and http_status == 429 and attempts == 1:
                    self.sleeper(min(int(retry_after or 1), 30))
                    continue
                raise TelegramApiError(
                    method,
                    http_status=http_status,
                    error_code=int(body.get("error_code") or http_status),
                    description=description,
                    retry_after=retry_after,
                ) from None
            except error.URLError as exc:
                raise TelegramApiError(method, description=str(exc.reason)) from None

            body = self._parse_body(raw, method, http_status)
            if body.get("ok"):
                return body
            description = str(body.get("description") or "telegram api rejected request")
            if (
                method == "editMessageText"
                and "message is not modified" in description.lower()
            ):
                return {"ok": True, "result": True, "description": description}
            retry_after = self._retry_after(body)
            if retry_on_429 and int(body.get("error_code") or 0) == 429 and attempts == 1:
                self.sleeper(min(int(retry_after or 1), 30))
                continue
            raise TelegramApiError(
                method,
                http_status=http_status,
                error_code=int(body.get("error_code") or 0) or None,
                description=description,
                retry_after=retry_after,
            )

    @staticmethod
    def _parse_body(raw: str, method: str, http_status: int) -> dict[str, Any]:
        try:
            body = json.loads(raw)
        except Exception as exc:
            raise TelegramApiError(
                method, http_status=http_status, description="malformed json response"
            ) from exc
        if not isinstance(body, dict):
            raise TelegramApiError(
                method, http_status=http_status, description="non-object json response"
            )
        return body

    @staticmethod
    def _retry_after(body: dict[str, Any]) -> int | None:
        params = body.get("parameters") or {}
        if not isinstance(params, dict):
            return None
        value = params.get("retry_after")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def get_updates(self, offset: int | None, timeout: int) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": timeout,
            "allowed_updates": json.dumps(["message", "callback_query"]),
        }
        if offset is not None:
            payload["offset"] = offset
        return list(self._post_form("getUpdates", payload).get("result", []))

    def send_message(self, chat_id: int, text: str, reply_markup: dict | None = None) -> Any:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)
        return self._post_form("sendMessage", payload)

    def edit_message_text(
        self, chat_id: int, message_id: int, text: str, reply_markup: dict | None = None
    ) -> Any:
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        }
        if reply_markup is not None:
            payload["reply_markup"] = json.dumps(reply_markup)
        return self._post_form("editMessageText", payload, retry_on_429=True)

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> Any:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = str(text)[:200]
        return self._post_form("answerCallbackQuery", payload)

    def send_document(
        self, chat_id: int, path: str, *, filename: str | None = None, caption: str | None = None
    ) -> Any:
        from pathlib import Path

        file_path = Path(path)
        size = file_path.stat().st_size
        if size > self.MAX_DOCUMENT_BYTES:
            raise TelegramApiError(
                "sendDocument",
                description=f"document too large: {size} bytes",
            )
        content = file_path.read_bytes()
        name = filename or file_path.name
        content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
        fields = {"chat_id": str(chat_id)}
        if caption:
            fields["caption"] = str(caption)[:1024]
        return self._post_multipart(
            "sendDocument",
            fields,
            file_field="document",
            filename=name,
            content=content,
            content_type=content_type,
        )


class TelegramPollingService:
    def __init__(
        self,
        api: TelegramApiProtocol,
        authorizer: TelegramAuthorizer,
        command_store,
        poll_timeout: int = 30,
        router=None,
    ):
        self.api = api
        self.authorizer = authorizer
        self.command_store = command_store
        self.poll_timeout = poll_timeout
        self.router = router
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
        if self.router is not None:
            self.router.handle_update(update)
            return
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
        if command in RETIRED_TELEGRAM_COMMANDS:
            self.api.send_message(
                chat_id, "该命令已停用；请使用控制台面板查看状态与警告。"
            )
            return
        if not self.authorizer.allowed(user_id, command):
            self.api.send_message(chat_id, "unauthorized")
            return
        if self.command_store is not None:
            self.command_store.insert_command(
                f"tg-{update.get('update_id')}",
                chat_id,
                user_id,
                command,
                {"args": args, "text": text},
            )

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
    """Drain allowlisted bot_notifications to Telegram sendMessage.

    Only confirmation prompts and final batch summaries are sent as standalone
    messages. Everything else is suppressed so the persistent panel remains the
    primary console surface.
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
        if not self._allowed(row):
            self.repo.mark_suppressed(notification_id, reason="panel_only_policy")
            return notification_id
        try:
            self.api.send_message(
                int(row["chat_id"]),
                str(row["message"]),
                reply_markup=self._reply_markup(row),
            )
        except Exception as exc:
            self.repo.schedule_retry(
                notification_id,
                error=str(redact(str(exc))),
                delay_sec=self.retry_delay,
            )
            return notification_id
        self.repo.mark_sent(notification_id)
        return notification_id

    def _allowed(self, row: dict[str, Any]) -> bool:
        topic = str(row.get("topic") or "")
        if topic == "download_confirmation":
            return True
        if topic == "add_batch_summary":
            payload = self._payload(row)
            return str(payload.get("summary") or "") == "final"
        return False

    def _payload(self, row: dict[str, Any]) -> dict[str, Any]:
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except Exception:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _reply_markup(self, row: dict[str, Any]) -> dict[str, Any] | None:
        reply_markup = self._payload(row).get("reply_markup")
        return reply_markup if isinstance(reply_markup, dict) else None
