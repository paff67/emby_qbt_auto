from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Callable

from .bot_add_queue import BotAddQueueRepository
from .observability import redact
from .telegram_control import TelegramAuthorizer
from .telegram_ui import DashboardRepository, TelegramPanelRenderer, encode_callback
from .warning_inbox import WarningInboxRepository


PANEL_COMMANDS = frozenset({"start", "status", "queue", "warnings", "add", "help"})
MUTATING_COMMANDS = frozenset({"add", "approve", "deny"})


class TelegramUpdateRouter:
    """Single update router for commands, navigation, add queue, approvals, warnings."""

    def __init__(
        self,
        *,
        api,
        authorizer: TelegramAuthorizer,
        command_store=None,
        state_db: str | Path | None = None,
        add_queue: BotAddQueueRepository | None = None,
        warnings: WarningInboxRepository | None = None,
        panel_enabled: bool = False,
        admin_user_id: str | None = None,
        now: Callable[[], int] | None = None,
    ):
        self.api = api
        self.authorizer = authorizer
        self.command_store = command_store
        self.state_db = Path(state_db) if state_db else None
        self.add_queue = add_queue
        self.warnings = warnings
        self.panel_enabled = bool(panel_enabled)
        self.admin_user_id = str(admin_user_id) if admin_user_id else None
        self.now = now or (lambda: __import__("time").time())
        self.renderer = None
        if self.state_db is not None:
            self.renderer = TelegramPanelRenderer(DashboardRepository(self.state_db, now=lambda: int(self.now())))
            if self.warnings is None:
                self.warnings = WarningInboxRepository(self.state_db, now=lambda: int(self.now()))

    def handle_update(self, update: dict[str, Any]) -> None:
        if update.get("callback_query"):
            self._handle_callback(update)
            return
        msg = update.get("message") or {}
        if not msg:
            return
        chat_id = int(msg.get("chat", {}).get("id"))
        user_id = int((msg.get("from") or {}).get("id") or 0)
        if msg.get("document"):
            self._reject_document(chat_id, user_id, msg.get("document") or {})
            return
        text = str(msg.get("text") or "")
        if text.startswith("/"):
            self._handle_command(update, chat_id, user_id, text)
            return
        if self.panel_enabled and self.add_queue is not None and text.strip():
            self._handle_add_text(chat_id, user_id, text, msg)

    def _handle_command(self, update: dict[str, Any], chat_id: int, user_id: int, text: str) -> None:
        parts = text[1:].split()
        command = parts[0].split("@", 1)[0].replace("-", "_") if parts else ""
        args = parts[1:]
        if command in PANEL_COMMANDS and self.panel_enabled and self.renderer is not None:
            if not self.authorizer.role_for(user_id):
                self.api.send_message(chat_id, "无权访问")
                return
            if command in MUTATING_COMMANDS and not self._can_mutate(user_id):
                self.api.send_message(chat_id, "只读账号不能执行此操作")
                return
            if command in {"start", "help"}:
                view = self.renderer.render_home()
                self.api.send_message(chat_id, view.text, reply_markup=view.reply_markup)
                return
            if command == "status":
                view = self.renderer.render_status(0)
                self.api.send_message(chat_id, view.text, reply_markup=view.reply_markup)
                return
            if command == "queue":
                view = self.renderer.render_queue(0)
                self.api.send_message(chat_id, view.text, reply_markup=view.reply_markup)
                return
            if command == "warnings":
                view = self.renderer.render_warnings(0)
                self.api.send_message(chat_id, view.text, reply_markup=view.reply_markup)
                return
            if command == "add":
                self.api.send_message(chat_id, "请发送磁力链或下载链接，完成后点击提交。")
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

    def _handle_add_text(self, chat_id: int, user_id: int, text: str, msg: dict[str, Any]) -> None:
        if not self._can_mutate(user_id) or self.add_queue is None:
            return
        try:
            self.add_queue.append_draft_text(
                chat_id=str(chat_id),
                user_id=str(user_id),
                text=text,
                source_message_id=str(msg.get("message_id") or ""),
            )
        except AttributeError:
            # Repository may expose a different ingress method name in older builds.
            return
        except ValueError as exc:
            self.api.send_message(chat_id, f"未能加入草稿：{redact(str(exc))}")

    def _reject_document(self, chat_id: int, user_id: int, document: dict[str, Any]) -> None:
        if not self.authorizer.role_for(user_id):
            return
        name = str(document.get("file_name") or "").lower()
        mime = str(document.get("mime_type") or "").lower()
        if name.endswith(".torrent") or "bittorrent" in mime:
            self.api.send_message(chat_id, "不支持上传 .torrent 文件，请发送磁力链或下载链接。")

    def _handle_callback(self, update: dict[str, Any]) -> None:
        callback = update.get("callback_query") or {}
        data = str(callback.get("data") or "")
        chat = (callback.get("message") or {}).get("chat") or {}
        chat_id = int(chat.get("id") or 0)
        user_id = int((callback.get("from") or {}).get("id") or 0)
        message_id = int((callback.get("message") or {}).get("message_id") or 0)
        callback_id = str(callback.get("id") or "")

        # Legacy approve/deny remain accepted for one release.
        if data.startswith("approve:") or data.startswith("deny:"):
            self._legacy_approval(callback, data, chat_id, user_id, callback_id)
            return

        if not self.panel_enabled or self.renderer is None:
            self._answer(callback_id, "面板未启用")
            return
        if not self.authorizer.role_for(user_id):
            self._answer(callback_id, "无权访问")
            return

        parts = data.split(":")
        try:
            if parts[0] == "n" and parts[1] == "h":
                view = self.renderer.render_home()
            elif parts[0] == "n" and parts[1] == "s":
                view = self.renderer.render_status(int(parts[2]))
            elif parts[0] == "n" and parts[1] == "q":
                view = self.renderer.render_queue(int(parts[2]))
            elif parts[0] == "n" and parts[1] == "w":
                view = self.renderer.render_warnings(int(parts[2]))
            elif parts[0] == "a" and parts[1] == "o":
                if not self._can_mutate(user_id):
                    self._answer(callback_id, "只读")
                    return
                self._answer(callback_id, "请发送链接")
                self.api.send_message(chat_id, "请发送磁力链或下载链接，完成后点击提交。")
                return
            elif parts[0] == "w" and parts[1] == "d":
                view = self._warning_detail(int(parts[2]), int(parts[3]), user_id)
            elif parts[0] == "w" and parts[1] == "r":
                ok = False
                if self.warnings is not None:
                    ok = self.warnings.mark_read(
                        int(parts[2]),
                        expected_occurrence=int(parts[3]),
                        admin_id=str(user_id),
                    )
                self._answer(callback_id, "已标为已读" if ok else "状态已变化")
                view = self.renderer.render_warnings(0)
            elif parts[0] == "w" and parts[1] == "x":
                self._export_warning(chat_id, int(parts[2]))
                self._answer(callback_id, "已导出")
                return
            elif parts[0] == "ap":
                self._compact_approval(parts, chat_id, user_id, callback_id)
                return
            else:
                self._answer(callback_id, "未知操作")
                return
            self.api.edit_message_text(
                chat_id, message_id, view.text, reply_markup=view.reply_markup
            )
            self._answer(callback_id)
        except Exception as exc:
            self._answer(callback_id, str(redact(str(exc)))[:180])

    def _warning_detail(self, warning_id: int, occurrence: int, user_id: int):
        assert self.warnings is not None and self.renderer is not None
        rows = [
            row
            for row in self.warnings.list_recent(limit=100)
            if int(row["id"]) == warning_id
        ]
        if not rows:
            return self.renderer.render_warnings(0)
        warning = rows[0]
        # Opening detail marks that occurrence read.
        self.warnings.mark_read(
            warning_id, expected_occurrence=occurrence, admin_id=str(user_id)
        )
        copy_text = self.warnings.copy_summary(warning_id)
        return self.renderer.render_warning_detail(warning, copy_text=copy_text)

    def _export_warning(self, chat_id: int, warning_id: int) -> None:
        assert self.warnings is not None
        payload = self.warnings.export_text(warning_id)
        if not payload:
            self.api.send_message(chat_id, "没有可导出的内容")
            return
        fd, path = tempfile.mkstemp(prefix="warning-export-", suffix=".txt")
        os.close(fd)
        try:
            Path(path).write_bytes(payload)
            self.api.send_document(chat_id, path, filename=f"warning-{warning_id}.txt")
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def _legacy_approval(
        self, callback: dict[str, Any], data: str, chat_id: int, user_id: int, callback_id: str
    ) -> None:
        action, approval_id = data.split(":", 1)
        action = action.replace("-", "_")
        if action not in {"approve", "deny"}:
            return
        if not self.authorizer.allowed(user_id, action):
            self.api.send_message(chat_id, "unauthorized")
            self._answer(callback_id, "unauthorized")
            return
        ok = False
        if self.command_store is not None:
            if action == "approve" and hasattr(self.command_store, "approve_once"):
                ok = bool(self.command_store.approve_once(approval_id, user_id))
            elif action == "deny" and hasattr(self.command_store, "deny_once"):
                ok = bool(self.command_store.deny_once(approval_id, user_id))
        self.api.send_message(chat_id, "approved" if ok else "approval unavailable" if action == "approve" else "denied" if ok else "approval unavailable")
        self._answer(callback_id)

    def _compact_approval(
        self, parts: list[str], chat_id: int, user_id: int, callback_id: str
    ) -> None:
        if len(parts) < 4:
            self._answer(callback_id, "无效审批")
            return
        action = "approve" if parts[1] == "y" else "deny"
        approval_id = parts[2]
        if not self.authorizer.allowed(user_id, action):
            self._answer(callback_id, "unauthorized")
            return
        ok = False
        if self.command_store is not None:
            if action == "approve" and hasattr(self.command_store, "approve_once"):
                ok = bool(self.command_store.approve_once(approval_id, user_id))
            elif action == "deny" and hasattr(self.command_store, "deny_once"):
                ok = bool(self.command_store.deny_once(approval_id, user_id))
        self._answer(callback_id, "已处理" if ok else "不可用")
        self.api.send_message(chat_id, "approved" if ok and action == "approve" else "denied" if ok else "approval unavailable")

    def _can_mutate(self, user_id: int) -> bool:
        role = self.authorizer.role_for(user_id)
        if role in {"admin", "operator"}:
            return True
        if self.admin_user_id and str(user_id) == self.admin_user_id:
            return True
        return False

    def _answer(self, callback_id: str, text: str | None = None) -> None:
        if not callback_id:
            return
        try:
            self.api.answer_callback_query(callback_id, text=text)
        except Exception:
            return
