from __future__ import annotations

import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from .bot_add_queue import BotAddQueueRepository
from .observability import redact
from .telegram_control import RETIRED_TELEGRAM_COMMANDS, TelegramAuthorizer
from .telegram_panel import PersistentPanelController
from .telegram_ui import (
    DashboardRepository,
    PanelView,
    TelegramPanelRenderer,
    _btn,
    encode_callback,
)
from .warning_inbox import WarningInboxRepository


PANEL_COMMANDS = frozenset({"start", "status", "queue", "warnings", "add", "help"})
MUTATING_COMMANDS = frozenset({"add", "approve", "deny"})
_LINK_SPLIT = re.compile(r"\s+")


def extract_links_from_text(text: str) -> list[str]:
    """Split a message into candidate links; empty lines/tokens are dropped."""
    links: list[str] = []
    for line in str(text or "").splitlines():
        for token in _LINK_SPLIT.split(line.strip()):
            candidate = token.strip()
            if candidate:
                links.append(candidate)
    return links


class TelegramUpdateRouter:
    """Single update router that drives one persistent Telegram console message."""

    def __init__(
        self,
        *,
        api,
        authorizer: TelegramAuthorizer,
        command_store=None,
        state_db: str | Path | None = None,
        add_queue: BotAddQueueRepository | None = None,
        checked_add=None,
        warnings: WarningInboxRepository | None = None,
        panel: PersistentPanelController | None = None,
        panel_enabled: bool = False,
        admin_user_id: str | None = None,
        now: Callable[[], int] | None = None,
    ):
        self.api = api
        self.authorizer = authorizer
        self.command_store = command_store
        self.state_db = Path(state_db) if state_db else None
        self.add_queue = add_queue
        self.checked_add = checked_add
        self.warnings = warnings
        self.panel_enabled = bool(panel_enabled)
        self.admin_user_id = str(admin_user_id) if admin_user_id else None
        self.now = now or (lambda: int(time.time()))
        self.renderer = None
        self.panel = panel
        if self.state_db is not None:
            self.renderer = TelegramPanelRenderer(
                DashboardRepository(self.state_db), now=self.now
            )
            if self.warnings is None:
                self.warnings = WarningInboxRepository(
                    self.state_db, now=lambda: int(self.now())
                )
            if self.panel is None and self.panel_enabled:
                self.panel = PersistentPanelController(
                    self.state_db,
                    api=self.api,
                    renderer=self.renderer,
                    now=self.now,
                )

    def refresh_panel_if_due(self, interval_sec: int = 60) -> bool:
        if self.panel is None:
            return False
        return bool(self.panel.refresh_if_due(interval_sec=interval_sec))

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

    def _handle_command(
        self, update: dict[str, Any], chat_id: int, user_id: int, text: str
    ) -> None:
        parts = text[1:].split()
        command = parts[0].split("@", 1)[0].replace("-", "_") if parts else ""
        args = parts[1:]
        if command in RETIRED_TELEGRAM_COMMANDS:
            message = "该命令已停用；请使用控制台面板查看状态与警告。"
            if self.panel is not None:
                self._panel_error(chat_id, message)
            else:
                self.api.send_message(chat_id, message)
            return
        if command in PANEL_COMMANDS and self.panel_enabled and self.panel is not None:
            if not self.authorizer.role_for(user_id):
                self._panel_error(chat_id, "无权访问")
                return
            if command in MUTATING_COMMANDS and not self._can_mutate(user_id):
                self._panel_error(chat_id, "只读账号不能执行此操作")
                return
            if command in {"start", "help"}:
                self.panel.open_home(chat_id)
                return
            if command == "status":
                self.panel.navigate(chat_id, "n:s:0")
                return
            if command == "queue":
                self.panel.navigate(chat_id, "n:q:0")
                return
            if command == "warnings":
                self.panel.navigate(chat_id, "n:w:0")
                return
            if command == "add":
                self._open_add_draft(chat_id, user_id)
                return
        if not self.authorizer.allowed(user_id, command):
            if self.panel is not None:
                self._panel_error(chat_id, "无权访问")
            else:
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

    def _open_add_draft(self, chat_id: int, user_id: int) -> None:
        if self.add_queue is None or self.panel is None or self.renderer is None:
            return
        draft = self.add_queue.open_draft(str(chat_id), str(user_id))
        batch_id = int(draft["id"])
        view = self.renderer.render_add_draft(batch_id)
        self.panel.show_view(chat_id, f"a:d:{batch_id}", view)

    def _handle_add_text(
        self, chat_id: int, user_id: int, text: str, msg: dict[str, Any]
    ) -> None:
        if (
            not self._can_mutate(user_id)
            or self.add_queue is None
            or self.panel is None
            or self.renderer is None
        ):
            return
        links = extract_links_from_text(text)
        if not links:
            return
        try:
            draft = self.add_queue.open_draft(str(chat_id), str(user_id))
            result = self.add_queue.append_message(
                int(draft["id"]),
                int(msg.get("message_id") or 0),
                links,
            )
        except ValueError as exc:
            session = self.panel.sessions.get() if self.panel else None
            route = str((session or {}).get("current_route") or "n:h")
            batch_id = None
            if route.startswith("a:d:"):
                try:
                    batch_id = int(route.split(":")[2])
                except (IndexError, ValueError):
                    batch_id = None
            if batch_id is None:
                try:
                    draft = self.add_queue.open_draft(str(chat_id), str(user_id))
                    batch_id = int(draft["id"])
                except Exception:
                    self._panel_error(chat_id, self._humanize_ingress_error(exc))
                    return
            view = self.renderer.render_add_draft(
                batch_id, error=self._humanize_ingress_error(exc)
            )
            self.panel.show_view(chat_id, f"a:d:{batch_id}", view)
            return
        batch_id = int(result["id"])
        view = self.renderer.render_add_draft(batch_id)
        self.panel.show_view(chat_id, f"a:d:{batch_id}", view)

    @staticmethod
    def _humanize_ingress_error(exc: Exception) -> str:
        code = str(exc)
        mapping = {
            "empty_message": "消息里没有可用链接",
            "unsupported_link_scheme": "存在不支持的链接格式；整条消息未接收",
            "batch_link_limit": "本批链接数量超过上限；整条消息未接收",
            "draft_byte_limit": "草稿总大小超过上限；整条消息未接收",
            "link_byte_limit": "单条链接过长；整条消息未接收",
            "duplicate_input": "消息内或草稿中存在重复链接；整条消息未接收",
            "batch_not_draft": "当前没有可写入的草稿",
            "draft_expired": "草稿已过期，请重新打开添加",
            "source_message_conflict": "该消息内容与已记录内容不一致",
            "global_backlog_limit": "全局待处理队列已满",
            "empty_batch": "草稿为空，无法提交",
            "draft_generation_conflict": "草稿已变化或已提交，请刷新后重试",
            "item_not_cancellable": "当前条目状态不允许取消",
            "qbt_write_fenced": "下载服务写入被保护，条目状态未改变",
            "approval_generation_conflict": "确认信息已过期，请从队列详情重新操作",
            "checked_add_unavailable": "确认服务未启用",
            "add_queue_unavailable": "添加队列未配置",
        }
        return mapping.get(code, str(redact(code))[:180])

    def _reject_document(
        self, chat_id: int, user_id: int, document: dict[str, Any]
    ) -> None:
        if not self.authorizer.role_for(user_id):
            return
        name = str(document.get("file_name") or "").lower()
        mime = str(document.get("mime_type") or "").lower()
        if name.endswith(".torrent") or "bittorrent" in mime:
            self._panel_error(
                chat_id, "不支持上传 .torrent 文件，请发送磁力链或下载链接。"
            )

    def _handle_callback(self, update: dict[str, Any]) -> None:
        callback = update.get("callback_query") or {}
        data = str(callback.get("data") or "")
        chat = (callback.get("message") or {}).get("chat") or {}
        chat_id = int(chat.get("id") or 0)
        user_id = int((callback.get("from") or {}).get("id") or 0)
        message_id = int((callback.get("message") or {}).get("message_id") or 0)
        callback_id = str(callback.get("id") or "")

        if data.startswith("approve:") or data.startswith("deny:"):
            self._answer(callback_id)
            self._legacy_approval(data, chat_id, user_id)
            return

        if not self.panel_enabled or self.panel is None or self.renderer is None:
            self._answer(callback_id, "面板未启用")
            return
        if not self.authorizer.role_for(user_id):
            self._answer(callback_id, "无权访问")
            return

        parts = data.split(":")
        try:
            if parts[0] == "n" and parts[1] == "rf":
                self._answer(callback_id, "已刷新")
                self.panel.refresh_now()
                return
            if parts[0] == "n" and parts[1] in {"h", "s", "q", "b", "w"}:
                self._answer(callback_id)
                self.panel.navigate(chat_id, data)
                return
            if parts[0] == "a" and parts[1] == "o":
                if not self._can_mutate(user_id):
                    self._answer(callback_id, "只读账号不能执行此操作")
                    return
                self._answer(callback_id)
                self._open_add_draft(chat_id, user_id)
                return
            if parts[0] == "a" and parts[1] in {"s", "c"}:
                self._answer(callback_id)
                self._handle_draft_action(parts, chat_id, user_id)
                return
            if parts[0] == "i" and parts[1] in {"y", "x", "r"}:
                toast = self._handle_item_action(parts, chat_id, user_id, message_id)
                self._answer(callback_id, toast)
                return
            if parts[0] == "w" and parts[1] == "d":
                self._answer(callback_id)
                warning_id = int(parts[2])
                self.panel.navigate(chat_id, f"w:d:{warning_id}")
                return
            if parts[0] == "w" and parts[1] == "r":
                ok = False
                if self.warnings is not None:
                    ok = self.warnings.mark_read(
                        int(parts[2]),
                        expected_occurrence=int(parts[3]),
                        admin_id=str(user_id),
                    )
                self._answer(callback_id, "已标为已读" if ok else "状态已变化")
                self.panel.navigate(chat_id, "n:w:0")
                return
            if parts[0] == "w" and parts[1] == "ra":
                count = 0
                if self.warnings is not None:
                    count = self.warnings.mark_all_read(
                        last_occurred_cutoff=int(self.now()),
                        admin_id=str(user_id),
                    )
                self._answer(callback_id, f"已读 {count} 条")
                self.panel.navigate(chat_id, "n:w:0")
                return
            if parts[0] == "w" and parts[1] == "x":
                self._answer(callback_id, "开始导出")
                self._export_warning(chat_id, int(parts[2]))
                return
            if parts[0] == "w" and parts[1] == "xa":
                self._answer(callback_id, "开始导出")
                self._export_warning(chat_id, None)
                return
            if parts[0] == "ap":
                self._answer(callback_id)
                self._compact_approval(parts, chat_id, user_id)
                return
            self._answer(callback_id, "未知操作")
            self._panel_error(chat_id, "未知操作")
        except Exception as exc:
            self._answer(callback_id)
            self._panel_error(chat_id, str(redact(str(exc)))[:180])

    def _handle_draft_action(
        self, parts: list[str], chat_id: int, user_id: int
    ) -> None:
        if not self._can_mutate(user_id) or self.add_queue is None or self.panel is None:
            self._panel_error(chat_id, "只读账号不能执行此操作")
            return
        batch_id = int(parts[2])
        expected_gen = int(parts[3])
        batch = self.add_queue.get_batch(batch_id)
        if str(batch.get("chat_id")) != str(chat_id) or str(batch.get("user_id")) != str(
            user_id
        ):
            self._panel_error(chat_id, "无权操作该草稿")
            return
        try:
            if parts[1] == "s":
                self.add_queue.submit_draft(batch_id, expected_gen)
                self.panel.navigate(chat_id, f"n:b:{batch_id}:0")
            else:
                self.add_queue.cancel_draft(
                    batch_id, expected_gen, actor=str(user_id)
                )
                self.panel.open_home(chat_id)
        except ValueError as exc:
            if self.renderer is None:
                return
            view = self.renderer.render_add_draft(
                batch_id, error=self._humanize_ingress_error(exc)
            )
            self.panel.show_view(chat_id, f"a:d:{batch_id}", view)

    def _handle_item_action(
        self,
        parts: list[str],
        chat_id: int,
        user_id: int,
        message_id: int,
    ) -> str | None:
        if not self._can_mutate(user_id):
            return "只读账号不能执行此操作"
        item_id = int(parts[2])
        generation = int(parts[3])
        action = parts[1]
        panel_message_id = None
        if self.panel is not None:
            session = self.panel.sessions.get()
            if session is not None and session.get("message_id") is not None:
                panel_message_id = int(session["message_id"])
        on_panel = panel_message_id is not None and int(message_id) == panel_message_id
        try:
            if action == "y":
                if self.checked_add is None:
                    raise ValueError("checked_add_unavailable")
                self.checked_add.approve_hold(item_id, str(user_id), generation)
                result_text = "✅ 已确认并保持暂停"
            elif action == "x":
                if self.add_queue is None:
                    raise ValueError("add_queue_unavailable")
                item = self.add_queue.get_item(item_id)
                state = str(item.get("state") or "")
                if state == "needs_confirmation":
                    if self.checked_add is None:
                        raise ValueError("checked_add_unavailable")
                    self.checked_add.cancel(item_id, str(user_id), generation)
                    result_text = "🗑 已取消，未删除已有文件"
                elif state == "metadata_unavailable":
                    self.add_queue.transition_item(
                        item_id,
                        {"metadata_unavailable"},
                        "cancelled",
                        "cancelled_by_operator",
                        approval_generation=generation,
                        metadata_action="cancel",
                    )
                    result_text = "🗑 已取消"
                else:
                    raise ValueError("item_not_cancellable")
            elif action == "r":
                if self.add_queue is None:
                    raise ValueError("add_queue_unavailable")
                self.add_queue.transition_item(
                    item_id,
                    {"metadata_unavailable"},
                    "waiting_probe_slot",
                    "manual_retry_now",
                    approval_generation=generation,
                    metadata_action="retry_now",
                )
                result_text = "已安排重新获取元数据"
            else:
                return "未知操作"
        except ValueError as exc:
            if on_panel and self.panel is not None:
                self._panel_error(chat_id, self._humanize_ingress_error(exc))
            return self._humanize_ingress_error(exc)

        if on_panel and self.panel is not None:
            session = self.panel.sessions.get() or {}
            route = str(session.get("current_route") or "n:h")
            self.panel.navigate(chat_id, route)
            return result_text
        # Independent confirmation message: edit in place, no new messages.
        try:
            self.api.edit_message_text(
                chat_id, message_id, result_text, reply_markup=None
            )
        except Exception:
            pass
        return result_text

    def _export_warning(self, chat_id: int, warning_id: int | None) -> None:
        assert self.warnings is not None
        payload = self.warnings.export_text(warning_id)
        if not payload:
            self._panel_error(chat_id, "没有可导出的内容")
            return
        fd, path = tempfile.mkstemp(prefix="warning-export-", suffix=".txt")
        os.close(fd)
        try:
            Path(path).write_bytes(payload)
            name = (
                f"warning-{warning_id}.txt"
                if warning_id is not None
                else "warnings-all.txt"
            )
            self.api.send_document(chat_id, path, filename=name)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass

    def _legacy_approval(self, data: str, chat_id: int, user_id: int) -> None:
        action, approval_id = data.split(":", 1)
        action = action.replace("-", "_")
        if action not in {"approve", "deny"}:
            return
        if not self.authorizer.allowed(user_id, action):
            self._panel_error(chat_id, "无权访问")
            return
        ok = False
        if self.command_store is not None:
            if action == "approve" and hasattr(self.command_store, "approve_once"):
                ok = bool(self.command_store.approve_once(approval_id, user_id))
            elif action == "deny" and hasattr(self.command_store, "deny_once"):
                ok = bool(self.command_store.deny_once(approval_id, user_id))
        self._panel_error(
            chat_id,
            "approved"
            if ok and action == "approve"
            else "denied"
            if ok
            else "approval unavailable",
        )

    def _compact_approval(
        self, parts: list[str], chat_id: int, user_id: int
    ) -> None:
        if len(parts) < 4:
            self._panel_error(chat_id, "无效审批")
            return
        action = "approve" if parts[1] == "y" else "deny"
        approval_id = parts[2]
        if not self.authorizer.allowed(user_id, action):
            self._panel_error(chat_id, "无权访问")
            return
        ok = False
        if self.command_store is not None:
            if action == "approve" and hasattr(self.command_store, "approve_once"):
                ok = bool(self.command_store.approve_once(approval_id, user_id))
            elif action == "deny" and hasattr(self.command_store, "deny_once"):
                ok = bool(self.command_store.deny_once(approval_id, user_id))
        self._panel_error(
            chat_id,
            "approved"
            if ok and action == "approve"
            else "denied"
            if ok
            else "approval unavailable",
        )

    def _panel_error(self, chat_id: int, message: str) -> None:
        if self.panel is None or self.renderer is None:
            try:
                self.api.send_message(chat_id, message)
            except Exception:
                return
            return
        session = self.panel.sessions.get()
        route = str((session or {}).get("current_route") or "n:h")
        base = self.panel.render_route(route)
        text = _clip_error(base.text, message)
        self.panel.show_view(
            chat_id,
            route,
            PanelView(text=text, reply_markup=base.reply_markup),
        )

    def _can_mutate(self, user_id: int) -> bool:
        if self.admin_user_id and str(user_id) == self.admin_user_id:
            return True
        return self.authorizer.role_for(user_id) == "admin"

    def _answer(self, callback_id: str, text: str | None = None) -> None:
        if not callback_id:
            return
        try:
            self.api.answer_callback_query(callback_id, text=text)
        except Exception:
            return


def _clip_error(body: str, message: str) -> str:
    note = f"\n\n⚠️ {message}"
    limit = 3500
    if len(body) + len(note) <= limit:
        return body + note
    keep = max(0, limit - len(note) - 1)
    return body[:keep] + "…" + note
