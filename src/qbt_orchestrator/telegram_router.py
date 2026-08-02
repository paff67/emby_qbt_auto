from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any, Callable

from .bot_add_queue import BotAddQueueRepository
from .observability import redact
from .telegram_control import TelegramAuthorizer
from .telegram_ui import (
    DashboardRepository,
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
    """Single update router for commands, navigation, add queue, approvals, warnings."""

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
        self.now = now or (lambda: int(__import__("time").time()))
        self.renderer = None
        if self.state_db is not None:
            self.renderer = TelegramPanelRenderer(
                DashboardRepository(self.state_db)
            )
            if self.warnings is None:
                self.warnings = WarningInboxRepository(
                    self.state_db, now=lambda: int(self.now())
                )

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
                self._open_add_draft(chat_id, user_id)
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

    def _open_add_draft(self, chat_id: int, user_id: int) -> None:
        if self.add_queue is None:
            self.api.send_message(chat_id, "添加队列未配置")
            return
        draft = self.add_queue.open_draft(str(chat_id), str(user_id))
        self.api.send_message(
            chat_id,
            self._draft_text(draft, received_now=0),
            reply_markup=self._draft_markup(draft),
        )

    def _handle_add_text(
        self, chat_id: int, user_id: int, text: str, msg: dict[str, Any]
    ) -> None:
        if not self._can_mutate(user_id) or self.add_queue is None:
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
            self.api.send_message(chat_id, f"未能加入草稿：{self._humanize_ingress_error(exc)}")
            return
        received_now = int(result.get("inserted_count") or 0)
        self.api.send_message(
            chat_id,
            self._draft_text(result, received_now=received_now),
            reply_markup=self._draft_markup(result),
        )

    def _draft_text(self, batch: dict[str, Any], *, received_now: int) -> str:
        total = int(batch.get("received_count") or 0)
        if received_now:
            return (
                f"已接收本条 {received_now} 个链接。\n"
                f"当前草稿累计 {total} 个。\n"
                "确认无误后请点击提交本批。"
            )
        return (
            f"已打开添加草稿（编号 {batch.get('id')}）。\n"
            f"当前累计 {total} 个链接。\n"
            "请发送磁力链或下载链接；完成后点击提交本批。"
        )

    def _draft_markup(self, batch: dict[str, Any]) -> dict[str, Any]:
        batch_id = int(batch["id"])
        gen = int(batch.get("updated_at") or 0)
        return {
            "inline_keyboard": [
                [
                    _btn("提交本批", encode_callback(["a", "s", str(batch_id), str(gen)])),
                    _btn("取消草稿", encode_callback(["a", "c", str(batch_id), str(gen)])),
                ],
                [_btn("返回首页", encode_callback(["n", "h"]))],
            ]
        }

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
            self.api.send_message(
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

        # Answer first so Telegram clears the spinner before any write work.
        self._answer(callback_id)

        if data.startswith("approve:") or data.startswith("deny:"):
            self._legacy_approval(data, chat_id, user_id)
            return

        if not self.panel_enabled or self.renderer is None:
            self.api.send_message(chat_id, "面板未启用")
            return
        if not self.authorizer.role_for(user_id):
            self.api.send_message(chat_id, "无权访问")
            return

        parts = data.split(":")
        try:
            if parts[0] == "n" and parts[1] == "h":
                view = self.renderer.render_home()
            elif parts[0] == "n" and parts[1] == "s":
                view = self.renderer.render_status(int(parts[2]))
            elif parts[0] == "n" and parts[1] == "q":
                view = self.renderer.render_queue(int(parts[2]))
            elif parts[0] == "n" and parts[1] == "b":
                # n:b:<batch_id>[:page] — omit page for legacy callbacks (page 0).
                detail_page = int(parts[3]) if len(parts) > 3 else 0
                view = self.renderer.render_queue_detail(int(parts[2]), detail_page)
            elif parts[0] == "n" and parts[1] == "w":
                view = self.renderer.render_warnings(int(parts[2]))
            elif parts[0] == "a" and parts[1] == "o":
                if not self._can_mutate(user_id):
                    self.api.send_message(chat_id, "只读账号不能执行此操作")
                    return
                self._open_add_draft(chat_id, user_id)
                return
            elif parts[0] == "a" and parts[1] in {"s", "c"}:
                self._handle_draft_action(parts, chat_id, user_id, message_id)
                return
            elif parts[0] == "i" and parts[1] in {"y", "x", "r"}:
                self._handle_item_action(parts, chat_id, user_id)
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
                self.api.send_message(chat_id, "已标为已读" if ok else "状态已变化，请刷新列表")
                view = self.renderer.render_warnings(0)
            elif parts[0] == "w" and parts[1] == "x":
                self._export_warning(chat_id, int(parts[2]))
                return
            elif parts[0] == "ap":
                self._compact_approval(parts, chat_id, user_id)
                return
            else:
                self.api.send_message(chat_id, "未知操作")
                return
            self.api.edit_message_text(
                chat_id, message_id, view.text, reply_markup=view.reply_markup
            )
        except Exception as exc:
            self.api.send_message(chat_id, str(redact(str(exc)))[:180])

    def _handle_draft_action(
        self, parts: list[str], chat_id: int, user_id: int, message_id: int
    ) -> None:
        if not self._can_mutate(user_id) or self.add_queue is None:
            self.api.send_message(chat_id, "只读账号不能执行此操作")
            return
        batch_id = int(parts[2])
        expected_gen = int(parts[3])
        batch = self.add_queue.get_batch(batch_id)
        if str(batch.get("chat_id")) != str(chat_id) or str(batch.get("user_id")) != str(
            user_id
        ):
            self.api.send_message(chat_id, "无权操作该草稿")
            return
        try:
            if parts[1] == "s":
                result = self.add_queue.submit_draft(batch_id, expected_gen)
                text = (
                    f"已提交批次 {batch_id}。\n"
                    f"共 {int(result.get('received_count') or 0)} 个链接进入检查队列。"
                )
            else:
                self.add_queue.cancel_draft(
                    batch_id, expected_gen, actor=str(user_id)
                )
                text = f"已取消草稿 {batch_id}。"
        except ValueError as exc:
            self.api.send_message(chat_id, self._humanize_ingress_error(exc))
            return
        self.api.edit_message_text(chat_id, message_id, text, reply_markup=None)
        if self.renderer is not None:
            view = self.renderer.render_queue(0)
            self.api.send_message(chat_id, view.text, reply_markup=view.reply_markup)

    def _handle_item_action(
        self, parts: list[str], chat_id: int, user_id: int
    ) -> None:
        if not self._can_mutate(user_id):
            self.api.send_message(chat_id, "只读账号不能执行此操作")
            return
        item_id = int(parts[2])
        generation = int(parts[3])
        action = parts[1]
        try:
            if action == "y":
                if self.checked_add is None:
                    raise ValueError("checked_add_unavailable")
                self.checked_add.approve_hold(item_id, str(user_id), generation)
                self.api.send_message(chat_id, f"已确认条目 {item_id}，将保持手动暂缓。")
            elif action == "x":
                if self.add_queue is None:
                    raise ValueError("add_queue_unavailable")
                item = self.add_queue.get_item(item_id)
                state = str(item.get("state") or "")
                if state == "needs_confirmation":
                    if self.checked_add is None:
                        raise ValueError("checked_add_unavailable")
                    # CheckedAdd owns qBT cleanup; never fall back to a bare DB cancel.
                    self.checked_add.cancel(item_id, str(user_id), generation)
                elif state == "metadata_unavailable":
                    self.add_queue.transition_item(
                        item_id,
                        {"metadata_unavailable"},
                        "cancelled",
                        "cancelled_by_operator",
                        approval_generation=generation,
                        metadata_action="cancel",
                    )
                else:
                    raise ValueError("item_not_cancellable")
                self.api.send_message(chat_id, f"已取消条目 {item_id}。")
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
                self.api.send_message(chat_id, f"已安排条目 {item_id} 重新获取元数据。")
        except ValueError as exc:
            self.api.send_message(chat_id, self._humanize_ingress_error(exc))

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

    def _legacy_approval(self, data: str, chat_id: int, user_id: int) -> None:
        action, approval_id = data.split(":", 1)
        action = action.replace("-", "_")
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
        self.api.send_message(
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
            self.api.send_message(chat_id, "无效审批")
            return
        action = "approve" if parts[1] == "y" else "deny"
        approval_id = parts[2]
        if not self.authorizer.allowed(user_id, action):
            self.api.send_message(chat_id, "unauthorized")
            return
        ok = False
        if self.command_store is not None:
            if action == "approve" and hasattr(self.command_store, "approve_once"):
                ok = bool(self.command_store.approve_once(approval_id, user_id))
            elif action == "deny" and hasattr(self.command_store, "deny_once"):
                ok = bool(self.command_store.deny_once(approval_id, user_id))
        self.api.send_message(
            chat_id,
            "approved"
            if ok and action == "approve"
            else "denied"
            if ok
            else "approval unavailable",
        )

    def _can_mutate(self, user_id: int) -> bool:
        # P1 personal deploy: only the configured administrator may mutate.
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
