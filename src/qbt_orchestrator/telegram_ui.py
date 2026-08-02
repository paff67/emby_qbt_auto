from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import readonly_connect


PAGE_SIZE = 8
BODY_LIMIT = 3500
CALLBACK_LIMIT = 64
COPY_TEXT_LIMIT = 256

_BATCH_STATE_ZH = {
    "draft": "草稿",
    "queued": "排队中",
    "processing": "检查中",
    "awaiting_confirmation": "等待确认",
    "complete": "已完成",
    "cancelled": "已取消",
    "draft_expired": "草稿已过期",
}

_ITEM_STATE_ZH = {
    "received": "已接收",
    "resolving": "解析中",
    "waiting_probe_slot": "等待元数据槽位",
    "metadata_wait": "获取元数据中",
    "metadata_retry_wait": "等待元数据重试",
    "metadata_unavailable": "元数据不可用",
    "prechecking": "预检查中",
    "needs_confirmation": "等待确认",
    "enrolling": "入库中",
    "enrolled": "已入库",
    "enrolled_hold": "已确认暂缓",
    "duplicate_local": "本地重复",
    "duplicate_remote": "远端已存在",
    "ready": "等待加入下载",
    "failed": "失败",
    "cancelled": "已取消",
    "invalid": "无效",
}


@dataclass(frozen=True)
class PanelView:
    text: str
    reply_markup: dict[str, Any]


def encode_callback(parts: list[str] | tuple[str, ...]) -> str:
    data = ":".join(str(part) for part in parts)
    encoded = data.encode("utf-8")
    if not 1 <= len(encoded) <= CALLBACK_LIMIT:
        raise ValueError("callback_data")
    return data


def humanize_scheduler_condition(condition: str) -> str:
    mapping = {
        "progress_possible": "空间允许，系统正在按优先级安排任务。",
        "limited_with_candidate": "可用空间较少，已暂缓启动新任务；系统正在安全释放空间。",
        "limited_without_candidate": "可用空间不足，当前没有能够安全回收的任务，需要人工处理。",
        "qbt_unavailable": "暂时无法连接下载服务，系统已停止调整任务。",
    }
    return mapping.get(str(condition or ""), "系统正在根据当前状态安排任务。")


def _progress_bar(ratio: float) -> str:
    clamped = max(0.0, min(1.0, float(ratio)))
    filled = int(round(clamped * 10))
    return "█" * filled + "░" * (10 - filled)


def _clip_body(text: str) -> str:
    if len(text) <= BODY_LIMIT:
        return text
    return text[: BODY_LIMIT - 1] + "…"


def _btn(text: str, callback_data: str) -> dict[str, str]:
    return {"text": text, "callback_data": callback_data}


def _copy_btn(text: str, copy_text: str) -> dict[str, Any]:
    return {"text": text, "copy_text": {"text": copy_text[:COPY_TEXT_LIMIT]}}


class DashboardRepository:
    def __init__(self, state_db: str | Path):
        self.state_db = Path(state_db)

    def home_snapshot(self) -> dict[str, Any]:
        con = readonly_connect(self.state_db)
        try:
            active = list(
                con.execute(
                    "select hash,progress,dlspeed_bps from torrent_health "
                    "where coalesce(dlspeed_bps,0)>0 or coalesce(active_since,0)>0 "
                    "order by dlspeed_bps desc, progress desc limit 3"
                )
            )
            free_row = con.execute(
                "select free_bytes from disk_state where id=1"
            ).fetchone()
            queue = con.execute(
                "select "
                "sum(case when state in ('received','resolving','waiting_probe_slot') then 1 else 0 end),"
                "sum(case when state in ('metadata_wait','metadata_retry_wait','prechecking') then 1 else 0 end),"
                "sum(case when state='needs_confirmation' then 1 else 0 end) "
                "from bot_add_items"
            ).fetchone()
            batch_queue = con.execute(
                "select "
                "sum(case when state in ('queued','processing') then 1 else 0 end),"
                "sum(case when state='awaiting_confirmation' then 1 else 0 end) "
                "from bot_add_batches"
            ).fetchone()
            processed = con.execute(
                "select "
                "sum(case when lifecycle_state in ('downloaded','uploaded','ingested_present') then 1 else 0 end),"
                "sum(case when lifecycle_state='ingested_present' then 1 else 0 end),"
                "sum(case when lifecycle_state in ('manual_delete_failed','missing_unknown') then 1 else 0 end),"
                "sum(case when lifecycle_state='manual_deleted' then 1 else 0 end) "
                "from processed_media"
            ).fetchone()
            unread = con.execute(
                "select count(*) from bot_warning_inbox where resolved=0"
            ).fetchone()[0]
            capacity = con.execute(
                "select scheduler_mode,state from capacity_state where id=1"
            ).fetchone()
        finally:
            con.close()
        free_bytes = int(free_row[0]) if free_row and free_row[0] is not None else 0
        mode = str(
            capacity["scheduler_mode"]
            if capacity and "scheduler_mode" in capacity.keys()
            else (capacity[0] if capacity else "normal")
        )
        state = str(
            capacity["state"]
            if capacity and "state" in capacity.keys()
            else (capacity[1] if capacity and len(capacity) > 1 else "")
        )
        # Map only to approved natural-language condition keys for the renderer.
        if mode in {"recovery", "emergency"} or state in {"recovery", "emergency"}:
            condition = "limited_with_candidate"
        elif mode in {"watch", "guard"} or state in {"watch", "guard"}:
            condition = "limited_without_candidate"
        elif mode in {"qbt_down", "unavailable"} or state in {"qbt_down", "unavailable"}:
            condition = "qbt_unavailable"
        else:
            condition = "progress_possible"
        return {
            "active": [
                {
                    "name": str(row["hash"])[:40],
                    "progress": float(row["progress"] or 0.0),
                    "dlspeed": int(row["dlspeed_bps"] or 0),
                }
                for row in active
            ],
            "free_bytes": free_bytes,
            "condition": condition,
            "queue_pending": int(queue[0] or 0) if queue else 0,
            "queue_metadata": int(queue[1] or 0) if queue else 0,
            "queue_confirm": int(queue[2] or 0) if queue else 0,
            "queue_batches": int((batch_queue[0] or 0) + (batch_queue[1] or 0)) if batch_queue else 0,
            "downloaded": int(processed[0] or 0) if processed else 0,
            "ingested": int(processed[1] or 0) if processed else 0,
            "abnormal": int(processed[2] or 0) if processed else 0,
            "manual_deleted": int(processed[3] or 0) if processed else 0,
            "unread_warnings": int(unread or 0),
        }

    def queue_pages(self, *, page: int = 0) -> tuple[list[dict[str, Any]], int]:
        offset = max(0, int(page)) * PAGE_SIZE
        con = readonly_connect(self.state_db)
        try:
            total = con.execute("select count(*) from bot_add_batches").fetchone()[0]
            rows = list(
                con.execute(
                    "select id,state,received_count,enrolled_count,duplicate_count,"
                    "confirmation_count,failed_count,blocked_history_count,updated_at "
                    "from bot_add_batches order by updated_at desc,id desc "
                    "limit ? offset ?",
                    (PAGE_SIZE, offset),
                )
            )
        finally:
            con.close()
        return [dict(row) for row in rows], int(total or 0)

    def queue_detail(
        self, batch_id: int, *, page: int = 0
    ) -> dict[str, Any] | None:
        offset = max(0, int(page)) * PAGE_SIZE
        con = readonly_connect(self.state_db)
        try:
            batch = con.execute(
                "select id,state,received_count,enrolled_count,duplicate_count,"
                "confirmation_count,failed_count,blocked_history_count,updated_at "
                "from bot_add_batches where id=?",
                (int(batch_id),),
            ).fetchone()
            if batch is None:
                return None
            total = con.execute(
                "select count(*) from bot_add_items where batch_id=?",
                (int(batch_id),),
            ).fetchone()[0]
            items = list(
                con.execute(
                    "select id,state,approval_generation,normalized_media_id,"
                    "display_name,last_error "
                    "from bot_add_items where batch_id=? order by id "
                    "limit ? offset ?",
                    (int(batch_id), PAGE_SIZE, offset),
                )
            )
        finally:
            con.close()
        return {
            "batch": dict(batch),
            "items": [dict(row) for row in items],
            "total": int(total or 0),
            "page": max(0, int(page)),
        }

    def warning_pages(self, *, page: int = 0) -> tuple[list[dict[str, Any]], int]:
        offset = max(0, int(page)) * PAGE_SIZE
        con = readonly_connect(self.state_db)
        try:
            total = con.execute("select count(*) from bot_warning_inbox").fetchone()[0]
            rows = list(
                con.execute(
                    "select id,severity,topic,safe_message,occurrence_count,resolved,"
                    "last_occurred_at from bot_warning_inbox "
                    "order by resolved asc, last_occurred_at desc, id desc "
                    "limit ? offset ?",
                    (PAGE_SIZE, offset),
                )
            )
        finally:
            con.close()
        return [dict(row) for row in rows], int(total or 0)


class TelegramPanelRenderer:
    def __init__(self, dashboard: DashboardRepository):
        self.dashboard = dashboard

    def render_home(self) -> PanelView:
        snap = self.dashboard.home_snapshot()
        lines = ["qBT Orchestrator", "", "当前任务"]
        if not snap["active"]:
            lines.append("暂无进行中的下载任务。")
        else:
            for item in snap["active"][:3]:
                bar = _progress_bar(item["progress"])
                lines.append(
                    f"{bar} {item['progress']*100:.1f}%  {item['name']}"
                )
        free_gib = snap["free_bytes"] / (1024**3)
        lines.extend(
            [
                "",
                f"磁盘可用空间：{free_gib:.1f} GiB",
                f"当前调度说明：{humanize_scheduler_condition(snap['condition'])}",
                (
                    "添加队列：待检查 "
                    f"{snap['queue_pending']}/获取元数据 {snap['queue_metadata']}/"
                    f"等待确认 {snap['queue_confirm']}/等待调度 {snap['queue_batches']}"
                ),
                (
                    "累计处理：完成下载 "
                    f"{snap['downloaded']}/完成入库 {snap['ingested']}/"
                    f"异常 {snap['abnormal']}/手动删除 {snap['manual_deleted']}"
                ),
                f"未读警告：{snap['unread_warnings']}",
            ]
        )
        text = _clip_body("\n".join(lines))
        markup = {
            "inline_keyboard": [
                [
                    _btn("状态/历史", encode_callback(["n", "s", "0"])),
                    _btn("添加队列", encode_callback(["n", "q", "0"])),
                ],
                [
                    _btn("警告", encode_callback(["n", "w", "0"])),
                    _btn("打开添加", encode_callback(["a", "o"])),
                ],
            ]
        }
        return PanelView(text=text, reply_markup=markup)

    def render_queue(self, page: int = 0) -> PanelView:
        rows, total = self.dashboard.queue_pages(page=page)
        lines = ["添加队列", f"第 {page + 1} 页"]
        buttons: list[list[dict[str, Any]]] = []
        if not rows:
            lines.append("当前没有批次。")
        for row in rows:
            state_zh = _BATCH_STATE_ZH.get(str(row["state"]), "处理中")
            lines.append(
                f"批次 {row['id']}（{state_zh}）收到{row['received_count']} "
                f"入库{row['enrolled_count']} 重复{row['duplicate_count']} "
                f"确认{row['confirmation_count']} 失败{row['failed_count']} "
                f"历史拦截{row.get('blocked_history_count') or 0}"
            )
            buttons.append(
                [
                    _btn(
                        f"查看批次 {row['id']}",
                        encode_callback(["n", "b", str(row["id"]), "0"]),
                    )
                ]
            )
        text = _clip_body("\n".join(lines))
        nav = [_btn("首页", encode_callback(["n", "h"]))]
        if page > 0:
            nav.append(_btn("上一页", encode_callback(["n", "q", str(page - 1)])))
        if (page + 1) * PAGE_SIZE < total:
            nav.append(_btn("下一页", encode_callback(["n", "q", str(page + 1)])))
        buttons.append(nav)
        return PanelView(text=text, reply_markup={"inline_keyboard": buttons})

    def render_queue_detail(self, batch_id: int, page: int = 0) -> PanelView:
        detail = self.dashboard.queue_detail(batch_id, page=page)
        if detail is None:
            return PanelView(
                text="未找到该批次。",
                reply_markup={
                    "inline_keyboard": [
                        [_btn("返回队列", encode_callback(["n", "q", "0"]))]
                    ]
                },
            )
        batch = detail["batch"]
        page = int(detail.get("page") or 0)
        total = int(detail.get("total") or 0)
        state_zh = _BATCH_STATE_ZH.get(str(batch["state"]), "处理中")
        lines = [
            f"批次 {batch['id']}（{state_zh}）",
            f"条目第 {page + 1} 页",
            (
                f"收到{batch['received_count']} 入库{batch['enrolled_count']} "
                f"重复{batch['duplicate_count']} 确认{batch['confirmation_count']} "
                f"失败{batch['failed_count']} 历史拦截{batch.get('blocked_history_count') or 0}"
            ),
            "",
            "条目：",
        ]
        buttons: list[list[dict[str, Any]]] = []
        items = detail["items"]
        if not items:
            lines.append("暂无条目。")
        for item in items:
            item_id = int(item["id"])
            generation = int(item.get("approval_generation") or 0)
            item_state = str(item.get("state") or "")
            item_zh = _ITEM_STATE_ZH.get(item_state, "处理中")
            media = str(item.get("normalized_media_id") or "").strip()
            display = str(item.get("display_name") or "").strip()
            label = f"#{item_id} {item_zh}"
            if media:
                label += f" {media}"
            elif display:
                label += f" {display[:40]}"
            lines.append(label)
            if item_state == "needs_confirmation":
                buttons.append(
                    [
                        _btn(
                            f"确认并暂缓 #{item_id}",
                            encode_callback(["i", "y", str(item_id), str(generation)]),
                        ),
                        _btn(
                            f"取消 #{item_id}",
                            encode_callback(["i", "x", str(item_id), str(generation)]),
                        ),
                    ]
                )
            elif item_state == "metadata_unavailable":
                buttons.append(
                    [
                        _btn(
                            f"立即重试 #{item_id}",
                            encode_callback(["i", "r", str(item_id), str(generation)]),
                        ),
                        _btn(
                            f"取消 #{item_id}",
                            encode_callback(["i", "x", str(item_id), str(generation)]),
                        ),
                    ]
                )
        text = _clip_body("\n".join(lines))
        page_nav: list[dict[str, Any]] = []
        if page > 0:
            page_nav.append(
                _btn(
                    "上一页",
                    encode_callback(["n", "b", str(batch["id"]), str(page - 1)]),
                )
            )
        if (page + 1) * PAGE_SIZE < total:
            page_nav.append(
                _btn(
                    "下一页",
                    encode_callback(["n", "b", str(batch["id"]), str(page + 1)]),
                )
            )
        if page_nav:
            buttons.append(page_nav)
        buttons.append(
            [
                _btn("返回队列", encode_callback(["n", "q", "0"])),
                _btn("首页", encode_callback(["n", "h"])),
            ]
        )
        return PanelView(text=text, reply_markup={"inline_keyboard": buttons})

    def render_warnings(self, page: int = 0) -> PanelView:
        rows, total = self.dashboard.warning_pages(page=page)
        lines = ["警告列表", f"第 {page + 1} 页"]
        buttons: list[list[dict[str, Any]]] = []
        if not rows:
            lines.append("当前没有警告。")
        for row in rows:
            marker = "未读" if int(row["resolved"] or 0) == 0 else "已读"
            lines.append(
                f"#{row['id']} [{row['severity']}] {marker} {row['topic']} "
                f"x{row['occurrence_count']}: {str(row['safe_message'])[:80]}"
            )
            buttons.append(
                [
                    _btn(
                        f"查看 #{row['id']}",
                        encode_callback(["w", "d", str(row["id"]), str(row["occurrence_count"])]),
                    )
                ]
            )
        text = _clip_body("\n".join(lines))
        nav = [_btn("首页", encode_callback(["n", "h"]))]
        if page > 0:
            nav.append(_btn("上一页", encode_callback(["n", "w", str(page - 1)])))
        if (page + 1) * PAGE_SIZE < total:
            nav.append(_btn("下一页", encode_callback(["n", "w", str(page + 1)])))
        buttons.append(nav)
        return PanelView(text=text, reply_markup={"inline_keyboard": buttons})

    def render_warning_detail(
        self, warning: Mapping[str, Any], *, copy_text: str
    ) -> PanelView:
        lines = [
            f"警告 #{warning['id']}",
            f"级别：{warning['severity']}",
            f"主题：{warning['topic']}",
            f"次数：{warning['occurrence_count']}",
            str(warning["safe_message"]),
        ]
        text = _clip_body("\n".join(lines))
        occ = int(warning["occurrence_count"])
        wid = int(warning["id"])
        markup = {
            "inline_keyboard": [
                [_copy_btn("复制摘要", copy_text[:COPY_TEXT_LIMIT])],
                [
                    _btn("导出相关日志", encode_callback(["w", "x", str(wid), str(occ)])),
                    _btn("标为已读", encode_callback(["w", "r", str(wid), str(occ)])),
                ],
                [_btn("返回警告列表", encode_callback(["n", "w", "0"]))],
            ]
        }
        return PanelView(text=text, reply_markup=markup)

    def render_status(self, page: int = 0) -> PanelView:
        snap = self.dashboard.home_snapshot()
        text = _clip_body(
            "\n".join(
                [
                    "状态/历史",
                    f"磁盘可用：{snap['free_bytes'] / (1024**3):.1f} GiB",
                    humanize_scheduler_condition(snap["condition"]),
                    f"已入库 {snap['ingested']}，手动删除 {snap['manual_deleted']}",
                    f"第 {page + 1} 页",
                ]
            )
        )
        return PanelView(
            text=text,
            reply_markup={
                "inline_keyboard": [[_btn("首页", encode_callback(["n", "h"]))]]
            },
        )
