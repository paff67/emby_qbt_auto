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
FORBIDDEN_INTERNAL = (
    "DRAIN",
    "capacity_deadlock",
    "plan_generation",
    "lease",
    "PID",
    "generation",
)


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
    def __init__(self, state_db: str | Path, *, now: Callable[[], int] | None = None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

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
        mode = str(capacity["scheduler_mode"] if capacity and "scheduler_mode" in capacity.keys() else (capacity[0] if capacity else "normal"))
        state = str(capacity["state"] if capacity and "state" in capacity.keys() else (capacity[1] if capacity and len(capacity) > 1 else ""))
        if mode in {"recovery", "emergency"} or state in {"recovery", "emergency"}:
            condition = "limited_with_candidate"
        elif mode in {"watch", "guard"} or state in {"watch", "guard"}:
            condition = "limited_without_candidate"
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
        for word in FORBIDDEN_INTERNAL:
            if word in text and word != "lease":
                text = text.replace(word, "内部状态")
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
        if not rows:
            lines.append("当前没有批次。")
        for row in rows:
            lines.append(
                f"#{row['id']} {row['state']} 收到{row['received_count']} "
                f"入库{row['enrolled_count']} 重复{row['duplicate_count']} "
                f"确认{row['confirmation_count']} 失败{row['failed_count']} "
                f"历史拦截{row.get('blocked_history_count') or 0}"
            )
        text = _clip_body("\n".join(lines))
        nav = [_btn("首页", encode_callback(["n", "h"]))]
        if page > 0:
            nav.append(_btn("上一页", encode_callback(["n", "q", str(page - 1)])))
        if (page + 1) * PAGE_SIZE < total:
            nav.append(_btn("下一页", encode_callback(["n", "q", str(page + 1)])))
        return PanelView(text=text, reply_markup={"inline_keyboard": [nav]})

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
