from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import readonly_connect


PAGE_SIZE = 8
BODY_LIMIT = 3500
CALLBACK_LIMIT = 64
COPY_TEXT_LIMIT = 256
_SHANGHAI = timezone(timedelta(hours=8))

_HISTORY_KINDS = {
    "d": "完成下载",
    "i": "完成入库",
    "e": "异常任务",
    "r": "自动回收",
    "m": "手动删除",
}

# Shared count/list predicates so home_snapshot and history_pages stay aligned.
_HISTORY_COUNT_SQL = {
    "d": (
        "select count(*) from processed_media "
        "where first_downloaded_at is not null"
    ),
    "i": (
        "select count(*) from processed_media "
        "where last_ingested_at is not null"
    ),
    "e": (
        "select count(*) from processed_media "
        "where lifecycle_state in ('manual_delete_failed','missing_unknown')"
    ),
    "r": "select count(*) from capacity_reclaims where state='reclaimed'",
    "m": (
        "select count(*) from processed_media "
        "where lifecycle_state='manual_deleted'"
    ),
}
_HISTORY_SELECT_SQL = {
    "d": (
        "select normalized_id,display_title,last_qbt_hash,"
        "first_downloaded_at as processed_at "
        "from processed_media where first_downloaded_at is not null "
        "order by first_downloaded_at desc,id desc limit ? offset ?"
    ),
    "i": (
        "select normalized_id,display_title,last_qbt_hash,"
        "last_ingested_at as processed_at "
        "from processed_media where last_ingested_at is not null "
        "order by last_ingested_at desc,id desc limit ? offset ?"
    ),
    "e": (
        "select normalized_id,display_title,last_qbt_hash,"
        "updated_at as processed_at "
        "from processed_media "
        "where lifecycle_state in ('manual_delete_failed','missing_unknown') "
        "order by updated_at desc,id desc limit ? offset ?"
    ),
    "r": (
        "select name,hash,reclaimed_at as processed_at,allocated_bytes "
        "from capacity_reclaims where state='reclaimed' "
        "order by reclaimed_at desc,id desc limit ? offset ?"
    ),
    "m": (
        "select normalized_id,display_title,last_qbt_hash,"
        "manually_deleted_at as processed_at "
        "from processed_media where lifecycle_state='manual_deleted' "
        "order by manually_deleted_at desc,id desc limit ? offset ?"
    ),
}

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


def _fmt_shanghai(ts: int | None) -> str:
    if ts is None:
        return "-"
    return datetime.fromtimestamp(int(ts), tz=_SHANGHAI).strftime("%Y-%m-%d %H:%M")


def _fmt_shanghai_clock(ts: int | None) -> str:
    if ts is None:
        return "--:--"
    return datetime.fromtimestamp(int(ts), tz=_SHANGHAI).strftime("%H:%M")


def _fmt_speed(dlspeed_bps: int) -> str:
    speed = max(0, int(dlspeed_bps or 0))
    if speed <= 0:
        return "等待数据源"
    return f"{speed / (1024 * 1024):.1f} MiB/s"


def _display_name(
    *,
    normalized_id: str | None = None,
    display_title: str | None = None,
    torrent_name: str | None = None,
    torrent_hash: str | None = None,
) -> str:
    for candidate in (normalized_id, display_title, torrent_name):
        text = str(candidate or "").strip()
        if text:
            return text
    digest = str(torrent_hash or "").strip()
    return digest[:12] if digest else "-"


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
                    "select hash,name,progress,dlspeed_bps from torrent_health "
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
            # Same口径 as history_pages(): cumulative timestamps for download/ingest,
            # current lifecycle for exceptions/manual delete, reclaim table for回收.
            downloaded = int(
                con.execute(_HISTORY_COUNT_SQL["d"]).fetchone()[0] or 0
            )
            ingested = int(con.execute(_HISTORY_COUNT_SQL["i"]).fetchone()[0] or 0)
            abnormal = int(con.execute(_HISTORY_COUNT_SQL["e"]).fetchone()[0] or 0)
            manual_deleted = int(
                con.execute(_HISTORY_COUNT_SQL["m"]).fetchone()[0] or 0
            )
            reclaimed = int(con.execute(_HISTORY_COUNT_SQL["r"]).fetchone()[0] or 0)
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
                    "name": (
                        str(row["name"]).strip()
                        if row["name"] is not None and str(row["name"]).strip()
                        else str(row["hash"])[:12]
                    ),
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
            "downloaded": downloaded,
            "ingested": ingested,
            "abnormal": abnormal,
            "manual_deleted": manual_deleted,
            "reclaimed": reclaimed,
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

    def history_pages(
        self, kind: str, *, page: int = 0
    ) -> tuple[list[dict[str, Any]], int]:
        key = str(kind)
        count_sql = _HISTORY_COUNT_SQL.get(key)
        select_sql = _HISTORY_SELECT_SQL.get(key)
        if count_sql is None or select_sql is None:
            return [], 0
        offset = max(0, int(page)) * PAGE_SIZE
        con = readonly_connect(self.state_db)
        try:
            total = con.execute(count_sql).fetchone()[0]
            rows = list(con.execute(select_sql, (PAGE_SIZE, offset)))
        finally:
            con.close()
        return [dict(row) for row in rows], int(total or 0)


class TelegramPanelRenderer:
    def __init__(
        self,
        dashboard: DashboardRepository,
        *,
        now: Callable[[], int] | None = None,
    ):
        self.dashboard = dashboard
        self.now = now or (lambda: int(time.time()))

    def render_home(self) -> PanelView:
        snap = self.dashboard.home_snapshot()
        active = list(snap["active"][:3])
        lines = [
            "📱 qBT 编排器控制台",
            f"系统在线 · 最后更新 {_fmt_shanghai_clock(int(self.now()))}",
            "",
            f"📥 当前任务（{len(active)}）",
        ]
        if not active:
            lines.append("暂无进行中的下载任务。")
        else:
            for index, item in enumerate(active, start=1):
                bar = _progress_bar(item["progress"])
                lines.append(f"{index}. {item['name']}")
                lines.append(
                    f"{bar} {item['progress'] * 100:.1f}% · {_fmt_speed(item['dlspeed'])}"
                )
        free_gib = snap["free_bytes"] / (1024**3)
        unread = int(snap["unread_warnings"] or 0)
        lines.extend(
            [
                "",
                f"💽 磁盘剩余：{free_gib:.1f} GiB",
                f"🧠 {humanize_scheduler_condition(snap['condition'])}",
                "",
                "📋 添加队列",
                (
                    f"待检查 {snap['queue_pending']} · 获取元数据 {snap['queue_metadata']} · "
                    f"等待确认 {snap['queue_confirm']}"
                ),
                "",
                "📈 累计处理",
                (
                    f"✅ 下载 {snap['downloaded']}  🗂 入库 {snap['ingested']}\n"
                    f"❌ 异常 {snap['abnormal']}  ♻️ 回收 {snap.get('reclaimed', 0)}"
                ),
                f"⚠️ {unread} 条警告未读",
            ]
        )
        text = _clip_body("\n".join(lines))
        warn_label = f"⚠️ 警告 · {unread}" if unread else "⚠️ 警告"
        markup = {
            "inline_keyboard": [
                [
                    _btn("📊 运行状态", encode_callback(["n", "s", "0"])),
                    _btn("📥 处理队列", encode_callback(["n", "q", "0"])),
                ],
                [
                    _btn("➕ 添加下载", encode_callback(["a", "o"])),
                    _btn(warn_label, encode_callback(["n", "w", "0"])),
                ],
                [_btn("🔄 刷新面板", encode_callback(["n", "rf"]))],
            ]
        }
        return PanelView(text=text, reply_markup=markup)

    def render_add_draft(
        self, batch_id: int, *, error: str | None = None
    ) -> PanelView:
        con = readonly_connect(self.dashboard.state_db)
        try:
            batch = con.execute(
                "select id,state,received_count,updated_at from bot_add_batches where id=?",
                (int(batch_id),),
            ).fetchone()
        finally:
            con.close()
        if batch is None:
            return PanelView(
                text="未找到添加草稿。",
                reply_markup={
                    "inline_keyboard": [
                        [_btn("🏠 返回首页", encode_callback(["n", "h"]))]
                    ]
                },
            )
        lines = [
            "➕ 添加下载",
            f"草稿 #{batch['id']}",
            f"已接收：{int(batch['received_count'] or 0)} 条链接",
            "请继续发送链接，或者提交当前批次。",
        ]
        if error:
            lines.extend(["", f"⚠️ {error}"])
        text = _clip_body("\n".join(lines))
        gen = int(batch["updated_at"] or 0)
        bid = int(batch["id"])
        return PanelView(
            text=text,
            reply_markup={
                "inline_keyboard": [
                    [
                        _btn(
                            "✅ 提交本批",
                            encode_callback(["a", "s", str(bid), str(gen)]),
                        ),
                        _btn(
                            "🗑 取消草稿",
                            encode_callback(["a", "c", str(bid), str(gen)]),
                        ),
                    ],
                    [
                        _btn("📥 查看队列", encode_callback(["n", "q", "0"])),
                        _btn("🏠 返回首页", encode_callback(["n", "h"])),
                    ],
                ]
            },
        )

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
        lines = ["⚠️ 警告中心", f"第 {page + 1} 页"]
        buttons: list[list[dict[str, Any]]] = []
        if not rows:
            lines.append("当前没有警告。")
        view_row: list[dict[str, Any]] = []
        for row in rows:
            marker = "未读" if int(row["resolved"] or 0) == 0 else "已读"
            lines.append(
                f"#{row['id']} [{row['severity']}] {marker} {row['topic']} "
                f"x{row['occurrence_count']}: {str(row['safe_message'])[:80]}"
            )
            view_row.append(
                _btn(
                    f"查看 #{row['id']}",
                    encode_callback(
                        ["w", "d", str(row["id"]), str(row["occurrence_count"])]
                    ),
                )
            )
            if len(view_row) == 2:
                buttons.append(view_row)
                view_row = []
        if view_row:
            buttons.append(view_row)
        text = _clip_body("\n".join(lines))
        buttons.append(
            [_btn("全部标为已读", encode_callback(["w", "ra", "0"]))]
        )
        buttons.append(
            [_btn("导出全部日志", encode_callback(["w", "xa"]))]
        )
        nav = [
            _btn("🔄 刷新", encode_callback(["n", "w", str(page)])),
            _btn("🏠 首页", encode_callback(["n", "h"])),
        ]
        if page > 0:
            nav.insert(
                0, _btn("上一页", encode_callback(["n", "w", str(page - 1)]))
            )
        if (page + 1) * PAGE_SIZE < total:
            nav.append(_btn("下一页", encode_callback(["n", "w", str(page + 1)])))
        buttons.append(nav)
        return PanelView(text=text, reply_markup={"inline_keyboard": buttons})

    def render_warning_detail_by_id(self, warning_id: int) -> PanelView:
        from .warning_inbox import WarningInboxRepository

        repo = WarningInboxRepository(self.dashboard.state_db)
        warning = repo.get(int(warning_id))
        if warning is None:
            return PanelView(
                text="该警告已不存在或无法读取。",
                reply_markup={
                    "inline_keyboard": [
                        [_btn("⬅️ 返回警告中心", encode_callback(["n", "w", "0"]))]
                    ]
                },
            )
        return self.render_warning_detail(
            warning, copy_text=repo.copy_summary(int(warning_id))
        )

    def render_warning_detail(
        self, warning: Mapping[str, Any], *, copy_text: str
    ) -> PanelView:
        lines = [
            f"⚠️ 警告 #{warning['id']}",
            f"级别：{warning['severity']}",
            f"主题：{warning['topic']}",
            f"首次时间：{_fmt_shanghai(warning.get('first_occurred_at'))}",
            f"最后时间：{_fmt_shanghai(warning.get('last_occurred_at'))}",
            f"累计次数：{warning['occurrence_count']}",
            "",
            str(warning["safe_message"]),
        ]
        text = _clip_body("\n".join(lines))
        occ = int(warning["occurrence_count"])
        wid = int(warning["id"])
        markup = {
            "inline_keyboard": [
                [
                    _btn(
                        "✅ 标为已读",
                        encode_callback(["w", "r", str(wid), str(occ)]),
                    )
                ],
                [
                    _btn(
                        "📄 导出相关日志",
                        encode_callback(["w", "x", str(wid), str(occ)]),
                    )
                ],
                [_btn("⬅️ 返回警告中心", encode_callback(["n", "w", "0"]))],
            ]
        }
        del copy_text  # kept for API compatibility with callers
        return PanelView(text=text, reply_markup=markup)

    def render_status(self, page: int = 0) -> PanelView:
        del page  # Legacy n:s:<n> opens the status home.
        return self.render_status_home()

    def render_status_home(self) -> PanelView:
        snap = self.dashboard.home_snapshot()
        text = _clip_body(
            "\n".join(
                [
                    "状态/历史",
                    f"磁盘可用：{snap['free_bytes'] / (1024**3):.1f} GiB",
                    humanize_scheduler_condition(snap["condition"]),
                    f"已入库 {snap['ingested']}，手动删除 {snap['manual_deleted']}",
                    "请选择要查看的历史分类。",
                ]
            )
        )
        return PanelView(
            text=text,
            reply_markup={
                "inline_keyboard": [
                    [
                        _btn("完成下载", encode_callback(["n", "s", "d", "0"])),
                        _btn("完成入库", encode_callback(["n", "s", "i", "0"])),
                    ],
                    [
                        _btn("异常任务", encode_callback(["n", "s", "e", "0"])),
                        _btn("自动回收", encode_callback(["n", "s", "r", "0"])),
                    ],
                    [_btn("手动删除", encode_callback(["n", "s", "m", "0"]))],
                    [_btn("返回首页", encode_callback(["n", "h"]))],
                ]
            },
        )

    def render_status_history(self, kind: str, page: int = 0) -> PanelView:
        title = _HISTORY_KINDS.get(str(kind))
        if title is None:
            return PanelView(
                text="未知的历史分类。",
                reply_markup={
                    "inline_keyboard": [
                        [_btn("返回状态页", encode_callback(["n", "s", "0"]))]
                    ]
                },
            )
        rows, total = self.dashboard.history_pages(str(kind), page=page)
        lines = [f"{title}记录 · 第 {page + 1} 页"]
        if not rows:
            lines.append("当前没有记录。")
        for row in rows:
            when = _fmt_shanghai(row.get("processed_at"))
            if kind == "r":
                name = _display_name(
                    torrent_name=row.get("name"),
                    torrent_hash=row.get("hash"),
                )
                gib = int(row.get("allocated_bytes") or 0) / (1024**3)
                lines.append(f"{when}  {name} · {gib:.1f} GiB")
            else:
                name = _display_name(
                    normalized_id=row.get("normalized_id"),
                    display_title=row.get("display_title"),
                    torrent_hash=row.get("last_qbt_hash"),
                )
                lines.append(f"{when}  {name}")
        text = _clip_body("\n".join(lines))
        nav: list[dict[str, Any]] = [
            _btn("返回状态页", encode_callback(["n", "s", "0"]))
        ]
        if page > 0:
            nav.append(
                _btn("上一页", encode_callback(["n", "s", str(kind), str(page - 1)]))
            )
        if (page + 1) * PAGE_SIZE < total:
            nav.append(
                _btn("下一页", encode_callback(["n", "s", str(kind), str(page + 1)]))
            )
        return PanelView(text=text, reply_markup={"inline_keyboard": [nav]})
