from __future__ import annotations

import pytest

from qbt_orchestrator.db import migrate, write_execute
from qbt_orchestrator.telegram_ui import (
    BODY_LIMIT,
    CALLBACK_LIMIT,
    COPY_TEXT_LIMIT,
    PAGE_SIZE,
    DashboardRepository,
    TelegramPanelRenderer,
    encode_callback,
    humanize_scheduler_condition,
)


@pytest.fixture
def state_db(tmp_path):
    path = tmp_path / "state.sqlite"
    migrate(path)
    return path


def test_humanize_scheduler_condition_and_encode_callback():
    assert "空间允许" in humanize_scheduler_condition("progress_possible")
    assert len(encode_callback(["n", "q", "0"]).encode("utf-8")) <= CALLBACK_LIMIT
    with pytest.raises(ValueError):
        encode_callback(["x" * 70])


def test_telegram_ui_home_and_limits(state_db):
    write_execute(
        state_db,
        "insert or replace into disk_state(id,sampled_at,free_bytes,pressure_state) values(1,1,?,?)",
        (8 * 1024**3, "normal"),
    )
    renderer = TelegramPanelRenderer(DashboardRepository(state_db))
    home = renderer.render_home()
    assert "qBT Orchestrator" in home.text
    assert len(home.text) <= BODY_LIMIT
    assert "DRAIN" not in home.text
    assert "capacity_deadlock" not in home.text
    for row in home.reply_markup["inline_keyboard"]:
        for button in row:
            if "callback_data" in button:
                assert len(button["callback_data"].encode("utf-8")) <= CALLBACK_LIMIT


@pytest.mark.parametrize("count", [0, 1, 8, 9, 17])
def test_telegram_ui_pagination_and_limits(state_db, count):
    for index in range(count):
        write_execute(
            state_db,
            "insert into bot_add_batches("
            "batch_key,chat_id,user_id,state,created_at,updated_at) values(?,?,?,?,?,?)",
            (f"b-{index}", "1", "1", "complete", index + 1, index + 1),
        )
    renderer = TelegramPanelRenderer(DashboardRepository(state_db))
    page0 = renderer.render_queue(0)
    assert len(page0.text) <= BODY_LIMIT
    if count > PAGE_SIZE:
        assert any(
            btn.get("text") == "下一页"
            for row in page0.reply_markup["inline_keyboard"]
            for btn in row
        )
        page1 = renderer.render_queue(1)
        assert "第 2 页" in page1.text
    else:
        assert "下一页" not in page0.text


def test_copy_summary_button_limit(state_db):
    write_execute(
        state_db,
        "insert into bot_warning_inbox("
        "warning_key,severity,topic,safe_message,occurrence_count,first_occurred_at,"
        "last_occurred_at,updated_at,resolved) values(?,?,?,?,?,?,?,?,0)",
        ("k", "warning", "t", "m" * 400, 1, 1, 1, 1),
    )
    from qbt_orchestrator.warning_inbox import WarningInboxRepository

    repo = WarningInboxRepository(state_db, now=lambda: 1)
    warning = repo.list_recent(limit=1)[0]
    copy_text = repo.copy_summary(int(warning["id"]))
    assert len(copy_text.encode("utf-8")) <= COPY_TEXT_LIMIT
    view = TelegramPanelRenderer(DashboardRepository(state_db)).render_warning_detail(
        warning, copy_text=copy_text
    )
    assert view.reply_markup["inline_keyboard"][0][0]["copy_text"]["text"]
