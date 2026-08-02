from __future__ import annotations

from qbt_orchestrator.alerts import SchedulerAlertConfig, SchedulerAlertService
from qbt_orchestrator.db import migrate, readonly_connect, write_execute
from qbt_orchestrator.planner import DownloadPlanner, PlannerResult
from qbt_orchestrator.runtime import BotNotificationRepository
from qbt_orchestrator.telegram_ui import DashboardRepository, TelegramPanelRenderer
from qbt_orchestrator.warning_inbox import WarningInboxRepository, WarningService
from tests.fakes import FakeExecutor


def test_planner_persists_torrent_name_into_health(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    planner = DownloadPlanner(
        state_db=db,
        executor=FakeExecutor(),
        dry_run=False,
        active_slots=1,
        disk_floor_bytes=0,
        now=lambda: 1000,
    )
    snapshots = {
        "abcdef0123456789": {
            "hash": "abcdef0123456789",
            "name": "  SONE-792  Complete  ",
            "category": "auto",
            "tags": "auto",
            "state": "downloading",
            "dlspeed": 1000,
            "upspeed": 0,
            "completed": 10,
            "progress": 0.5,
            "amount_left": 10,
            "size": 20,
            "num_seeds": 1,
            "num_peers": 1,
        }
    }
    planner.plan_and_apply(snapshots, free_bytes=10 * 1024**3, sync_healthy=True)
    con = readonly_connect(db)
    try:
        name = con.execute(
            "select name from torrent_health where hash='abcdef0123456789'"
        ).fetchone()[0]
    finally:
        con.close()
    assert name == "SONE-792 Complete"

    # Empty/missing name must not overwrite a previously stored name.
    planner2 = DownloadPlanner(
        state_db=db,
        executor=FakeExecutor(),
        dry_run=False,
        active_slots=1,
        disk_floor_bytes=0,
        now=lambda: 1001,
    )
    snapshots["abcdef0123456789"] = {
        **snapshots["abcdef0123456789"],
        "name": "",
        "dlspeed": 0,
    }
    planner2.plan_and_apply(snapshots, free_bytes=10 * 1024**3, sync_healthy=True)
    con = readonly_connect(db)
    try:
        kept = con.execute(
            "select name from torrent_health where hash='abcdef0123456789'"
        ).fetchone()[0]
    finally:
        con.close()
    assert kept == "SONE-792 Complete"


def test_telegram_home_shows_name_or_hash_fallback(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    write_execute(
        db,
        "insert into torrent_health("
        "hash,name,sampled_at,dlspeed_bps,progress,updated_at,active_since) "
        "values('hashwithname000','BBAN-582',1,100,0.624,1,1)",
    )
    write_execute(
        db,
        "insert into torrent_health("
        "hash,name,sampled_at,dlspeed_bps,progress,updated_at,active_since) "
        "values('abcdef0123456789abcdef',null,1,50,0.931,1,1)",
    )
    snap = DashboardRepository(db).home_snapshot()
    names = [item["name"] for item in snap["active"]]
    assert "BBAN-582" in names
    assert "abcdef012345" in names
    home = TelegramPanelRenderer(
        DashboardRepository(db), now=lambda: 1000
    ).render_home()
    assert "BBAN-582" in home.text
    assert "abcdef012345" in home.text
    assert "编排器控制台" in home.text


def test_warning_detail_opens_beyond_recent_page(tmp_path):
    from qbt_orchestrator.telegram_control import TelegramAuthorizer
    from qbt_orchestrator.telegram_router import TelegramUpdateRouter

    db = tmp_path / "warn.sqlite"
    migrate(db)
    for i in range(101):
        write_execute(
            db,
            "insert into bot_warning_inbox("
            "warning_key,severity,topic,safe_message,occurrence_count,"
            "first_occurred_at,last_occurred_at,updated_at,resolved) "
            "values(?,?,?,?,?,?,?,?,0)",
            (f"w-{i}", "warning", "t", f"msg-{i}", 1, i + 1, i + 1, i + 1),
        )
    warnings = WarningInboxRepository(db)
    oldest = warnings.get(1)
    assert oldest is not None
    assert oldest["safe_message"] == "msg-0"

    class FakeApi:
        def __init__(self):
            self.edits: list[tuple] = []
            self.messages: list[tuple] = []
            self._next_id = 20

        def answer_callback_query(self, *a, **k):
            return {"ok": True}

        def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
            self.edits.append((chat_id, message_id, text, reply_markup))
            return {"ok": True}

        def send_message(self, chat_id, text, reply_markup=None):
            self._next_id += 1
            self.messages.append((chat_id, text, reply_markup))
            return {"ok": True, "result": {"message_id": self._next_id}}

    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        warnings=warnings,
        panel_enabled=True,
        admin_user_id="42",
    )
    router.handle_update(
        {
            "update_id": 0,
            "message": {
                "message_id": 1,
                "chat": {"id": 7},
                "from": {"id": 42},
                "text": "/start",
            },
        }
    )
    panel_mid = int(router.panel.sessions.get()["message_id"])
    router.handle_update(
        {
            "update_id": 2,
            "callback_query": {
                "id": "cb2",
                "data": "w:d:1:1",
                "from": {"id": 42},
                "message": {"message_id": panel_mid, "chat": {"id": 7}},
            },
        }
    )
    assert api.edits
    detail_text = api.edits[-1][2]
    assert "该警告已不存在或无法读取" not in detail_text
    assert "msg-0" in detail_text
    assert int(warnings.get(1)["resolved"]) == 0
    router.handle_update(
        {
            "update_id": 3,
            "callback_query": {
                "id": "cb3",
                "data": "w:r:1:1",
                "from": {"id": 42},
                "message": {"message_id": panel_mid, "chat": {"id": 7}},
            },
        }
    )
    assert int(warnings.get(1)["resolved"]) == 1
    assert warnings.unread_count() == 100

    missing = router.renderer.render_warning_detail_by_id(999999)
    assert "该警告已不存在或无法读取" in missing.text


def test_scheduler_all_stopped_does_not_write_inbox_or_notifications(tmp_path):
    db = tmp_path / "stopped.sqlite"
    migrate(db)
    warnings = WarningService(db, admin_chat_id="7", now=lambda: 100)
    repo = BotNotificationRepository(db, now=lambda: 100)
    alerts = SchedulerAlertService(
        repo,
        SchedulerAlertConfig(enabled=True, chat_ids=["7"], interval_sec=60),
        now=lambda: 100,
        warning_service=warnings,
    )
    snapshots = {
        f"h{i}": {
            "hash": f"h{i}",
            "category": "auto",
            "tags": "auto",
            "state": "stoppedDL",
            "amount_left": 10,
            "progress": 0.1,
        }
        for i in range(25)
    }
    result = PlannerResult(
        selected_hashes=[],
        paused_hashes=[],
        conservative=False,
        budget_bytes=0,
        mode="normal",
    )
    alerts.evaluate_and_enqueue(
        snapshots=snapshots,
        free_bytes=20 * 1024**3,
        disk_floor_bytes=3 * 1024**3,
        recovery_enter_bytes=4 * 1024**3,
        emergency_floor_bytes=2 * 1024**3,
        planner_result=result,
        sync_healthy=True,
    )
    con = readonly_connect(db)
    try:
        notes = con.execute(
            "select count(*) from bot_notifications where topic='scheduler_all_stopped'"
        ).fetchone()[0]
        inbox = con.execute(
            "select count(*) from bot_warning_inbox "
            "where warning_key='daemon_task:scheduler:all_stopped' "
            "or topic='scheduler_all_stopped'"
        ).fetchone()[0]
    finally:
        con.close()
    assert int(notes) == 0
    assert int(inbox) == 0
