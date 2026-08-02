from __future__ import annotations

from pathlib import Path

import pytest

from qbt_orchestrator.db import migrate, readonly_connect, write_execute
from qbt_orchestrator.telegram_ui import DashboardRepository, TelegramPanelRenderer
from qbt_orchestrator.warning_inbox import WarningInboxRepository


def _seed_related_sources(db: Path) -> None:
    write_execute(
        db,
        "insert into torrent_jobs(id, hash, job_type, state, created_at, updated_at) "
        "values(9, 'jobhash', 'upload', 'queued', 1, 1)",
    )
    write_execute(
        db,
        "insert into events_v2(ts, level, component, event_type, message, hash, job_id) "
        "values(100, 'INFO', 'planner', 'download_started', 'hash event', 'abc123', null)",
    )
    write_execute(
        db,
        "insert into events_v2(ts, level, component, event_type, message, hash, job_id) "
        "values(110, 'INFO', 'planner', 'download_started', 'job event', null, 9)",
    )
    write_execute(
        db,
        "insert into bot_add_batches("
        "id,batch_key,chat_id,user_id,state,created_at,updated_at) "
        "values(7,'b7','1','1','complete',1,1)",
    )
    write_execute(
        db,
        "insert into bot_add_items("
        "id,batch_id,source_message_id,source_index,input_kind,redacted_input,"
        "input_sha256,state,created_at,updated_at) "
        "values(11,7,1,0,'magnet','r','sha-11','enrolled',1,1)",
    )
    write_execute(
        db,
        "insert into bot_add_events("
        "created_at,batch_id,item_id,event_type,from_state,to_state,"
        "reason_code,safe_evidence_json) "
        "values(120,7,null,'batch_complete','processing','complete','ok','{}')",
    )
    write_execute(
        db,
        "insert into bot_add_events("
        "created_at,batch_id,item_id,event_type,from_state,to_state,"
        "reason_code,safe_evidence_json) "
        "values(130,7,11,'item_enrolled','ready','enrolled','ok','{}')",
    )


@pytest.mark.parametrize(
    ("fields", "needle"),
    [
        ({"related_hash": "abc123"}, "hash event"),
        ({"related_job_id": 9}, "job event"),
        ({"related_batch_id": 7}, "batch_complete"),
        ({"related_item_id": 11}, "item_enrolled"),
    ],
)
def test_warning_export_covers_four_relations(tmp_path, fields, needle):
    db = tmp_path / "export.sqlite"
    migrate(db)
    _seed_related_sources(db)
    warnings = WarningInboxRepository(db)
    row = warnings.upsert(
        warning_key=f"export:{needle}",
        severity="warning",
        topic="test",
        safe_message="summary",
        **fields,
    )
    text = warnings.export_text(int(row["id"])).decode("utf-8")
    assert needle in text


def test_warning_export_dedupe_sort_and_byte_cap(tmp_path):
    db = tmp_path / "cap.sqlite"
    migrate(db)
    for index, ts in enumerate((198, 199, 200)):
        write_execute(
            db,
            "insert into events_v2(ts, level, component, event_type, message, hash) "
            "values(?, 'INFO', 'c', 't', ?, 'hh')",
            (ts, f"msg-{index}"),
        )
    warnings = WarningInboxRepository(db)
    row = warnings.upsert(
        warning_key="cap",
        severity="warning",
        topic="test",
        safe_message="s",
        related_hash="hh",
    )
    warning_id = int(row["id"])
    text = warnings.export_text(warning_id).decode("utf-8")
    assert text.index("msg-0") < text.index("msg-1") < text.index("msg-2")

    write_execute(db, "delete from events_v2")
    big = "漢" * 20000
    for i in range(40):
        write_execute(
            db,
            "insert into events_v2(ts, level, component, event_type, message, hash) "
            "values(?, 'INFO', 'c', 't', ?, 'hh')",
            (i, big),
        )
    payload = warnings.export_text(warning_id)
    capped = payload.decode("utf-8")
    assert "--- 后续日志因导出限制已省略 ---" in capped
    assert len(payload) <= 512_000
    assert len(payload.splitlines()) <= 1_000


def test_warning_export_has_no_mark_read_side_effect(tmp_path):
    db = tmp_path / "side.sqlite"
    migrate(db)
    warnings = WarningInboxRepository(db)
    row = warnings.upsert(
        warning_key="side",
        severity="warning",
        topic="test",
        safe_message="s",
    )
    warning_id = int(row["id"])
    warnings.export_text(warning_id)
    current = warnings.get(warning_id)
    assert current is not None
    assert int(current["resolved"]) == 0


def test_warning_export_all_includes_related_events(tmp_path):
    db = tmp_path / "all.sqlite"
    migrate(db)
    write_execute(
        db,
        "insert into events_v2(ts, level, component, event_type, message, hash) "
        "values(10, 'WARN', 'c', 'boom', 'related-line', 'hh')",
    )
    warnings = WarningInboxRepository(db)
    warnings.upsert(
        warning_key="all-1",
        severity="warning",
        topic="t1",
        safe_message="summary-one",
        related_hash="hh",
    )
    warnings.upsert(
        warning_key="all-2",
        severity="error",
        topic="t2",
        safe_message="summary-two",
    )
    text = warnings.export_text(None).decode("utf-8")
    assert "summary-one" in text
    assert "related-line" in text
    assert "summary-two" in text


def test_warning_export_router_temp_cleanup(tmp_path):
    from qbt_orchestrator.telegram_control import TelegramAuthorizer
    from qbt_orchestrator.telegram_router import TelegramUpdateRouter

    db = tmp_path / "tmp.sqlite"
    migrate(db)
    warnings = WarningInboxRepository(db)
    row = warnings.upsert(
        warning_key="tmp",
        severity="warning",
        topic="test",
        safe_message="s",
    )
    warning_id = int(row["id"])

    class FakeApi:
        def __init__(self):
            self.paths: list[str] = []

        def send_document(self, chat_id, path, *, filename=None, caption=None):
            self.paths.append(str(path))
            assert Path(path).exists()
            return {"ok": True}

        def send_message(self, chat_id, text, reply_markup=None):
            return {"ok": True}

        def answer_callback_query(self, *args, **kwargs):
            return {"ok": True}

        def edit_message_text(self, *args, **kwargs):
            return {"ok": True}

    api = FakeApi()
    router = TelegramUpdateRouter(
        api=api,
        authorizer=TelegramAuthorizer(admins={42}, single_admin_id=42),
        state_db=db,
        warnings=warnings,
        panel_enabled=True,
        admin_user_id="42",
    )
    before = {p.name for p in Path("/tmp").glob("warning-export-*")}
    router._export_warning(7, warning_id)
    after = {p.name for p in Path("/tmp").glob("warning-export-*")}
    assert api.paths
    assert before == after

    class BoomApi(FakeApi):
        def send_document(self, chat_id, path, *, filename=None, caption=None):
            self.paths.append(str(path))
            raise RuntimeError("send failed")

    boom = BoomApi()
    router.api = boom
    before = {p.name for p in Path("/tmp").glob("warning-export-*")}
    with pytest.raises(RuntimeError):
        router._export_warning(7, warning_id)
    after = {p.name for p in Path("/tmp").glob("warning-export-*")}
    assert before == after


@pytest.mark.parametrize(
    ("kind", "title"),
    [
        ("d", "完成下载"),
        ("i", "完成入库"),
        ("e", "异常任务"),
        ("r", "自动回收"),
        ("m", "手动删除"),
    ],
)
def test_status_history_categories(tmp_path, kind, title):
    db = tmp_path / f"hist-{kind}.sqlite"
    migrate(db)
    now = 1_700_000_000
    if kind == "d":
        write_execute(
            db,
            "insert into processed_media("
            "normalized_id,display_title,origin,lifecycle_state,download_policy,"
            "first_seen_at,first_downloaded_at,last_qbt_hash,created_at,updated_at) "
            "values('BBAN-582','Title','test','downloaded','normal',1,?, 'h1',1,1)",
            (now,),
        )
    elif kind == "i":
        write_execute(
            db,
            "insert into processed_media("
            "normalized_id,display_title,origin,lifecycle_state,download_policy,"
            "first_seen_at,last_ingested_at,last_qbt_hash,created_at,updated_at) "
            "values('SONE-792','Title','test','ingested_present','normal',1,?,'h2',1,1)",
            (now,),
        )
    elif kind == "e":
        write_execute(
            db,
            "insert into processed_media("
            "normalized_id,display_title,origin,lifecycle_state,download_policy,"
            "first_seen_at,manual_delete_requested_at,created_at,updated_at) "
            "values('FAIL-1','Title','test','manual_delete_failed','block_permanent',1,1,1,?)",
            (now,),
        )
    elif kind == "r":
        write_execute(
            db,
            "insert into capacity_reclaims("
            "reclaim_key,hash,name,magnet_uri,host_path,content_path,allocated_bytes,"
            "state,created_at,updated_at,reclaimed_at) "
            "values('rk1','rh1','ABF-055CH','magnet:x','/host','/content',8700000000,"
            "'reclaimed',1,1,?)",
            (now,),
        )
    else:
        write_execute(
            db,
            "insert into processed_media("
            "normalized_id,display_title,origin,lifecycle_state,download_policy,"
            "first_seen_at,manual_delete_requested_at,manually_deleted_at,"
            "created_at,updated_at) "
            "values('DEL-1','Title','test','manual_deleted','block_permanent',1,1,?,1,1)",
            (now,),
        )
    renderer = TelegramPanelRenderer(DashboardRepository(db))
    view = renderer.render_status_history(kind, 0)
    assert f"{title}记录 · 第 1 页" in view.text
    assert "manual_delete_failed" not in view.text
    assert "missing_unknown" not in view.text
    if kind == "r":
        assert "ABF-055CH" in view.text
        assert "GiB" in view.text
    elif kind == "d":
        assert "BBAN-582" in view.text
    elif kind == "i":
        assert "SONE-792" in view.text


@pytest.mark.parametrize("count", [0, 8, 9])
def test_status_history_pagination_boundaries(tmp_path, count):
    db = tmp_path / f"page-{count}.sqlite"
    migrate(db)
    for i in range(count):
        write_execute(
            db,
            "insert into processed_media("
            "normalized_id,display_title,origin,lifecycle_state,download_policy,"
            "first_seen_at,first_downloaded_at,created_at,updated_at) "
            "values(?,?, 'test','downloaded','normal',1,?,?,1)",
            (f"ID-{i}", "T", 1_700_000_000 + i, 1),
        )
    dash = DashboardRepository(db)
    rows, total = dash.history_pages("d", page=0)
    assert total == count
    assert len(rows) == min(count, 8)
    view = TelegramPanelRenderer(dash).render_status_history("d", 0)
    assert "第 1 页" in view.text
    if count == 0:
        assert "当前没有记录" in view.text
    if count == 9:
        assert any(
            btn.get("text") == "下一页"
            for row in view.reply_markup["inline_keyboard"]
            for btn in row
        )
        page1 = TelegramPanelRenderer(dash).render_status_history("d", 1)
        assert "第 2 页" in page1.text


def test_status_home_and_legacy_callback(tmp_path):
    db = tmp_path / "home.sqlite"
    migrate(db)
    renderer = TelegramPanelRenderer(DashboardRepository(db))
    home = renderer.render_status_home()
    assert "完成下载" in str(home.reply_markup)
    assert "手动删除" in str(home.reply_markup)
    legacy = renderer.render_status(0)
    assert legacy.text == home.text


def test_home_snapshot_matches_history_page_totals_after_manual_delete(tmp_path):
    db = tmp_path / "align.sqlite"
    migrate(db)
    now = 1_700_000_000
    # Lifecycle is manual_deleted, but cumulative download/ingest timestamps remain.
    write_execute(
        db,
        "insert into processed_media("
        "normalized_id,display_title,origin,lifecycle_state,download_policy,"
        "first_seen_at,first_downloaded_at,last_ingested_at,"
        "manual_delete_requested_at,manually_deleted_at,created_at,updated_at) "
        "values('BBAN-582','Title','test','manual_deleted','block_permanent',"
        "1,?,?,?,?,1,1)",
        (now - 100, now - 50, now - 10, now),
    )
    write_execute(
        db,
        "insert into capacity_reclaims("
        "reclaim_key,hash,name,magnet_uri,host_path,content_path,allocated_bytes,"
        "state,created_at,updated_at,reclaimed_at) "
        "values('rk1','rh1','ABF-055CH','magnet:x','/host','/content',100,"
        "'reclaimed',1,1,?)",
        (now,),
    )
    dash = DashboardRepository(db)
    snap = dash.home_snapshot()
    assert snap["downloaded"] == dash.history_pages("d")[1]
    assert snap["ingested"] == dash.history_pages("i")[1]
    assert snap["abnormal"] == dash.history_pages("e")[1]
    assert snap["manual_deleted"] == dash.history_pages("m")[1]
    assert snap["reclaimed"] == dash.history_pages("r")[1]
    assert snap["downloaded"] == 1
    assert snap["ingested"] == 1
    assert snap["manual_deleted"] == 1
    assert snap["abnormal"] == 0
    assert snap["reclaimed"] == 1


def test_migration_22_idempotent_stamps_once(tmp_path):
    db = tmp_path / "m22.sqlite"
    migrate(db)
    write_execute(db, "delete from schema_migrations where version=22")
    write_execute(
        db,
        "insert into bot_add_batches("
        "batch_key,chat_id,user_id,state,created_at,updated_at,"
        "initial_summary_sent_at,final_summary_sent_at) "
        "values('old','1','1','complete',1,1,null,null)",
    )
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        cols = {str(r[1]) for r in con.execute("pragma table_info(torrent_health)")}
        bcols = {str(r[1]) for r in con.execute("pragma table_info(bot_add_batches)")}
        stamped = con.execute(
            "select initial_summary_sent_at is not null, "
            "final_summary_sent_at is not null from bot_add_batches where batch_key='old'"
        ).fetchone()
    finally:
        con.close()
    assert "name" in cols
    assert "final_summary_sent_at" in bcols
    assert stamped[0] == 1
    assert stamped[1] == 1
    write_execute(
        db,
        "insert into bot_add_batches("
        "batch_key,chat_id,user_id,state,created_at,updated_at,submitted_at) "
        "values('new','1','1','processing',1,1,1)",
    )
    migrate(db)
    con = readonly_connect(db)
    try:
        fresh = con.execute(
            "select initial_summary_sent_at, final_summary_sent_at "
            "from bot_add_batches where batch_key='new'"
        ).fetchone()
    finally:
        con.close()
    assert fresh[0] is None
    assert fresh[1] is None
