from __future__ import annotations

import sqlite3

import pytest

from qbt_orchestrator.db import migrate, readonly_connect


def test_processed_media_schema_and_triggers(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = sqlite3.connect(db)
    con.execute("pragma foreign_keys=ON")
    try:
        tables = {
            str(row[0]) for row in con.execute("select name from sqlite_master where type='table'")
        }
        assert {
            "processed_media",
            "processed_media_aliases",
            "processed_media_events",
        } <= tables

        media_columns = {
            str(row[1]) for row in con.execute("pragma table_info(processed_media)")
        }
        assert {
            "normalized_id",
            "lifecycle_state",
            "download_policy",
            "ingestion_count",
            "manual_delete_requested_at",
            "manually_deleted_at",
            "deletion_manifest",
            "row_version",
        } <= media_columns

        batch_columns = {
            str(row[1]) for row in con.execute("pragma table_info(bot_add_batches)")
        }
        assert "blocked_history_count" in batch_columns

        index_columns = {
            tuple(str(column[2]) for column in con.execute(f'pragma index_info("{row[1]}")'))
            for row in con.execute("pragma index_list(processed_media)")
        }
        assert ("download_policy", "manually_deleted_at", "id") in index_columns
        assert ("lifecycle_state", "updated_at", "id") in index_columns

        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into processed_media("
                "normalized_id,origin,lifecycle_state,download_policy,"
                "first_seen_at,created_at,updated_at) values(?,?,?,?,?,?,?)",
                ("BAD-1", "test", "bogus", "normal", 1, 1, 1),
            )

        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into processed_media("
                "normalized_id,origin,lifecycle_state,download_policy,"
                "first_seen_at,created_at,updated_at) values(?,?,?,?,?,?,?)",
                ("BAD-2", "test", "observed", "block_permanent", 1, 1, 1),
            )

        cur = con.execute(
            "insert into processed_media("
            "normalized_id,origin,lifecycle_state,download_policy,"
            "first_seen_at,manual_delete_requested_at,created_at,updated_at) "
            "values(?,?,?,?,?,?,?,?)",
            ("BBAN-582", "test", "manual_deleted", "block_permanent", 1, 2, 1, 1),
        )
        media_id = int(cur.lastrowid)
        con.execute(
            "insert into processed_media_events("
            "processed_media_id,event_type,event_at,actor_type,payload_json) "
            "values(?,?,?,?,?)",
            (media_id, "manual_deleted", 3, "cli", "{}"),
        )
        event_id = int(con.execute("select id from processed_media_events").fetchone()[0])

        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update processed_media set download_policy='normal' where id=?",
                (media_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("delete from processed_media where id=?", (media_id,))
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update processed_media_events set event_type='mutated' where id=?",
                (event_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute("delete from processed_media_events where id=?", (event_id,))

        con.execute(
            "insert into processed_media_aliases("
            "processed_media_id,alias_type,alias_value,alias_sha256,created_at) "
            "values(?,?,?,?,?)",
            (media_id, "normalized_id", "BBAN-582", "a" * 64, 1),
        )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into processed_media_aliases("
                "processed_media_id,alias_type,alias_value,alias_sha256,created_at) "
                "values(?,?,?,?,?)",
                (media_id, "normalized_id", "BBAN-582", "a" * 64, 1),
            )
    finally:
        con.close()


def test_processed_media_migration_idempotent_and_readable(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        count = con.execute("select count(*) from processed_media").fetchone()[0]
        assert int(count) == 0
        version21 = con.execute(
            "select name from schema_migrations where version=21"
        ).fetchone()
        version22 = con.execute(
            "select name from schema_migrations where version=22"
        ).fetchone()
        version23 = con.execute(
            "select name from schema_migrations where version=23"
        ).fetchone()
        version24 = con.execute(
            "select name from schema_migrations where version=24"
        ).fetchone()
        assert version21 is not None
        assert version21[0] == "processed_media_tombstone_ledger_v1"
        assert version22 is not None
        assert version22[0] == "torrent_name_and_batch_summary_v1"
        assert version23 is not None
        assert version23[0] == "telegram_persistent_panel_v1"
        assert version24 is not None
        assert version24[0] == "telegram_panel_refresh_attempt_v1"
        assert "name" in {
            row[1] for row in con.execute("pragma table_info(torrent_health)")
        }
        assert "final_summary_sent_at" in {
            row[1] for row in con.execute("pragma table_info(bot_add_batches)")
        }
        tables = {
            str(row[0])
            for row in con.execute("select name from sqlite_master where type='table'")
        }
        assert "telegram_panel_session" in tables
    finally:
        con.close()


def test_processed_media_repository_lifecycle_and_no_untombstone(tmp_path):
    from qbt_orchestrator.processed_media import ProcessedMediaRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 1000, enforce=True)
    observed = repo.observe("BBAN-574", origin="test", display_title="BBAN-574")
    assert observed["lifecycle_state"] == "observed"
    downloaded = repo.mark_downloaded("BBAN-574", qbt_hash="abc")
    assert downloaded["lifecycle_state"] == "downloaded"
    uploaded = repo.mark_uploaded("BBAN-574", remote_path="gcrypt:/BBAN-574/a.mp4", remote_size=10)
    assert uploaded["lifecycle_state"] == "uploaded"
    ingested = repo.mark_ingested("BBAN-574", remote_path="gcrypt:/BBAN-574/a.mp4", remote_size=10)
    assert ingested["lifecycle_state"] == "ingested_present"
    assert int(ingested["ingestion_count"]) == 1
    again = repo.mark_ingested("BBAN-574", remote_path="gcrypt:/BBAN-574/a.mp4", remote_size=10)
    assert int(again["ingestion_count"]) == 2
    assert len(repo.history("BBAN-574", limit=10)) >= 5
    assert repo.upsert_alias("BBAN-574", alias_type="video_basename", alias_value="a.mp4")
    pending = repo.begin_manual_delete("BBAN-574", actor_id="ops", reason="cleanup")
    assert pending["download_policy"] == "block_permanent"
    assert pending["lifecycle_state"] == "manual_delete_pending"
    final = repo.finalize_manual_delete("BBAN-574", actor_id="ops")
    assert final["lifecycle_state"] == "manual_deleted"
    blocked = repo.is_permanently_blocked("BBAN-574")
    assert blocked is not None
    con = sqlite3.connect(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update processed_media set download_policy='normal' where normalized_id=?",
                ("BBAN-574",),
            )
    finally:
        con.close()


def test_finalize_manual_delete_writes_stage_atomically_and_resists_fail(tmp_path):
    import json

    from qbt_orchestrator.db import readonly_connect, write_execute
    from qbt_orchestrator.processed_media import ProcessedMediaRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    repo = ProcessedMediaRepository(db, now=lambda: 77, enforce=True)
    repo.observe("BBAN-600", origin="test")
    repo.begin_manual_delete("BBAN-600", actor_id="ops", reason="cleanup")
    final = repo.finalize_manual_delete(
        "BBAN-600",
        actor_id="ops",
        stage_payload={"remote_dir": "gcrypt:/BBAN-600", "trash_path": "trash:BBAN-600"},
    )
    assert final["lifecycle_state"] == "manual_deleted"
    assert int(final["manually_deleted_at"]) == 77
    history = repo.history("BBAN-600", limit=20)
    assert any(item["event_type"] == "manual_deleted" for item in history)
    stage_events = [
        item for item in history if item["event_type"] == "manual_delete_stage"
    ]
    assert stage_events
    assert json.loads(stage_events[0]["payload_json"])["stage"] == "manual_deleted"

    kept = repo.fail_manual_delete("BBAN-600", actor_id="ops", error="should_not_downgrade")
    assert kept["lifecycle_state"] == "manual_deleted"
    assert int(kept["manually_deleted_at"]) == 77
    # Idempotent finalize must not rewrite deletion time or duplicate events.
    again = repo.finalize_manual_delete("BBAN-600", actor_id="ops", effective_at=999)
    assert int(again["manually_deleted_at"]) == 77
    con = readonly_connect(db)
    try:
        failed_events = con.execute(
            "select count(*) from processed_media_events where event_type='manual_delete_failed'"
        ).fetchone()[0]
        deleted_events = con.execute(
            "select count(*) from processed_media_events where event_type='manual_deleted'"
        ).fetchone()[0]
        bad = con.execute(
            "select count(*) from processed_media "
            "where manually_deleted_at is not null "
            "and lifecycle_state='manual_delete_failed'"
        ).fetchone()[0]
    finally:
        con.close()
    assert int(failed_events) == 0
    assert int(deleted_events) == 1
    assert int(bad) == 0

    # Historical contradiction is normalized in-transaction.
    write_execute(
        db,
        "update processed_media set lifecycle_state='manual_delete_failed' "
        "where normalized_id=?",
        ("BBAN-600",),
    )
    # Leave latest stage before final so repair must append manual_deleted stage.
    write_execute(
        db,
        "insert into processed_media_events("
        "processed_media_id,event_type,event_at,actor_type,actor_id,payload_json) "
        "values((select id from processed_media where normalized_id=?),"
        "'manual_delete_stage',78,'cli','ops',?)",
        (
            "BBAN-600",
            json.dumps({"stage": "emby_refreshed"}, ensure_ascii=False, sort_keys=True),
        ),
    )
    # Repair clock must advance past the stale stage timestamp (78).
    repair_repo = ProcessedMediaRepository(db, now=lambda: 80, enforce=True)
    repaired = repair_repo.fail_manual_delete("BBAN-600", actor_id="ops", error="stale")
    assert repaired["lifecycle_state"] == "manual_deleted"
    assert int(repaired["manually_deleted_at"]) == 77
    history = repair_repo.history("BBAN-600", limit=30)
    assert any(
        item["event_type"] == "manual_delete_state_normalized" for item in history
    )
    latest_stage = next(
        item for item in history if item["event_type"] == "manual_delete_stage"
    )
    assert json.loads(latest_stage["payload_json"])["stage"] == "manual_deleted"


def test_processed_media_backfill_dry_run(tmp_path):
    from qbt_orchestrator.db import write_execute
    from qbt_orchestrator.processed_media import ProcessedMediaRepository

    db = tmp_path / "state.sqlite"
    migrate(db)
    write_execute(
        db,
        "insert into media_groups(media_group_key,normalized_id,emby_media_dir,created_at,updated_at) "
        "values(?,?,?,?,?)",
        ("BBAN-580", "BBAN-580", "/media/gcrypt/BBAN-580", 1, 1),
    )
    write_execute(
        db,
        "insert into media_groups(media_group_key,normalized_id,emby_media_dir,created_at,updated_at) "
        "values(?,?,?,?,?)",
        ("normalize_failed", "normalize_failed", "/media/gcrypt/x", 1, 1),
    )
    repo = ProcessedMediaRepository(db, now=lambda: 50)
    counts = repo.backfill_from_sources(dry_run=True)
    assert counts["inserted"] >= 1
    assert counts["skipped"] >= 1
    assert repo.get_by_normalized_id("BBAN-580") is None


def test_processed_media_tombstone_cli(tmp_path):
    from qbt_orchestrator.cli import main

    db = tmp_path / "state.sqlite"
    migrate(db)
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"ids":["BBAN-586"]}', encoding="utf-8")
    code = main(
        [
            "processed-media",
            "tombstone",
            "--state-db",
            str(db),
            "--id",
            "BBAN-586",
            "--deleted-at",
            "1754116704",
            "--actor",
            "ops",
            "--manifest",
            str(manifest),
            "--create-from-audit",
            "--apply",
            "--json",
        ]
    )
    assert code == 0
    from qbt_orchestrator.processed_media import ProcessedMediaRepository

    row = ProcessedMediaRepository(db, enforce=True).is_permanently_blocked("BBAN-586")
    assert row is not None
    assert row["lifecycle_state"] == "manual_deleted"
