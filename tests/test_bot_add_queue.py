from __future__ import annotations

import json
import errno
import os
import sqlite3
import stat
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import qbt_orchestrator.db as db_module
import qbt_orchestrator.bot_add_queue as queue_module
from qbt_orchestrator.db import migrate, migration_sql, readonly_connect
from qbt_orchestrator.bot_add_queue import AddQueueLimits, BotAddQueueRepository


EXPECTED_TABLES = {
    "bot_add_batches",
    "bot_add_shards",
    "bot_add_items",
    "bot_add_events",
    "remote_media_index",
    "remote_media_index_refresh_state",
    "bot_warning_inbox",
    "processed_media",
    "processed_media_aliases",
    "processed_media_events",
}

ITEM_COLUMNS = {
    "id",
    "batch_id",
    "source_message_id",
    "source_index",
    "input_kind",
    "raw_input",
    "raw_input_expires_at",
    "redacted_input",
    "input_sha256",
    "canonical_identity",
    "infohash_v1",
    "infohash_v2",
    "display_name",
    "normalized_media_id",
    "total_size",
    "primary_video_size",
    "state",
    "decision",
    "decision_reason",
    "qbt_hash",
    "qbt_precheck_tag",
    "remote_match_json",
    "approval_generation",
    "metadata_probe_attempt",
    "metadata_probe_started_at",
    "metadata_probe_deadline",
    "metadata_next_poll_at",
    "metadata_retry_at",
    "metadata_lease_owner",
    "metadata_lease_generation",
    "metadata_lease_until",
    "approved_by",
    "approved_at",
    "attempts",
    "next_run_at",
    "last_error",
    "created_at",
    "updated_at",
}

ITEM_STATES = {
    "received",
    "invalid",
    "resolving",
    "duplicate_local",
    "waiting_probe_slot",
    "metadata_wait",
    "metadata_retry_wait",
    "metadata_unavailable",
    "prechecking",
    "duplicate_remote",
    "needs_confirmation",
    "ready",
    "enrolling",
    "enrolled",
    "enrolled_hold",
    "failed",
    "cancelled",
}


def test_writer_connection_applies_private_sqlite_preparation(tmp_path, monkeypatch):
    db = tmp_path / "state.sqlite"
    calls: list[Path] = []
    original = db_module._prepare_private_sqlite

    def record(path):
        calls.append(Path(path))
        original(path)

    monkeypatch.setattr(db_module, "_prepare_private_sqlite", record)
    con = db_module._connect(db)
    con.close()

    assert calls == [db]


def test_private_sqlite_preparation_uses_explicit_0600_without_umask(
    tmp_path, monkeypatch
):
    db = tmp_path / "state.sqlite"
    opened: list[tuple[str, int, int | None, int | None]] = []
    closed: list[int] = []
    hardened: list[tuple[int, int]] = []

    monkeypatch.setattr(db_module, "_ENFORCE_POSIX_SQLITE_MODE", True)
    monkeypatch.setattr(db_module, "_SQLITE_NOFOLLOW", 0x20000)
    monkeypatch.setattr(db_module.os, "geteuid", lambda: 0, raising=False)

    def fake_open(path, flags, mode=None, *, dir_fd=None):
        opened.append(
            (
                os.fspath(path),
                int(flags),
                None if mode is None else int(mode),
                None if dir_fd is None else int(dir_fd),
            )
        )
        if dir_fd is None:
            return 40
        if os.fspath(path) == db.name:
            return 41
        raise FileNotFoundError(path)

    def fake_fstat(fd):
        if fd == 40:
            return os.stat_result((stat.S_IFDIR | 0o700, 2, 1, 1, 0, 0, 0, 0, 0, 0))
        return os.stat_result((stat.S_IFREG | 0o644, 4, 3, 1, 0, 0, 0, 0, 0, 0))

    monkeypatch.setattr(db_module.os, "open", fake_open)
    monkeypatch.setattr(db_module.os, "fstat", fake_fstat)
    monkeypatch.setattr(db_module.os, "close", lambda fd: closed.append(int(fd)))
    monkeypatch.setattr(db_module.os, "fchmod", lambda fd, mode: hardened.append((fd, mode)))
    monkeypatch.setattr(
        db_module.os,
        "chmod",
        lambda *_args, **_kwargs: pytest.fail("path chmod must not be used"),
    )

    guard = db_module._prepare_private_sqlite(db)

    nofollow = db_module._SQLITE_NOFOLLOW
    assert opened[0][0] == str(db.parent)
    assert opened[0][1] & nofollow == nofollow
    assert opened[1][0] == db.name
    assert opened[1][2:] == (0o600, 40)
    assert opened[1][1] & nofollow == nofollow
    assert hardened == [(41, 0o600)]
    assert guard.fingerprint == (3, 4)
    guard.close()
    assert closed == [41, 40]


def test_private_sqlite_preparation_fails_closed_on_nofollow_error(
    tmp_path, monkeypatch
):
    db = tmp_path / "state.sqlite"
    monkeypatch.setattr(db_module, "_ENFORCE_POSIX_SQLITE_MODE", True)
    monkeypatch.setattr(db_module, "_SQLITE_NOFOLLOW", 0x20000)
    monkeypatch.setattr(db_module.os, "geteuid", lambda: 0, raising=False)

    def reject_link(path, _flags, _mode=None, *, dir_fd=None):
        if dir_fd is None:
            return 40
        raise OSError(errno.ELOOP, "link rejected")

    monkeypatch.setattr(db_module.os, "open", reject_link)
    monkeypatch.setattr(
        db_module.os,
        "fstat",
        lambda _fd: os.stat_result(
            (stat.S_IFDIR | 0o700, 0, 0, 1, 0, 0, 0, 0, 0, 0)
        ),
    )
    monkeypatch.setattr(db_module.os, "close", lambda _fd: None)
    monkeypatch.setattr(
        db_module.os,
        "chmod",
        lambda *_args, **_kwargs: pytest.fail("must not chmod a link target"),
    )

    with pytest.raises(db_module.SQLiteSecurityError, match="symbolic link"):
        db_module._prepare_private_sqlite(db)


def test_private_sqlite_preparation_rejects_foreign_owned_parent(
    tmp_path, monkeypatch
):
    db = tmp_path / "state.sqlite"
    closed: list[int] = []
    monkeypatch.setattr(db_module, "_ENFORCE_POSIX_SQLITE_MODE", True)
    monkeypatch.setattr(db_module, "_SQLITE_NOFOLLOW", 0x20000)
    monkeypatch.setattr(db_module.os, "geteuid", lambda: 1000, raising=False)
    monkeypatch.setattr(db_module.os, "open", lambda *_args, **_kwargs: 40)
    monkeypatch.setattr(
        db_module.os,
        "fstat",
        lambda _fd: os.stat_result(
            (stat.S_IFDIR | 0o700, 2, 1, 1, 2000, 0, 0, 0, 0, 0)
        ),
    )
    monkeypatch.setattr(db_module.os, "close", lambda fd: closed.append(int(fd)))

    with pytest.raises(db_module.SQLiteSecurityError, match="parent directory owner"):
        db_module._prepare_private_sqlite(db)
    assert closed == [40]


def test_private_sqlite_preparation_requires_nofollow_support(tmp_path, monkeypatch):
    db = tmp_path / "state.sqlite"
    monkeypatch.setattr(db_module, "_ENFORCE_POSIX_SQLITE_MODE", True)
    monkeypatch.setattr(db_module, "_SQLITE_NOFOLLOW", 0)
    monkeypatch.setattr(db_module.os, "geteuid", lambda: 1000, raising=False)
    monkeypatch.setattr(
        db_module.os,
        "open",
        lambda *_args, **_kwargs: pytest.fail("writer must fail before opening paths"),
    )

    with pytest.raises(db_module.SQLiteSecurityError, match="O_NOFOLLOW"):
        db_module._prepare_private_sqlite(db)


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode bits are not available")
def test_posix_writer_creates_private_database_wal_and_shm_under_umask_022(tmp_path):
    db = tmp_path / "state.sqlite"
    root = Path(__file__).resolve().parents[1]
    script = """
import json
import os
import stat
import sys
from pathlib import Path

from qbt_orchestrator.db import _connect

db = Path(sys.argv[1])
os.umask(0o022)
con = _connect(db)
con.execute("pragma journal_mode=WAL")
con.execute("create table permission_probe(id integer primary key, value text)")
con.execute("insert into permission_probe(value) values('safe')")
con.commit()
paths = [db, Path(f"{db}-wal"), Path(f"{db}-shm")]
print(json.dumps({path.name: stat.S_IMODE(path.stat().st_mode) for path in paths}))
con.close()
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(root / "src"), str(root), env.get("PYTHONPATH", "")]
    )

    completed = subprocess.run(
        [sys.executable, "-c", script, str(db)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    modes = json.loads(completed.stdout)

    assert modes == {
        "state.sqlite": stat.S_IRUSR | stat.S_IWUSR,
        "state.sqlite-wal": stat.S_IRUSR | stat.S_IWUSR,
        "state.sqlite-shm": stat.S_IRUSR | stat.S_IWUSR,
    }


@pytest.mark.skipif(os.name != "posix", reason="POSIX inode semantics are required")
def test_posix_writer_rejects_main_and_sidecar_symlinks_without_chmod_target(tmp_path):
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    target = private / "target"
    target.write_text("not a database", encoding="utf-8")
    target.chmod(0o640)
    target_mode = stat.S_IMODE(target.stat().st_mode)

    linked_db = private / "linked.sqlite"
    linked_db.symlink_to(target)
    with pytest.raises(db_module.SQLiteSecurityError, match="symbolic link"):
        db_module._connect(linked_db)
    assert stat.S_IMODE(target.stat().st_mode) == target_mode

    db = private / "state.sqlite"
    con = db_module._connect(db)
    con.close()
    wal_link = Path(f"{db}-wal")
    wal_link.symlink_to(target)
    with pytest.raises(db_module.SQLiteSecurityError, match="sidecar"):
        db_module._connect(db)
    assert stat.S_IMODE(target.stat().st_mode) == target_mode


@pytest.mark.skipif(os.name != "posix", reason="POSIX file types are required")
def test_posix_writer_rejects_non_regular_main_and_sidecar_files(tmp_path):
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    fifo = private / "pipe.sqlite"
    os.mkfifo(fifo)
    with pytest.raises(db_module.SQLiteSecurityError, match="regular file"):
        db_module._connect(fifo)

    db = private / "state.sqlite"
    con = db_module._connect(db)
    con.close()
    shm = Path(f"{db}-shm")
    shm.mkdir()
    with pytest.raises(db_module.SQLiteSecurityError, match="sidecar"):
        db_module._connect(db)


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory modes are required")
def test_posix_writer_requires_a_private_parent_directory(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o770)
    shared.chmod(0o770)
    try:
        with pytest.raises(db_module.SQLiteSecurityError, match="private parent directory"):
            db_module._connect(shared / "state.sqlite")
    finally:
        shared.chmod(0o700)


@pytest.mark.skipif(
    os.name != "posix" or os.geteuid() != 0,
    reason="root is required to create a foreign-owned directory",
)
def test_posix_writer_rejects_foreign_owned_private_parent(tmp_path):
    foreign_uid = 65534
    private = tmp_path / "foreign-parent"
    private.mkdir(mode=0o700)
    os.chown(private, foreign_uid, -1)
    try:
        with pytest.raises(db_module.SQLiteSecurityError, match="parent directory owner"):
            db_module._connect(private / "state.sqlite")
    finally:
        os.chown(private, os.geteuid(), -1)


@pytest.mark.skipif(
    os.name != "posix" or os.geteuid() != 0,
    reason="root is required to create a foreign-owned database",
)
def test_posix_writer_rejects_foreign_owned_main_database(tmp_path):
    private = tmp_path / "private-main"
    private.mkdir(mode=0o700)
    db = private / "state.sqlite"
    db.touch(mode=0o600)
    os.chown(db, 65534, -1)

    with pytest.raises(db_module.SQLiteSecurityError, match="database owner"):
        db_module._connect(db)


@pytest.mark.skipif(
    os.name != "posix" or os.geteuid() != 0,
    reason="root is required to create a foreign-owned sidecar",
)
def test_posix_writer_rejects_foreign_owned_sidecar(tmp_path):
    private = tmp_path / "private-sidecar"
    private.mkdir(mode=0o700)
    db = private / "state.sqlite"
    con = db_module._connect(db)
    con.close()
    wal = Path(f"{db}-wal")
    wal.touch(mode=0o600)
    os.chown(wal, 65534, -1)

    with pytest.raises(db_module.SQLiteSecurityError, match="sidecar.*owner"):
        db_module._connect(db)


@pytest.mark.skipif(os.name != "posix", reason="POSIX inode semantics are required")
def test_posix_writer_rejects_inode_swap_during_sqlite_connect(tmp_path, monkeypatch):
    db = tmp_path / "state.sqlite"
    original_connect = db_module.sqlite3.connect
    swapped = tmp_path / "original.sqlite"

    def replace_during_connect(path, *args, **kwargs):
        os.replace(path, swapped)
        return original_connect(path, *args, **kwargs)

    monkeypatch.setattr(db_module.sqlite3, "connect", replace_during_connect)
    with pytest.raises(db_module.SQLiteSecurityError, match="changed during connect"):
        db_module._connect(db)


def _table_names(con: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in con.execute("select name from sqlite_master where type='table'")
    }


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"pragma table_info({table})")}


def _index_columns(con: sqlite3.Connection, table: str) -> dict[str, tuple[str, ...]]:
    indexes: dict[str, tuple[str, ...]] = {}
    for row in con.execute(f"pragma index_list({table})"):
        name = str(row[1])
        indexes[name] = tuple(
            str(column[2]) for column in con.execute(f'pragma index_info("{name}")')
        )
    return indexes


def _insert_batch(
    con: sqlite3.Connection,
    *,
    batch_key: str,
    state: str = "draft",
    chat_id: str = "chat",
    user_id: str = "user",
) -> int:
    cursor = con.execute(
        "insert into bot_add_batches(batch_key,chat_id,user_id,state,created_at,updated_at) "
        "values(?,?,?,?,?,?)",
        (batch_key, chat_id, user_id, state, 100, 100),
    )
    return int(cursor.lastrowid)


def _insert_item(
    con: sqlite3.Connection,
    batch_id: int,
    *,
    source_message_id: int,
    source_index: int = 0,
    state: str = "received",
    input_kind: str = "magnet",
) -> int:
    cursor = con.execute(
        "insert into bot_add_items("
        "batch_id,source_message_id,source_index,input_kind,redacted_input,input_sha256,"
        "state,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
        (
            batch_id,
            source_message_id,
            source_index,
            input_kind,
            "magnet:[redacted]",
            f"sha-{source_message_id}-{source_index}",
            state,
            100,
            100,
        ),
    )
    return int(cursor.lastrowid)


def test_bot_queue_schema_contains_all_durable_state(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        assert EXPECTED_TABLES <= _table_names(con)
        assert ITEM_COLUMNS <= _columns(con, "bot_add_items")
    finally:
        con.close()


def test_migration_16_is_recorded_once_and_is_idempotent(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        assert [
            str(row[0])
            for row in con.execute(
                "select name from schema_migrations where version=16"
            )
        ] == ["telegram_add_queue_v1"]
        assert con.execute(
            "select count(*) from schema_migrations where version=16"
        ).fetchone()[0] == 1
        assert EXPECTED_TABLES <= _table_names(con)
    finally:
        con.close()


def test_migration_16_repairs_legacy_item_lease_schema_without_losing_rows(tmp_path):
    db = tmp_path / "state.sqlite"
    statements = migration_sql()
    batch_sql = next(
        stmt for stmt in statements if stmt.startswith("create table if not exists bot_add_batches(")
    )
    item_sql = next(
        stmt for stmt in statements if stmt.startswith("create table if not exists bot_add_items(")
    ).replace("metadata_lease_until integer,", "")
    con = sqlite3.connect(db)
    con.execute(
        "create table schema_migrations("
        "version integer primary key,name text not null,applied_at integer not null)"
    )
    con.execute(batch_sql)
    con.execute(item_sql)
    batch_id = _insert_batch(con, batch_key="legacy-v16", state="queued")
    item_id = _insert_item(con, batch_id, source_message_id=1600)
    con.execute(
        "insert into schema_migrations(version,name,applied_at) values(16,?,?)",
        ("telegram_add_queue_v1", 100),
    )
    con.commit()
    con.close()

    migrate(db)
    con = readonly_connect(db)
    try:
        assert "metadata_lease_until" in _columns(con, "bot_add_items")
        assert con.execute("select id from bot_add_items where id=?", (item_id,)).fetchone()
        assert con.execute("select max(version) from schema_migrations").fetchone()[0] == 22
    finally:
        con.close()


def test_migration_16_upgrades_a_database_marked_at_version_15(tmp_path):
    db = tmp_path / "state.sqlite"
    con = sqlite3.connect(db)
    con.execute(
        "create table schema_migrations("
        "version integer primary key,name text not null,applied_at integer not null)"
    )
    con.execute(
        "insert into schema_migrations(version,name,applied_at) values(15,?,?)",
        ("shared_capacity_assessment_v1", 100),
    )
    con.commit()
    con.close()

    migrate(db)
    con = readonly_connect(db)
    try:
        assert EXPECTED_TABLES <= _table_names(con)
        assert con.execute(
            "select count(*) from schema_migrations where version=16"
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_migration_17_adds_durable_remote_index_refresh_metadata_idempotently(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    migrate(db)
    con = readonly_connect(db)
    try:
        assert {
            "source",
            "requested_generation",
            "applied_generation",
            "refreshed_at",
            "row_count",
            "last_attempt_at",
            "last_result",
        } <= _columns(con, "remote_media_index_refresh_state")
        assert con.execute(
            "select name from schema_migrations where version=17"
        ).fetchone()[0] == "remote_media_index_refresh_v1"
        assert con.execute(
            "select count(*) from schema_migrations where version=17"
        ).fetchone()[0] == 1
    finally:
        con.close()


def test_batch_and_shard_state_checks_and_open_draft_uniqueness(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    try:
        for index, state in enumerate(
            (
                "draft",
                "queued",
                "processing",
                "awaiting_confirmation",
                "complete",
                "cancelled",
                "draft_expired",
            )
        ):
            _insert_batch(
                con,
                batch_key=f"batch-{state}",
                state=state,
                chat_id=f"chat-{index}",
            )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_batch(con, batch_key="batch-illegal", state="unknown")

        with pytest.raises(sqlite3.IntegrityError):
            _insert_batch(
                con,
                batch_key="batch-draft",
                state="queued",
                chat_id="another-chat",
            )

        first = _insert_batch(
            con, batch_key="open-one", chat_id="same-chat", user_id="same-user"
        )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_batch(
                con, batch_key="open-two", chat_id="same-chat", user_id="same-user"
            )

        for index, state in enumerate(("queued", "processing", "complete", "cancelled")):
            con.execute(
                "insert into bot_add_shards("
                "batch_id,shard_index,state,created_at,updated_at) values(?,?,?,?,?)",
                (first, index, state, 100, 100),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_add_shards("
                "batch_id,shard_index,state,created_at,updated_at) values(?,?,?,?,?)",
                (first, 0, "queued", 100, 100),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_add_shards("
                "batch_id,shard_index,state,created_at,updated_at) values(?,?,?,?,?)",
                (first, 100, "unknown", 100, 100),
            )
    finally:
        con.close()


def test_item_state_input_kind_unique_ingress_and_integrity_checks(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    try:
        batch_id = _insert_batch(con, batch_key="items", state="queued")
        for index, state in enumerate(sorted(ITEM_STATES)):
            _insert_item(
                con,
                batch_id,
                source_message_id=1000 + index,
                state=state,
                input_kind=("magnet", "http_url", "https_url", "bc_link")[index % 4],
            )

        with pytest.raises(sqlite3.IntegrityError):
            _insert_item(
                con,
                batch_id,
                source_message_id=2000,
                state="unknown",
            )
        with pytest.raises(sqlite3.IntegrityError):
            _insert_item(
                con,
                batch_id,
                source_message_id=2001,
                input_kind="torrent_file",
            )

        _insert_item(con, batch_id, source_message_id=3000, source_index=4)
        with pytest.raises(sqlite3.IntegrityError):
            _insert_item(con, batch_id, source_message_id=3000, source_index=4)
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "insert into bot_add_items("
                "batch_id,source_message_id,source_index,input_kind,raw_input,"
                "redacted_input,input_sha256,state,created_at,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?)",
                (
                    batch_id,
                    4000,
                    0,
                    "magnet",
                    "secret-link",
                    "redacted",
                    "sha-4000",
                    "received",
                    100,
                    100,
                ),
            )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "update bot_add_items set attempts=-1 where batch_id=?",
                (batch_id,),
            )
    finally:
        con.close()


def test_queue_indexes_cover_claim_identity_and_lookup_paths(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        item_indexes = _index_columns(con, "bot_add_items")
        assert ("state", "metadata_retry_at", "id") in item_indexes.values()
        assert ("state", "metadata_lease_until", "id") in item_indexes.values()
        assert ("state", "metadata_next_poll_at", "id") in item_indexes.values()
        assert ("metadata_probe_deadline", "state") in item_indexes.values()
        assert ("canonical_identity",) in item_indexes.values()
        assert ("qbt_hash",) in item_indexes.values()
        assert ("source_message_id", "batch_id", "source_index") in item_indexes.values()
        assert ("raw_input_expires_at", "id") in item_indexes.values()
        batch_indexes = _index_columns(con, "bot_add_batches")
        assert ("chat_id", "user_id") in batch_indexes.values()
        open_draft = next(
            row
            for row in con.execute("pragma index_list(bot_add_batches)")
            if str(row[1]) == "idx_bot_add_open_draft"
        )
        assert int(open_draft[2]) == 1
        assert int(open_draft[4]) == 1
        shard_indexes = _index_columns(con, "bot_add_shards")
        assert ("batch_id", "shard_index") in shard_indexes.values()
    finally:
        con.close()


def test_queue_query_plans_use_ingress_and_raw_expiry_indexes(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        ingress_plan = " ".join(
            str(row[3])
            for row in con.execute(
                "explain query plan select i.batch_id,i.source_index,i.input_sha256 "
                "from bot_add_items i join bot_add_batches b on b.id=i.batch_id "
                "where b.chat_id=? and i.source_message_id=? "
                "order by i.batch_id,i.source_index",
                ("chat", 1),
            )
        )
        raw_plan = " ".join(
            str(row[3])
            for row in con.execute(
                "explain query plan select id,batch_id from bot_add_items "
                "where raw_input is not null and raw_input_expires_at<=? "
                "order by raw_input_expires_at,id limit ?",
                (100, 10),
            )
        )
        assert "idx_bot_add_items_source_message" in ingress_plan
        assert "idx_bot_add_items_raw_expiry" in raw_plan
    finally:
        con.close()


def test_bot_add_events_are_append_only_and_retain_safe_audit_fields(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = sqlite3.connect(db)
    con.execute("pragma foreign_keys=ON")
    try:
        batch_id = _insert_batch(con, batch_key="audit", state="queued")
        item_id = _insert_item(con, batch_id, source_message_id=1)
        event_id = con.execute(
            "insert into bot_add_events("
            "batch_id,item_id,event_type,from_state,to_state,actor_chat_id,actor_user_id,"
            "actor_role,reason_code,safe_evidence_json,created_at) "
            "values(?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id,
                item_id,
                "state_transition",
                "received",
                "resolving",
                "chat",
                "user",
                "operator",
                "accepted",
                '{"input_sha256":"safe"}',
                100,
            ),
        ).lastrowid
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            con.execute(
                "update bot_add_events set reason_code='changed' where id=?", (event_id,)
            )
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            con.execute("delete from bot_add_events where id=?", (event_id,))
    finally:
        con.close()


def test_remote_media_index_has_primary_key_and_normalized_id_index(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        assert {
            "video_path",
            "normalized_id",
            "size",
            "raw_basename",
            "status",
            "source",
            "updated_at",
        } <= _columns(con, "remote_media_index")
        primary_key = {
            str(row[1]) for row in con.execute("pragma table_info(remote_media_index)") if row[5]
        }
        assert primary_key == {"video_path"}
        assert ("normalized_id",) in _index_columns(
            con, "remote_media_index"
        ).values()
        assert ("normalized_id", "video_path") in _index_columns(
            con, "remote_media_index"
        ).values()
        ordered_plan = " ".join(
            str(row[3])
            for row in con.execute(
                "explain query plan select video_path,size from remote_media_index "
                "where normalized_id=? order by video_path",
                ("BBAN-582",),
            )
        )
        assert "idx_remote_media_normalized_path" in ordered_plan
        assert "TEMP B-TREE" not in ordered_plan.upper()
    finally:
        con.close()


class _QueueClock:
    def __init__(self, now: int = 1_800_000_000):
        self.value = now

    def __call__(self) -> int:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += seconds


@pytest.fixture
def queue_fixture(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock()
    return BotAddQueueRepository(db, now=clock), clock, db


def _magnets(start: int, count: int) -> list[str]:
    prefix = "magnet:?xt=" + "urn:btih:"
    return [f"{prefix}{value:040x}" for value in range(start, start + count)]


def _independent_write_transaction(path, callback):
    con = sqlite3.connect(path, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("pragma busy_timeout=5000")
    try:
        result = callback(con)
        con.commit()
        return result
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _advance_item_to_ready(queue, item_id: int) -> dict:
    queue.transition_item(item_id, {"received"}, "resolving", "resolve")
    queue.transition_item(item_id, {"resolving"}, "prechecking", "metadata_ready")
    return queue.transition_item(item_id, {"prechecking"}, "ready", "validated")


def _complete_enrollment(queue, item_id: int, target: str = "enrolled") -> dict:
    _advance_item_to_ready(queue, item_id)
    enrolling = queue.transition_item(item_id, {"ready"}, "enrolling", "enroll_start")
    return queue.transition_item(
        item_id,
        {"enrolling"},
        target,
        "enroll_finish",
        approval_generation=enrolling["approval_generation"],
    )


def test_queue_limits_validate_positive_integer_relationships():
    for field, value in (
        ("max_links_per_batch", 0),
        ("shard_size", 0),
        ("max_submitted_items", -1),
        ("max_link_bytes", True),
        ("max_draft_bytes", 0),
        ("draft_ttl_sec", 0),
        ("raw_input_ttl_sec", 0),
    ):
        with pytest.raises(ValueError, match=field):
            AddQueueLimits(**{field: value})
    with pytest.raises(ValueError, match="shard_size"):
        AddQueueLimits(max_links_per_batch=10, shard_size=11)
    with pytest.raises(ValueError, match="raw_input_ttl_sec"):
        AddQueueLimits(raw_input_ttl_sec=8 * 86400)


def test_ingress_accepts_multi_message_batch_and_creates_fifty_item_shards(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], message_id=10, links=_magnets(0, 40))
    queue.append_message(batch["id"], message_id=11, links=_magnets(40, 35))

    submitted = queue.submit(batch["id"])

    assert submitted["state"] == "queued"
    assert submitted["received_count"] == 75
    assert [row["item_count"] for row in queue.list_shards(batch["id"])] == [50, 25]
    assert [row["source_message_id"] for row in queue.list_items(batch["id"])[:41]] == [10] * 40 + [11]


def test_overflow_message_is_rejected_atomically(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 10, _magnets(0, 490))

    with pytest.raises(ValueError, match="^batch_link_limit$"):
        queue.append_message(batch["id"], 11, _magnets(600, 20))

    assert queue.get_batch(batch["id"])["received_count"] == 490
    assert len(queue.list_items(batch["id"])) == 490


def test_link_limit_counts_utf8_bytes_not_characters(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(max_link_bytes=64),
        now=lambda: 100,
    )
    batch = queue.open_draft("1", "2")
    prefix = "https://example.test/"
    fitting = prefix + ("界" * ((64 - len(prefix.encode("utf-8"))) // 3))
    queue.append_message(batch["id"], 1, [fitting])

    with pytest.raises(ValueError, match="^link_byte_limit$"):
        queue.append_message(batch["id"], 2, [fitting + "界"])

    assert queue.get_batch(batch["id"])["received_count"] == 1


def test_default_link_limit_accepts_exactly_eight_kib_and_rejects_one_more(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    prefix = "https://example.test/"
    exact = prefix + "a" * (8192 - len(prefix.encode("utf-8")))
    queue.append_message(batch["id"], 1, [exact])

    with pytest.raises(ValueError, match="^link_byte_limit$"):
        queue.append_message(batch["id"], 2, [exact + "a"])

    assert queue.get_batch(batch["id"])["received_count"] == 1


def test_draft_raw_byte_limit_rejects_whole_message(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            max_links_per_batch=10,
            shard_size=5,
            max_link_bytes=128,
            max_draft_bytes=100,
        ),
        now=lambda: 100,
    )
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 1, ["https://example.test/" + "a" * 40])

    with pytest.raises(ValueError, match="^draft_byte_limit$"):
        queue.append_message(
            batch["id"],
            2,
            ["https://example.test/" + "b" * 20, "https://example.test/" + "c" * 20],
        )

    assert queue.get_batch(batch["id"])["received_count"] == 1


def test_default_draft_limit_accepts_exactly_two_mib_and_rejects_next_message(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    prefix = "https://example.test/"
    links = []
    for index in range(256):
        suffix = f"/{index:03d}"
        links.append(
            prefix
            + "a" * (8192 - len(prefix.encode("utf-8")) - len(suffix.encode("utf-8")))
            + suffix
        )
    assert sum(len(link.encode("utf-8")) for link in links) == 2 * 1024 * 1024
    queue.append_message(batch["id"], 1, links)

    with pytest.raises(ValueError, match="^draft_byte_limit$"):
        queue.append_message(batch["id"], 2, ["magnet:?xt=" + "urn:btih:" + "f" * 40])

    assert queue.get_batch(batch["id"])["received_count"] == 256


def test_open_draft_is_idempotent_and_scoped_to_chat_and_user(queue_fixture):
    queue, _clock, _db = queue_fixture
    first = queue.open_draft("chat", "user")
    assert queue.open_draft("chat", "user")["id"] == first["id"]
    assert queue.open_draft("chat", "other")["id"] != first["id"]
    assert queue.open_draft("other", "user")["id"] != first["id"]
    assert len(queue.list_batches()) == 3


def test_submit_rejects_entire_draft_when_global_backlog_is_full(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            max_links_per_batch=500,
            shard_size=50,
            max_submitted_items=500,
        ),
        now=lambda: 100,
    )
    first = queue.open_draft("1", "1")
    queue.append_message(first["id"], 1, _magnets(0, 490))
    queue.submit(first["id"])
    second = queue.open_draft("2", "2")
    queue.append_message(second["id"], 2, _magnets(600, 20))

    with pytest.raises(ValueError, match="^global_backlog_limit$"):
        queue.submit(second["id"])

    assert queue.get_batch(second["id"])["state"] == "draft"
    assert queue.list_shards(second["id"]) == []
    assert queue.submitted_nonterminal_count() == 490


def test_default_global_backlog_accepts_one_thousand_then_rejects_next(queue_fixture):
    queue, _clock, _db = queue_fixture
    first = queue.open_draft("1", "1")
    queue.append_message(first["id"], 1, _magnets(0, 500))
    queue.submit(first["id"])
    second = queue.open_draft("2", "2")
    queue.append_message(second["id"], 2, _magnets(1000, 500))
    queue.submit(second["id"])
    overflow = queue.open_draft("3", "3")
    queue.append_message(overflow["id"], 3, _magnets(2000, 1))

    with pytest.raises(ValueError, match="^global_backlog_limit$"):
        queue.submit(overflow["id"])

    assert queue.submitted_nonterminal_count() == 1000
    assert queue.get_batch(overflow["id"])["state"] == "draft"


def test_global_backlog_ignores_drafts_cancelled_and_terminal_items(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            max_links_per_batch=10,
            shard_size=5,
            max_submitted_items=2,
        ),
        now=lambda: 100,
    )
    queued = queue.open_draft("1", "1")
    queue.append_message(queued["id"], 1, _magnets(0, 2))
    queue.submit(queued["id"])
    first_item, second_item = queue.list_items(queued["id"])
    queue.transition_item(first_item["id"], {"received"}, "invalid", "invalid")
    queue.transition_item(second_item["id"], {"received"}, "cancelled", "cancelled")
    draft = queue.open_draft("2", "2")
    queue.append_message(draft["id"], 2, _magnets(20, 2))

    assert queue.submit(draft["id"])["state"] == "queued"
    assert queue.submitted_nonterminal_count() == 2


def test_replayed_telegram_message_is_idempotent_without_duplicate_events(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    links = _magnets(0, 2)
    first = queue.append_message(batch["id"], 100, links)
    event_count = len(queue.list_events(batch["id"]))

    replay = queue.append_message(batch["id"], 100, links)

    assert replay["received_count"] == first["received_count"] == 2
    assert replay["idempotent"] is True
    assert len(queue.list_items(batch["id"])) == 2
    assert len(queue.list_events(batch["id"])) == event_count


def test_replayed_message_id_with_different_content_fails_closed(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 100, _magnets(0, 2))

    with pytest.raises(ValueError, match="^source_message_conflict$"):
        queue.append_message(batch["id"], 100, _magnets(0, 1))
    with pytest.raises(ValueError, match="^source_message_conflict$"):
        queue.append_message(batch["id"], 100, _magnets(10, 2))

    assert queue.get_batch(batch["id"])["received_count"] == 2


def test_replayed_update_is_idempotent_across_successive_batches_in_same_chat(queue_fixture):
    queue, _clock, _db = queue_fixture
    first = queue.open_draft("chat", "user")
    links = _magnets(0, 2)
    queue.append_message(first["id"], 100, links)
    queue.submit(first["id"])
    second = queue.open_draft("chat", "user")
    first_events = len(queue.list_events(first["id"]))

    replay = queue.append_message(second["id"], 100, links)

    assert replay["id"] == first["id"]
    assert replay["idempotent"] is True
    assert queue.get_batch(second["id"])["received_count"] == 0
    assert len(queue.list_events(first["id"])) == first_events


def test_reused_message_id_across_batches_with_different_content_fails_closed(queue_fixture):
    queue, _clock, _db = queue_fixture
    first = queue.open_draft("chat", "user")
    queue.append_message(first["id"], 100, _magnets(0, 1))
    queue.submit(first["id"])
    second = queue.open_draft("chat", "user")

    with pytest.raises(ValueError, match="^source_message_conflict$"):
        queue.append_message(second["id"], 100, _magnets(10, 1))

    assert queue.get_batch(second["id"])["received_count"] == 0


def test_append_rejects_empty_non_string_unsupported_and_duplicate_inputs_atomically(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    for links, error in (
        ([], "empty_message"),
        (["magnet:?xt=" + "urn:btih:" + "0" * 40, 7], "link_type"),
        (["ftp://example.test/file"], "unsupported_link_scheme"),
    ):
        with pytest.raises(ValueError, match=f"^{error}$"):
            queue.append_message(batch["id"], 1, links)
    queue.append_message(batch["id"], 2, _magnets(0, 1))
    with pytest.raises(ValueError, match="^duplicate_input$"):
        queue.append_message(batch["id"], 3, _magnets(0, 1))
    assert queue.get_batch(batch["id"])["received_count"] == 1


def test_ignored_insert_rolls_back_the_entire_message(queue_fixture):
    queue, _clock, db = queue_fixture
    batch = queue.open_draft("1", "2")
    write_transaction = db_module.write_transaction
    write_transaction(
        db,
        lambda con: con.execute(
            "create trigger ignore_second_bot_add_item before insert on bot_add_items "
            "when new.source_message_id=99 and new.source_index=1 "
            "begin select raise(ignore); end"
        ),
    )

    with pytest.raises(ValueError, match="^ingress_conflict$"):
        queue.append_message(batch["id"], 99, _magnets(0, 2))

    assert queue.get_batch(batch["id"])["received_count"] == 0
    assert queue.list_items(batch["id"]) == []
    assert queue.list_events(batch["id"]) == []


def test_redacted_inputs_never_retain_url_credentials_or_query(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(
        batch["id"],
        1,
        [
            "https://alice:secret@example.test/private/path?token=very-secret#frag",
            "magnet:?xt=" + "urn:btih:" + "a" * 40 + "&tr=https://secret.test/a",
            "bc://bt/SECRET",
        ],
    )
    redacted = [row["redacted_input"] for row in queue.list_items(batch["id"])]
    rendered = " ".join(redacted)
    assert "alice" not in rendered
    assert "secret" not in rendered.lower()
    assert "token" not in rendered
    assert "?" not in rendered
    assert queue.list_items(batch["id"], include_raw=False)[0]["raw_input"] is None


def test_message_event_evidence_is_bounded_and_contains_no_raw_links(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 1, _magnets(0, 500))

    evidence = queue.list_events(batch["id"])[0]["safe_evidence_json"]

    assert len(evidence.encode("utf-8")) < 1024
    assert "magnet:" not in evidence


def test_draft_expires_after_thirty_minutes_and_clears_raw(queue_fixture):
    queue, clock, db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    clock.advance(1800)

    replacement = queue.open_draft("1", "2")

    assert replacement["id"] != batch["id"]
    assert queue.get_batch(batch["id"])["state"] == "draft_expired"
    con = readonly_connect(db)
    try:
        raw = con.execute(
            "select raw_input,raw_input_expires_at from bot_add_items where batch_id=?",
            (batch["id"],),
        ).fetchone()
        assert tuple(raw) == (None, None)
    finally:
        con.close()


def test_accessed_draft_expires_even_beyond_bounded_maintenance_window(queue_fixture):
    queue, clock, _db = queue_fixture
    drafts = [queue.open_draft(str(index), str(index)) for index in range(11)]
    queue.append_message(drafts[-1]["id"], 1, _magnets(0, 1))
    clock.advance(1800)

    with pytest.raises(ValueError, match="^draft_expired$"):
        queue.append_message(drafts[-1]["id"], 2, _magnets(2, 1))
    replacement = queue.open_draft("10", "10")

    assert queue.get_batch(drafts[-1]["id"])["state"] == "draft_expired"
    assert replacement["id"] != drafts[-1]["id"]


def test_append_and_submit_reject_expired_or_submitted_drafts(queue_fixture):
    queue, clock, db = queue_fixture
    expired = queue.open_draft("1", "1")
    queue.append_message(expired["id"], 1, _magnets(0, 1))
    clock.advance(1800)
    with pytest.raises(ValueError, match="^draft_expired$"):
        queue.append_message(expired["id"], 2, _magnets(2, 1))
    assert queue.get_batch(expired["id"])["state"] == "draft_expired"
    con = readonly_connect(db)
    try:
        assert con.execute(
            "select raw_input from bot_add_items where batch_id=?", (expired["id"],)
        ).fetchone()[0] is None
    finally:
        con.close()
    submitted = queue.open_draft("2", "2")
    queue.append_message(submitted["id"], 3, _magnets(3, 1))
    queue.submit(submitted["id"])
    with pytest.raises(ValueError, match="^batch_not_draft$"):
        queue.append_message(submitted["id"], 4, _magnets(4, 1))
    assert queue.submit(submitted["id"])["state"] == "queued"


def test_submit_persists_expiry_before_reporting_expired_draft(queue_fixture):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    clock.advance(1800)

    with pytest.raises(ValueError, match="^draft_expired$"):
        queue.submit(batch["id"])

    assert queue.get_batch(batch["id"])["state"] == "draft_expired"
    assert queue.list_shards(batch["id"]) == []


def test_expired_draft_preserves_source_conflict_semantics(queue_fixture):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    clock.advance(1800)

    with pytest.raises(ValueError, match="^source_message_conflict$"):
        queue.append_message(batch["id"], 1, _magnets(10, 1))

    assert queue.get_batch(batch["id"])["state"] == "draft_expired"


def test_expired_draft_replay_returns_existing_batch_without_restoring_raw(queue_fixture):
    queue, clock, db = queue_fixture
    batch = queue.open_draft("1", "1")
    links = _magnets(0, 2)
    queue.append_message(batch["id"], 1, links)
    event_count = len(queue.list_events(batch["id"]))
    clock.advance(1800)

    replay = queue.append_message(batch["id"], 1, links)

    assert replay["id"] == batch["id"]
    assert replay["state"] == "draft_expired"
    assert replay["idempotent"] is True
    assert replay["inserted_count"] == 0
    assert len(queue.list_events(batch["id"])) == event_count + 1
    con = readonly_connect(db)
    try:
        assert con.execute(
            "select count(*) from bot_add_items "
            "where batch_id=? and (raw_input is not null or raw_input_expires_at is not null)",
            (batch["id"],),
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_cancel_clears_raw_cancels_enrolling_and_preserves_only_enrolled_items(queue_fixture):
    queue, _clock, db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 1, _magnets(0, 4))
    queue.submit(batch["id"])
    items = queue.list_items(batch["id"])
    _advance_item_to_ready(queue, items[1]["id"])
    enrolling = queue.transition_item(
        items[1]["id"], {"ready"}, "enrolling", "start"
    )
    _complete_enrollment(queue, items[2]["id"], "enrolled")
    _complete_enrollment(queue, items[3]["id"], "enrolled_hold")

    cancelled = queue.cancel_batch(batch["id"], "operator")

    assert cancelled["state"] == "cancelled"
    assert [row["state"] for row in queue.list_items(batch["id"])] == [
        "cancelled",
        "cancelled",
        "enrolled",
        "enrolled_hold",
    ]
    with pytest.raises(ValueError, match="^state_conflict$"):
        queue.transition_item(
            items[1]["id"],
            {"enrolling"},
            "enrolled",
            "late_result",
            approval_generation=enrolling["approval_generation"],
        )
    assert queue.get_item(items[1]["id"])["approval_generation"] == (
        enrolling["approval_generation"] + 1
    )
    assert queue.submitted_nonterminal_count() == 0
    con = readonly_connect(db)
    try:
        assert con.execute(
            "select count(*) from bot_add_items where batch_id=? and raw_input is not null",
            (batch["id"],),
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_cancelled_batch_allows_only_fenced_enrolled_hold_release(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 2))
    queue.submit(batch["id"])
    pending, held = queue.list_items(batch["id"])
    held_result = _complete_enrollment(queue, held["id"], "enrolled_hold")

    queue.cancel_batch(batch["id"], "operator")

    assert queue.get_batch(batch["id"])["state"] == "cancelled"
    assert queue.get_item(pending["id"])["state"] == "cancelled"
    assert queue.get_item(held["id"])["state"] == "enrolled_hold"
    assert queue.submitted_nonterminal_count() == 0
    with pytest.raises(ValueError, match="^approval_generation_conflict$"):
        queue.transition_item(
            held["id"],
            {"enrolled_hold"},
            "enrolled",
            "wrong_confirmation",
            approval_generation=held_result["approval_generation"] + 1,
        )
    with pytest.raises(ValueError, match="^illegal_transition$"):
        queue.transition_item(
            pending["id"], {"cancelled"}, "resolving", "invalid_reopen"
        )

    released = queue.transition_item(
        held["id"],
        {"enrolled_hold"},
        "enrolled",
        "operator_confirmation",
        approval_generation=held_result["approval_generation"],
    )

    assert released["state"] == "enrolled"
    assert queue.get_batch(batch["id"])["state"] == "cancelled"
    assert queue.get_item(pending["id"])["state"] == "cancelled"
    assert queue.submitted_nonterminal_count() == 0


def test_cancel_rewrites_every_non_enrolled_outcome_to_cancelled(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "2")
    queue.append_message(batch["id"], 1, _magnets(0, 6))
    queue.submit(batch["id"])
    items = queue.list_items(batch["id"])
    queue.transition_item(items[0]["id"], {"received"}, "invalid", "outcome")
    queue.transition_item(items[1]["id"], {"received"}, "duplicate_local", "outcome")
    for item, state in ((items[2], "metadata_unavailable"), (items[3], "failed")):
        queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
        queue.transition_item(item["id"], {"resolving"}, state, "outcome")
    _complete_enrollment(queue, items[4]["id"], "enrolled")

    queue.cancel_batch(batch["id"], "operator")

    assert [row["state"] for row in queue.list_items(batch["id"])] == [
        "cancelled",
        "cancelled",
        "cancelled",
        "cancelled",
        "enrolled",
        "cancelled",
    ]


def test_transition_uses_cas_field_allowlist_and_item_batch_for_events(queue_fixture):
    queue, _clock, _db = queue_fixture
    first = queue.open_draft("1", "1")
    second = queue.open_draft("2", "2")
    queue.append_message(first["id"], 1, _magnets(0, 1))
    queue.append_message(second["id"], 2, _magnets(2, 1))
    queue.submit(second["id"])
    item = queue.list_items(second["id"])[0]

    changed = queue.transition_item(
        item["id"],
        {"received"},
        "resolving",
        "accepted",
        {"attempts": 1, "decision_reason": "safe"},
    )
    assert changed["state"] == "resolving"
    assert changed["raw_input"] is None
    with pytest.raises(ValueError, match="^state_conflict$"):
        queue.transition_item(item["id"], {"received"}, "ready", "raced")
    with pytest.raises(ValueError, match="^invalid_field$"):
        queue.transition_item(item["id"], {"resolving"}, "ready", "bad", {"state = 'failed' --": 1})
    for protected in (
        "metadata_lease_owner",
        "metadata_lease_generation",
        "metadata_lease_until",
        "approval_generation",
    ):
        with pytest.raises(ValueError, match="^invalid_field$"):
            queue.transition_item(
                item["id"], {"resolving"}, "prechecking", "bad", {protected: 1}
            )
    event = queue.list_events(second["id"])[-1]
    assert event["batch_id"] == second["id"]
    assert event["item_id"] == item["id"]
    assert not [
        row
        for row in queue.list_events(first["id"])
        if row["event_type"] == "state_transition"
    ]


def test_draft_terminal_and_same_state_transitions_are_rejected(queue_fixture):
    queue, _clock, _db = queue_fixture
    draft = queue.open_draft("1", "1")
    queue.append_message(draft["id"], 1, _magnets(0, 1))
    draft_item = queue.list_items(draft["id"])[0]
    with pytest.raises(ValueError, match="^batch_not_submitted$"):
        queue.transition_item(draft_item["id"], {"received"}, "resolving", "early")

    queue.submit(draft["id"])
    with pytest.raises(ValueError, match="^illegal_transition$"):
        queue.transition_item(draft_item["id"], {"received"}, "received", "same")
    queue.transition_item(draft_item["id"], {"received"}, "invalid", "invalid")
    with pytest.raises(ValueError, match="^illegal_transition$"):
        queue.transition_item(draft_item["id"], {"invalid"}, "resolving", "reopen")

    cancelled = queue.open_draft("2", "2")
    queue.append_message(cancelled["id"], 2, _magnets(10, 1))
    cancelled_item = queue.list_items(cancelled["id"])[0]
    queue.cancel_batch(cancelled["id"], "operator")
    with pytest.raises(ValueError, match="^illegal_transition$"):
        queue.transition_item(
            cancelled_item["id"], {"cancelled"}, "resolving", "reopen"
        )


def test_metadata_lease_generation_fences_old_workers(queue_fixture):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")

    first = queue.claim_metadata_lease(item["id"], "worker-a", clock.value + 60)
    assert first["metadata_lease_generation"] == 1
    renewed = queue.renew_metadata_lease(
        item["id"], "worker-a", first["metadata_lease_generation"], clock.value + 120
    )
    assert renewed["metadata_lease_until"] == clock.value + 120
    with pytest.raises(ValueError, match="^metadata_lease_conflict$"):
        queue.renew_metadata_lease(
            item["id"], "worker-a", first["metadata_lease_generation"] + 1, clock.value + 180
        )

    fenced = queue.fence_metadata_lease(
        item["id"], "worker-a", first["metadata_lease_generation"]
    )
    assert fenced["metadata_lease_generation"] == 2
    second = queue.claim_metadata_lease(item["id"], "worker-b", clock.value + 60)
    assert second["metadata_lease_generation"] == 3

    with pytest.raises(ValueError, match="^metadata_lease_conflict$"):
        queue.transition_item(
            item["id"],
            {"resolving"},
            "prechecking",
            "stale_worker",
            metadata_lease_owner="worker-a",
            metadata_lease_generation=first["metadata_lease_generation"],
        )
    changed = queue.transition_item(
        item["id"],
        {"resolving"},
        "prechecking",
        "current_worker",
        metadata_lease_owner="worker-b",
        metadata_lease_generation=second["metadata_lease_generation"],
    )
    assert changed["state"] == "prechecking"


def test_active_metadata_lease_requires_explicit_token(queue_fixture):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    lease = queue.claim_metadata_lease(item["id"], "worker", clock.value + 60)

    with pytest.raises(ValueError, match="^metadata_lease_token_required$"):
        queue.transition_item(item["id"], {"resolving"}, "prechecking", "missing")
    queue.release_metadata_lease(
        item["id"], "worker", lease["metadata_lease_generation"]
    )
    assert queue.transition_item(
        item["id"], {"resolving"}, "prechecking", "released"
    )["state"] == "prechecking"


def test_concurrent_metadata_claims_have_one_generation_winner(tmp_path, monkeypatch):
    db = tmp_path / "state.sqlite"
    migrate(db)
    setup = BotAddQueueRepository(db, now=lambda: 100)
    batch = setup.open_draft("1", "1")
    setup.append_message(batch["id"], 1, _magnets(0, 1))
    setup.submit(batch["id"])
    item = setup.list_items(batch["id"])[0]
    setup.transition_item(item["id"], {"received"}, "resolving", "resolve")
    repos = [BotAddQueueRepository(db, now=lambda: 100) for _ in range(2)]
    monkeypatch.setattr(queue_module, "write_transaction", _independent_write_transaction)

    def claim(index: int):
        try:
            result = repos[index].claim_metadata_lease(
                item["id"], f"worker-{index}", 200
            )
            return ("accepted", result["metadata_lease_generation"])
        except ValueError as exc:
            return (str(exc), None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, range(2)))

    assert sorted(result[0] for result in results) == [
        "accepted",
        "metadata_lease_active",
    ]
    assert [result[1] for result in results if result[0] == "accepted"] == [1]
    stored = setup.get_item(item["id"])
    assert stored["metadata_lease_generation"] == 1
    assert stored["metadata_lease_owner"] in {"worker-0", "worker-1"}


def test_active_metadata_lease_precedes_raw_window_checks_and_expires_for_takeover(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(raw_input_ttl_sec=1_000),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    first = queue.claim_metadata_lease(item["id"], "worker-a", 150)

    with pytest.raises(ValueError, match="^metadata_lease_active$"):
        queue.claim_metadata_lease(item["id"], "worker-b", 2_000)

    protected = queue.get_item(item["id"], include_raw=True)
    assert protected["state"] == "resolving"
    assert protected["raw_input"] is not None
    assert protected["metadata_lease_owner"] == "worker-a"
    assert protected["metadata_lease_until"] == 150
    assert protected["metadata_lease_generation"] == first["metadata_lease_generation"]
    assert protected["approval_generation"] == 0

    clock.advance(51)
    takeover = queue.claim_metadata_lease(item["id"], "worker-b", 200)
    assert takeover["metadata_lease_owner"] == "worker-b"
    assert takeover["metadata_lease_until"] == 200
    assert takeover["metadata_lease_generation"] == (
        first["metadata_lease_generation"] + 1
    )


def test_enrollment_generation_is_required_and_completed_at_is_stable(queue_fixture):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    _advance_item_to_ready(queue, item["id"])

    enrolling = queue.transition_item(
        item["id"], {"ready"}, "enrolling", "enroll_start"
    )
    generation = enrolling["approval_generation"]
    assert generation == 1
    with pytest.raises(ValueError, match="^approval_token_required$"):
        queue.transition_item(item["id"], {"enrolling"}, "enrolled_hold", "missing")
    with pytest.raises(ValueError, match="^approval_generation_conflict$"):
        queue.transition_item(
            item["id"],
            {"enrolling"},
            "enrolled_hold",
            "stale",
            approval_generation=generation + 1,
        )
    queue.transition_item(
        item["id"],
        {"enrolling"},
        "enrolled_hold",
        "held",
        approval_generation=generation,
    )
    first_completed_at = queue.get_batch(batch["id"])["completed_at"]
    clock.advance(100)

    queue.transition_item(
        item["id"],
        {"enrolled_hold"},
        "enrolled",
        "allow_scheduling",
        approval_generation=generation,
    )

    assert queue.get_batch(batch["id"])["completed_at"] == first_completed_at


def test_completed_shard_timestamp_is_not_rewritten_by_later_item_progress(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(max_links_per_batch=10, shard_size=1),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 2))
    queue.submit(batch["id"])
    items = queue.list_items(batch["id"])
    queue.transition_item(items[0]["id"], {"received"}, "invalid", "invalid")
    first_completed_at = queue.list_shards(batch["id"])[0]["completed_at"]
    clock.advance(100)

    queue.transition_item(items[1]["id"], {"received"}, "invalid", "invalid")

    shards = queue.list_shards(batch["id"])
    assert shards[0]["completed_at"] == first_completed_at
    assert shards[1]["completed_at"] == clock.value


def test_terminal_transition_clears_raw_expiry_and_retry_secrets(queue_fixture):
    queue, _clock, db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    lease = queue.claim_metadata_lease(item["id"], "worker-secret", 1_800_000_100)
    queue.transition_item(
        item["id"],
        {"resolving"},
        "failed",
        "permanent",
        {
            "metadata_retry_at": 500,
            "metadata_next_poll_at": 501,
            "next_run_at": 503,
            "qbt_precheck_tag": "opaque-secret",
        },
        metadata_lease_owner="worker-secret",
        metadata_lease_generation=lease["metadata_lease_generation"],
    )
    con = readonly_connect(db)
    try:
        row = con.execute(
            "select raw_input,raw_input_expires_at,metadata_retry_at,metadata_next_poll_at,"
            "metadata_lease_owner,metadata_lease_until,next_run_at,qbt_precheck_tag "
            "from bot_add_items where id=?",
            (item["id"],),
        ).fetchone()
        assert tuple(row) == (None,) * 8
    finally:
        con.close()


def test_metadata_unavailable_retains_raw_and_manual_retry_reopens_complete_batch(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"], include_raw=True)[0]
    raw = item["raw_input"]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")

    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )

    assert unavailable["raw_input"] is None  # safe return view
    persisted = queue.get_item(item["id"], include_raw=True)
    assert persisted["raw_input"] == raw
    assert persisted["raw_input_expires_at"] is not None
    terminal_batch = queue.get_batch(batch["id"])
    assert terminal_batch["state"] == "complete"
    assert terminal_batch["failed_count"] == 1
    assert queue.submitted_nonterminal_count() == 0

    retried = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "metadata_retry_wait",
        "manual_retry",
        metadata_action="retry_24h",
        approval_generation=unavailable["approval_generation"],
    )

    assert retried["state"] == "metadata_retry_wait"
    assert queue.get_item(item["id"], include_raw=True)["raw_input"] == raw
    reopened_batch = queue.get_batch(batch["id"])
    assert reopened_batch["state"] == "processing"
    assert reopened_batch["failed_count"] == 0
    assert queue.submitted_nonterminal_count() == 1
    assert queue.list_shards(batch["id"])[0]["processed_count"] == 0


def test_metadata_callbacks_are_generation_fenced_across_repeated_exhaustion(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    first = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )

    with pytest.raises(ValueError, match="^metadata_action_required$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "waiting_probe_slot",
            "not_authorized_by_reason",
            approval_generation=first["approval_generation"],
        )
    with pytest.raises(ValueError, match="^approval_generation_conflict$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "waiting_probe_slot",
            "retry",
            metadata_action="retry_now",
            approval_generation=first["approval_generation"] + 1,
        )

    first_retry = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "waiting_probe_slot",
        "retry",
        metadata_action="retry_now",
        approval_generation=first["approval_generation"],
    )
    assert first_retry["approval_generation"] == first["approval_generation"] + 1
    with pytest.raises(ValueError, match="^state_conflict$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "waiting_probe_slot",
            "duplicate_callback",
            metadata_action="retry_now",
            approval_generation=first["approval_generation"],
        )

    second = queue.transition_item(
        item["id"],
        {"waiting_probe_slot"},
        "metadata_unavailable",
        "probe_exhausted_again",
    )
    assert second["approval_generation"] == first_retry["approval_generation"] + 1
    with pytest.raises(ValueError, match="^approval_generation_conflict$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "waiting_probe_slot",
            "stale_first_callback",
            metadata_action="retry_now",
            approval_generation=first["approval_generation"],
        )
    second_retry = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "waiting_probe_slot",
        "new_callback",
        metadata_action="retry_now",
        approval_generation=second["approval_generation"],
    )
    assert second_retry["approval_generation"] == second["approval_generation"] + 1


@pytest.mark.parametrize(
    (
        "metadata_action",
        "target_state",
        "expected_attempt",
        "due_offset",
        "expected_next_poll_offset",
    ),
    [
        ("retry_now", "waiting_probe_slot", 0, 0, None),
        ("retry_24h", "metadata_retry_wait", 2, 24 * 60 * 60, 24 * 60 * 60),
    ],
)
def test_metadata_retry_actions_reset_three_window_probe_policy(
    queue_fixture,
    metadata_action,
    target_state,
    expected_attempt,
    due_offset,
    expected_next_poll_offset,
):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(
        item["id"],
        {"received"},
        "resolving",
        "third_probe_window",
        {
            "metadata_probe_attempt": 3,
            "metadata_probe_started_at": clock.value - 10,
            "metadata_probe_deadline": clock.value + 10,
            "metadata_next_poll_at": clock.value + 1,
            "metadata_retry_at": clock.value - 20,
            "next_run_at": clock.value - 20,
        },
    )
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )

    retried = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        target_state,
        "operator_action",
        metadata_action=metadata_action,
        approval_generation=unavailable["approval_generation"],
    )

    assert retried["metadata_probe_attempt"] == expected_attempt
    assert retried["metadata_probe_started_at"] is None
    assert retried["metadata_probe_deadline"] is None
    assert retried["metadata_lease_owner"] is None
    assert retried["metadata_lease_until"] is None
    assert retried["metadata_retry_at"] == clock.value + due_offset
    assert retried["next_run_at"] == clock.value + due_offset
    expected_next_poll = (
        None
        if expected_next_poll_offset is None
        else clock.value + expected_next_poll_offset
    )
    assert retried["metadata_next_poll_at"] == expected_next_poll


def test_metadata_retry_24h_is_persisted_and_does_not_claim_probe_slot(queue_fixture):
    queue, clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )

    delayed = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "metadata_retry_wait",
        "retry_later",
        metadata_action="retry_24h",
        approval_generation=unavailable["approval_generation"],
    )

    due_at = clock.value + 24 * 60 * 60
    assert delayed["approval_generation"] == unavailable["approval_generation"] + 1
    assert delayed["metadata_retry_at"] == due_at
    assert delayed["metadata_next_poll_at"] == due_at
    assert delayed["next_run_at"] == due_at
    with pytest.raises(ValueError, match="^metadata_retry_not_due$"):
        queue.claim_metadata_lease(item["id"], "worker", clock.value + 60)
    assert queue.get_item(item["id"])["metadata_lease_owner"] is None

    clock.advance(24 * 60 * 60)
    claimed = queue.claim_metadata_lease(item["id"], "worker", clock.value + 60)
    assert claimed["metadata_lease_owner"] == "worker"
    assert claimed["metadata_probe_attempt"] == 2


def test_metadata_retry_24h_requires_raw_ttl_through_approval_window(tmp_path):
    retry_delay = 24 * 60 * 60
    approval_window = 15 * 60
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            raw_input_ttl_sec=retry_delay + approval_window - 1
        ),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )
    before_batch = queue.get_batch(batch["id"])

    with pytest.raises(ValueError, match="^raw_input_ttl_insufficient$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "metadata_retry_wait",
            "retry_later",
            metadata_action="retry_24h",
            approval_generation=unavailable["approval_generation"],
        )

    unchanged = queue.get_item(item["id"], include_raw=True)
    assert unchanged["state"] == "metadata_unavailable"
    assert unchanged["approval_generation"] == unavailable["approval_generation"]
    assert unchanged["raw_input"] is not None
    assert queue.get_batch(batch["id"]) == before_batch
    assert queue.submitted_nonterminal_count() == 0


def test_metadata_retry_24h_accepts_exact_raw_ttl_boundary(tmp_path):
    retry_delay = 24 * 60 * 60
    approval_window = 15 * 60
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            raw_input_ttl_sec=retry_delay + approval_window
        ),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )

    delayed = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "metadata_retry_wait",
        "retry_later",
        metadata_action="retry_24h",
        approval_generation=unavailable["approval_generation"],
    )

    assert delayed["metadata_retry_at"] == clock.value + retry_delay
    assert delayed["raw_input"] is None  # safe output remains redacted
    assert delayed["raw_input_expires_at"] == clock.value + retry_delay + approval_window
    persisted = queue.get_item(item["id"], include_raw=True)
    assert persisted["raw_input_expires_at"] == clock.value + retry_delay + approval_window


def test_delayed_metadata_claim_with_expired_raw_terminalizes_without_lease(tmp_path):
    retry_delay = 24 * 60 * 60
    approval_window = 15 * 60
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            raw_input_ttl_sec=retry_delay + approval_window
        ),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )
    delayed = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "metadata_retry_wait",
        "retry_later",
        metadata_action="retry_24h",
        approval_generation=unavailable["approval_generation"],
    )
    clock.advance(retry_delay + approval_window)

    with pytest.raises(ValueError, match="^raw_input_unavailable$"):
        queue.claim_metadata_lease(item["id"], "late-worker", clock.value + 60)

    terminal = queue.get_item(item["id"], include_raw=True)
    assert terminal["state"] == "metadata_unavailable"
    assert terminal["raw_input"] is None
    assert terminal["raw_input_expires_at"] is None
    assert terminal["metadata_lease_owner"] is None
    assert terminal["metadata_lease_until"] is None
    assert terminal["approval_generation"] == delayed["approval_generation"] + 1
    terminal_batch = queue.get_batch(batch["id"])
    assert terminal_batch["state"] == "complete"
    assert terminal_batch["failed_count"] == 1
    assert queue.submitted_nonterminal_count() == 0
    with pytest.raises(ValueError, match="^approval_generation_conflict$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "cancelled",
            "stale_callback",
            metadata_action="cancel",
            approval_generation=delayed["approval_generation"],
        )


def test_metadata_renewal_cannot_extend_lease_past_raw_window(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(raw_input_ttl_sec=100),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    claimed = queue.claim_metadata_lease(item["id"], "worker", 150)

    with pytest.raises(ValueError, match="^raw_input_unavailable$"):
        queue.renew_metadata_lease(
            item["id"],
            "worker",
            claimed["metadata_lease_generation"],
            201,
        )

    terminal = queue.get_item(item["id"], include_raw=True)
    assert terminal["state"] == "metadata_unavailable"
    assert terminal["raw_input"] is None
    assert terminal["metadata_lease_owner"] is None
    assert terminal["metadata_lease_until"] is None
    assert terminal["metadata_lease_generation"] == (
        claimed["metadata_lease_generation"] + 1
    )
    assert terminal["approval_generation"] == 1
    assert queue.get_batch(batch["id"])["state"] == "complete"
    assert queue.submitted_nonterminal_count() == 0


def test_metadata_claim_requires_raw_through_probe_deadline(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(raw_input_ttl_sec=100),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(
        item["id"],
        {"received"},
        "resolving",
        "resolve",
        {"metadata_probe_deadline": 201},
    )

    with pytest.raises(ValueError, match="^raw_input_unavailable$"):
        queue.claim_metadata_lease(item["id"], "worker", 150)

    terminal = queue.get_item(item["id"], include_raw=True)
    assert terminal["state"] == "metadata_unavailable"
    assert terminal["raw_input"] is None
    assert terminal["metadata_probe_deadline"] is None
    assert terminal["approval_generation"] == 1
    assert queue.get_batch(batch["id"])["state"] == "complete"


def test_metadata_cancel_consumes_token_without_reopening_complete_batch(queue_fixture):
    queue, _clock, _db = queue_fixture
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )
    completed_at = queue.get_batch(batch["id"])["completed_at"]

    with pytest.raises(ValueError, match="^approval_generation_conflict$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "cancelled",
            "cancel",
            metadata_action="cancel",
            approval_generation=unavailable["approval_generation"] + 1,
        )
    cancelled = queue.transition_item(
        item["id"],
        {"metadata_unavailable"},
        "cancelled",
        "cancel",
        metadata_action="cancel",
        approval_generation=unavailable["approval_generation"],
    )

    assert cancelled["approval_generation"] == unavailable["approval_generation"] + 1
    assert queue.get_item(item["id"], include_raw=True)["raw_input"] is None
    terminal_batch = queue.get_batch(batch["id"])
    assert terminal_batch["state"] == "complete"
    assert terminal_batch["completed_at"] == completed_at
    assert terminal_batch["failed_count"] == 0
    assert queue.submitted_nonterminal_count() == 0
    with pytest.raises(ValueError, match="^batch_not_cancellable$"):
        queue.cancel_batch(batch["id"], "operator")


def test_expired_metadata_unavailable_raw_is_cleared_and_cannot_retry(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(raw_input_ttl_sec=10),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )
    clock.advance(10)

    with pytest.raises(ValueError, match="^raw_input_expired$"):
        queue.transition_item(
            item["id"],
            {"metadata_unavailable"},
            "metadata_retry_wait",
            "manual_retry",
            {"metadata_retry_at": 200, "attempts": 9},
            metadata_action="retry_24h",
            approval_generation=unavailable["approval_generation"],
        )

    persisted = queue.get_item(item["id"], include_raw=True)
    assert persisted["state"] == "metadata_unavailable"
    assert persisted["raw_input"] is None
    assert persisted["raw_input_expires_at"] is None
    assert persisted["metadata_retry_at"] is None
    assert persisted["attempts"] == 0
    assert queue.get_batch(batch["id"])["state"] == "complete"
    assert queue.submitted_nonterminal_count() == 0


def test_manual_retry_reopen_respects_global_backlog_atomically(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(
            max_links_per_batch=10,
            shard_size=5,
            max_submitted_items=1,
        ),
        now=lambda: 100,
    )
    retry_batch = queue.open_draft("1", "1")
    queue.append_message(retry_batch["id"], 1, _magnets(0, 1))
    queue.submit(retry_batch["id"])
    retry_item = queue.list_items(retry_batch["id"])[0]
    queue.transition_item(retry_item["id"], {"received"}, "resolving", "resolve")
    unavailable = queue.transition_item(
        retry_item["id"], {"resolving"}, "metadata_unavailable", "probe_exhausted"
    )
    blocking_batch = queue.open_draft("2", "2")
    queue.append_message(blocking_batch["id"], 2, _magnets(10, 1))
    queue.submit(blocking_batch["id"])

    with pytest.raises(ValueError, match="^global_backlog_limit$"):
        queue.transition_item(
            retry_item["id"],
            {"metadata_unavailable"},
            "waiting_probe_slot",
            "manual_retry",
            {"attempts": 7},
            metadata_action="retry_now",
            approval_generation=unavailable["approval_generation"],
        )

    unchanged = queue.get_item(retry_item["id"], include_raw=True)
    assert unchanged["state"] == "metadata_unavailable"
    assert unchanged["attempts"] == 0
    assert unchanged["raw_input"] is not None
    assert queue.get_batch(retry_batch["id"])["state"] == "complete"
    assert queue.submitted_nonterminal_count() == 1

    blocking_item = queue.list_items(blocking_batch["id"])[0]
    queue.transition_item(blocking_item["id"], {"received"}, "invalid", "invalid")
    queue.transition_item(
        retry_item["id"],
        {"metadata_unavailable"},
        "waiting_probe_slot",
        "manual_retry",
        metadata_action="retry_now",
        approval_generation=unavailable["approval_generation"],
    )
    assert queue.get_batch(retry_batch["id"])["state"] == "processing"
    assert queue.submitted_nonterminal_count() == 1


def test_persisted_shard_counts_survive_runtime_limit_changes(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    original = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(max_links_per_batch=10, shard_size=4),
        now=lambda: 100,
    )
    batch = original.open_draft("1", "1")
    original.append_message(batch["id"], 1, _magnets(0, 6))
    original.submit(batch["id"])
    changed_config = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(max_links_per_batch=10, shard_size=2),
        now=lambda: 101,
    )

    items = changed_config.list_items(batch["id"])
    assert [row["shard_index"] for row in items] == [0, 0, 0, 0, 1, 1]
    changed_config.transition_item(items[4]["id"], {"received"}, "invalid", "invalid")

    assert [row["processed_count"] for row in changed_config.list_shards(batch["id"])] == [0, 1]


def test_raw_input_ttl_cleanup_is_explicit_and_audited(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(draft_ttl_sec=5, raw_input_ttl_sec=10),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    clock.advance(10)

    assert queue.expire_raw_inputs() == {"expired_count": 1, "has_more": False}
    assert queue.expire_raw_inputs() == {"expired_count": 0, "has_more": False}
    assert queue.list_items(batch["id"], include_raw=True)[0]["raw_input"] is None
    assert [event["event_type"] for event in queue.list_events(batch["id"])] == [
        "message_appended",
        "batch_submitted",
        "raw_input_expired",
    ]


def test_raw_input_maintenance_is_bounded_and_reports_more_work(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(raw_input_ttl_sec=10),
        now=clock,
    )
    batches = []
    for index in range(5):
        batch = queue.open_draft(str(index), str(index))
        queue.append_message(batch["id"], index + 1, _magnets(index, 1))
        queue.submit(batch["id"])
        batches.append(batch)
    clock.advance(10)

    assert queue.expire_raw_inputs(limit=2) == {"expired_count": 2, "has_more": True}
    assert queue.expire_raw_inputs(limit=2) == {"expired_count": 2, "has_more": True}
    assert queue.expire_raw_inputs(limit=2) == {"expired_count": 1, "has_more": False}
    assert queue.expire_raw_inputs(limit=2) == {"expired_count": 0, "has_more": False}


def test_transition_immediately_clears_its_expired_raw_input(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    clock = _QueueClock(100)
    queue = BotAddQueueRepository(
        db,
        limits=AddQueueLimits(raw_input_ttl_sec=10),
        now=clock,
    )
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 1))
    queue.submit(batch["id"])
    item = queue.list_items(batch["id"])[0]
    clock.advance(10)

    queue.transition_item(item["id"], {"received"}, "resolving", "resolve")

    assert queue.get_item(item["id"], include_raw=True)["raw_input"] is None
    assert queue.list_events(batch["id"])[-2]["event_type"] == "raw_input_expired"


def test_concurrent_appends_enforce_batch_limit_inside_sqlite_transaction(tmp_path, monkeypatch):
    db = tmp_path / "state.sqlite"
    migrate(db)
    limits = AddQueueLimits(max_links_per_batch=10, shard_size=5)
    queue = BotAddQueueRepository(db, limits=limits, now=lambda: 100)
    batch = queue.open_draft("1", "1")
    queue.append_message(batch["id"], 1, _magnets(0, 6))
    repos = [BotAddQueueRepository(db, limits=limits, now=lambda: 100) for _ in range(2)]
    monkeypatch.setattr(queue_module, "write_transaction", _independent_write_transaction)

    def append(index: int):
        try:
            repos[index].append_message(batch["id"], 10 + index, _magnets(100 + index * 10, 4))
            return "accepted"
        except ValueError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(append, range(2)))

    assert sorted(results) == ["accepted", "batch_link_limit"]
    assert queue.get_batch(batch["id"])["received_count"] == 10


def test_concurrent_submits_enforce_global_backlog_without_partial_shards(tmp_path, monkeypatch):
    db = tmp_path / "state.sqlite"
    migrate(db)
    limits = AddQueueLimits(
        max_links_per_batch=10,
        shard_size=5,
        max_submitted_items=10,
    )
    queues = [BotAddQueueRepository(db, limits=limits, now=lambda: 100) for _ in range(2)]
    batches = []
    for index, queue in enumerate(queues):
        batch = queue.open_draft(str(index), str(index))
        queue.append_message(batch["id"], index + 1, _magnets(index * 20, 6))
        batches.append(batch)
    monkeypatch.setattr(queue_module, "write_transaction", _independent_write_transaction)

    def submit(index: int):
        try:
            queues[index].submit(batches[index]["id"])
            return "accepted"
        except ValueError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, range(2)))

    assert sorted(results) == ["accepted", "global_backlog_limit"]
    states = [queues[0].get_batch(batch["id"])["state"] for batch in batches]
    assert sorted(states) == ["draft", "queued"]
    assert sorted(len(queues[0].list_shards(batch["id"])) for batch in batches) == [0, 2]
    assert queues[0].submitted_nonterminal_count() == 6
