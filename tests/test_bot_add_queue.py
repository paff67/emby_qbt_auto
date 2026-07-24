from __future__ import annotations

import json
import errno
import os
import sqlite3
import stat
import subprocess
import sys
from pathlib import Path

import pytest

import qbt_orchestrator.db as db_module
from qbt_orchestrator.db import migrate, migration_sql, readonly_connect


EXPECTED_TABLES = {
    "bot_add_batches",
    "bot_add_shards",
    "bot_add_items",
    "bot_add_events",
    "remote_media_index",
    "bot_warning_inbox",
    "bot_warning_reads",
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
        assert con.execute("select max(version) from schema_migrations").fetchone()[0] == 16
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
    finally:
        con.close()
