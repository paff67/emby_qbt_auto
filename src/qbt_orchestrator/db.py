from __future__ import annotations
import asyncio, atexit, json, queue, sqlite3, threading, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def readonly_connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    con.execute("pragma query_only=ON")
    con.execute("pragma busy_timeout=2000")
    return con


class ReadonlyConnectionPool:
    """Bounded readonly pool for Telegram/CLI/status reads.

    Reads must not enter the single-writer queue.  Every connection is opened
    `mode=ro` plus `query_only=ON`, so an accidental write from a status path is
    rejected by SQLite instead of contending with the DbActor writer.
    """

    def __init__(self, path: str | Path, max_size: int = 4):
        self.path = Path(path)
        self.max_size = max(1, int(max_size))
        self._pool: "queue.LifoQueue[sqlite3.Connection]" = queue.LifoQueue(maxsize=self.max_size)
        self._created = 0
        self._closed = False
        self._lock = threading.Lock()

    def acquire(self) -> sqlite3.Connection:
        if self._closed:
            raise RuntimeError("readonly connection pool is closed")
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self.max_size:
                    self._created += 1
                    return readonly_connect(self.path)
            return self._pool.get()

    def release(self, con: sqlite3.Connection) -> None:
        if self._closed:
            con.close()
            return
        try:
            self._pool.put_nowait(con)
        except queue.Full:
            con.close()

    def close(self) -> None:
        self._closed = True
        while True:
            try:
                self._pool.get_nowait().close()
            except queue.Empty:
                break


@dataclass(frozen=True)
class WriteResult:
    lastrowid: int
    rowcount: int


def _detach_cursor(value: Any) -> Any:
    """Convert a SQLite cursor into connection-independent write metadata."""
    if not isinstance(value, sqlite3.Cursor):
        return value
    lastrowid = int(value.lastrowid or 0)
    rowcount = int(value.rowcount if value.rowcount is not None else -1)
    value.close()
    return WriteResult(lastrowid, rowcount)


class _SyncWriteActor:
    """Per-DB single writer for synchronous daemon code.

    This is the sync counterpart of `DbActor`: feature modules call
    `write_transaction()` / `write_execute()`, which enqueue work onto one
    writer thread per SQLite file.  The writer owns the only write connection,
    uses WAL + busy_timeout, and never handles read-only status queries.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._queue: "queue.Queue[tuple[str, Any, queue.Queue]]" = queue.Queue()
        self._thread: threading.Thread | None = None
        self._thread_id: int | None = None
        self._connection: sqlite3.Connection | None = None
        self._ready = threading.Event()
        self._persistent = False
        self._direct_lock = threading.RLock()
        self._direct_thread_id: int | None = None
        self._lock = threading.Lock()
        self._stopped = True
        self._enqueued_total = 0
        self._completed_total = 0
        self._failed_total = 0
        self._flushes_completed = 0

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                ready = self._ready
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self._stopped = False
                self._ready.clear()
                self._thread = threading.Thread(target=self._run, name=f"qbt-db-writer:{self.path}", daemon=True)
                self._thread.start()
                ready = self._ready
        if not ready.wait(timeout=5):
            raise TimeoutError(f"SQLite writer did not start for {self.path}")

    def enable_persistence(self) -> None:
        """Keep the writer connection open until actor shutdown."""
        with self._lock:
            self._persistent = True

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            if not thread or not thread.is_alive():
                self._stopped = True
                self._thread = None
                self._thread_id = None
                return
            reply: "queue.Queue[tuple[bool, Any]]" = queue.Queue(maxsize=1)
            self._queue.put(("stop", None, reply))
        ok, value = reply.get()
        thread.join(timeout=5)
        with self._lock:
            self._stopped = True
            self._thread = None
            self._thread_id = None
        if not ok:
            raise value

    def transaction(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        with self._lock:
            persistent = self._persistent
        if not persistent:
            return self._direct_transaction(fn)
        self.start()
        if self._thread_id == threading.get_ident() and self._connection is not None:
            return fn(self._connection)
        reply: "queue.Queue[tuple[bool, Any]]" = queue.Queue(maxsize=1)
        with self._lock:
            self._enqueued_total += 1
        self._queue.put(("transaction", fn, reply))
        ok, value = reply.get()
        if ok:
            return value
        raise value

    def _direct_transaction(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        """Serialize one-off writes without leaving a Windows file handle open."""
        with self._direct_lock:
            if self._direct_thread_id == threading.get_ident() and self._connection is not None:
                return fn(self._connection)
            con = _connect(self.path)
            con.execute("pragma journal_mode=WAL")
            con.execute("pragma busy_timeout=5000")
            self._direct_thread_id = threading.get_ident()
            self._connection = con
            with self._lock:
                self._enqueued_total += 1
            try:
                result = _detach_cursor(fn(con))
                con.commit()
                with self._lock:
                    self._completed_total += 1
                return result
            except Exception:
                con.rollback()
                with self._lock:
                    self._failed_total += 1
                raise
            finally:
                self._connection = None
                self._direct_thread_id = None
                con.close()

    def flush(self) -> None:
        with self._lock:
            persistent = self._persistent
        if not persistent:
            with self._lock:
                self._flushes_completed += 1
            return
        self.start()
        reply: "queue.Queue[tuple[bool, Any]]" = queue.Queue(maxsize=1)
        self._queue.put(("flush", None, reply))
        ok, value = reply.get()
        if not ok:
            raise value

    def metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "path": str(self.path),
                "queue_depth": int(self._queue.qsize()),
                "writes_enqueued": int(self._enqueued_total),
                "writes_completed": int(self._completed_total),
                "writes_failed": int(self._failed_total),
                "flushes_completed": int(self._flushes_completed),
                "running": bool(self._thread and self._thread.is_alive()),
            }

    def _run(self) -> None:
        self._thread_id = threading.get_ident()
        con: sqlite3.Connection | None = None
        self._ready.set()
        try:
            while True:
                op, payload, reply = self._queue.get()
                if op == "stop":
                    try:
                        reply.put((True, None))
                    except Exception as exc:  # pragma: no cover - defensive
                        reply.put((False, exc))
                    finally:
                        self._queue.task_done()
                    return
                try:
                    if op == "flush":
                        if con is not None:
                            con.commit()
                        with self._lock:
                            self._flushes_completed += 1
                        reply.put((True, True))
                    else:
                        if con is None:
                            con = _connect(self.path)
                            con.execute("pragma journal_mode=WAL")
                            con.execute("pragma busy_timeout=5000")
                            self._connection = con
                        try:
                            result = _detach_cursor(payload(con))
                            con.commit()
                        except Exception:
                            con.rollback()
                            raise
                        with self._lock:
                            self._completed_total += 1
                        reply.put((True, result))
                except Exception as exc:
                    with self._lock:
                        self._failed_total += 1
                    reply.put((False, exc))
                finally:
                    with self._lock:
                        persistent = self._persistent
                    if con is not None and not persistent:
                        self._connection = None
                        con.close()
                        con = None
                    self._queue.task_done()
        finally:
            self._connection = None
            if con is not None:
                con.close()


_WRITE_ACTORS: dict[str, _SyncWriteActor] = {}
_WRITE_ACTORS_LOCK = threading.Lock()


def _actor_for(path: str | Path) -> _SyncWriteActor:
    key = str(Path(path).resolve())
    with _WRITE_ACTORS_LOCK:
        actor = _WRITE_ACTORS.get(key)
        if actor is None:
            actor = _SyncWriteActor(Path(key))
            _WRITE_ACTORS[key] = actor
        return actor


def write_transaction(path: str | Path, fn: Callable[[sqlite3.Connection], Any]) -> Any:
    return _actor_for(path).transaction(fn)


def start_persistent_write_actor(path: str | Path) -> None:
    """Acquire a long-lived writer for a daemon-scoped SQLite database."""
    actor = _actor_for(path)
    actor.enable_persistence()
    actor.start()


def stop_write_actor(path: str | Path) -> None:
    key = str(Path(path).resolve())
    with _WRITE_ACTORS_LOCK:
        actor = _WRITE_ACTORS.pop(key, None)
    if actor is not None:
        actor.stop()


def flush_write_actor(path: str | Path) -> None:
    _actor_for(path).flush()


def db_actor_metrics(path: str | Path) -> Dict[str, Any]:
    return _actor_for(path).metrics()


def write_execute(path: str | Path, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> WriteResult:
    def txn(con: sqlite3.Connection) -> WriteResult:
        cur = con.execute(sql, tuple(params))
        return WriteResult(int(cur.lastrowid or 0), int(cur.rowcount if cur.rowcount is not None else -1))
    return write_transaction(path, txn)


def write_executemany(path: str | Path, sql: str, params: list[tuple[Any, ...]]) -> WriteResult:
    def txn(con: sqlite3.Connection) -> WriteResult:
        cur = con.executemany(sql, params)
        return WriteResult(int(cur.lastrowid or 0), int(cur.rowcount if cur.rowcount is not None else -1))
    return write_transaction(path, txn)


def stop_write_actors() -> None:
    with _WRITE_ACTORS_LOCK:
        actors = list(_WRITE_ACTORS.values())
        _WRITE_ACTORS.clear()
    for actor in actors:
        actor.stop()


atexit.register(stop_write_actors)

def migration_sql() -> list[str]:
    return [
        "create table if not exists schema_migrations(version integer primary key, name text not null, applied_at integer not null)",
        "create table if not exists torrent_health(hash text primary key, sampled_at integer, dlspeed_bps integer default 0, upspeed_bps integer default 0, ema_fast_bps real default 0, ema_slow_bps real default 0, completed_bytes integer default 0, last_completed_bytes integer default 0, progress real default 0, num_seeds integer default 0, num_peers integer default 0, last_swarm_seen_at integer, no_swarm_since integer, low_speed_since integer, no_progress_since integer, active_since integer, soak_since integer, dead_since integer, promote_candidate_since integer, updated_at integer)",
        "alter table torrent_health add column no_swarm_since integer",
        "create table if not exists torrent_batches(id integer primary key autoincrement, hash text not null, batch_no integer not null, state text not null, mode text, indices_json text not null default '[]', total_bytes integer default 0, downloaded_bytes integer default 0, reserved_bytes integer default 0, piece_size integer default 0, selected_extents integer default 0, piece_spill_overhead_bytes integer default 0, payload_efficiency real default 1.0, priority_applied integer default 0, upload_job_id integer, created_at integer, updated_at integer, local_pinned_bytes integer default 0, cleanup_deferred_at integer, cleanup_done_at integer, unique(hash,batch_no))",
        "alter table torrent_batches add column downloaded_at integer",
        "alter table torrent_batches add column upload_queued_at integer",
        "alter table torrent_batches add column verified_at integer",
        "alter table torrent_batches add column lease_until integer",
        "alter table torrent_batches add column last_progress_at integer",
        "alter table torrent_batches add column last_progress_bytes integer not null default 0",
        "alter table torrent_batches add column source_present integer not null default 1",
        "create table if not exists batch_file_claims(batch_id integer not null, hash text not null, file_index integer not null, state text not null, created_at integer not null, released_at integer, primary key(batch_id,file_index))",
        "create unique index if not exists idx_batch_file_claim_active on batch_file_claims(hash,file_index) where state='active'",
        "create table if not exists batch_inventory_state(id integer primary key check(id=1), cursor_hash text, window_started_at integer not null, probes_in_window integer not null default 0, updated_at integer not null)",
        "create table if not exists batch_inventory_cache(hash text primary key, snapshot_fingerprint text not null, files_json text not null, piece_size integer not null default 0, refreshed_at integer not null)",
        "create index if not exists idx_torrent_batches_hash_state on torrent_batches(hash,state)",
        "create index if not exists idx_torrent_batches_state on torrent_batches(state)",
        "create index if not exists idx_torrent_batches_cleanup_deferred on torrent_batches(state,cleanup_deferred_at)",
        "create table if not exists resource_reservations(id integer primary key autoincrement, hash text, batch_id integer, kind text, bytes integer not null, state text default 'active', created_at integer, expires_at integer, released_at integer, reason text)",
        "alter table resource_reservations add column accounting_class text not null default 'future_growth'",
        "alter table resource_reservations add column owner text",
        "alter table resource_reservations add column lease_generation integer not null default 0",
        "alter table resource_reservations add column last_observed_at integer",
        "update resource_reservations set accounting_class='current_pinned' where kind='cleanup_pending'",
        "create index if not exists idx_reservations_active on resource_reservations(state,expires_at)",
        "create index if not exists idx_reservations_hash on resource_reservations(hash)",
        "create index if not exists idx_reservations_accounting_active on resource_reservations(accounting_class,state,expires_at)",
        "create table if not exists scheduler_allocations(hash text primary key, desired_state text, applied_state text, slot_kind text, priority_score real default 0, reserved_bytes integer default 0, download_limit_bps integer, upload_limit_bps integer, force_start integer default 0, desired_seq_dl integer, applied_seq_dl integer, allocated_at integer, applied_at integer, expires_at integer, reason text)",
        "alter table scheduler_allocations add column owner text not null default 'legacy'",
        "alter table scheduler_allocations add column plan_generation integer not null default 0",
        "create table if not exists scheduler_intents(component text not null, hash text not null, intent text not null, priority integer not null, expires_at integer, data_json text not null, primary key(component,hash))",
        "create index if not exists idx_scheduler_intents_expiry_priority on scheduler_intents(expires_at,priority,component,hash)",
        "insert or replace into scheduler_intents(component,hash,intent,priority,expires_at,data_json) "
        "select 'batch',tb.hash,'protect_batch',20,"
        "case when sum(case when rr.expires_at is null then 1 else 0 end)>0 then null else max(rr.expires_at) end,"
        "'{\"batch_id\":'||max(tb.id)||'}' "
        "from torrent_batches tb join resource_reservations rr on rr.batch_id=tb.id and rr.kind='batch' "
        "where tb.state in ('reserved','applied_to_qbt','downloading') and rr.state='active' "
        "and (rr.expires_at is null or rr.expires_at>strftime('%s','now')) group by tb.hash",
        "create table if not exists scheduler_plan_state(id integer primary key check(id=1), current_generation integer not null default 0, updated_at integer not null)",
        "create table if not exists disk_state(id integer primary key check(id=1), sampled_at integer, free_bytes integer, pressure_state text, previous_state text, state_since integer, resume_allowed integer default 1)",
        "create table if not exists capacity_state(id integer primary key check(id=1), scheduler_mode text not null, state text not null, entered_at integer not null, last_evaluated_at integer not null, reason text, details_json text not null default '{}')",
        "create table if not exists torrent_jobs(id integer primary key autoincrement, hash text, batch_id integer, job_type text, state text default 'queued', priority integer default 100, payload_json text, payload_schema_version integer default 1, lease_owner text, lease_until integer, attempts integer default 0, max_attempts integer default 6, next_run_at integer, last_exit_code integer, last_stderr_tail text, cancel_requested integer default 0, cancel_requested_at integer, created_at integer, updated_at integer)",
        "alter table torrent_jobs add column phase text",
        "alter table torrent_jobs add column copy_completed_at integer",
        "alter table torrent_jobs add column verification_method text",
        "alter table torrent_jobs add column verification_result_json text",
        "alter table torrent_jobs add column verified_at integer",
        "alter table torrent_jobs add column parent_job_id integer",
        "create unique index if not exists idx_torrent_jobs_cleanup_parent on torrent_jobs(job_type,parent_job_id) where job_type='cleanup_full_torrent' and parent_job_id is not null",
        "update torrent_jobs set phase=case "
        "when state='verify_pending' then 'verifying' "
        "when state='cleanup_wait' then 'cleanup_wait' "
        "when state in ('done','cleanup_deferred') then 'done' "
        "else 'queued_copy' end "
        "where phase is null and job_type in ('upload','sidecar_upload')",
        "create table if not exists action_log(id integer primary key autoincrement, ts integer, correlation_id text, hash text, job_id integer, action_type text, path text, payload_json text, status text, dry_run integer default 0, error text)",
        "create table if not exists events_v2(id integer primary key autoincrement, ts integer, level text, component text, event_type text, hash text, job_id integer, correlation_id text, message text, data_json text)",
        "create table if not exists decision_log(id integer primary key autoincrement, ts integer, component text, hash text, decision text, reason_code text, data_json text)",
        "create table if not exists decision_state(component text not null, hash text not null default '', decision text not null, reason_code text not null, data_fingerprint text not null, updated_at integer not null, primary key(component,hash))",
        "create index if not exists idx_events_component_type_ts on events_v2(component,event_type,ts)",
        "create index if not exists idx_decisions_component_hash_ts on decision_log(component,hash,ts)",
        "create index if not exists idx_decisions_ts_id on decision_log(ts,id)",
        "create table if not exists metrics_snapshots(id integer primary key autoincrement, ts integer, component text, metrics_json text)",
        "create index if not exists idx_metrics_component_id on metrics_snapshots(component,id desc)",
        "create table if not exists media_groups(id integer primary key autoincrement, media_group_key text unique, normalized_id text, emby_media_dir text, created_at integer, updated_at integer)",
        "create table if not exists media_pipeline_runs(id integer primary key autoincrement, upload_manifest_id text, media_group_id integer, state text, created_at integer, updated_at integer, unique(upload_manifest_id, media_group_id))",
        "alter table media_pipeline_runs add column metadata_policy text",
        "alter table media_pipeline_runs add column metadata_quality text",
        "alter table media_pipeline_runs add column passthrough_reason text",
        "alter table media_pipeline_runs add column normalize_confidence real",
        "alter table media_pipeline_runs add column normalize_result_json text",
        "alter table media_pipeline_runs add column missing_outputs_json text",
        "create table if not exists sidecar_manifests(id integer primary key autoincrement, media_group_id integer, staging_dir text, artifacts_json text, state text, created_at integer, updated_at integer)",
        "alter table sidecar_manifests add column local_artifact_dir text",
        "alter table sidecar_manifests add column artifact_manifest_json text",
        "alter table sidecar_manifests add column artifact_total_bytes integer",
        "alter table sidecar_manifests add column scraper_exit_code integer",
        "alter table sidecar_manifests add column scraper_log_tail text",
        "alter table sidecar_manifests add column media_group_key_snapshot text",
        "create table if not exists emby_refresh_tasks(id integer primary key autoincrement, emby_media_dir text, state text default 'queued', earliest_run_at integer, max_run_at integer, payload_json text, created_at integer, updated_at integer)",
        "alter table emby_refresh_tasks add column last_error text",
        "alter table emby_refresh_tasks add column attempts integer not null default 0",
        "alter table emby_refresh_tasks add column max_attempts integer not null default 6",
        "alter table emby_refresh_tasks add column next_run_at integer",
        "create table if not exists bot_commands(id integer primary key autoincrement, command_id text unique, chat_id text, user_id text, command text, payload_json text, state text default 'queued', created_at integer, updated_at integer)",
        "create table if not exists bot_approvals(id integer primary key autoincrement, approval_id text unique, command_id text, action text, payload_json text, state text default 'pending', expires_at integer, approved_by text, approved_at integer, created_at integer)",
        "create table if not exists bot_notifications(id integer primary key autoincrement, dedupe_key text unique, chat_id text not null, level text default 'info', topic text, message text not null, payload_json text, state text default 'queued', attempts integer default 0, next_run_at integer, last_error text, created_at integer, updated_at integer, sent_at integer)",
        "create table if not exists orphan_candidates(path text primary key, first_seen_at integer, last_seen_at integer, confirmations integer default 0, state text default 'seen', quarantined_at integer, trash_path text)",
        "create table if not exists carousel_state(hash text primary key, state text not null, probe_started_at integer, last_probe_at integer, backoff_until integer, backoff_level integer default 0, updated_at integer)",
        "create table if not exists soak_state(hash text primary key, state text not null default 'candidate', ema_dlspeed_bps integer not null default 0, hot_since integer, resident_since integer, cooldown_until integer, last_started_at integer, last_stopped_at integer, exposure_bytes integer not null default 0, last_sample_at integer, updated_at integer not null, reason text)",
        "create index if not exists idx_soak_state_state on soak_state(state)",
        "create index if not exists idx_soak_state_cooldown on soak_state(cooldown_until)",
        "create table if not exists dynamic_junk_rules(id integer primary key autoincrement, pattern text not null, pattern_type text not null, confidence text default 'hard', source text, hits integer default 0, created_at integer, updated_at integer, enabled integer default 1)",
        "create index if not exists idx_dynamic_junk_rules_enabled on dynamic_junk_rules(enabled, confidence)",
        "create table if not exists junk_janitor_events(id integer primary key autoincrement, ts integer not null, hash text, file_index integer, path text not null, size integer, action text, reason text, rule_id integer, qbt_priority integer, mtime integer, data_json text)",
        "create index if not exists idx_junk_janitor_events_ts on junk_janitor_events(ts)",
        "create index if not exists idx_junk_janitor_events_hash on junk_janitor_events(hash)",
        "create table if not exists seeding_preemptions(id integer primary key autoincrement, ts integer not null, seeding_hash text not null, target_hash text, disk_state text, new_task_score real, preemptability_score real, score_margin real, released_bytes_estimate integer, reason text, guard_json text, decision_json text, upload_job_id integer, cleanup_done_at integer)",
        "create index if not exists idx_seeding_preemptions_ts on seeding_preemptions(ts)",
        "create index if not exists idx_seeding_preemptions_hash on seeding_preemptions(seeding_hash)",
        "insert or ignore into schema_migrations(version,name,applied_at) values(2,'schema_v2',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(3,'resource_ledger_v2',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(4,'capacity_state_v1',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(5,'scheduler_intents_v1',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(6,'batch_lease_claims_v1',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(7,'batch_inventory_v1',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(8,'upload_phases_v1',strftime('%s','now'))",
        "insert or ignore into schema_migrations(version,name,applied_at) values(9,'job_recovery_v1',strftime('%s','now'))",
    ]

def migrate(path: str | Path, dry_run: bool = False) -> list[str]:
    sql = migration_sql()
    if dry_run: return sql
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = _connect(path); con.execute("pragma journal_mode=WAL"); con.execute("pragma busy_timeout=5000")
    for stmt in sql:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise
    con.commit(); con.close(); return sql

def readonly_counts(path: str | Path) -> Dict[str, int]:
    con = readonly_connect(path); tables = [r[0] for r in con.execute("select name from sqlite_master where type='table'")]
    counts = {t: con.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}; con.close(); return counts

def recover_jobs(path: str | Path) -> List[Dict[str, Any]]:
    con = readonly_connect(path); rows = [dict(r) for r in con.execute("select * from torrent_jobs where state in ('queued','running','verify_pending','retry_wait') order by priority,id")]; con.close(); return rows

class DbActor:
    """Async single-writer DbActor used by coroutine-based workers/tests."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._started = False
        self._writes_enqueued = 0
        self._writes_completed = 0
        self._writes_failed = 0
        self._flushes_completed = 0

    async def start(self) -> None:
        if self._started:
            return
        migrate(self.path, False)
        self._started = True
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        con = _connect(self.path)
        con.execute("pragma journal_mode=WAL")
        con.execute("pragma busy_timeout=5000")
        try:
            while True:
                op, payload, fut = await self.queue.get()
                if op == "stop":
                    try:
                        con.commit()
                        fut.set_result(True)
                    finally:
                        self.queue.task_done()
                    break
                try:
                    if op == "enqueue_job":
                        now = int(time.time())
                        phase = "queued_copy" if payload["job_type"] in {"upload", "sidecar_upload"} else None
                        cur = con.execute(
                            "insert into torrent_jobs(hash,batch_id,job_type,state,phase,priority,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
                            (payload["hash"], payload["batch_id"], payload["job_type"], "queued", phase, payload["priority"], json.dumps(payload["payload"], ensure_ascii=False), now, now),
                        )
                        con.commit()
                        self._writes_completed += 1
                        fut.set_result(int(cur.lastrowid))
                    elif op == "execute":
                        cur = con.execute(payload["sql"], tuple(payload.get("params") or ()))
                        con.commit()
                        self._writes_completed += 1
                        fut.set_result(int(cur.lastrowid or 0))
                    elif op == "transaction":
                        out = payload(con)
                        con.commit()
                        self._writes_completed += 1
                        fut.set_result(out)
                    elif op == "flush":
                        con.commit()
                        self._flushes_completed += 1
                        fut.set_result(True)
                except Exception as exc:
                    con.rollback()
                    self._writes_failed += 1
                    fut.set_exception(exc)
                finally:
                    self.queue.task_done()
        finally:
            con.close()

    async def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> int:
        fut = asyncio.get_running_loop().create_future()
        self._writes_enqueued += 1
        await self.queue.put(("execute", {"sql": sql, "params": tuple(params)}, fut))
        return int(await fut)

    async def transaction(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        fut = asyncio.get_running_loop().create_future()
        self._writes_enqueued += 1
        await self.queue.put(("transaction", fn, fut))
        return await fut

    async def enqueue_job(self, hash: str | None, batch_id: int | None, job_type: str, payload: Dict[str, Any], priority: int = 100) -> int:
        fut = asyncio.get_running_loop().create_future()
        self._writes_enqueued += 1
        await self.queue.put(("enqueue_job", {"hash": hash, "batch_id": batch_id, "job_type": job_type, "payload": payload, "priority": priority}, fut))
        return int(await fut)

    async def flush(self) -> None:
        fut = asyncio.get_running_loop().create_future()
        await self.queue.put(("flush", {}, fut))
        await fut

    async def stop(self) -> None:
        if not self._started:
            return
        fut = asyncio.get_running_loop().create_future()
        await self.queue.put(("stop", {}, fut))
        await fut
        await self.queue.join()
        if self._task:
            await self._task
        self._started = False

    def metrics(self) -> Dict[str, Any]:
        return {
            "path": str(self.path),
            "queue_depth": int(self.queue.qsize()),
            "writes_enqueued": int(self._writes_enqueued),
            "writes_completed": int(self._writes_completed),
            "writes_failed": int(self._writes_failed),
            "flushes_completed": int(self._flushes_completed),
            "running": bool(self._task and not self._task.done()),
        }
