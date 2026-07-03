from __future__ import annotations
import asyncio, atexit, json, queue, sqlite3, threading, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List

def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path); con.row_factory = sqlite3.Row; return con

def readonly_connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con

@dataclass(frozen=True)
class WriteResult:
    lastrowid: int
    rowcount: int

class _SyncWriteActor:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._queue: "queue.Queue[tuple[str, Any, queue.Queue]]" = queue.Queue()
        self._thread: threading.Thread | None = None
        self._thread_id: int | None = None
        self._lock = threading.Lock()
        self._stopped = False

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._stopped = False
            self._thread = threading.Thread(target=self._run, name=f"qbt-db-writer:{self.path}", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._stopped = True

    def transaction(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        # Synchronous daemon code cannot await an asyncio actor.  This helper
        # gives those paths the same single-writer guarantee with a per-DB
        # actor lock, while opening and closing the SQLite handle per
        # transaction so test/prod maintenance can move or back up DB files.
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            con = _connect(self.path)
            con.execute("pragma journal_mode=WAL")
            con.execute("pragma busy_timeout=5000")
            try:
                out = fn(con)
                con.commit()
                return out
            except Exception:
                con.rollback()
                raise
            finally:
                con.close()

    def _run(self) -> None:
        self._thread_id = threading.get_ident()
        con = _connect(self.path)
        con.execute("pragma journal_mode=WAL")
        con.execute("pragma busy_timeout=5000")
        try:
            while True:
                op, payload, reply = self._queue.get()
                if op == "stop":
                    con.commit()
                    reply.put((True, None))
                    self._queue.task_done()
                    return
                try:
                    result = payload(con)
                    con.commit()
                    reply.put((True, result))
                except Exception as exc:
                    con.rollback()
                    reply.put((False, exc))
                finally:
                    self._queue.task_done()
        finally:
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
        "create table if not exists torrent_health(hash text primary key, sampled_at integer, dlspeed_bps integer default 0, upspeed_bps integer default 0, ema_fast_bps real default 0, ema_slow_bps real default 0, completed_bytes integer default 0, last_completed_bytes integer default 0, progress real default 0, num_seeds integer default 0, num_peers integer default 0, last_swarm_seen_at integer, low_speed_since integer, no_progress_since integer, active_since integer, soak_since integer, dead_since integer, promote_candidate_since integer, updated_at integer)",
        "create table if not exists torrent_batches(id integer primary key autoincrement, hash text not null, batch_no integer not null, state text not null, mode text, indices_json text not null default '[]', total_bytes integer default 0, downloaded_bytes integer default 0, reserved_bytes integer default 0, piece_size integer default 0, selected_extents integer default 0, piece_spill_overhead_bytes integer default 0, payload_efficiency real default 1.0, priority_applied integer default 0, upload_job_id integer, created_at integer, updated_at integer, local_pinned_bytes integer default 0, cleanup_deferred_at integer, cleanup_done_at integer, unique(hash,batch_no))",
        "create table if not exists resource_reservations(id integer primary key autoincrement, hash text, batch_id integer, kind text, bytes integer not null, state text default 'active', created_at integer, expires_at integer, released_at integer, reason text)",
        "create table if not exists scheduler_allocations(hash text primary key, desired_state text, applied_state text, slot_kind text, priority_score real default 0, reserved_bytes integer default 0, download_limit_bps integer, upload_limit_bps integer, force_start integer default 0, desired_seq_dl integer, applied_seq_dl integer, allocated_at integer, applied_at integer, expires_at integer, reason text)",
        "create table if not exists disk_state(id integer primary key check(id=1), sampled_at integer, free_bytes integer, pressure_state text, previous_state text, state_since integer, resume_allowed integer default 1)",
        "create table if not exists torrent_jobs(id integer primary key autoincrement, hash text, batch_id integer, job_type text, state text default 'queued', priority integer default 100, payload_json text, payload_schema_version integer default 1, lease_owner text, lease_until integer, attempts integer default 0, max_attempts integer default 6, next_run_at integer, last_exit_code integer, last_stderr_tail text, cancel_requested integer default 0, cancel_requested_at integer, created_at integer, updated_at integer)",
        "create table if not exists action_log(id integer primary key autoincrement, ts integer, correlation_id text, hash text, job_id integer, action_type text, path text, payload_json text, status text, dry_run integer default 0, error text)",
        "create table if not exists events_v2(id integer primary key autoincrement, ts integer, level text, component text, event_type text, hash text, job_id integer, correlation_id text, message text, data_json text)",
        "create table if not exists decision_log(id integer primary key autoincrement, ts integer, component text, hash text, decision text, reason_code text, data_json text)",
        "create table if not exists metrics_snapshots(id integer primary key autoincrement, ts integer, component text, metrics_json text)",
        "create table if not exists media_groups(id integer primary key autoincrement, media_group_key text unique, normalized_id text, emby_media_dir text, created_at integer, updated_at integer)",
        "create table if not exists media_pipeline_runs(id integer primary key autoincrement, upload_manifest_id text, media_group_id integer, state text, created_at integer, updated_at integer, unique(upload_manifest_id, media_group_id))",
        "create table if not exists sidecar_manifests(id integer primary key autoincrement, media_group_id integer, staging_dir text, artifacts_json text, state text, created_at integer, updated_at integer)",
        "create table if not exists emby_refresh_tasks(id integer primary key autoincrement, emby_media_dir text, state text default 'queued', earliest_run_at integer, max_run_at integer, payload_json text, created_at integer, updated_at integer)",
        "alter table emby_refresh_tasks add column last_error text",
        "create table if not exists bot_commands(id integer primary key autoincrement, command_id text unique, chat_id text, user_id text, command text, payload_json text, state text default 'queued', created_at integer, updated_at integer)",
        "create table if not exists bot_approvals(id integer primary key autoincrement, approval_id text unique, command_id text, action text, payload_json text, state text default 'pending', expires_at integer, approved_by text, approved_at integer, created_at integer)",
        "create table if not exists bot_notifications(id integer primary key autoincrement, dedupe_key text unique, chat_id text not null, level text default 'info', topic text, message text not null, payload_json text, state text default 'queued', attempts integer default 0, next_run_at integer, last_error text, created_at integer, updated_at integer, sent_at integer)",
        "create table if not exists orphan_candidates(path text primary key, first_seen_at integer, last_seen_at integer, confirmations integer default 0, state text default 'seen', quarantined_at integer, trash_path text)",
        "create table if not exists carousel_state(hash text primary key, state text not null, probe_started_at integer, last_probe_at integer, backoff_until integer, backoff_level integer default 0, updated_at integer)",
        "create table if not exists dynamic_junk_rules(id integer primary key autoincrement, pattern text not null, pattern_type text not null, confidence text default 'hard', source text, hits integer default 0, created_at integer, updated_at integer, enabled integer default 1)",
        "create index if not exists idx_dynamic_junk_rules_enabled on dynamic_junk_rules(enabled, confidence)",
        "create table if not exists junk_janitor_events(id integer primary key autoincrement, ts integer not null, hash text, file_index integer, path text not null, size integer, action text, reason text, rule_id integer, qbt_priority integer, mtime integer, data_json text)",
        "create index if not exists idx_junk_janitor_events_ts on junk_janitor_events(ts)",
        "create index if not exists idx_junk_janitor_events_hash on junk_janitor_events(hash)",
        "create table if not exists seeding_preemptions(id integer primary key autoincrement, ts integer not null, seeding_hash text not null, target_hash text, disk_state text, new_task_score real, preemptability_score real, score_margin real, released_bytes_estimate integer, reason text, guard_json text, decision_json text, upload_job_id integer, cleanup_done_at integer)",
        "create index if not exists idx_seeding_preemptions_ts on seeding_preemptions(ts)",
        "create index if not exists idx_seeding_preemptions_hash on seeding_preemptions(seeding_hash)",
        "insert or ignore into schema_migrations(version,name,applied_at) values(2,'schema_v2',strftime('%s','now'))",
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
    con = _connect(path); tables = [r[0] for r in con.execute("select name from sqlite_master where type='table'")]
    counts = {t: con.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}; con.close(); return counts

def recover_jobs(path: str | Path) -> List[Dict[str, Any]]:
    con = _connect(path); rows = [dict(r) for r in con.execute("select * from torrent_jobs where state in ('queued','running','verify_pending','retry_wait') order by priority,id")]; con.close(); return rows

class DbActor:
    def __init__(self, path: str | Path): self.path = Path(path); self.queue: asyncio.Queue = asyncio.Queue(); self._task = None; self._stopping = False
    async def start(self) -> None: migrate(self.path, False); self._task = asyncio.create_task(self._run())
    async def _run(self) -> None:
        con = _connect(self.path)
        while not self._stopping or not self.queue.empty():
            item = await self.queue.get(); op, payload, fut = item
            if op == "stop": self.queue.task_done(); continue
            try:
                if op == "enqueue_job":
                    now = int(time.time()); cur = con.execute("insert into torrent_jobs(hash,batch_id,job_type,state,priority,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?,?)", (payload["hash"], payload["batch_id"], payload["job_type"], "queued", payload["priority"], json.dumps(payload["payload"]), now, now)); con.commit(); fut.set_result(cur.lastrowid)
                elif op == "flush": con.commit(); fut.set_result(True)
            except Exception as e: fut.set_exception(e)
            finally: self.queue.task_done()
        con.close()
    async def enqueue_job(self, hash: str | None, batch_id: int | None, job_type: str, payload: Dict[str, Any], priority: int = 100) -> int:
        fut = asyncio.get_running_loop().create_future(); await self.queue.put(("enqueue_job", {"hash": hash, "batch_id": batch_id, "job_type": job_type, "payload": payload, "priority": priority}, fut)); return await fut
    async def flush(self) -> None:
        fut = asyncio.get_running_loop().create_future(); await self.queue.put(("flush", {}, fut)); await fut
    async def stop(self) -> None:
        self._stopping = True; fut = asyncio.get_running_loop().create_future(); fut.set_result(True); await self.queue.put(("stop", {}, fut)); await self.queue.join();
        if self._task: await self._task
