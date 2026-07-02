from __future__ import annotations
import asyncio, json, sqlite3, time
from pathlib import Path
from typing import Any, Dict, List

def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path); con.row_factory = sqlite3.Row; return con

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
