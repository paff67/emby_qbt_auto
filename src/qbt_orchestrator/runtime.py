from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from .db import write_transaction
from .observability import redact
from .upload import RcloneUploadWorker, UploadJob


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


class ObservabilityStore:
    def __init__(self, state_db: str | Path, now=None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def event(self, level: str, component: str, event_type: str, message: str, data: dict[str, Any] | None = None, hash: str | None = None, job_id: int | None = None, correlation_id: str | None = None) -> int:
        safe_message = redact(message)
        safe_data = redact(data or {})
        def txn(con: sqlite3.Connection) -> int:
            cur = con.execute(
                "insert into events_v2(ts,level,component,event_type,hash,job_id,correlation_id,message,data_json) values(?,?,?,?,?,?,?,?,?)",
                (int(self.now()), level, component, event_type, hash, job_id, correlation_id, safe_message, json.dumps(safe_data, ensure_ascii=False)),
            )
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def action(self, hash: str | None, job_id: int | None, action_type: str, path: str, payload: dict[str, Any], status: str, dry_run: bool = False, correlation_id: str | None = None, error: str | None = None) -> int:
        def txn(con: sqlite3.Connection) -> int:
            cur = con.execute(
                "insert into action_log(ts,correlation_id,hash,job_id,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?,?,?)",
                (int(self.now()), correlation_id, hash, job_id, action_type, path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
            )
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def trace(self, target: str) -> dict[str, list[dict[str, Any]]]:
        con = _connect(self.state_db)
        events = [dict(r) for r in con.execute("select * from events_v2 where hash=? or correlation_id=? order by id", (target, target))]
        actions = [dict(r) for r in con.execute("select * from action_log where hash=? or correlation_id=? or job_id=case when ? glob '[0-9]*' then cast(? as integer) else -1 end order by id", (target, target, target, target))]
        decisions = [dict(r) for r in con.execute("select * from decision_log where hash=? order by id", (target,))]
        con.close()
        return {"events": events, "actions": actions, "decisions": decisions}


class TorrentJobRepository:
    def __init__(self, state_db: str | Path, now=None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def enqueue(self, hash: str | None, batch_id: int | None, job_type: str, payload: dict[str, Any], priority: int = 100) -> int:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> int:
            cur = con.execute("insert into torrent_jobs(hash,batch_id,job_type,state,priority,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?,?)", (hash, batch_id, job_type, "queued", priority, json.dumps(payload, ensure_ascii=False), now, now))
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def claim_next(self, job_type: str) -> dict[str, Any] | None:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            row = con.execute(
                "select * from torrent_jobs where job_type=? and state in ('queued','verify_pending','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) order by priority,id limit 1",
                (job_type, now),
            ).fetchone()
            if not row:
                return None
            con.execute("update torrent_jobs set state='running', lease_owner='local', lease_until=?, attempts=attempts+1, updated_at=? where id=?", (now + 1800, now, row["id"]))
            return dict(con.execute("select * from torrent_jobs where id=?", (row["id"],)).fetchone())

        return write_transaction(self.state_db, txn)

    def claim_next_any(self, job_types: tuple[str, ...] | list[str]) -> dict[str, Any] | None:
        if not job_types:
            return None
        now = int(self.now())
        placeholders = ",".join("?" for _ in job_types)
        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            row = con.execute(
                f"select * from torrent_jobs where job_type in ({placeholders}) and state in ('queued','verify_pending','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) order by priority,id limit 1",
                (*tuple(job_types), now),
            ).fetchone()
            if not row:
                return None
            con.execute("update torrent_jobs set state='running', lease_owner='local', lease_until=?, attempts=attempts+1, updated_at=? where id=?", (now + 1800, now, row["id"]))
            return dict(con.execute("select * from torrent_jobs where id=?", (row["id"],)).fetchone())

        return write_transaction(self.state_db, txn)

    def peek_next(self, job_type: str) -> dict[str, Any] | None:
        con = _connect(self.state_db)
        row = con.execute("select * from torrent_jobs where job_type=? and state in ('queued','retry_wait','verify_pending') order by priority,id limit 1", (job_type,)).fetchone()
        out = dict(row) if row else None
        con.close()
        return out

    def update_state(self, job_id: int, state: str, stderr_tail: str | None = None, exit_code: int | None = None) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute("update torrent_jobs set state=?, last_stderr_tail=coalesce(?,last_stderr_tail), last_exit_code=coalesce(?,last_exit_code), updated_at=? where id=?", (state, stderr_tail, exit_code, int(self.now()), job_id)),
        )

    def schedule_retry(self, job_id: int, stderr_tail: str | None = None, exit_code: int | None = None, delay_sec: int = 60) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update torrent_jobs set state='retry_wait', lease_owner=null, lease_until=null, next_run_at=?, "
                "last_stderr_tail=coalesce(?,last_stderr_tail), last_exit_code=coalesce(?,last_exit_code), updated_at=? where id=?",
                (int(self.now()) + int(delay_sec), stderr_tail, exit_code, int(self.now()), job_id),
            ),
        )

    def get(self, job_id: int) -> dict[str, Any]:
        con = _connect(self.state_db); row = dict(con.execute("select * from torrent_jobs where id=?", (job_id,)).fetchone()); con.close(); return row


class UploadJobRunner:
    def __init__(self, repo: TorrentJobRepository, rclone, executor, backoff_schedule=(60, 180, 600, 1800, 7200, 21600), job_types=("upload", "sidecar_upload")):
        self.repo = repo; self.worker = RcloneUploadWorker(rclone, executor); self.backoff_schedule = tuple(int(x) for x in backoff_schedule); self.job_types = tuple(job_types)

    def run_next(self) -> int | None:
        row = self.repo.claim_next_any(self.job_types)
        if not row: return None
        payload = json.loads(row["payload_json"] or "{}")
        job = UploadJob(
            hash=row["hash"],
            batch_id=row["batch_id"],
            local=payload["local"],
            remote=payload["remote"],
            size=int(payload["size"]),
            full_torrent=bool(payload.get("full_torrent")),
            files=payload.get("files"),
            copy_mode=str(payload.get("copy_mode") or "copy"),
        )
        try:
            result = self.worker.run_once(job)
        except Exception as exc:
            delay = self._delay_for_attempt(int(row.get("attempts") or 1))
            self.repo.schedule_retry(row["id"], redact(str(exc))[:500], exit_code=1, delay_sec=delay)
            return int(row["id"])
        if result.state == "done": self.repo.update_state(row["id"], "done", exit_code=0)
        elif result.state == "verify_pending": self.repo.update_state(row["id"], "verify_pending", "remote size mismatch")
        elif result.state == "retry_wait": self.repo.schedule_retry(row["id"], "retry requested", exit_code=1, delay_sec=self._delay_for_attempt(int(row.get("attempts") or 1)))
        elif row["job_type"] == "sidecar_upload" and result.remote_verified: self.repo.update_state(row["id"], "done", exit_code=0)
        else: self.repo.update_state(row["id"], result.state)
        if row["job_type"] == "upload" and result.remote_verified:
            self._enqueue_media_pipeline_if_present(row, payload)
        return int(row["id"])

    def _enqueue_media_pipeline_if_present(self, row: dict[str, Any], payload: dict[str, Any]) -> None:
        media_files = payload.get("media_files")
        if not media_files:
            return
        manifest_id = str(payload.get("upload_manifest_id") or f"upload-job-{row['id']}")
        media_payload = {
            "upload_job_id": int(row["id"]),
            "upload_manifest_id": manifest_id,
            "files": media_files,
        }
        self.repo.enqueue(row.get("hash"), row.get("batch_id"), "media_pipeline", media_payload, priority=30)

    def _delay_for_attempt(self, attempt: int) -> int:
        if not self.backoff_schedule:
            return 60
        idx = max(0, min(int(attempt) - 1, len(self.backoff_schedule) - 1))
        return self.backoff_schedule[idx]


def reconcile_jobs(state_db: str | Path, now: int | None = None, dry_run: bool = True, retry_delay_sec: int = 60) -> dict[str, int]:
    now = int(now if now is not None else time.time())
    con = _connect(state_db)
    rows = [dict(r) for r in con.execute("select * from torrent_jobs where state='running' and lease_until is not null and lease_until<?", (now,))]
    con.close()
    if rows and not dry_run:
        def txn(wcon: sqlite3.Connection) -> None:
            for row in rows:
                wcon.execute(
                    "update torrent_jobs set state='retry_wait', lease_owner=null, lease_until=null, next_run_at=?, "
                    "last_stderr_tail=?, last_exit_code=coalesce(last_exit_code,1), updated_at=? where id=?",
                    (now + retry_delay_sec, "lease expired during reconcile", now, row["id"]),
                )

        write_transaction(state_db, txn)
    return {"expired_running": len(rows), "dry_run": 1 if dry_run else 0}


class BotCommandRepository:
    def __init__(self, state_db: str | Path, now=None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def insert_command(self, command_id, chat_id, user_id, command, payload):
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert or ignore into bot_commands(command_id,chat_id,user_id,command,payload_json,state,created_at,updated_at) values(?,?,?,?,?,?,?,?)",
                (str(command_id), str(chat_id), str(user_id), str(command), json.dumps(payload, ensure_ascii=False), "queued", now, now),
            ),
        )

    def claim_next(self) -> dict[str, Any] | None:
        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            row = con.execute(
                "select * from bot_commands where state in ('queued','approved') order by id limit 1"
            ).fetchone()
            if not row:
                return None
            con.execute("update bot_commands set state='running', updated_at=? where id=?", (int(self.now()), row["id"]))
            out = dict(con.execute("select * from bot_commands where id=?", (row["id"],)).fetchone())
            out["_claimed_from_state"] = row["state"]
            return out

        return write_transaction(self.state_db, txn)

    def set_state(self, command_id: str, state: str) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute("update bot_commands set state=?, updated_at=? where command_id=?", (state, int(self.now()), command_id)),
        )

    def create_approval(self, command_id: str, action: str, payload: dict[str, Any], ttl: int = 300) -> str:
        aid = f"approval-{command_id}"
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert or ignore into bot_approvals(approval_id,command_id,action,payload_json,state,expires_at,created_at) values(?,?,?,?,?,?,?)",
                (aid, command_id, action, json.dumps(payload, ensure_ascii=False), "pending", now + ttl, now),
            ),
        )
        return aid

    def approve_once(self, approval_id: str, user_id: int | str) -> bool:
        return self._decide_once(approval_id, user_id, "approved")

    def deny_once(self, approval_id: str, user_id: int | str) -> bool:
        return self._decide_once(approval_id, user_id, "denied")

    def _decide_once(self, approval_id: str, user_id: int | str, decision: str) -> bool:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> bool:
            row = con.execute(
                "select * from bot_approvals where approval_id=? and state='pending'",
                (str(approval_id),),
            ).fetchone()
            if not row:
                return False
            if row["expires_at"] is not None and int(row["expires_at"]) < now:
                con.execute(
                    "update bot_approvals set state='expired', approved_by=?, approved_at=? where approval_id=? and state='pending'",
                    (str(user_id), now, str(approval_id)),
                )
                con.execute(
                    "update bot_commands set state='expired', updated_at=? where command_id=? and state='approval_required'",
                    (now, row["command_id"]),
                )
                return False
            cur = con.execute(
                "update bot_approvals set state=?, approved_by=?, approved_at=? where approval_id=? and state='pending'",
                (decision, str(user_id), now, str(approval_id)),
            )
            if cur.rowcount != 1:
                return False
            if decision == "approved":
                con.execute(
                    "update bot_commands set state='approved', updated_at=? where command_id=? and state='approval_required'",
                    (now, row["command_id"]),
                )
            else:
                con.execute(
                    "update bot_commands set state='denied', updated_at=? where command_id=? and state='approval_required'",
                    (now, row["command_id"]),
                )
            return True

        return bool(write_transaction(self.state_db, txn))

    def get(self, command_id: str) -> dict[str, Any]:
        con = _connect(self.state_db)
        row = dict(con.execute("select * from bot_commands where command_id=?", (command_id,)).fetchone())
        con.close()
        return row

    def pending_approvals(self) -> list[dict[str, Any]]:
        con = _connect(self.state_db)
        rows = [dict(r) for r in con.execute("select * from bot_approvals where state in ('pending','approved','denied','expired') order by id")]
        con.close()
        return rows


class BotNotificationRepository:
    def __init__(self, state_db: str | Path, now=None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def enqueue(
        self,
        chat_id: int | str,
        topic: str,
        message: str,
        level: str = "info",
        payload: dict[str, Any] | None = None,
        dedupe_key: str | None = None,
    ) -> int:
        now = int(self.now())
        safe_message = str(redact(message))
        safe_payload = json.dumps(redact(payload or {}), ensure_ascii=False)
        def txn(con: sqlite3.Connection) -> int:
            if dedupe_key:
                con.execute(
                    "insert or ignore into bot_notifications(dedupe_key,chat_id,level,topic,message,payload_json,state,attempts,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
                    (str(dedupe_key), str(chat_id), level, topic, safe_message, safe_payload, "queued", 0, now, now),
                )
                row = con.execute("select id from bot_notifications where dedupe_key=?", (str(dedupe_key),)).fetchone()
                assert row is not None
                return int(row["id"])
            cur = con.execute(
                "insert into bot_notifications(chat_id,level,topic,message,payload_json,state,attempts,created_at,updated_at) values(?,?,?,?,?,?,?,?,?)",
                (str(chat_id), level, topic, safe_message, safe_payload, "queued", 0, now, now),
            )
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def peek_next(self) -> dict[str, Any] | None:
        con = _connect(self.state_db)
        now = int(self.now())
        row = con.execute(
            "select * from bot_notifications where state in ('queued','retry_wait') "
            "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) order by id limit 1",
            (now,),
        ).fetchone()
        out = dict(row) if row else None
        con.close()
        return out

    def claim_next(self) -> dict[str, Any] | None:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            row = con.execute(
                "select * from bot_notifications where state in ('queued','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) order by id limit 1",
                (now,),
            ).fetchone()
            if not row:
                return None
            con.execute(
                "update bot_notifications set state='running', attempts=attempts+1, updated_at=? where id=?",
                (now, row["id"]),
            )
            return dict(con.execute("select * from bot_notifications where id=?", (row["id"],)).fetchone())

        return write_transaction(self.state_db, txn)

    def mark_sent(self, notification_id: int) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update bot_notifications set state='sent', sent_at=?, updated_at=? where id=?",
                (now, now, int(notification_id)),
            ),
        )

    def schedule_retry(self, notification_id: int, error: str, delay_sec: int = 60) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update bot_notifications set state='retry_wait', next_run_at=?, last_error=?, updated_at=? where id=?",
                (now + int(delay_sec), str(redact(error))[:1000], now, int(notification_id)),
            ),
        )

    def get(self, notification_id: int) -> dict[str, Any]:
        con = _connect(self.state_db)
        row = con.execute("select * from bot_notifications where id=?", (int(notification_id),)).fetchone()
        con.close()
        if row is None:
            raise KeyError(notification_id)
        return dict(row)

    def list_all(self) -> list[dict[str, Any]]:
        con = _connect(self.state_db)
        rows = [dict(r) for r in con.execute("select * from bot_notifications order by id")]
        con.close()
        return rows


class CommandProcessor:
    DANGEROUS = {"cleanup", "force_upload", "preempt", "config"}

    def __init__(self, commands: BotCommandRepository, executor, notifications: BotNotificationRepository | None = None, preemption_service=None):
        self.commands = commands
        self.executor = executor
        self.notifications = notifications
        self.preemption_service = preemption_service

    def run_next(self) -> str | None:
        row = self.commands.claim_next()
        if not row: return None
        command_id = row["command_id"]; command = row["command"]; payload = json.loads(row["payload_json"] or "{}"); args = payload.get("args") or []
        claimed_from = row.get("_claimed_from_state") or row.get("state")
        if command in self.DANGEROUS and claimed_from != "approved":
            self.commands.create_approval(command_id, command, payload); self.commands.set_state(command_id, "approval_required"); return command_id
        if command == "pause" and args:
            self.executor.qbt_post("/api/v2/torrents/stop", {"hashes": args[0]}); self.commands.set_state(command_id, "done"); return command_id
        if command == "resume" and args:
            self.executor.qbt_post("/api/v2/torrents/start", {"hashes": args[0]}); self.commands.set_state(command_id, "done"); return command_id
        if command == "preempt" and args:
            if self.preemption_service is not None and hasattr(self.preemption_service, "force_preempt_hash"):
                target_hash = str(args[1]) if len(args) > 1 else None
                self.preemption_service.force_preempt_hash(str(args[0]), target_hash=target_hash, reason="telegram")
            else:
                self.executor.qbt_post("/api/v2/torrents/stop", {"hashes": args[0]})
            self.commands.set_state(command_id, "done"); return command_id
        if command in {"status", "trace", "perf"}:
            if self.notifications is not None:
                self.notifications.enqueue(
                    chat_id=row["chat_id"],
                    topic=command,
                    message=self._readonly_message(command, args),
                    dedupe_key=f"command-result:{command_id}",
                )
            self.commands.set_state(command_id, "done")
            return command_id
        self.commands.set_state(command_id, "ignored"); return command_id

    def _readonly_message(self, command: str, args: list[Any]) -> str:
        if command == "status":
            return self._status_message(str(args[0]) if args else "all")
        if command == "trace":
            target = str(args[0]) if args else ""
            trace = ObservabilityStore(self.commands.state_db).trace(target)
            event_types = [str(e.get("event_type")) for e in trace["events"][:5]]
            action_types = [str(a.get("action_type")) for a in trace["actions"][:5]]
            return f"trace {target}: events={len(trace['events'])} {','.join(event_types)} actions={len(trace['actions'])} {','.join(action_types)} decisions={len(trace['decisions'])}"
        if command == "perf":
            con = _connect(self.commands.state_db)
            events = con.execute("select count(*) from events_v2").fetchone()[0]
            actions = con.execute("select count(*) from action_log").fetchone()[0]
            jobs = con.execute("select count(*) from torrent_jobs where state in ('queued','running','retry_wait','verify_pending')").fetchone()[0]
            con.close()
            return f"perf: events={int(events)} actions={int(actions)} active_jobs={int(jobs)}"
        return f"{command}: unsupported"

    def _status_message(self, view: str) -> str:
        con = _connect(self.commands.state_db)
        try:
            disk = con.execute("select pressure_state,free_bytes,resume_allowed from disk_state where id=1").fetchone()
            job_rows = con.execute("select state,count(*) as c from torrent_jobs group by state").fetchall()
        finally:
            con.close()
        jobs = ",".join(f"{r['state']}={int(r['c'])}" for r in job_rows) or "none"
        if disk:
            gib = int(disk["free_bytes"] or 0) / 1024**3
            return f"status {view}: disk={disk['pressure_state']} free={gib:.2f}GiB resume_allowed={int(disk['resume_allowed'] or 0)} jobs={jobs}"
        return f"status {view}: disk=unknown jobs={jobs}"
