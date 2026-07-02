from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from .observability import redact
from .upload import RcloneUploadWorker, UploadJob


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


class ObservabilityStore:
    def __init__(self, state_db: str | Path):
        self.state_db = Path(state_db)

    def event(self, level: str, component: str, event_type: str, message: str, data: dict[str, Any] | None = None, hash: str | None = None, job_id: int | None = None, correlation_id: str | None = None) -> int:
        safe_message = redact(message)
        safe_data = redact(data or {})
        con = _connect(self.state_db)
        cur = con.execute(
            "insert into events_v2(ts,level,component,event_type,hash,job_id,correlation_id,message,data_json) values(?,?,?,?,?,?,?,?,?)",
            (int(time.time()), level, component, event_type, hash, job_id, correlation_id, safe_message, json.dumps(safe_data, ensure_ascii=False)),
        )
        con.commit(); con.close(); return int(cur.lastrowid)

    def action(self, hash: str | None, job_id: int | None, action_type: str, path: str, payload: dict[str, Any], status: str, dry_run: bool, correlation_id: str | None = None, error: str | None = None) -> int:
        con = _connect(self.state_db)
        cur = con.execute(
            "insert into action_log(ts,correlation_id,hash,job_id,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?,?,?)",
            (int(time.time()), correlation_id, hash, job_id, action_type, path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
        )
        con.commit(); con.close(); return int(cur.lastrowid)

    def trace(self, target: str) -> dict[str, list[dict[str, Any]]]:
        con = _connect(self.state_db)
        events = [dict(r) for r in con.execute("select * from events_v2 where hash=? or correlation_id=? order by id", (target, target))]
        actions = [dict(r) for r in con.execute("select * from action_log where hash=? or correlation_id=? or job_id=case when ? glob '[0-9]*' then cast(? as integer) else -1 end order by id", (target, target, target, target))]
        decisions = [dict(r) for r in con.execute("select * from decision_log where hash=? order by id", (target,))]
        con.close()
        return {"events": events, "actions": actions, "decisions": decisions}


class TorrentJobRepository:
    def __init__(self, state_db: str | Path): self.state_db = Path(state_db)

    def enqueue(self, hash: str | None, batch_id: int | None, job_type: str, payload: dict[str, Any], priority: int = 100) -> int:
        now = int(time.time()); con = _connect(self.state_db)
        cur = con.execute("insert into torrent_jobs(hash,batch_id,job_type,state,priority,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?,?)", (hash, batch_id, job_type, "queued", priority, json.dumps(payload, ensure_ascii=False), now, now))
        con.commit(); con.close(); return int(cur.lastrowid)

    def claim_next(self, job_type: str) -> dict[str, Any] | None:
        con = _connect(self.state_db)
        row = con.execute("select * from torrent_jobs where job_type=? and state in ('queued','retry_wait','verify_pending') order by priority,id limit 1", (job_type,)).fetchone()
        if not row:
            con.close(); return None
        con.execute("update torrent_jobs set state='running', lease_owner='local', lease_until=?, attempts=attempts+1, updated_at=? where id=?", (int(time.time()) + 1800, int(time.time()), row["id"]))
        con.commit(); out = dict(con.execute("select * from torrent_jobs where id=?", (row["id"],)).fetchone()); con.close(); return out

    def update_state(self, job_id: int, state: str, stderr_tail: str | None = None, exit_code: int | None = None) -> None:
        con = _connect(self.state_db)
        con.execute("update torrent_jobs set state=?, last_stderr_tail=coalesce(?,last_stderr_tail), last_exit_code=coalesce(?,last_exit_code), updated_at=? where id=?", (state, stderr_tail, exit_code, int(time.time()), job_id))
        con.commit(); con.close()

    def get(self, job_id: int) -> dict[str, Any]:
        con = _connect(self.state_db); row = dict(con.execute("select * from torrent_jobs where id=?", (job_id,)).fetchone()); con.close(); return row


class UploadJobRunner:
    def __init__(self, repo: TorrentJobRepository, rclone, executor):
        self.repo = repo; self.worker = RcloneUploadWorker(rclone, executor)

    def run_next(self) -> int | None:
        row = self.repo.claim_next("upload")
        if not row: return None
        payload = json.loads(row["payload_json"] or "{}")
        job = UploadJob(hash=row["hash"], batch_id=row["batch_id"], local=payload["local"], remote=payload["remote"], size=int(payload["size"]), full_torrent=bool(payload.get("full_torrent")))
        result = self.worker.run_once(job)
        if result.state == "done": self.repo.update_state(row["id"], "done", exit_code=0)
        elif result.state == "verify_pending": self.repo.update_state(row["id"], "verify_pending", "remote size mismatch")
        else: self.repo.update_state(row["id"], result.state)
        return int(row["id"])


class BotCommandRepository:
    def __init__(self, state_db: str | Path): self.state_db = Path(state_db)
    def insert_command(self, command_id, chat_id, user_id, command, payload):
        now = int(time.time()); con = _connect(self.state_db)
        con.execute("insert or ignore into bot_commands(command_id,chat_id,user_id,command,payload_json,state,created_at,updated_at) values(?,?,?,?,?,?,?,?)", (str(command_id), str(chat_id), str(user_id), str(command), json.dumps(payload, ensure_ascii=False), "queued", now, now))
        con.commit(); con.close()
    def claim_next(self) -> dict[str, Any] | None:
        con = _connect(self.state_db); row = con.execute("select * from bot_commands where state='queued' order by id limit 1").fetchone()
        if not row: con.close(); return None
        con.execute("update bot_commands set state='running', updated_at=? where id=?", (int(time.time()), row["id"])); con.commit(); out = dict(con.execute("select * from bot_commands where id=?", (row["id"],)).fetchone()); con.close(); return out
    def set_state(self, command_id: str, state: str) -> None:
        con = _connect(self.state_db); con.execute("update bot_commands set state=?, updated_at=? where command_id=?", (state, int(time.time()), command_id)); con.commit(); con.close()
    def create_approval(self, command_id: str, action: str, payload: dict[str, Any], ttl: int = 300) -> str:
        aid = f"approval-{command_id}"; now = int(time.time()); con = _connect(self.state_db)
        con.execute("insert or ignore into bot_approvals(approval_id,command_id,action,payload_json,state,expires_at,created_at) values(?,?,?,?,?,?,?)", (aid, command_id, action, json.dumps(payload, ensure_ascii=False), "pending", now + ttl, now)); con.commit(); con.close(); return aid
    def get(self, command_id: str) -> dict[str, Any]:
        con = _connect(self.state_db); row = dict(con.execute("select * from bot_commands where command_id=?", (command_id,)).fetchone()); con.close(); return row
    def pending_approvals(self) -> list[dict[str, Any]]:
        con = _connect(self.state_db); rows = [dict(r) for r in con.execute("select * from bot_approvals where state='pending' order by id")]; con.close(); return rows


class CommandProcessor:
    DANGEROUS = {"cleanup", "force_upload", "preempt", "config"}
    def __init__(self, commands: BotCommandRepository, executor): self.commands = commands; self.executor = executor
    def run_next(self) -> str | None:
        row = self.commands.claim_next()
        if not row: return None
        command_id = row["command_id"]; command = row["command"]; payload = json.loads(row["payload_json"] or "{}"); args = payload.get("args") or []
        if command in self.DANGEROUS:
            self.commands.create_approval(command_id, command, payload); self.commands.set_state(command_id, "approval_required"); return command_id
        if command == "pause" and args:
            self.executor.qbt_post("/api/v2/torrents/stop", {"hashes": args[0]}); self.commands.set_state(command_id, "done"); return command_id
        if command == "resume" and args:
            self.executor.qbt_post("/api/v2/torrents/start", {"hashes": args[0]}); self.commands.set_state(command_id, "done"); return command_id
        self.commands.set_state(command_id, "ignored"); return command_id
