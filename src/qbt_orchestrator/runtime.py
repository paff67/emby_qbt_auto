from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

from .db import readonly_connect, write_transaction
from .cleanup_policy import cleanup_eligibility
from .io_governor import JobPriority
from .observability import redact
from .promotion import finalize_canonical_upload
from .upload import RcloneUploadWorker, UploadJob, UploadResult


def _connect(path: str | Path) -> sqlite3.Connection:
    return readonly_connect(path)


def _nearest_rank(values: list[int], percentile: int) -> int:
    """Return a deterministic nearest-rank percentile for a bounded sample."""
    ordered = sorted(int(value) for value in values)
    rank = max(1, (len(ordered) * int(percentile) + 99) // 100)
    return ordered[min(len(ordered), rank) - 1]


class LeaseLostError(RuntimeError):
    """Raised when a worker no longer owns the fenced job lease."""


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

    def rolling_timing_metric(
        self,
        component: str,
        *,
        duration_ms: int,
        max_runtime_ms: int,
        succeeded: bool,
        recent_limit: int = 60,
    ) -> int:
        """Update one bounded timing row for a repeatedly executed component.

        Keeping a short recent-duration window makes P50/P95 available without
        appending a metrics row on every loop execution.  Cumulative counters
        remain useful across the whole process lifetime.
        """
        duration_ms = max(0, int(duration_ms))
        max_runtime_ms = max(0, int(max_runtime_ms))
        recent_limit = max(1, int(recent_limit))
        deadline_missed = duration_ms > max_runtime_ms
        now = int(self.now())

        def txn(con: sqlite3.Connection) -> int:
            row = con.execute(
                "select id,metrics_json from metrics_snapshots where component=? order by id desc limit 1",
                (component,),
            ).fetchone()
            previous: dict[str, Any] = {}
            if row and row["metrics_json"]:
                try:
                    loaded = json.loads(row["metrics_json"])
                    if isinstance(loaded, dict):
                        previous = loaded
                except (TypeError, ValueError, json.JSONDecodeError):
                    previous = {}

            recent = [int(value) for value in previous.get("recent_duration_ms", []) if isinstance(value, (int, float))]
            recent.append(duration_ms)
            recent = recent[-recent_limit:]
            sample_count = int(previous.get("sample_count") or 0) + 1
            total_duration_ms = int(previous.get("total_duration_ms") or 0) + duration_ms
            metrics = {
                "duration_ms": duration_ms,
                "max_runtime_ms": max_runtime_ms,
                "deadline_missed": deadline_missed,
                "succeeded": bool(succeeded),
                "sample_count": sample_count,
                "failure_count": int(previous.get("failure_count") or 0) + (0 if succeeded else 1),
                "deadline_miss_count": int(previous.get("deadline_miss_count") or 0) + (1 if deadline_missed else 0),
                "total_duration_ms": total_duration_ms,
                "average_duration_ms": total_duration_ms // sample_count,
                "max_duration_ms": max(int(previous.get("max_duration_ms") or 0), duration_ms),
                "p50_duration_ms": _nearest_rank(recent, 50),
                "p95_duration_ms": _nearest_rank(recent, 95),
                "recent_duration_ms": recent,
            }
            payload = json.dumps(redact(metrics), ensure_ascii=False)
            if row:
                con.execute("update metrics_snapshots set ts=?,metrics_json=? where id=?", (now, payload, int(row["id"])))
                return int(row["id"])
            cur = con.execute(
                "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                (now, component, payload),
            )
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def metric_snapshot(self, component: str, metrics: dict[str, Any]) -> int:
        """Append one redacted aggregate sample for a completed periodic loop."""

        payload = json.dumps(redact(metrics), sort_keys=True, separators=(",", ":"), ensure_ascii=False)

        def txn(con: sqlite3.Connection) -> int:
            cur = con.execute(
                "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                (int(self.now()), component, payload),
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
    def __init__(self, state_db: str | Path, now=None, lease_duration_sec: int = 1800):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))
        self.lease_duration_sec = max(1, int(lease_duration_sec))

    @staticmethod
    def _new_lease_owner() -> str:
        return f"local:{uuid.uuid4().hex}"

    @staticmethod
    def _fence_clause(
        lease_owner: str | None, lease_generation: int | None, now: int
    ) -> tuple[str, tuple[Any, ...]]:
        if lease_owner is None and lease_generation is None:
            return "", ()
        if lease_owner is None or lease_generation is None:
            raise ValueError("lease_owner and lease_generation must be supplied together")
        return (
            " and state='running' and lease_owner=? and lease_generation=? "
            "and lease_until is not null and lease_until>=?",
            (str(lease_owner), int(lease_generation), int(now)),
        )

    def _update_job(
        self,
        job_id: int,
        set_sql: str,
        values: tuple[Any, ...],
        *,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
    ) -> bool:
        now = int(self.now())
        fence_sql, fence_values = self._fence_clause(
            lease_owner, lease_generation, now
        )

        def txn(con: sqlite3.Connection) -> bool:
            cur = con.execute(
                f"update torrent_jobs set {set_sql} where id=?{fence_sql}",
                (*values, int(job_id), *fence_values),
            )
            return int(cur.rowcount or 0) == 1

        return bool(write_transaction(self.state_db, txn))

    def enqueue(
        self,
        hash: str | None,
        batch_id: int | None,
        job_type: str,
        payload: dict[str, Any],
        priority: int = 100,
        parent_job_id: int | None = None,
    ) -> int:
        now = int(self.now())
        phase = "queued_copy" if job_type in {"upload", "sidecar_upload"} else None
        def txn(con: sqlite3.Connection) -> int:
            cur = con.execute(
                "insert into torrent_jobs(hash,batch_id,job_type,state,phase,priority,payload_json,parent_job_id,created_at,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?)",
                (
                    hash,
                    batch_id,
                    job_type,
                    "queued",
                    phase,
                    priority,
                    json.dumps(payload, ensure_ascii=False),
                    parent_job_id,
                    now,
                    now,
                ),
            )
            return int(cur.lastrowid)

        return int(write_transaction(self.state_db, txn))

    def claim_next(self, job_type: str) -> dict[str, Any] | None:
        now = int(self.now())
        lease_owner = self._new_lease_owner()
        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            row = con.execute(
                "select * from torrent_jobs where job_type=? and attempts<max_attempts "
                "and state in ('queued','verify_pending','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) order by priority,id limit 1",
                (job_type, now),
            ).fetchone()
            if not row:
                return None
            con.execute(
                "update torrent_jobs set state='running',lease_owner=?,lease_until=?,"
                "lease_generation=lease_generation+1,attempts=attempts+1,updated_at=? where id=?",
                (lease_owner, now + self.lease_duration_sec, now, row["id"]),
            )
            return dict(con.execute("select * from torrent_jobs where id=?", (row["id"],)).fetchone())

        return write_transaction(self.state_db, txn)

    def claim_next_cleanup(self, *, prefer_largest: bool) -> dict[str, Any] | None:
        now = int(self.now())
        lease_owner = self._new_lease_owner()

        def reclaimable_bytes(row: sqlite3.Row) -> int:
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                return 0
            manifest = payload.get("final_manifest") or []
            if isinstance(manifest, list):
                return sum(
                    max(0, int(item.get("size") or 0))
                    for item in manifest
                    if isinstance(item, dict)
                )
            snapshot = payload.get("cleanup_policy_snapshot") or {}
            return max(0, int(snapshot.get("size") or 0))

        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            candidates = con.execute(
                "select * from torrent_jobs where job_type='cleanup_full_torrent' and attempts<max_attempts "
                "and state in ('queued','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?)",
                (now,),
            ).fetchall()
            if not candidates:
                return None
            row = min(
                candidates,
                key=lambda item: (
                    int(item["priority"]),
                    -reclaimable_bytes(item) if prefer_largest else int(item["id"]),
                    int(item["id"]),
                ),
            )
            con.execute(
                "update torrent_jobs set state='running',lease_owner=?,lease_until=?,"
                "lease_generation=lease_generation+1,attempts=attempts+1,updated_at=? where id=?",
                (lease_owner, now + self.lease_duration_sec, now, int(row["id"])),
            )
            return dict(
                con.execute(
                    "select * from torrent_jobs where id=?", (int(row["id"]),)
                ).fetchone()
            )

        return write_transaction(self.state_db, txn)

    def claim_next_any(self, job_types: tuple[str, ...] | list[str]) -> dict[str, Any] | None:
        if not job_types:
            return None
        now = int(self.now())
        lease_owner = self._new_lease_owner()
        placeholders = ",".join("?" for _ in job_types)
        def txn(con: sqlite3.Connection) -> dict[str, Any] | None:
            row = con.execute(
                f"select * from torrent_jobs where job_type in ({placeholders}) and attempts<max_attempts "
                "and state in ('queued','verify_pending','retry_wait') "
                "and (state!='retry_wait' or next_run_at is null or next_run_at<=?) order by priority,id limit 1",
                (*tuple(job_types), now),
            ).fetchone()
            if not row:
                return None
            con.execute(
                "update torrent_jobs set state='running',lease_owner=?,lease_until=?,"
                "lease_generation=lease_generation+1,attempts=attempts+1,updated_at=? where id=?",
                (lease_owner, now + self.lease_duration_sec, now, row["id"]),
            )
            return dict(con.execute("select * from torrent_jobs where id=?", (row["id"],)).fetchone())

        return write_transaction(self.state_db, txn)

    def peek_next(self, job_type: str) -> dict[str, Any] | None:
        con = _connect(self.state_db)
        row = con.execute("select * from torrent_jobs where job_type=? and state in ('queued','retry_wait','verify_pending') order by priority,id limit 1", (job_type,)).fetchone()
        out = dict(row) if row else None
        con.close()
        return out

    def renew_lease(
        self,
        job_id: int,
        lease_owner: str,
        lease_generation: int,
        *,
        lease_duration_sec: int | None = None,
    ) -> bool:
        now = int(self.now())
        duration = max(
            1,
            int(
                self.lease_duration_sec
                if lease_duration_sec is None
                else lease_duration_sec
            ),
        )
        return self._update_job(
            job_id,
            "lease_until=?,updated_at=?",
            (now + duration, now),
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )

    def update_state(
        self,
        job_id: int,
        state: str,
        stderr_tail: str | None = None,
        exit_code: int | None = None,
        *,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
    ) -> bool:
        successful = str(state) in {
            "done",
            "cleanup_deferred",
            "promotion_wait",
            "cleanup_wait",
        }
        terminal = str(state) in {
            "done",
            "failed",
            "cancelled",
            "cleanup_deferred",
            "promotion_wait",
            "cleanup_wait",
        }
        lease_reset = (
            ",lease_owner=null,lease_until=null,next_run_at=null" if terminal else ""
        )
        stderr_update = (
            "last_stderr_tail=null," if successful else "last_stderr_tail=coalesce(?,last_stderr_tail),"
        )
        values: tuple[Any, ...] = (
            (state, exit_code, int(self.now()))
            if successful
            else (state, stderr_tail, exit_code, int(self.now()))
        )
        return self._update_job(
            job_id,
            f"state=?,{stderr_update}"
            f"last_exit_code=coalesce(?,last_exit_code),updated_at=?{lease_reset}",
            values,
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )

    def schedule_retry(
        self,
        job_id: int,
        stderr_tail: str | None = None,
        exit_code: int | None = None,
        delay_sec: int = 60,
        *,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
    ) -> bool:
        now = int(self.now())
        return self._update_job(
            job_id,
            "state='retry_wait',lease_owner=null,lease_until=null,next_run_at=?,"
            "last_stderr_tail=coalesce(?,last_stderr_tail),"
            "last_exit_code=coalesce(?,last_exit_code),updated_at=?",
            (now + int(delay_sec), stderr_tail, exit_code, now),
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )

    def set_phase(
        self,
        job_id: int,
        phase: str,
        *,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
    ) -> bool:
        return self._update_job(
            job_id,
            "phase=?,updated_at=?",
            (str(phase), int(self.now())),
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )

    def mark_copy_completed(
        self,
        job_id: int,
        *,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
    ) -> bool:
        now = int(self.now())
        return self._update_job(
            job_id,
            "phase='copied',copy_completed_at=coalesce(copy_completed_at,?),updated_at=?",
            (now, now),
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )

    def record_verification_failure(
        self,
        job_id: int,
        result,
        *,
        lease_owner: str | None = None,
        lease_generation: int | None = None,
    ) -> bool:
        now = int(self.now())
        details = json.dumps(
            {"verified": False, "mismatches": list(result.mismatches)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        message = (
            "remote size mismatch"
            if list(result.mismatches) == ["size:remote"]
            else (",".join(result.mismatches)[:500] or "remote verification failed")
        )
        return self._update_job(
            job_id,
            "state='verify_pending',phase='verifying',lease_owner=null,lease_until=null,"
            "verification_method=?,verification_result_json=?,last_stderr_tail=?,updated_at=?",
            (result.method, details, message, now),
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )

    def finalize_verified(self, row: dict[str, Any], payload: dict[str, Any], result) -> str:
        """Persist verification and idempotently enqueue destructive cleanup work."""
        now = int(self.now())
        job_id = int(row["id"])
        is_sidecar = str(row.get("job_type") or "") == "sidecar_upload"
        full_torrent = bool(payload.get("full_torrent")) and not is_sidecar
        state = "promotion_wait" if full_torrent else ("done" if is_sidecar else "cleanup_deferred")
        phase = "promotion_wait" if full_torrent else "done"
        details = json.dumps(
            {"verified": True, "mismatches": []},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

        lease_owner = row.get("lease_owner")
        lease_generation = (
            int(row.get("lease_generation") or 0)
            if lease_owner is not None
            else None
        )
        updated = self._update_job(
            job_id,
            "state=?,phase=?,verification_method=?,verification_result_json=?,verified_at=?,"
            "lease_owner=null,lease_until=null,last_exit_code=0,last_stderr_tail=null,"
            "next_run_at=null,updated_at=?",
            (state, phase, result.method, details, now, now),
            lease_owner=lease_owner,
            lease_generation=lease_generation,
        )
        if not updated:
            raise LeaseLostError(
                f"upload job {job_id} lost lease before verified completion"
            )
        return state

    def get(self, job_id: int) -> dict[str, Any]:
        con = _connect(self.state_db); row = dict(con.execute("select * from torrent_jobs where id=?", (job_id,)).fetchone()); con.close(); return row


class UploadLeaseHeartbeat:
    """Periodically extend one upload lease while blocking rclone work runs."""

    def __init__(
        self,
        repo: TorrentJobRepository,
        row: Mapping[str, Any],
        *,
        interval_sec: float = 60.0,
    ):
        self.repo = repo
        self.job_id = int(row["id"])
        self.lease_owner = str(row["lease_owner"])
        self.lease_generation = int(row.get("lease_generation") or 0)
        self.interval_sec = max(0.01, float(interval_sec))
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"upload-lease-{self.job_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self.interval_sec * 2))

    def raise_if_lost(self) -> None:
        if self._lost.is_set():
            raise LeaseLostError(
                f"upload job {self.job_id} lost lease generation {self.lease_generation}"
            )

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            try:
                renewed = self.repo.renew_lease(
                    self.job_id,
                    self.lease_owner,
                    self.lease_generation,
                )
            except Exception:
                renewed = False
            if not renewed:
                self._lost.set()
                self._stop.set()
                return


class UploadJobRunner:
    def __init__(
        self,
        repo: TorrentJobRepository,
        rclone,
        executor,
        backoff_schedule=(60, 180, 600, 1800, 7200, 21600),
        job_types=("upload", "sidecar_upload"),
        lease_heartbeat_interval_sec: float = 60.0,
    ):
        self.repo = repo
        self.worker = RcloneUploadWorker(rclone, executor)
        self.backoff_schedule = tuple(int(x) for x in backoff_schedule)
        self.job_types = tuple(job_types)
        self.lease_heartbeat_interval_sec = max(
            0.01, float(lease_heartbeat_interval_sec)
        )

    @staticmethod
    def _lease_kwargs(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "lease_owner": str(row["lease_owner"]),
            "lease_generation": int(row.get("lease_generation") or 0),
        }

    @staticmethod
    def _require_lease(updated: bool, row: Mapping[str, Any], operation: str) -> None:
        if not updated:
            raise LeaseLostError(
                f"upload job {int(row['id'])} lost lease before {operation}"
            )

    def run_next(self) -> int | None:
        row = self.repo.claim_next_any(self.job_types)
        if not row: return None
        payload = json.loads(row["payload_json"] or "{}")
        is_sidecar = row["job_type"] == "sidecar_upload"
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
        phase = str(row.get("phase") or "queued_copy")
        copied = True
        verification = None
        operation_error: Exception | None = None
        lease_lost = False
        heartbeat = UploadLeaseHeartbeat(
            self.repo,
            row,
            interval_sec=self.lease_heartbeat_interval_sec,
        )
        heartbeat.start()
        try:
            if phase in {"queued_copy", "copying"}:
                self._require_lease(
                    self.repo.set_phase(
                        int(row["id"]), "copying", **self._lease_kwargs(row)
                    ),
                    row,
                    "copy phase",
                )
                copied = self.worker.copy(job)
                heartbeat.raise_if_lost()
                if copied:
                    self._require_lease(
                        self.repo.mark_copy_completed(
                            int(row["id"]), **self._lease_kwargs(row)
                        ),
                        row,
                        "copy completion",
                    )

            if copied:
                self._require_lease(
                    self.repo.set_phase(
                        int(row["id"]), "verifying", **self._lease_kwargs(row)
                    ),
                    row,
                    "verification phase",
                )
                verification = self.worker.verify(job)
                heartbeat.raise_if_lost()
        except LeaseLostError:
            lease_lost = True
        except Exception as exc:
            operation_error = exc
        finally:
            heartbeat.stop()

        if lease_lost:
            return int(row["id"])
        if operation_error is not None:
            self._handle_upload_exception(
                row, payload, is_sidecar, operation_error
            )
            return int(row["id"])
        if not copied:
            if is_sidecar and self._attempts_exhausted(row):
                updated = self.repo.update_state(
                    row["id"],
                    "failed",
                    "retry requested",
                    exit_code=1,
                    **self._lease_kwargs(row),
                )
                if updated:
                    self._mark_sidecar_upload_failed(
                        row, payload, "sidecar_upload_failed"
                    )
            else:
                self.repo.schedule_retry(
                    row["id"],
                    "retry requested",
                    exit_code=1,
                    delay_sec=self._delay_for_attempt(int(row.get("attempts") or 1)),
                    **self._lease_kwargs(row),
                )
            return int(row["id"])

        if verification is None:
            return int(row["id"])
        if not verification.verified:
            result = UploadResult(
                "verify_pending",
                False,
                False,
                verification.method,
                tuple(verification.mismatches),
            )
            if is_sidecar and self._attempts_exhausted(row):
                updated = self.repo.update_state(
                    row["id"],
                    "failed",
                    "remote verification failed",
                    exit_code=1,
                    **self._lease_kwargs(row),
                )
                if updated:
                    self._mark_sidecar_upload_failed(
                        row, payload, "sidecar_upload_failed"
                    )
            else:
                updated = self.repo.record_verification_failure(
                    int(row["id"]),
                    verification,
                    **self._lease_kwargs(row),
                )
                if not updated:
                    return int(row["id"])
            if row["job_type"] == "upload" and row.get("batch_id") is not None:
                self._update_batch_upload_state(row, payload, result)
            return int(row["id"])

        try:
            final_state = self.repo.finalize_verified(row, payload, verification)
        except LeaseLostError:
            return int(row["id"])
        result = UploadResult(
            final_state,
            True,
            bool(payload.get("full_torrent")) and not is_sidecar,
            verification.method,
            (),
        )
        if is_sidecar:
            self._update_sidecar_upload_state(row, payload)
        if row["job_type"] == "upload" and row.get("batch_id") is not None:
            self._update_batch_upload_state(row, payload, result)
        if row["job_type"] == "upload" and result.remote_verified:
            self._enqueue_media_pipeline_if_present(row, payload)
        return int(row["id"])

    def _handle_upload_exception(
        self,
        row: dict[str, Any],
        payload: dict[str, Any],
        is_sidecar: bool,
        exc: Exception,
    ) -> None:
        error = redact(str(exc))[:500]
        if is_sidecar and self._attempts_exhausted(row):
            updated = self.repo.update_state(
                row["id"],
                "failed",
                error,
                exit_code=1,
                **self._lease_kwargs(row),
            )
            if updated:
                self._mark_sidecar_upload_failed(
                    row, payload, "sidecar_upload_failed"
                )
            return
        delay = self._delay_for_attempt(int(row.get("attempts") or 1))
        self.repo.schedule_retry(
            row["id"],
            error,
            exit_code=1,
            delay_sec=delay,
            **self._lease_kwargs(row),
        )

    def _update_batch_upload_state(self, row: dict[str, Any], payload: dict[str, Any], result) -> None:
        batch_id = int(row["batch_id"])
        job_id = int(row["id"])
        now = int(self.repo.now())
        size = int(payload.get("size") or 0)
        if result.state in {"cleanup_deferred", "cleanup_wait"} and result.remote_verified:
            state = str(result.state)
            cleanup_deferred_at = now
            reservation_reason = "batch_cleanup_deferred" if state == "cleanup_deferred" else "batch_cleanup_wait"
        elif result.state == "verify_pending":
            state = "verify_pending"
            cleanup_deferred_at = None
            reservation_reason = None
        elif result.state == "retry_wait":
            state = "retry_wait"
            cleanup_deferred_at = None
            reservation_reason = None
        else:
            state = str(result.state)
            cleanup_deferred_at = now if state in {"cleanup_deferred", "cleanup_wait"} else None
            reservation_reason = (
                "batch_cleanup_deferred"
                if state == "cleanup_deferred"
                else ("batch_cleanup_wait" if state == "cleanup_wait" else None)
            )

        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "update torrent_batches set state=?, upload_job_id=?, local_pinned_bytes=case when ?>0 then ? else local_pinned_bytes end, "
                "cleanup_deferred_at=coalesce(?, cleanup_deferred_at), updated_at=? where id=?",
                (state, job_id, size, size, cleanup_deferred_at, now, batch_id),
            )
            if reservation_reason is None:
                return
            existing = con.execute(
                "select id from resource_reservations where batch_id=? and kind='cleanup_pending' order by id limit 1",
                (batch_id,),
            ).fetchone()
            h = row.get("hash")
            if existing:
                con.execute(
                    "update resource_reservations set hash=?,accounting_class='current_pinned',owner='upload_job_runner',"
                    "bytes=?,state='active',expires_at=null,released_at=null,lease_generation=lease_generation+1,last_observed_at=?,"
                    "reason=? where id=?",
                    (h, size, now, reservation_reason, int(existing["id"])),
                )
            else:
                con.execute(
                    "insert into resource_reservations("
                    "hash,batch_id,kind,accounting_class,owner,bytes,state,created_at,expires_at,last_observed_at,reason) "
                    "values(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        h,
                        batch_id,
                        "cleanup_pending",
                        "current_pinned",
                        "upload_job_runner",
                        size,
                        "active",
                        now,
                        None,
                        now,
                        reservation_reason,
                    ),
                )

        write_transaction(self.repo.state_db, txn)

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
        self.repo.enqueue(
            row.get("hash"),
            row.get("batch_id"),
            "media_pipeline",
            media_payload,
            priority=int(JobPriority.MEDIA_PIPELINE),
        )

    def _update_sidecar_upload_state(self, row: dict[str, Any], payload: dict[str, Any]) -> None:
        sidecar_manifest_id = self._sidecar_manifest_id(payload)
        if sidecar_manifest_id is None:
            return
        now = int(self.repo.now())

        def txn(con: sqlite3.Connection) -> list[int]:
            manifest = con.execute(
                "select sm.*, mg.media_group_key, mg.emby_media_dir from sidecar_manifests sm "
                "left join media_groups mg on mg.id=sm.media_group_id where sm.id=?",
                (sidecar_manifest_id,),
            ).fetchone()
            if not manifest:
                return []

            pending = False
            terminal_failure = False
            total = 0
            for job_row in con.execute("select id,state,payload_json from torrent_jobs where job_type='sidecar_upload'"):
                try:
                    job_payload = json.loads(job_row["payload_json"] or "{}")
                except Exception:
                    continue
                try:
                    same_manifest = int(job_payload.get("sidecar_manifest_id") or 0) == sidecar_manifest_id
                except (TypeError, ValueError):
                    same_manifest = False
                if not same_manifest:
                    continue
                total += 1
                state = str(job_row["state"])
                if state in {"failed", "cancelled"}:
                    terminal_failure = True
                    break
                if state != "done":
                    pending = True
                    break

            if terminal_failure:
                return self._apply_sidecar_upload_failure_txn(
                    con,
                    manifest,
                    sidecar_manifest_id,
                    now,
                    allow_passthrough=self._payload_bool(payload, "allow_unrecognized_passthrough", True),
                    reason="sidecar_upload_failed",
                )

            if pending or total == 0:
                con.execute("update sidecar_manifests set state='sidecar_uploading', updated_at=? where id=?", (now, sidecar_manifest_id))
                con.execute(
                    "update media_pipeline_runs set state='SidecarUploading', updated_at=? "
                    "where media_group_id=? and state in ('SidecarUploadQueued','SidecarUploading')",
                    (now, int(manifest["media_group_id"])),
                )
                return []

            con.execute("update sidecar_manifests set state='sidecar_verified', updated_at=? where id=?", (now, sidecar_manifest_id))
            con.execute(
                "update media_pipeline_runs set state='SidecarVerified', metadata_policy='sidecar', passthrough_reason=null, updated_at=? "
                "where media_group_id=? and state in ('SidecarUploadQueued','SidecarUploading')",
                (now, int(manifest["media_group_id"])),
            )
            upload_ids = self._promotion_upload_ids_txn(
                con, int(manifest["media_group_id"])
            )
            if not upload_ids:
                self._queue_emby_refresh_for_sidecar_txn(
                    con, manifest, sidecar_manifest_id, now, "SidecarVerified"
                )
            return upload_ids

        upload_ids = write_transaction(self.repo.state_db, txn)
        for upload_id in upload_ids:
            finalize_canonical_upload(
                self.repo.state_db, upload_id=int(upload_id), now=now
            )

    def _mark_sidecar_upload_failed(self, row: dict[str, Any], payload: dict[str, Any], reason: str) -> None:
        sidecar_manifest_id = self._sidecar_manifest_id(payload)
        if sidecar_manifest_id is None:
            return
        now = int(self.repo.now())

        def txn(con: sqlite3.Connection) -> list[int]:
            manifest = con.execute(
                "select sm.*, mg.media_group_key, mg.emby_media_dir from sidecar_manifests sm "
                "left join media_groups mg on mg.id=sm.media_group_id where sm.id=?",
                (sidecar_manifest_id,),
            ).fetchone()
            if not manifest:
                return []
            return self._apply_sidecar_upload_failure_txn(
                con,
                manifest,
                sidecar_manifest_id,
                now,
                allow_passthrough=self._payload_bool(payload, "allow_unrecognized_passthrough", True),
                reason=reason,
            )

        upload_ids = write_transaction(self.repo.state_db, txn)
        for upload_id in upload_ids:
            finalize_canonical_upload(
                self.repo.state_db, upload_id=int(upload_id), now=now
            )

    def _apply_sidecar_upload_failure_txn(
        self,
        con: sqlite3.Connection,
        manifest: sqlite3.Row,
        sidecar_manifest_id: int,
        now: int,
        *,
        allow_passthrough: bool,
        reason: str,
    ) -> list[int]:
        con.execute("update sidecar_manifests set state='sidecar_upload_failed', updated_at=? where id=?", (now, sidecar_manifest_id))
        if allow_passthrough:
            con.execute(
                "update media_pipeline_runs set state='PassthroughAllowed', metadata_policy='passthrough', "
                "passthrough_reason=?, updated_at=? where media_group_id=? and state in ('SidecarUploadQueued','SidecarUploading')",
                (reason, now, int(manifest["media_group_id"])),
            )
            upload_ids = self._promotion_upload_ids_txn(
                con, int(manifest["media_group_id"])
            )
            if not upload_ids:
                self._queue_emby_refresh_for_sidecar_txn(
                    con,
                    manifest,
                    sidecar_manifest_id,
                    now,
                    "PassthroughAllowed",
                    passthrough_reason=reason,
                )
            return upload_ids
        else:
            con.execute(
                "update media_pipeline_runs set state='ManualReview', metadata_policy='manual_review', "
                "passthrough_reason=?, updated_at=? where media_group_id=? and state in ('SidecarUploadQueued','SidecarUploading')",
                (reason, now, int(manifest["media_group_id"])),
            )
        return []

    @staticmethod
    def _promotion_upload_ids_txn(
        con: sqlite3.Connection, media_group_id: int
    ) -> list[int]:
        return [
            int(row["upload_job_id"])
            for row in con.execute(
                "select distinct upload_job_id from media_promotions where media_group_id=? order by upload_job_id",
                (int(media_group_id),),
            ).fetchall()
        ]

    def _queue_emby_refresh_for_sidecar_txn(
        self,
        con: sqlite3.Connection,
        manifest: sqlite3.Row,
        sidecar_manifest_id: int,
        now: int,
        trigger_state: str,
        *,
        passthrough_reason: str | None = None,
    ) -> None:
        emby_dir = str(manifest["emby_media_dir"] or "").rstrip("/")
        media_group_key = str(manifest["media_group_key"] or "")
        if not emby_dir or not media_group_key or emby_dir in {"/media", "/media/gcrypt"}:
            return
        run = con.execute(
            "select upload_manifest_id from media_pipeline_runs where media_group_id=? order by updated_at desc,id desc limit 1",
            (int(manifest["media_group_id"]),),
        ).fetchone()
        upload_manifest_id = str(run["upload_manifest_id"] if run else "")
        refresh_payload = {
            "media_group_key": media_group_key,
            "upload_manifest_id": upload_manifest_id,
            "sidecar_manifest_id": sidecar_manifest_id,
            "trigger_state": trigger_state,
        }
        if passthrough_reason:
            refresh_payload["passthrough_reason"] = passthrough_reason
        payload_json = json.dumps(refresh_payload, ensure_ascii=False)
        earliest = now + 300
        max_run = now + 900
        existing = con.execute(
            "select * from emby_refresh_tasks where emby_media_dir=? and state='queued' order by id limit 1",
            (emby_dir,),
        ).fetchone()
        if existing:
            con.execute(
                "update emby_refresh_tasks set earliest_run_at=?, max_run_at=?, payload_json=?, updated_at=? where id=?",
                (min(int(existing["max_run_at"]), earliest), int(existing["max_run_at"]), payload_json, now, int(existing["id"])),
            )
        else:
            con.execute(
                "insert into emby_refresh_tasks(emby_media_dir,state,earliest_run_at,max_run_at,payload_json,created_at,updated_at) values(?,?,?,?,?,?,?)",
                (emby_dir, "queued", earliest, max_run, payload_json, now, now),
            )

    @staticmethod
    def _sidecar_manifest_id(payload: dict[str, Any]) -> int | None:
        raw_manifest_id = payload.get("sidecar_manifest_id")
        if raw_manifest_id in (None, ""):
            return None
        try:
            return int(raw_manifest_id)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _payload_bool(payload: dict[str, Any], key: str, default: bool) -> bool:
        value = payload.get(key, default)
        if isinstance(value, str):
            return value.strip().lower() not in {"0", "false", "no", "off", ""}
        return bool(value)

    @staticmethod
    def _attempts_exhausted(row: dict[str, Any]) -> bool:
        attempts = int(row.get("attempts") or 0)
        max_attempts = int(row.get("max_attempts") or 1)
        return attempts >= max_attempts

    def _delay_for_attempt(self, attempt: int) -> int:
        if not self.backoff_schedule:
            return 60
        idx = max(0, min(int(attempt) - 1, len(self.backoff_schedule) - 1))
        return self.backoff_schedule[idx]


class FullTorrentCleanupRunner:
    """Execute verified full-torrent cleanup only after seed policy approval."""

    def __init__(
        self,
        repo: TorrentJobRepository,
        executor,
        torrent_provider=None,
        *,
        free_bytes_provider=None,
        pressure_free_bytes: int = 5 * 1024**3,
        min_seed_sec: int = 900,
        min_ratio: float = 1.0,
        max_retention_sec: int = 7200,
    ):
        self.repo = repo
        self.executor = executor
        self.torrent_provider = torrent_provider
        self.free_bytes_provider = free_bytes_provider or (lambda: 2**63 - 1)
        self.pressure_free_bytes = max(0, int(pressure_free_bytes))
        self.min_seed_sec = max(0, int(min_seed_sec))
        self.min_ratio = max(0.0, float(min_ratio))
        self.max_retention_sec = max(0, int(max_retention_sec))

    def run_next(self) -> int | None:
        free_bytes = int(self.free_bytes_provider())
        row = self.repo.claim_next_cleanup(
            prefer_largest=free_bytes < self.pressure_free_bytes
        )
        if not row:
            return None
        payload = json.loads(row["payload_json"] or "{}")
        h = str(row.get("hash") or payload.get("hash") or "")
        torrent = None
        if self.torrent_provider is not None and h:
            torrent = self.torrent_provider(h)
            if not torrent:
                self.repo.update_state(int(row["id"]), "blocked", "source_absent", exit_code=1)
                return int(row["id"])
        if torrent is None:
            torrent = dict(payload.get("cleanup_policy_snapshot") or {})
        elif not isinstance(torrent, Mapping):
            torrent = vars(torrent)
        now = int(self.repo.now())
        decision = cleanup_eligibility(
            torrent,
            canonical_remote_verified=bool(
                payload.get("canonical_remote_verified")
            ),
            promotion_conflict=bool(payload.get("promotion_conflict")),
            free_bytes=free_bytes,
            pressure_free_bytes=self.pressure_free_bytes,
            min_seed_sec=self.min_seed_sec,
            min_ratio=self.min_ratio,
            max_retention_sec=self.max_retention_sec,
            now=now,
        )
        if not decision.allowed:
            if decision.next_check_at is None:
                self.repo.update_state(int(row["id"]), "blocked", decision.reason, exit_code=1)
            else:
                self.repo.schedule_retry(
                    int(row["id"]),
                    decision.reason,
                    exit_code=1,
                    delay_sec=max(1, int(decision.next_check_at) - now),
                )
            return int(row["id"])
        try:
            self.executor.qbt_post(
                "/api/v2/torrents/delete",
                {"hashes": h, "deleteFiles": "true"},
            )
        except Exception as exc:
            self.repo.schedule_retry(
                int(row["id"]),
                str(redact(str(exc)))[:500],
                exit_code=1,
                delay_sec=60,
            )
            return int(row["id"])

        parent_job_id = row.get("parent_job_id") or payload.get("upload_job_id")

        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "update torrent_jobs set state='done',phase='done',last_exit_code=0,last_stderr_tail=null,next_run_at=null,lease_owner=null,lease_until=null,updated_at=? where id=?",
                (now, int(row["id"])),
            )
            if parent_job_id is not None:
                con.execute(
                    "update torrent_jobs set state='done',phase='done',last_exit_code=0,last_stderr_tail=null,next_run_at=null,updated_at=? where id=? and state='cleanup_wait'",
                    (now, int(parent_job_id)),
                )
            con.execute(
                "insert into action_log(ts,hash,job_id,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?,?,?)",
                (
                    now,
                    h,
                    int(row["id"]),
                    "cleanup_full_torrent",
                    "/api/v2/torrents/delete",
                    json.dumps({"hash": h, "deleteFiles": True, "policy": decision.reason}, ensure_ascii=False),
                    "done",
                    0,
                ),
            )

        write_transaction(self.repo.state_db, txn)
        return int(row["id"])


class CleanupRequestRunner:
    """Handle approved cleanup requests conservatively.

    Pipeline middle batches are still qBT-managed and may share piece boundary
    data with neighboring/future batches, so this runner only records a logical
    cleanup request and keeps cleanup_pending reservations active.  It never
    calls qBT deleteFiles or removes files directly.
    """

    def __init__(self, repo: TorrentJobRepository, executor=None):
        self.repo = repo
        self.executor = executor

    def run_next(self) -> int | None:
        row = self.repo.claim_next("cleanup_request")
        if not row:
            return None
        payload = json.loads(row["payload_json"] or "{}")
        now = int(self.repo.now())
        batch_ids = self._requested_batch_ids(row, payload)
        target = str(payload.get("target") or row.get("hash") or "").strip()
        args = payload.get("args") if isinstance(payload.get("args"), list) else []
        if not target and args:
            target = str(args[0])

        def txn(con: sqlite3.Connection) -> tuple[list[int], str]:
            if batch_ids:
                placeholders = ",".join("?" for _ in batch_ids)
                rows = [
                    dict(r)
                    for r in con.execute(
                        f"select * from torrent_batches where id in ({placeholders}) and state='cleanup_deferred'",
                        tuple(batch_ids),
                    ).fetchall()
                ]
            elif target:
                rows = [
                    dict(r)
                    for r in con.execute(
                        "select * from torrent_batches where hash=? and state='cleanup_deferred' order by id",
                        (target,),
                    ).fetchall()
                ]
            else:
                rows = []
            if not rows:
                con.execute(
                    "update torrent_jobs set state='blocked', last_stderr_tail=?, last_exit_code=1, lease_owner=null, lease_until=null, updated_at=? where id=?",
                    ("no cleanup_deferred batch matched request", now, int(row["id"])),
                )
                con.execute(
                    "insert into action_log(ts,hash,job_id,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?,?)",
                    (
                        now,
                        row.get("hash"),
                        int(row["id"]),
                        "cleanup_request",
                        "cleanup_request",
                        json.dumps(redact({"target": target, "batch_ids": batch_ids, "physical_delete": False}), ensure_ascii=False),
                        "blocked",
                        0,
                        "no cleanup_deferred batch matched request",
                    ),
                )
                return [], "blocked"

            ids = [int(r["id"]) for r in rows]
            placeholders = ",".join("?" for _ in ids)
            con.execute(f"update torrent_batches set state='cleanup_requested', updated_at=? where id in ({placeholders})", (now, *ids))
            con.execute(
                f"update resource_reservations set accounting_class='current_pinned',owner='command_processor',"
                f"state='active',lease_generation=lease_generation+1,last_observed_at=?,reason=? "
                f"where kind='cleanup_pending' and batch_id in ({placeholders})",
                (now, "cleanup_requested_logical_only", *ids),
            )
            con.execute(
                "update torrent_jobs set state='done',last_exit_code=0,last_stderr_tail=null,next_run_at=null,lease_owner=null,lease_until=null,updated_at=? where id=?",
                (now, int(row["id"])),
            )
            con.execute(
                "insert into action_log(ts,hash,job_id,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?,?)",
                (
                    now,
                    row.get("hash"),
                    int(row["id"]),
                    "cleanup_request",
                    "torrent_batches/" + "|".join(str(i) for i in ids),
                    json.dumps(redact({"target": target, "batch_ids": ids, "physical_delete": False}), ensure_ascii=False),
                    "logical_only",
                    0,
                    None,
                ),
            )
            return ids, "logical_only"

        write_transaction(self.repo.state_db, txn)
        return int(row["id"])

    @staticmethod
    def _requested_batch_ids(row: dict[str, Any], payload: dict[str, Any]) -> list[int]:
        raw_values: list[Any] = []
        if row.get("batch_id") is not None:
            raw_values.append(row.get("batch_id"))
        if payload.get("batch_id") is not None:
            raw_values.append(payload.get("batch_id"))
        if isinstance(payload.get("batch_ids"), list):
            raw_values.extend(payload.get("batch_ids") or [])
        out: list[int] = []
        for value in raw_values:
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed not in out:
                out.append(parsed)
        return out


def reconcile_jobs(
    state_db: str | Path,
    now: int | None = None,
    dry_run: bool = True,
    retry_delay_sec: int = 60,
    max_retry_delay_sec: int = 21_600,
) -> dict[str, int]:
    now = int(now if now is not None else time.time())
    con = _connect(state_db)
    rows = [dict(r) for r in con.execute("select * from torrent_jobs where state='running' and lease_until is not null and lease_until<?", (now,))]
    exhausted_attempts = [
        dict(r)
        for r in con.execute(
            "select * from torrent_jobs where state in ('queued','verify_pending','retry_wait') and attempts>=max_attempts"
        )
    ]
    con.close()
    if (rows or exhausted_attempts) and not dry_run:
        def txn(wcon: sqlite3.Connection) -> None:
            for row in rows:
                if int(row.get("attempts") or 0) >= int(row.get("max_attempts") or 1):
                    previous = str(row.get("last_stderr_tail") or "").strip()
                    message = "lease expired and attempts exhausted"
                    if previous:
                        message = f"{message}; last error: {previous}"
                    wcon.execute(
                        "update torrent_jobs set state='failed',lease_owner=null,lease_until=null,next_run_at=null,"
                        "last_stderr_tail=?,last_exit_code=coalesce(last_exit_code,1),updated_at=? where id=?",
                        (redact(message)[:1000], now, row["id"]),
                    )
                    continue
                exponent = max(0, int(row.get("attempts") or 1) - 1)
                delay = min(max(1, int(max_retry_delay_sec)), max(1, int(retry_delay_sec)) * (2**exponent))
                wcon.execute(
                    "update torrent_jobs set state='retry_wait', lease_owner=null, lease_until=null, next_run_at=?, "
                    "last_stderr_tail=?, last_exit_code=coalesce(last_exit_code,1), updated_at=? where id=?",
                    (now + delay, "lease expired during reconcile", now, row["id"]),
                )
            for row in exhausted_attempts:
                previous = str(row.get("last_stderr_tail") or "").strip()
                message = "retry attempts exhausted"
                if previous:
                    message = f"{message}; last error: {previous}"
                wcon.execute(
                    "update torrent_jobs set state='failed', lease_owner=null, lease_until=null, next_run_at=null, "
                    "last_stderr_tail=?, last_exit_code=coalesce(last_exit_code,1), updated_at=? where id=?",
                    (redact(message)[:1000], now, row["id"]),
                )

        write_transaction(state_db, txn)
    return {
        "expired_running": len(rows),
        "exhausted_retry_wait": len(exhausted_attempts),
        "exhausted_attempts": len(exhausted_attempts),
        "dry_run": 1 if dry_run else 0,
    }


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
        self.jobs = TorrentJobRepository(commands.state_db, now=commands.now)

    def run_next(self) -> str | None:
        row = self.commands.claim_next()
        if not row: return None
        command_id = row["command_id"]; command = row["command"]; payload = json.loads(row["payload_json"] or "{}"); args = payload.get("args") or []
        claimed_from = row.get("_claimed_from_state") or row.get("state")
        if command in self.DANGEROUS and claimed_from != "approved":
            approval_id = self.commands.create_approval(command_id, command, payload)
            if self.notifications is not None:
                self.notifications.enqueue(
                    chat_id=row["chat_id"],
                    topic="approval",
                    level="warning",
                    message=self._approval_message(command, args),
                    payload={
                        "approval_id": approval_id,
                        "command_id": command_id,
                        "action": command,
                        "reply_markup": self._approval_reply_markup(approval_id),
                    },
                    dedupe_key=f"approval-request:{approval_id}",
                )
            self.commands.set_state(command_id, "approval_required"); return command_id
        if command == "pause" and args:
            self.executor.qbt_post("/api/v2/torrents/stop", {"hashes": args[0]}); self.commands.set_state(command_id, "done"); return command_id
        if command == "resume" and args:
            self.executor.qbt_post("/api/v2/torrents/start", {"hashes": args[0]}); self.commands.set_state(command_id, "done"); return command_id
        if command == "queue":
            self._enqueue_command_job(command, payload, args, default_job_type="upload", default_priority=50)
            self.commands.set_state(command_id, "done")
            return command_id
        if command == "force_upload":
            self._enqueue_command_job(command, payload, args, default_job_type="upload", default_priority=0, force_upload=True)
            self.commands.set_state(command_id, "done")
            return command_id
        if command == "cleanup":
            self._enqueue_command_job(command, payload, args, default_job_type="cleanup_request", default_priority=10)
            self.commands.set_state(command_id, "done")
            return command_id
        if command == "config":
            self._audit_config_command(payload, args)
            self.commands.set_state(command_id, "done")
            return command_id
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

    def _approval_message(self, command: str, args: list[Any]) -> str:
        suffix = " ".join(str(a) for a in args)
        command_line = f"/{command} {suffix}".strip()
        return f"approval required: {command_line}"

    def _approval_reply_markup(self, approval_id: str) -> dict[str, Any]:
        return {
            "inline_keyboard": [[
                {"text": "Approve", "callback_data": f"approve:{approval_id}"},
                {"text": "Deny", "callback_data": f"deny:{approval_id}"},
            ]]
        }

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

    def _enqueue_command_job(
        self,
        command: str,
        payload: dict[str, Any],
        args: list[Any],
        *,
        default_job_type: str,
        default_priority: int,
        force_upload: bool = False,
    ) -> int:
        target = str(payload.get("hash") or payload.get("target") or (args[0] if args else "") or "")
        raw_batch_id = payload.get("batch_id")
        try:
            batch_id = int(raw_batch_id) if raw_batch_id not in (None, "") else None
        except (TypeError, ValueError):
            batch_id = None

        job_payload = payload.get("job_payload") or payload.get("upload_payload") or payload.get("payload")
        if isinstance(job_payload, dict) and job_payload:
            durable_payload = dict(job_payload)
            job_type = str(payload.get("job_type") or default_job_type)
        else:
            durable_payload = {"target": target, "args": [str(a) for a in args], "source": "telegram"}
            fallback_job_type = f"{command}_request" if default_job_type == "upload" else default_job_type
            job_type = str(payload.get("job_type") or fallback_job_type)
        if force_upload:
            durable_payload["force_upload"] = True
        priority = int(payload.get("priority") if payload.get("priority") is not None else default_priority)
        return self.jobs.enqueue(target or None, batch_id, job_type, durable_payload, priority=priority)

    def _audit_config_command(self, payload: dict[str, Any], args: list[Any]) -> None:
        now = int(self.commands.now())
        audit_payload = dict(payload)
        audit_payload.setdefault("args", [str(a) for a in args])
        write_transaction(
            self.commands.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?)",
                (now, "bot_config", "config", json.dumps(redact(audit_payload), ensure_ascii=False), "queued", 0),
            ),
        )
