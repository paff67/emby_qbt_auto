from __future__ import annotations

import sqlite3
import time
import json
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import write_transaction
from .observability import redact
from .runtime import reconcile_jobs

RETENTION_TABLES = (
    "events_v2",
    "action_log",
    "decision_log",
    "metrics_snapshots",
    "junk_janitor_events",
)


class SQLiteMaintenanceService:
    """Level-3 SQLite maintenance: retention + WAL checkpoint.

    This service is deliberately small and synchronous because it runs only in
    the 5-minute maintenance loop, never in the 2s safety path.  Deletes are
    performed in bounded batches so a production DB cannot be locked by a large
    retention cleanup.
    """

    def __init__(
        self,
        state_db: str | Path,
        now: Callable[[], int] | None = None,
        retention_days: int = 5,
        retention_delete_batch_size: int = 1000,
        journal_size_limit_bytes: int = 64 * 1024 * 1024,
        preferences_guard=None,
    ):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))
        self.retention_days = int(retention_days)
        self.retention_delete_batch_size = max(1, int(retention_delete_batch_size))
        self.journal_size_limit_bytes = int(journal_size_limit_bytes)
        self.preferences_guard = preferences_guard

    def run_once(
        self,
        present_hashes: set[str] | None = None,
        torrent_snapshots: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict:
        job_reconcile = reconcile_jobs(self.state_db, now=int(self.now()), dry_run=False)
        cutoff = int(self.now()) - self.retention_days * 86400
        deleted: dict[str, int] = {}
        batch_sources_absent = [0]
        batch_suspect_expired = [0]
        missing_files_audit: dict[str, Any] = {"count": 0, "sample_hashes": []}
        def txn(con: sqlite3.Connection) -> int:
            con.execute("pragma busy_timeout=5000")
            con.execute(f"pragma journal_size_limit={self.journal_size_limit_bytes}")
            for table in RETENTION_TABLES:
                deleted[table] = self._delete_old_rows(con, table, cutoff)
            if present_hashes is not None:
                batch_sources_absent[0] = self._reconcile_absent_batch_sources(
                    con,
                    {str(value) for value in present_hashes},
                    int(self.now()),
                )
            batch_suspect_expired[0] = self._mark_suspect_expired_batches(con, int(self.now()))
            if torrent_snapshots is not None:
                missing_files_audit.update(self._audit_unmanaged_missing_files(con, torrent_snapshots, int(self.now())))
            reservations_expired[0] = self._expire_resource_reservations(con, int(self.now()))
            journal_size_limit = int(con.execute("pragma journal_size_limit").fetchone()[0])
            return journal_size_limit

        reservations_expired = [0]
        journal_size_limit = int(write_transaction(self.state_db, txn))
        checkpoint = tuple(write_transaction(self.state_db, lambda con: con.execute("pragma wal_checkpoint(passive)").fetchone()))
        result = {
            "retention_cutoff": cutoff,
            "retention_deleted": deleted,
            "retention_days": self.retention_days,
            "retention_delete_batch_size": self.retention_delete_batch_size,
            "wal_checkpoint": checkpoint,
            "journal_size_limit_bytes": journal_size_limit,
            "reservations_expired": reservations_expired[0],
            "batch_sources_absent": batch_sources_absent[0],
            "batch_suspect_expired": batch_suspect_expired[0],
            "job_reconcile": job_reconcile,
            "unmanaged_missing_files": missing_files_audit,
        }
        if self.preferences_guard is not None:
            result["qbt_preferences"] = self.preferences_guard.reconcile()
        return result

    @staticmethod
    def _audit_unmanaged_missing_files(
        con: sqlite3.Connection,
        snapshots: Mapping[str, Mapping[str, Any]],
        now: int,
    ) -> dict[str, Any]:
        hashes: list[str] = []
        for fallback_hash, raw in snapshots.items():
            torrent = dict(raw)
            tags = {part.strip() for part in str(torrent.get("tags") or "").split(",") if part.strip()}
            managed = str(torrent.get("category") or "") == "auto" or "auto" in tags
            if managed or str(torrent.get("state") or "").lower() != "missingfiles":
                continue
            hashes.append(str(torrent.get("hash") or fallback_hash))
        audit = {"count": len(hashes), "sample_hashes": sorted(hashes)[:20]}
        payload = json.dumps(audit, ensure_ascii=False, sort_keys=True)
        row = con.execute(
            "select id from metrics_snapshots where component='unmanaged_missing_files' order by id desc limit 1"
        ).fetchone()
        if row:
            con.execute("update metrics_snapshots set ts=?,metrics_json=? where id=?", (now, payload, int(row["id"])))
        else:
            con.execute(
                "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                (now, "unmanaged_missing_files", payload),
            )
        return audit

    def _delete_old_rows(self, con: sqlite3.Connection, table: str, cutoff: int) -> int:
        total = 0
        while True:
            cur = con.execute(
                f"delete from {table} where id in (select id from {table} where ts < ? order by id limit ?)",
                (cutoff, self.retention_delete_batch_size),
            )
            total += int(cur.rowcount or 0)
            if int(cur.rowcount or 0) < self.retention_delete_batch_size:
                return total

    def _expire_resource_reservations(self, con: sqlite3.Connection, now: int) -> int:
        rows = [
            dict(r)
            for r in con.execute(
                "select id,hash,kind,bytes,expires_at from resource_reservations "
                "where state='active' and coalesce(kind,'')!='batch' and expires_at is not null and expires_at<=?",
                (now,),
            ).fetchall()
        ]
        if not rows:
            return 0
        ids = [int(r["id"]) for r in rows]
        placeholders = ",".join("?" for _ in ids)
        con.execute(
            f"update resource_reservations set state='expired', released_at=?, reason='reservation_expired' where id in ({placeholders})",
            (now, *ids),
        )
        data = {"expired_count": len(ids), "reservations": rows[:20]}
        con.execute(
            "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
            (
                now,
                "info",
                "reservation",
                "reservation_expired",
                f"expired {len(ids)} resource reservations",
                json.dumps(redact(data), ensure_ascii=False),
            ),
        )
        return len(ids)

    @staticmethod
    def _reconcile_absent_batch_sources(
        con: sqlite3.Connection,
        present_hashes: set[str],
        now: int,
    ) -> int:
        states = ("reserved", "applied_to_qbt", "downloading", "suspect_expired")
        placeholders = ",".join("?" for _ in states)
        rows = con.execute(
            f"select id,hash from torrent_batches where state in ({placeholders})",
            states,
        ).fetchall()
        absent = [(int(row["id"]), str(row["hash"] or "")) for row in rows if str(row["hash"] or "") not in present_hashes]
        if not absent:
            return 0
        ids = [batch_id for batch_id, _hash in absent]
        id_placeholders = ",".join("?" for _ in ids)
        con.execute(
            f"update torrent_batches set state='source_absent',source_present=0,updated_at=? where id in ({id_placeholders})",
            (now, *ids),
        )
        con.execute(
            f"update resource_reservations set state='released',released_at=?,last_observed_at=?,reason='batch_source_absent' "
            f"where kind='batch' and state='active' and batch_id in ({id_placeholders})",
            (now, now, *ids),
        )
        con.execute(
            f"update batch_file_claims set state='released',released_at=? where state='active' and batch_id in ({id_placeholders})",
            (now, *ids),
        )
        hashes = sorted({torrent_hash for _batch_id, torrent_hash in absent if torrent_hash})
        if hashes:
            hash_placeholders = ",".join("?" for _ in hashes)
            con.execute(
                f"delete from scheduler_intents where component='batch' and hash in ({hash_placeholders})",
                hashes,
            )
        return len(ids)

    @staticmethod
    def _mark_suspect_expired_batches(con: sqlite3.Connection, now: int) -> int:
        rows = con.execute(
            "select id,hash from torrent_batches where state in ('reserved','applied_to_qbt','downloading') "
            "and source_present=1 and lease_until is not null and lease_until<=?",
            (now,),
        ).fetchall()
        if not rows:
            return 0
        ids = [int(row["id"]) for row in rows]
        placeholders = ",".join("?" for _ in ids)
        con.execute(
            f"update torrent_batches set state='suspect_expired',updated_at=? where id in ({placeholders})",
            (now, *ids),
        )
        con.execute(
            f"update resource_reservations set state='active',expires_at=null,released_at=null,last_observed_at=?,"
            f"reason='batch_suspect_expired' where kind='batch' and state='active' and batch_id in ({placeholders})",
            (now, *ids),
        )
        hashes = sorted({str(row["hash"] or "") for row in rows if str(row["hash"] or "")})
        if hashes:
            hash_placeholders = ",".join("?" for _ in hashes)
            con.execute(
                f"delete from scheduler_intents where component='batch' and hash in ({hash_placeholders})",
                hashes,
            )
        return len(ids)
