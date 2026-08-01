from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .capacity_reclaim import OPEN_JOB_STATES, capacity_reclaim_locked_hashes
from .db import readonly_connect, write_transaction
from .hash_identity import canonical_torrent_hash
from .observability import redact
from .scheduler_intents import SchedulerIntent, SchedulerIntentRepository


MIB = 1024**2
GIB = 1024**3
DEFAULT_PROBE_BUDGET_BYTES = 512 * MIB
DEFAULT_PROBE_DURATION_SEC = 10 * 60
DEFAULT_REPROBE_INTERVAL_SEC = 30 * 60
DEFAULT_BACKOFF_SCHEDULE_SEC = (30 * 60, 2 * 3600, 6 * 3600, 24 * 3600)
ACTIVE_CAROUSEL_STATES = frozenset({"pending", "probing"})


def _connect(path: str | Path) -> sqlite3.Connection:
    return readonly_connect(path)


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    raw = str(torrent.get("tags") or "")
    return {p.strip() for p in raw.split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _completed_bytes(snapshot: Mapping[str, Any]) -> int:
    for field in ("completed_bytes", "completed", "downloaded"):
        if snapshot.get(field) is not None:
            try:
                return max(0, int(snapshot.get(field) or 0))
            except (TypeError, ValueError):
                return 0
    return 0


def _active_soak_cooldown_hashes(state_db: str | Path, now: int) -> set[str]:
    con = readonly_connect(state_db)
    try:
        return {
            canonical
            for row in con.execute(
                "select hash from soak_state "
                "where state='soak_cooldown' and cooldown_until is not null "
                "and cooldown_until>?",
                (int(now),),
            ).fetchall()
            if (canonical := canonical_torrent_hash(row["hash"]))
        }
    finally:
        con.close()


def _snapshot_dict(snapshots: Mapping[str, Any], h: str) -> dict[str, Any]:
    raw = snapshots.get(h)
    if raw is None:
        return {}
    if hasattr(raw, "__dict__"):
        return dict(vars(raw))
    return dict(raw)


def list_availability_probe_candidates(
    state_db: str | Path,
    snapshots: Mapping[str, Any],
    *,
    now: int,
    reprobe_interval_sec: int = DEFAULT_REPROBE_INTERVAL_SEC,
    free_bytes: int | None = None,
    min_free_bytes: int = 5 * GIB,
    carousel_enabled: bool = True,
    dry_run: bool = False,
    concurrency: int = 1,
) -> list[str]:
    """Return hashes that would actually be eligible for a new probe request.

    Shared by Carousel and capacity-state accounting so speculative probeable
    counts cannot diverge from the live guard/filter path.
    """

    if not carousel_enabled or bool(dry_run) or int(concurrency) <= 0:
        return []
    if free_bytes is not None and int(free_bytes) < int(min_free_bytes):
        return []

    con = readonly_connect(state_db)
    try:
        rows = [
            dict(r)
            for r in con.execute(
                "select hash,desired_state,reason,allocated_at from scheduler_allocations "
                "where desired_state='dead' or reason='capacity_nonviable'"
            )
        ]
        state_rows = {
            str(r["hash"]): dict(r)
            for r in con.execute("select * from carousel_state")
        }
        open_job_hashes = {
            canonical_torrent_hash(r["hash"])
            for r in con.execute(
                f"select distinct hash from torrent_jobs where state in ({','.join('?' for _ in OPEN_JOB_STATES)})",
                OPEN_JOB_STATES,
            )
            if canonical_torrent_hash(r["hash"])
        }
        active_claim_hashes = {
            canonical_torrent_hash(r["hash"])
            for r in con.execute(
                "select distinct hash from resource_reservations where state='active' "
                "and (expires_at is null or expires_at>?)",
                (int(now),),
            )
            if canonical_torrent_hash(r["hash"])
        }
        active_probe_intents = {
            canonical_torrent_hash(r["hash"])
            for r in con.execute(
                "select hash from scheduler_intents "
                "where intent='availability_probe' and (expires_at is null or expires_at>?)",
                (int(now),),
            )
            if canonical_torrent_hash(r["hash"])
        }
    finally:
        con.close()

    cooldown_hashes = _active_soak_cooldown_hashes(state_db, now)
    reclaim_locked = capacity_reclaim_locked_hashes(state_db)
    scored: list[tuple[int, int, str, str]] = []
    seen: set[str] = set()
    for row in rows:
        h = str(row["hash"])
        canonical = canonical_torrent_hash(h) or h
        if canonical in seen:
            continue
        snap = _snapshot_dict(snapshots, h)
        if not snap:
            snap = _snapshot_dict(snapshots, canonical)
        if not snap or not _is_managed(snap) or int(snap.get("amount_left") or 0) <= 0:
            continue
        if canonical in cooldown_hashes:
            continue
        if canonical in reclaim_locked:
            continue
        if canonical in open_job_hashes or canonical in active_claim_hashes:
            continue
        if canonical in active_probe_intents:
            continue
        state = state_rows.get(h) or state_rows.get(canonical)
        last_probe_at = None if state is None else state.get("last_probe_at")
        if state:
            state_name = str(state.get("state") or "")
            if state_name in ACTIVE_CAROUSEL_STATES:
                continue
            backoff_until = state.get("backoff_until")
            if backoff_until is not None and int(backoff_until) > now:
                continue
            if state_name == "soak":
                if str(row.get("reason") or "") != "capacity_nonviable":
                    continue
                if last_probe_at is not None and now - int(last_probe_at) < int(reprobe_interval_sec):
                    continue
        # Never-probed first, then oldest last_probe_at, then hash.
        never_probed = 0 if last_probe_at is None else 1
        probe_age = 0 if last_probe_at is None else int(last_probe_at)
        scored.append((never_probed, probe_age, canonical, h))
        seen.add(canonical)
    scored.sort()
    return [item[3] for item in scored]


def confirm_availability_probes_started(
    state_db: str | Path,
    completed_by_hash: Mapping[str, int],
    *,
    now: int,
    probe_duration_sec: int = DEFAULT_PROBE_DURATION_SEC,
) -> list[str]:
    """Promote pending probe intents to confirmed probing after a real qBT start."""

    confirmed: list[str] = []
    intent_repo = SchedulerIntentRepository(state_db)

    def txn(con: sqlite3.Connection) -> None:
        for raw_hash, completed in completed_by_hash.items():
            torrent_hash = canonical_torrent_hash(raw_hash) or str(raw_hash)
            intent = con.execute(
                "select hash,expires_at,data_json from scheduler_intents "
                "where component='carousel' and lower(trim(hash))=? "
                "and intent='availability_probe' "
                "and (expires_at is null or expires_at>?) "
                "order by rowid desc limit 1",
                (torrent_hash, int(now)),
            ).fetchone()
            if intent is None:
                continue
            state = con.execute(
                "select state from carousel_state where lower(trim(hash))=? "
                "order by rowid desc limit 1",
                (torrent_hash,),
            ).fetchone()
            if state is not None and str(state["state"]) == "probing":
                continue
            # Restart the probe timer from real confirmation, not request time.
            expires_at = int(now) + int(probe_duration_sec)
            con.execute(
                "insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_until,backoff_level,updated_at) "
                "values(?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state='probing', probe_started_at=excluded.probe_started_at, "
                "last_probe_at=excluded.last_probe_at, backoff_until=null, updated_at=excluded.updated_at",
                (torrent_hash, "probing", int(now), int(now), None, 0, int(now)),
            )
            intent_repo.upsert_in_transaction(
                con,
                SchedulerIntent(
                    "carousel",
                    torrent_hash,
                    "availability_probe",
                    40,
                    expires_at,
                    {
                        "phase": "probing",
                        "probe_started_completed_bytes": max(0, int(completed)),
                    },
                ),
            )
            confirmed.append(torrent_hash)

    if completed_by_hash:
        write_transaction(state_db, txn)
    return confirmed


class CarouselService:
    """Bounded availability-probe loop for dead / capacity-nonviable torrents.

    Carousel only requests probes.  Confirmed probing (probe_started_at +
    completed baseline) happens after Planner actually starts the torrent.
    """

    def __init__(
        self,
        state_db: str | Path,
        executor,
        dry_run: bool = True,
        concurrency: int = 1,
        probe_duration_sec: int = DEFAULT_PROBE_DURATION_SEC,
        backoff_schedule_sec: tuple[int, ...] = DEFAULT_BACKOFF_SCHEDULE_SEC,
        min_free_bytes: int = 5 * GIB,
        reprobe_interval_sec: int = DEFAULT_REPROBE_INTERVAL_SEC,
        probe_budget_bytes: int = DEFAULT_PROBE_BUDGET_BYTES,
        live_verify: bool = False,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = bool(dry_run)
        self.concurrency = max(0, int(concurrency))
        self.live_verify = bool(live_verify)
        self.probe_duration_sec = int(probe_duration_sec)
        self.backoff_schedule_sec = tuple(int(x) for x in backoff_schedule_sec) or (30 * 60,)
        self.min_free_bytes = int(min_free_bytes)
        self.reprobe_interval_sec = max(0, int(reprobe_interval_sec))
        self.probe_budget_bytes = max(0, int(probe_budget_bytes))
        self.now = now or (lambda: int(time.time()))
        self.intent_repository = SchedulerIntentRepository(self.state_db)

    def run_once(self, snapshots: Mapping[str, Any], sync_healthy: bool, free_bytes: int | None = None) -> dict[str, Any]:
        now = int(self.now())
        effective_concurrency = self._effective_concurrency()
        if not sync_healthy:
            self._event("warning", "suspended_unhealthy_sync", "carousel suspended because qBT sync is unhealthy", {"dry_run": self.dry_run})
            result = {"suspended": True, "reason": "unhealthy_sync", "started": [], "promoted": [], "stopped": [], "dry_run": self.dry_run, "live_verify": self.live_verify, "effective_concurrency": effective_concurrency}
            self._metrics(result)
            return result
        promoted, stopped = self._reconcile_active_probes(snapshots, now)
        if free_bytes is not None and int(free_bytes) < self.min_free_bytes:
            cancelled = self._cancel_active_probes(now, reason="disk_guard")
            stopped = list(stopped) + cancelled
            data = {
                "free_bytes": int(free_bytes),
                "min_free_bytes": self.min_free_bytes,
                "dry_run": self.dry_run,
                "cancelled_probes": cancelled,
            }
            self._event("warning", "suspended_disk_guard", "carousel suspended because disk free space is below live guard", data)
            result = {
                "suspended": True,
                "reason": "disk_guard",
                "started": [],
                "promoted": promoted,
                "stopped": stopped,
                "active_probes": self._active_probe_count(),
                "dry_run": self.dry_run,
                "live_verify": self.live_verify,
                "effective_concurrency": effective_concurrency,
                **data,
            }
            self._metrics(result)
            return result

        active_count = self._active_probe_count()
        capacity = max(0, effective_concurrency - active_count)
        started = self._start_new_probes(snapshots, now, capacity)
        active_after = self._active_probe_count()
        result = {
            "suspended": False,
            "started": started,
            "promoted": promoted,
            "stopped": stopped,
            "active_probes": active_after,
            "dry_run": self.dry_run,
            "live_verify": self.live_verify,
            "effective_concurrency": effective_concurrency,
        }
        self._metrics(result)
        return result

    def _effective_concurrency(self) -> int:
        if self.live_verify:
            return min(self.concurrency, 1)
        return self.concurrency

    def _reconcile_active_probes(self, snapshots: Mapping[str, Any], now: int) -> tuple[list[str], list[str]]:
        promoted: list[str] = []
        expired: list[str] = []
        con = _connect(self.state_db)
        rows = [
            dict(r)
            for r in con.execute(
                "select * from carousel_state where state in ('pending','probing') "
                "order by coalesce(probe_started_at, updated_at),hash"
            )
        ]
        intent_rows = {
            canonical_torrent_hash(r["hash"]): dict(r)
            for r in con.execute(
                "select hash,expires_at,data_json from scheduler_intents "
                "where component='carousel' and intent='availability_probe'"
            )
            if canonical_torrent_hash(r["hash"])
        }
        con.close()
        for row in rows:
            h = str(row["hash"])
            canonical = canonical_torrent_hash(h) or h
            state_name = str(row.get("state") or "")
            intent = intent_rows.get(canonical)
            if state_name == "pending":
                expires_at = None if intent is None else intent.get("expires_at")
                started_marker = row.get("updated_at")
                pending_age = now - int(started_marker if started_marker is not None else now)
                expired_pending = (
                    expires_at is not None and int(expires_at) <= now
                ) or pending_age >= self.probe_duration_sec
                if expired_pending:
                    expired.append(h)
                    level = int(row.get("backoff_level") or 0)
                    backoff = self.backoff_schedule_sec[min(level, len(self.backoff_schedule_sec) - 1)]
                    self._mark_dead(h, now, backoff_until=now + backoff, backoff_level=level + 1)
                    self._decision(h, "dead", "carousel_probe_pending_expired", {"backoff_sec": backoff})
                continue

            snap = self._snapshot(snapshots, h)
            started_completed = self._probe_started_completed_bytes(intent)
            budget_exhausted = (
                started_completed is not None
                and (_completed_bytes(snap) - int(started_completed)) >= self.probe_budget_bytes
            )
            if self._probe_succeeded(snap, started_completed) or budget_exhausted:
                self._mark_soak(h, now)
                self._decision(
                    h,
                    "soak",
                    "carousel_probe_succeeded" if not budget_exhausted else "carousel_probe_budget_exhausted",
                    {
                        "probe_started_at": row.get("probe_started_at"),
                        "probe_started_completed_bytes": started_completed,
                        "probe_budget_bytes": self.probe_budget_bytes,
                    },
                )
                promoted.append(h)
                continue
            started_at = int(row["probe_started_at"]) if row.get("probe_started_at") is not None else now
            if now - started_at >= self.probe_duration_sec:
                expired.append(h)
                level = int(row.get("backoff_level") or 0)
                backoff = self.backoff_schedule_sec[min(level, len(self.backoff_schedule_sec) - 1)]
                self._mark_dead(h, now, backoff_until=now + backoff, backoff_level=level + 1)
                self._decision(h, "dead", "carousel_no_swarm", {"probe_started_at": started_at, "backoff_sec": backoff})
        return promoted, expired

    def _cancel_active_probes(self, now: int, *, reason: str) -> list[str]:
        con = _connect(self.state_db)
        rows = [
            dict(r)
            for r in con.execute(
                "select hash,backoff_level from carousel_state where state in ('pending','probing')"
            )
        ]
        con.close()
        cancelled: list[str] = []
        for row in rows:
            h = str(row["hash"])
            level = int(row.get("backoff_level") or 0)
            backoff = self.backoff_schedule_sec[min(level, len(self.backoff_schedule_sec) - 1)]
            self._mark_dead(h, now, backoff_until=now + backoff, backoff_level=level + 1)
            self._decision(h, "dead", f"carousel_{reason}_cancel", {"backoff_sec": backoff})
            cancelled.append(h)
        return cancelled

    def _start_new_probes(self, snapshots: Mapping[str, Any], now: int, capacity: int) -> list[str]:
        if capacity <= 0 or self.concurrency <= 0:
            return []
        candidates = self._probe_candidates(snapshots, now)
        selected = candidates[:capacity]
        if not selected:
            return []
        if self.dry_run:
            for h in selected:
                self._decision(
                    h,
                    "carousel_probe",
                    "carousel_probe_dry_run",
                    {"concurrency": self.concurrency, "dry_run": True},
                )
            return selected
        for h in selected:
            self._mark_probe_pending(h, now)
            self._decision(h, "carousel_probe", "carousel_probe_requested", {"concurrency": self.concurrency})
        return selected

    def _probe_candidates(self, snapshots: Mapping[str, Any], now: int) -> list[str]:
        return list_availability_probe_candidates(
            self.state_db,
            snapshots,
            now=now,
            reprobe_interval_sec=self.reprobe_interval_sec,
            free_bytes=None,
            min_free_bytes=self.min_free_bytes,
            carousel_enabled=True,
            dry_run=False,
            concurrency=self.concurrency,
        )

    # Backward-compatible name used by older tests/helpers.
    def _dead_candidates(self, snapshots: Mapping[str, Any], now: int) -> list[str]:
        return self._probe_candidates(snapshots, now)

    def _active_probe_count(self) -> int:
        con = _connect(self.state_db)
        count = int(
            con.execute(
                "select count(*) from carousel_state where state in ('pending','probing')"
            ).fetchone()[0]
        )
        con.close()
        return count

    def confirmed_probe_count(self) -> int:
        con = _connect(self.state_db)
        count = int(con.execute("select count(*) from carousel_state where state='probing'").fetchone()[0])
        con.close()
        return count

    def _mark_probe_pending(self, h: str, now: int) -> None:
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_until,backoff_level,updated_at) values(?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state=excluded.state, probe_started_at=null, "
                "backoff_until=null, updated_at=excluded.updated_at",
                (h, "pending", None, None, None, 0, now),
            )
            self.intent_repository.upsert_in_transaction(
                con,
                SchedulerIntent(
                    "carousel",
                    h,
                    "availability_probe",
                    40,
                    now + int(self.probe_duration_sec),
                    {"phase": "pending"},
                ),
            )

        write_transaction(self.state_db, txn)

    def _mark_soak(self, h: str, now: int) -> None:
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "update carousel_state set state='soak', last_probe_at=?, updated_at=? where hash=?",
                (now, now, h),
            )
            self.intent_repository.delete_in_transaction(con, "carousel", h)

        write_transaction(self.state_db, txn)

    def _mark_dead(self, h: str, now: int, backoff_until: int, backoff_level: int) -> None:
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_until,backoff_level,updated_at) values(?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state='dead', probe_started_at=null, last_probe_at=excluded.last_probe_at, "
                "backoff_until=excluded.backoff_until, backoff_level=excluded.backoff_level, updated_at=excluded.updated_at",
                (h, "dead", None, now, backoff_until, backoff_level, now),
            )
            self.intent_repository.delete_in_transaction(con, "carousel", h)

        write_transaction(self.state_db, txn)

    def _decision(self, h: str, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (int(self.now()), "carousel", h, decision, reason_code, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    def _event(self, level: str, event_type: str, message: str, data: dict[str, Any]) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
                (int(self.now()), level, "carousel", event_type, message, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    def _metrics(self, result: dict[str, Any]) -> None:
        metrics = {
            "dry_run": self.dry_run,
            "live_verify": self.live_verify,
            "configured_concurrency": self.concurrency,
            "effective_concurrency": result.get("effective_concurrency", self._effective_concurrency()),
            "started_count": len(result.get("started") or []),
            "promoted_count": len(result.get("promoted") or []),
            "stopped_count": len(result.get("stopped") or []),
            "active_probes": int(result.get("active_probes") or 0),
            "suspended": bool(result.get("suspended")),
            "reason": result.get("reason"),
        }
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                (int(self.now()), "carousel", json.dumps(redact(metrics), ensure_ascii=False)),
            ),
        )

    @staticmethod
    def _snapshot(snapshots: Mapping[str, Any], h: str) -> dict[str, Any]:
        return _snapshot_dict(snapshots, h)

    @staticmethod
    def _probe_started_completed_bytes(intent_row: Mapping[str, Any] | None) -> int | None:
        if not intent_row:
            return None
        try:
            data = json.loads(str(intent_row.get("data_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(data, dict):
            return None
        if "probe_started_completed_bytes" not in data:
            return None
        try:
            return max(0, int(data["probe_started_completed_bytes"]))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _probe_succeeded(snapshot: Mapping[str, Any], started_completed_bytes: int | None) -> bool:
        """Promote only on complete sources or real download progress.

        Missing completed baselines must not treat pre-existing bytes as probe
        progress.  Leech peers alone never prove finishability.
        """

        if not snapshot:
            return False
        complete_sources = max(
            0,
            int(snapshot.get("num_seeds") or 0),
            int(snapshot.get("num_complete") or 0),
        )
        if complete_sources > 0:
            return True
        try:
            availability = snapshot.get("availability")
            if availability is not None and float(availability) >= 1.0:
                return True
        except (TypeError, ValueError):
            pass
        dlspeed = int(snapshot.get("dlspeed_bps") or snapshot.get("dlspeed") or 0)
        if dlspeed > 0:
            return True
        if started_completed_bytes is None:
            return False
        return _completed_bytes(snapshot) > int(started_completed_bytes)

    @staticmethod
    def _has_swarm(snapshot: Mapping[str, Any]) -> bool:
        """Deprecated peer-or-seed check kept for callers; prefer ``_probe_succeeded``."""

        return int(snapshot.get("num_seeds") or 0) > 0 or int(snapshot.get("num_peers") or 0) > 0
