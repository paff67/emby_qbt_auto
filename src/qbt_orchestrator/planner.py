from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .db import readonly_connect, write_transaction
from .decision_recorder import DecisionEntry, DecisionRecorder
from .models import LifecycleState
from .observability import redact
from .policies.download_mode import desired_seq_dl


STOPPED_STATES = {"pauseddl", "pausedup", "stoppeddl", "stoppedup", "paused", "stopped"}

ALLOCATION_UPSERT_SQL = (
    "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,priority_score,reserved_bytes,desired_seq_dl,allocated_at,reason) "
    "values(?,?,?,?,?,?,?,?,?) "
    "on conflict(hash) do update set desired_state=excluded.desired_state, applied_state=excluded.applied_state, "
    "slot_kind=excluded.slot_kind, priority_score=excluded.priority_score, reserved_bytes=excluded.reserved_bytes, "
    "desired_seq_dl=excluded.desired_seq_dl, allocated_at=excluded.allocated_at, reason=excluded.reason"
)

HEALTH_UPSERT_SQL = (
    "insert into torrent_health(hash,sampled_at,dlspeed_bps,upspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,last_swarm_seen_at,low_speed_since,no_progress_since,active_since,soak_since,dead_since,updated_at) "
    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
    "on conflict(hash) do update set sampled_at=excluded.sampled_at,dlspeed_bps=excluded.dlspeed_bps,upspeed_bps=excluded.upspeed_bps,completed_bytes=excluded.completed_bytes,last_completed_bytes=excluded.last_completed_bytes,progress=excluded.progress,num_seeds=excluded.num_seeds,num_peers=excluded.num_peers,last_swarm_seen_at=excluded.last_swarm_seen_at,low_speed_since=excluded.low_speed_since,no_progress_since=excluded.no_progress_since,active_since=excluded.active_since,soak_since=excluded.soak_since,dead_since=excluded.dead_since,updated_at=excluded.updated_at"
)

@dataclass(frozen=True)
class PlannerResult:
    selected_hashes: list[str]
    paused_hashes: list[str]
    conservative: bool = False
    budget_bytes: int = 0
    mode: str = "normal"


@dataclass
class PlannerPersistenceBatch:
    """All durable state mutations produced by one planner calculation."""

    allocations: list[dict[str, Any]] = field(default_factory=list)
    health_rows: list[dict[str, Any]] = field(default_factory=list)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    soak_cooldowns: list[dict[str, Any]] = field(default_factory=list)
    soak_residents: list[dict[str, Any]] = field(default_factory=list)
    reservation_sync: tuple[dict[str, int], set[str], int] | None = None


def _connect(path: str | Path) -> sqlite3.Connection:
    return readonly_connect(path)


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    raw = str(torrent.get("tags") or "")
    return {p.strip() for p in raw.split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _is_running_download(torrent: Mapping[str, Any]) -> bool:
    state = str(torrent.get("state") or "").lower()
    return state not in STOPPED_STATES and float(torrent.get("progress") or 0) < 1.0 and int(torrent.get("amount_left") or 0) > 0


def _is_stopped_download(torrent: Mapping[str, Any]) -> bool:
    return str(torrent.get("state") or "").lower() in STOPPED_STATES


class DownloadPlanner:
    """15s planner loop: desired download state + safe qBT action coalescing."""

    def __init__(
        self,
        state_db: str | Path,
        executor,
        dry_run: bool = True,
        active_slots: int = 5,
        disk_floor_bytes: int = 3 * 1024**3,
        slow_active_demote_sec: int = 180,
        recovery_enabled: bool = False,
        recovery_enter_bytes: int | None = None,
        emergency_floor_bytes: int | None = None,
        recovery_margin_bytes: int = 256 * 1024**2,
        recovery_active_slots: int = 4,
        recovery_max_remaining_bytes: int = int(1.5 * 1024**3),
        now=None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = dry_run
        self.active_slots = int(active_slots)
        self.disk_floor_bytes = int(disk_floor_bytes)
        self.slow_active_demote_sec = int(slow_active_demote_sec)
        self.recovery_enabled = bool(recovery_enabled)
        self.recovery_enter_bytes = int(recovery_enter_bytes if recovery_enter_bytes is not None else self.disk_floor_bytes)
        self.emergency_floor_bytes = int(emergency_floor_bytes if emergency_floor_bytes is not None else min(2 * 1024**3, max(0, self.disk_floor_bytes)))
        self.recovery_margin_bytes = int(recovery_margin_bytes)
        self.recovery_active_slots = int(recovery_active_slots)
        self.recovery_max_remaining_bytes = int(recovery_max_remaining_bytes)
        self.now = now or (lambda: int(time.time()))
        self.decision_recorder = DecisionRecorder(self.state_db, now=self.now)
        self._pending_persistence: PlannerPersistenceBatch | None = None

    def plan_and_apply(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        free_bytes: int,
        sync_healthy: bool,
        protected_running_hashes: set[str] | None = None,
        forced_active_hashes: set[str] | None = None,
        cooldown_hashes: set[str] | None = None,
        external_reserved_bytes: int = 0,
    ) -> PlannerResult:
        if self._pending_persistence is not None:
            raise RuntimeError("planner persistence batch is already active")
        self._pending_persistence = PlannerPersistenceBatch()
        try:
            return self._plan_and_apply_impl(
                snapshots,
                free_bytes,
                sync_healthy,
                protected_running_hashes=protected_running_hashes,
                forced_active_hashes=forced_active_hashes,
                cooldown_hashes=cooldown_hashes,
                external_reserved_bytes=external_reserved_bytes,
            )
        finally:
            self._pending_persistence = None

    def _plan_and_apply_impl(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        free_bytes: int,
        sync_healthy: bool,
        protected_running_hashes: set[str] | None = None,
        forced_active_hashes: set[str] | None = None,
        cooldown_hashes: set[str] | None = None,
        external_reserved_bytes: int = 0,
    ) -> PlannerResult:
        protected_running_hashes = {str(h) for h in (protected_running_hashes or set())}
        forced_active_hashes = {str(h) for h in (forced_active_hashes or set())}
        cooldown_hashes = {str(h) for h in (cooldown_hashes or set())}
        managed = [dict(t, hash=h if not t.get("hash") else t.get("hash")) for h, t in snapshots.items() if _is_managed(t)]
        now = int(self.now())
        previous_allocations = self._allocation_rows()
        dead_hashes = {h for h, row in previous_allocations.items() if str(row.get("desired_state")) == "dead"}
        active_reservations = self._active_reservation_bytes(now, ignored_kinds={"soak_probe"} if int(external_reserved_bytes or 0) > 0 else set())
        mode = self._mode_for_free_bytes(int(free_bytes))
        budget_floor = self.disk_floor_bytes
        extra_margin = 0
        slot_limit = self.active_slots
        if mode == "recovery":
            budget_floor = self.emergency_floor_bytes
            extra_margin = self.recovery_margin_bytes
            slot_limit = self.recovery_active_slots
        elif mode == "emergency":
            budget_floor = self.emergency_floor_bytes
            extra_margin = 0
            slot_limit = 0
        budget = max(0, int(free_bytes) - int(budget_floor) - int(extra_margin) - sum(active_reservations.values()) - int(external_reserved_bytes or 0))
        if not sync_healthy:
            for torrent in managed:
                self._decision(str(torrent["hash"]), "hold", "sync_unhealthy", {"free_bytes": free_bytes})
            self._flush_persistence_batch()
            return PlannerResult([], [], conservative=True, budget_bytes=budget, mode=mode)
        if not self.dry_run:
            self._reconcile_absent_allocations(set(str(h) for h in snapshots.keys()), now)
            previous_allocations = self._allocation_rows()
            dead_hashes = {h for h, row in previous_allocations.items() if str(row.get("desired_state")) == "dead"}
            active_reservations = self._active_reservation_bytes(now, ignored_kinds={"soak_probe"} if int(external_reserved_bytes or 0) > 0 else set())
            budget = max(0, int(free_bytes) - int(budget_floor) - int(extra_margin) - sum(active_reservations.values()) - int(external_reserved_bytes or 0))

        health_rows = self._health_rows()
        auto_dead = {
            str(t["hash"])
            for t in managed
            if self._should_mark_dead(str(t["hash"]), t, health_rows.get(str(t["hash"])), previous_allocations.get(str(t["hash"])), now)
        }
        dead_hashes |= auto_dead
        active_slow = {
            str(t["hash"])
            for t in managed
            if str(t["hash"]) not in auto_dead
            and str((previous_allocations.get(str(t["hash"])) or {}).get("desired_state") or "") == "active"
            and self._should_demote_active(str(t["hash"]), t, health_rows.get(str(t["hash"])), now)
        }
        cooldown_hashes |= active_slow
        candidates, skipped_reasons = self._candidate_lists(
            managed,
            mode=mode,
            dead_hashes=dead_hashes,
            cooldown_hashes=cooldown_hashes,
            protected_running_hashes=protected_running_hashes,
            forced_active_hashes=forced_active_hashes,
        )
        selected: list[dict[str, Any]] = []
        used = 0
        forced_candidates = [t for t in candidates if str(t.get("hash")) in forced_active_hashes]
        regular_candidates = [t for t in candidates if str(t.get("hash")) not in forced_active_hashes]
        for torrent in forced_candidates:
            amount_left = int(torrent.get("amount_left") or 0)
            incremental_reserved_bytes = max(0, amount_left - int(active_reservations.get(str(torrent.get("hash")), 0)))
            if len(selected) >= slot_limit or used + incremental_reserved_bytes > budget:
                continue
            selected.append(torrent)
            used += incremental_reserved_bytes
        for torrent in regular_candidates:
            amount_left = int(torrent.get("amount_left") or 0)
            incremental_reserved_bytes = max(0, amount_left - int(active_reservations.get(str(torrent.get("hash")), 0)))
            if len(selected) >= slot_limit or used + incremental_reserved_bytes > budget:
                continue
            selected.append(torrent)
            used += incremental_reserved_bytes

        selected_hashes = [str(t["hash"]) for t in selected]
        selected_set = set(selected_hashes)
        start_hashes = [str(t["hash"]) for t in selected if _is_stopped_download(t)]
        recovery_keep_running_slow = set(active_slow) if mode == "recovery" else set()
        paused_hashes = [
            str(t["hash"])
            for t in managed
            if str(t["hash"]) not in selected_set
            and str(t["hash"]) not in protected_running_hashes
            and str(t["hash"]) not in recovery_keep_running_slow
            and _is_running_download(t)
        ]

        seq_desired_actions: list[tuple[str, bool]] = []
        for torrent in managed:
            h = str(torrent["hash"])
            if h in auto_dead:
                self._allocation(h, "dead", "dead", 0, False, now, "health_no_swarm_no_progress")
                self._decision(
                    h,
                    "dead",
                    "health_no_swarm_no_progress",
                    {"last_swarm_seen_at": (health_rows.get(h) or {}).get("last_swarm_seen_at"), "no_progress_since": (health_rows.get(h) or {}).get("no_progress_since")},
                )
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))
            elif h in cooldown_hashes:
                reason = "active_slow_3min" if h in active_slow else "cooldown"
                if h in recovery_keep_running_slow:
                    reason = "active_slow_3min_recovery_soak"
                desired = "soak" if h in active_slow else "soak_cooldown"
                self._allocation(h, desired, desired, 0, False, now, reason)
                self._decision(h, desired, reason, {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0)})
                if h in recovery_keep_running_slow:
                    self._mark_soak_resident(h, now, "recovery_active_slow")
                elif h in active_slow:
                    self._mark_soak_cooldown(h, now, "cooldown_active_slow")
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))
        for torrent in candidates:
            h = str(torrent["hash"])
            if h in selected_set:
                reason = "recovery_budget_fit" if mode == "recovery" else "budget_fit"
                seq = desired_seq_dl(
                    LifecycleState.ACTIVE,
                    int(torrent.get("num_seeds") or 0),
                    int(torrent.get("num_peers") or 0),
                    int(torrent.get("stalled_seconds") or 0),
                )
                self._allocation(h, "active", "stable", int(torrent.get("amount_left") or 0), seq, now, reason)
                self._decision(h, "active", reason, {"reserved_bytes": int(torrent.get("amount_left") or 0), "budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0), "mode": mode})
                if seq and self._needs_seq_desired(h, True, previous_allocations):
                    seq_desired_actions.append((h, True))
            elif h in active_slow:
                self._allocation(h, "soak", "soak", 0, False, now, "active_slow_3min")
                self._decision(h, "soak", "active_slow_3min", {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0)})
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))
            else:
                reason = "recovery_budget_or_slot_exhausted" if mode == "recovery" else "budget_or_slot_exhausted"
                self._allocation(h, "soak", "soak", 0, False, now, reason)
                self._decision(h, "soak", reason, {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0), "mode": mode})
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))
        for torrent in managed:
            h = str(torrent["hash"])
            reason = skipped_reasons.get(h)
            if not reason or h in selected_set or h in dead_hashes or h in cooldown_hashes:
                continue
            self._allocation(h, "soak", "soak", 0, False, now, reason)
            self._decision(h, "soak", reason, {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0), "mode": mode, "amount_left": int(torrent.get("amount_left") or 0)})
            if self._needs_seq_desired(h, False, previous_allocations):
                seq_desired_actions.append((h, False))

        self._update_health(managed, selected_set, now, health_rows, dead_set=auto_dead, previous_allocations=previous_allocations)
        if not self.dry_run:
            self._sync_active_download_reservations(selected, {str(t["hash"]) for t in managed}, now)
        self._flush_persistence_batch()
        self._apply_seq_desired(seq_desired_actions)
        self._qbt_post("/api/v2/torrents/start", start_hashes)
        self._qbt_post("/api/v2/torrents/stop", paused_hashes)
        return PlannerResult(selected_hashes, paused_hashes, conservative=False, budget_bytes=budget, mode=mode)

    def _mode_for_free_bytes(self, free_bytes: int) -> str:
        if int(free_bytes) < int(self.emergency_floor_bytes):
            return "emergency"
        if self.recovery_enabled and int(free_bytes) < int(self.recovery_enter_bytes):
            return "recovery"
        return "normal"

    def _candidate_lists(
        self,
        managed: list[Mapping[str, Any]],
        mode: str,
        dead_hashes: set[str],
        cooldown_hashes: set[str],
        protected_running_hashes: set[str],
        forced_active_hashes: set[str],
    ) -> tuple[list[dict[str, Any]], dict[str, str]]:
        candidates: list[dict[str, Any]] = []
        skipped: dict[str, str] = {}
        for torrent in managed:
            h = str(torrent.get("hash") or "")
            amount_left = int(torrent.get("amount_left") or 0)
            if amount_left <= 0 or h in dead_hashes or h in cooldown_hashes:
                continue
            if h in protected_running_hashes and h not in forced_active_hashes:
                skipped[h] = "protected_running"
                continue
            if mode == "recovery" and amount_left > int(self.recovery_max_remaining_bytes):
                skipped[h] = "recovery_remaining_too_large"
                continue
            candidates.append(dict(torrent))
        if mode == "recovery":
            candidates.sort(key=self._recovery_sort_key)
        else:
            candidates.sort(key=lambda t: (int(t.get("amount_left") or 0), -int(t.get("num_seeds") or 0), -int(t.get("num_peers") or 0), str(t.get("hash"))))
        return candidates, skipped

    def _recovery_sort_key(self, torrent: Mapping[str, Any]) -> tuple[Any, ...]:
        progress = float(torrent.get("progress") or 0)
        amount_left = int(torrent.get("amount_left") or 0)
        completed = int(torrent.get("completed_bytes") or torrent.get("completed") or torrent.get("downloaded") or 0)
        if completed <= 0:
            size = int(torrent.get("size") or 0)
            completed = max(0, size - amount_left)
        seeds = int(torrent.get("num_seeds") or torrent.get("num_complete") or 0)
        peers = int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0)
        return (-progress, amount_left, -completed, -seeds, -peers, str(torrent.get("hash") or ""))

    def _allocation_rows(self) -> dict[str, dict[str, Any]]:
        con = _connect(self.state_db)
        try:
            rows = con.execute("select * from scheduler_allocations").fetchall()
            return {str(r["hash"]): dict(r) for r in rows}
        finally:
            con.close()

    def _reconcile_absent_allocations(self, snapshot_hashes: set[str], now: int) -> list[str]:
        snapshot_hashes = {str(h) for h in snapshot_hashes if str(h)}

        def txn(con: sqlite3.Connection) -> list[str]:
            rows = [str(r["hash"]) for r in con.execute("select hash from scheduler_allocations").fetchall()]
            absent = sorted(h for h in rows if h and h not in snapshot_hashes)
            if not absent:
                return []
            placeholders = ",".join("?" for _ in absent)
            con.execute(f"delete from scheduler_allocations where hash in ({placeholders})", absent)
            con.execute(
                f"update resource_reservations set state='released', released_at=?, reason=? "
                f"where state='active' and hash in ({placeholders})",
                (int(now), "qbt_absent_reconciled", *absent),
            )
            con.execute(f"delete from soak_state where hash in ({placeholders})", absent)
            self.decision_recorder.record_many_in_transaction(
                con,
                [
                    DecisionEntry(
                        "planner",
                        h,
                        "allocation_reconciled",
                        "qbt_absent",
                        {"released_reservations": True},
                    )
                    for h in absent
                ],
                ts=now,
            )
            return absent

        return list(write_transaction(self.state_db, txn) or [])

    def _needs_seq_false(self, hash: str, previous_allocations: dict[str, dict[str, Any]]) -> bool:
        return self._needs_seq_desired(hash, False, previous_allocations)

    def _needs_seq_desired(self, hash: str, desired: bool, previous_allocations: dict[str, dict[str, Any]]) -> bool:
        row = previous_allocations.get(hash)
        if row is None or row.get("desired_seq_dl") is None:
            return True
        return int(row.get("desired_seq_dl") or 0) != (1 if desired else 0)

    def _health_rows(self) -> dict[str, dict[str, Any]]:
        con = _connect(self.state_db)
        try:
            rows = con.execute("select * from torrent_health").fetchall()
            return {str(r["hash"]): dict(r) for r in rows}
        finally:
            con.close()

    def _active_reservation_bytes(self, now: int, ignored_kinds: set[str] | None = None) -> dict[str, int]:
        ignored_kinds = ignored_kinds or set()
        con = _connect(self.state_db)
        try:
            rows = con.execute(
                "select id,hash,kind,bytes from resource_reservations "
                "where state='active' and (expires_at is null or expires_at>?)",
                (int(now),),
            ).fetchall()
            grouped: dict[str, dict[str, int]] = {}
            for row in rows:
                key = str(row["hash"] or f"{row['kind']}:{row['id']}")
                bucket = grouped.setdefault(key, {"active_download": 0, "batch": 0, "other": 0})
                kind = str(row["kind"] or "")
                if kind in ignored_kinds:
                    continue
                if kind == "active_download":
                    bucket["active_download"] += int(row["bytes"] or 0)
                elif kind == "batch":
                    bucket["batch"] += int(row["bytes"] or 0)
                else:
                    bucket["other"] += int(row["bytes"] or 0)
            out: dict[str, int] = {}
            for key, bucket in grouped.items():
                out[key] = max(bucket["active_download"], bucket["batch"]) + bucket["other"]
            return out
        finally:
            con.close()

    def _should_demote_active(self, hash: str, torrent: Mapping[str, Any], health: dict[str, Any] | None, now: int) -> bool:
        if not health:
            return False
        active_since = health.get("active_since")
        low_speed_since = health.get("low_speed_since")
        if active_since is None or low_speed_since is None:
            return False
        dlspeed = int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or health.get("dlspeed_bps") or 0)
        return int(now) - int(active_since) >= 90 and int(now) - int(low_speed_since) >= self.slow_active_demote_sec and dlspeed < 100 * 1024

    def _should_mark_dead(
        self,
        hash: str,
        torrent: Mapping[str, Any],
        health: dict[str, Any] | None,
        allocation: dict[str, Any] | None,
        now: int,
    ) -> bool:
        if not health or not allocation:
            return False
        if str(allocation.get("desired_state") or "") not in {"soak", "dead"}:
            return False
        if int(torrent.get("amount_left") or 0) <= 0:
            return False
        if int(torrent.get("num_seeds") or torrent.get("num_complete") or 0) > 0:
            return False
        if int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0) > 0:
            return False
        last_swarm_seen_at = health.get("last_swarm_seen_at")
        no_progress_since = health.get("no_progress_since")
        if last_swarm_seen_at is None or no_progress_since is None:
            return False
        return int(now) - int(last_swarm_seen_at) >= 3600 and int(now) - int(no_progress_since) >= 3600

    def _update_health(
        self,
        torrents: list[Mapping[str, Any]],
        selected_set: set[str],
        now: int,
        previous: dict[str, dict[str, Any]],
        dead_set: set[str] | None = None,
        previous_allocations: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        dead_set = dead_set or set()
        previous_allocations = previous_allocations or {}
        rows: list[dict[str, Any]] = []
        for torrent in torrents:
            h = str(torrent.get("hash") or "")
            if not h:
                continue
            old = previous.get(h) or {}
            dlspeed = int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0)
            upspeed = int(torrent.get("upspeed_bps") or torrent.get("upspeed") or 0)
            completed = int(torrent.get("completed_bytes") or torrent.get("completed") or torrent.get("downloaded") or 0)
            progress = float(torrent.get("progress") or 0)
            seeds = int(torrent.get("num_seeds") or torrent.get("num_complete") or 0)
            peers = int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0)
            prev_alloc_state = str((previous_allocations.get(h) or {}).get("desired_state") or "")
            if dlspeed < 100 * 1024:
                low_speed_since = now if h in selected_set and prev_alloc_state != "active" else old.get("low_speed_since") or now
            else:
                low_speed_since = None
            old_completed = int(old.get("completed_bytes") or 0)
            old_progress = float(old.get("progress") or 0)
            no_progress_since = old.get("no_progress_since") if (completed <= old_completed and progress <= old_progress and old) else None
            if old and completed <= old_completed and progress <= old_progress and no_progress_since is None:
                no_progress_since = now
            last_swarm_seen_at = now if (seeds > 0 or peers > 0) else old.get("last_swarm_seen_at")
            if h in selected_set:
                active_since = old.get("active_since") if prev_alloc_state == "active" else now
                if active_since is None:
                    active_since = now
                soak_since = None
                dead_since = None
            elif h in dead_set:
                active_since = None
                soak_since = None
                dead_since = old.get("dead_since") or now
            else:
                active_since = None
                soak_since = old.get("soak_since") or now
                dead_since = old.get("dead_since")
            rows.append(
                {
                    "hash": h,
                    "now": int(now),
                    "dlspeed": dlspeed,
                    "upspeed": upspeed,
                    "completed": completed,
                    "old_completed": old_completed,
                    "progress": progress,
                    "seeds": seeds,
                    "peers": peers,
                    "last_swarm_seen_at": last_swarm_seen_at,
                    "low_speed_since": low_speed_since,
                    "no_progress_since": no_progress_since,
                    "active_since": active_since,
                    "soak_since": soak_since,
                    "dead_since": dead_since,
                }
            )

        if self._pending_persistence is not None:
            self._pending_persistence.health_rows.extend(rows)
            return
        write_transaction(
            self.state_db,
            lambda con: con.executemany(HEALTH_UPSERT_SQL, [self._health_params(row) for row in rows]),
        )

    def _qbt_post(self, path: str, hashes: list[str]) -> None:
        if not hashes:
            return
        payload = {"hashes": "|".join(hashes)}
        if self.dry_run:
            self._action(path, payload, "dry_run", True)
            return
        try:
            self.executor.qbt_post(path, payload)
            self._action(path, payload, "succeeded", False)
        except Exception as exc:
            self._action(path, payload, "failed", False, str(exc))
            raise

    def _force_seq_false(self, hashes: list[str]) -> None:
        self._apply_seq_desired([(h, False) for h in hashes])

    def _apply_seq_desired(self, desired_actions: list[tuple[str, bool]]) -> None:
        if not desired_actions or not hasattr(self.executor, "set_seq_dl"):
            return
        seen: set[tuple[str, bool]] = set()
        deduped: list[tuple[str, bool]] = []
        for h, desired in desired_actions:
            item = (h, bool(desired))
            if item not in seen:
                seen.add(item)
                deduped.append(item)
        if not deduped:
            return
        path = "/api/v2/torrents/toggleSequentialDownload"
        for h, desired in deduped:
            payload = {"hashes": h, "desired": bool(desired)}
            if self.dry_run:
                self._action(path, payload, "dry_run", True)
                continue
            try:
                changed = bool(self.executor.set_seq_dl(h, bool(desired)))
                if changed:
                    self._action(path, payload, "succeeded", False)
            except Exception as exc:
                self._action(path, payload, "failed", False, str(exc))
                raise

    @staticmethod
    def _allocation_params(row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            row["hash"],
            row["desired_state"],
            row["desired_state"],
            row["slot_kind"],
            0,
            int(row["reserved_bytes"]),
            1 if row["seq_dl"] else 0,
            int(row["ts"]),
            row["reason"],
        )

    @staticmethod
    def _health_params(row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            row["hash"],
            int(row["now"]),
            int(row["dlspeed"]),
            int(row["upspeed"]),
            int(row["completed"]),
            int(row["old_completed"]),
            float(row["progress"]),
            int(row["seeds"]),
            int(row["peers"]),
            row["last_swarm_seen_at"],
            row["low_speed_since"],
            row["no_progress_since"],
            row["active_since"],
            row["soak_since"],
            row["dead_since"],
            int(row["now"]),
        )

    def _flush_persistence_batch(self) -> None:
        batch = self._pending_persistence
        if batch is None:
            return
        self._pending_persistence = None
        self._persist_planner_batch(batch)

    def _persist_planner_batch(self, batch: PlannerPersistenceBatch) -> None:
        def txn(con: sqlite3.Connection) -> None:
            if batch.allocations:
                con.executemany(ALLOCATION_UPSERT_SQL, [self._allocation_params(row) for row in batch.allocations])
            if batch.soak_cooldowns:
                con.executemany(
                    "insert into soak_state(hash,state,ema_dlspeed_bps,cooldown_until,last_stopped_at,exposure_bytes,last_sample_at,updated_at,reason) "
                    "values(?,?,?,?,?,?,?,?,?) "
                    "on conflict(hash) do update set state=excluded.state,cooldown_until=excluded.cooldown_until,last_stopped_at=excluded.last_stopped_at,exposure_bytes=excluded.exposure_bytes,updated_at=excluded.updated_at,reason=excluded.reason",
                    [
                        (
                            row["hash"],
                            "soak_cooldown",
                            0,
                            int(row["cooldown_until"]),
                            int(row["now"]),
                            0,
                            int(row["now"]),
                            int(row["now"]),
                            row["reason"],
                        )
                        for row in batch.soak_cooldowns
                    ],
                )
            if batch.soak_residents:
                con.executemany(
                    "insert into soak_state(hash,state,ema_dlspeed_bps,resident_since,cooldown_until,last_started_at,exposure_bytes,last_sample_at,updated_at,reason) "
                    "values(?,?,?,?,?,?,?,?,?,?) "
                    "on conflict(hash) do update set state=excluded.state,resident_since=coalesce(soak_state.resident_since,excluded.resident_since),cooldown_until=null,last_started_at=coalesce(soak_state.last_started_at,excluded.last_started_at),exposure_bytes=excluded.exposure_bytes,last_sample_at=excluded.last_sample_at,updated_at=excluded.updated_at,reason=excluded.reason",
                    [
                        (
                            row["hash"],
                            "soak_resident",
                            0,
                            int(row["now"]),
                            None,
                            int(row["now"]),
                            0,
                            int(row["now"]),
                            int(row["now"]),
                            row["reason"],
                        )
                        for row in batch.soak_residents
                    ],
                )
            if batch.health_rows:
                con.executemany(HEALTH_UPSERT_SQL, [self._health_params(row) for row in batch.health_rows])
            if batch.reservation_sync is not None:
                selected_by_hash, managed_hashes, now = batch.reservation_sync
                self._apply_active_download_reservations(con, selected_by_hash, managed_hashes, now)
            if batch.decisions:
                self.decision_recorder.record_many_in_transaction(
                    con,
                    [
                        DecisionEntry(
                            "planner",
                            row["hash"],
                            row["decision"],
                            row["reason_code"],
                            row["data"],
                            ts=int(row["ts"]),
                        )
                        for row in batch.decisions
                    ],
                )

        write_transaction(self.state_db, txn)

    def _allocation(self, hash: str, desired_state: str, slot_kind: str, reserved_bytes: int, seq_dl: bool, ts: int, reason: str) -> None:
        if self._pending_persistence is not None:
            self._pending_persistence.allocations.append(
                {
                    "hash": hash,
                    "desired_state": desired_state,
                    "slot_kind": slot_kind,
                    "reserved_bytes": int(reserved_bytes),
                    "seq_dl": bool(seq_dl),
                    "ts": int(ts),
                    "reason": reason,
                }
            )
            return
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                ALLOCATION_UPSERT_SQL,
                (hash, desired_state, desired_state, slot_kind, 0, reserved_bytes, 1 if seq_dl else 0, ts, reason),
            ),
        )

    def _sync_active_download_reservations(self, selected: list[Mapping[str, Any]], managed_hashes: set[str], now: int) -> None:
        selected_by_hash = {str(t.get("hash")): int(t.get("amount_left") or 0) for t in selected if str(t.get("hash") or "")}
        if self._pending_persistence is not None:
            self._pending_persistence.reservation_sync = (selected_by_hash, set(managed_hashes), int(now))
            return

        def txn(con: sqlite3.Connection) -> None:
            self._apply_active_download_reservations(con, selected_by_hash, set(managed_hashes), int(now))

        write_transaction(self.state_db, txn)

    @staticmethod
    def _apply_active_download_reservations(
        con: sqlite3.Connection,
        selected_by_hash: dict[str, int],
        managed_hashes: set[str],
        now: int,
    ) -> None:
        selected_hashes = set(selected_by_hash)
        active_rows = [
            dict(row)
            for row in con.execute(
                "select id,hash from resource_reservations where kind='active_download' and state='active'"
            ).fetchall()
        ]
        active_by_hash: dict[str, list[int]] = {}
        for row in active_rows:
            active_by_hash.setdefault(str(row["hash"] or ""), []).append(int(row["id"]))

        for torrent_hash in sorted(managed_hashes - selected_hashes):
            con.execute(
                "update resource_reservations set state='released', released_at=?, reason=? "
                "where kind='active_download' and state='active' and hash=?",
                (int(now), "planner_reallocated", torrent_hash),
            )

        for torrent_hash, bytes_reserved in selected_by_hash.items():
            existing_ids = active_by_hash.get(torrent_hash) or []
            keep_id = existing_ids[0] if existing_ids else None
            expires_at = int(now) + 120
            if keep_id is None:
                con.execute(
                    "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
                    (torrent_hash, "active_download", int(bytes_reserved), "active", int(now), expires_at, "planner_active_download"),
                )
            else:
                con.execute(
                    "update resource_reservations set bytes=?, expires_at=?, released_at=null, reason=? where id=?",
                    (int(bytes_reserved), expires_at, "planner_active_download", keep_id),
                )
                if len(existing_ids) > 1:
                    placeholders = ",".join("?" for _ in existing_ids[1:])
                    con.execute(
                        f"update resource_reservations set state='released', released_at=?, reason=? where id in ({placeholders})",
                        (int(now), "planner_duplicate_released", *existing_ids[1:]),
                    )

    def _mark_soak_cooldown(self, hash: str, now: int, reason: str) -> None:
        cooldown_until = int(now) + 1800
        if self._pending_persistence is not None:
            self._pending_persistence.soak_cooldowns.append(
                {"hash": hash, "now": int(now), "cooldown_until": cooldown_until, "reason": reason}
            )
            return
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into soak_state(hash,state,ema_dlspeed_bps,cooldown_until,last_stopped_at,exposure_bytes,last_sample_at,updated_at,reason) "
                "values(?,?,?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state=excluded.state,cooldown_until=excluded.cooldown_until,last_stopped_at=excluded.last_stopped_at,exposure_bytes=excluded.exposure_bytes,updated_at=excluded.updated_at,reason=excluded.reason",
                (hash, "soak_cooldown", 0, cooldown_until, now, 0, now, now, reason),
            ),
        )

    def _mark_soak_resident(self, hash: str, now: int, reason: str) -> None:
        if self._pending_persistence is not None:
            self._pending_persistence.soak_residents.append(
                {"hash": hash, "now": int(now), "reason": reason}
            )
            return
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into soak_state(hash,state,ema_dlspeed_bps,resident_since,cooldown_until,last_started_at,exposure_bytes,last_sample_at,updated_at,reason) "
                "values(?,?,?,?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state=excluded.state,resident_since=coalesce(soak_state.resident_since,excluded.resident_since),cooldown_until=null,last_started_at=coalesce(soak_state.last_started_at,excluded.last_started_at),exposure_bytes=excluded.exposure_bytes,last_sample_at=excluded.last_sample_at,updated_at=excluded.updated_at,reason=excluded.reason",
                (hash, "soak_resident", 0, now, None, now, 0, now, now, reason),
            ),
        )

    def _decision(self, hash: str, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        if self._pending_persistence is not None:
            self._pending_persistence.decisions.append(
                {
                    "ts": int(self.now()),
                    "hash": hash,
                    "decision": decision,
                    "reason_code": reason_code,
                    "data": dict(data),
                }
            )
            return
        self.decision_recorder.record("planner", hash, decision, reason_code, data)

    def _action(self, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?)",
                (int(self.now()), "qbt_post", path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
            ),
        )
