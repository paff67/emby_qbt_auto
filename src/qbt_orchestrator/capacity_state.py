from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import readonly_connect, write_transaction
from .observability import redact


SCHEDULER_MODES = frozenset({"emergency", "drain", "normal", "explore"})


@dataclass(frozen=True)
class ModeController:
    """Disk-watermark controller with an explicit drain exit hysteresis."""

    emergency_enter: int
    drain_enter: int
    drain_exit: int
    explore_enter: int

    def __post_init__(self) -> None:
        thresholds = (
            int(self.emergency_enter),
            int(self.drain_enter),
            int(self.drain_exit),
            int(self.explore_enter),
        )
        if any(value < 0 for value in thresholds):
            raise ValueError("scheduler mode thresholds must be non-negative")
        if not (thresholds[0] <= thresholds[1] < thresholds[2] <= thresholds[3]):
            raise ValueError("expected emergency_enter <= drain_enter < drain_exit <= explore_enter")

    def next_mode(self, current_mode: str, free_bytes: int) -> str:
        current = str(current_mode or "normal").lower()
        if current not in SCHEDULER_MODES:
            current = "normal"
        free = max(0, int(free_bytes))

        if free < int(self.emergency_enter):
            return "emergency"
        if current in {"emergency", "drain"}:
            if free < int(self.drain_exit):
                return "drain"
            return "explore" if free >= int(self.explore_enter) else "normal"
        if free < int(self.drain_enter):
            return "drain"
        if current == "explore" and free < int(self.explore_enter):
            return "normal"
        if free >= int(self.explore_enter):
            return "explore"
        return "normal"


@dataclass(frozen=True)
class CapacityResult:
    state: str
    reason: str
    # Detection remains side-effect free.  The separately configured dead
    # partial reclaimer consumes the persisted transition behind its own path,
    # age, ownership and dry-run gates.
    actions: list[dict[str, Any]] = field(default_factory=list)


def detect_capacity_state(
    *,
    mode: str,
    managed_incomplete: int,
    feasible_full_finish: int,
    disk_releasing_jobs: int,
    capacity_pressure: bool = False,
) -> CapacityResult:
    if (
        (str(mode) == "drain" or bool(capacity_pressure))
        and int(managed_incomplete) > 0
        and int(feasible_full_finish) == 0
        and int(disk_releasing_jobs) == 0
    ):
        return CapacityResult("capacity_deadlock", "no_finishable_or_releasing_work", actions=[])
    return CapacityResult("progress_possible", "feasible_work_exists", actions=[])


@dataclass(frozen=True)
class CapacityObservation:
    managed_incomplete: int
    viable_finish: int
    nonviable_finish: int
    feasible_full_finish: int
    disk_releasing_jobs: int
    required_minimum_growth_bytes: int
    available_growth_bytes: int
    free_bytes: int
    top_manual_candidates: tuple[dict[str, Any], ...]

    def as_details(self) -> dict[str, Any]:
        return {
            "managed_incomplete": int(self.managed_incomplete),
            "viable_finish": int(self.viable_finish),
            "nonviable_finish": int(self.nonviable_finish),
            "feasible_full_finish": int(self.feasible_full_finish),
            "disk_releasing_jobs": int(self.disk_releasing_jobs),
            "required_minimum_growth_bytes": int(self.required_minimum_growth_bytes),
            "available_growth_bytes": int(self.available_growth_bytes),
            "free_bytes": int(self.free_bytes),
            "top_manual_candidates": [dict(item) for item in self.top_manual_candidates],
        }


def finish_viability(
    torrent: Mapping[str, Any],
    health: Mapping[str, Any] | None,
    *,
    observed_at: int,
    stale_sec: int,
) -> tuple[bool, str]:
    health = health or {}
    seeds = max(
        0,
        int(torrent.get("num_seeds") or 0),
        int(torrent.get("num_complete") or 0),
    )
    dlspeed = max(
        0,
        int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0),
    )
    raw_availability = torrent.get("availability")
    availability = None if raw_availability is None else float(raw_availability)
    no_progress_since = health.get("no_progress_since")
    has_complete_source = seeds > 0 or (
        availability is not None and availability >= 0.999999
    )
    recently_progressing = (
        dlspeed > 0
        or no_progress_since is None
        or int(observed_at) - int(no_progress_since) < max(0, int(stale_sec))
    )
    if has_complete_source:
        return True, "complete_source"
    if recently_progressing:
        return True, "recent_progress"
    return False, "stale_without_complete_source"


def build_capacity_observation(
    snapshots: Mapping[str, Mapping[str, Any]],
    *,
    available_growth_bytes: int,
    selected_hashes: set[str],
    disk_releasing_jobs: int,
    free_bytes: int,
    health_by_hash: Mapping[str, Mapping[str, Any]] | None = None,
    observed_at: int | None = None,
    viability_stale_sec: int = 1_800,
) -> CapacityObservation:
    """Build deterministic aggregate evidence without proposing any action."""

    candidates: list[dict[str, Any]] = []
    health_by_hash = health_by_hash or {}
    now = int(time.time()) if observed_at is None else int(observed_at)
    stale_after = max(0, int(viability_stale_sec))
    for fallback_hash, raw in snapshots.items():
        torrent = dict(raw)
        torrent_hash = str(torrent.get("hash") or fallback_hash)
        tags = {part.strip() for part in str(torrent.get("tags") or "").split(",") if part.strip()}
        managed = (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags
        amount_left = max(0, int(torrent.get("amount_left") or 0))
        if not managed or amount_left <= 0:
            continue
        viable, viability_reason = finish_viability(
            torrent,
            health_by_hash.get(torrent_hash),
            observed_at=now,
            stale_sec=stale_after,
        )
        candidates.append(
            {
                "hash": torrent_hash,
                "required_growth_bytes": amount_left,
                "viable": viable,
                "viability_reason": viability_reason,
            }
        )
    candidates.sort(key=lambda item: (item["required_growth_bytes"], item["hash"]))

    budget = max(0, int(available_growth_bytes))
    selected = {str(item) for item in selected_hashes}
    feasible = sum(
        1
        for candidate in candidates
        if candidate["viable"]
        and (
            candidate["hash"] in selected
            or candidate["required_growth_bytes"] <= budget
        )
    )
    viable_finish = sum(1 for candidate in candidates if candidate["viable"])
    return CapacityObservation(
        managed_incomplete=len(candidates),
        viable_finish=viable_finish,
        nonviable_finish=len(candidates) - viable_finish,
        feasible_full_finish=feasible,
        disk_releasing_jobs=max(0, int(disk_releasing_jobs)),
        required_minimum_growth_bytes=candidates[0]["required_growth_bytes"] if candidates else 0,
        available_growth_bytes=budget,
        free_bytes=max(0, int(free_bytes)),
        top_manual_candidates=tuple(dict(item) for item in candidates[:3]),
    )


@dataclass(frozen=True)
class CapacityTransition:
    scheduler_mode: str
    state: str
    reason: str
    entered_at: int
    last_evaluated_at: int
    details: dict[str, Any]
    transitioned: bool
    previous_state: str | None


class CapacityStateStore:
    """Persist the latest aggregate capacity state as a single durable row."""

    def __init__(self, state_db: str | Path, now: Callable[[], int] | None = None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def current_mode(self, default: str = "normal") -> str:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute("select scheduler_mode from capacity_state where id=1").fetchone()
            mode = str(row["scheduler_mode"] if row else default)
            return mode if mode in SCHEDULER_MODES else str(default)
        finally:
            con.close()

    def persist(
        self,
        scheduler_mode: str,
        result: CapacityResult,
        details: Mapping[str, Any] | None = None,
    ) -> CapacityTransition:
        mode = str(scheduler_mode)
        if mode not in SCHEDULER_MODES:
            raise ValueError(f"unsupported scheduler mode: {mode}")
        now = int(self.now())
        safe_details = dict(redact(dict(details or {})))
        payload = json.dumps(safe_details, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

        def txn(con: sqlite3.Connection) -> CapacityTransition:
            previous = con.execute("select state,entered_at from capacity_state where id=1").fetchone()
            previous_state = str(previous["state"]) if previous else None
            transitioned = previous_state != str(result.state)
            entered_at = now if transitioned or previous is None else int(previous["entered_at"])
            con.execute(
                "insert into capacity_state(id,scheduler_mode,state,entered_at,last_evaluated_at,reason,details_json) "
                "values(1,?,?,?,?,?,?) "
                "on conflict(id) do update set scheduler_mode=excluded.scheduler_mode,state=excluded.state,"
                "entered_at=excluded.entered_at,last_evaluated_at=excluded.last_evaluated_at,"
                "reason=excluded.reason,details_json=excluded.details_json",
                (mode, str(result.state), entered_at, now, str(result.reason), payload),
            )
            return CapacityTransition(
                scheduler_mode=mode,
                state=str(result.state),
                reason=str(result.reason),
                entered_at=entered_at,
                last_evaluated_at=now,
                details=safe_details,
                transitioned=transitioned,
                previous_state=previous_state,
            )

        return write_transaction(self.state_db, txn)
