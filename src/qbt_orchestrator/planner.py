from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .models import LifecycleState
from .observability import redact
from .policies.download_mode import desired_seq_dl


STOPPED_STATES = {"pauseddl", "pausedup", "stoppeddl", "stoppedup", "paused", "stopped"}


@dataclass(frozen=True)
class PlannerResult:
    selected_hashes: list[str]
    paused_hashes: list[str]
    conservative: bool = False
    budget_bytes: int = 0


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


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
        active_slots: int = 2,
        disk_floor_bytes: int = 2 * 1024**3,
        now=None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = dry_run
        self.active_slots = active_slots
        self.disk_floor_bytes = disk_floor_bytes
        self.now = now or (lambda: int(time.time()))

    def plan_and_apply(self, snapshots: Mapping[str, Mapping[str, Any]], free_bytes: int, sync_healthy: bool) -> PlannerResult:
        managed = [dict(t, hash=h if not t.get("hash") else t.get("hash")) for h, t in snapshots.items() if _is_managed(t)]
        previous_allocations = self._allocation_rows()
        dead_hashes = {h for h, row in previous_allocations.items() if str(row.get("desired_state")) == "dead"}
        budget = max(0, int(free_bytes) - self.disk_floor_bytes)
        if not sync_healthy:
            for torrent in managed:
                self._decision(str(torrent["hash"]), "hold", "sync_unhealthy", {"free_bytes": free_bytes})
            return PlannerResult([], [], conservative=True, budget_bytes=budget)

        health_rows = self._health_rows()
        now = int(self.now())
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
        candidates = sorted(
            [t for t in managed if int(t.get("amount_left") or 0) > 0 and str(t.get("hash")) not in dead_hashes],
            key=lambda t: (int(t.get("amount_left") or 0), -int(t.get("num_seeds") or 0), -int(t.get("num_peers") or 0), str(t.get("hash"))),
        )
        selected: list[dict[str, Any]] = []
        used = 0
        for torrent in candidates:
            if str(torrent.get("hash")) in active_slow:
                continue
            amount_left = int(torrent.get("amount_left") or 0)
            if len(selected) >= self.active_slots or used + amount_left > budget:
                continue
            selected.append(torrent)
            used += amount_left

        selected_hashes = [str(t["hash"]) for t in selected]
        selected_set = set(selected_hashes)
        start_hashes = [str(t["hash"]) for t in selected if _is_stopped_download(t)]
        paused_hashes = [str(t["hash"]) for t in managed if str(t["hash"]) not in selected_set and _is_running_download(t)]

        seq_false_hashes: list[str] = []
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
        for torrent in candidates:
            h = str(torrent["hash"])
            if h in selected_set:
                seq = desired_seq_dl(
                    LifecycleState.ACTIVE,
                    int(torrent.get("num_seeds") or 0),
                    int(torrent.get("num_peers") or 0),
                    int(torrent.get("stalled_seconds") or 0),
                )
                self._allocation(h, "active", "stable", int(torrent.get("amount_left") or 0), seq, now, "budget_fit")
                self._decision(h, "active", "budget_fit", {"reserved_bytes": int(torrent.get("amount_left") or 0), "budget_bytes": budget})
            elif h in active_slow:
                self._allocation(h, "soak", "soak", 0, False, now, "active_slow_5min")
                self._decision(h, "soak", "active_slow_5min", {"budget_bytes": budget})
                if self._needs_seq_false(h, previous_allocations):
                    seq_false_hashes.append(h)
            else:
                self._allocation(h, "soak", "soak", 0, False, now, "budget_or_slot_exhausted")
                self._decision(h, "soak", "budget_or_slot_exhausted", {"budget_bytes": budget})
                if self._needs_seq_false(h, previous_allocations):
                    seq_false_hashes.append(h)

        self._update_health(managed, selected_set, now, health_rows, dead_set=auto_dead, previous_allocations=previous_allocations)
        self._force_seq_false(seq_false_hashes)
        self._qbt_post("/api/v2/torrents/start", start_hashes)
        self._qbt_post("/api/v2/torrents/stop", paused_hashes)
        return PlannerResult(selected_hashes, paused_hashes, conservative=False, budget_bytes=budget)

    def _allocation_rows(self) -> dict[str, dict[str, Any]]:
        con = _connect(self.state_db)
        try:
            rows = con.execute("select * from scheduler_allocations").fetchall()
            return {str(r["hash"]): dict(r) for r in rows}
        finally:
            con.close()

    def _needs_seq_false(self, hash: str, previous_allocations: dict[str, dict[str, Any]]) -> bool:
        row = previous_allocations.get(hash)
        return row is None or row.get("desired_seq_dl") is None or int(row.get("desired_seq_dl") or 0) != 0

    def _health_rows(self) -> dict[str, dict[str, Any]]:
        con = _connect(self.state_db)
        try:
            rows = con.execute("select * from torrent_health").fetchall()
            return {str(r["hash"]): dict(r) for r in rows}
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
        return int(now) - int(active_since) >= 90 and int(now) - int(low_speed_since) >= 300 and dlspeed < 100 * 1024

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
        con = _connect(self.state_db)
        try:
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
                    if h in selected_set and prev_alloc_state != "active":
                        low_speed_since = now
                    else:
                        low_speed_since = old.get("low_speed_since") or now
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
                con.execute(
                    "insert into torrent_health(hash,sampled_at,dlspeed_bps,upspeed_bps,completed_bytes,last_completed_bytes,progress,num_seeds,num_peers,last_swarm_seen_at,low_speed_since,no_progress_since,active_since,soak_since,dead_since,updated_at) "
                    "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                    "on conflict(hash) do update set sampled_at=excluded.sampled_at,dlspeed_bps=excluded.dlspeed_bps,upspeed_bps=excluded.upspeed_bps,completed_bytes=excluded.completed_bytes,last_completed_bytes=excluded.last_completed_bytes,progress=excluded.progress,num_seeds=excluded.num_seeds,num_peers=excluded.num_peers,last_swarm_seen_at=excluded.last_swarm_seen_at,low_speed_since=excluded.low_speed_since,no_progress_since=excluded.no_progress_since,active_since=excluded.active_since,soak_since=excluded.soak_since,dead_since=excluded.dead_since,updated_at=excluded.updated_at",
                    (
                        h,
                        now,
                        dlspeed,
                        upspeed,
                        completed,
                        old_completed,
                        progress,
                        seeds,
                        peers,
                        last_swarm_seen_at,
                        low_speed_since,
                        no_progress_since,
                        active_since,
                        soak_since,
                        dead_since,
                        now,
                    ),
                )
            con.commit()
        finally:
            con.close()

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
        if not hashes or not hasattr(self.executor, "set_seq_dl"):
            return
        path = "/api/v2/torrents/toggleSequentialDownload"
        for h in hashes:
            payload = {"hashes": h, "desired": False}
            if self.dry_run:
                self._action(path, payload, "dry_run", True)
                continue
            try:
                changed = bool(self.executor.set_seq_dl(h, False))
                if changed:
                    self._action(path, payload, "succeeded", False)
            except Exception as exc:
                self._action(path, payload, "failed", False, str(exc))
                raise

    def _allocation(self, hash: str, desired_state: str, slot_kind: str, reserved_bytes: int, seq_dl: bool, ts: int, reason: str) -> None:
        con = _connect(self.state_db)
        con.execute(
            "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,priority_score,reserved_bytes,desired_seq_dl,allocated_at,reason) "
            "values(?,?,?,?,?,?,?,?,?) "
            "on conflict(hash) do update set desired_state=excluded.desired_state, applied_state=excluded.applied_state, "
            "slot_kind=excluded.slot_kind, priority_score=excluded.priority_score, reserved_bytes=excluded.reserved_bytes, "
            "desired_seq_dl=excluded.desired_seq_dl, allocated_at=excluded.allocated_at, reason=excluded.reason",
            (hash, desired_state, desired_state, slot_kind, 0, reserved_bytes, 1 if seq_dl else 0, ts, reason),
        )
        con.commit()
        con.close()

    def _decision(self, hash: str, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        con = _connect(self.state_db)
        con.execute(
            "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
            (int(self.now()), "planner", hash, decision, reason_code, json.dumps(redact(data), ensure_ascii=False)),
        )
        con.commit()
        con.close()

    def _action(self, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        con = _connect(self.state_db)
        con.execute(
            "insert into action_log(ts,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?)",
            (int(self.now()), "qbt_post", path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
        )
        con.commit()
        con.close()
