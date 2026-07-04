from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .db import readonly_connect, write_transaction
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
        disk_floor_bytes: int = 2 * 1024**3,
        slow_active_demote_sec: int = 180,
        now=None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = dry_run
        self.active_slots = int(active_slots)
        self.disk_floor_bytes = disk_floor_bytes
        self.slow_active_demote_sec = int(slow_active_demote_sec)
        self.now = now or (lambda: int(time.time()))

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
        protected_running_hashes = {str(h) for h in (protected_running_hashes or set())}
        forced_active_hashes = {str(h) for h in (forced_active_hashes or set())}
        cooldown_hashes = {str(h) for h in (cooldown_hashes or set())}
        managed = [dict(t, hash=h if not t.get("hash") else t.get("hash")) for h, t in snapshots.items() if _is_managed(t)]
        previous_allocations = self._allocation_rows()
        dead_hashes = {h for h, row in previous_allocations.items() if str(row.get("desired_state")) == "dead"}
        now = int(self.now())
        active_reservations = self._active_reservation_bytes(now, ignored_kinds={"soak_probe"} if int(external_reserved_bytes or 0) > 0 else set())
        budget = max(0, int(free_bytes) - self.disk_floor_bytes - sum(active_reservations.values()) - int(external_reserved_bytes or 0))
        if not sync_healthy:
            for torrent in managed:
                self._decision(str(torrent["hash"]), "hold", "sync_unhealthy", {"free_bytes": free_bytes})
            return PlannerResult([], [], conservative=True, budget_bytes=budget)

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
        candidates = sorted(
            [
                t
                for t in managed
                if int(t.get("amount_left") or 0) > 0
                and str(t.get("hash")) not in dead_hashes
                and str(t.get("hash")) not in cooldown_hashes
                and (str(t.get("hash")) not in protected_running_hashes or str(t.get("hash")) in forced_active_hashes)
            ],
            key=lambda t: (int(t.get("amount_left") or 0), -int(t.get("num_seeds") or 0), -int(t.get("num_peers") or 0), str(t.get("hash"))),
        )
        selected: list[dict[str, Any]] = []
        used = 0
        forced_candidates = [t for t in candidates if str(t.get("hash")) in forced_active_hashes]
        regular_candidates = [t for t in candidates if str(t.get("hash")) not in forced_active_hashes]
        for torrent in forced_candidates:
            amount_left = int(torrent.get("amount_left") or 0)
            incremental_reserved_bytes = max(0, amount_left - int(active_reservations.get(str(torrent.get("hash")), 0)))
            if len(selected) >= self.active_slots or used + incremental_reserved_bytes > budget:
                continue
            selected.append(torrent)
            used += incremental_reserved_bytes
        for torrent in regular_candidates:
            amount_left = int(torrent.get("amount_left") or 0)
            incremental_reserved_bytes = max(0, amount_left - int(active_reservations.get(str(torrent.get("hash")), 0)))
            if len(selected) >= self.active_slots or used + incremental_reserved_bytes > budget:
                continue
            selected.append(torrent)
            used += incremental_reserved_bytes

        selected_hashes = [str(t["hash"]) for t in selected]
        selected_set = set(selected_hashes)
        start_hashes = [str(t["hash"]) for t in selected if _is_stopped_download(t)]
        paused_hashes = [
            str(t["hash"])
            for t in managed
            if str(t["hash"]) not in selected_set
            and str(t["hash"]) not in protected_running_hashes
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
                desired = "soak" if h in active_slow else "soak_cooldown"
                self._allocation(h, desired, desired, 0, False, now, reason)
                self._decision(h, desired, reason, {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0)})
                if h in active_slow:
                    self._mark_soak_cooldown(h, now, "cooldown_active_slow")
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))
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
                self._decision(h, "active", "budget_fit", {"reserved_bytes": int(torrent.get("amount_left") or 0), "budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0)})
                if seq and self._needs_seq_desired(h, True, previous_allocations):
                    seq_desired_actions.append((h, True))
            elif h in active_slow:
                self._allocation(h, "soak", "soak", 0, False, now, "active_slow_3min")
                self._decision(h, "soak", "active_slow_3min", {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0)})
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))
            else:
                self._allocation(h, "soak", "soak", 0, False, now, "budget_or_slot_exhausted")
                self._decision(h, "soak", "budget_or_slot_exhausted", {"budget_bytes": budget, "external_reserved_bytes": int(external_reserved_bytes or 0)})
                if self._needs_seq_desired(h, False, previous_allocations):
                    seq_desired_actions.append((h, False))

        self._update_health(managed, selected_set, now, health_rows, dead_set=auto_dead, previous_allocations=previous_allocations)
        if not self.dry_run:
            self._sync_active_download_reservations(selected, {str(t["hash"]) for t in managed}, now)
        self._apply_seq_desired(seq_desired_actions)
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
        def txn(con: sqlite3.Connection) -> None:
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

        write_transaction(self.state_db, txn)

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

    def _allocation(self, hash: str, desired_state: str, slot_kind: str, reserved_bytes: int, seq_dl: bool, ts: int, reason: str) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,priority_score,reserved_bytes,desired_seq_dl,allocated_at,reason) "
                "values(?,?,?,?,?,?,?,?,?) "
                "on conflict(hash) do update set desired_state=excluded.desired_state, applied_state=excluded.applied_state, "
                "slot_kind=excluded.slot_kind, priority_score=excluded.priority_score, reserved_bytes=excluded.reserved_bytes, "
                "desired_seq_dl=excluded.desired_seq_dl, allocated_at=excluded.allocated_at, reason=excluded.reason",
                (hash, desired_state, desired_state, slot_kind, 0, reserved_bytes, 1 if seq_dl else 0, ts, reason),
            ),
        )

    def _sync_active_download_reservations(self, selected: list[Mapping[str, Any]], managed_hashes: set[str], now: int) -> None:
        selected_by_hash = {str(t.get("hash")): int(t.get("amount_left") or 0) for t in selected if str(t.get("hash") or "")}
        selected_hashes = set(selected_by_hash)

        def txn(con: sqlite3.Connection) -> None:
            active_rows = [
                dict(r)
                for r in con.execute(
                    "select id,hash from resource_reservations where kind='active_download' and state='active'"
                ).fetchall()
            ]
            active_by_hash: dict[str, list[int]] = {}
            for row in active_rows:
                active_by_hash.setdefault(str(row["hash"] or ""), []).append(int(row["id"]))

            for h in sorted(managed_hashes - selected_hashes):
                con.execute(
                    "update resource_reservations set state='released', released_at=?, reason=? "
                    "where kind='active_download' and state='active' and hash=?",
                    (now, "planner_reallocated", h),
                )

            for h, bytes_reserved in selected_by_hash.items():
                existing_ids = active_by_hash.get(h) or []
                keep_id = existing_ids[0] if existing_ids else None
                expires_at = now + 120
                if keep_id is None:
                    con.execute(
                        "insert into resource_reservations(hash,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?)",
                        (h, "active_download", bytes_reserved, "active", now, expires_at, "planner_active_download"),
                    )
                else:
                    con.execute(
                        "update resource_reservations set bytes=?, expires_at=?, released_at=null, reason=? where id=?",
                        (bytes_reserved, expires_at, "planner_active_download", keep_id),
                    )
                    if len(existing_ids) > 1:
                        placeholders = ",".join("?" for _ in existing_ids[1:])
                        con.execute(
                            f"update resource_reservations set state='released', released_at=?, reason=? where id in ({placeholders})",
                            (now, "planner_duplicate_released", *existing_ids[1:]),
                        )

        write_transaction(self.state_db, txn)

    def _mark_soak_cooldown(self, hash: str, now: int, reason: str) -> None:
        cooldown_until = int(now) + 1800
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into soak_state(hash,state,ema_dlspeed_bps,cooldown_until,last_stopped_at,exposure_bytes,last_sample_at,updated_at,reason) "
                "values(?,?,?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state=excluded.state,cooldown_until=excluded.cooldown_until,last_stopped_at=excluded.last_stopped_at,exposure_bytes=excluded.exposure_bytes,updated_at=excluded.updated_at,reason=excluded.reason",
                (hash, "soak_cooldown", 0, cooldown_until, now, 0, now, now, reason),
            ),
        )

    def _decision(self, hash: str, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (int(self.now()), "planner", hash, decision, reason_code, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    def _action(self, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?)",
                (int(self.now()), "qbt_post", path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
            ),
        )
