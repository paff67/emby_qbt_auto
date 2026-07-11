from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .budget import future_growth_by_hash, resource_claims_from_rows
from .db import readonly_connect, write_transaction
from .decision_recorder import DecisionRecorder
from .observability import redact
from .scheduler_intents import SchedulerIntent, SchedulerIntentRepository


STOPPED_STATES = {"pauseddl", "pausedup", "stoppeddl", "stoppedup", "paused", "stopped"}


@dataclass(frozen=True)
class SoakQueueConfig:
    enabled: bool = True
    resident_slots: int = 8
    allowed_modes: tuple[str, ...] = ("normal", "explore")
    require_swarm: bool = True
    max_cold_partial_bytes: int = 4 * 1024**3
    max_cold_partial_torrents: int = 8
    max_new_per_hour: int = 4
    # Deprecated compatibility knob: retained for config traceability, but no
    # longer acts as a hard gate. Disk-floor budget below decides starts.
    min_free_bytes: int = 0
    disk_floor_bytes: int = 3 * 1024**3
    emergency_floor_bytes: int = int(1.5 * 1024**3)
    recovery_margin_bytes: int = 256 * 1024**2
    max_total_exposure_bytes: int = 4 * 1024**3
    min_exposure_bytes: int = 128 * 1024**2
    max_per_torrent_exposure_bytes: int = 512 * 1024**2
    exposure_horizon_sec: int = 900
    hot_bps: int = 1024**2
    low_bps: int = 100 * 1024
    hot_confirm_sec: int = 60
    cooldown_sec: int = 1800
    max_qbt_active_downloads: int = 16
    low_capacity_throttle_margin_bytes: int = 1024**3
    low_capacity_soak_limit_bps: int = 256 * 1024
    low_capacity_throttle_trigger_bps: int = 1024**2


@dataclass(frozen=True)
class SoakQueueResult:
    started: list[str] = field(default_factory=list)
    stopped: list[str] = field(default_factory=list)
    resident_hashes: list[str] = field(default_factory=list)
    hot_hashes: list[str] = field(default_factory=list)
    cooldown_hashes: list[str] = field(default_factory=list)
    preempted_hashes: list[str] = field(default_factory=list)
    throttled_hashes: list[str] = field(default_factory=list)
    unthrottled_hashes: list[str] = field(default_factory=list)
    reserved_bytes: int = 0
    cold_partial_bytes: int = 0
    cold_partial_torrents: int = 0
    blocked_reason: str | None = None
    dry_run: bool = True

    @property
    def protected_hashes(self) -> set[str]:
        return set(self.resident_hashes) | set(self.hot_hashes)

    def as_dict(self) -> dict[str, Any]:
        return {
            "started": list(self.started),
            "stopped": list(self.stopped),
            "resident_hashes": list(self.resident_hashes),
            "hot_hashes": list(self.hot_hashes),
            "cooldown_hashes": list(self.cooldown_hashes),
            "preempted_hashes": list(self.preempted_hashes),
            "throttled_hashes": list(self.throttled_hashes),
            "unthrottled_hashes": list(self.unthrottled_hashes),
            "reserved_bytes": int(self.reserved_bytes),
            "cold_partial_bytes": int(self.cold_partial_bytes),
            "cold_partial_torrents": int(self.cold_partial_torrents),
            "blocked_reason": self.blocked_reason,
            "dry_run": bool(self.dry_run),
        }


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    raw = str(torrent.get("tags") or "")
    return {p.strip() for p in raw.split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _is_stopped_download(torrent: Mapping[str, Any]) -> bool:
    return str(torrent.get("state") or "").lower() in STOPPED_STATES


def _is_running_download(torrent: Mapping[str, Any]) -> bool:
    state = str(torrent.get("state") or "").lower()
    return state not in STOPPED_STATES and float(torrent.get("progress") or 0) < 1.0 and int(torrent.get("amount_left") or 0) > 0


class SoakQueueService:
    """Resident low-risk soak queue with bounded disk exposure reservations."""

    def __init__(
        self,
        state_db: str | Path,
        executor,
        dry_run: bool = True,
        config: SoakQueueConfig | None = None,
        now=None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = bool(dry_run)
        self.config = config or SoakQueueConfig()
        self.now = now or (lambda: int(time.time()))
        self.intent_repository = SchedulerIntentRepository(self.state_db)
        self.decision_recorder = DecisionRecorder(self.state_db, now=self.now)

    def calculate_exposure(self, torrent: Mapping[str, Any], ema_dlspeed_bps: int) -> int:
        amount_left = max(0, int(torrent.get("amount_left") or 0))
        if amount_left <= 0:
            return 0
        piece_size = int(torrent.get("piece_size") or torrent.get("piece_size_bytes") or 0)
        piece_spill = max(piece_size * 2, 32 * 1024 * 1024)
        base = max(int(self.config.min_exposure_bytes), int(piece_spill))
        ema_projection = int(max(0, int(ema_dlspeed_bps)) * int(self.config.exposure_horizon_sec))
        raw_exposure = max(base, ema_projection + piece_spill)
        return int(min(amount_left, int(self.config.max_per_torrent_exposure_bytes), raw_exposure))

    def run_once(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        free_bytes: int,
        sync_healthy: bool,
        active_hashes: set[str] | None = None,
        scheduler_mode: str = "normal",
    ) -> SoakQueueResult:
        now = int(self.now())
        scheduler_mode = str(scheduler_mode or "normal").strip().lower()
        managed = [dict(t, hash=str(t.get("hash") or h)) for h, t in snapshots.items() if _is_managed(t) and int(t.get("amount_left") or 0) > 0]
        rows = self._state_rows()
        ema_by_hash = self._update_samples(managed, rows, now)
        if not self.config.enabled:
            self._decision(None, "blocked", "disabled", {"free_bytes": free_bytes})
            return SoakQueueResult(blocked_reason="disabled", dry_run=self.dry_run)
        if not sync_healthy:
            self._decision(None, "blocked", "sync_unhealthy", {"free_bytes": free_bytes})
            return SoakQueueResult(blocked_reason="sync_unhealthy", dry_run=self.dry_run)

        active_hashes = set(active_hashes or set())
        active_like = {str(t["hash"]) for t in managed if _is_running_download(t)}
        active_like |= active_hashes
        available_qbt_slots = max(0, int(self.config.max_qbt_active_downloads) - len(active_like))

        cooldown_hashes = self._cooldown_hashes(rows, now)
        existing_residents = [
            t
            for t in managed
            if str(t["hash"]) in active_like
            and str((rows.get(str(t["hash"])) or {}).get("state") or "") in {"soak_resident", "soak_hot"}
            and str(t["hash"]) not in cooldown_hashes
        ]
        all_candidates = [
            t
            for t in managed
            if str(t["hash"]) not in active_like
            and str(t["hash"]) not in cooldown_hashes
            and _is_stopped_download(t)
        ]
        candidates: list[dict[str, Any]] = []
        for torrent in all_candidates:
            h = str(torrent["hash"])
            if self.config.require_swarm and not self._has_swarm(torrent):
                self._decision(h, "blocked", "swarm_required", {"scheduler_mode": scheduler_mode})
                continue
            candidates.append(torrent)
        candidates.sort(key=lambda t: self._candidate_sort_key(t, ema_by_hash.get(str(t["hash"]), 0)))

        cold_partial_bytes, cold_partial_torrents = self._cold_partial_debt(managed)
        recent_probe_count = self._recent_probe_count(now)
        blocked_reason: str | None = None
        if all_candidates and scheduler_mode not in self._allowed_modes():
            blocked_reason = "mode_disallows_new_probe"
        elif all_candidates and self._partial_debt_cap_reached(cold_partial_bytes, cold_partial_torrents):
            blocked_reason = "cold_partial_debt_cap_reached"
        elif all_candidates and recent_probe_count >= max(0, int(self.config.max_new_per_hour)):
            blocked_reason = "hourly_probe_cap_reached"
        elif all_candidates and not candidates and self.config.require_swarm:
            blocked_reason = "swarm_required"

        if blocked_reason is not None:
            for torrent in candidates:
                self._decision(
                    str(torrent["hash"]),
                    "blocked",
                    blocked_reason,
                    {
                        "scheduler_mode": scheduler_mode,
                        "cold_partial_bytes": cold_partial_bytes,
                        "cold_partial_torrents": cold_partial_torrents,
                        "recent_probe_count": recent_probe_count,
                    },
                )
            candidates = []
        else:
            hourly_remaining = max(0, int(self.config.max_new_per_hour) - recent_probe_count)
            candidates = candidates[:hourly_remaining]

        self._record_partial_debt_metric(
            now,
            scheduler_mode=scheduler_mode,
            cold_partial_bytes=cold_partial_bytes,
            cold_partial_torrents=cold_partial_torrents,
            recent_probe_count=recent_probe_count,
            blocked_reason=blocked_reason,
        )
        selected = list(existing_residents)
        remaining_slots = max(0, int(self.config.resident_slots) - len(selected))
        startable = min(remaining_slots, available_qbt_slots)
        selected.extend(candidates[:startable])

        selected_hashes = {str(t["hash"]) for t in selected}
        exposures = {str(t["hash"]): self.calculate_exposure(t, ema_by_hash.get(str(t["hash"]), 0)) for t in selected}
        selected, exposures, preempted_hashes = self._trim_to_budget(
            selected,
            exposures,
            free_bytes,
            rows=rows,
            ema_by_hash=ema_by_hash,
            snapshots=snapshots,
            now=now,
        )
        selected_hashes = {str(t["hash"]) for t in selected}
        started = [str(t["hash"]) for t in selected if _is_stopped_download(t)]

        if self.dry_run:
            hot_hashes = {
                str(t["hash"])
                for t in selected
                if self._is_hot_ready(
                    str(t["hash"]),
                    rows.get(str(t["hash"])) or {},
                    ema_by_hash.get(str(t["hash"]), 0),
                    now,
                )
            }
            resident_hashes = [str(t["hash"]) for t in selected if str(t["hash"]) not in hot_hashes]
            managed_by_hash = {str(t["hash"]): t for t in managed}
            stopped = [
                h
                for h, row in rows.items()
                if str(row.get("state") or "") in {"soak_resident", "soak_hot"}
                and h not in selected_hashes
                and h in managed_by_hash
                and _is_running_download(managed_by_hash[h])
            ]
            for h in started:
                self._decision(h, "resident_start_preview", "dry_run", {"exposure_bytes": exposures.get(h, 0)})
            return SoakQueueResult(
                started=started,
                stopped=stopped,
                resident_hashes=resident_hashes,
                hot_hashes=sorted(hot_hashes),
                cooldown_hashes=sorted(cooldown_hashes | set(preempted_hashes)),
                preempted_hashes=preempted_hashes,
                reserved_bytes=sum(int(v) for v in exposures.values()),
                cold_partial_bytes=cold_partial_bytes,
                cold_partial_torrents=cold_partial_torrents,
                blocked_reason=blocked_reason,
                dry_run=True,
            )

        hot_hashes = self._update_resident_state(selected, rows, ema_by_hash, exposures, now)
        resident_hashes = [str(t["hash"]) for t in selected if str(t["hash"]) not in hot_hashes]
        stopped = self._pause_stale_residents(managed, selected_hashes, rows, now)
        self._release_stale_reservations(selected_hashes, now, reason="soak_reallocated")
        self._sync_reservations(exposures, now)
        throttled_hashes, unthrottled_hashes = self._apply_low_capacity_download_limits(
            selected_hashes,
            rows=rows,
            ema_by_hash=ema_by_hash,
            free_bytes=free_bytes,
            now=now,
        )
        for h in started:
            self._decision(h, "resident_start", "budget_fit", {"exposure_bytes": exposures.get(h, 0)})
        for h in resident_hashes:
            if h not in started:
                self._decision(h, "resident_keep", "budget_fit", {"exposure_bytes": exposures.get(h, 0)})

        return SoakQueueResult(
            started=started,
            stopped=stopped,
            resident_hashes=resident_hashes,
            hot_hashes=sorted(hot_hashes),
            cooldown_hashes=sorted(cooldown_hashes | set(preempted_hashes)),
            preempted_hashes=preempted_hashes,
            throttled_hashes=throttled_hashes,
            unthrottled_hashes=unthrottled_hashes,
            reserved_bytes=sum(int(v) for v in exposures.values()),
            cold_partial_bytes=cold_partial_bytes,
            cold_partial_torrents=cold_partial_torrents,
            blocked_reason=blocked_reason,
            dry_run=self.dry_run,
        )

    def _allowed_modes(self) -> set[str]:
        return {str(mode).strip().lower() for mode in self.config.allowed_modes if str(mode).strip()}

    @staticmethod
    def _has_swarm(torrent: Mapping[str, Any]) -> bool:
        return (
            int(torrent.get("num_seeds") or torrent.get("num_complete") or 0) > 0
            or int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0) > 0
        )

    @staticmethod
    def _completed_bytes(torrent: Mapping[str, Any]) -> int:
        explicit = int(
            torrent.get("completed_bytes")
            or torrent.get("completed")
            or torrent.get("downloaded")
            or 0
        )
        size = max(0, int(torrent.get("size") or torrent.get("total_size") or 0))
        amount_left = max(0, int(torrent.get("amount_left") or 0))
        inferred = max(0, size - amount_left) if size > 0 else 0
        progress_inferred = int(size * max(0.0, min(1.0, float(torrent.get("progress") or 0))))
        return max(0, min(size, max(explicit, inferred, progress_inferred))) if size > 0 else max(0, explicit)

    def _cold_partial_debt(self, managed: list[Mapping[str, Any]]) -> tuple[int, int]:
        completed = [
            self._completed_bytes(torrent)
            for torrent in managed
            if _is_stopped_download(torrent) and int(torrent.get("amount_left") or 0) > 0
        ]
        positive = [value for value in completed if value > 0]
        return sum(positive), len(positive)

    def _partial_debt_cap_reached(self, cold_bytes: int, cold_torrents: int) -> bool:
        bytes_cap = int(self.config.max_cold_partial_bytes)
        torrent_cap = int(self.config.max_cold_partial_torrents)
        return (
            (bytes_cap > 0 and int(cold_bytes) >= bytes_cap)
            or (torrent_cap > 0 and int(cold_torrents) >= torrent_cap)
        )

    def _recent_probe_count(self, now: int) -> int:
        con = readonly_connect(self.state_db)
        try:
            return int(
                con.execute(
                    "select count(*) from decision_log "
                    "where component='soak_queue' and decision='resident_start' and ts>?",
                    (int(now) - 3600,),
                ).fetchone()[0]
            )
        finally:
            con.close()

    def _record_partial_debt_metric(
        self,
        now: int,
        *,
        scheduler_mode: str,
        cold_partial_bytes: int,
        cold_partial_torrents: int,
        recent_probe_count: int,
        blocked_reason: str | None,
    ) -> None:
        metrics = {
            "scheduler_mode": scheduler_mode,
            "allowed_modes": sorted(self._allowed_modes()),
            "require_swarm": bool(self.config.require_swarm),
            "cold_partial_bytes": int(cold_partial_bytes),
            "cold_partial_torrents": int(cold_partial_torrents),
            "max_cold_partial_bytes": int(self.config.max_cold_partial_bytes),
            "max_cold_partial_torrents": int(self.config.max_cold_partial_torrents),
            "recent_probe_count": int(recent_probe_count),
            "max_new_per_hour": int(self.config.max_new_per_hour),
            "blocked_new_probes": blocked_reason is not None,
            "blocked_reason": blocked_reason,
        }
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                (int(now), "soak_partial_debt", json.dumps(redact(metrics), ensure_ascii=False, sort_keys=True)),
            ),
        )

    def _state_rows(self) -> dict[str, dict[str, Any]]:
        con = readonly_connect(self.state_db)
        try:
            return {str(r["hash"]): dict(r) for r in con.execute("select * from soak_state").fetchall()}
        finally:
            con.close()

    def _update_samples(self, torrents: list[Mapping[str, Any]], rows: dict[str, dict[str, Any]], now: int) -> dict[str, int]:
        out: dict[str, int] = {}

        def txn(con: sqlite3.Connection) -> None:
            for torrent in torrents:
                h = str(torrent.get("hash") or "")
                if not h:
                    continue
                old = rows.get(h) or {}
                current = int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0)
                if old.get("last_sample_at") is None:
                    ema = current
                else:
                    ema = int(round(float(old.get("ema_dlspeed_bps") or 0) * 0.7 + current * 0.3))
                out[h] = ema
                con.execute(
                    "insert into soak_state(hash,state,ema_dlspeed_bps,last_sample_at,updated_at,reason) "
                    "values(?,?,?,?,?,?) "
                    "on conflict(hash) do update set ema_dlspeed_bps=excluded.ema_dlspeed_bps,last_sample_at=excluded.last_sample_at,updated_at=excluded.updated_at",
                    (h, str(old.get("state") or "candidate"), ema, now, now, old.get("reason")),
                )

        write_transaction(self.state_db, txn)
        return out

    def _cooldown_hashes(self, rows: dict[str, dict[str, Any]], now: int) -> set[str]:
        return {
            h
            for h, row in rows.items()
            if str(row.get("state") or "") == "soak_cooldown"
            and row.get("cooldown_until") is not None
            and int(row.get("cooldown_until") or 0) > now
        }

    def _candidate_sort_key(self, torrent: Mapping[str, Any], ema: int) -> tuple[Any, ...]:
        amount_left = int(torrent.get("amount_left") or 0)
        progress = float(torrent.get("progress") or 0)
        seeds = int(torrent.get("num_seeds") or torrent.get("num_complete") or 0)
        peers = int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0)
        exposure = self.calculate_exposure(torrent, ema)
        return (-progress, exposure, amount_left, -seeds, -peers, str(torrent.get("hash") or ""))

    def _trim_to_budget(
        self,
        selected: list[Mapping[str, Any]],
        exposures: dict[str, int],
        free_bytes: int,
        rows: dict[str, dict[str, Any]],
        ema_by_hash: dict[str, int],
        snapshots: Mapping[str, Mapping[str, Any]],
        now: int,
    ) -> tuple[list[Mapping[str, Any]], dict[str, int], list[str]]:
        recovery_mode = int(self.config.emergency_floor_bytes) <= int(free_bytes) < int(self.config.disk_floor_bytes)
        budget_floor = int(self.config.emergency_floor_bytes) if recovery_mode else int(self.config.disk_floor_bytes)
        recovery_margin = int(self.config.recovery_margin_bytes) if recovery_mode else 0
        safe_budget = max(0, int(free_bytes) - budget_floor - recovery_margin - self._non_soak_reservation_bytes(now))
        cap = min(int(self.config.max_total_exposure_bytes), safe_budget)
        out: list[Mapping[str, Any]] = []
        used = 0
        preempted: list[str] = []
        for torrent in selected:
            h = str(torrent["hash"])
            exposure = int(exposures.get(h) or 0)
            if used + exposure > cap:
                if self._is_hot_ready(h, rows.get(h) or {}, ema_by_hash.get(h, 0), now):
                    needed = used + exposure - cap
                    victims, released = self._select_preemption_victims(snapshots, needed, excluded_hashes={h} | set(preempted))
                    if victims and released >= needed:
                        self._preempt_active(victims, now)
                        preempted.extend(victims)
                        cap += released
                    else:
                        self._decision(h, "blocked", "no_safe_victim", {"needed_bytes": needed, "cap": cap, "used": used})
                        continue
                else:
                    self._decision(h, "blocked", "budget_insufficient", {"exposure_bytes": exposure, "cap": cap, "used": used})
                    continue
            out.append(torrent)
            used += exposure
        return out, {str(t["hash"]): int(exposures.get(str(t["hash"]), 0)) for t in out}, preempted

    def _is_hot_ready(self, hash: str, row: dict[str, Any], ema: int, now: int) -> bool:
        hot_since = row.get("hot_since")
        return (
            int(ema) >= int(self.config.hot_bps)
            and hot_since is not None
            and int(now) - int(hot_since) >= int(self.config.hot_confirm_sec)
        )

    def _non_soak_reservation_bytes(self, now: int) -> int:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select id,hash,kind,accounting_class,bytes from resource_reservations "
                "where state='active' and (expires_at is null or expires_at>?)",
                (int(now),),
            ).fetchall()
            return sum(
                future_growth_by_hash(
                    resource_claims_from_rows(rows),
                    ignored_kinds={"soak_probe"},
                ).values()
            )
        finally:
            con.close()

    def _select_preemption_victims(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        needed_bytes: int,
        excluded_hashes: set[str],
    ) -> tuple[list[str], int]:
        con = readonly_connect(self.state_db)
        try:
            rows = [
                dict(r)
                for r in con.execute(
                    "select rr.hash,rr.bytes,sa.desired_state from resource_reservations rr "
                    "left join scheduler_allocations sa on sa.hash=rr.hash "
                    "where rr.kind='active_download' and rr.state='active'"
                ).fetchall()
            ]
        finally:
            con.close()
        scored: list[tuple[float, str, int]] = []
        for row in rows:
            h = str(row.get("hash") or "")
            if not h or h in excluded_hashes:
                continue
            torrent = snapshots.get(h) or {}
            tags = _tags(torrent)
            if "hold" in tags or "seed-long" in tags:
                continue
            if str(row.get("desired_state") or "") != "active":
                continue
            progress = float(torrent.get("progress") or 0)
            amount_left = int(torrent.get("amount_left") or 0)
            if progress >= 0.95 or amount_left <= 0:
                continue
            if not _is_running_download(torrent):
                continue
            reserved = int(row.get("bytes") or 0)
            dlspeed = int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0)
            near_completion_bonus = int(progress * reserved)
            score = float(dlspeed * 2 + reserved - near_completion_bonus)
            scored.append((score, h, reserved))
        scored.sort(reverse=True)
        victims: list[str] = []
        released = 0
        for _score, h, reserved in scored:
            victims.append(h)
            released += reserved
            if released >= int(needed_bytes):
                break
        if released < int(needed_bytes):
            return [], 0
        return victims, released

    def _preempt_active(self, hashes: list[str], now: int) -> None:
        if not hashes:
            return
        if self.dry_run:
            for h in hashes:
                self._decision(h, "active_preempted", "hot_soak_preempted_active", {"dry_run": True})
            return

        def txn(con: sqlite3.Connection) -> None:
            for h in hashes:
                con.execute(
                    "update resource_reservations set state='released', released_at=?, reason=? "
                    "where kind='active_download' and state='active' and hash=?",
                    (now, "hot_soak_preempted_active", h),
                )
                self.intent_repository.upsert_in_transaction(
                    con,
                    SchedulerIntent(
                        "soak",
                        h,
                        "cooldown",
                        30,
                        now + int(self.config.cooldown_sec),
                        {"reason": "hot_soak_preempted_active"},
                    ),
                )
                con.execute(
                    "insert into soak_state(hash,state,ema_dlspeed_bps,cooldown_until,last_stopped_at,exposure_bytes,last_sample_at,updated_at,reason) "
                    "values(?,?,?,?,?,?,?,?,?) "
                    "on conflict(hash) do update set state=excluded.state,cooldown_until=excluded.cooldown_until,last_stopped_at=excluded.last_stopped_at,exposure_bytes=excluded.exposure_bytes,updated_at=excluded.updated_at,reason=excluded.reason",
                    (h, "soak_cooldown", 0, now + int(self.config.cooldown_sec), now, 0, now, now, "hot_soak_preempted_active"),
                )
                self._decision(h, "active_preempted", "hot_soak_preempted_active", {"cooldown_until": now + int(self.config.cooldown_sec)})

        write_transaction(self.state_db, txn)

    def _update_resident_state(
        self,
        selected: list[Mapping[str, Any]],
        rows: dict[str, dict[str, Any]],
        ema_by_hash: dict[str, int],
        exposures: dict[str, int],
        now: int,
    ) -> set[str]:
        hot_hashes: set[str] = set()

        def txn(con: sqlite3.Connection) -> None:
            for torrent in selected:
                h = str(torrent["hash"])
                old = rows.get(h) or {}
                ema = int(ema_by_hash.get(h, 0))
                hot_since = old.get("hot_since")
                state = "soak_resident"
                reason = "resident"
                if ema >= int(self.config.hot_bps):
                    if hot_since is None:
                        hot_since = now
                        reason = "hot_confirming"
                        self._decision(h, "hot_confirming", "hot_threshold_seen", {"ema_dlspeed_bps": ema})
                    elif now - int(hot_since) >= int(self.config.hot_confirm_sec):
                        state = "soak_hot"
                        reason = "hot_promoted"
                        hot_hashes.add(h)
                        self._decision(h, "hot_promoted", "hot_confirmed", {"ema_dlspeed_bps": ema, "hot_since": hot_since})
                elif ema < int(self.config.low_bps):
                    hot_since = None
                resident_since = old.get("resident_since") or now
                old_state = str(old.get("state") or "")
                last_started_at = old.get("last_started_at")
                if last_started_at is None or (_is_stopped_download(torrent) and old_state not in {"soak_resident", "soak_hot"}):
                    last_started_at = now
                con.execute(
                    "insert into soak_state(hash,state,ema_dlspeed_bps,hot_since,resident_since,cooldown_until,last_started_at,exposure_bytes,last_sample_at,updated_at,reason) "
                    "values(?,?,?,?,?,?,?,?,?,?,?) "
                    "on conflict(hash) do update set state=excluded.state,ema_dlspeed_bps=excluded.ema_dlspeed_bps,hot_since=excluded.hot_since,resident_since=excluded.resident_since,cooldown_until=null,last_started_at=excluded.last_started_at,exposure_bytes=excluded.exposure_bytes,last_sample_at=excluded.last_sample_at,updated_at=excluded.updated_at,reason=excluded.reason",
                    (h, state, ema, hot_since, resident_since, None, last_started_at, int(exposures.get(h, 0)), now, now, reason),
                )

        write_transaction(self.state_db, txn)
        return hot_hashes

    def _pause_stale_residents(
        self,
        managed: list[Mapping[str, Any]],
        selected_hashes: set[str],
        rows: dict[str, dict[str, Any]],
        now: int,
    ) -> list[str]:
        managed_by_hash = {str(t["hash"]): t for t in managed}
        stale = [
            h
            for h, row in rows.items()
            if str(row.get("state") or "") in {"soak_resident", "soak_hot"}
            and h not in selected_hashes
            and h in managed_by_hash
            and _is_running_download(managed_by_hash[h])
        ]
        if stale:
            for h in stale:
                if str((rows.get(h) or {}).get("reason") or "") == "low_capacity_throttled":
                    self._set_download_limit(h, 0)
                    self._decision(h, "download_unlimited", "resident_reallocated", {"limit_bps": 0})
        if stale:
            def txn(con: sqlite3.Connection) -> None:
                for h in stale:
                    con.execute(
                        "update soak_state set state='soak_cooldown', cooldown_until=?, last_stopped_at=?, exposure_bytes=0, updated_at=?, reason=? where hash=?",
                        (now + int(self.config.cooldown_sec), now, now, "resident_pause", h),
                    )
                    self.intent_repository.upsert_in_transaction(
                        con,
                        SchedulerIntent(
                            "soak",
                            h,
                            "cooldown",
                            30,
                            now + int(self.config.cooldown_sec),
                            {"reason": "resident_pause"},
                        ),
                    )
                    self._decision(h, "resident_pause", "resident_reallocated", {"cooldown_until": now + int(self.config.cooldown_sec)})
            write_transaction(self.state_db, txn)
        return stale

    def _release_stale_reservations(self, selected_hashes: set[str], now: int, reason: str) -> None:
        placeholders = ",".join("?" for _ in selected_hashes)

        def txn(con: sqlite3.Connection) -> None:
            if selected_hashes:
                con.execute(
                    f"update resource_reservations set state='released', released_at=?, reason=? where kind='soak_probe' and state='active' and hash not in ({placeholders})",
                    (now, reason, *sorted(selected_hashes)),
                )
                con.execute(
                    f"delete from scheduler_intents where component='soak' and intent='probe' and hash not in ({placeholders})",
                    tuple(sorted(selected_hashes)),
                )
            else:
                con.execute(
                    "update resource_reservations set state='released', released_at=?, reason=? where kind='soak_probe' and state='active'",
                    (now, reason),
                )
                con.execute(
                    "delete from scheduler_intents where component='soak' and intent='probe'"
                )

        write_transaction(self.state_db, txn)

    def _sync_reservations(self, exposures: dict[str, int], now: int) -> None:
        def txn(con: sqlite3.Connection) -> None:
            existing: dict[str, list[int]] = {}
            for row in con.execute("select id,hash from resource_reservations where kind='soak_probe' and state='active'").fetchall():
                existing.setdefault(str(row["hash"] or ""), []).append(int(row["id"]))
            for h, bytes_reserved in exposures.items():
                ids = existing.get(h) or []
                keep_id = ids[0] if ids else None
                expires_at = now + 120
                self.intent_repository.upsert_in_transaction(
                    con,
                    SchedulerIntent(
                        "soak",
                        h,
                        "probe",
                        30,
                        expires_at,
                        {"exposure_bytes": int(bytes_reserved)},
                    ),
                )
                if keep_id is None:
                    reason = "soak_resident_recovery_preserve" if int(bytes_reserved) <= 0 else "soak_resident"
                    con.execute(
                        "insert into resource_reservations("
                        "hash,kind,accounting_class,owner,bytes,state,created_at,expires_at,last_observed_at,reason) "
                        "values(?,?,?,?,?,?,?,?,?,?)",
                        (h, "soak_probe", "future_growth", "soak_queue", int(bytes_reserved), "active", now, expires_at, now, reason),
                    )
                else:
                    reason = "soak_resident_recovery_preserve" if int(bytes_reserved) <= 0 else "soak_resident"
                    con.execute(
                        "update resource_reservations set accounting_class='future_growth',owner='soak_queue',bytes=?,"
                        "expires_at=?,released_at=null,lease_generation=lease_generation+1,last_observed_at=?,reason=? where id=?",
                        (int(bytes_reserved), expires_at, now, reason, keep_id),
                    )
                    if len(ids) > 1:
                        placeholders = ",".join("?" for _ in ids[1:])
                        con.execute(
                            f"update resource_reservations set state='released', released_at=?, reason=? where id in ({placeholders})",
                            (now, "soak_duplicate_released", *ids[1:]),
                        )

        write_transaction(self.state_db, txn)

    def _apply_low_capacity_download_limits(
        self,
        selected_hashes: set[str],
        rows: dict[str, dict[str, Any]],
        ema_by_hash: dict[str, int],
        free_bytes: int,
        now: int,
    ) -> tuple[list[str], list[str]]:
        if not selected_hashes:
            return [], []
        floor = int(self.config.disk_floor_bytes)
        margin = max(0, int(self.config.low_capacity_throttle_margin_bytes))
        low_capacity = int(free_bytes) < floor + margin
        trigger_bps = int(self.config.low_capacity_throttle_trigger_bps or self.config.hot_bps)
        limit_bps = max(0, int(self.config.low_capacity_soak_limit_bps))
        throttled: list[str] = []
        unthrottled: list[str] = []
        for h in sorted(selected_hashes):
            previous_reason = str((rows.get(h) or {}).get("reason") or "")
            ema = int(ema_by_hash.get(h, 0))
            if low_capacity and limit_bps > 0 and ema >= trigger_bps:
                self._set_download_limit(h, limit_bps)
                self._mark_limit_state(h, now, "low_capacity_throttled")
                self._decision(
                    h,
                    "download_limited",
                    "low_capacity_soak_speed_spike",
                    {
                        "free_bytes": int(free_bytes),
                        "disk_floor_bytes": floor,
                        "throttle_margin_bytes": margin,
                        "ema_dlspeed_bps": ema,
                        "limit_bps": limit_bps,
                    },
                )
                throttled.append(h)
            elif previous_reason == "low_capacity_throttled":
                self._set_download_limit(h, 0)
                self._mark_limit_state(h, now, "low_capacity_unthrottled")
                self._decision(
                    h,
                    "download_unlimited",
                    "capacity_recovered",
                    {
                        "free_bytes": int(free_bytes),
                        "disk_floor_bytes": floor,
                        "throttle_margin_bytes": margin,
                    },
                )
                unthrottled.append(h)
        return throttled, unthrottled

    def _set_download_limit(self, hash: str, limit_bps: int) -> None:
        payload = {"hashes": hash, "limit": int(limit_bps)}
        path = "/api/v2/torrents/setDownloadLimit"
        if self.dry_run:
            self._action(path, payload, "dry_run", True)
            return
        try:
            if hasattr(self.executor, "set_download_limit"):
                self.executor.set_download_limit(hash, int(limit_bps))
            else:
                self.executor.qbt_post(path, {"hashes": hash, "limit": str(int(limit_bps))})
            self._action(path, payload, "succeeded", False)
        except Exception as exc:
            self._action(path, payload, "failed", False, str(exc))
            raise

    def _mark_limit_state(self, hash: str, now: int, reason: str) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update soak_state set updated_at=?, reason=? where hash=?",
                (int(now), str(reason), str(hash)),
            ),
        )

    def _decision(self, hash: str | None, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        self.decision_recorder.record("soak_queue", hash, decision, reason_code, data)

    def _action(self, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?)",
                (int(self.now()), "qbt_post", path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
            ),
        )
