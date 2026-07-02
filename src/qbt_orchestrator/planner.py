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


class DownloadPlanner:
    """15s planner loop: desired download state + safe qBT action coalescing."""

    def __init__(
        self,
        state_db: str | Path,
        executor,
        dry_run: bool = True,
        active_slots: int = 2,
        disk_floor_bytes: int = 2 * 1024**3,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = dry_run
        self.active_slots = active_slots
        self.disk_floor_bytes = disk_floor_bytes

    def plan_and_apply(self, snapshots: Mapping[str, Mapping[str, Any]], free_bytes: int, sync_healthy: bool) -> PlannerResult:
        managed = [dict(t, hash=h if not t.get("hash") else t.get("hash")) for h, t in snapshots.items() if _is_managed(t)]
        budget = max(0, int(free_bytes) - self.disk_floor_bytes)
        if not sync_healthy:
            for torrent in managed:
                self._decision(str(torrent["hash"]), "hold", "sync_unhealthy", {"free_bytes": free_bytes})
            return PlannerResult([], [], conservative=True, budget_bytes=budget)

        candidates = sorted(
            [t for t in managed if int(t.get("amount_left") or 0) > 0],
            key=lambda t: (int(t.get("amount_left") or 0), -int(t.get("num_seeds") or 0), -int(t.get("num_peers") or 0), str(t.get("hash"))),
        )
        selected: list[dict[str, Any]] = []
        used = 0
        for torrent in candidates:
            amount_left = int(torrent.get("amount_left") or 0)
            if len(selected) >= self.active_slots or used + amount_left > budget:
                continue
            selected.append(torrent)
            used += amount_left

        selected_hashes = [str(t["hash"]) for t in selected]
        selected_set = set(selected_hashes)
        paused_hashes = [str(t["hash"]) for t in managed if str(t["hash"]) not in selected_set and _is_running_download(t)]

        now = int(time.time())
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
            else:
                self._allocation(h, "soak", "soak", 0, False, now, "budget_or_slot_exhausted")
                self._decision(h, "soak", "budget_or_slot_exhausted", {"budget_bytes": budget})

        self._qbt_post("/api/v2/torrents/start", selected_hashes)
        self._qbt_post("/api/v2/torrents/stop", paused_hashes)
        return PlannerResult(selected_hashes, paused_hashes, conservative=False, budget_bytes=budget)

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
            (int(time.time()), "planner", hash, decision, reason_code, json.dumps(redact(data), ensure_ascii=False)),
        )
        con.commit()
        con.close()

    def _action(self, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        con = _connect(self.state_db)
        con.execute(
            "insert into action_log(ts,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?)",
            (int(time.time()), "qbt_post", path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
        )
        con.commit()
        con.close()
