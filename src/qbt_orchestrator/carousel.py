from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import write_transaction
from .observability import redact


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


class CarouselService:
    """Dead torrent carousel probe loop.

    Dead torrents are normally stopped.  Every carousel tick probes a bounded
    number of dead candidates, disables sequential download, and either
    promotes them back to Soak when swarm appears or stops them with backoff.
    """

    def __init__(
        self,
        state_db: str | Path,
        executor,
        dry_run: bool = True,
        concurrency: int = 3,
        probe_duration_sec: int = 30 * 60,
        backoff_schedule_sec: tuple[int, ...] = (30 * 60, 2 * 3600, 6 * 3600, 24 * 3600),
        min_free_bytes: int = 5 * 1024**3,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = bool(dry_run)
        self.concurrency = max(0, int(concurrency))
        self.probe_duration_sec = int(probe_duration_sec)
        self.backoff_schedule_sec = tuple(int(x) for x in backoff_schedule_sec) or (30 * 60,)
        self.min_free_bytes = int(min_free_bytes)
        self.now = now or (lambda: int(time.time()))

    def run_once(self, snapshots: Mapping[str, Any], sync_healthy: bool, free_bytes: int | None = None) -> dict[str, Any]:
        now = int(self.now())
        if not sync_healthy:
            self._event("warning", "suspended_unhealthy_sync", "carousel suspended because qBT sync is unhealthy", {"dry_run": self.dry_run})
            return {"suspended": True, "reason": "unhealthy_sync", "started": [], "promoted": [], "stopped": [], "dry_run": self.dry_run}
        if free_bytes is not None and int(free_bytes) < self.min_free_bytes:
            data = {"free_bytes": int(free_bytes), "min_free_bytes": self.min_free_bytes, "dry_run": self.dry_run}
            self._event("warning", "suspended_disk_guard", "carousel suspended because disk free space is below live guard", data)
            return {"suspended": True, "reason": "disk_guard", "started": [], "promoted": [], "stopped": [], "active_probes": self._active_probe_count(), "dry_run": self.dry_run, **data}

        promoted, stopped = self._reconcile_active_probes(snapshots, now)
        active_count = self._active_probe_count()
        capacity = max(0, self.concurrency - active_count)
        started = self._start_new_probes(snapshots, now, capacity)
        active_after = self._active_probe_count()
        return {
            "suspended": False,
            "started": started,
            "promoted": promoted,
            "stopped": stopped,
            "active_probes": active_after,
            "dry_run": self.dry_run,
        }

    def _reconcile_active_probes(self, snapshots: Mapping[str, Any], now: int) -> tuple[list[str], list[str]]:
        promoted: list[str] = []
        expired: list[str] = []
        con = _connect(self.state_db)
        rows = [dict(r) for r in con.execute("select * from carousel_state where state='probing' order by probe_started_at,hash")]
        con.close()
        for row in rows:
            h = str(row["hash"])
            snap = self._snapshot(snapshots, h)
            if self._has_swarm(snap):
                self._mark_soak(h, now)
                self._allocation(h, "soak", "soak", 0, "carousel_swarm_seen", now)
                self._decision(h, "soak", "carousel_swarm_seen", {"probe_started_at": row.get("probe_started_at")})
                promoted.append(h)
                continue
            started_at = int(row["probe_started_at"]) if row.get("probe_started_at") is not None else now
            if now - started_at >= self.probe_duration_sec:
                expired.append(h)
                level = int(row.get("backoff_level") or 0)
                backoff = self.backoff_schedule_sec[min(level, len(self.backoff_schedule_sec) - 1)]
                self._mark_dead(h, now, backoff_until=now + backoff, backoff_level=level + 1)
                self._allocation(h, "dead", "dead", 0, "carousel_no_swarm", now)
                self._decision(h, "dead", "carousel_no_swarm", {"probe_started_at": started_at, "backoff_sec": backoff})
        if expired:
            self._post("/api/v2/torrents/stop", expired, action_type="carousel_stop_expired")
        return promoted, expired

    def _start_new_probes(self, snapshots: Mapping[str, Any], now: int, capacity: int) -> list[str]:
        if capacity <= 0 or self.concurrency <= 0:
            return []
        candidates = self._dead_candidates(snapshots, now)
        selected = candidates[:capacity]
        if not selected:
            return []
        for h in selected:
            self._disable_seq_dl(h)
            self._mark_probing(h, now)
            self._allocation(h, "carousel_probe", "carousel", 0, "carousel_probe_started", now)
            self._decision(h, "carousel_probe", "carousel_probe_started", {"concurrency": self.concurrency})
        self._post("/api/v2/torrents/start", selected, action_type="carousel_start_probe")
        return selected

    def _dead_candidates(self, snapshots: Mapping[str, Any], now: int) -> list[str]:
        con = _connect(self.state_db)
        rows = [
            dict(r)
            for r in con.execute(
                "select hash from scheduler_allocations where desired_state='dead' order by allocated_at,hash"
            )
        ]
        state_rows = {str(r["hash"]): dict(r) for r in con.execute("select * from carousel_state")}
        con.close()
        out: list[str] = []
        for row in rows:
            h = str(row["hash"])
            snap = self._snapshot(snapshots, h)
            if not snap or not _is_managed(snap) or int(snap.get("amount_left") or 0) <= 0:
                continue
            state = state_rows.get(h)
            if state:
                if state.get("state") in {"probing", "soak"}:
                    continue
                backoff_until = state.get("backoff_until")
                if backoff_until is not None and int(backoff_until) > now:
                    continue
            out.append(h)
        return out

    def _disable_seq_dl(self, h: str) -> None:
        if self.dry_run:
            self._action("carousel_set_seq_dl", h, "/api/v2/torrents/toggleSequentialDownload", {"hashes": h, "desired_seq_dl": False}, "dry_run", True)
            return
        if hasattr(self.executor, "set_seq_dl"):
            self.executor.set_seq_dl(h, False)
            self._action("carousel_set_seq_dl", h, "/api/v2/torrents/toggleSequentialDownload", {"hashes": h, "desired_seq_dl": False}, "succeeded", False)

    def _post(self, path: str, hashes: list[str], action_type: str) -> None:
        if not hashes:
            return
        payload = {"hashes": "|".join(hashes)}
        if self.dry_run:
            self._action(action_type, None, path, payload, "dry_run", True)
            return
        try:
            self.executor.qbt_post(path, payload)
            self._action(action_type, None, path, payload, "succeeded", False)
        except Exception as exc:
            self._action(action_type, None, path, payload, "failed", False, str(exc))
            raise

    def _active_probe_count(self) -> int:
        con = _connect(self.state_db)
        count = int(con.execute("select count(*) from carousel_state where state='probing'").fetchone()[0])
        con.close()
        return count

    def _mark_probing(self, h: str, now: int) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_until,backoff_level,updated_at) values(?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state=excluded.state, probe_started_at=excluded.probe_started_at, "
                "last_probe_at=excluded.last_probe_at, backoff_until=null, updated_at=excluded.updated_at",
                (h, "probing", now, now, None, 0, now),
            ),
        )

    def _mark_soak(self, h: str, now: int) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update carousel_state set state='soak', last_probe_at=?, updated_at=? where hash=?",
                (now, now, h),
            ),
        )

    def _mark_dead(self, h: str, now: int, backoff_until: int, backoff_level: int) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into carousel_state(hash,state,probe_started_at,last_probe_at,backoff_until,backoff_level,updated_at) values(?,?,?,?,?,?,?) "
                "on conflict(hash) do update set state='dead', last_probe_at=excluded.last_probe_at, "
                "backoff_until=excluded.backoff_until, backoff_level=excluded.backoff_level, updated_at=excluded.updated_at",
                (h, "dead", None, now, backoff_until, backoff_level, now),
            ),
        )

    def _allocation(self, h: str, desired_state: str, slot_kind: str, desired_seq_dl: int, reason: str, now: int) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into scheduler_allocations(hash,desired_state,applied_state,slot_kind,desired_seq_dl,allocated_at,reason) "
                "values(?,?,?,?,?,?,?) "
                "on conflict(hash) do update set desired_state=excluded.desired_state, applied_state=excluded.applied_state, "
                "slot_kind=excluded.slot_kind, desired_seq_dl=excluded.desired_seq_dl, allocated_at=excluded.allocated_at, reason=excluded.reason",
                (h, desired_state, desired_state, slot_kind, int(desired_seq_dl), now, reason),
            ),
        )

    def _decision(self, h: str, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (int(self.now()), "carousel", h, decision, reason_code, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    def _action(self, action_type: str, h: str | None, path: str, payload: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,hash,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?)",
                (int(self.now()), h, action_type, path, json.dumps(redact(payload), ensure_ascii=False), status, 1 if dry_run else 0, redact(error) if error else None),
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

    @staticmethod
    def _snapshot(snapshots: Mapping[str, Any], h: str) -> dict[str, Any]:
        raw = snapshots.get(h)
        if raw is None:
            return {}
        if hasattr(raw, "__dict__"):
            return dict(vars(raw))
        return dict(raw)

    @staticmethod
    def _has_swarm(snapshot: Mapping[str, Any]) -> bool:
        return int(snapshot.get("num_seeds") or 0) > 0 or int(snapshot.get("num_peers") or 0) > 0
