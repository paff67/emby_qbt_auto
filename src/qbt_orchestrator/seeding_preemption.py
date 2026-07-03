from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping

from .db import readonly_connect, write_transaction
from .observability import redact
from .runtime import TorrentJobRepository

GIB = 1024**3
MIB = 1024**2


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    return {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _name_or_hash(torrent: Mapping[str, Any]) -> str:
    return str(torrent.get("name") or torrent.get("hash") or "unknown")


@dataclass(frozen=True)
class PreemptionConfig:
    trigger_disk_states: tuple[str, ...] = ("watch", "guard", "critical")
    min_new_task_score: float = 75.0
    min_preemptability_score: float = 65.0
    preemption_score_margin: float = 10.0
    min_absolute_seed_sec: int = 900
    protect_seed_long: bool = True
    allow_seed_long_only_in_disk_emergency: bool = True
    do_not_preempt_if_upload_bps_above: int = 64 * 1024
    do_not_preempt_if_ratio_below: float = 0.01
    cooldown_after_preemption_sec: int = 21600
    max_preemptions_per_hour: int = 3
    max_preemptions_per_torrent_per_day: int = 1


@dataclass(frozen=True)
class PreemptionDecision:
    accepted: bool
    seeding_hash: str | None
    target_hash: str | None
    new_task_score: float
    preemptability_score: float
    score_margin: float
    reason: str
    guards_passed: list[str]
    guards_blocked: list[str]
    upload_job_id: int | None = None


class SeedingPreemptionService:
    """Disk/slot pressure seeding preemption policy.

    Accepted preemptions only stop the seeding torrent and enqueue a durable
    upload job.  Cleanup still depends on UploadWorker copy + verify gates.
    """

    def __init__(
        self,
        state_db: str | Path,
        executor,
        *,
        dry_run: bool = True,
        config: PreemptionConfig | None = None,
        now: Callable[[], int] | None = None,
        host_downloads: str = "/data/downloads",
        container_downloads: str = "/downloads",
        remote: str = "gcrypt:",
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.dry_run = bool(dry_run)
        self.config = config or PreemptionConfig()
        self.now = now or (lambda: int(time.time()))
        self.host_downloads = host_downloads.rstrip("/")
        self.container_downloads = container_downloads.rstrip("/")
        self.remote = remote.rstrip(":") + ":"
        self.jobs = TorrentJobRepository(self.state_db, now=self.now)
        self.last_snapshots: dict[str, dict[str, Any]] = {}

    def evaluate_and_apply(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        disk_state: str,
        trigger_reason: str,
        selected_hashes: set[str] | None = None,
    ) -> PreemptionDecision | None:
        disk_state = str(disk_state).lower()
        self.last_snapshots = {str(h): dict(t, hash=h if not t.get("hash") else t.get("hash")) for h, t in snapshots.items()}
        if disk_state not in self.config.trigger_disk_states:
            self._decision(None, None, "hold", "disk_state_not_triggering", {"disk_state": disk_state, "trigger_reason": trigger_reason})
            return None
        if self._hourly_rate_exhausted():
            self._decision(
                None,
                None,
                "hold",
                "hourly_rate_limit",
                {"disk_state": disk_state, "trigger_reason": trigger_reason, "limit": self.config.max_preemptions_per_hour},
            )
            return None
        selected_hashes = selected_hashes or set()
        torrents = [dict(t, hash=h if not t.get("hash") else t.get("hash")) for h, t in snapshots.items() if _is_managed(t)]
        new_candidates = [t for t in torrents if self._is_download_waiting(t, selected_hashes)]
        seed_candidates = [t for t in torrents if self._is_seeding_candidate(t)]
        if not new_candidates or not seed_candidates:
            self._decision(None, None, "hold", "missing_new_or_seed_candidate", {"new_candidates": len(new_candidates), "seed_candidates": len(seed_candidates)})
            return None

        scored_new = sorted(((self.score_new_task(t), t) for t in new_candidates), key=lambda x: x[0], reverse=True)
        new_score, new_task = scored_new[0]
        if new_score < self.config.min_new_task_score:
            self._decision(str(new_task.get("hash")), None, "hold", "new_task_score_below_threshold", {"new_task_score": new_score})
            return None

        blocked: list[dict[str, Any]] = []
        best: tuple[float, Mapping[str, Any], list[str], list[str]] | None = None
        for seed in seed_candidates:
            guards_passed, guards_blocked = self._guards(seed, disk_state)
            if guards_blocked:
                blocked.append({"hash": seed.get("hash"), "blocked": guards_blocked})
                continue
            seed_score = self.score_preemptability(seed)
            if seed_score < self.config.min_preemptability_score:
                blocked.append({"hash": seed.get("hash"), "blocked": ["preemptability_score_below_threshold"], "score": seed_score})
                continue
            if best is None or seed_score > best[0]:
                best = (seed_score, seed, guards_passed, guards_blocked)

        if best is None:
            self._decision(str(new_task.get("hash")), None, "hold", "all_seed_candidates_guarded", {"blocked": blocked, "new_task_score": new_score})
            return None

        seed_score, seed, guards_passed, guards_blocked = best
        score_margin = float(new_score - (100.0 - seed_score))
        if score_margin < self.config.preemption_score_margin:
            self._decision(str(new_task.get("hash")), str(seed.get("hash")), "hold", "score_margin_below_threshold", {"new_task_score": new_score, "preemptability_score": seed_score, "score_margin": score_margin})
            return None

        decision = PreemptionDecision(
            accepted=True,
            seeding_hash=str(seed.get("hash")),
            target_hash=str(new_task.get("hash")),
            new_task_score=float(new_score),
            preemptability_score=float(seed_score),
            score_margin=score_margin,
            reason=trigger_reason,
            guards_passed=guards_passed,
            guards_blocked=guards_blocked,
        )
        return self._apply(decision, seed, new_task, disk_state)

    def force_preempt_hash(self, seeding_hash: str, *, target_hash: str | None = None, reason: str = "manual") -> dict[str, Any]:
        seed = self.last_snapshots.get(str(seeding_hash))
        if not seed:
            self._decision(target_hash, str(seeding_hash), "hold", "manual_seed_snapshot_missing", {"reason": reason})
            return {"accepted": False, "reason": "manual_seed_snapshot_missing", "seeding_hash": str(seeding_hash)}
        if not self._local_path(seed):
            self._decision(target_hash, str(seeding_hash), "hold", "manual_seed_not_uploadable", {"reason": reason})
            return {"accepted": False, "reason": "manual_seed_not_uploadable", "seeding_hash": str(seeding_hash)}
        target = self.last_snapshots.get(str(target_hash)) if target_hash else {"hash": target_hash or "", "name": target_hash or "manual"}
        decision = PreemptionDecision(
            accepted=True,
            seeding_hash=str(seeding_hash),
            target_hash=str(target_hash or ""),
            new_task_score=100.0,
            preemptability_score=max(self.config.min_preemptability_score, self.score_preemptability(seed)),
            score_margin=100.0,
            reason=reason,
            guards_passed=["manual_approval", "uploadable_payload"],
            guards_blocked=[],
        )
        applied = self._apply(decision, seed, target or {}, "manual")
        return {
            "accepted": applied.accepted,
            "seeding_hash": applied.seeding_hash,
            "target_hash": applied.target_hash,
            "upload_job_id": applied.upload_job_id,
            "reason": applied.reason,
        }

    def score_new_task(self, torrent: Mapping[str, Any]) -> float:
        tags = _tags(torrent)
        score = 0.0
        if {"hot", "priority-hot"}.intersection(tags):
            score += 40.0
        dlspeed = int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0)
        score += min(35.0, 35.0 * dlspeed / (5 * MIB))
        seeds = int(torrent.get("num_seeds") or torrent.get("num_complete") or 0)
        peers = int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0)
        score += min(20.0, seeds * 2.0 + peers * 0.5)
        added_on = torrent.get("added_on")
        if added_on is not None and int(self.now()) - int(added_on) <= 3600:
            score += 10.0
        if float(torrent.get("progress") or 0) >= 0.8:
            score += 15.0
        amount_left = int(torrent.get("amount_left") or 0)
        score -= (amount_left / GIB) * 1.5
        if dlspeed <= 0 and peers < 2:
            score -= 20.0
        return round(score, 3)

    def score_preemptability(self, torrent: Mapping[str, Any]) -> float:
        score = 0.0
        upspeed = int(torrent.get("upspeed_bps") or torrent.get("upspeed") or 0)
        if upspeed < 4 * 1024:
            score += 30.0
        size = int(torrent.get("size") or torrent.get("total_size") or torrent.get("completed") or 0)
        score += min(25.0, (size / GIB) * 2.0)
        if int(torrent.get("seeding_time") or torrent.get("seeding_time_sec") or 0) >= self.config.min_absolute_seed_sec:
            score += 20.0
        if float(torrent.get("ratio") or 0.0) >= self.config.do_not_preempt_if_ratio_below:
            score += 15.0
        if "seed-long" in _tags(torrent):
            score -= 80.0
        if upspeed > self.config.do_not_preempt_if_upload_bps_above:
            score -= 40.0
        if int(torrent.get("seeding_time") or torrent.get("seeding_time_sec") or 0) < self.config.min_absolute_seed_sec:
            score -= 50.0
        return round(score, 3)

    def _apply(
        self,
        decision: PreemptionDecision,
        seed: Mapping[str, Any],
        target: Mapping[str, Any],
        disk_state: str,
    ) -> PreemptionDecision:
        h = str(seed.get("hash"))
        upload_job_id: int | None = None
        payload = self._upload_payload(seed)
        audit = {
            "seeding_hash": h,
            "seeding_name": _name_or_hash(seed),
            "target_hash": str(target.get("hash") or ""),
            "target_name": _name_or_hash(target),
            "new_task_score": decision.new_task_score,
            "preemptability_score": decision.preemptability_score,
            "score_margin": decision.score_margin,
            "trigger_reason": decision.reason,
            "guards_passed": decision.guards_passed,
            "guards_blocked": decision.guards_blocked,
            "dry_run": self.dry_run,
            "upload_payload": payload,
        }
        if self.dry_run:
            self._record(decision, seed, target, disk_state, None, audit, action_status="dry_run", action_dry_run=True)
            return decision

        self.executor.qbt_post("/api/v2/torrents/stop", {"hashes": h})
        upload_job_id = self.jobs.enqueue(h, None, "upload", payload, priority=10)
        self._record(decision, seed, target, disk_state, upload_job_id, audit, action_status="succeeded", action_dry_run=False)
        return PreemptionDecision(
            accepted=decision.accepted,
            seeding_hash=decision.seeding_hash,
            target_hash=decision.target_hash,
            new_task_score=decision.new_task_score,
            preemptability_score=decision.preemptability_score,
            score_margin=decision.score_margin,
            reason=decision.reason,
            guards_passed=decision.guards_passed,
            guards_blocked=decision.guards_blocked,
            upload_job_id=upload_job_id,
        )

    def _record(
        self,
        decision: PreemptionDecision,
        seed: Mapping[str, Any],
        target: Mapping[str, Any],
        disk_state: str,
        upload_job_id: int | None,
        audit: dict[str, Any],
        *,
        action_status: str,
        action_dry_run: bool,
    ) -> None:
        now = int(self.now())
        guard_json = json.dumps(redact({"guards_passed": decision.guards_passed, "guards_blocked": decision.guards_blocked}), ensure_ascii=False)
        decision_json = json.dumps(redact(audit), ensure_ascii=False)
        released = int(seed.get("size") or seed.get("total_size") or seed.get("completed") or 0)

        def txn(con):
            con.execute(
                "insert into seeding_preemptions(ts,seeding_hash,target_hash,disk_state,new_task_score,preemptability_score,score_margin,released_bytes_estimate,reason,guard_json,decision_json,upload_job_id) values(?,?,?,?,?,?,?,?,?,?,?,?)",
                (now, decision.seeding_hash, decision.target_hash, disk_state, decision.new_task_score, decision.preemptability_score, decision.score_margin, released, decision.reason, guard_json, decision_json, upload_job_id),
            )
            con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (now, "seeding_preemption", decision.seeding_hash, "preempt", decision.reason, decision_json),
            )
            con.execute(
                "insert into events_v2(ts,level,component,event_type,hash,message,data_json) values(?,?,?,?,?,?,?)",
                (now, "warning", "seeding_preemption", "preempted", decision.seeding_hash, f"seeding preempted for {decision.target_hash}", decision_json),
            )
            con.execute(
                "insert into action_log(ts,hash,job_id,action_type,path,payload_json,status,dry_run) values(?,?,?,?,?,?,?,?)",
                (now, decision.seeding_hash, upload_job_id, "seeding_preempt", "/api/v2/torrents/stop", json.dumps(redact({"hashes": decision.seeding_hash}), ensure_ascii=False), action_status, 1 if action_dry_run else 0),
            )

        write_transaction(self.state_db, txn)

    def _decision(self, target_hash: str | None, seeding_hash: str | None, decision: str, reason: str, data: dict[str, Any]) -> None:
        now = int(self.now())
        payload = {"target_hash": target_hash, "seeding_hash": seeding_hash, "reason": reason, **data}
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (now, "seeding_preemption", seeding_hash or target_hash, decision, reason, json.dumps(redact(payload), ensure_ascii=False)),
            ),
        )

    def _guards(self, seed: Mapping[str, Any], disk_state: str) -> tuple[list[str], list[str]]:
        passed: list[str] = []
        blocked: list[str] = []
        tags = _tags(seed)
        upspeed = int(seed.get("upspeed_bps") or seed.get("upspeed") or 0)
        seeding_time = int(seed.get("seeding_time") or seed.get("seeding_time_sec") or 0)
        ratio = float(seed.get("ratio") or 0.0)
        if self.config.protect_seed_long and "seed-long" in tags and not (self.config.allow_seed_long_only_in_disk_emergency and disk_state == "emergency"):
            blocked.append("seed_long")
        else:
            passed.append("seed_long")
        if upspeed > self.config.do_not_preempt_if_upload_bps_above:
            blocked.append("active_upload")
        else:
            passed.append("idle_upload")
        if seeding_time < self.config.min_absolute_seed_sec:
            blocked.append("early_seed")
        else:
            passed.append("min_seed_time")
        if ratio < self.config.do_not_preempt_if_ratio_below:
            blocked.append("ratio_below_min")
        else:
            passed.append("ratio")
        seed_hash = str(seed.get("hash") or "")
        if seed_hash and self._recent_seed_preemption_count(seed_hash, self.config.cooldown_after_preemption_sec) > 0:
            blocked.append("cooldown")
        else:
            passed.append("cooldown")
        if seed_hash and self.config.max_preemptions_per_torrent_per_day >= 0 and self._recent_seed_preemption_count(seed_hash, 86400) >= self.config.max_preemptions_per_torrent_per_day:
            blocked.append("torrent_daily_limit")
        else:
            passed.append("torrent_daily_limit")
        if not self._local_path(seed):
            blocked.append("not_uploadable_payload")
        else:
            passed.append("uploadable_payload")
        return passed, blocked

    def _hourly_rate_exhausted(self) -> bool:
        limit = int(self.config.max_preemptions_per_hour)
        if limit < 0:
            return False
        return self._preemption_count_since(int(self.now()) - 3600) >= limit

    def _recent_seed_preemption_count(self, seeding_hash: str, seconds: int) -> int:
        if int(seconds) <= 0:
            return 0
        return self._preemption_count_since(int(self.now()) - int(seconds), seeding_hash=seeding_hash)

    def _preemption_count_since(self, cutoff: int, *, seeding_hash: str | None = None) -> int:
        try:
            con = readonly_connect(self.state_db)
            try:
                if seeding_hash is None:
                    row = con.execute("select count(*) from seeding_preemptions where ts>=?", (int(cutoff),)).fetchone()
                else:
                    row = con.execute("select count(*) from seeding_preemptions where ts>=? and seeding_hash=?", (int(cutoff), str(seeding_hash))).fetchone()
                return int(row[0] if row else 0)
            finally:
                con.close()
        except sqlite3.Error:
            return 0

    def _is_download_waiting(self, torrent: Mapping[str, Any], selected_hashes: set[str]) -> bool:
        h = str(torrent.get("hash") or "")
        return h not in selected_hashes and int(torrent.get("amount_left") or 0) > 0 and float(torrent.get("progress") or 0) < 1.0

    def _is_seeding_candidate(self, torrent: Mapping[str, Any]) -> bool:
        if float(torrent.get("progress") or 0) < 1.0:
            return False
        if int(torrent.get("amount_left") or 0) > 0:
            return False
        state = str(torrent.get("state") or "").lower()
        if not state:
            return True
        return "up" in state or "seed" in state or state in {"uploading", "stalledup", "forcedup", "queuedup"}

    def _upload_payload(self, seed: Mapping[str, Any]) -> dict[str, Any]:
        h = str(seed.get("hash") or "")
        local = self._local_path(seed)
        name = _name_or_hash(seed)
        remote = f"{self.remote}/{PurePosixPath(name).name}"
        return {
            "hash": h,
            "local": local,
            "remote": remote,
            "size": int(seed.get("size") or seed.get("total_size") or seed.get("completed") or 0),
            "full_torrent": True,
            "copy_mode": "copy",
            "upload_manifest_id": f"preempt-{h}",
            "preempted": True,
        }

    def _local_path(self, seed: Mapping[str, Any]) -> str:
        raw = str(seed.get("content_path") or "")
        if not raw:
            save = str(seed.get("save_path") or "")
            name = _name_or_hash(seed)
            raw = str(PurePosixPath(save, name)) if save else ""
        if not raw:
            return ""
        if raw == self.container_downloads:
            return self.host_downloads
        if raw.startswith(self.container_downloads + "/"):
            return self.host_downloads + raw[len(self.container_downloads):]
        return raw
