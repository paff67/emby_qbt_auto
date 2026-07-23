from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class CleanupEligibility:
    allowed: bool
    reason: str
    next_check_at: int | None


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    return {part.strip() for part in str(torrent.get("tags") or "").split(",") if part.strip()}


def cleanup_eligibility(
    torrent: Mapping[str, Any],
    *,
    canonical_remote_verified: bool,
    free_bytes: int,
    pressure_free_bytes: int,
    min_seed_sec: int,
    min_ratio: float,
    max_retention_sec: int,
    promotion_conflict: bool = False,
    completion_age_sec: int | None = None,
    now: int | None = None,
) -> CleanupEligibility:
    """Evaluate hard safety gates, then independent release conditions."""
    observed_at = int(time.time()) if now is None else int(now)
    tags = _tags(torrent)
    if not canonical_remote_verified:
        return CleanupEligibility(False, "remote_not_canonical", None)
    if "hold" in tags:
        return CleanupEligibility(False, "hold", None)
    if "seed-long" in tags:
        return CleanupEligibility(False, "seed_long", None)
    if promotion_conflict:
        return CleanupEligibility(False, "promotion_conflict", None)
    if int(free_bytes) < max(0, int(pressure_free_bytes)):
        return CleanupEligibility(True, "disk_pressure", None)
    if float(torrent.get("ratio") or 0.0) >= max(0.0, float(min_ratio)):
        return CleanupEligibility(True, "ratio", None)
    if int(torrent.get("seeding_time") or 0) >= max(0, int(min_seed_sec)):
        return CleanupEligibility(True, "seed_time", None)
    if str(torrent.get("state") or "") == "stoppedUP" and bool(
        torrent.get("share_limit_reached")
    ):
        return CleanupEligibility(True, "share_limit", None)
    if completion_age_sec is None:
        completion_on = int(torrent.get("completion_on") or 0)
        completion_age_sec = (
            max(0, observed_at - completion_on) if completion_on > 0 else None
        )
    if completion_age_sec is not None and int(completion_age_sec) >= max(
        0, int(max_retention_sec)
    ):
        return CleanupEligibility(True, "retention", None)
    return CleanupEligibility(False, "policy_wait", observed_at + 300)
