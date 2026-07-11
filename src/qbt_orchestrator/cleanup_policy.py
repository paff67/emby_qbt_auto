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
    remote_verified: bool,
    min_seed_sec: int,
    min_ratio: float,
    now: int | None = None,
) -> CleanupEligibility:
    """Return the sole policy gate for destructive full-torrent cleanup."""
    observed_at = int(time.time()) if now is None else int(now)
    if not remote_verified:
        return CleanupEligibility(False, "remote_not_verified", None)
    if "seed-long" in _tags(torrent):
        return CleanupEligibility(False, "seed_long", None)
    if int(torrent.get("seeding_time") or 0) < max(0, int(min_seed_sec)):
        return CleanupEligibility(False, "seed_time", observed_at + 300)
    if float(torrent.get("ratio") or 0.0) < max(0.0, float(min_ratio)):
        return CleanupEligibility(False, "ratio", observed_at + 300)
    return CleanupEligibility(True, "policy_satisfied", None)
