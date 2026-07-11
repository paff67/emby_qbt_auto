from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class WorkKind(str, Enum):
    FULL_FINISH = "full_finish"
    BATCH_DELIVERY = "batch_delivery"
    SOAK_PROBE = "soak_probe"


@dataclass(frozen=True)
class WorkItem:
    id: str
    hash: str
    kind: WorkKind
    incremental_growth_bytes: int
    releasable_bytes: int
    pinned_after_success_bytes: int
    completion_probability: float
    throughput_bps: int
    wait_age_sec: int
    operator_priority: int
    hold: bool = False
    data: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def stable_key(self) -> tuple[str, str]:
        return str(self.hash), str(self.id)


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    return {part.strip() for part in str(torrent.get("tags") or "").split(",") if part.strip()}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _completion_probability(torrent: Mapping[str, Any]) -> float:
    explicit = torrent.get("completion_probability")
    if explicit is not None:
        return max(0.0, min(1.0, float(explicit)))
    seeds = max(0, int(torrent.get("num_seeds") or torrent.get("num_complete") or 0))
    peers = max(0, int(torrent.get("num_peers") or torrent.get("num_incomplete") or 0))
    if seeds > 0:
        return min(0.99, 0.65 + min(seeds, 34) * 0.01)
    if peers > 0:
        return min(0.60, 0.20 + min(peers, 40) * 0.01)
    return 0.05


def build_full_finish_work_items(
    snapshots: Mapping[str, Mapping[str, Any]],
    *,
    now: int | None = None,
    default_piece_uncertainty_bytes: int = 32 * 1024**2,
) -> list[WorkItem]:
    """Convert managed incomplete torrents into conservative finish work."""

    observed_at = int(time.time()) if now is None else int(now)
    items: list[WorkItem] = []
    for fallback_hash, raw in snapshots.items():
        torrent = dict(raw)
        torrent_hash = str(torrent.get("hash") or fallback_hash)
        tags = _tags(torrent)
        managed = str(torrent.get("category") or "") == "auto" or "auto" in tags
        amount_left = max(0, int(torrent.get("amount_left") or 0))
        if not managed or not torrent_hash or amount_left <= 0:
            continue

        piece_size = max(0, int(torrent.get("piece_size") or torrent.get("piece_size_bytes") or 0))
        explicit_uncertainty = torrent.get("piece_uncertainty_bytes")
        uncertainty = (
            max(0, int(explicit_uncertainty))
            if explicit_uncertainty is not None
            else (2 * piece_size if piece_size > 0 else max(0, int(default_piece_uncertainty_bytes)))
        )
        size = max(0, int(torrent.get("size") or torrent.get("total_size") or 0))
        completed = max(
            0,
            int(
                torrent.get("completed_bytes")
                or torrent.get("completed")
                or torrent.get("downloaded")
                or max(0, size - amount_left)
            ),
        )
        eventual_cleanup = _truthy(torrent.get("cleanup_eventually_permitted")) and "seed-long" not in tags
        releasable = completed + amount_left if _truthy(torrent.get("remote_verified")) and eventual_cleanup else 0
        added_at = int(torrent.get("added_on") or torrent.get("queued_at") or observed_at)
        throughput = max(0, int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0))
        local_after_success = max(size, completed + amount_left)
        items.append(
            WorkItem(
                id=f"full:{torrent_hash}",
                hash=torrent_hash,
                kind=WorkKind.FULL_FINISH,
                incremental_growth_bytes=amount_left + uncertainty,
                releasable_bytes=releasable,
                pinned_after_success_bytes=0 if releasable > 0 else local_after_success,
                completion_probability=_completion_probability(torrent),
                throughput_bps=throughput,
                wait_age_sec=max(0, observed_at - added_at),
                operator_priority=int(torrent.get("operator_priority") or 0),
                hold="hold" in tags,
            )
        )
    return sorted(items, key=lambda item: item.stable_key)


def build_batch_delivery_work_item(
    torrent: Mapping[str, Any],
    *,
    candidate_id: str,
    incremental_growth_bytes: int,
    payload_bytes: int,
    data: Mapping[str, Any] | None = None,
    now: int | None = None,
) -> WorkItem:
    """Build delivery-only work that cannot claim future cleanup relief."""

    observed_at = int(time.time()) if now is None else int(now)
    torrent_hash = str(torrent.get("hash") or "")
    added_at = int(torrent.get("added_on") or torrent.get("queued_at") or observed_at)
    return WorkItem(
        id=str(candidate_id),
        hash=torrent_hash,
        kind=WorkKind.BATCH_DELIVERY,
        incremental_growth_bytes=max(0, int(incremental_growth_bytes)),
        releasable_bytes=0,
        pinned_after_success_bytes=max(1, int(payload_bytes)),
        completion_probability=_completion_probability(torrent),
        throughput_bps=max(0, int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0)),
        wait_age_sec=max(0, observed_at - added_at),
        operator_priority=int(torrent.get("operator_priority") or 0),
        hold="hold" in _tags(torrent),
        data=dict(data or {}),
    )
