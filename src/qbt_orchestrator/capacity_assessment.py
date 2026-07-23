from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Mapping


@dataclass(frozen=True)
class TorrentCapacityEvidence:
    hash: str
    managed: bool
    incomplete: bool
    amount_left: int
    completed_bytes: int
    availability: float | None
    complete_sources: int
    no_progress_since: int | None
    viable: bool
    viability_reason: str


@dataclass(frozen=True)
class CapacityAssessment:
    generation: int
    observed_at: int
    scheduler_mode: str
    free_bytes: int
    target_free_bytes: int
    available_growth_bytes: int
    selected_hashes: frozenset[str]
    disk_releasing_jobs: int
    torrents: Mapping[str, TorrentCapacityEvidence] = field(default_factory=dict)

    @property
    def managed_incomplete(self) -> int:
        return sum(1 for item in self.torrents.values() if item.managed and item.incomplete)

    @property
    def viable_finish(self) -> int:
        return sum(
            1
            for item in self.torrents.values()
            if item.managed and item.incomplete and item.viable
        )

    @property
    def nonviable_finish(self) -> int:
        return self.managed_incomplete - self.viable_finish

    def with_generation(self, generation: int) -> "CapacityAssessment":
        return replace(self, generation=int(generation))


class CapacityAssessmentBuilder:
    def __init__(self, viability_stale_sec: int = 1800):
        self.viability_stale_sec = max(0, int(viability_stale_sec))

    def build(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        health_by_hash: Mapping[str, Mapping[str, Any]],
        *,
        observed_at: int,
        scheduler_mode: str,
        free_bytes: int,
        target_free_bytes: int,
        available_growth_bytes: int,
        selected_hashes: set[str] | frozenset[str],
        disk_releasing_jobs: int,
    ) -> CapacityAssessment:
        items: dict[str, TorrentCapacityEvidence] = {}
        for fallback_hash, raw in snapshots.items():
            torrent = dict(raw)
            torrent_hash = str(torrent.get("hash") or fallback_hash)
            tags = {
                part.strip()
                for part in str(torrent.get("tags") or "").split(",")
                if part.strip()
            }
            managed = (
                str(torrent.get("category") or "") == "auto" or "auto" in tags
            ) and "hold" not in tags
            amount_left = max(0, int(torrent.get("amount_left") or 0))
            raw_availability = torrent.get("availability")
            availability = (
                None
                if raw_availability is None or float(raw_availability) < 0
                else float(raw_availability)
            )
            complete_sources = max(
                0,
                int(torrent.get("num_seeds") or 0),
                int(torrent.get("num_complete") or 0),
            )
            health = dict(health_by_hash.get(torrent_hash) or {})
            no_progress_since = health.get("no_progress_since")
            dlspeed = max(
                0,
                int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0),
            )
            has_complete_source = complete_sources > 0 or (
                availability is not None and availability >= 1.0
            )
            recent_progress = (
                dlspeed > 0
                or no_progress_since is None
                or int(observed_at) - int(no_progress_since) < self.viability_stale_sec
            )
            viable = has_complete_source or recent_progress
            reason = (
                "complete_source"
                if has_complete_source
                else "recent_progress"
                if recent_progress
                else "stale_without_complete_source"
            )
            items[torrent_hash] = TorrentCapacityEvidence(
                hash=torrent_hash,
                managed=managed,
                incomplete=amount_left > 0,
                amount_left=amount_left,
                completed_bytes=max(
                    0,
                    int(
                        torrent.get("completed_bytes")
                        or torrent.get("completed")
                        or torrent.get("downloaded")
                        or 0
                    ),
                ),
                availability=availability,
                complete_sources=complete_sources,
                no_progress_since=(
                    None if no_progress_since is None else int(no_progress_since)
                ),
                viable=bool(viable),
                viability_reason=reason,
            )
        return CapacityAssessment(
            generation=0,
            observed_at=int(observed_at),
            scheduler_mode=str(scheduler_mode),
            free_bytes=max(0, int(free_bytes)),
            target_free_bytes=max(0, int(target_free_bytes)),
            available_growth_bytes=max(0, int(available_growth_bytes)),
            selected_hashes=frozenset(str(item) for item in selected_hashes),
            disk_releasing_jobs=max(0, int(disk_releasing_jobs)),
            torrents=items,
        )
