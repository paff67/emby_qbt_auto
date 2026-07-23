from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from .db import write_transaction
from .hash_identity import canonical_torrent_hash


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

    def __post_init__(self) -> None:
        canonical_torrents: dict[str, TorrentCapacityEvidence] = {}
        for fallback_hash, evidence in self.torrents.items():
            torrent_hash = canonical_torrent_hash(
                evidence.hash or fallback_hash
            )
            canonical_torrents[torrent_hash] = (
                evidence
                if evidence.hash == torrent_hash
                else replace(evidence, hash=torrent_hash)
            )
        object.__setattr__(
            self,
            "selected_hashes",
            frozenset(
                canonical
                for value in self.selected_hashes
                if (canonical := canonical_torrent_hash(value))
            ),
        )
        object.__setattr__(
            self,
            "torrents",
            MappingProxyType(canonical_torrents),
        )

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
        canonical_health = {
            canonical_torrent_hash(key): dict(value)
            for key, value in health_by_hash.items()
            if canonical_torrent_hash(key)
        }
        for fallback_hash, raw in snapshots.items():
            torrent = dict(raw)
            torrent_hash = canonical_torrent_hash(
                torrent.get("hash") or fallback_hash
            )
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
            health = dict(canonical_health.get(torrent_hash) or {})
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
            selected_hashes=frozenset(
                canonical
                for item in selected_hashes
                if (canonical := canonical_torrent_hash(item))
            ),
            disk_releasing_jobs=max(0, int(disk_releasing_jobs)),
            torrents=items,
        )


def project_progress_health(
    torrent: Mapping[str, Any],
    old_health: Mapping[str, Any] | None,
    *,
    observed_at: int,
) -> dict[str, Any]:
    """Project the current progress sample with Planner stall semantics."""

    old = dict(old_health or {})
    completed = max(
        0,
        int(
            torrent.get("completed_bytes")
            or torrent.get("completed")
            or torrent.get("downloaded")
            or 0
        ),
    )
    progress = max(0.0, float(torrent.get("progress") or 0.0))
    dlspeed = max(
        0,
        int(torrent.get("dlspeed_bps") or torrent.get("dlspeed") or 0),
    )
    previous_completed = max(0, int(old.get("completed_bytes") or 0))
    previous_progress = max(0.0, float(old.get("progress") or 0.0))
    no_growth = (
        completed <= previous_completed and progress <= previous_progress
    )
    no_progress_since = old.get("no_progress_since") if old and no_growth else None
    if old and no_growth and no_progress_since is None:
        no_progress_since = int(observed_at)
    return {
        "hash": canonical_torrent_hash(torrent.get("hash")),
        "completed_bytes": completed,
        "previous_completed_bytes": previous_completed,
        "progress": progress,
        "dlspeed_bps": dlspeed,
        "no_progress_since": (
            None if no_progress_since is None else int(no_progress_since)
        ),
    }


def _core_reclaimable(
    item: TorrentCapacityEvidence,
    observed_at: int,
    min_no_progress_sec: int,
) -> bool:
    return bool(
        item.managed
        and item.incomplete
        and not item.viable
        and item.availability is not None
        and 0.0 <= item.availability < 1.0
        and item.complete_sources == 0
        and item.no_progress_since is not None
        and int(observed_at) - int(item.no_progress_since)
        >= int(min_no_progress_sec)
    )


class CapacityAssessmentStore:
    def __init__(self, state_db: str | Path, *, min_no_progress_sec: int):
        self.state_db = Path(state_db)
        self.min_no_progress_sec = int(min_no_progress_sec)

    def commit(self, assessment: CapacityAssessment) -> CapacityAssessment:
        core_by_hash = {
            canonical_torrent_hash(item.hash): _core_reclaimable(
                item,
                assessment.observed_at,
                self.min_no_progress_sec,
            )
            for item in assessment.torrents.values()
        }
        summary_json = json.dumps(
            {
                "managed_incomplete": assessment.managed_incomplete,
                "nonviable_finish": assessment.nonviable_finish,
                "reclaimable": sum(core_by_hash.values()),
                "torrent_count": len(assessment.torrents),
                "viable_finish": assessment.viable_finish,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

        def txn(con: sqlite3.Connection) -> int:
            state = con.execute(
                "insert into capacity_assessment_state("
                "id,current_generation,observed_at,summary_json) values(1,1,?,?) "
                "on conflict(id) do update set "
                "current_generation=capacity_assessment_state.current_generation+1,"
                "observed_at=excluded.observed_at,summary_json=excluded.summary_json "
                "returning current_generation",
                (int(assessment.observed_at), summary_json),
            ).fetchone()
            assert state is not None
            generation = int(state["current_generation"])

            for item in assessment.torrents.values():
                torrent_hash = canonical_torrent_hash(item.hash)
                previous = con.execute(
                    "select rowid,reclaimable_since,capacity_generation "
                    "from torrent_health where lower(trim(hash))=? "
                    "order by rowid desc limit 1",
                    (torrent_hash,),
                ).fetchone()
                if previous is None:
                    con.execute(
                        "insert into torrent_health(hash,sampled_at,updated_at) "
                        "values(?,?,?)",
                        (
                            torrent_hash,
                            assessment.observed_at,
                            assessment.observed_at,
                        ),
                    )
                    previous = con.execute(
                        "select rowid,reclaimable_since,capacity_generation "
                        "from torrent_health where rowid=last_insert_rowid()"
                    ).fetchone()
                assert previous is not None
                core = core_by_hash[torrent_hash]
                reclaimable_since = (
                    int(previous["reclaimable_since"])
                    if core
                    and previous
                    and previous["reclaimable_since"] is not None
                    and previous["capacity_generation"] == generation - 1
                    else int(assessment.observed_at)
                    if core
                    else None
                )
                con.execute(
                    "update torrent_health set reclaimable_since=?,capacity_viable=?,"
                    "capacity_reason=?,capacity_assessed_at=?,capacity_generation=? "
                    "where rowid=?",
                    (
                        reclaimable_since,
                        1 if item.viable else 0,
                        item.viability_reason,
                        assessment.observed_at,
                        generation,
                        int(previous["rowid"]),
                    ),
                )
            return generation

        generation = int(write_transaction(self.state_db, txn))
        return assessment.with_generation(generation)
