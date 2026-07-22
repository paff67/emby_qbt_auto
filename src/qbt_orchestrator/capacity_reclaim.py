from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping

from .db import readonly_connect


STOPPED_DOWNLOAD_STATES = frozenset({"stoppedDL", "pausedDL"})
PROTECTED_TAGS = frozenset({"hold", "seed-long"})
OPEN_JOB_STATES = (
    "queued",
    "running",
    "verify_pending",
    "retry_wait",
    "promotion_wait",
    "cleanup_wait",
)


@dataclass(frozen=True)
class CapacityReclaimResult:
    dry_run: bool
    planned: int = 0
    reclaimed: int = 0
    planned_bytes: int = 0
    reclaimed_bytes: int = 0
    candidates: list[dict[str, Any]] = field(default_factory=list)
    rejection_counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": bool(self.dry_run),
            "planned": int(self.planned),
            "reclaimed": int(self.reclaimed),
            "planned_bytes": int(self.planned_bytes),
            "reclaimed_bytes": int(self.reclaimed_bytes),
            "candidates": [dict(item) for item in self.candidates],
            "rejection_counts": dict(self.rejection_counts),
            "errors": list(self.errors),
        }


class DeadPartialReclaimer:
    """Reclaim payload bytes from persistently dead torrents without deleting torrents.

    The torrent remains registered in qBittorrent.  Live mode stops it, removes
    only its validated content path, and requests a recheck so it can be retried
    later from zero if availability returns.
    """

    def __init__(
        self,
        state_db: str | Path,
        executor,
        *,
        host_downloads: str | Path,
        container_downloads: str,
        managed_root: str | Path,
        dry_run: bool = True,
        min_dead_age_sec: int = 21_600,
        min_reclaim_bytes: int = 64 * 1024**2,
        max_per_tick: int = 1,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.host_downloads = Path(host_downloads).resolve()
        self.container_downloads = PurePosixPath(str(container_downloads))
        self.managed_root = Path(managed_root).resolve()
        self.dry_run = bool(dry_run)
        self.min_dead_age_sec = max(0, int(min_dead_age_sec))
        self.min_reclaim_bytes = max(0, int(min_reclaim_bytes))
        self.max_per_tick = max(0, int(max_per_tick))
        self.now = now or (lambda: int(__import__("time").time()))
        if not self.managed_root.is_relative_to(self.host_downloads):
            raise ValueError("capacity reclaim managed_root must be inside host_downloads")

    def run(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        capacity_state: str,
        free_bytes: int,
        target_free_bytes: int,
    ) -> CapacityReclaimResult:
        if (
            str(capacity_state) != "capacity_deadlock"
            or int(free_bytes) >= int(target_free_bytes)
            or self.max_per_tick <= 0
        ):
            return CapacityReclaimResult(dry_run=self.dry_run)

        eligible_rows, open_jobs, active_claims = self._eligibility_state()
        all_paths = self._snapshot_paths(snapshots)
        rejection_counts: dict[str, int] = {}

        def reject(reason: str) -> None:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

        candidates: list[dict[str, Any]] = []
        now = int(self.now())
        for fallback_hash, raw in snapshots.items():
            torrent = dict(raw)
            torrent_hash = str(torrent.get("hash") or fallback_hash)
            row = eligible_rows.get(torrent_hash)
            if row is None:
                continue
            tags = {
                item.strip()
                for item in str(torrent.get("tags") or "").split(",")
                if item.strip()
            }
            if tags & PROTECTED_TAGS:
                reject("protected_tag")
                continue
            if str(torrent.get("state") or "") not in STOPPED_DOWNLOAD_STATES:
                reject("not_stopped")
                continue
            if int(torrent.get("amount_left") or 0) <= 0:
                reject("not_incomplete")
                continue
            dead_since = row.get("dead_since")
            no_progress_since = row.get("no_progress_since")
            no_swarm_since = row.get("no_swarm_since")
            if (
                dead_since is None
                or no_progress_since is None
                or now - max(
                    int(dead_since),
                    int(no_progress_since),
                )
                < self.min_dead_age_sec
            ):
                reject("dead_age")
                continue
            seeds = max(
                0,
                int(torrent.get("num_seeds") or 0),
                int(torrent.get("num_complete") or 0),
            )
            raw_availability = torrent.get("availability")
            availability = (
                None
                if raw_availability is None
                else float(raw_availability)
            )
            if seeds > 0 or (
                availability is not None and availability >= 0.999999
            ):
                reject("complete_source")
                continue
            if availability is None and (
                no_swarm_since is None
                or now - int(no_swarm_since) < self.min_dead_age_sec
            ):
                reject("unavailability_unconfirmed")
                continue
            if torrent_hash in open_jobs:
                reject("open_job")
                continue
            if torrent_hash in active_claims:
                reject("active_reservation")
                continue
            host_path = self._host_path(torrent.get("content_path"))
            if host_path is None:
                reject("unsafe_path")
                continue
            if self._overlaps_other(torrent_hash, host_path, all_paths):
                reject("path_overlap")
                continue
            if not host_path.exists():
                reject("path_missing")
                continue
            try:
                allocated = self._allocated_bytes(host_path)
            except OSError:
                reject("path_inspection_failed")
                continue
            if allocated < self.min_reclaim_bytes:
                reject("below_min_reclaim")
                continue
            candidates.append(
                {
                    "hash": torrent_hash,
                    "name": str(torrent.get("name") or ""),
                    "host_path": str(host_path),
                    "content_path": str(torrent.get("content_path") or ""),
                    "allocated_bytes": int(allocated),
                    "completed_bytes": max(
                        0,
                        int(
                            torrent.get("completed_bytes")
                            or torrent.get("completed")
                            or torrent.get("downloaded")
                            or 0
                        ),
                    ),
                    "progress": float(torrent.get("progress") or 0.0),
                    "dead_since": int(dead_since),
                }
            )

        candidates.sort(
            key=lambda item: (
                -int(item["allocated_bytes"]),
                float(item["progress"]),
                str(item["hash"]),
            )
        )
        needed = max(0, int(target_free_bytes) - int(free_bytes))
        selected: list[dict[str, Any]] = []
        planned_bytes = 0
        for candidate in candidates:
            if len(selected) >= self.max_per_tick or planned_bytes >= needed:
                break
            selected.append(candidate)
            planned_bytes += int(candidate["allocated_bytes"])

        if self.dry_run:
            return CapacityReclaimResult(
                dry_run=True,
                planned=len(selected),
                planned_bytes=planned_bytes,
                candidates=selected,
                rejection_counts=rejection_counts,
            )

        reclaimed = 0
        reclaimed_bytes = 0
        errors: list[str] = []
        completed: list[dict[str, Any]] = []
        for candidate in selected:
            torrent_hash = str(candidate["hash"])
            host_path = Path(str(candidate["host_path"]))
            try:
                self.executor.qbt_post(
                    "/api/v2/torrents/stop", {"hashes": torrent_hash}
                )
                self._delete_path(host_path)
                reclaimed += 1
                reclaimed_bytes += int(candidate["allocated_bytes"])
                completed.append(candidate)
            except Exception as exc:
                errors.append(f"{torrent_hash}: {exc}")
                continue
            try:
                self.executor.qbt_post(
                    "/api/v2/torrents/recheck", {"hashes": torrent_hash}
                )
            except Exception as exc:
                errors.append(f"{torrent_hash}: {exc}")
        return CapacityReclaimResult(
            dry_run=False,
            planned=len(selected),
            reclaimed=reclaimed,
            planned_bytes=planned_bytes,
            reclaimed_bytes=reclaimed_bytes,
            candidates=completed,
            rejection_counts=rejection_counts,
            errors=errors,
        )

    def _eligibility_state(
        self,
    ) -> tuple[dict[str, dict[str, Any]], set[str], set[str]]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select sa.hash,th.dead_since,th.no_progress_since,th.no_swarm_since "
                "from scheduler_allocations sa join torrent_health th on th.hash=sa.hash "
                "where sa.desired_state='dead'"
            ).fetchall()
            placeholders = ",".join("?" for _ in OPEN_JOB_STATES)
            jobs = con.execute(
                f"select distinct hash from torrent_jobs where state in ({placeholders})",
                OPEN_JOB_STATES,
            ).fetchall()
            claims = con.execute(
                "select distinct hash from resource_reservations where state='active' "
                "and (expires_at is null or expires_at>?)",
                (int(self.now()),),
            ).fetchall()
        finally:
            con.close()
        return (
            {str(row["hash"]): dict(row) for row in rows},
            {str(row["hash"]) for row in jobs if row["hash"]},
            {str(row["hash"]) for row in claims if row["hash"]},
        )

    def _snapshot_paths(
        self, snapshots: Mapping[str, Mapping[str, Any]]
    ) -> dict[str, Path]:
        result: dict[str, Path] = {}
        for fallback_hash, raw in snapshots.items():
            torrent_hash = str(raw.get("hash") or fallback_hash)
            path = self._host_path(raw.get("content_path"))
            if path is not None:
                result[torrent_hash] = path
        return result

    def _host_path(self, raw_path: Any) -> Path | None:
        text = str(raw_path or "").strip()
        if not text:
            return None
        container_path = PurePosixPath(text)
        try:
            relative = container_path.relative_to(self.container_downloads)
        except ValueError:
            return None
        unresolved = self.host_downloads.joinpath(*relative.parts)
        if self._has_symlink_component(unresolved):
            return None
        resolved = unresolved.resolve()
        if resolved == self.managed_root or not resolved.is_relative_to(
            self.managed_root
        ):
            return None
        return resolved

    def _has_symlink_component(self, path: Path) -> bool:
        current = path
        while current != self.host_downloads and current.is_relative_to(
            self.host_downloads
        ):
            if current.exists() and current.is_symlink():
                return True
            current = current.parent
        return False

    @staticmethod
    def _overlaps_other(
        torrent_hash: str, candidate: Path, all_paths: Mapping[str, Path]
    ) -> bool:
        for other_hash, other in all_paths.items():
            if str(other_hash) == str(torrent_hash):
                continue
            if other == candidate or other.is_relative_to(candidate) or candidate.is_relative_to(other):
                return True
        return False

    @classmethod
    def _allocated_bytes(cls, path: Path) -> int:
        def allocated(item: Path) -> int:
            stat = item.lstat()
            blocks = getattr(stat, "st_blocks", None)
            return int(blocks) * 512 if blocks is not None else int(stat.st_size)

        if path.is_file():
            return allocated(path)
        total = allocated(path)
        for root, dirs, files in os.walk(path, followlinks=False):
            base = Path(root)
            total += sum(allocated(base / name) for name in dirs)
            total += sum(allocated(base / name) for name in files)
        return total

    @staticmethod
    def _delete_path(path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
