from __future__ import annotations

import time
from typing import Any, Callable, Mapping

from .db import readonly_connect
from .metadata_probe import MetadataProbeCoordinator


class ManagedMetadataAdopter:
    """Adopt explicitly managed qBT magnets that bypassed the durable ingress."""

    def __init__(
        self,
        repository,
        gateway,
        *,
        limit: int = 20,
        now: Callable[[], int] | None = None,
        warning_service=None,
    ) -> None:
        self.repository = repository
        self.gateway = gateway
        self.limit = max(1, int(limit))
        self.now = now or (lambda: int(time.time()))

    def tick(
        self,
        snapshots: Mapping[str, Any],
        *,
        sync_healthy: bool,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "suspended": not bool(sync_healthy),
            "adopted": [],
            "failed": [],
            "skipped": 0,
        }
        if not sync_healthy:
            return result
        registered = self._registered_hashes()
        candidates: list[tuple[str, dict[str, Any]]] = []
        for key, raw_snapshot in sorted(dict(snapshots).items()):
            snapshot = dict(raw_snapshot)
            torrent_hash = str(snapshot.get("hash") or key).strip().lower()
            if torrent_hash in registered:
                result["skipped"] += 1
                continue
            if snapshot.get("has_metadata") is not False:
                result["skipped"] += 1
                continue
            if not {"auto", "checked"}.issubset(self._tags(snapshot)):
                result["skipped"] += 1
                continue
            magnet = str(snapshot.get("magnet_uri") or "").strip()
            try:
                expected = self.gateway.expected_hash(magnet)
            except ValueError:
                result["skipped"] += 1
                continue
            if expected != torrent_hash:
                result["skipped"] += 1
                continue
            candidates.append((torrent_hash, snapshot))
            if len(candidates) >= self.limit:
                break
        if not candidates:
            return result

        identity = f"qbt-adoption-{int(self.now())}"
        batch = self.repository.open_draft(identity, identity)
        magnets = [str(snapshot["magnet_uri"]) for _, snapshot in candidates]
        self.repository.append_message(batch["id"], int(self.now()), magnets)
        self.repository.submit(batch["id"])
        items = self.repository.list_items(batch["id"])
        by_hash = {torrent_hash: snapshot for torrent_hash, snapshot in candidates}
        for item in items:
            current = self.repository.get_item(item["id"], include_raw=True)
            torrent_hash = self.gateway.expected_hash(str(current["raw_input"]))
            tag = MetadataProbeCoordinator._tag_for(current)
            try:
                self.repository.transition_item(
                    item["id"], {"received"}, "resolving", "qbt_adoption_started"
                )
                self.repository.transition_item(
                    item["id"],
                    {"resolving"},
                    "waiting_probe_slot",
                    "qbt_adopted_existing_managed",
                    {
                        "canonical_identity": torrent_hash,
                        "infohash_v1": torrent_hash,
                        "qbt_hash": torrent_hash,
                        "qbt_precheck_tag": tag,
                        "display_name": by_hash[torrent_hash].get("name"),
                        "last_error": None,
                    },
                )
                guard = self._ownership_guard(item["id"], torrent_hash, tag)
                self._require(self.gateway.stop(torrent_hash, guard=guard))
                self._require(
                    self.gateway.set_download_limit(
                        torrent_hash, 1024, guard=guard
                    )
                )
                self._require(
                    self.gateway.add_tags(
                        torrent_hash,
                        f"precheck,metadata-probe,hold,{tag}",
                        guard=guard,
                    )
                )
                self._require(
                    self.gateway.set_category(
                        torrent_hash, "precheck", guard=guard
                    )
                )
                self._require(
                    self.gateway.remove_tags(
                        torrent_hash, "auto", guard=guard
                    )
                )
                result["adopted"].append(torrent_hash)
            except (RuntimeError, ValueError):
                try:
                    self.repository.transition_item(
                        item["id"],
                        {"resolving", "waiting_probe_slot"},
                        "failed",
                        "qbt_adoption_write_failed",
                        {"last_error": "qbt_adoption_write_failed"},
                    )
                except ValueError:
                    pass
                result["failed"].append(torrent_hash)
        return result

    def _registered_hashes(self) -> set[str]:
        con = readonly_connect(self.repository.state_db)
        try:
            return {
                str(row[0]).lower()
                for row in con.execute(
                    "select coalesce(qbt_hash,canonical_identity) from bot_add_items "
                    "where coalesce(qbt_hash,canonical_identity) is not null"
                )
            }
        finally:
            con.close()

    def _ownership_guard(
        self, item_id: int, torrent_hash: str, tag: str
    ) -> Callable[[], bool]:
        def current() -> bool:
            item = self.repository.get_item(item_id)
            return (
                str(item.get("state")) == "waiting_probe_slot"
                and str(item.get("qbt_hash") or "").lower() == torrent_hash
                and str(item.get("qbt_precheck_tag") or "") == tag
            )

        return current

    @staticmethod
    def _tags(snapshot: Mapping[str, Any]) -> set[str]:
        return {
            part.strip()
            for part in str(snapshot.get("tags") or "").split(",")
            if part.strip()
        }

    @staticmethod
    def _require(value: bool) -> None:
        if not value:
            raise ValueError("qbt_write_fenced")
