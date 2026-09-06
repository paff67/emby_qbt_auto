from __future__ import annotations

import base64
import re
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import parse_qs, urlsplit

from .hash_identity import canonical_torrent_hash


_PRECHECK_TAG = re.compile(r"^add-item-[a-f0-9]{16,64}$")


class QbtPrecheckGateway:
    """Small precheck adapter over the existing qBT client and Executor."""

    def __init__(self, qbt, executor) -> None:
        self.qbt = qbt
        self.executor = executor
        self._snapshots: dict[str, dict[str, Any]] = {}

    def set_snapshots(self, snapshots: Mapping[str, Any]) -> None:
        self._snapshots = {
            str(key): self._snapshot(value)
            for key, value in dict(snapshots).items()
        }

    def add_magnet(
        self,
        magnet: str,
        tag: str,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        raw = str(magnet or "").strip()
        if not raw.lower().startswith("magnet:?"):
            raise ValueError("magnet_uri")
        safe_tag = self._tag(tag)
        return bool(
            self._post(
                "/api/v2/torrents/add",
                {
                    "urls": raw,
                    "category": "precheck",
                    "tags": f"precheck,metadata-probe,{safe_tag},hold",
                    "stopped": "false",
                    "dlLimit": "1024",
                },
                guard,
            )
        )

    def find_by_tag(self, tag: str) -> dict[str, Any] | None:
        safe_tag = self._tag(tag)
        matches = [
            dict(snapshot)
            for snapshot in self._snapshots.values()
            if safe_tag in self._tags(snapshot)
        ]
        if len(matches) > 1:
            raise ValueError("qbt_precheck_tag_ambiguous")
        if not matches:
            return None
        torrent_hash = self._hash(matches[0].get("hash"))
        matches[0]["hash"] = torrent_hash
        return matches[0]

    def find_by_hash(self, torrent_hash: str) -> dict[str, Any] | None:
        safe_hash = self._hash(torrent_hash)
        snapshot = next(
            (
                dict(row)
                for key, row in self._snapshots.items()
                if self._hash(row.get("hash") or key) == safe_hash
            ),
            None,
        )
        if snapshot is not None:
            snapshot["hash"] = safe_hash
            return snapshot
        current = self.torrent_info(safe_hash)
        if not str(current.get("state") or "").strip():
            return None
        current_hash = self._hash(current.get("hash") or safe_hash)
        if current_hash != safe_hash:
            raise ValueError("qbt_precheck_hash_mismatch")
        current["hash"] = current_hash
        return current

    @classmethod
    def expected_hash(cls, magnet: str) -> str:
        raw = str(magnet or "").strip()
        if not raw.lower().startswith("magnet:?"):
            raise ValueError("magnet_uri")
        exact_topics = parse_qs(urlsplit(raw).query).get("xt", [])
        for topic in exact_topics:
            lowered = str(topic).strip().lower()
            if lowered.startswith("urn:btih:"):
                value = lowered.rsplit(":", 1)[-1]
                if len(value) == 40 and all(c in "0123456789abcdef" for c in value):
                    return cls._hash(value)
                if len(value) == 32:
                    try:
                        decoded = base64.b32decode(value.upper()).hex()
                    except (ValueError, TypeError) as exc:
                        raise ValueError("magnet_infohash") from exc
                    return cls._hash(decoded)
            if lowered.startswith("urn:btmh:"):
                value = lowered.rsplit(":", 1)[-1]
                if value.startswith("1220"):
                    value = value[4:]
                if len(value) == 64 and all(c in "0123456789abcdef" for c in value):
                    return cls._hash(value)
        raise ValueError("magnet_infohash")

    def stop(
        self,
        torrent_hash: str,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        safe_hash = self._hash(torrent_hash)
        return bool(
            self._post(
                "/api/v2/torrents/stop", {"hashes": safe_hash}, guard
            )
        )

    def torrent_files(self, torrent_hash: str) -> list[dict[str, Any]]:
        safe_hash = self._hash(torrent_hash)
        return [dict(row) for row in self.qbt.torrent_files(safe_hash)]

    def torrent_info(self, torrent_hash: str) -> dict[str, Any]:
        safe_hash = self._hash(torrent_hash)
        return dict(self.qbt.torrent_info(safe_hash))

    def zero_file_priorities(
        self,
        torrent_hash: str,
        files: Sequence[Mapping[str, Any]],
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        indices = [row.get("index") for row in files]
        return self.set_file_priorities(
            torrent_hash, indices, 0, guard=guard
        )

    def set_file_priorities(
        self,
        torrent_hash: str,
        indices: Sequence[Any],
        priority: int,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        safe_hash = self._hash(torrent_hash)
        if type(priority) is not int or priority not in {0, 1, 6, 7}:
            raise ValueError("file_priority")
        raw_indices = list(indices)
        parsed_indices: set[int] = set()
        for raw_index in raw_indices:
            if isinstance(raw_index, bool):
                raise ValueError("file_index")
            try:
                index = int(raw_index)
            except (TypeError, ValueError) as exc:
                raise ValueError("file_index") from exc
            if index < 0:
                raise ValueError("file_index")
            parsed_indices.add(index)
        if not parsed_indices:
            raise ValueError("file_index")
        return bool(
            self._post(
                "/api/v2/torrents/filePrio",
                {
                    "hash": safe_hash,
                    "id": "|".join(str(index) for index in sorted(parsed_indices)),
                    "priority": str(priority),
                },
                guard,
            )
        )

    def remove_registration(
        self,
        torrent_hash: str,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        safe_hash = self._hash(torrent_hash)
        return bool(
            self._post(
                "/api/v2/torrents/delete",
                {"hashes": safe_hash, "deleteFiles": "false"},
                guard,
            )
        )

    def set_category(
        self,
        torrent_hash: str,
        category: str,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        safe_hash = self._hash(torrent_hash)
        value = str(category or "").strip()
        if value not in {"auto", "precheck"}:
            raise ValueError("qbt_category")
        return bool(
            self._post(
                "/api/v2/torrents/setCategory",
                {"hashes": safe_hash, "category": value},
                guard,
            )
        )

    def add_tags(
        self,
        torrent_hash: str,
        tags: str,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        return self._write_tags("addTags", torrent_hash, tags, guard)

    def remove_tags(
        self,
        torrent_hash: str,
        tags: str,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        return self._write_tags("removeTags", torrent_hash, tags, guard)

    def set_force_start(
        self,
        torrent_hash: str,
        value: bool,
        *,
        guard: Callable[[], bool] | None = None,
    ) -> bool:
        safe_hash = self._hash(torrent_hash)
        if type(value) is not bool:
            raise ValueError("force_start")
        return bool(
            self._post(
                "/api/v2/torrents/setForceStart",
                {"hashes": safe_hash, "value": str(value).lower()},
                guard,
            )
        )

    def _write_tags(
        self,
        action: str,
        torrent_hash: str,
        tags: str,
        guard: Callable[[], bool] | None,
    ) -> bool:
        safe_hash = self._hash(torrent_hash)
        parts = [part.strip() for part in str(tags or "").split(",") if part.strip()]
        if not parts or any("," in part or len(part) > 128 for part in parts):
            raise ValueError("qbt_tags")
        return bool(
            self._post(
                f"/api/v2/torrents/{action}",
                {"hashes": safe_hash, "tags": ",".join(dict.fromkeys(parts))},
                guard,
            )
        )

    def _post(
        self,
        path: str,
        payload: dict[str, Any],
        guard: Callable[[], bool] | None,
    ) -> bool:
        if guard is None:
            return bool(self.executor.qbt_post(path, payload))
        guarded = getattr(self.executor, "qbt_post_guarded", None)
        if guarded is not None:
            return bool(guarded(path, payload, guard=guard))
        if not guard():
            return False
        return bool(self.executor.qbt_post(path, payload))

    @staticmethod
    def metadata_ready(snapshot: Mapping[str, Any]) -> bool:
        explicit = snapshot.get("has_metadata")
        if explicit is False or str(explicit).strip().lower() in {"0", "false", "no"}:
            return False
        state = str(snapshot.get("state") or "").strip().lower()
        return "meta" not in state

    @staticmethod
    def all_priorities_zero(files: Sequence[Mapping[str, Any]]) -> bool:
        return bool(files) and all(int(row.get("priority") or 0) == 0 for row in files)

    @staticmethod
    def _tags(snapshot: Mapping[str, Any]) -> set[str]:
        return {
            part.strip()
            for part in str(snapshot.get("tags") or "").split(",")
            if part.strip()
        }

    @staticmethod
    def _snapshot(value: Any) -> dict[str, Any]:
        if hasattr(value, "__dict__"):
            return dict(vars(value))
        return dict(value)

    @staticmethod
    def _tag(value: str) -> str:
        tag = str(value or "").strip()
        if not _PRECHECK_TAG.fullmatch(tag):
            raise ValueError("qbt_precheck_tag")
        return tag

    @staticmethod
    def _hash(value: Any) -> str:
        torrent_hash = canonical_torrent_hash(value)
        if len(torrent_hash) not in {40, 64} or any(
            character not in "0123456789abcdef" for character in torrent_hash
        ):
            raise ValueError("torrent_hash")
        return torrent_hash
