from __future__ import annotations

import copy
import threading
from collections.abc import Iterable, Mapping
from typing import Any

from .models import TorrentSnapshot


def _merge_mapping(base: Mapping[str, Any], delta: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge a qBT delta without retaining caller-owned objects."""
    merged = copy.deepcopy(dict(base))
    for key, value in delta.items():
        current = merged.get(key)
        if isinstance(current, Mapping) and isinstance(value, Mapping):
            merged[key] = _merge_mapping(current, value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


class TorrentRawSnapshotStore:
    """Thread-safe raw qBT snapshot cache with correct partial-delta semantics.

    qBT's sync API omits unchanged fields in delta responses.  Retaining raw
    rows is therefore essential: converting each delta directly into a typed
    snapshot would replace omitted fields with model defaults.
    """

    def __init__(self) -> None:
        self._raw: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def replace_full(self, torrents: Mapping[str, Mapping[str, Any]]) -> None:
        replacement: dict[str, dict[str, Any]] = {}
        for torrent_hash, row in torrents.items():
            key = str(torrent_hash)
            replacement[key] = _merge_mapping({}, row)
            replacement[key]["hash"] = key
        with self._lock:
            self._raw = replacement

    def apply_delta(
        self,
        torrents: Mapping[str, Mapping[str, Any]],
        removed: Iterable[str],
    ) -> None:
        with self._lock:
            for torrent_hash in removed:
                self._raw.pop(str(torrent_hash), None)
            for torrent_hash, delta in torrents.items():
                key = str(torrent_hash)
                self._raw[key] = _merge_mapping(self._raw.get(key, {}), delta)
                self._raw[key]["hash"] = key

    def snapshots(self) -> dict[str, TorrentSnapshot]:
        with self._lock:
            return {
                torrent_hash: TorrentSnapshot.from_qbt(copy.deepcopy(row))
                for torrent_hash, row in self._raw.items()
            }
