from __future__ import annotations

import re
from typing import Any, Mapping


LEGACY_ADD_TAG_RE = re.compile(r"^add-item-[a-f0-9]{16,64}$")
CURRENT_ADD_TAG_RE = re.compile(r"^add-item-[a-f0-9]{32}$")


def torrent_tags(torrent: Mapping[str, Any]) -> set[str]:
    return {
        part.strip()
        for part in str(torrent.get("tags") or "").split(",")
        if part.strip()
    }


def transient_add_tags(torrent: Mapping[str, Any]) -> set[str]:
    return {tag for tag in torrent_tags(torrent) if LEGACY_ADD_TAG_RE.fullmatch(tag)}


def has_transient_add_fence(torrent: Mapping[str, Any]) -> bool:
    return bool(transient_add_tags(torrent))


def is_gc_eligible_add_tag(tag: str) -> bool:
    return bool(CURRENT_ADD_TAG_RE.fullmatch(str(tag or "").strip()))


def is_managed_auto(torrent: Mapping[str, Any]) -> bool:
    tags = torrent_tags(torrent)
    selected = str(torrent.get("category") or "") == "auto" or "auto" in tags
    return selected and "hold" not in tags and not has_transient_add_fence(torrent)
