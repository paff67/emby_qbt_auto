from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import PurePosixPath

_INVALID = re.compile(r'[\\/:*?"<>|\x00-\x1f]+')
_SPACE = re.compile(r"\s+")
_PART = re.compile(
    r"(?i)(?:[._ -]?((?:cd|disc|disk|part|pt)[._ -]?[0-9]{1,2}|上|下|前編|後編))$"
)


@dataclass(frozen=True)
class CanonicalMediaName:
    normalized_id: str
    metadata_title: str
    display_title: str
    canonical_basename: str

    def remote_dir(self, remote: str = "gcrypt:") -> str:
        return f"{remote.rstrip('/')}/{self.normalized_id}"


def canonical_media_name(
    normalized_id: str,
    metadata_title: str,
    *,
    max_basename_chars: int = 120,
) -> CanonicalMediaName:
    media_id = _SPACE.sub(
        "-", unicodedata.normalize("NFKC", str(normalized_id)).strip()
    ).upper()
    if not media_id:
        raise ValueError("normalized_id is required")

    title = _SPACE.sub(
        " ", unicodedata.normalize("NFKC", str(metadata_title)).strip()
    ) or media_id
    display = f"{media_id} {title}" if title != media_id else media_id
    safe = _SPACE.sub(" ", _INVALID.sub("_", display)).strip(" .")
    limit = max(len(media_id) + 1, int(max_basename_chars))
    safe = safe[:limit].rstrip(" .")
    return CanonicalMediaName(media_id, title, display, safe)


def canonical_file_basename(
    name: CanonicalMediaName,
    source_filename: str,
    *,
    collision_digest: str = "",
) -> str:
    stem = PurePosixPath(str(source_filename).replace("\\", "/")).stem
    match = _PART.search(stem)
    suffix = re.sub(r"[._ ]+", "", match.group(1)).upper() if match else ""
    if collision_digest:
        suffix = str(collision_digest).lower()[:8]
    return f"{name.canonical_basename}-{suffix}" if suffix else name.canonical_basename
