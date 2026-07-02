#!/usr/bin/env python3
"""Shared junk-file classification rules for the qBT automation stack.

The text/link cleaner is intentionally *not* suffix-only: a file must have a
text/link suffix and also match an advertising/jump-site filename pattern.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

TEXT_LINK_EXTS = {".html", ".htm", ".url", ".txt"}

_TEXT_LINK_PATTERNS = [
    ("latest_address", r"最新地址|最新地址获取|最新地址獲取|最新位址|最新位址获取|最新位址獲取|收藏不迷路"),
    ("aggregate_live", r"聚合全网.*直播|聚合全網.*直播|全网.*直播|全網.*直播|社[区區].*最新.*情[报報]"),
    ("forum_promo", r"論壇文宣|论坛文宣|草榴|1024|t66y|2048|169bbs|sex169|sex8|avlang|waikeung|powered\s*by\s*discuz"),
    ("adult_app_promo", r"麻豆传媒|麻豆傳媒|快手直播|91国产|91國產|91av|成人抖音|泡芙短视频|杏吧|含羞草|全国外围|全國外圍|一键约炮|一鍵約炮|91约炮|91約炮"),
    ("gambling_sports_promo", r"体育|體育|电竞|電競|赌场|賭場|博彩|世足|官方指定|福利机制|福利機制"),
    ("known_ad_domain", r"(489155|996gg|x18r|manko|tuu\d*|g6575|cl656|vip11\d{2})\s*(?:\.|点)?\s*(?:com|cc|tv|fun|net)?"),
    ("generic_domain_link", r"[a-z0-9][a-z0-9-]{1,30}\s*\.\s*(?:com|cc|tv|fun|net|org|top|xyz|vip|app)\b"),
]


def _norm_path(name: str) -> str:
    text = str(name or "").replace("\\", "/").lstrip("/")
    return text


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", text).lower()


def text_link_junk_reason(name: str) -> Optional[str]:
    """Return the matched rule name when a text/link file is ad/jump junk.

    A plain ``readme.txt`` or ``index.html`` must not match. This avoids the
    previous suffix-only behavior for .html/.url/.txt files.
    """

    text = _norm_path(name)
    ext = Path(text).suffix.lower()
    if ext not in TEXT_LINK_EXTS:
        return None

    base = os.path.basename(text)
    haystacks = [text.lower(), base.lower(), _compact(text), _compact(base)]
    for label, pattern in _TEXT_LINK_PATTERNS:
        rx = re.compile(pattern, re.IGNORECASE)
        if any(rx.search(h) for h in haystacks):
            return label
    return None


def is_text_link_junk(name: str) -> bool:
    return text_link_junk_reason(name) is not None
