#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

current = Path(os.environ.get("QBT_ORCH_CURRENT", "/opt/emby_qbt_auto/current"))
source_root = current / "src"
if source_root.exists() and str(source_root) not in sys.path:
    sys.path.insert(0, str(source_root))

from qbt_orchestrator.naming import canonical_media_name

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".iso"}
DOMAIN_PREFIX_RE = re.compile(r"(?i)^(?:\[[^\]]*?(?:\.com|\.net|\.org|\.me|\.tv|\.cc|\.xyz|\.top|\.hk|\.jp|\.club|\.site|\.info|\.biz|\.io|\.to|\.co|\.la|\.ru|\.cn|\.tw)[^\]]*?\]\s*)?(?:https?://)?(?:www\.)?[a-z0-9][a-z0-9.-]{0,80}\.(?:com|net|org|me|tv|cc|xyz|top|hk|jp|club|site|info|biz|io|to|co|la|ru|cn|tw)\s*@+")
BRACKET_DOMAIN_RE = re.compile(r"(?i)^\[[^\]]*?(?:\.com|\.net|\.org|\.me|\.tv|\.cc|\.xyz|\.top|\.hk|\.jp|\.club|\.site|\.info|\.biz|\.io|\.to|\.co|\.la|\.ru|\.cn|\.tw)[^\]]*?\]")
# CJK noise can be removed as substrings, but ASCII noise (HD, FHD, SUB, etc.)
# must only be removed as standalone tokens. Some real JAV prefixes contain
# those letters, e.g. NHDTC/NHDTA; substring deletion breaks IDs.
CJK_NOISE_WORDS_RE = re.compile(r"(?i)(中文字幕|中字|字幕|无码|無碼|破解|流出|合集|完整版|高清|蓝光)")
ASCII_NOISE_WORDS_RE = re.compile(r"(?i)(?<![A-Z0-9])(4K|2160P|1080P|720P|FHD|UHD|HD|UNCENSORED|LEAK|CHINESE|SUBBED|CENSORED|SUB)(?![A-Z0-9])")
TAIL_NOISE_RE = re.compile(r"(?i)(?:[-_. ]+(?:C|UC|U|CH|SUB|字幕|中字|4K|1080P|720P|HD|FHD|UHD|H264|H265|X264|X265|HEVC))+$")
# Some indexers append language/source suffixes directly to the numeric part,
# e.g. abf-063ch / ABW-358ch.  Treat compact trailing CH as noise
# only when it follows a plausible JAV id shape.
COMPACT_TRAILING_CH_RE = re.compile(r"(?i)^((?:\d{2,4}[A-Z]{2,10}|[A-Z][0-9]{1,4}|[A-Z]{2,10})[-_ ]?\d{2,7})CH$")
# Avoid treating spam/source site names or collection names as JAV IDs.
BANNED_ID_PREFIXES = {"COM", "NET", "ORG", "TUU", "FULIBL", "YCANCAN"}

def raw_basename(name: str) -> str:
    p = Path(name)
    if p.suffix.lower() in VIDEO_EXTS or p.suffix:
        return p.stem
    return name

def strip_domain_prefix(s: str) -> Tuple[str, bool]:
    changed = False
    before = None
    while before != s:
        before = s
        s2 = DOMAIN_PREFIX_RE.sub("", s).strip()
        s2 = BRACKET_DOMAIN_RE.sub("", s2).strip()
        if s2 != s:
            changed = True; s = s2
    return s, changed

def preclean(base: str) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    s = unicodedata.normalize("NFKC", base).strip()
    s2, changed = strip_domain_prefix(s)
    if changed:
        reasons.append("domain_prefix_removed"); s = s2
    s = re.sub(r"(?i)\.torrent$", "", s)
    s = re.sub(r"[\[【(（][^\]】)）]{0,60}[\]】)）]", " ", s)
    s = CJK_NOISE_WORDS_RE.sub(" ", s)
    s = ASCII_NOISE_WORDS_RE.sub(" ", s)
    s = s.replace("＠", "@").replace("＿", "_").replace("－", "-")
    s = re.sub(r"[@]+", " ", s)
    s = re.sub(r"[._\s]+", "-", s)
    s = re.sub(r"-+", "-", s).strip(" -_")
    s = TAIL_NOISE_RE.sub("", s)
    m = COMPACT_TRAILING_CH_RE.match(s)
    if m:
        s = m.group(1).upper()
        reasons.append("compact_ch_suffix_removed")
    return s, reasons

def normalize(name: str, ext: str | None = None) -> Dict[str, object]:
    raw_name = Path(name).name
    rb = raw_basename(raw_name)
    ext0 = ext or Path(raw_name).suffix or ".mp4"
    clean, reasons = preclean(rb)
    upper = clean.upper()
    candidates: List[Tuple[float, str, str]] = []
    for m in re.finditer(r"(?i)\bFC2[-_ ]?(?:PPV)?[-_ ]*(\d{5,9})\b", upper):
        candidates.append((0.98, f"FC2-PPV-{m.group(1)}", "fc2_id_matched"))
    for m in re.finditer(r"(?i)\bHEYZO[-_ ]?(\d{3,6})\b", upper):
        candidates.append((0.96, f"HEYZO-{m.group(1)}", "heyzo_id_matched"))
    for m in re.finditer(r"\b(\d{6})[-_ ]?(\d{3})\b", upper):
        candidates.append((0.84, f"{m.group(1)}-{m.group(2)}", "date_number_id_matched"))
    for m in re.finditer(r"\b(\d{2,4}[A-Z]{2,10})[-_ ]+(\d{2,7})(?!\d)\b", upper):
        candidates.append((0.95, f"{m.group(1)}-{m.group(2)}", "numeric_prefix_jav_id_matched"))
    for m in re.finditer(r"\b(\d{2,4}[A-Z]{2,10})(\d{2,7})(?!\d)\b", upper):
        candidates.append((0.88, f"{m.group(1)}-{m.group(2)}", "joined_numeric_prefix_jav_id_matched"))
    for m in re.finditer(r"\b([A-Z][0-9]{1,4})[-_ ]+(\d{2,7})(?!\d)\b", upper):
        candidates.append((0.95, f"{m.group(1)}-{m.group(2)}", "letter_digit_prefix_jav_id_matched"))
    for m in re.finditer(r"\b([A-Z][0-9]{1,4})(\d{2,7})(?!\d)\b", upper):
        candidates.append((0.88, f"{m.group(1)}-{m.group(2)}", "joined_letter_digit_prefix_jav_id_matched"))
    for m in re.finditer(r"\b([A-Z]{2,10})[-_ ]+(\d{2,7})(?!\d)\b", upper):
        prefix = m.group(1)
        if prefix not in {"HEVC", "H264", "H265", "X264", "X265", "FHD", "UHD"}:
            candidates.append((0.95, f"{prefix}-{m.group(2)}", "standard_jav_id_matched"))
    for m in re.finditer(r"\b([A-Z]{2,10})(\d{2,7})(?!\d)\b", upper):
        prefix = m.group(1)
        if prefix not in {"HEVC", "H264", "H265", "X264", "X265", "FHD", "UHD", "P"}:
            candidates.append((0.88, f"{prefix}-{m.group(2)}", "joined_jav_id_matched"))
    candidates = [c for c in candidates if c[1].split("-", 1)[0] not in BANNED_ID_PREFIXES]
    if candidates:
        conf, normalized_id, match_reason = sorted(candidates, key=lambda x: x[0], reverse=True)[0]
        reason = "_and_".join(reasons + [match_reason]) if reasons else match_reason
    else:
        conf, normalized_id, reason = 0.0, "", "no_jav_id_matched"
    suffix = ext0 if ext0.startswith(".") else "." + ext0
    scrape_filename = f"{normalized_id}{suffix.lower()}" if normalized_id else ""
    return {"raw_name": raw_name, "raw_basename": rb, "cleaned_name": clean, "normalized_id": normalized_id, "scrape_filename": scrape_filename, "confidence": round(conf, 4), "reason": reason}


def enrich_with_title(
    result: Dict[str, object],
    metadata_title: str,
    remote: str = "gcrypt:",
) -> Dict[str, object]:
    enriched = dict(result)
    name = canonical_media_name(str(enriched.get("normalized_id") or ""), metadata_title)
    enriched.update(
        {
            "normalized_id": name.normalized_id,
            "metadata_title": name.metadata_title,
            "display_title": name.display_title,
            "canonical_basename": name.canonical_basename,
            "canonical_remote_dir": name.remote_dir(remote),
        }
    )
    return enriched

def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize JAV filenames and extract stable IDs for scraping.")
    ap.add_argument("names", nargs="+", help="file names to normalize")
    ap.add_argument("--json-lines", action="store_true")
    ap.add_argument("--title", default="", help="optional metadata title used to emit the canonical media name")
    ap.add_argument("--remote", default="gcrypt:")
    args = ap.parse_args()
    rows = [normalize(n) for n in args.names]
    if args.title:
        rows = [enrich_with_title(row, args.title, args.remote) for row in rows]
    if args.json_lines:
        for row in rows: print(json.dumps(row, ensure_ascii=False))
    else:
        print(json.dumps(rows[0] if len(rows) == 1 else rows, ensure_ascii=False, indent=2))
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
