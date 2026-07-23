#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import math
import os
import re
import shutil
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import unquote, urlparse

from backfill_common import IMAGE_EXTS, basename_no_ext, env_bool, env_int

EXTRAFANART_DIR = "extrafanart"
CHAPTER_THUMB_DIR = "chapter-thumbs"
EXTRAFANART_DIR_NAMES = {"extrafanart", "extra-fanart", "extra_fanart", "screenshots", "samples"}
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Referer": "https://javdb.com/",
}


def _walk_files(work_dir: Path) -> List[Path]:
    out: List[Path] = []
    for p in work_dir.rglob("*"):
        if p.is_file() and "sidecar_out" not in p.parts and not p.name.startswith("."):
            out.append(p)
    return out


def _is_extra_asset(path: Path) -> bool:
    return any(part.lower() in EXTRAFANART_DIR_NAMES or part.lower() == CHAPTER_THUMB_DIR for part in path.parts)


def _score(path: Path, scrape_base: str, kind: str) -> int:
    n = path.name.lower()
    sb = scrape_base.lower()
    score = 0
    if n.startswith(sb):
        score += 50
    if kind == "nfo":
        if n == f"{sb}.nfo":
            score += 100
        elif n.endswith(".nfo"):
            score += 20
    elif kind == "poster":
        if n in {"poster.jpg", "poster.jpeg", "folder.jpg", "folder.jpeg"}:
            score += 100
        if any(x in n for x in ["poster", "folder", "cover", "front", "封面", "海报"]):
            score += 60
        if n == f"{sb}.jpg":
            score += 40
    elif kind == "fanart":
        if n in {"fanart.jpg", "fanart.jpeg", "background.jpg", "background.jpeg"}:
            score += 100
        if any(x in n for x in ["fanart", "background", "backdrop"]):
            score += 70
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        score += 8
    return score


def _pick(cands: List[Path], scrape_base: str, kind: str) -> Optional[Path]:
    return None if not cands else sorted(cands, key=lambda p: (_score(p, scrape_base, kind), -len(str(p))), reverse=True)[0]


def _image_out_name(raw_base: str, role: str, src: Path) -> str:
    ext = src.suffix.lower()
    if ext == ".jpeg":
        ext = ".jpg"
    if ext not in IMAGE_EXTS:
        ext = ".jpg"
    return f"{raw_base}-{role}{ext}"


def _extract_fanart_urls(nfo_path: Optional[Path]) -> List[str]:
    if not nfo_path or not nfo_path.exists():
        return []
    text = nfo_path.read_text(encoding="utf-8", errors="ignore")
    urls: List[str] = []
    fanart_blocks = re.findall(r"<fanart\b[^>]*>(.*?)</fanart>", text, flags=re.I | re.S)
    for block in fanart_blocks:
        urls.extend(re.findall(r"<thumb\b[^>]*>(.*?)</thumb>", block, flags=re.I | re.S))
    if not urls:
        for raw in re.findall(r"<thumb\b[^>]*>(.*?)</thumb>", text, flags=re.I | re.S):
            if re.search(r"/(samples|sample|screenshots?)/", raw, flags=re.I):
                urls.append(raw)
    seen = set()
    cleaned: List[str] = []
    for raw in urls:
        url = html.unescape(re.sub(r"<.*?>", "", raw).strip())
        if not url or url in seen:
            continue
        if not re.match(r"^(https?|file)://", url, flags=re.I):
            continue
        seen.add(url)
        cleaned.append(url)
    return cleaned


def _ext_from_url(url: str, content_type: str = "") -> str:
    suffix = Path(unquote(urlparse(url).path)).suffix.lower()
    if suffix == ".jpeg":
        return ".jpg"
    if suffix in IMAGE_EXTS:
        return suffix
    ct = content_type.lower()
    if "png" in ct:
        return ".png"
    if "webp" in ct:
        return ".webp"
    return ".jpg"


def _copy_or_download(url: str, dst_without_ext: Path, timeout: int) -> Optional[Path]:
    parsed = urlparse(url)
    if parsed.scheme == "file":
        src = Path(unquote(parsed.path))
        if not src.exists():
            return None
        ext = src.suffix.lower() if src.suffix.lower() in IMAGE_EXTS else ".jpg"
        if ext == ".jpeg":
            ext = ".jpg"
        dst = dst_without_ext.with_suffix(ext)
        shutil.copy2(src, dst)
        return dst
    req = urllib.request.Request(url, headers=HTTP_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        content_type = resp.headers.get("Content-Type", "")
    if not data:
        return None
    ext = _ext_from_url(url, content_type)
    dst = dst_without_ext.with_suffix(ext)
    dst.write_bytes(data)
    return dst


def _download_preview_images(nfo_path: Optional[Path], out_dir: Path, env: Dict[str, str], preview_dir_name: str = EXTRAFANART_DIR) -> List[Path]:
    if not env_bool(env, "DOWNLOAD_JAVDB_PREVIEWS", True):
        return []
    urls = _extract_fanart_urls(nfo_path)
    if not urls:
        return []
    max_items = max(0, env_int(env, "JAVDB_PREVIEW_MAX", 24))
    if max_items:
        urls = urls[:max_items]
    timeout = max(3, env_int(env, "JAVDB_PREVIEW_TIMEOUT_SEC", 30))
    preview_dir = out_dir / preview_dir_name
    preview_dir.mkdir(parents=True, exist_ok=True)
    downloaded: List[Path] = []
    errors: List[str] = []
    start_idx = 1
    for idx, url in enumerate(urls, start_idx):
        try:
            dst = _copy_or_download(url, preview_dir / f"fanart{idx}", timeout)
            if dst and dst.exists() and dst.stat().st_size > 0:
                downloaded.append(dst)
        except Exception as exc:  # non-fatal; keep main NFO/poster/fanart path working
            errors.append(f"{url}: {exc}")
    if errors:
        (out_dir / "preview_download_errors.log").write_text("\n".join(errors) + "\n", encoding="utf-8")
    return downloaded


def _copy_existing_extrafanart(files: Iterable[Path], out_dir: Path, preview_dir_name: str = EXTRAFANART_DIR) -> List[Path]:
    existing = sorted([p for p in files if p.suffix.lower() in IMAGE_EXTS and _is_extra_asset(p)], key=lambda p: str(p).lower())
    copied: List[Path] = []
    if not existing:
        return copied
    preview_dir = out_dir / preview_dir_name
    preview_dir.mkdir(parents=True, exist_ok=True)
    for idx, src in enumerate(existing, 1):
        ext = ".jpg" if src.suffix.lower() == ".jpeg" else src.suffix.lower()
        if ext not in IMAGE_EXTS:
            ext = ".jpg"
        dst = preview_dir / f"fanart{idx}{ext}"
        shutil.copy2(src, dst)
        copied.append(dst)
    return copied


def _make_contact_sheet_pillow(images: List[Path], dst: Path, columns: int, cell_w: int, cell_h: int) -> bool:
    try:
        from PIL import Image, ImageOps  # type: ignore
    except Exception:
        return False
    rows = int(math.ceil(len(images) / columns))
    sheet = Image.new("RGB", (columns * cell_w, rows * cell_h), (0, 0, 0))
    for idx, src in enumerate(images):
        try:
            im = Image.open(src).convert("RGB")
            im.thumbnail((cell_w, cell_h), Image.Resampling.LANCZOS)
            padded = ImageOps.pad(im, (cell_w, cell_h), method=Image.Resampling.LANCZOS, color=(0, 0, 0))
            x = (idx % columns) * cell_w
            y = (idx // columns) * cell_h
            sheet.paste(padded, (x, y))
        except Exception:
            continue
    sheet.save(dst, format="JPEG", quality=88)
    return dst.exists() and dst.stat().st_size > 0


def _make_contact_sheet_ffmpeg(images: List[Path], dst: Path, columns: int, cell_w: int, cell_h: int) -> bool:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False
    rows = int(math.ceil(len(images) / columns))
    tmp = dst.parent / f".contact-sheet-{int(time.time() * 1000)}-{os.getpid()}"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        for idx, src in enumerate(images, 1):
            frame = tmp / f"frame{idx:04d}.jpg"
            if src.suffix.lower() in {".jpg", ".jpeg"}:
                shutil.copy2(src, frame)
            else:
                proc = subprocess.run([ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-i", str(src), str(frame)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
                if proc.returncode != 0:
                    return False
        vf = f"scale={cell_w}:{cell_h}:force_original_aspect_ratio=decrease,pad={cell_w}:{cell_h}:(ow-iw)/2:(oh-ih)/2:black,tile={columns}x{rows}:padding=6:margin=6:color=black"
        proc = subprocess.run([ffmpeg, "-y", "-hide_banner", "-loglevel", "error", "-framerate", "1", "-start_number", "1", "-i", str(tmp / "frame%04d.jpg"), "-vf", vf, "-frames:v", "1", "-q:v", "3", str(dst)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=90)
        return proc.returncode == 0 and dst.exists() and dst.stat().st_size > 0
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _make_contact_sheet(images: List[Path], dst: Path, env: Dict[str, str]) -> bool:
    if not images or not env_bool(env, "GENERATE_CONTACT_SHEET", True):
        return False
    columns = max(1, env_int(env, "CONTACT_SHEET_COLUMNS", 4))
    cell_w = max(160, env_int(env, "CONTACT_SHEET_CELL_WIDTH", 320))
    cell_h = max(90, env_int(env, "CONTACT_SHEET_CELL_HEIGHT", 180))
    if _make_contact_sheet_pillow(images, dst, columns, cell_w, cell_h):
        return True
    return _make_contact_sheet_ffmpeg(images, dst, columns, cell_w, cell_h)


def _copy_chapter_thumbs(images: List[Path], out_dir: Path, env: Dict[str, str], chapter_dir_name: str = CHAPTER_THUMB_DIR) -> List[Path]:
    if not images or not env_bool(env, "GENERATE_CHAPTER_THUMBS", True):
        return []
    max_items = max(0, env_int(env, "CHAPTER_THUMB_MAX", 24))
    selected = images[:max_items] if max_items else images
    chapter_dir = out_dir / chapter_dir_name
    chapter_dir.mkdir(parents=True, exist_ok=True)
    copied: List[Path] = []
    for idx, src in enumerate(selected, 1):
        ext = ".jpg" if src.suffix.lower() == ".jpeg" else src.suffix.lower()
        if ext not in IMAGE_EXTS:
            ext = ".jpg"
        dst = chapter_dir / f"chapter{idx:03d}{ext}"
        shutil.copy2(src, dst)
        copied.append(dst)
    return copied


def normalize_outputs(work_dir: str | Path, scrape_filename: str, raw_basename: str, single_video_dir: bool = True, env: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    env = env or {}
    wd = Path(work_dir)
    out_dir = wd / "sidecar_out"
    out_dir.mkdir(parents=True, exist_ok=True)
    scrape_base = basename_no_ext(scrape_filename)
    files = _walk_files(wd)
    nfo = _pick([p for p in files if p.suffix.lower() == ".nfo"], scrape_base, "nfo")
    images = [p for p in files if p.suffix.lower() in IMAGE_EXTS and not _is_extra_asset(p)]
    poster = _pick(images, scrape_base, "poster")
    fanart = _pick([p for p in images if p != poster], scrape_base, "fanart")
    results: List[Dict[str, str]] = []
    if nfo:
        dst = out_dir / f"{raw_basename}.nfo"
        shutil.copy2(nfo, dst)
        results.append({"kind": "nfo", "path": str(dst), "name": dst.name, "source": str(nfo)})
    if poster:
        dst = out_dir / _image_out_name(raw_basename, "poster", poster)
        shutil.copy2(poster, dst)
        results.append({"kind": "poster", "path": str(dst), "name": dst.name, "source": str(poster)})
        if single_video_dir:
            generic = out_dir / ("poster.jpg" if poster.suffix.lower() in {".jpg", ".jpeg"} else f"poster{poster.suffix.lower()}")
            shutil.copy2(poster, generic)
            results.append({"kind": "poster_generic", "path": str(generic), "name": generic.name, "source": str(poster)})
    if fanart:
        dst = out_dir / _image_out_name(raw_basename, "fanart", fanart)
        shutil.copy2(fanart, dst)
        results.append({"kind": "fanart", "path": str(dst), "name": dst.name, "source": str(fanart)})
        if single_video_dir:
            generic = out_dir / ("fanart.jpg" if fanart.suffix.lower() in {".jpg", ".jpeg"} else f"fanart{fanart.suffix.lower()}")
            shutil.copy2(fanart, generic)
            results.append({"kind": "fanart_generic", "path": str(generic), "name": generic.name, "source": str(fanart)})

    preview_dir_name = EXTRAFANART_DIR if single_video_dir else f"{raw_basename}-extrafanart"
    chapter_dir_name = CHAPTER_THUMB_DIR if single_video_dir else f"{raw_basename}-chapter-thumbs"
    preview_images = _copy_existing_extrafanart(files, out_dir, preview_dir_name)
    preview_images.extend(_download_preview_images(nfo, out_dir, env, preview_dir_name))
    # De-duplicate by final path while preserving order.
    seen_preview = set()
    ordered_previews: List[Path] = []
    for p in preview_images:
        key = str(p)
        if key not in seen_preview and p.exists():
            seen_preview.add(key)
            ordered_previews.append(p)
    for p in ordered_previews:
        rel = p.relative_to(out_dir).as_posix()
        results.append({"kind": "extrafanart", "path": str(p), "name": rel, "source": str(nfo or p)})

    if ordered_previews:
        contact = out_dir / f"{raw_basename}-thumb.jpg"
        if _make_contact_sheet(ordered_previews, contact, env):
            results.append({"kind": "thumb", "path": str(contact), "name": contact.name, "source": preview_dir_name})
            if single_video_dir:
                generic = out_dir / "thumb.jpg"
                shutil.copy2(contact, generic)
                results.append({"kind": "thumb_generic", "path": str(generic), "name": generic.name, "source": preview_dir_name})
        for p in _copy_chapter_thumbs(ordered_previews, out_dir, env, chapter_dir_name):
            rel = p.relative_to(out_dir).as_posix()
            results.append({"kind": "chapter_thumb", "path": str(p), "name": rel, "source": preview_dir_name})
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description="Normalize scraper outputs into Emby-compatible sidecar filenames.")
    ap.add_argument("work_dir")
    ap.add_argument("scrape_filename")
    ap.add_argument("raw_basename")
    ap.add_argument("--multi-video-dir", action="store_true")
    ap.add_argument("--no-preview-download", action="store_true", help="Do not download JavDB fanart/thumb preview URLs from NFO")
    args = ap.parse_args()
    env = {"DOWNLOAD_JAVDB_PREVIEWS": "0" if args.no_preview_download else "1"}
    rows = normalize_outputs(args.work_dir, args.scrape_filename, args.raw_basename, not args.multi_video_dir, env=env)
    print(json.dumps(rows, ensure_ascii=False, indent=2))
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
