#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from xml.sax.saxutils import escape

from backfill_common import DEFAULT_ENV, parse_env_file
from normalize_sidecar import _copy_or_download

current = Path(os.environ.get("QBT_ORCH_CURRENT", "/opt/emby_qbt_auto/current"))
source_root = current / "src"
if source_root.exists() and str(source_root) not in sys.path:
    sys.path.insert(0, str(source_root))

from qbt_orchestrator.naming import canonical_media_name


def _text(v) -> str:
    return "" if v is None else str(v).strip()


def _xml(v) -> str:
    return escape(_text(v), {'"': '&quot;'})


def _date_only(v) -> str:
    s = _text(v)
    if not s:
        return ""
    return s.split(" ", 1)[0].split("T", 1)[0]


def _year(movie: sqlite3.Row) -> str:
    y = _text(movie["release_year"])
    if y and y != "0":
        return y
    d = _date_only(movie["release_date"])
    return d[:4] if len(d) >= 4 else ""


def _json_list(v) -> List[str]:
    s = _text(v)
    if not s:
        return []
    try:
        data = json.loads(s)
        if isinstance(data, list):
            return [_text(x) for x in data if _text(x)]
    except Exception:
        pass
    return []


def _connect_db(env: Dict[str, str]) -> sqlite3.Connection:
    db_path = Path(env.get("JAVINIZER_DB", "").strip() or Path(env.get("JAVINIZER_DATA_DIR", "/opt/qbt/gdrive-backfill/javinizer/data")) / "javinizer.db")
    if not db_path.exists():
        raise FileNotFoundError(f"Javinizer DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def load_movie(conn: sqlite3.Connection, movie_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM movies
        WHERE content_id = ? COLLATE NOCASE OR id = ? COLLATE NOCASE
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (movie_id, movie_id),
    ).fetchone()


def load_genres(conn: sqlite3.Connection, content_id: str) -> List[str]:
    return [
        _text(r["name"])
        for r in conn.execute(
            """
            SELECT g.name FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.id
            WHERE mg.movie_content_id = ?
            ORDER BY g.name
            """,
            (content_id,),
        )
        if _text(r["name"])
    ]


def _actress_name(row: sqlite3.Row) -> str:
    jp = _text(row["japanese_name"])
    if jp:
        return jp
    parts = [_text(row["first_name"]), _text(row["last_name"])]
    return " ".join(p for p in parts if p).strip()


def load_actresses(conn: sqlite3.Connection, content_id: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for r in conn.execute(
        """
        SELECT a.first_name, a.last_name, a.japanese_name, a.thumb_url
        FROM actresses a
        JOIN movie_actresses ma ON ma.actress_id = a.id
        WHERE ma.movie_content_id = ?
        ORDER BY a.id
        """,
        (content_id,),
    ):
        name = _actress_name(r)
        if name:
            out.append({"name": name, "thumb": _text(r["thumb_url"])})
    return out


def render_nfo(movie: sqlite3.Row, genres: Iterable[str], actresses: Iterable[Dict[str, str]]) -> str:
    content_id = _text(movie["content_id"] or movie["id"])
    source_title = _text(movie["display_title"] or movie["title"] or movie["original_title"] or content_id)
    name = canonical_media_name(content_id, source_title)
    title = name.display_title
    original_title = _text(movie["original_title"] or movie["title"] or source_title)
    release_date = _date_only(movie["release_date"])
    year = _year(movie)
    screenshots = _json_list(movie["screenshots"])
    lines: List[str] = ['<?xml version="1.0" encoding="UTF-8"?>', "<movie>"]
    def tag(name: str, value: str) -> None:
        if _text(value):
            lines.append(f"  <{name}>{_xml(value)}</{name}>")
    tag("title", title)
    tag("originaltitle", original_title)
    tag("sorttitle", content_id)
    tag("id", content_id)
    if content_id:
        lines.append(f'  <uniqueid type="contentid" default="true">{_xml(content_id)}</uniqueid>')
    tag("plot", _text(movie["description"]))
    tag("runtime", _text(movie["runtime"]))
    tag("year", year)
    tag("releasedate", release_date)
    tag("premiered", release_date)
    if movie["rating_score"] is not None:
        lines.extend([
            "  <ratings>",
            '    <rating name="javdb" max="10" default="true">',
            f"      <value>{_xml(movie['rating_score'])}</value>",
            f"      <votes>{_xml(movie['rating_votes'])}</votes>",
            "    </rating>",
            "  </ratings>",
        ])
    tag("director", _text(movie["director"]))
    for idx, actor in enumerate(actresses):
        lines.append("  <actor>")
        lines.append(f"    <name>{_xml(actor.get('name',''))}</name>")
        if idx:
            lines.append(f"    <order>{idx}</order>")
        if _text(actor.get("thumb")):
            lines.append(f"    <thumb>{_xml(actor.get('thumb',''))}</thumb>")
        lines.append("  </actor>")
    tag("studio", _text(movie["maker"] or movie["label"]))
    tag("maker", _text(movie["maker"]))
    tag("set", _text(movie["series"]))
    for genre in genres:
        tag("genre", genre)
    poster_url = _text(movie["poster_url"] or movie["cropped_poster_url"] or movie["cover_url"])
    if poster_url:
        lines.append(f'  <thumb aspect="poster">{_xml(poster_url)}</thumb>')
    if screenshots:
        lines.append("  <fanart>")
        for url in screenshots:
            lines.append(f"    <thumb>{_xml(url)}</thumb>")
        lines.append("  </fanart>")
    tag("website", _text(movie["source_url"]))
    lines.append("</movie>")
    return "\n".join(lines) + "\n"


def write_sidecar_from_db(work_dir: str | Path, movie_id: str, env: Optional[Dict[str, str]] = None) -> Dict[str, object]:
    env = env or parse_env_file(DEFAULT_ENV)
    wd = Path(work_dir)
    wd.mkdir(parents=True, exist_ok=True)
    conn = _connect_db(env)
    movie = load_movie(conn, movie_id)
    if not movie:
        raise RuntimeError(f"movie not found in Javinizer DB after scrape: {movie_id}")
    content_id = _text(movie["content_id"] or movie["id"] or movie_id)
    source_title = _text(movie["display_title"] or movie["title"] or movie["original_title"] or content_id)
    name = canonical_media_name(content_id, source_title)
    genres = load_genres(conn, content_id)
    actresses = load_actresses(conn, content_id)
    nfo = wd / f"{name.canonical_basename}.nfo"
    nfo.write_text(render_nfo(movie, genres, actresses), encoding="utf-8")
    written = [str(nfo)]
    errors: List[str] = []
    timeout = int(env.get("JAVDB_PREVIEW_TIMEOUT_SEC", env.get("JAVINIZER_DOWNLOAD_TIMEOUT", "30")) or 30)
    poster_url = _text(movie["poster_url"] or movie["cropped_poster_url"] or movie["cover_url"])
    fanart_url = _text(movie["cover_url"] or movie["poster_url"] or ( _json_list(movie["screenshots"])[0] if _json_list(movie["screenshots"]) else ""))
    for role, url in (("poster", poster_url), ("fanart", fanart_url)):
        if not url:
            continue
        try:
            dst = _copy_or_download(url, wd / f"{name.canonical_basename}-{role}", timeout)
            if dst:
                written.append(str(dst))
        except Exception as exc:
            errors.append(f"{role}: {exc}")
    metadata = {
        "normalized_id": name.normalized_id,
        "metadata_title": name.metadata_title,
        "display_title": name.display_title,
        "canonical_basename": name.canonical_basename,
        "canonical_remote_dir": name.remote_dir("gcrypt:"),
    }
    (wd / "media_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    conn.close()
    if errors:
        (wd / "javinizer_db_to_sidecar_errors.log").write_text("\n".join(errors) + "\n", encoding="utf-8")
    return {
        "movie_id": movie_id,
        "content_id": content_id,
        "written": written,
        "errors": errors,
        "metadata": metadata,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate Emby-compatible sidecar source files from Javinizer-Go DB after `javinizer scrape <id>`.")
    ap.add_argument("work_dir")
    ap.add_argument("movie_id")
    ap.add_argument("--env", default=str(DEFAULT_ENV))
    args = ap.parse_args()
    env = parse_env_file(Path(args.env))
    result = write_sidecar_from_db(args.work_dir, args.movie_id, env)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
