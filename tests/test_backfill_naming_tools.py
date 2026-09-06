from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path


TOOLS = Path(__file__).parents[1] / "tools" / "gdrive_backfill"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, TOOLS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _movie(**overrides):
    row = {
        "content_id": "BBAN-582",
        "id": "BBAN-582",
        "display_title": "影片名称",
        "title": "影片名称",
        "original_title": "影片名称",
        "description": "简介",
        "release_date": "2026-05-12 00:00:00+00:00",
        "release_year": 2026,
        "runtime": 120,
        "director": "导演",
        "maker": "片商",
        "label": "",
        "series": "",
        "rating_score": 8.5,
        "rating_votes": 10,
        "poster_url": "https://example.invalid/poster.jpg",
        "cover_url": "https://example.invalid/fanart.jpg",
        "cropped_poster_url": "",
        "screenshots": "[]",
        "source_url": "https://example.invalid/movie",
    }
    row.update(overrides)
    return row


def test_normalizer_enriches_result_with_title():
    tool = _load("jav_name_normalize")

    row = tool.enrich_with_title(
        tool.normalize("489155.com@BBAN-582.mp4"),
        "影片名称",
    )

    assert row["normalized_id"] == "BBAN-582"
    assert row["metadata_title"] == "影片名称"
    assert row["display_title"] == "BBAN-582 影片名称"
    assert row["canonical_basename"] == "BBAN-582 影片名称"
    assert row["canonical_remote_dir"] == "gcrypt:/BBAN-582"


def test_nfo_title_is_id_plus_title():
    tool = _load("javinizer_db_to_sidecar")

    xml = tool.render_nfo(_movie(), [], [])

    assert "<title>BBAN-582 影片名称</title>" in xml
    assert "<originaltitle>影片名称</originaltitle>" in xml
    assert "<sorttitle>BBAN-582</sorttitle>" in xml
    assert "<id>BBAN-582</id>" in xml


def test_sidecar_writer_uses_canonical_basename_and_writes_metadata(tmp_path, monkeypatch):
    tool = _load("javinizer_db_to_sidecar")
    db = tmp_path / "javinizer.db"
    con = sqlite3.connect(db)
    con.execute(
        "create table movies(content_id text,id text,display_title text,title text,original_title text,description text,release_date text,release_year integer,runtime integer,director text,maker text,label text,series text,rating_score real,rating_votes integer,poster_url text,cover_url text,cropped_poster_url text,screenshots text,source_url text,updated_at text)"
    )
    movie = _movie(updated_at="2026-07-15")
    con.execute(
        f"insert into movies({','.join(movie)}) values({','.join('?' for _ in movie)})",
        tuple(movie.values()),
    )
    con.execute("create table genres(id integer,name text)")
    con.execute("create table movie_genres(movie_content_id text,genre_id integer)")
    con.execute(
        "create table actresses(id integer,first_name text,last_name text,japanese_name text,thumb_url text)"
    )
    con.execute("create table movie_actresses(movie_content_id text,actress_id integer)")
    con.commit()
    con.close()

    def fake_download(_url, destination, _timeout):
        path = Path(destination).with_suffix(".jpg")
        path.write_bytes(b"image")
        return path

    monkeypatch.setattr(tool, "_copy_or_download", fake_download)
    work = tmp_path / "work"

    result = tool.write_sidecar_from_db(
        work,
        "BBAN-582",
        {"JAVINIZER_DB": str(db), "JAVINIZER_DOWNLOAD_TIMEOUT": "1"},
    )

    assert (work / "BBAN-582 影片名称.nfo").exists()
    assert (work / "BBAN-582 影片名称-poster.jpg").exists()
    assert (work / "BBAN-582 影片名称-fanart.jpg").exists()
    metadata = json.loads((work / "media_metadata.json").read_text("utf-8"))
    assert metadata == {
        "normalized_id": "BBAN-582",
        "metadata_title": "影片名称",
        "display_title": "BBAN-582 影片名称",
        "canonical_basename": "BBAN-582 影片名称",
        "canonical_remote_dir": "gcrypt:/BBAN-582",
    }
    assert result["metadata"] == metadata
