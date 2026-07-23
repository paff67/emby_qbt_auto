#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_ENV = BASE_DIR / "config" / "backfill.env"
DEFAULT_ROOTS = BASE_DIR / "config" / "roots.txt"

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".iso"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

SCHEMA_COLUMNS: List[Tuple[str, str]] = [
    ("remote_root", "TEXT"), ("remote_dir", "TEXT"), ("video_name", "TEXT"),
    ("video_path", "TEXT"), ("size", "INTEGER"), ("raw_name", "TEXT"),
    ("raw_basename", "TEXT"), ("normalized_id", "TEXT"), ("scrape_filename", "TEXT"),
    ("normalize_confidence", "REAL DEFAULT 0"), ("normalize_reason", "TEXT"),
    ("dir_video_count", "INTEGER DEFAULT 1"), ("has_nfo", "INTEGER DEFAULT 0"),
    ("has_poster", "INTEGER DEFAULT 0"), ("has_fanart", "INTEGER DEFAULT 0"),
    ("status", "TEXT"), ("attempts", "INTEGER DEFAULT 0"), ("last_error", "TEXT"),
    ("created_at", "TEXT"), ("updated_at", "TEXT"),
]

def parse_env_file(path: Path = DEFAULT_ENV) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            env[key.strip()] = val.strip().strip('"').strip("'")
    env.update({k: v for k, v in os.environ.items() if k in env or k.startswith(("RCLONE_", "SCRAPE_", "SCRAPER_", "JAVINIZER_", "NORMALIZE_", "OVERWRITE_", "MAX_", "WORK_", "STATE_", "LOG_", "KEEP_", "FORCE_", "DOWNLOAD_", "GENERATE_", "JAVDB_", "CONTACT_", "CHAPTER_", "UPLOAD_"))})
    return env

def env_int(env: Dict[str, str], key: str, default: int) -> int:
    try: return int(str(env.get(key, default)).strip())
    except Exception: return default

def env_float(env: Dict[str, str], key: str, default: float) -> float:
    try: return float(str(env.get(key, default)).strip())
    except Exception: return default

def env_bool(env: Dict[str, str], key: str, default: bool = False) -> bool:
    return str(env.get(key, "1" if default else "0")).strip().lower() in {"1", "true", "yes", "on", "y"}

def load_roots(path: Path = DEFAULT_ROOTS) -> List[str]:
    if not path.exists(): return []
    return [line.strip().rstrip("/") for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.strip().startswith("#")]

def ensure_dirs(env: Dict[str, str]) -> None:
    for key in ("WORK_ROOT", "LOG_DIR"):
        Path(env[key]).mkdir(parents=True, exist_ok=True)
    Path(env["STATE_DB"]).parent.mkdir(parents=True, exist_ok=True)

def connect_db(env: Dict[str, str]) -> sqlite3.Connection:
    ensure_dirs(env)
    conn = sqlite3.connect(env["STATE_DB"])
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    return conn

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    cols = {row[1] for row in conn.execute("PRAGMA table_info(items)")}
    for name, typ in SCHEMA_COLUMNS:
        if name not in cols:
            conn.execute(f"ALTER TABLE items ADD COLUMN {name} {typ}")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_video_path ON items(video_path)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_status ON items(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_remote_dir ON items(remote_dir)")
    conn.commit()

def remote_join(base: str, *parts: str) -> str:
    out = base.rstrip("/")
    clean = "/".join(str(p).strip("/") for p in parts if str(p).strip("/") not in ("", "."))
    return out if not clean else out + "/" + clean

def basename_no_ext(name: str) -> str:
    return Path(name).stem

def ext_lower(name: str) -> str:
    return Path(name).suffix.lower()

def rclone_base_cmd(env: Dict[str, str]) -> List[str]:
    cmd = [env.get("RCLONE_BIN", "/usr/bin/rclone")]
    cfg = env.get("RCLONE_CONFIG", "").strip()
    if cfg: cmd += ["--config", cfg]
    return cmd

def run_cmd(args: Sequence[str], timeout: Optional[int] = None, env_extra: Optional[Dict[str, str]] = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    if env_extra: env.update(env_extra)
    return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout, env=env)

def human_bytes(num: int) -> str:
    n = float(num or 0)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if abs(n) < 1024.0 or unit == "TiB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024.0
