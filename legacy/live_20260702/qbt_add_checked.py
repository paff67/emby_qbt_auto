#!/usr/bin/env python3
"""Controlled qBittorrent magnet add with Google Drive/gcrypt de-dup precheck.

Records every request/event in the qBT orchestrator SQLite DB and writes
structured JSONL logs. Designed for the existing Paff qBT + gcrypt workflow.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import time
import urllib.parse
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from junk_rules import text_link_junk_reason

VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".iso"}
FINAL_STATUSES = {
    "accepted_auto",
    "duplicate_gdrive",
    "maybe_duplicate",
    "unknown_allowed",
    "unknown_hold",
}
SENSITIVE_MAGNET_KEYS = {"tr", "xs", "as", "kt", "ws", "mt"}


def now() -> int:
    return int(time.time())


def iso(ts: Optional[int] = None) -> str:
    return datetime.fromtimestamp(ts or now()).strftime("%Y-%m-%d %H:%M:%S")


def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "surrogatepass")).hexdigest()


def parse_bytes(s: str) -> int:
    raw = str(s).strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(b|kb|kib|mb|mib|gb|gib|tb|tib)?", raw)
    if not m:
        raise argparse.ArgumentTypeError(f"bad size: {s}")
    n = float(m.group(1))
    u = m.group(2) or "b"
    mult = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }[u]
    return int(n * mult)


def redacted_magnet(magnet: str) -> str:
    try:
        p = urllib.parse.urlsplit(magnet)
        if p.scheme != "magnet":
            return magnet[:300]
        q = urllib.parse.parse_qsl(p.query, keep_blank_values=True)
        safe = []
        for k, v in q:
            lk = k.lower()
            if lk in SENSITIVE_MAGNET_KEYS:
                safe.append((k, "<redacted>"))
            elif lk == "dn":
                safe.append((k, v[:160]))
            else:
                safe.append((k, v))
        return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, urllib.parse.urlencode(safe), p.fragment))
    except Exception:
        return magnet[:120] + "...<redacted>"


def extract_btih(magnet: str) -> Optional[str]:
    try:
        p = urllib.parse.urlsplit(magnet)
        xts = urllib.parse.parse_qs(p.query).get("xt", [])
    except Exception:
        xts = []
    for xt in xts:
        if xt.lower().startswith("urn:btih:"):
            val = xt.split(":")[-1].strip()
            if re.fullmatch(r"[0-9a-fA-F]{40}", val):
                return val.lower()
            if re.fullmatch(r"[A-Z2-7a-z]{32}", val):
                try:
                    return base64.b32decode(val.upper()).hex().lower()
                except Exception:
                    return None
    m = re.search(r"btih:([0-9a-fA-F]{40}|[A-Z2-7a-z]{32})", magnet)
    if m:
        val = m.group(1)
        if len(val) == 40:
            return val.lower()
        try:
            return base64.b32decode(val.upper()).hex().lower()
        except Exception:
            return None
    return None


def magnet_display_name(magnet: str) -> str:
    try:
        dn = urllib.parse.parse_qs(urllib.parse.urlsplit(magnet).query).get("dn", [""])[0]
        return urllib.parse.unquote_plus(dn)[:240]
    except Exception:
        return ""


class CheckedAdder:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.cfg = json.loads(Path(args.config).read_text())
        self.qbt = self.cfg["qbt"]
        self.paths = self.cfg["paths"]
        self.rclone = self.cfg.get("rclone", {})
        self.dedupe = self.default_dedupe_config()
        self.dedupe.update(self.cfg.get("dedupe", {}) or {})
        if args.metadata_timeout is not None:
            self.dedupe["metadata_timeout_sec"] = args.metadata_timeout
        if args.poll_interval is not None:
            self.dedupe["metadata_poll_interval_sec"] = args.poll_interval
        if args.size_tolerance is not None:
            self.dedupe["size_tolerance_ratio"] = args.size_tolerance
        if args.probe_remote:
            self.dedupe["remote_probe_on_miss"] = True
        if args.no_refresh_index:
            self.dedupe["refresh_index_on_start"] = False

        Path(self.paths["work_dir"]).mkdir(parents=True, exist_ok=True)
        self.log_file = Path(self.dedupe["log_file"])
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.paths["state_db"], timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("pragma busy_timeout=30000")
        self.init_db()
        self.normalizer = self.load_normalizer()
        if self.dedupe.get("refresh_index_on_start", True):
            self.refresh_index_if_needed(force=args.refresh_index)

    def default_dedupe_config(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "backfill_db": "/opt/qbt/gdrive-backfill/state/backfill.sqlite",
            "normalizer": "/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py",
            "log_file": "/var/log/qbt-orchestrator/add_checked.log",
            "metadata_timeout_sec": 900,
            "metadata_poll_interval_sec": 5,
            "min_video_size_mb": 100,
            "size_tolerance_ratio": 0.15,
            "remote_index_ttl_sec": 6 * 3600,
            "refresh_index_on_start": True,
            "remote_probe_on_miss": False,
            "unknown_id_action": "allow",
            "precheck_tags": ["precheck", "hold"],
            "accepted_tags": ["auto", "checked"],
            "duplicate_tags": ["hold", "duplicate", "exists-gdrive", "checked"],
            "maybe_duplicate_tags": ["hold", "maybe-duplicate", "exists-gdrive", "checked"],
            "metadata_timeout_tags": ["precheck", "metadata-timeout", "observe"],
            "unknown_allowed_tags": ["auto", "checked", "unknown-id"],
            "unknown_hold_tags": ["hold", "unknown-id", "checked"],
            "clear_tags_after_check": [
                "precheck",
                "hold",
                "auto",
                "checked",
                "duplicate",
                "maybe-duplicate",
                "exists-gdrive",
                "metadata-timeout",
                "unknown-id",
            ],
        }

    def init_db(self) -> None:
        self.db.executescript(
            """
            create table if not exists checked_add_requests (
              id integer primary key autoincrement,
              request_id text unique,
              created_at integer,
              updated_at integer,
              input_kind text,
              input_redacted text,
              input_sha256 text,
              infohash text,
              qbt_hash text,
              torrent_name text,
              total_size integer default 0,
              status text,
              decision text,
              reason text,
              normalized_ids text default '[]',
              matched_remote_paths text default '[]',
              selected_files_json text default '[]',
              qbt_tags text default '[]',
              dry_run integer default 0,
              added_by text,
              notes text
            );
            create table if not exists checked_add_events (
              id integer primary key autoincrement,
              ts integer,
              request_id text,
              level text,
              action text,
              message text,
              data_json text
            );
            create table if not exists remote_media_index (
              video_path text primary key,
              normalized_id text,
              size integer default 0,
              raw_basename text,
              status text,
              source text,
              updated_at integer
            );
            create index if not exists idx_remote_media_index_norm
              on remote_media_index(normalized_id);
            create table if not exists checked_add_kv (
              key text primary key,
              value text,
              updated_at integer
            );
            """
        )
        self.db.commit()

    def log(self, level: str, action: str, message: str, request_id: Optional[str] = None, data: Optional[Dict[str, Any]] = None) -> None:
        rec = {
            "ts": iso(),
            "level": level,
            "request_id": request_id,
            "action": action,
            "message": message,
            "data": data or {},
        }
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(jdump(rec) + "\n")

    def event(self, request_id: str, level: str, action: str, message: str, data: Optional[Dict[str, Any]] = None) -> None:
        data_json = jdump(data or {})
        self.db.execute(
            "insert into checked_add_events(ts,request_id,level,action,message,data_json) values(?,?,?,?,?,?)",
            (now(), request_id, level, action, message, data_json),
        )
        self.db.commit()
        self.log(level, action, message, request_id, data)

    def create_request(self, input_kind: str, raw_input: str, infohash: Optional[str], dry_run: bool, notes: str = "") -> str:
        request_id = f"add-{datetime.fromtimestamp(now()).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        self.db.execute(
            """
            insert into checked_add_requests(
              request_id,created_at,updated_at,input_kind,input_redacted,input_sha256,infohash,status,dry_run,added_by,notes
            ) values(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                request_id,
                now(),
                now(),
                input_kind,
                redacted_magnet(raw_input) if input_kind == "magnet" else raw_input[:500],
                sha256_text(raw_input),
                infohash,
                "received",
                1 if dry_run else 0,
                os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown",
                notes,
            ),
        )
        self.db.commit()
        self.event(request_id, "INFO", "request_received", f"received {input_kind}", {"infohash": infohash, "dry_run": dry_run})
        return request_id

    def update_request(self, request_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = now()
        cols, vals = [], []
        json_cols = {"normalized_ids", "matched_remote_paths", "selected_files_json", "qbt_tags"}
        for k, v in fields.items():
            cols.append(f"{k}=?")
            vals.append(jdump(v) if k in json_cols and not isinstance(v, str) else v)
        vals.append(request_id)
        self.db.execute(f"update checked_add_requests set {', '.join(cols)} where request_id=?", vals)
        self.db.commit()

    def load_normalizer(self):
        path = Path(self.dedupe["normalizer"])
        spec = importlib.util.spec_from_file_location("jav_name_normalize", path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load normalizer: {path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    def normalize_name(self, name: str) -> Dict[str, Any]:
        ext = Path(name).suffix or None
        return dict(self.normalizer.normalize(name, ext))

    def refresh_index_if_needed(self, force: bool = False) -> None:
        ttl = int(self.dedupe.get("remote_index_ttl_sec", 21600))
        row = self.db.execute("select value,updated_at from checked_add_kv where key='remote_index_last_refresh'").fetchone()
        last = int(row["updated_at"]) if row else 0
        if not force and last and now() - last < ttl:
            self.log("DEBUG", "remote_index_refresh_skip", "remote index fresh", None, {"age_sec": now() - last, "ttl_sec": ttl})
            return
        self.refresh_index()

    def refresh_index(self) -> int:
        backfill_db = Path(self.dedupe["backfill_db"])
        if not backfill_db.exists():
            self.log("WARN", "remote_index_refresh", "backfill db not found", None, {"path": str(backfill_db)})
            return 0
        src = sqlite3.connect(str(backfill_db))
        src.row_factory = sqlite3.Row
        rows = list(
            src.execute(
                """
                select video_path, normalized_id, size, raw_basename, status
                from items
                where normalized_id is not null and normalized_id != '' and video_path is not null and video_path != ''
                  and coalesce(status,'') not in ('missing_remote','duplicate_alias','normalize_failed')
                """
            )
        )
        t = now()
        with self.db:
            self.db.execute("delete from remote_media_index where source='backfill'")
            self.db.executemany(
                """
                insert or replace into remote_media_index(video_path,normalized_id,size,raw_basename,status,source,updated_at)
                values(?,?,?,?,?,'backfill',?)
                """,
                [(r["video_path"], r["normalized_id"], int(r["size"] or 0), r["raw_basename"], r["status"], t) for r in rows],
            )
            self.db.execute(
                "insert or replace into checked_add_kv(key,value,updated_at) values('remote_index_last_refresh',?,?)",
                (str(len(rows)), t),
            )
        self.log("INFO", "remote_index_refresh", "remote index refreshed from backfill db", None, {"rows": len(rows), "backfill_db": str(backfill_db)})
        return len(rows)

    def qcurl(self, method: str, path: str, data: Optional[Dict[str, Any]] = None, ok: Sequence[int] = (200,)) -> str:
        url = self.qbt["api_base"] + path
        cmd = ["docker", "exec", self.qbt["container"], "curl", "-sS", "-w", "\n%{http_code}", "-X", method]
        if data:
            # /torrents/add requires multipart when adding magnet URLs.
            use_form = path == "/api/v2/torrents/add"
            for k, v in data.items():
                if use_form:
                    cmd += ["-F", f"{k}={v}"]
                else:
                    cmd += ["--data-urlencode", f"{k}={v}"]
        cmd.append(url)
        p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=180)
        out = p.stdout or ""
        body, code_s = out.rsplit("\n", 1) if "\n" in out else (out, "000")
        try:
            code = int(code_s.strip())
        except Exception:
            code = 0
        if p.returncode != 0 or code not in ok:
            raise RuntimeError(f"qBT API {method} {path} failed rc={p.returncode} http={code} stderr={p.stderr.strip()} body={body[:300]}")
        return body

    def qget(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        if params:
            path += "?" + urllib.parse.urlencode(params, doseq=True)
        return self.qcurl("GET", path)

    def qjson(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        txt = self.qget(path, params)
        return json.loads(txt) if txt else None

    def qpost(self, path: str, data: Optional[Dict[str, Any]] = None, ok: Sequence[int] = (200,)) -> str:
        return self.qcurl("POST", path, data=data or {}, ok=ok)

    def stop_torrent(self, h: str) -> None:
        last = None
        for ep in ("/api/v2/torrents/stop", "/api/v2/torrents/pause"):
            try:
                self.qpost(ep, {"hashes": h})
                return
            except Exception as e:
                last = e
        raise RuntimeError(f"cannot stop torrent: {last}")

    def start_torrent(self, h: str) -> None:
        last = None
        for ep in ("/api/v2/torrents/start", "/api/v2/torrents/resume"):
            try:
                self.qpost(ep, {"hashes": h})
                return
            except Exception as e:
                last = e
        raise RuntimeError(f"cannot start torrent: {last}")

    def add_tags(self, h: str, tags: Iterable[str]) -> None:
        tags = [t for t in tags if t]
        if tags:
            self.qpost("/api/v2/torrents/addTags", {"hashes": h, "tags": ",".join(tags)})

    def remove_tags(self, h: str, tags: Iterable[str]) -> None:
        tags = [t for t in tags if t]
        if tags:
            self.qpost("/api/v2/torrents/removeTags", {"hashes": h, "tags": ",".join(tags)})

    def set_category(self, h: str, category: str) -> None:
        self.qpost("/api/v2/torrents/setCategory", {"hashes": h, "category": category})

    def set_force_start(self, h: str, value: bool) -> None:
        self.qpost("/api/v2/torrents/setForceStart", {"hashes": h, "value": "true" if value else "false"})

    def set_all_file_priority_zero(self, request_id: str, h: str, files: Sequence[Dict[str, Any]]) -> None:
        if files:
            self.qpost("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in range(len(files))), "priority": "0"})
            self.event(request_id, "INFO", "file_priority_zero", "set all file priorities to 0", {"file_count": len(files)})

    def set_selected_file_priorities(self, request_id: str, h: str, files: Sequence[Dict[str, Any]], keep_indexes: Iterable[int]) -> None:
        if not files:
            return
        text_link_junk = []
        for idx, f in enumerate(files):
            name = str(f.get("name") or "")
            reason = text_link_junk_reason(name)
            if reason:
                text_link_junk.append({"index": idx, "name": name, "reason": reason})
        junk_indexes = {int(x["index"]) for x in text_link_junk}

        valid_keep = sorted({int(i) for i in keep_indexes if isinstance(i, int) and 0 <= int(i) < len(files) and int(i) not in junk_indexes})
        if not valid_keep:
            if junk_indexes:
                self.qpost("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in sorted(junk_indexes)), "priority": "0"})
                self.event(
                    request_id,
                    "INFO",
                    "file_priority_text_link_junk",
                    "disabled text/link junk files matched by suffix plus filename regex",
                    {"junk_count": len(text_link_junk), "junk_files": text_link_junk[:50]},
                )
            msg = "no valid selected file index; disabled matched text/link junk only" if junk_indexes else "no valid selected file index; leaving qBT priorities unchanged"
            self.event(request_id, "WARN", "file_priority_keep_selected_skipped", msg, {"file_count": len(files), "text_link_junk_count": len(text_link_junk)})
            return
        all_indexes = set(range(len(files)))
        drop = sorted(all_indexes - set(valid_keep))
        if drop:
            self.qpost("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in drop), "priority": "0"})
        # Explicitly restore selected files to normal priority. This protects rechecks
        # after an earlier failed/duplicate decision or an operator repair.
        self.qpost("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in valid_keep), "priority": "1"})
        self.event(
            request_id,
            "INFO",
            "file_priority_keep_selected",
            "kept selected candidate files and disabled non-selected files",
            {
                "file_count": len(files),
                "keep_indexes": valid_keep,
                "drop_indexes": drop,
                "drop_count": len(drop),
                "text_link_junk_count": len(text_link_junk),
                "text_link_junk_files": text_link_junk[:50],
            },
        )

    def torrent_info(self, h: str) -> Optional[Dict[str, Any]]:
        rows = self.qjson("/api/v2/torrents/info", {"hashes": h}) or []
        return rows[0] if rows else None

    def torrent_files(self, h: str) -> List[Dict[str, Any]]:
        try:
            return self.qjson("/api/v2/torrents/files", {"hash": h}) or []
        except Exception:
            return []

    def add_magnet_to_qbt(self, request_id: str, magnet: str, infohash: str) -> Tuple[str, bool]:
        existing = self.torrent_info(infohash)
        if existing:
            self.event(request_id, "INFO", "qbt_existing", "torrent already exists in qBT", {"hash": infohash, "state": existing.get("state"), "name": existing.get("name")})
            return infohash, False
        tags = ",".join(self.dedupe.get("precheck_tags", ["precheck", "hold"]))
        data = {
            "urls": magnet,
            "savepath": self.qbt.get("save_path", "/downloads/active"),
            "tags": tags,
            "paused": "false",
            "stopped": "false",
            "autoTMM": "false",
        }
        self.event(request_id, "INFO", "qbt_add_start", "adding magnet to qBT for metadata precheck", {"hash": infohash, "savepath": data["savepath"], "tags": tags})
        body = self.qcurl("POST", "/api/v2/torrents/add", data=data)
        if "Fails" in body:
            existing = self.torrent_info(infohash)
            if existing:
                self.event(request_id, "WARN", "qbt_add_duplicate", "qBT add returned Fails but torrent exists", {"body": body[:120]})
                return infohash, False
            raise RuntimeError(f"qBT add failed: {body[:300]}")
        self.event(request_id, "INFO", "qbt_add_done", "magnet submitted to qBT", {"body": body[:120]})
        return infohash, True

    def wait_metadata(self, request_id: str, h: str) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], bool]:
        timeout = int(self.dedupe.get("metadata_timeout_sec", 900))
        interval = max(1, int(self.dedupe.get("metadata_poll_interval_sec", 5)))
        deadline = time.time() + timeout
        poll = 0
        last_info = None
        while True:
            poll += 1
            info = self.torrent_info(h)
            files = self.torrent_files(h) if info else []
            last_info = info
            file_ready = bool(files) and any((f.get("name") or "") for f in files)
            self.event(
                request_id,
                "DEBUG",
                "metadata_poll",
                "poll qBT metadata",
                {
                    "poll": poll,
                    "state": info.get("state") if info else None,
                    "name": info.get("name") if info else None,
                    "progress": info.get("progress") if info else None,
                    "completed": info.get("completed") if info else None,
                    "dlspeed": info.get("dlspeed") if info else None,
                    "file_count": len(files),
                },
            )
            if file_ready:
                try:
                    self.stop_torrent(h)
                    self.event(request_id, "INFO", "metadata_ready_stop", "metadata ready; stopped torrent before decision", {"file_count": len(files)})
                except Exception as e:
                    self.event(request_id, "WARN", "metadata_ready_stop_failed", "metadata ready but stop failed", {"error": str(e)})
                return info, files, True
            if timeout <= 0 or time.time() >= deadline:
                return last_info, files, False
            if info and str(info.get("state") or "").lower().startswith("stopped"):
                try:
                    self.start_torrent(h)
                    self.event(request_id, "INFO", "metadata_start", "started stopped torrent to fetch metadata", {"state": info.get("state")})
                except Exception as e:
                    self.event(request_id, "WARN", "metadata_start_failed", "failed to start torrent for metadata", {"error": str(e)})
            time.sleep(interval)

    def candidate_files(self, files: Sequence[Dict[str, Any]], torrent_name: str = "") -> List[Dict[str, Any]]:
        min_size = int(float(self.dedupe.get("min_video_size_mb", 100)) * 1024 * 1024)
        videos = []
        for idx, f in enumerate(files):
            name = str(f.get("name") or "")
            size = int(f.get("size") or 0)
            if Path(name).suffix.lower() in VIDEO_EXTS:
                videos.append({"index": idx, "name": name, "size": size, "progress": f.get("progress")})
        chosen = [v for v in videos if int(v.get("size") or 0) >= min_size] or videos
        chosen.sort(key=lambda x: int(x.get("size") or 0), reverse=True)
        return chosen or ([{"index": -1, "name": torrent_name, "size": 0, "progress": None}] if torrent_name else [])

    def remote_matches_for_id(self, normalized_id: str) -> List[Dict[str, Any]]:
        rows = self.db.execute(
            """
            select video_path, normalized_id, size, raw_basename, status, source, updated_at
            from remote_media_index
            where normalized_id=?
            order by size desc, video_path
            """,
            (normalized_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def probe_remote_id(self, request_id: str, normalized_id: str) -> List[Dict[str, Any]]:
        if not self.dedupe.get("remote_probe_on_miss", False):
            return []
        remote = self.rclone.get("remote", "gcrypt:")
        cmd = [
            "rclone",
            "--config",
            self.rclone.get("config", "/root/.config/rclone/rclone.conf"),
            "lsjson",
            remote,
            "--recursive",
            "--files-only",
        ]
        self.event(request_id, "INFO", "remote_probe_start", "probing gcrypt by rclone lsjson on index miss", {"normalized_id": normalized_id})
        p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1800)
        if p.returncode != 0:
            self.event(request_id, "WARN", "remote_probe_failed", "rclone remote probe failed", {"rc": p.returncode, "stderr_tail": p.stderr[-400:]})
            return []
        try:
            data = json.loads(p.stdout or "[]")
        except Exception as e:
            self.event(request_id, "WARN", "remote_probe_parse_failed", "cannot parse rclone lsjson", {"error": str(e)})
            return []
        out, t = [], now()
        for item in data:
            path = item.get("Path") or item.get("Name") or ""
            if item.get("IsDir") or normalized_id.upper() not in path.upper() or Path(path).suffix.lower() not in VIDEO_EXTS:
                continue
            video_path = f"{remote.rstrip('/')}/{path}" if not str(remote).endswith(":") else f"{remote}{path}"
            out.append({"video_path": video_path, "normalized_id": normalized_id, "size": int(item.get("Size") or 0), "raw_basename": Path(path).stem, "status": "remote_probe", "source": "remote_probe", "updated_at": t})
        if out:
            with self.db:
                self.db.executemany(
                    "insert or replace into remote_media_index(video_path,normalized_id,size,raw_basename,status,source,updated_at) values(?,?,?,?,?,?,?)",
                    [(r["video_path"], r["normalized_id"], r["size"], r["raw_basename"], r["status"], r["source"], r["updated_at"]) for r in out],
                )
            self.event(request_id, "INFO", "remote_probe_match", "remote probe found matches", {"normalized_id": normalized_id, "count": len(out)})
        else:
            self.event(request_id, "INFO", "remote_probe_no_match", "remote probe found no match", {"normalized_id": normalized_id})
        return out

    def evaluate_candidates(self, request_id: str, candidates: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        normalized = []
        matches: List[Dict[str, Any]] = []
        tolerance = float(self.dedupe.get("size_tolerance_ratio", 0.15))
        for c in candidates:
            n = self.normalize_name(c["name"])
            rec = {**c, "normalize": n}
            normalized.append(rec)
            nid = str(n.get("normalized_id") or "")
            if not nid:
                continue
            m = self.remote_matches_for_id(nid) or self.probe_remote_id(request_id, nid)
            for r in m:
                local_size = int(c.get("size") or 0)
                remote_size = int(r.get("size") or 0)
                if local_size > 0 and remote_size > 0:
                    ratio = abs(local_size - remote_size) / max(local_size, remote_size)
                    close = ratio <= tolerance
                else:
                    ratio = None
                    close = local_size == 0 and remote_size == 0
                matches.append({"candidate": {"index": c.get("index"), "name": c.get("name"), "size": local_size}, "normalized_id": nid, "remote": r, "size_close": close, "size_diff_ratio": ratio})
        ids = sorted({str(x["normalize"].get("normalized_id")) for x in normalized if x.get("normalize", {}).get("normalized_id")})
        close_matches = [m for m in matches if m.get("size_close")]
        if close_matches:
            decision = "duplicate_gdrive"
            action = "hold"
            reason = f"normalized_id already exists in Google Drive index with close size: {', '.join(sorted({m['normalized_id'] for m in close_matches}))}"
        elif matches:
            decision = "maybe_duplicate"
            action = "hold"
            reason = f"same normalized_id exists in Google Drive index but size differs/unknown: {', '.join(sorted({m['normalized_id'] for m in matches}))}"
        elif ids:
            decision = "accepted_auto"
            action = "auto"
            reason = f"no Google Drive match for normalized_id: {', '.join(ids)}"
        else:
            if str(self.dedupe.get("unknown_id_action", "allow")) == "hold":
                decision, action, reason = "unknown_hold", "hold", "no normalized_id recognized; held by policy"
            else:
                decision, action, reason = "unknown_allowed", "auto", "no normalized_id recognized; allowed by policy"
        return {"decision": decision, "action": action, "reason": reason, "normalized": normalized, "normalized_ids": ids, "matches": matches}

    def previous_final_for_hash(self, infohash: str) -> Optional[sqlite3.Row]:
        return self.db.execute(
            """
            select * from checked_add_requests
            where infohash=? and status in (%s)
            order by updated_at desc limit 1
            """ % ",".join("?" for _ in FINAL_STATUSES),
            [infohash, *sorted(FINAL_STATUSES)],
        ).fetchone()

    def finalize_qbt(self, request_id: str, h: str, decision: str, files: Sequence[Dict[str, Any]], keep_indexes: Optional[Iterable[int]] = None) -> List[str]:
        workflow_tags = [
            "precheck",
            "hold",
            "auto",
            "checked",
            "duplicate",
            "maybe-duplicate",
            "exists-gdrive",
            "metadata-timeout",
            "unknown-id",
            "observe",
        ]
        clear = sorted({str(t) for t in list(self.dedupe.get("clear_tags_after_check", [])) + workflow_tags if t})
        if decision == "accepted_auto":
            tags = list(self.dedupe.get("accepted_tags", ["auto", "checked"]))
            self.set_selected_file_priorities(request_id, h, files, keep_indexes or [])
            self.remove_tags(h, clear)
            self.add_tags(h, tags)
            self.set_category(h, self.qbt.get("category_auto", "auto"))
            self.stop_torrent(h)
            self.event(request_id, "INFO", "qbt_enrolled_auto", "torrent enrolled into auto workflow and stopped for planner", {"hash": h, "tags": tags, "category": self.qbt.get("category_auto", "auto")})
            return tags
        if decision == "unknown_allowed":
            tags = list(self.dedupe.get("unknown_allowed_tags", ["auto", "checked", "unknown-id"]))
            self.set_selected_file_priorities(request_id, h, files, keep_indexes or [])
            self.remove_tags(h, clear)
            self.add_tags(h, tags)
            self.set_category(h, self.qbt.get("category_auto", "auto"))
            self.stop_torrent(h)
            self.event(request_id, "WARN", "qbt_enrolled_unknown", "torrent has no normalized id but policy allows auto workflow", {"hash": h, "tags": tags})
            return tags
        if decision == "metadata_timeout":
            tags = list(self.dedupe.get("metadata_timeout_tags", ["precheck", "metadata-timeout", "observe"]))
            self.remove_tags(h, clear)
            self.add_tags(h, tags)
            try:
                self.set_force_start(h, True)
                self.start_torrent(h)
                self.event(request_id, "INFO", "qbt_observe_started", "metadata timeout torrent kept running under observe for metadata acquisition", {"hash": h, "tags": tags})
            except Exception as e:
                self.event(request_id, "WARN", "qbt_observe_start_failed", "failed to start observe torrent after metadata timeout", {"hash": h, "error": str(e), "tags": tags})
            return tags
        if decision == "maybe_duplicate":
            tags = list(self.dedupe.get("maybe_duplicate_tags", ["hold", "maybe-duplicate", "exists-gdrive", "checked"]))
        elif decision == "unknown_hold":
            tags = list(self.dedupe.get("unknown_hold_tags", ["hold", "unknown-id", "checked"]))
        else:
            tags = list(self.dedupe.get("duplicate_tags", ["hold", "duplicate", "exists-gdrive", "checked"]))
        self.stop_torrent(h)
        if files:
            try:
                self.set_all_file_priority_zero(request_id, h, files)
            except Exception as e:
                self.event(request_id, "WARN", "file_priority_zero_failed", "failed to set file priorities to 0", {"error": str(e)})
        self.remove_tags(h, clear)
        self.add_tags(h, tags)
        self.event(request_id, "INFO", "qbt_held", "torrent held and excluded from auto workflow", {"hash": h, "decision": decision, "tags": tags})
        return tags

    def process_check_name(self, name: str, size: int = 0) -> Dict[str, Any]:
        rid = self.create_request("check_name", name, None, self.args.dry_run, self.args.notes or "")
        candidate = {"index": -1, "name": name, "size": size, "progress": None}
        ev = self.evaluate_candidates(rid, [candidate])
        self.update_request(rid, status=ev["decision"], decision=ev["action"], reason=ev["reason"], normalized_ids=ev["normalized_ids"], matched_remote_paths=[m["remote"] for m in ev["matches"]], selected_files_json=ev["normalized"])
        self.event(rid, "INFO", "check_name_done", ev["reason"], {"decision": ev["decision"], "action": ev["action"], "matches": len(ev["matches"])})
        return {
            "request_id": rid,
            "status": ev["decision"],
            "decision": ev["action"],
            "reason": ev["reason"],
            "qbt_hash": None,
            "normalized_ids": ev["normalized_ids"],
            "matches": ev["matches"],
            "normalized": ev["normalized"],
        }

    def process_magnet(self, magnet: str) -> Dict[str, Any]:
        infohash = extract_btih(magnet)
        rid = self.create_request("magnet", magnet, infohash, self.args.dry_run, self.args.notes or "")
        if not infohash:
            reason = "magnet has no supported btih infohash"
            self.update_request(rid, status="error", decision="reject", reason=reason)
            self.event(rid, "ERROR", "invalid_magnet", reason, {})
            return {"request_id": rid, "status": "error", "decision": "reject", "reason": reason}
        if not self.args.force:
            prev = self.previous_final_for_hash(infohash)
            if prev:
                reason = f"same infohash was already processed by {prev['request_id']} with status={prev['status']}"
                self.update_request(rid, status="duplicate_hash", decision="reject", reason=reason, qbt_hash=infohash, torrent_name=prev["torrent_name"], normalized_ids=prev["normalized_ids"] or "[]", matched_remote_paths=prev["matched_remote_paths"] or "[]", selected_files_json=prev["selected_files_json"] or "[]", qbt_tags=prev["qbt_tags"] or "[]")
                self.event(rid, "WARN", "duplicate_hash_previous", reason, {"previous_request_id": prev["request_id"], "previous_status": prev["status"]})
                return {"request_id": rid, "status": "duplicate_hash", "decision": "reject", "reason": reason, "qbt_hash": infohash}
        if self.args.dry_run:
            reason = "dry-run: magnet was not submitted to qBT, metadata unavailable"
            self.update_request(rid, status="dry_run", decision="no_add", reason=reason, qbt_hash=infohash)
            self.event(rid, "INFO", "dry_run_done", reason, {})
            return {"request_id": rid, "status": "dry_run", "decision": "no_add", "reason": reason, "qbt_hash": infohash}
        h, _added = self.add_magnet_to_qbt(rid, magnet, infohash)
        self.update_request(rid, qbt_hash=h, status="metadata_wait")
        info, files, ready = self.wait_metadata(rid, h)
        torrent_name = (info or {}).get("name") or magnet_display_name(magnet) or h
        total_size = int((info or {}).get("size") or sum(int(f.get("size") or 0) for f in files))
        self.update_request(rid, torrent_name=torrent_name, total_size=total_size)
        if not ready:
            reason = f"metadata not ready within {self.dedupe.get('metadata_timeout_sec')}s; torrent kept running under observe/precheck"
            tags = self.finalize_qbt(rid, h, "metadata_timeout", files)
            self.update_request(rid, status="metadata_timeout", decision="hold", reason=reason, qbt_tags=tags)
            self.event(rid, "WARN", "metadata_timeout", reason, {"hash": h, "name": torrent_name})
            return {"request_id": rid, "status": "metadata_timeout", "decision": "hold", "reason": reason, "qbt_hash": h, "torrent_name": torrent_name}
        candidates = self.candidate_files(files, torrent_name)
        self.event(rid, "INFO", "candidate_selected", "selected video candidates for dedupe", {"candidate_count": len(candidates), "candidates": candidates[:20]})
        ev = self.evaluate_candidates(rid, candidates)
        keep_indexes = [int(c["index"]) for c in candidates if isinstance(c.get("index"), int) and int(c.get("index")) >= 0]
        tags = self.finalize_qbt(rid, h, ev["decision"], files, keep_indexes)
        self.update_request(rid, status=ev["decision"], decision=ev["action"], reason=ev["reason"], normalized_ids=ev["normalized_ids"], matched_remote_paths=[m["remote"] for m in ev["matches"]], selected_files_json=ev["normalized"], qbt_tags=tags, torrent_name=torrent_name, total_size=total_size)
        self.event(rid, "INFO", "request_done", ev["reason"], {"status": ev["decision"], "action": ev["action"], "tags": tags, "matches": len(ev["matches"])})
        return {"request_id": rid, "status": ev["decision"], "decision": ev["action"], "reason": ev["reason"], "qbt_hash": h, "torrent_name": torrent_name, "normalized_ids": ev["normalized_ids"], "matches": ev["matches"], "tags": tags}

    def list_recent(self, limit: int) -> List[Dict[str, Any]]:
        rows = self.db.execute(
            """
            select request_id,datetime(created_at,'unixepoch','localtime') created_at,infohash,qbt_hash,torrent_name,status,decision,reason,normalized_ids,matched_remote_paths,qbt_tags
            from checked_add_requests order by id desc limit ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def status(self, request_id: str) -> Dict[str, Any]:
        req = self.db.execute("select * from checked_add_requests where request_id=?", (request_id,)).fetchone()
        if not req:
            raise SystemExit(f"request not found: {request_id}")
        events = self.db.execute(
            "select datetime(ts,'unixepoch','localtime') ts,level,action,message,data_json from checked_add_events where request_id=? order by id",
            (request_id,),
        ).fetchall()
        out = dict(req)
        out["events"] = [dict(e) for e in events]
        return out


def print_result(obj: Any, as_json: bool = False) -> None:
    if as_json:
        print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))
        return
    if isinstance(obj, list):
        for r in obj:
            print(f"{r.get('created_at','')} {r.get('request_id')} status={r.get('status')} decision={r.get('decision')} hash={str(r.get('infohash') or '')[:12]} name={r.get('torrent_name') or ''}")
            print(f"  reason: {r.get('reason') or ''}")
        return
    print(f"request_id: {obj.get('request_id')}")
    print(f"status: {obj.get('status')}")
    print(f"decision: {obj.get('decision')}")
    if obj.get("qbt_hash"):
        print(f"qbt_hash: {obj.get('qbt_hash')}")
    if obj.get("torrent_name"):
        print(f"torrent_name: {obj.get('torrent_name')}")
    if obj.get("normalized_ids"):
        print(f"normalized_ids: {', '.join(obj.get('normalized_ids') or [])}")
    print(f"reason: {obj.get('reason')}")
    matches = obj.get("matches") or []
    if matches:
        print("matches:")
        for m in matches[:20]:
            r = m.get("remote", {})
            print(f"  - {m.get('normalized_id')} close={m.get('size_close')} remote={r.get('video_path')} size={r.get('size')}")
    if obj.get("tags"):
        print(f"qbt_tags: {', '.join(obj.get('tags'))}")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Add magnet to qBT only after Google Drive duplicate precheck")
    ap.add_argument("magnets", nargs="*", help="magnet URI(s)")
    ap.add_argument("--config", default="/etc/qbt-orchestrator/config.json")
    ap.add_argument("--dry-run", action="store_true", help="record and evaluate what is possible, but do not submit magnets to qBT")
    ap.add_argument("--force", action="store_true", help="ignore previous checked_add final record for the same infohash")
    ap.add_argument("--json", action="store_true", help="print machine-readable JSON")
    ap.add_argument("--notes", default="", help="operator notes saved with the request")
    ap.add_argument("--metadata-timeout", type=int, default=None, help="override metadata wait timeout seconds")
    ap.add_argument("--poll-interval", type=int, default=None, help="override metadata poll interval seconds")
    ap.add_argument("--size-tolerance", type=float, default=None, help="close-size duplicate threshold, default 0.15")
    ap.add_argument("--refresh-index", action="store_true", help="force refresh remote index from backfill DB before processing")
    ap.add_argument("--no-refresh-index", action="store_true", help="do not refresh remote index on start")
    ap.add_argument("--probe-remote", action="store_true", help="on ID index miss, scan gcrypt: with rclone lsjson once")
    ap.add_argument("--check-name", help="test duplicate logic using a filename without touching qBT")
    ap.add_argument("--check-size", type=parse_bytes, default=0, help="optional size for --check-name, e.g. 4.2GiB")
    ap.add_argument("--refresh-index-only", action="store_true", help="refresh remote index and exit")
    ap.add_argument("--list-recent", type=int, default=0, help="show recent checked add records")
    ap.add_argument("--status", help="show one request with event trail")
    return ap


def main() -> int:
    args = build_parser().parse_args()
    app = CheckedAdder(args)
    if args.refresh_index_only:
        rows = app.refresh_index()
        print_result({"request_id": None, "status": "index_refreshed", "decision": "none", "reason": f"refreshed {rows} rows"}, args.json)
        return 0
    if args.list_recent:
        print_result(app.list_recent(args.list_recent), args.json)
        return 0
    if args.status:
        print_result(app.status(args.status), args.json)
        return 0
    if args.check_name:
        print_result(app.process_check_name(args.check_name, args.check_size), args.json)
        return 0
    if not args.magnets:
        raise SystemExit("no magnet provided; use --check-name/--list-recent/--status or pass magnet URI")
    results = [app.process_magnet(m) for m in args.magnets]
    print_result(results[0] if len(results) == 1 else results, args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
