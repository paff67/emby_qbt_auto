#!/usr/bin/env python3
"""Small production WebUI for qbt-add-checked.

No external web framework is required. The service binds to localhost and is
expected to be exposed through Nginx Proxy Manager at /add-checked/.
"""
from __future__ import annotations

import base64
import concurrent.futures
import html
import hmac
import json
import os
import re
import secrets
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.parse
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

APP_TITLE = "qBT Checked Add"
BASE_PATH = os.environ.get("QBT_ADD_WEBUI_BASE_PATH", "/add-checked").rstrip("/")
BIND = os.environ.get("QBT_ADD_WEBUI_BIND", "127.0.0.1")
PORT = int(os.environ.get("QBT_ADD_WEBUI_PORT", "18088"))
USER = os.environ.get("QBT_ADD_WEBUI_USER", "paff")
PASSWORD = os.environ.get("QBT_ADD_WEBUI_PASSWORD", "")
SESSION_SECRET = os.environ.get("QBT_ADD_WEBUI_SESSION_SECRET", "")
SESSION_MAX_AGE = int(os.environ.get("QBT_ADD_WEBUI_SESSION_MAX_AGE", str(7 * 86400)))
STATE_DB = os.environ.get("QBT_ADD_STATE_DB", "/var/lib/qbt-orchestrator/state.sqlite")
CLI = os.environ.get("QBT_ADD_CHECKED_CLI", "/usr/local/bin/qbt-add-checked")
LOG_FILE = os.environ.get("QBT_ADD_WEBUI_LOG", "/var/log/qbt-orchestrator/add_webui.log")
SPOOL_DIR = os.environ.get("QBT_ADD_WEBUI_SPOOL_DIR", "/var/lib/qbt-orchestrator/webui_spool")
MAX_BODY = int(os.environ.get("QBT_ADD_WEBUI_MAX_BODY", str(256 * 1024)))
JOB_SEMAPHORE = threading.Semaphore(int(os.environ.get("QBT_ADD_WEBUI_MAX_JOBS", "2")))
JOB_STDIO_LIMIT = int(os.environ.get("QBT_ADD_WEBUI_STDIO_LIMIT", "200000"))
DEFAULT_METADATA_TIMEOUT = int(os.environ.get("QBT_ADD_WEBUI_DEFAULT_METADATA_TIMEOUT", "180"))
SUBPROCESS_TIMEOUT_MARGIN = int(os.environ.get("QBT_ADD_WEBUI_TIMEOUT_MARGIN", "300"))
BATCH_CONCURRENCY = int(os.environ.get("QBT_ADD_WEBUI_BATCH_CONCURRENCY", "6"))


def now() -> int:
    return int(time.time())


def iso(ts: Optional[int] = None) -> str:
    return datetime.fromtimestamp(ts or now()).strftime("%Y-%m-%d %H:%M:%S")


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v), quote=True)


def sign_session(user: str, ts: int) -> str:
    msg = f"{user}:{ts}".encode()
    sig = hmac.new(SESSION_SECRET.encode(), msg, "sha256").hexdigest()
    raw = f"{user}:{ts}:{sig}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def verify_session(token: str) -> bool:
    if not SESSION_SECRET or not token:
        return False
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        user, ts_s, sig = raw.split(":", 2)
        ts = int(ts_s)
    except Exception:
        return False
    if user != USER or now() - ts > SESSION_MAX_AGE:
        return False
    expected = hmac.new(SESSION_SECRET.encode(), f"{user}:{ts}".encode(), "sha256").hexdigest()
    return hmac.compare_digest(sig, expected)


def cookie_value(header: str, name: str) -> str:
    for part in (header or "").split(";"):
        if "=" not in part:
            continue
        k, v = part.strip().split("=", 1)
        if k == name:
            return v
    return ""


def db() -> sqlite3.Connection:
    con = sqlite3.connect(STATE_DB, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("pragma busy_timeout=30000")
    return con


def init_db() -> None:
    with db() as con:
        con.executescript(
            """
            create table if not exists qbt_add_webui_jobs (
              job_id text primary key,
              created_at integer,
              updated_at integer,
              status text,
              kind text,
              input_summary text,
              command_summary text,
              returncode integer,
              stdout text,
              stderr text,
              request_ids text default '[]',
              error text
            );
            """
        )


def log(action: str, message: str, **data: Any) -> None:
    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "ts": iso(),
        "action": action,
        "message": message,
        "data": data,
    }
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


def redact_magnet(m: str) -> str:
    try:
        p = urllib.parse.urlsplit(m)
        if p.scheme != "magnet":
            return m[:180]
        q = []
        for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True):
            if k.lower() in {"tr", "xs", "as", "kt", "ws", "mt"}:
                q.append((k, "<redacted>"))
            elif k.lower() == "dn":
                q.append((k, v[:120]))
            else:
                q.append((k, v))
        return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, urllib.parse.urlencode(q), p.fragment))
    except Exception:
        return m[:120] + "...<redacted>"


def parse_request_ids(stdout: str) -> List[str]:
    ids = set(re.findall(r"add-\d{8}-\d{6}-[0-9a-f]{8}", stdout or ""))
    try:
        data = json.loads(stdout)
        stack = data if isinstance(data, list) else [data]
        for item in stack:
            if isinstance(item, dict) and item.get("request_id"):
                ids.add(str(item["request_id"]))
    except Exception:
        pass
    return sorted(ids)


def limited_append(current: str, addition: str, limit: int = JOB_STDIO_LIMIT) -> str:
    text = (current or "") + (addition or "")
    if len(text) > limit:
        return text[-limit:]
    return text


def command_metadata_timeout(cmd: List[str]) -> int:
    for i, part in enumerate(cmd):
        if part == "--metadata-timeout" and i + 1 < len(cmd):
            try:
                return max(1, int(cmd[i + 1]))
            except Exception:
                return DEFAULT_METADATA_TIMEOUT
    return DEFAULT_METADATA_TIMEOUT


def command_without_forced_refresh(cmd: List[str]) -> List[str]:
    """Keep --refresh-index for the first batch item only."""
    out: List[str] = []
    for part in cmd:
        if part == "--refresh-index":
            continue
        out.append(part)
    return out


def write_spool(job_id: str, payload: Dict[str, Any]) -> str:
    path = Path(SPOOL_DIR)
    path.mkdir(parents=True, exist_ok=True)
    try:
        path.chmod(0o700)
    except Exception:
        pass
    final = path / f"{job_id}.json"
    tmp = path / f"{job_id}.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.chmod(tmp, 0o600)
    os.replace(tmp, final)
    return str(final)


def job_insert(job_id: str, kind: str, input_summary: str, cmd_summary: str) -> None:
    with db() as con:
        con.execute(
            """
            insert into qbt_add_webui_jobs(job_id,created_at,updated_at,status,kind,input_summary,command_summary)
            values(?,?,?,?,?,?,?)
            """,
            (job_id, now(), now(), "queued", kind, input_summary, cmd_summary),
        )


def job_update(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    cols, vals = [], []
    for k, v in fields.items():
        cols.append(f"{k}=?")
        vals.append(v)
    vals.append(job_id)
    with db() as con:
        con.execute(f"update qbt_add_webui_jobs set {', '.join(cols)} where job_id=?", vals)


def run_job(job_id: str, cmd: List[str]) -> None:
    acquired = JOB_SEMAPHORE.acquire(blocking=False)
    if not acquired:
        job_update(job_id, status="error", error="too many running jobs; retry later")
        log("job_rejected", "too many running jobs", job_id=job_id)
        return
    try:
        job_update(job_id, status="running")
        log("job_started", "started qbt-add-checked job", job_id=job_id)
        p = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=24 * 3600)
        request_ids = parse_request_ids(p.stdout)
        status = "done" if p.returncode == 0 else "error"
        job_update(
            job_id,
            status=status,
            returncode=p.returncode,
            stdout=(p.stdout or "")[-200000:],
            stderr=(p.stderr or "")[-200000:],
            request_ids=json.dumps(request_ids, ensure_ascii=False),
            error="" if p.returncode == 0 else f"returncode={p.returncode}",
        )
        log("job_finished", "finished qbt-add-checked job", job_id=job_id, returncode=p.returncode, request_ids=request_ids)
    except Exception as e:
        job_update(job_id, status="error", error=str(e))
        log("job_error", "job raised exception", job_id=job_id, error=str(e))
    finally:
        JOB_SEMAPHORE.release()


def run_batch_job(job_id: str, base_cmd: List[str], magnets: List[str]) -> None:
    acquired = JOB_SEMAPHORE.acquire(blocking=False)
    if not acquired:
        job_update(job_id, status="error", error="too many running jobs; retry later")
        log("job_rejected", "too many running jobs", job_id=job_id)
        return

    stdout_acc = ""
    stderr_acc = ""
    request_ids: List[str] = []
    ids_seen = set()
    failures = 0
    processed = 0
    total = len(magnets)
    item_timeout = max(command_metadata_timeout(base_cmd) + SUBPROCESS_TIMEOUT_MARGIN, 60)
    concurrency = max(1, min(BATCH_CONCURRENCY, total or 1))
    acc_lock = threading.Lock()

    def run_one(idx: int, magnet: str):
        cmd = (base_cmd if idx == 1 else command_without_forced_refresh(base_cmd)) + [magnet]
        redacted = redact_magnet(magnet)
        log("batch_item_started", "started checked-add item", job_id=job_id, index=idx, total=total, input=redacted)
        try:
            p = subprocess.run(
                cmd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=item_timeout,
            )
            rc = p.returncode
            out = p.stdout or ""
            err = p.stderr or ""
        except subprocess.TimeoutExpired as e:
            rc = 124
            out = e.stdout or ""
            err = (e.stderr or "") + f"\nsubprocess timeout after {item_timeout}s"
        except Exception as e:
            rc = 125
            out = ""
            err = str(e)
        return idx, redacted, rc, out, err, parse_request_ids(out)

    try:
        job_update(job_id, status="running", stdout="", stderr="", request_ids="[]", error=f"processed 0/{total}; concurrency={concurrency}")
        log("batch_started", "started checked-add batch", job_id=job_id, count=total, concurrency=concurrency, item_timeout=item_timeout)
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(run_one, idx, magnet) for idx, magnet in enumerate(magnets, start=1)]
            for fut in concurrent.futures.as_completed(futures):
                idx, redacted, rc, out, err, rids = fut.result()
                with acc_lock:
                    processed += 1
                    if rc != 0:
                        failures += 1
                    header = f"\n\n===== item {idx}/{total}: {redacted} =====\n"
                    stdout_acc = limited_append(stdout_acc, header + out)
                    stderr_acc = limited_append(stderr_acc, header + err)
                    for rid in rids:
                        ids_seen.add(rid)
                    request_ids = sorted(ids_seen)
                    job_update(
                        job_id,
                        status="running",
                        returncode=rc,
                        stdout=stdout_acc,
                        stderr=stderr_acc,
                        request_ids=json.dumps(request_ids, ensure_ascii=False),
                        error=f"processed {processed}/{total}; failures={failures}; concurrency={concurrency}; last_item={idx}; last_returncode={rc}",
                    )
                    log(
                        "batch_item_finished",
                        "finished checked-add item",
                        job_id=job_id,
                        index=idx,
                        total=total,
                        returncode=rc,
                        request_ids=rids,
                    )

        status = "done" if failures == 0 else "error"
        final_rc = 0 if failures == 0 else 1
        job_update(
            job_id,
            status=status,
            returncode=final_rc,
            stdout=stdout_acc,
            stderr=stderr_acc,
            request_ids=json.dumps(request_ids, ensure_ascii=False),
            error="" if failures == 0 else f"batch completed with {failures} failed subprocess item(s)",
        )
        log("batch_finished", "finished checked-add batch", job_id=job_id, count=total, failures=failures, request_ids=request_ids)
    except Exception as e:
        job_update(job_id, status="error", error=str(e), stdout=stdout_acc, stderr=stderr_acc, request_ids=json.dumps(request_ids, ensure_ascii=False))
        log("batch_error", "batch job raised exception", job_id=job_id, error=str(e))
    finally:
        JOB_SEMAPHORE.release()


def start_job(kind: str, cmd: List[str], input_summary: str, cmd_summary: str) -> str:
    job_id = f"job-{datetime.fromtimestamp(now()).strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(4)}"
    job_insert(job_id, kind, input_summary, cmd_summary)
    t = threading.Thread(target=run_job, args=(job_id, cmd), daemon=True)
    t.start()
    return job_id


def start_batch_job(kind: str, base_cmd: List[str], magnets: List[str], input_summary: str, cmd_summary: str) -> str:
    job_id = f"job-{datetime.fromtimestamp(now()).strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(4)}"
    job_insert(job_id, kind, input_summary, cmd_summary)
    spool_path = write_spool(
        job_id,
        {
            "job_id": job_id,
            "created_at": now(),
            "kind": kind,
            "base_cmd": base_cmd,
            "magnet_count": len(magnets),
            "magnets": magnets,
            "input_summary": input_summary,
            "command_summary": cmd_summary,
        },
    )
    log("batch_spooled", "saved full batch input to root-only spool", job_id=job_id, count=len(magnets), spool_path=spool_path)
    t = threading.Thread(target=run_batch_job, args=(job_id, base_cmd, magnets), daemon=True)
    t.start()
    return job_id


def rows_recent_requests(limit: int = 30) -> List[sqlite3.Row]:
    with db() as con:
        return list(
            con.execute(
                """
                select request_id,datetime(created_at,'unixepoch','localtime') created_at,infohash,qbt_hash,torrent_name,
                       status,decision,reason,normalized_ids,matched_remote_paths,qbt_tags
                from checked_add_requests
                order by id desc limit ?
                """,
                (limit,),
            )
        )


def rows_recent_jobs(limit: int = 20) -> List[sqlite3.Row]:
    with db() as con:
        return list(
            con.execute(
                """
                select job_id,datetime(created_at,'unixepoch','localtime') created_at,
                       datetime(updated_at,'unixepoch','localtime') updated_at,
                       status,kind,input_summary,returncode,request_ids,error
                from qbt_add_webui_jobs
                order by created_at desc limit ?
                """,
                (limit,),
            )
        )


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with db() as con:
        row = con.execute("select * from qbt_add_webui_jobs where job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None


def get_request(request_id: str) -> Optional[Dict[str, Any]]:
    with db() as con:
        req = con.execute("select * from checked_add_requests where request_id=?", (request_id,)).fetchone()
        if not req:
            return None
        events = list(
            con.execute(
                "select datetime(ts,'unixepoch','localtime') ts,level,action,message,data_json from checked_add_events where request_id=? order by id",
                (request_id,),
            )
        )
        d = dict(req)
        d["events"] = [dict(e) for e in events]
        return d


def layout(title: str, body: str, extra_head: str = "") -> bytes:
    full_title = f"{title} - {APP_TITLE}"
    html_doc = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{esc(full_title)}</title>
  <style>
    :root {{ color-scheme: light dark; --bg:#0b1020; --card:#141a2e; --muted:#8ea0c5; --text:#eaf0ff; --line:#293452; --good:#2fbf71; --warn:#f0b429; --bad:#ff5c7a; --blue:#6ea8fe; }}
    body {{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background:var(--bg); color:var(--text); }}
    header {{ padding:20px 26px; border-bottom:1px solid var(--line); background:rgba(20,26,46,.85); position:sticky; top:0; backdrop-filter: blur(8px); z-index:2; }}
    a {{ color:var(--blue); text-decoration:none; }} a:hover {{ text-decoration:underline; }}
    .wrap {{ max-width:1180px; margin:0 auto; padding:22px; }}
    .grid {{ display:grid; grid-template-columns: 1.1fr .9fr; gap:18px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:18px; box-shadow:0 8px 24px rgba(0,0,0,.22); }}
    textarea,input,select {{ width:100%; box-sizing:border-box; border:1px solid var(--line); border-radius:10px; background:#0d1326; color:var(--text); padding:10px 12px; font:inherit; }}
    textarea {{ min-height:170px; resize:vertical; font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }}
    button {{ border:0; border-radius:10px; background:var(--blue); color:#07101f; font-weight:700; padding:10px 16px; cursor:pointer; }}
    button.secondary {{ background:#273455; color:var(--text); }}
    label {{ display:block; margin:10px 0 6px; color:#cbd7f5; }}
    .row {{ display:flex; gap:10px; align-items:center; flex-wrap:wrap; }}
    .row > * {{ flex:1; }}
    .check {{ display:flex; gap:8px; align-items:center; margin:10px 0; color:#cbd7f5; }}
    .check input {{ width:auto; }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th,td {{ border-bottom:1px solid var(--line); padding:8px 6px; text-align:left; vertical-align:top; }}
    th {{ color:#cbd7f5; font-weight:600; }}
    .muted {{ color:var(--muted); }}
    .pill {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; border:1px solid var(--line); }}
    .accepted_auto,.done {{ color:var(--good); }} .duplicate_gdrive,.maybe_duplicate,.metadata_timeout,.running,.queued {{ color:var(--warn); }} .error {{ color:var(--bad); }}
    pre {{ white-space:pre-wrap; overflow:auto; background:#0d1326; border:1px solid var(--line); border-radius:12px; padding:12px; }}
    .nav {{ display:flex; gap:16px; align-items:center; }} .nav strong {{ margin-right:auto; }}
    @media (max-width: 880px) {{ .grid {{ grid-template-columns:1fr; }} }}
  </style>
  {extra_head}
</head>
<body>
  <header><div class="nav"><strong>{esc(APP_TITLE)}</strong><a href="{BASE_PATH}/">添加</a><a href="{BASE_PATH}/jobs">任务</a><a href="{BASE_PATH}/records">记录</a><a href="{BASE_PATH}/logout">退出</a></div></header>
  <main class="wrap">{body}</main>
</body></html>"""
    return html_doc.encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    server_version = "qbt-add-webui/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        log("access", fmt % args, client=self.client_address[0], path=getattr(self, "path", ""))

    def unauthorized(self) -> None:
        next_url = urllib.parse.quote(urllib.parse.urlsplit(self.path).path or "/", safe="")
        self.redirect(f"{BASE_PATH}/login?next={next_url}")

    def is_auth(self) -> bool:
        if not PASSWORD:
            return False
        if verify_session(cookie_value(self.headers.get("Cookie", ""), "qbt_add_session")):
            return True
        auth = self.headers.get("Authorization", "")
        expected = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
        return hmac.compare_digest(auth, expected)

    def send_bytes(self, data: bytes, status: int = 200, ctype: str = "text/html; charset=utf-8") -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def redirect(self, target: str) -> None:
        self.send_response(302)
        self.send_header("Location", target)
        self.end_headers()

    def redirect_with_cookie(self, target: str, cookie: str) -> None:
        self.send_response(302)
        self.send_header("Location", target)
        self.send_header("Set-Cookie", cookie)
        self.end_headers()

    def path_inside(self) -> str:
        path = urllib.parse.urlsplit(self.path).path
        if path == "/":
            return "/"
        if path == BASE_PATH:
            return "/"
        if path.startswith(BASE_PATH + "/"):
            return path[len(BASE_PATH):] or "/"
        return path

    def parse_post(self) -> Dict[str, List[str]]:
        n = int(self.headers.get("Content-Length", "0") or "0")
        if n > MAX_BODY:
            raise ValueError("request body too large")
        raw = self.rfile.read(n).decode("utf-8", "replace")
        return urllib.parse.parse_qs(raw, keep_blank_values=True)

    def do_GET(self) -> None:
        p = self.path_inside()
        if p == "/healthz":
            return self.send_bytes(b"ok\n", ctype="text/plain; charset=utf-8")
        if p == "/login":
            return self.page_login()
        if p == "/logout":
            return self.redirect_with_cookie(f"{BASE_PATH}/login", "qbt_add_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax; Secure")
        if not self.is_auth():
            return self.unauthorized()
        if p == "/":
            return self.page_index()
        if p == "/jobs":
            return self.page_jobs()
        if p.startswith("/jobs/"):
            return self.page_job(p.rsplit("/", 1)[-1])
        if p.startswith("/api/jobs/"):
            return self.api_job(p.rsplit("/", 1)[-1])
        if p == "/records":
            return self.page_records()
        if p.startswith("/records/"):
            return self.page_record(p.rsplit("/", 1)[-1])
        self.send_error(404)

    def do_POST(self) -> None:
        p = self.path_inside()
        try:
            form = self.parse_post()
            if p == "/login":
                return self.handle_login(form)
            if not self.is_auth():
                return self.unauthorized()
            if p == "/add":
                return self.handle_add(form)
            if p == "/check-name":
                return self.handle_check_name(form)
            if p == "/refresh-index":
                return self.handle_refresh_index()
        except Exception as e:
            log("post_error", "POST failed", path=p, error=str(e))
            return self.send_bytes(layout("错误", f"<div class='card'><h2>错误</h2><pre>{esc(e)}</pre></div>"), status=500)
        self.send_error(404)

    def page_login(self, error: str = "") -> None:
        qs = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
        next_url = qs.get("next", [f"{BASE_PATH}/"])[0] or f"{BASE_PATH}/"
        if not next_url.startswith("/"):
            next_url = f"{BASE_PATH}/"
        body = f"""
<section class="card" style="max-width:460px;margin:60px auto">
  <h2>登录</h2>
  {'<p class="error">'+esc(error)+'</p>' if error else ''}
  <form method="post" action="{BASE_PATH}/login">
    <input type="hidden" name="next" value="{esc(next_url)}">
    <label>用户名</label><input name="username" autocomplete="username" autofocus>
    <label>密码</label><input name="password" type="password" autocomplete="current-password">
    <p><button type="submit">登录</button></p>
  </form>
</section>"""
        self.send_bytes(layout("登录", body))

    def handle_login(self, form: Dict[str, List[str]]) -> None:
        username = form.get("username", [""])[0]
        password = form.get("password", [""])[0]
        next_url = form.get("next", [f"{BASE_PATH}/"])[0] or f"{BASE_PATH}/"
        if not next_url.startswith("/"):
            next_url = f"{BASE_PATH}/"
        if hmac.compare_digest(username, USER) and hmac.compare_digest(password, PASSWORD):
            token = sign_session(USER, now())
            cookie = f"qbt_add_session={token}; Path=/; Max-Age={SESSION_MAX_AGE}; HttpOnly; SameSite=Lax; Secure"
            log("login_success", "webui login success", user=username, client=self.client_address[0])
            return self.redirect_with_cookie(next_url, cookie)
        log("login_failed", "webui login failed", user=username, client=self.client_address[0])
        return self.page_login("用户名或密码错误")

    def page_index(self) -> None:
        recent = rows_recent_requests(12)
        jobs = rows_recent_jobs(8)
        rows = "".join(
            f"<tr><td><a href='{BASE_PATH}/records/{esc(r['request_id'])}'>{esc(r['request_id'])}</a></td><td class='{esc(r['status'])}'>{esc(r['status'])}</td><td>{esc(r['decision'])}</td><td>{esc(r['torrent_name'] or '')}</td><td>{esc(r['reason'] or '')}</td></tr>"
            for r in recent
        )
        job_rows = "".join(
            f"<tr><td><a href='{BASE_PATH}/jobs/{esc(j['job_id'])}'>{esc(j['job_id'])}</a></td><td class='{esc(j['status'])}'>{esc(j['status'])}</td><td>{esc(j['kind'])}</td><td>{esc(j['input_summary'])}</td></tr>"
            for j in jobs
        )
        body = f"""
<div class="grid">
  <section class="card">
    <h2>添加 magnet</h2>
    <p class="muted">每行一个 magnet。提交后会创建后台任务，先获取 metadata，再判断 Google Drive 是否已有。</p>
    <form method="post" action="{BASE_PATH}/add">
      <label>Magnet 链接</label>
      <textarea name="magnets" placeholder="magnet:?xt=urn:btih:..."></textarea>
      <div class="row">
        <div><label>metadata 超时秒数</label><input name="metadata_timeout" value="{DEFAULT_METADATA_TIMEOUT}"></div>
        <div><label>备注</label><input name="notes" placeholder="可选"></div>
      </div>
      <label class="check"><input type="checkbox" name="refresh_index" value="1">提交前强制刷新 Google Drive 索引</label>
      <label class="check"><input type="checkbox" name="probe_remote" value="1">索引未命中时实时扫一次 gcrypt:</label>
      <label class="check"><input type="checkbox" name="dry_run" value="1">dry-run：只记录，不提交 qBT</label>
      <button type="submit">提交预检任务</button>
    </form>
  </section>
  <section class="card">
    <h2>文件名快速检查</h2>
    <form method="post" action="{BASE_PATH}/check-name">
      <label>文件名</label><input name="name" placeholder="hhd800.com@BOKO-037.mp4">
      <label>大小，可选，例如 10GiB</label><input name="size" placeholder="可空">
      <button type="submit">检查是否已存在</button>
    </form>
    <hr>
    <h2>索引维护</h2>
    <form method="post" action="{BASE_PATH}/refresh-index">
      <button class="secondary" type="submit">刷新 Google Drive 索引</button>
    </form>
  </section>
</div>
<section class="card" style="margin-top:18px"><h2>最近 WebUI 任务</h2><table><tr><th>Job</th><th>状态</th><th>类型</th><th>输入</th></tr>{job_rows}</table></section>
<section class="card" style="margin-top:18px"><h2>最近处理记录</h2><table><tr><th>Request</th><th>状态</th><th>决策</th><th>名称</th><th>原因</th></tr>{rows}</table></section>
"""
        self.send_bytes(layout("添加", body))

    def handle_add(self, form: Dict[str, List[str]]) -> None:
        raw = (form.get("magnets", [""])[0] or "").strip()
        magnets = [x.strip() for x in raw.splitlines() if x.strip()]
        if not magnets:
            return self.send_bytes(layout("错误", "<div class='card'><h2>没有 magnet</h2></div>"), status=400)
        cmd = [CLI, "--json"]
        timeout = (form.get("metadata_timeout", [""])[0] or str(DEFAULT_METADATA_TIMEOUT)).strip()
        try:
            timeout_int = int(timeout)
            if timeout_int < 30 or timeout_int > 86400:
                raise ValueError
        except Exception:
            return self.send_bytes(layout("错误", "<div class='card'><h2>metadata 超时必须是 30-86400 秒整数</h2></div>"), status=400)
        cmd += ["--metadata-timeout", str(timeout_int)]
        notes = (form.get("notes", [""])[0] or "").strip()
        if notes:
            cmd += ["--notes", notes]
        if form.get("refresh_index"):
            cmd.append("--refresh-index")
        if form.get("probe_remote"):
            cmd.append("--probe-remote")
        if form.get("dry_run"):
            cmd.append("--dry-run")
        summary = "; ".join(redact_magnet(m) for m in magnets[:3])
        if len(magnets) > 3:
            summary += f"; ... +{len(magnets)-3}"
        safe_cmd = "qbt-add-checked --json " + ("--dry-run " if form.get("dry_run") else "") + f"--metadata-timeout {timeout_int} [{len(magnets)} magnet(s)]"
        job_id = start_batch_job("add_magnet", cmd, magnets, summary, safe_cmd)
        self.redirect(f"{BASE_PATH}/jobs/{job_id}")

    def handle_check_name(self, form: Dict[str, List[str]]) -> None:
        name = (form.get("name", [""])[0] or "").strip()
        size = (form.get("size", [""])[0] or "").strip()
        if not name:
            return self.send_bytes(layout("错误", "<div class='card'><h2>没有文件名</h2></div>"), status=400)
        cmd = [CLI, "--json", "--check-name", name]
        if size:
            cmd += ["--check-size", size]
        job_id = start_job("check_name", cmd, name[:200], "qbt-add-checked --json --check-name")
        self.redirect(f"{BASE_PATH}/jobs/{job_id}")

    def handle_refresh_index(self) -> None:
        job_id = start_job("refresh_index", [CLI, "--json", "--refresh-index-only"], "refresh remote index", "qbt-add-checked --refresh-index-only")
        self.redirect(f"{BASE_PATH}/jobs/{job_id}")

    def page_jobs(self) -> None:
        jobs = rows_recent_jobs(50)
        rows = "".join(
            f"<tr><td><a href='{BASE_PATH}/jobs/{esc(j['job_id'])}'>{esc(j['job_id'])}</a></td><td>{esc(j['created_at'])}</td><td class='{esc(j['status'])}'>{esc(j['status'])}</td><td>{esc(j['kind'])}</td><td>{esc(j['input_summary'])}</td><td>{esc(j['error'] or '')}</td></tr>"
            for j in jobs
        )
        self.send_bytes(layout("任务", f"<section class='card'><h2>WebUI 任务</h2><table><tr><th>Job</th><th>创建</th><th>状态</th><th>类型</th><th>输入</th><th>错误</th></tr>{rows}</table></section>"))

    def page_job(self, job_id: str) -> None:
        job = get_job(job_id)
        if not job:
            self.send_error(404)
            return
        req_ids = json.loads(job.get("request_ids") or "[]")
        links = " ".join(f"<a class='pill' href='{BASE_PATH}/records/{esc(r)}'>{esc(r)}</a>" for r in req_ids) or "<span class='muted'>暂无</span>"
        head = ""
        if job["status"] in {"queued", "running"}:
            head = "<meta http-equiv='refresh' content='3'>"
        body = f"""
<section class="card">
  <h2>任务 {esc(job_id)}</h2>
  <p>状态：<strong class="{esc(job['status'])}">{esc(job['status'])}</strong>　类型：{esc(job['kind'])}　返回码：{esc(job.get('returncode'))}</p>
  <p>输入：{esc(job.get('input_summary'))}</p>
  <p>关联请求：{links}</p>
  <h3>stdout</h3><pre>{esc(job.get('stdout') or '')}</pre>
  <h3>stderr</h3><pre>{esc(job.get('stderr') or '')}</pre>
  <h3>error</h3><pre>{esc(job.get('error') or '')}</pre>
</section>"""
        self.send_bytes(layout("任务详情", body, head))

    def api_job(self, job_id: str) -> None:
        job = get_job(job_id)
        if not job:
            self.send_error(404)
            return
        self.send_bytes(json.dumps(job, ensure_ascii=False, default=str, indent=2).encode(), ctype="application/json; charset=utf-8")

    def page_records(self) -> None:
        rows = rows_recent_requests(100)
        trs = "".join(
            f"<tr><td><a href='{BASE_PATH}/records/{esc(r['request_id'])}'>{esc(r['request_id'])}</a></td><td>{esc(r['created_at'])}</td><td class='{esc(r['status'])}'>{esc(r['status'])}</td><td>{esc(r['decision'])}</td><td>{esc(r['torrent_name'] or '')}</td><td>{esc(r['reason'] or '')}</td></tr>"
            for r in rows
        )
        self.send_bytes(layout("记录", f"<section class='card'><h2>处理记录</h2><table><tr><th>Request</th><th>时间</th><th>状态</th><th>决策</th><th>名称</th><th>原因</th></tr>{trs}</table></section>"))

    def page_record(self, request_id: str) -> None:
        rec = get_request(request_id)
        if not rec:
            self.send_error(404)
            return
        events = "".join(
            f"<tr><td>{esc(e['ts'])}</td><td>{esc(e['level'])}</td><td>{esc(e['action'])}</td><td>{esc(e['message'])}</td><td><pre>{esc(e['data_json'])}</pre></td></tr>"
            for e in rec["events"]
        )
        body = f"""
<section class="card">
  <h2>处理记录 {esc(request_id)}</h2>
  <p>状态：<strong class="{esc(rec.get('status'))}">{esc(rec.get('status'))}</strong>　决策：{esc(rec.get('decision'))}</p>
  <p>名称：{esc(rec.get('torrent_name') or '')}</p>
  <p>原因：{esc(rec.get('reason') or '')}</p>
  <p>Hash：{esc(rec.get('infohash') or '')}</p>
  <h3>normalized_ids</h3><pre>{esc(rec.get('normalized_ids') or '[]')}</pre>
  <h3>matched_remote_paths</h3><pre>{esc(rec.get('matched_remote_paths') or '[]')}</pre>
  <h3>事件链</h3>
  <table><tr><th>时间</th><th>级别</th><th>动作</th><th>消息</th><th>数据</th></tr>{events}</table>
</section>"""
        self.send_bytes(layout("记录详情", body))


def main() -> int:
    if not PASSWORD:
        raise SystemExit("QBT_ADD_WEBUI_PASSWORD is required")
    if not SESSION_SECRET:
        raise SystemExit("QBT_ADD_WEBUI_SESSION_SECRET is required")
    init_db()
    httpd = ThreadingHTTPServer((BIND, PORT), Handler)
    log("service_start", "qbt add webui started", bind=BIND, port=PORT, base_path=BASE_PATH)

    def shutdown(_signum, _frame):
        log("service_stop", "qbt add webui stopping")
        httpd.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    httpd.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
