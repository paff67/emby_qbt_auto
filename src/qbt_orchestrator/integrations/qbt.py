from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode

Runner = Callable[[Sequence[str], str | None, int | None], tuple[int, str, str]]
HttpTransport = Callable[[str, str, str | None, Mapping[str, str], int | None], tuple[int, str, Mapping[str, str]]]


def default_runner(argv: Sequence[str], input_text: str | None = None, timeout: int | None = None) -> tuple[int, str, str]:
    p = subprocess.run(list(argv), input=input_text, text=True, capture_output=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


class TokenBucket:
    def __init__(
        self,
        rate_per_sec: float,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.rate_per_sec = float(rate_per_sec)
        self.clock = clock
        self.sleeper = sleeper
        self.capacity = max(1.0, self.rate_per_sec)
        self.tokens = self.capacity
        self.updated_at = float(self.clock())

    def acquire(self) -> None:
        if self.rate_per_sec <= 0:
            return
        now = float(self.clock())
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
        self.updated_at = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return
        wait_for = (1.0 - self.tokens) / self.rate_per_sec
        self.sleeper(wait_for)
        self.updated_at = float(self.clock())
        self.tokens = 0.0


class QbtDockerClient:
    """qBT WebAPI client using docker-exec container-local curl.

    The live VPS config advertises http://127.0.0.1:8080 inside the container; from
    the host, the safest automation path is the legacy pattern: docker exec
    qbittorrent curl http://127.0.0.1:8080/api/v2/...
    """

    def __init__(
        self,
        container: str = "qbittorrent",
        api_base: str = "http://127.0.0.1:8080",
        runner: Runner = default_runner,
        timeout: int = 10,
        api_max_requests_per_sec: float = 4.0,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.container = container
        self.api_base = api_base.rstrip("/")
        self.runner = runner
        self.timeout = timeout
        self.rate_limiter = TokenBucket(api_max_requests_per_sec, clock=clock, sleeper=sleeper)

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.api_base}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        return url

    def _curl(self, path: str, params: dict[str, Any] | None = None, data: dict[str, Any] | None = None) -> str:
        self.rate_limiter.acquire()
        curl_max_time = max(1, int(self.timeout))
        curl_connect_timeout = min(5, curl_max_time)
        argv = ["docker", "exec"]
        input_text = None
        if data is not None:
            # qBT write APIs expect application/x-www-form-urlencoded POST
            # bodies.  Because curl is executed *inside* the qbittorrent
            # container, docker exec must keep stdin attached; otherwise curl's
            # --data-binary @- reads an empty body and qBT v5 returns HTTP 400
            # for missing required fields such as "hashes".
            argv.append("-i")
        argv += [self.container, "curl", "-fsS", "--connect-timeout", str(curl_connect_timeout), "--max-time", str(curl_max_time)]
        if data is not None:
            argv += ["-X", "POST", "-H", "Content-Type: application/x-www-form-urlencoded", "--data-binary", "@-"]
            input_text = urlencode(data)
        argv.append(self._url(path, params))
        rc, out, err = self.runner(argv, input_text, self.timeout)
        if rc != 0:
            raise RuntimeError(f"qBT API failed rc={rc}: {err[-400:]}")
        return out

    def get_maindata(self, rid: int) -> dict[str, Any]:
        return json.loads(self._curl("/api/v2/sync/maindata", {"rid": rid}))

    def torrent_info(self, hash: str) -> dict[str, Any]:
        rows = json.loads(self._curl("/api/v2/torrents/info", {"hashes": hash}))
        return rows[0] if rows else {"hash": hash}

    def torrent_files(self, hash: str) -> list[dict[str, Any]]:
        rows = json.loads(self._curl("/api/v2/torrents/files", {"hash": hash}))
        out = []
        for idx, row in enumerate(rows if isinstance(rows, list) else []):
            item = dict(row)
            item.setdefault("index", idx)
            out.append(item)
        return out

    def torrent_properties(self, hash: str) -> dict[str, Any]:
        return json.loads(self._curl("/api/v2/torrents/properties", {"hash": hash}))

    def get_preferences(self) -> dict[str, Any]:
        return json.loads(self._curl("/api/v2/app/preferences"))

    def app_version(self) -> str:
        return self._curl("/api/v2/app/version").strip()

    def set_preferences(self, preferences: dict[str, Any]) -> str:
        return self._curl("/api/v2/app/setPreferences", data={"json": json.dumps(preferences, ensure_ascii=False)})

    def post(self, path: str, payload: dict[str, Any]) -> str:
        return self._curl(path, data=payload)


def default_http_transport(
    method: str,
    url: str,
    body: str | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: int | None = None,
) -> tuple[int, str, Mapping[str, str]]:
    data = None if body is None else body.encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=dict(headers or {}), method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), resp.read().decode("utf-8", errors="replace"), dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", errors="replace"), dict(exc.headers.items())


class QbtHttpClient:
    """qBT WebAPI client using host HTTP API and qBT SID cookie auth."""

    def __init__(
        self,
        api_base: str = "http://127.0.0.1:8081",
        username: str = "",
        password: str = "",
        transport: HttpTransport = default_http_transport,
        timeout: int = 10,
        api_max_requests_per_sec: float = 4.0,
        auth_mode: str = "auto",
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.api_base = api_base.rstrip("/")
        self.username = username
        self.password = password
        self.auth_mode = str(auth_mode or "auto").strip().lower()
        self.transport = transport
        self.timeout = timeout
        self.cookie: str | None = None
        self.rate_limiter = TokenBucket(api_max_requests_per_sec, clock=clock, sleeper=sleeper)

    @property
    def auth_enabled(self) -> bool:
        return self.auth_mode not in {"none", "noauth", "disabled", "off"}

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.api_base}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        return url

    def _login(self) -> None:
        if not self.username and not self.password:
            raise RuntimeError("qBT host API returned unauthorized and no credentials are configured")
        body = urlencode({"username": self.username, "password": self.password})
        status, text, headers = self.transport(
            "POST",
            self._url("/api/v2/auth/login"),
            body,
            {"Content-Type": "application/x-www-form-urlencoded"},
            self.timeout,
        )
        if status >= 400 or not text.strip().lower().startswith("ok"):
            raise RuntimeError(f"qBT host API login failed status={status}")
        set_cookie = None
        for key, value in headers.items():
            if key.lower() == "set-cookie":
                set_cookie = value
                break
        if not set_cookie:
            raise RuntimeError("qBT host API login did not return a session cookie")
        self.cookie = set_cookie.split(";", 1)[0]

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        retry_auth: bool = True,
    ) -> str:
        self.rate_limiter.acquire()
        if self.auth_enabled and self.cookie is None and (self.username or self.password):
            self._login()
        body = None if data is None else urlencode(data)
        headers: dict[str, str] = {}
        if body is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if self.cookie:
            headers["Cookie"] = self.cookie
        status, text, _headers = self.transport(method, self._url(path, params), body, headers, self.timeout)
        if status in {401, 403} and retry_auth and self.auth_enabled:
            self.cookie = None
            self._login()
            return self._request(method, path, params=params, data=data, retry_auth=False)
        if status in {401, 403} and not self.auth_enabled:
            raise RuntimeError(f"qBT host API returned unauthorized while auth is disabled status={status}")
        if status >= 400:
            raise RuntimeError(f"qBT host API failed status={status}: {text[-400:]}")
        return text

    def get_maindata(self, rid: int) -> dict[str, Any]:
        return json.loads(self._request("GET", "/api/v2/sync/maindata", {"rid": rid}))

    def torrent_info(self, hash: str) -> dict[str, Any]:
        rows = json.loads(self._request("GET", "/api/v2/torrents/info", {"hashes": hash}))
        return rows[0] if rows else {"hash": hash}

    def torrent_files(self, hash: str) -> list[dict[str, Any]]:
        rows = json.loads(self._request("GET", "/api/v2/torrents/files", {"hash": hash}))
        out = []
        for idx, row in enumerate(rows if isinstance(rows, list) else []):
            item = dict(row)
            item.setdefault("index", idx)
            out.append(item)
        return out

    def torrent_properties(self, hash: str) -> dict[str, Any]:
        return json.loads(self._request("GET", "/api/v2/torrents/properties", {"hash": hash}))

    def get_preferences(self) -> dict[str, Any]:
        return json.loads(self._request("GET", "/api/v2/app/preferences"))

    def app_version(self) -> str:
        return self._request("GET", "/api/v2/app/version").strip()

    def set_preferences(self, preferences: dict[str, Any]) -> str:
        return self._request("POST", "/api/v2/app/setPreferences", data={"json": json.dumps(preferences, ensure_ascii=False)})

    def post(self, path: str, payload: dict[str, Any]) -> str:
        return self._request("POST", path, data=payload)
