from __future__ import annotations

import json
import subprocess
from typing import Any, Callable, Sequence
from urllib.parse import urlencode

Runner = Callable[[Sequence[str], str | None, int | None], tuple[int, str, str]]


def default_runner(argv: Sequence[str], input_text: str | None = None, timeout: int | None = None) -> tuple[int, str, str]:
    p = subprocess.run(list(argv), input=input_text, text=True, capture_output=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


class QbtDockerClient:
    """qBT WebAPI client using docker-exec container-local curl.

    The live VPS config advertises http://127.0.0.1:8080 inside the container; from
    the host, the safest automation path is the legacy pattern: docker exec
    qbittorrent curl http://127.0.0.1:8080/api/v2/...
    """

    def __init__(self, container: str = "qbittorrent", api_base: str = "http://127.0.0.1:8080", runner: Runner = default_runner, timeout: int = 10):
        self.container = container
        self.api_base = api_base.rstrip("/")
        self.runner = runner
        self.timeout = timeout

    def _url(self, path: str, params: dict[str, Any] | None = None) -> str:
        url = f"{self.api_base}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        return url

    def _curl(self, path: str, params: dict[str, Any] | None = None, data: dict[str, Any] | None = None) -> str:
        curl_max_time = max(1, int(self.timeout))
        curl_connect_timeout = min(5, curl_max_time)
        argv = ["docker", "exec", self.container, "curl", "-fsS", "--connect-timeout", str(curl_connect_timeout), "--max-time", str(curl_max_time)]
        input_text = None
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

    def post(self, path: str, payload: dict[str, Any]) -> str:
        return self._curl(path, data=payload)
