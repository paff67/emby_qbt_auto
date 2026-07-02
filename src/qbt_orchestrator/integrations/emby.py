from __future__ import annotations

import json
from typing import Callable
from urllib import request

Transport = Callable[[str, dict, dict, int], dict]


def default_transport(url: str, payload: dict, headers: dict, timeout: int) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json", **headers}, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body) if body else {}


class EmbyClient:
    def __init__(self, base_url: str, api_key: str, media_prefix: str = "/media/gcrypt", transport: Transport = default_transport, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.media_prefix = media_prefix.rstrip("/")
        self.transport = transport
        self.timeout = timeout

    def _validate_path(self, path: str) -> None:
        normalized = path.rstrip("/")
        if normalized == self.media_prefix or not normalized.startswith(self.media_prefix + "/"):
            raise ValueError("refresh path too broad or outside media prefix")

    def media_updated(self, path: str) -> dict:
        self._validate_path(path)
        payload = {"Updates": [{"Path": path.rstrip("/"), "UpdateType": "Created"}]}
        return self.transport(f"{self.base_url}/Library/Media/Updated", payload, {"X-Emby-Token": self.api_key}, self.timeout)
