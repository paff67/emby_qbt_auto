from __future__ import annotations

import json
import re
import subprocess
import threading
import time
from typing import Callable, Sequence
from urllib import parse, request

Transport = Callable[[str, dict, dict, int], dict]
GetTransport = Callable[[str, dict, int], dict]
CommandRunner = Callable[[Sequence[str], int], tuple[int, str, str]]


def default_command_runner(argv: Sequence[str], timeout: int) -> tuple[int, str, str]:
    process = subprocess.run(
        list(argv), text=True, capture_output=True, timeout=timeout
    )
    return process.returncode, process.stdout, process.stderr


class RcloneMountCacheFlusher:
    """Flush the configured rclone mount directory cache with SIGHUP."""

    _SERVICE = re.compile(r"^[A-Za-z0-9_.@-]+$")

    def __init__(
        self,
        service_name: str = "rclone-gcrypt-emby.service",
        *,
        runner: CommandRunner = default_command_runner,
        timeout: int = 15,
        min_interval_sec: float = 30.0,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        if not self._SERVICE.fullmatch(str(service_name)):
            raise ValueError("invalid rclone mount service name")
        self.service_name = str(service_name)
        self.runner = runner
        self.timeout = max(1, int(timeout))
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self.monotonic = monotonic
        self._last_flush_at: float | None = None
        self._lock = threading.Lock()

    def flush(self, _path: str) -> None:
        argv = [
            "systemctl",
            "kill",
            "--kill-who=main",
            "--signal=HUP",
            self.service_name,
        ]
        with self._lock:
            now = float(self.monotonic())
            if self._last_flush_at is not None and (
                now - self._last_flush_at < self.min_interval_sec
            ):
                return
            rc, _stdout, stderr = self.runner(argv, self.timeout)
            if rc != 0:
                raise ConnectionError(
                    f"rclone mount cache flush failed rc={rc}: {stderr[-300:]}"
                )
            self._last_flush_at = now


def default_transport(url: str, payload: dict, headers: dict, timeout: int) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type": "application/json", **headers}, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body) if body else {}


def default_get_transport(url: str, headers: dict, timeout: int) -> dict:
    req = request.Request(url, headers=headers, method="GET")
    with request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body) if body else {}


class EmbyClient:
    def __init__(self, base_url: str, api_key: str, media_prefix: str = "/media/gcrypt", transport: Transport = default_transport, get_transport: GetTransport = default_get_transport, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.media_prefix = media_prefix.rstrip("/")
        self.transport = transport
        self.get_transport = get_transport
        self.timeout = timeout

    def _validate_path(self, path: str) -> None:
        normalized = path.rstrip("/")
        if normalized == self.media_prefix or not normalized.startswith(self.media_prefix + "/"):
            raise ValueError("refresh path too broad or outside media prefix")

    def media_updated(self, path: str) -> dict:
        self._validate_path(path)
        payload = {"Updates": [{"Path": path.rstrip("/"), "UpdateType": "Created"}]}
        return self.transport(f"{self.base_url}/Library/Media/Updated", payload, {"X-Emby-Token": self.api_key}, self.timeout)

    def refresh_path(self, path: str) -> dict:
        self._validate_path(path)
        normalized = path.rstrip("/")
        notified = self.media_updated(normalized)
        headers = {"X-Emby-Token": self.api_key}
        query = parse.urlencode(
            {
                "Path": normalized,
                "Recursive": "true",
                "Fields": "Path",
                "Limit": "20",
            }
        )
        response = self.get_transport(
            f"{self.base_url}/Items?{query}", headers, self.timeout
        )
        item_ids = [
            str(item.get("Id"))
            for item in response.get("Items", [])
            if item.get("Type") == "Folder"
            and str(item.get("Path") or "").rstrip("/") == normalized
            and item.get("Id")
        ]
        if not item_ids:
            raise ConnectionError(f"emby folder item not ready for {normalized}")
        refresh_query = parse.urlencode(
            {
                "Recursive": "true",
                "MetadataRefreshMode": "FullRefresh",
                "ImageRefreshMode": "Default",
                "ReplaceAllMetadata": "false",
                "ReplaceAllImages": "false",
            }
        )
        payload = {"ReplaceThumbnailImages": False}
        for item_id in item_ids:
            self.transport(
                f"{self.base_url}/Items/{parse.quote(item_id, safe='')}/Refresh?{refresh_query}",
                payload,
                headers,
                self.timeout,
            )
        return {"notified": notified, "refreshed_item_ids": item_ids}
