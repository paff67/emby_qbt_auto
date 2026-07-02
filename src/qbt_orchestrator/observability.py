from __future__ import annotations

import re
from typing import Any

SECRET_KEY = re.compile(r"(token|password|passwd|secret|key|apikey|api_key|cookie|authorization|auth|credential)", re.I)
MAGNET = re.compile(r"magnet:\?xt=urn:btih:[A-Za-z0-9]{32,}", re.I)
ROOT_RCLONE = re.compile(r"/root/\.config/rclone/[^\s\"']+", re.I)
BEARER = re.compile(r"Bearer\s+[A-Za-z0-9._\-]+", re.I)
TELEGRAM_TOKEN = re.compile(r"\b\d{5,}:\S+", re.I)


def redact(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {k: ("<redacted>" if SECRET_KEY.search(str(k)) else redact(v, str(k))) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v, key) for v in value]
    if isinstance(value, tuple):
        return tuple(redact(v, key) for v in value)
    if isinstance(value, str):
        if SECRET_KEY.search(key):
            return "<redacted>"
        value = MAGNET.sub("<redacted-magnet>", value)
        value = ROOT_RCLONE.sub("<redacted-rclone-config>", value)
        value = BEARER.sub("Bearer <redacted>", value)
        value = TELEGRAM_TOKEN.sub("<redacted-token>", value)
        return value
    return value
