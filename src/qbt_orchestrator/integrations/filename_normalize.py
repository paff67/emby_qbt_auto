from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any, Callable

from ..observability import redact


def _default_runner(cmd: list[str], env: dict[str, str], timeout: int):
    return subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, timeout=timeout)


class FilenameNormalizeScript:
    """Adapter for the existing US1 `jav_name_normalize.py` JSON interface."""

    def __init__(
        self,
        script_path: str | Path = "/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py",
        *,
        runner: Callable[[list[str], dict[str, str], int], Any] = _default_runner,
        timeout_sec: int = 30,
        env: dict[str, str] | None = None,
    ):
        self.script_path = Path(script_path)
        self.script_command = str(script_path)
        self.runner = runner
        self.timeout_sec = int(timeout_sec)
        self.env = dict(env or {})

    def normalize(self, raw_filename: str) -> dict[str, Any]:
        env = os.environ.copy()
        env.update(self.env)
        cmd = [self.script_command, str(raw_filename)]
        try:
            proc = self.runner(cmd, env, self.timeout_sec)
            stdout = str(getattr(proc, "stdout", "") or "")
            if int(getattr(proc, "returncode", 0) or 0) != 0:
                return {
                    "normalized_id": "",
                    "confidence": 0.0,
                    "raw_name": str(raw_filename),
                    "reason": "normalizer_nonzero_exit",
                    "returncode": int(getattr(proc, "returncode", 1) or 1),
                    "stdout_tail": redact(stdout[-1000:]),
                }
            payload = json.loads(stdout)
            if not isinstance(payload, dict):
                raise ValueError("normalizer JSON root is not an object")
            payload.setdefault("raw_name", str(raw_filename))
            payload.setdefault("reason", "normalizer_json_parsed")
            payload["confidence"] = float(payload.get("confidence") or 0.0)
            payload["normalized_id"] = str(payload.get("normalized_id") or "").strip()
            return payload
        except Exception as exc:
            return {
                "normalized_id": "",
                "confidence": 0.0,
                "raw_name": str(raw_filename),
                "reason": "normalizer_exception",
                "error": redact(str(exc))[:500],
            }
