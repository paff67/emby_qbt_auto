from __future__ import annotations

import json
import subprocess
from typing import Callable, Sequence

Runner = Callable[[Sequence[str], str | None, int | None], tuple[int, str, str]]


def default_runner(argv: Sequence[str], input_text: str | None = None, timeout: int | None = None) -> tuple[int, str, str]:
    p = subprocess.run(list(argv), input=input_text, text=True, capture_output=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


class RcloneClient:
    def __init__(self, config_path: str, transfers: int = 1, checkers: int = 2, runner: Runner = default_runner, timeout: int = 21600):
        self.config_path = config_path
        self.transfers = transfers
        self.checkers = checkers
        self.runner = runner
        self.timeout = timeout

    def _base(self) -> list[str]:
        return ["rclone", "--config", self.config_path, "--transfers", str(self.transfers), "--checkers", str(self.checkers)]

    def copyto(self, local: str, remote: str) -> bool:
        rc, _out, err = self.runner(self._base() + ["copyto", local, remote], None, self.timeout)
        if rc != 0:
            raise RuntimeError(f"rclone copyto failed rc={rc}: {err[-400:]}")
        return True

    def lsjson_size(self, remote: str) -> int | None:
        rc, out, err = self.runner(self._base() + ["lsjson", remote], None, 300)
        if rc != 0:
            raise RuntimeError(f"rclone lsjson failed rc={rc}: {err[-400:]}")
        data = json.loads(out or "[]")
        if isinstance(data, list) and data:
            return int(data[0].get("Size", 0))
        if isinstance(data, dict):
            return int(data.get("Size", 0))
        return None
