from __future__ import annotations

import json
import subprocess
from typing import Callable, Sequence

from ..io_governor import RcloneLimits

Runner = Callable[[Sequence[str], str | None, int | None], tuple[int, str, str]]


def default_runner(argv: Sequence[str], input_text: str | None = None, timeout: int | None = None) -> tuple[int, str, str]:
    p = subprocess.run(list(argv), input=input_text, text=True, capture_output=True, timeout=timeout)
    return p.returncode, p.stdout, p.stderr


class RcloneClient:
    def __init__(
        self,
        config_path: str,
        transfers: int = 1,
        checkers: int = 2,
        runner: Runner = default_runner,
        timeout: int = 21600,
        limits_provider: Callable[[], RcloneLimits | dict] | None = None,
        bwlimit: str | None = None,
    ):
        self.config_path = config_path
        self.transfers = transfers
        self.checkers = checkers
        self.runner = runner
        self.timeout = timeout
        self.limits_provider = limits_provider
        self.bwlimit = bwlimit

    def _base(self) -> list[str]:
        transfers = self.transfers
        checkers = self.checkers
        bwlimit = self.bwlimit
        if self.limits_provider is not None:
            limits = self.limits_provider()
            if isinstance(limits, dict):
                transfers = int(limits.get("transfers", transfers))
                checkers = int(limits.get("checkers", checkers))
                bwlimit = limits.get("bwlimit", bwlimit)
            else:
                transfers = int(limits.transfers)
                checkers = int(limits.checkers)
                bwlimit = limits.bwlimit if limits.bwlimit is not None else bwlimit
        base = ["rclone", "--config", self.config_path, "--transfers", str(transfers), "--checkers", str(checkers)]
        if bwlimit:
            base.extend(["--bwlimit", str(bwlimit)])
        return base

    def copyto(self, local: str, remote: str) -> bool:
        rc, _out, err = self.runner(self._base() + ["copyto", local, remote], None, self.timeout)
        if rc != 0:
            raise RuntimeError(f"rclone copyto failed rc={rc}: {err[-400:]}")
        return True

    def copy(self, local: str, remote: str) -> bool:
        rc, _out, err = self.runner(self._base() + ["copy", local, remote], None, self.timeout)
        if rc != 0:
            raise RuntimeError(f"rclone copy failed rc={rc}: {err[-400:]}")
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

    def lsjson(self, remote: str, recursive: bool = False) -> list[dict]:
        argv = self._base() + ["lsjson"]
        if recursive:
            argv.append("--recursive")
        argv.append(remote)
        rc, out, err = self.runner(argv, None, 300)
        if rc != 0:
            raise RuntimeError(f"rclone lsjson failed rc={rc}: {err[-400:]}")
        data = json.loads(out or "[]")
        if isinstance(data, list):
            return [dict(x) for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            return [dict(data)]
        return []
