from __future__ import annotations
from dataclasses import dataclass
from typing import Sequence
@dataclass(frozen=True)
class GuardResult:
    allowed: bool; reason: str = "ok"
class ScrapeCommandGuard:
    def __init__(self, staging_dir: str): self.staging_dir = staging_dir
    def validate(self, command: Sequence[str]) -> GuardResult:
        joined = " ".join(command)
        if "rclone" in command[0] or " gcrypt:" in joined or joined.endswith("gcrypt:"):
            return GuardResult(False, "scraper_io_bypass_blocked")
        return GuardResult(True)
