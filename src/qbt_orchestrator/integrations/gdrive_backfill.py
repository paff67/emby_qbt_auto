from __future__ import annotations
from dataclasses import dataclass
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from typing import Sequence

@dataclass(frozen=True)
class GuardResult:
    allowed: bool; reason: str = "ok"

class ScrapeCommandGuard:
    def __init__(self, staging_dir: str): self.staging_dir = staging_dir
    def validate(self, command: Sequence[str]) -> GuardResult:
        joined = " ".join(command)
        if any(Path(part).name == "rclone" or part == "rclone" for part in command) or " gcrypt:" in joined or joined.endswith("gcrypt:"):
            return GuardResult(False, "scraper_io_bypass_blocked")
        return GuardResult(True)


SIDECAR_SUFFIXES = (
    ".nfo",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
)


def _default_runner(cmd: list[str], env: dict[str, str], timeout: int):
    return subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, timeout=timeout)


def _safe_segment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._ -]+", "_", value).strip(" ._")
    return cleaned or "unknown"


class GDriveBackfillScraper:
    """Live adapter for the existing VPS Javinizer backfill scraper.

    The adapter deliberately runs the legacy scraper only against a local
    staging directory and returns sidecar files as UploadWorker artifacts.  It
    never runs rclone and never writes directly to `gcrypt:`.
    """

    def __init__(
        self,
        script_path: str | Path = "/opt/qbt/gdrive-backfill/bin/javinizer_scrape_one.sh",
        staging_root: str | Path = "/var/lib/qbt-orchestrator/sidecar-staging",
        *,
        remote: str = "gcrypt:",
        runner: Callable[[list[str], dict[str, str], int], Any] = _default_runner,
        timeout_sec: int = 1020,
        lock_file: str | Path | None = "/tmp/gdrive-backfill.lock",
        command_mode: str = "auto",
        env: dict[str, str] | None = None,
        keep_failed_staging: bool = True,
        now: Callable[[], int] | None = None,
    ):
        self.script_path = Path(script_path)
        self.staging_root = Path(staging_root)
        self.remote = remote.rstrip(":") + ":"
        self.runner = runner
        self.timeout_sec = int(timeout_sec)
        self.lock_file = Path(lock_file) if lock_file else None
        self.command_mode = command_mode
        self.env = dict(env or {})
        self.keep_failed_staging = bool(keep_failed_staging)
        self.now = now or (lambda: int(time.time()))

    def scrape_one(self, media_group_key: str, manifest_id: str) -> dict[str, Any]:
        key = _safe_segment(str(media_group_key))
        mid = _safe_segment(str(manifest_id))
        work_dir = self.staging_root / key / f"{int(self.now())}-{mid}"
        work_dir.mkdir(parents=True, exist_ok=True)
        if not self.script_path.exists():
            return self._result("not_found", key, manifest_id, work_dir, [], f"scraper script not found: {self.script_path}")

        manifest_path = self._write_manifest(work_dir, key, str(manifest_id))
        command = self._build_command(work_dir, manifest_path, key)
        guard = ScrapeCommandGuard(str(work_dir)).validate(command)
        if not guard.allowed:
            return self._result("blocked", key, manifest_id, work_dir, [], guard.reason)

        env = os.environ.copy()
        env.update(self.env)
        proc = self.runner(command, env, self.timeout_sec)
        stdout = str(getattr(proc, "stdout", "") or "")
        (work_dir / "scraper.stdout.log").write_text(stdout, encoding="utf-8")
        artifacts = self._collect_artifacts(work_dir, key)
        if artifacts:
            artifact_manifest = self._write_artifact_manifest(work_dir, key, str(manifest_id), artifacts, command, int(getattr(proc, "returncode", 0) or 0), stdout)
            return self._result("sidecar_verified", key, manifest_id, work_dir, artifacts, None, returncode=int(getattr(proc, "returncode", 0) or 0), artifact_manifest=artifact_manifest)
        if int(getattr(proc, "returncode", 1) or 0) == 0:
            return self._result("not_found", key, manifest_id, work_dir, [], "scraper produced no sidecar artifacts", returncode=0)
        if not self.keep_failed_staging:
            shutil.rmtree(work_dir, ignore_errors=True)
        return self._result("not_found", key, manifest_id, work_dir, [], stdout[-1000:] or f"scraper rc={getattr(proc, 'returncode', 'unknown')}", returncode=int(getattr(proc, "returncode", 1) or 1))

    def _write_manifest(self, work_dir: Path, key: str, manifest_id: str) -> Path:
        manifest = {
            "schema_version": 1,
            "media_group_key": key,
            "manifest_id": manifest_id,
            "output_dir": str(work_dir),
            "remote_write_allowed": False,
            "allow_internal_rclone": False,
            "created_at": int(self.now()),
        }
        path = work_dir / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _build_command(self, work_dir: Path, manifest_path: Path, key: str) -> list[str]:
        mode = self.command_mode
        if mode == "auto":
            mode = "manifest" if self.script_path.name in {"scrape-one", "scrape-one.sh"} else "vps_legacy"
        if mode == "manifest":
            command = [str(self.script_path), "--manifest", str(manifest_path), "--output-dir", str(work_dir)]
        elif mode == "vps_legacy":
            # Current US1 reality: /opt/qbt/gdrive-backfill/bin/javinizer_scrape_one.sh
            # accepts `/host/work/dir [movie-id]`.  We still create manifest.json
            # and artifact_manifest.json around it so the daemon owns audit,
            # UploadWorker handoff, and the remote-write prohibition.
            command = [str(self.script_path), str(work_dir), key]
        else:
            raise ValueError(f"unknown scraper command_mode: {mode}")
        if self.lock_file:
            return ["flock", "-n", str(self.lock_file), *command]
        return command

    def _write_artifact_manifest(self, work_dir: Path, key: str, manifest_id: str, artifacts: list[dict[str, Any]], command: list[str], returncode: int, stdout: str) -> str:
        path = work_dir / "artifact_manifest.json"
        payload = {
            "schema_version": 1,
            "media_group_key": key,
            "manifest_id": manifest_id,
            "artifacts": artifacts,
            "scraper_command": command,
            "scraper_exit_code": int(returncode),
            "scraper_log_tail": stdout[-1000:],
            "remote_write_allowed": False,
            "allow_internal_rclone": False,
            "created_at": int(self.now()),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path)

    def _collect_artifacts(self, work_dir: Path, key: str) -> list[dict[str, Any]]:
        artifacts: list[dict[str, Any]] = []
        root = work_dir.resolve()
        for path in sorted(p for p in work_dir.rglob("*") if p.is_file()):
            if path.name.startswith(".") or path.name.endswith(".log"):
                continue
            if path.suffix.lower() not in SIDECAR_SUFFIXES:
                continue
            resolved = path.resolve()
            try:
                rel = resolved.relative_to(root)
            except ValueError:
                continue
            size = int(path.stat().st_size)
            if size <= 0:
                continue
            remote_path = str(PurePosixPath("/", key, *rel.parts))
            artifacts.append({"local": str(path), "remote": f"{self.remote}{remote_path}", "size": size})
        return artifacts

    @staticmethod
    def _result(status: str, key: str, manifest_id: str, work_dir: Path, artifacts: list[dict[str, Any]], error: str | None, returncode: int | None = None, artifact_manifest: str | None = None) -> dict[str, Any]:
        out: dict[str, Any] = {
            "status": status,
            "artifacts": artifacts,
            "media_group_key": key,
            "manifest_id": manifest_id,
            "staging_dir": str(work_dir),
        }
        if artifact_manifest:
            out["artifact_manifest"] = artifact_manifest
        if error:
            out["error"] = error
        if returncode is not None:
            out["returncode"] = returncode
        return out
