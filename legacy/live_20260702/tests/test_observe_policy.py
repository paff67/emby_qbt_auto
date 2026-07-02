#!/usr/bin/env python3
import importlib.util
import sys
from pathlib import Path

ROOT = Path("/opt/qbt-orchestrator")
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("orchestrator", ROOT / "orchestrator.py")
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)
Orchestrator = mod.Orchestrator


class FakeOrchestrator(Orchestrator):
    def __init__(self):
        self.ops = []
        self.qbt = {
            "tag_hold": "hold",
            "tag_auto": "auto",
            "category_auto": "auto",
            "save_path": "/downloads/active",
        }
        self.cfg = {
            "dedupe": {"observe_tags": ["observe"], "metadata_timeout_tags": ["precheck", "metadata-timeout", "observe"]},
            "batching": {"min_file_size_mb_for_main": 100, "skip_junk_patterns": []},
            "cover_policy": {"enabled": True, "hard_junk_regex": [], "video_ext_regex": [r"(?i)\.(mp4|mkv|avi|mov|wmv|ts|m4v)$"], "image_ext_regex": [r"(?i)\.(jpg|jpeg|png|webp)$"]},
        }

    def qjson(self, path, params=None):
        self.ops.append(("qjson", path, params or {}))
        if path == "/api/v2/torrents/files":
            return [
                {"name": "ABF-063/ABF-063.mp4", "size": 200 * 1024 * 1024, "progress": 0.01},
                {"name": "ABF-063/聚 合 全 網 H 直 播.html", "size": 145, "progress": 0},
            ]
        return []

    def qpost(self, path, data=None, files=None, ok=(200,)):
        self.ops.append(("qpost", path, data or {}))
        return "Ok."

    def event(self, h, level, msg):
        self.ops.append(("event", h, level, msg))

    def stop_torrent(self, h):
        self.ops.append(("stop", h))


def test_observe_torrent_with_metadata_promotes_to_auto():
    o = FakeOrchestrator()
    t = {"hash": "h1", "name": "abf-063ch", "tags": "precheck, metadata-timeout, observe", "category": "", "state": "downloading", "size": 123456, "total_size": 123456}
    promoted = o.promote_observe_if_ready(t)
    assert promoted is True
    assert ("qpost", "/api/v2/torrents/removeTags", {"hashes": "h1", "tags": "hold,metadata-timeout,observe,precheck"}) in o.ops
    assert ("qpost", "/api/v2/torrents/addTags", {"hashes": "h1", "tags": "auto,checked"}) in o.ops
    assert ("qpost", "/api/v2/torrents/filePrio", {"hash": "h1", "id": "1", "priority": "0"}) in o.ops
    assert ("qpost", "/api/v2/torrents/filePrio", {"hash": "h1", "id": "0", "priority": "1"}) in o.ops
    assert ("qpost", "/api/v2/torrents/setCategory", {"hashes": "h1", "category": "auto"}) in o.ops
    assert ("qpost", "/api/v2/torrents/setForceStart", {"hashes": "h1", "value": "false"}) in o.ops
    assert ("stop", "h1") in o.ops


if __name__ == "__main__":
    test_observe_torrent_with_metadata_promotes_to_auto()
    print("ok")
