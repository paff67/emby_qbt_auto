#!/usr/bin/env python3
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "legacy" / "live_20260702"
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("orchestrator", ROOT / "orchestrator.py")
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)
Orchestrator = mod.Orchestrator
GB = mod.GB
MB = mod.MB


class FakeOrchestrator(Orchestrator):
    def __init__(self, free_gb=8.0):
        self.ops = []
        self._free_gb = free_gb
        self.mode = "live"
        self.qbt = {
            "category_auto": "auto",
            "tag_auto": "auto",
            "tag_hold": "hold",
            "tag_no_batch": "no-batch",
            "tag_seed_long": "seed-long",
            "save_path": "/downloads/active",
            "temp_path": "/downloads/incomplete",
        }
        self.cfg = {
            "scheduler": {
                "planner": "size_aware",
                "size_aware_enabled": True,
                "dynamic_set_qbt_queue_limits": True,
                "max_active_downloads": 6,
                "stable_slots": 4,
                "probe_slots": 1,
                "qbt_extra_active_torrents": 12,
                "per_torrent_overhead_mb": 256,
            },
            "disk": {
                "target_min_free_gb": 3,
                "batch_overhead_gb": 0.5,
                "max_batch_gb": 4,
                "pause_new_free_below_gb": 3,
            },
            "batching": {
                "enabled": True,
                "huge_torrent_threshold_gb": 1,
                "min_file_size_mb_for_main": 100,
                "skip_junk_patterns": [],
            },
            "cover_policy": {
                "enabled": True,
                "hard_junk_regex": [],
                "video_ext_regex": [r"(?i)\.(mp4|mkv|avi|mov|wmv|ts|m4v)$"],
                "image_ext_regex": [r"(?i)\.(jpg|jpeg|png|webp)$"],
            },
            "slow_policy": {"enabled": False},
        }

    def qpost(self, path, data=None, files=None, ok=(200,)):
        self.ops.append(("qpost", path, data or {}))
        return "Ok."

    def qjson(self, path, params=None):
        self.ops.append(("qjson", path, params or {}))
        if path == "/api/v2/torrents/info" and hasattr(self, "torrent_info"):
            return self.torrent_info
        return []

    def log(self, msg):
        self.ops.append(("log", msg))

    def event(self, h, level, msg):
        self.ops.append(("event", h, level, msg))

    def free_gb(self):
        return self._free_gb

    def backup_torrent_file(self, h):
        self.ops.append(("backup", h))

    def start_torrent(self, h):
        self.ops.append(("start", h))

    def stop_torrent(self, h):
        self.ops.append(("stop", h))

    def put_state(self, h, **kw):
        self.ops.append(("put_state", h, kw))

    def get_state(self, h, name=None, added_on=0):
        return {"archived_indices": "[]", "skipped_indices": "[]", "current_batch": None, "done": 0}


def test_ensure_qbt_basics_forces_preallocation_off():
    o = FakeOrchestrator()
    o.ensure_qbt_basics()
    prefs = [op[2]["json"] for op in o.ops if op[0] == "qpost" and op[1] == "/api/v2/app/setPreferences"][-1]
    assert prefs["preallocate_all"] is False


def test_huge_batch_start_enables_sequential_download():
    o = FakeOrchestrator(free_gb=20)
    o.size_aware_enabled = True
    o.planned_hashes = {"h1"}
    o.planned_budgets = {"h1": int(3 * GB)}
    files = [
        {"name": "BIG/BIG-001.mp4", "size": int(2 * GB), "progress": 0},
        {"name": "BIG/ad.html", "size": 100, "progress": 0},
    ]
    t = {"hash": "h1", "name": "BIG", "tags": "auto", "category": "auto", "total_size": int(2 * GB), "progress": 0, "uploaded": 0}
    st = {"archived_indices": "[]", "skipped_indices": "[]", "current_batch": None, "batch_no": 0}
    o.handle_huge(t, files, st)
    assert ("qpost", "/api/v2/torrents/toggleSequentialDownload", {"hashes": "h1"}) in o.ops
    assert ("start", "h1") in o.ops


def test_dynamic_batch_limit_holds_when_no_remaining_file_fits():
    o = FakeOrchestrator(free_gb=8.0)
    files = [
        {"name": "BIG/BIG-001.mp4", "size": int(5 * GB), "progress": 0},
        {"name": "BIG/BIG-002.mp4", "size": int(6 * GB), "progress": 0},
    ]
    t = {"hash": "h2", "name": "BIG2", "tags": "auto", "category": "auto", "state": "stoppedDL", "total_size": int(11 * GB), "progress": 0}
    st = {"archived_indices": "[]", "skipped_indices": "[]", "current_batch": None, "done": 0}
    o.build_size_aware_plan([t], {"h2": files}, {"h2": st})
    assert "h2" not in o.planned_hashes
    assert ("qpost", "/api/v2/torrents/addTags", {"hashes": "h2", "tags": "hold,space-insufficient"}) in o.ops
    assert ("stop", "h2") in o.ops


def test_choose_batch_never_selects_file_larger_than_dynamic_limit_even_if_free_space_is_large():
    o = FakeOrchestrator(free_gb=20.0)
    files = [{"name": "BIG/BIG-001.mp4", "size": int(5 * GB), "progress": 0}]
    batch = o.choose_batch(files, archived=set(), skipped=set(), budget_bytes=int(10 * GB))
    assert batch == []




def test_next_main_file_over_limit_is_held_even_if_later_file_fits():
    o = FakeOrchestrator(free_gb=20.0)
    files = [
        {"name": "BIG/BIG-001.mp4", "size": int(5 * GB), "progress": 0},
        {"name": "BIG/BIG-002.mp4", "size": int(1 * GB), "progress": 0},
    ]
    t = {"hash": "h4", "name": "BIG4", "tags": "auto", "category": "auto", "state": "stoppedDL", "total_size": int(6 * GB), "progress": 0}
    st = {"archived_indices": "[]", "skipped_indices": "[]", "current_batch": None, "done": 0}
    o.handle_huge(t, files, st)
    assert ("qpost", "/api/v2/torrents/addTags", {"hashes": "h4", "tags": "hold,space-insufficient"}) in o.ops
    assert ("stop", "h4") in o.ops
    assert ("start", "h4") not in o.ops




def test_existing_current_batch_keeps_sequential_download_enabled():
    o = FakeOrchestrator(free_gb=20.0)
    files = [
        {"name": "BIG/BIG-001.mp4", "size": int(2 * GB), "progress": 0.5},
    ]
    t = {"hash": "h5", "name": "BIG5", "tags": "auto", "category": "auto", "state": "downloading", "total_size": int(2 * GB), "progress": 0.5, "uploaded": 0}
    st = {"archived_indices": "[]", "skipped_indices": "[]", "current_batch": "[0]", "done": 0}
    o.handle_huge(t, files, st)
    assert ("qpost", "/api/v2/torrents/toggleSequentialDownload", {"hashes": "h5"}) in o.ops




def test_sequential_download_is_not_toggled_when_already_enabled():
    o = FakeOrchestrator(free_gb=20.0)
    o.size_aware_enabled = True
    o.planned_hashes = {"h6"}
    o.planned_budgets = {"h6": int(3 * GB)}
    o.torrent_info = [{"hash": "h6", "seq_dl": True}]
    files = [{"name": "BIG/BIG-001.mp4", "size": int(2 * GB), "progress": 0}]
    t = {"hash": "h6", "name": "BIG6", "tags": "auto", "category": "auto", "total_size": int(2 * GB), "progress": 0, "uploaded": 0}
    st = {"archived_indices": "[]", "skipped_indices": "[]", "current_batch": None, "batch_no": 0}
    o.handle_huge(t, files, st)
    assert not any(op[0] == "qpost" and op[1] == "/api/v2/torrents/toggleSequentialDownload" for op in o.ops)
    assert ("start", "h6") in o.ops


if __name__ == "__main__":
    test_ensure_qbt_basics_forces_preallocation_off()
    test_huge_batch_start_enables_sequential_download()
    test_dynamic_batch_limit_holds_when_no_remaining_file_fits()
    test_choose_batch_never_selects_file_larger_than_dynamic_limit_even_if_free_space_is_large()
    test_next_main_file_over_limit_is_held_even_if_later_file_fits()
    test_existing_current_batch_keeps_sequential_download_enabled()
    test_sequential_download_is_not_toggled_when_already_enabled()
    print("ok")
