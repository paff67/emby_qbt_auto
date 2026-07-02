#!/usr/bin/env python3
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "legacy" / "live_20260702"
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("qbt_add_checked", ROOT / "qbt_add_checked.py")
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)
CheckedAdder = mod.CheckedAdder


class FakeAdder(CheckedAdder):
    def __init__(self):
        self.ops = []
        self.dedupe = {
            "clear_tags_after_check": [],
            "metadata_timeout_tags": ["precheck", "metadata-timeout", "observe"],
        }
        self.qbt = {"category_auto": "auto"}

    def stop_torrent(self, h):
        self.ops.append(("stop", h))

    def start_torrent(self, h):
        self.ops.append(("start", h))

    def add_tags(self, h, tags):
        self.ops.append(("add_tags", h, tuple(tags)))

    def remove_tags(self, h, tags):
        self.ops.append(("remove_tags", h, tuple(tags)))

    def set_category(self, h, category):
        self.ops.append(("set_category", h, category))

    def set_force_start(self, h, value):
        self.ops.append(("force_start", h, value))

    def set_all_file_priority_zero(self, request_id, h, files):
        self.ops.append(("zero", h, len(files)))

    def event(self, request_id, level, action, message, data=None):
        self.ops.append(("event", action, message, data or {}))


def test_metadata_timeout_enters_observe_and_keeps_running():
    a = FakeAdder()
    tags = a.finalize_qbt("req1", "hash1", "metadata_timeout", [])
    assert tags == ["precheck", "metadata-timeout", "observe"]
    assert ("force_start", "hash1", True) in a.ops
    assert ("start", "hash1") in a.ops
    assert ("stop", "hash1") not in a.ops
    add_ops = [op for op in a.ops if op[0] == "add_tags"]
    assert add_ops and add_ops[-1][2] == ("precheck", "metadata-timeout", "observe")


if __name__ == "__main__":
    test_metadata_timeout_enters_observe_and_keeps_running()
    print("ok")
