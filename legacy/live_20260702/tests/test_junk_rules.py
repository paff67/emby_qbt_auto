#!/usr/bin/env python3
import importlib.util
import sys
from pathlib import Path

ROOT = Path("/opt/qbt-orchestrator")
sys.path.insert(0, str(ROOT))

from junk_rules import is_text_link_junk, text_link_junk_reason  # noqa: E402


def load_orchestrator_class():
    spec = importlib.util.spec_from_file_location("orchestrator", ROOT / "orchestrator.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.Orchestrator


def assert_junk(path):
    assert is_text_link_junk(path), f"expected junk: {path}"
    assert text_link_junk_reason(path), f"expected reason: {path}"


def assert_not_junk(path):
    assert not is_text_link_junk(path), f"expected clean: {path}"
    assert text_link_junk_reason(path) is None, f"expected no reason: {path}"


def test_text_link_junk_requires_suffix_and_name_regex():
    # 命中：后缀属于 html/url/txt，且文件名/路径命中广告、跳转、推广正则
    assert_junk("ABF-361/聚 合 全 網 H 直 播.html")
    assert_junk("ABF-361/最 新 位 址 獲 取：489155.com 收藏不迷路.txt")
    assert_junk("JUR-734/manko.fun.url")
    assert_junk("0421/論壇文宣/东方秋白@1024草榴社区t66y.com.url")
    assert_junk("SQTE-685/[ x18r.tv ].url")
    assert_junk("boko-028/全 网 最 劲 体 育 电 竞 直 播 平台.url")

    # 不命中：不能只因为后缀是 .txt/.html/.url 就清洗
    assert_not_junk("Movie/readme.txt")
    assert_not_junk("Movie/notes.txt")
    assert_not_junk("Movie/index.html")
    assert_not_junk("Movie/metadata.html")
    assert_not_junk("Movie/plain.url")
    assert_not_junk("Movie/subtitle.srt")


def test_orchestrator_junk_and_full_upload_filter_use_text_link_regex():
    Orchestrator = load_orchestrator_class()
    o = Orchestrator.__new__(Orchestrator)
    o.cfg = {
        "batching": {
            "enabled": True,
            "min_file_size_mb_for_main": 100,
            "skip_junk_patterns": [],
        },
        "cover_policy": {
            "enabled": True,
            "hard_junk_regex": [],
            "image_ext_regex": [r"(?i)\.(jpg|jpeg|png|webp)$"],
            "video_ext_regex": [r"(?i)\.(mp4|mkv|avi|mov|wmv|ts|m4v)$"],
        },
    }

    assert o.junk("ABF-361/聚 合 全 網 H 直 播.html", 145)
    assert o.junk("ABF-361/最 新 位 址 獲 取：489155.com 收藏不迷路.txt", 136)
    assert not o.junk("Movie/readme.txt", 100)
    assert not o.junk("Movie/index.html", 100)

    files = [
        {"name": "ABF-361/489155.com@ABF-361.mp4", "size": 4 * 1024**3, "progress": 1},
        {"name": "ABF-361/聚 合 全 網 H 直 播.html", "size": 145, "progress": 1},
        {"name": "ABF-361/readme.txt", "size": 100, "progress": 1},
    ]
    assert o.full_upload_indices(files) == [0, 2]


if __name__ == "__main__":
    test_text_link_junk_requires_suffix_and_name_regex()
    test_orchestrator_junk_and_full_upload_filter_use_text_link_regex()
    print("ok")
