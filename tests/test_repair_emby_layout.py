from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = ROOT / "tools" / "gdrive_backfill" / "repair_emby_layout.py"


def _load_tool():
    spec = importlib.util.spec_from_file_location("repair_emby_layout", TOOL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class NfoRemote:
    def __init__(self, objects: dict[str, int]):
        self.objects = dict(objects)
        self.copytos: list[tuple[str, str]] = []

    def stat(self, remote: str):
        size = self.objects.get(remote)
        return None if size is None else {"Path": remote, "Size": size, "Hashes": {}}

    def copyto(self, source: str, target: str):
        self.copytos.append((source, target))
        if source.startswith("gcrypt:"):
            Path(target).write_bytes(b"<movie><plot>keep</plot></movie>")
            return
        self.objects[target] = Path(source).stat().st_size


def test_rewrite_uses_short_local_filename_for_multibyte_remote_title(tmp_path: Path):
    tool = _load_tool()
    media_id = "BLK-694"
    title = "【FANZA限定】" + ("兄貴の彼女が早漏改善訓練" * 12)
    source = f"gcrypt:/{media_id}-old/{media_id}.mp4"
    plan = tool.build_migration_plan(
        [{"Path": source.split(":/", 1)[1], "Size": 100, "Hashes": {}}],
        {media_id: {"title": title, "confidence": 1.0}},
    )
    video_target = next(action.target for action in plan.actions if action.kind == "video")
    remote = NfoRemote({video_target: 100})

    rewritten = tool.rewrite_verified_nfos(
        plan,
        {media_id: {"title": title}},
        remote,
        report_dir=tmp_path,
    )

    assert rewritten == {media_id}
    local_upload = next(source for source, target in remote.copytos if target.endswith(".nfo"))
    assert Path(local_upload).name == f"{media_id}.nfo"


def test_rewrite_resumes_verified_nfo_without_uploading_again(tmp_path: Path):
    tool = _load_tool()
    media_id = "BBAN-582"
    title = "影片名称"
    source = f"gcrypt:/{media_id}-old/{media_id}.mp4"
    plan = tool.build_migration_plan(
        [{"Path": source.split(":/", 1)[1], "Size": 100, "Hashes": {}}],
        {media_id: {"title": title, "confidence": 1.0}},
    )
    video_target = next(action.target for action in plan.actions if action.kind == "video")
    nfo_target = video_target.rsplit(".", 1)[0] + ".nfo"
    remote = NfoRemote({video_target: 100, nfo_target: 321})
    (tmp_path / "nfo-rewrite.jsonl").write_text(
        tool.json.dumps(
            {
                "normalized_id": media_id,
                "state": "verified",
                "target": nfo_target,
                "size": 321,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    rewritten = tool.rewrite_verified_nfos(
        plan,
        {media_id: {"title": title}},
        remote,
        report_dir=tmp_path,
    )

    assert rewritten == {media_id}
    assert remote.copytos == []
