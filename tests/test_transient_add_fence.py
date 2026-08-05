from __future__ import annotations

from qbt_orchestrator.capacity_assessment import CapacityAssessmentBuilder
from qbt_orchestrator.capacity_state import build_capacity_observation
from qbt_orchestrator.cleanup_policy import cleanup_eligibility
from qbt_orchestrator.planner import _is_managed
from qbt_orchestrator.torrent_ownership import has_transient_add_fence, is_managed_auto
from qbt_orchestrator.work_items import build_full_finish_work_items


def _fenced_snapshot():
    return {
        "hash": "a" * 40,
        "category": "auto",
        "tags": "checked,add-item-" + "b" * 32,
        "state": "downloading",
        "amount_left": 64 * 1024**2,
        "progress": 0.1,
        "size": 128 * 1024**2,
    }


def test_opaque_fence_excludes_planner_managed_selection():
    snap = _fenced_snapshot()
    assert has_transient_add_fence(snap)
    assert not is_managed_auto(snap)
    assert not _is_managed(snap)


def test_opaque_fence_skips_full_finish_work_items():
    snap = _fenced_snapshot()
    items = build_full_finish_work_items({snap["hash"]: snap}, now=1_700_000_000)
    assert items == []


def test_opaque_fence_excluded_from_capacity_observation_and_assessment():
    snap = _fenced_snapshot()
    observation = build_capacity_observation(
        {snap["hash"]: snap},
        available_growth_bytes=10 * 1024**3,
        selected_hashes=set(),
        disk_releasing_jobs=0,
        free_bytes=10 * 1024**3,
        observed_at=1_700_000_000,
    )
    assert observation.managed_incomplete == 0

    assessment = CapacityAssessmentBuilder().build(
        {snap["hash"]: snap},
        health_by_hash={},
        free_bytes=10 * 1024**3,
        target_free_bytes=5 * 1024**3,
        available_growth_bytes=10 * 1024**3,
        selected_hashes=set(),
        disk_releasing_jobs=0,
        observed_at=1_700_000_000,
        scheduler_mode="normal",
    )
    evidence = assessment.torrents[snap["hash"]]
    assert evidence.managed is False


def test_opaque_fence_blocks_cleanup_eligibility():
    snap = _fenced_snapshot()
    result = cleanup_eligibility(
        snap,
        canonical_remote_verified=True,
        free_bytes=10 * 1024**3,
        pressure_free_bytes=1 * 1024**3,
        min_seed_sec=0,
        min_ratio=0.0,
        max_retention_sec=0,
        now=1_700_000_000,
    )
    assert result.allowed is False
    assert result.reason == "transient_add_fence"


def test_opaque_fence_shared_helpers_across_workers():
    from qbt_orchestrator.carousel import _is_managed as carousel_managed
    from qbt_orchestrator.file_batch import _is_managed as file_batch_managed
    from qbt_orchestrator.junk_janitor import _is_managed as junk_managed
    from qbt_orchestrator.path_reconcile import _is_managed as path_managed
    from qbt_orchestrator.seeding_preemption import _is_managed as seed_managed
    from qbt_orchestrator.soak_queue import _is_managed as soak_managed

    snap = _fenced_snapshot()
    normal = {
        "hash": "c" * 40,
        "category": "auto",
        "tags": "checked",
        "state": "downloading",
        "amount_left": 64 * 1024**2,
    }
    for helper in (
        carousel_managed,
        file_batch_managed,
        junk_managed,
        path_managed,
        seed_managed,
        soak_managed,
    ):
        assert helper(snap) is False
        assert helper(normal) is True
