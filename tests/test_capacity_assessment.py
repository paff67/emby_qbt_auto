from dataclasses import fields

import pytest

from qbt_orchestrator.capacity_assessment import (
    CapacityAssessment,
    CapacityAssessmentBuilder,
    TorrentCapacityEvidence,
)
from qbt_orchestrator.db import migrate, readonly_connect


def _evidence(
    torrent_hash="h", *, managed=True, incomplete=True, viable=True
):
    return TorrentCapacityEvidence(
        hash=torrent_hash,
        managed=managed,
        incomplete=incomplete,
        amount_left=1 if incomplete else 0,
        completed_bytes=10,
        availability=1.0 if viable else 0.5,
        complete_sources=1 if viable else 0,
        no_progress_since=100,
        viable=viable,
        viability_reason="complete_source" if viable else "stale_without_complete_source",
    )


def _assessment(torrents):
    return CapacityAssessment(
        generation=0,
        observed_at=1900,
        scheduler_mode="drain",
        free_bytes=100,
        target_free_bytes=1000,
        available_growth_bytes=100,
        selected_hashes=frozenset(),
        disk_releasing_jobs=0,
        torrents=torrents,
    )


def test_capacity_assessment_exposes_read_only_torrents():
    evidence = _evidence()
    assessment = _assessment({"h": evidence})

    with pytest.raises(TypeError):
        assessment.torrents["other"] = evidence
    with pytest.raises(AttributeError):
        assessment.torrents.clear()


def test_capacity_assessment_defensively_copies_torrents():
    evidence = _evidence()
    source = {"h": evidence}
    assessment = _assessment(source)

    source.clear()

    assert assessment.torrents == {"h": evidence}


def test_with_generation_preserves_isolated_read_only_evidence():
    evidence = _evidence()
    source = {"h": evidence}
    assessment = _assessment(source)

    updated = assessment.with_generation(7)
    source.clear()

    assert updated.generation == 7
    for item in fields(CapacityAssessment):
        if item.name != "generation":
            assert getattr(updated, item.name) == getattr(assessment, item.name)
    assert updated.torrents == assessment.torrents == {"h": evidence}
    assert updated.torrents is not assessment.torrents
    assert updated.torrents["h"] is assessment.torrents["h"]
    with pytest.raises(TypeError):
        updated.torrents["other"] = evidence


def test_builder_contracts_cover_management_staleness_and_normalization():
    assessment = CapacityAssessmentBuilder(viability_stale_sec=1800).build(
        {
            "boundary": {
                "hash": "boundary",
                "category": "auto",
                "amount_left": 5,
                "completed": -1,
                "availability": 0.5,
                "num_seeds": -1,
                "num_complete": -2,
                "dlspeed": -3,
            },
            "recent": {
                "hash": "recent",
                "tags": "auto",
                "amount_left": 5,
                "completed": 10,
                "availability": 0.5,
            },
            "held": {
                "hash": "held",
                "category": "auto",
                "tags": "auto, hold",
                "amount_left": -5,
                "completed": -10,
                "availability": -1,
                "num_seeds": -1,
                "num_complete": -2,
            },
        },
        {
            "boundary": {"no_progress_since": 100},
            "recent": {"no_progress_since": 101},
            "held": {"no_progress_since": 100},
        },
        observed_at=1900,
        scheduler_mode="drain",
        free_bytes=-1,
        target_free_bytes=-2,
        available_growth_bytes=-3,
        selected_hashes={"recent"},
        disk_releasing_jobs=-4,
    )

    boundary = assessment.torrents["boundary"]
    assert boundary.managed is True
    assert boundary.viable is False
    assert boundary.viability_reason == "stale_without_complete_source"
    assert boundary.completed_bytes == 0
    assert boundary.complete_sources == 0

    recent = assessment.torrents["recent"]
    assert recent.managed is True
    assert recent.viable is True
    assert recent.viability_reason == "recent_progress"

    held = assessment.torrents["held"]
    assert held.managed is False
    assert held.incomplete is False
    assert held.amount_left == 0
    assert held.completed_bytes == 0
    assert held.availability is None
    assert held.complete_sources == 0

    assert assessment.managed_incomplete == 2
    assert assessment.viable_finish == 1
    assert assessment.nonviable_finish == 1
    assert assessment.free_bytes == 0
    assert assessment.target_free_bytes == 0
    assert assessment.available_growth_bytes == 0
    assert assessment.disk_releasing_jobs == 0


def test_leech_peer_does_not_make_incomplete_torrent_viable():
    assessment = CapacityAssessmentBuilder(viability_stale_sec=1800).build(
        {
            "h": {
                "hash": "h",
                "category": "auto",
                "amount_left": 900,
                "completed": 100,
                "availability": 0.5,
                "num_seeds": 0,
                "num_complete": 0,
                "num_peers": 4,
            }
        },
        {"h": {"completed_bytes": 100, "progress": 0.1, "no_progress_since": 100}},
        observed_at=7300,
        scheduler_mode="drain",
        free_bytes=100,
        target_free_bytes=1000,
        available_growth_bytes=100,
        selected_hashes=set(),
        disk_releasing_jobs=0,
    )
    item = assessment.torrents["h"]
    assert item.viable is False
    assert item.viability_reason == "stale_without_complete_source"
    assert item.complete_sources == 0


def test_unknown_availability_is_not_reclaimable_evidence():
    assessment = CapacityAssessmentBuilder(viability_stale_sec=1800).build(
        {
            "h": {
                "hash": "h",
                "category": "auto",
                "amount_left": 900,
                "completed": 100,
                "availability": -1,
                "num_seeds": 0,
                "num_peers": 0,
            }
        },
        {"h": {"completed_bytes": 100, "progress": 0.1, "no_progress_since": 100}},
        observed_at=7300,
        scheduler_mode="drain",
        free_bytes=100,
        target_free_bytes=1000,
        available_growth_bytes=100,
        selected_hashes=set(),
        disk_releasing_jobs=0,
    )
    assert assessment.torrents["h"].availability is None


def test_capacity_assessment_schema_is_additive(tmp_path):
    db = tmp_path / "state.sqlite"
    migrate(db)
    con = readonly_connect(db)
    try:
        health = {row[1] for row in con.execute("pragma table_info(torrent_health)")}
        capacity = {row[1] for row in con.execute("pragma table_info(capacity_state)")}
        reclaims = {row[1] for row in con.execute("pragma table_info(capacity_reclaims)")}
        assert {"reclaimable_since", "capacity_viable", "capacity_reason", "capacity_assessed_at", "capacity_generation"} <= health
        assert "assessment_generation" in capacity
        assert {"reclaimable_since", "capacity_generation", "capacity_reason", "assessment_json"} <= reclaims
        row = con.execute("select current_generation from capacity_assessment_state where id=1").fetchone()
        assert row is None
    finally:
        con.close()
