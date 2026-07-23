from qbt_orchestrator.capacity_assessment import CapacityAssessmentBuilder
from qbt_orchestrator.db import migrate, readonly_connect


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
