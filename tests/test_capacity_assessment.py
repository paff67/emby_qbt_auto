from qbt_orchestrator.db import migrate, readonly_connect


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
