from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _stop_db_write_actors_after_test():
    yield
    try:
        from qbt_orchestrator.db import stop_write_actors

        stop_write_actors()
    except Exception:
        pass
