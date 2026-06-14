"""Tests for SQLAlchemy pool helpers in backend.database."""
import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from backend.database import db_session, get_db_pool_stats


def test_get_db_pool_stats_shape():
    stats = get_db_pool_stats()
    if not stats.get("available"):
        pytest.skip("database engine not initialized")
    assert "checked_out" in stats
    assert "max_capacity" in stats
    assert "stressed" in stats
    assert stats["max_capacity"] >= stats["checked_out"]


def test_db_session_closes_after_use():
    stats_before = get_db_pool_stats()
    if not stats_before.get("available"):
        pytest.skip("database engine not initialized")
    try:
        with db_session() as db:
            db.execute(text("SELECT 1"))
    except OperationalError:
        pytest.skip("postgres unavailable locally")
    stats_after = get_db_pool_stats()
    assert stats_after["checked_out"] <= stats_before["checked_out"]
