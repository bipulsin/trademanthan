"""Tests for Vajra rating staleness guards."""
from datetime import datetime, timedelta

import pytz

from backend.services.vajra.staleness import (
    DEPLOY_EPOCH_FILE,
    is_vajra_db_snapshot_stale,
    is_vajra_ratings_stale,
    write_deploy_epoch,
)

IST = pytz.timezone("Asia/Kolkata")


def _dt(minutes_ago: int = 0) -> datetime:
    return datetime.now(IST) - timedelta(minutes=minutes_ago)


def test_fresh_db_rows_not_stale():
    rows = [{"evs_score": 60.0, "stock": "A"}]
    stale, reason = is_vajra_ratings_stale(rows, _dt(2))
    assert stale is False
    assert reason is None


def test_missing_evs_marks_stale():
    rows = [{"evs_score": None}] * 10
    stale, reason = is_vajra_ratings_stale(rows, _dt(1))
    assert stale is True
    assert reason == "missing_evs"


def test_pre_deploy_snapshot_stale():
    rows = [{"evs_score": 55.0}]
    write_deploy_epoch(_dt(0))
    try:
        stale, reason = is_vajra_ratings_stale(rows, _dt(5))
        assert stale is True
        assert reason == "pre_deploy"
    finally:
        DEPLOY_EPOCH_FILE.unlink(missing_ok=True)


def test_age_threshold_stale():
    rows = [{"evs_score": 55.0}]
    stale, reason = is_vajra_ratings_stale(rows, _dt(10))
    assert stale is True
    assert reason == "age"


def test_db_snapshot_missing_evs_ratio():
    stale, reason = is_vajra_db_snapshot_stale(
        row_count=100,
        missing_evs_count=10,
        updated_at=_dt(1),
    )
    assert stale is True
    assert reason == "missing_evs"


def test_db_snapshot_fresh():
    stale, reason = is_vajra_db_snapshot_stale(
        row_count=100,
        missing_evs_count=2,
        updated_at=_dt(2),
    )
    assert stale is False
