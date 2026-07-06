"""L1 — indicator staleness helpers."""
from datetime import datetime, timedelta

import pytz

from backend.services.daily_checklist_live import is_indicator_stale

IST = pytz.timezone("Asia/Kolkata")


def test_stale_when_indicator_as_of_missing():
    assert is_indicator_stale(None, None) is True


def test_fresh_when_recent():
    now = datetime.now(IST)
    assert is_indicator_stale(now - timedelta(minutes=2), now) is False


def test_stale_when_older_than_threshold():
    now = datetime.now(IST)
    old = now - timedelta(minutes=15)
    assert is_indicator_stale(old, now) is True


def test_stale_when_rs_scan_much_newer():
    now = datetime.now(IST)
    ia = now - timedelta(minutes=5)
    scan = now
    assert is_indicator_stale(ia, scan, stale_minutes=3) is True
