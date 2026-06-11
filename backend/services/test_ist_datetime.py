"""Tests for IST datetime helpers."""
from datetime import datetime

import pytz

from backend.services.ist_datetime import ist_isoformat, ist_midnight, naive_ist

IST = pytz.timezone("Asia/Kolkata")


def test_naive_ist_from_utc_aware():
    utc = pytz.UTC.localize(datetime(2026, 6, 11, 7, 45, 0))
    assert naive_ist(utc) == datetime(2026, 6, 11, 13, 15, 0)


def test_ist_isoformat_appends_offset():
    dt = datetime(2026, 6, 11, 13, 15, 0)
    assert ist_isoformat(dt) == "2026-06-11T13:15:00+05:30"


def test_ist_midnight():
    aware = IST.localize(datetime(2026, 6, 11, 15, 30, 0))
    assert ist_midnight(aware) == datetime(2026, 6, 11, 0, 0, 0)
