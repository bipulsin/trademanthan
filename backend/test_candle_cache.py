"""Tests for the shared, range-aware candle cache."""
import importlib

import backend.services.market_data.candle_cache as cc


def _reset():
    # Fresh module state per test (cache + metrics are module-level).
    importlib.reload(cc)
    return cc


def _candles(dates):
    # One candle per date at 09:15 IST.
    return [{"timestamp": f"{d}T09:15:00+05:30", "close": 100.0} for d in dates]


def test_canonical_widens_hot_intervals():
    c = _reset()
    assert c.canonical_days_back("minutes/5", 3) == 6
    assert c.canonical_days_back("minutes/5", 8) == 8  # caller wider than canonical
    assert c.canonical_days_back("days/1", 12) == 45
    assert c.canonical_days_back("hours/1", 4) == 4  # not a canonical interval


def test_filter_from_keeps_requested_window():
    c = _reset()
    candles = _candles(["2026-06-25", "2026-06-26", "2026-06-27", "2026-06-30"])
    out = c.filter_from(candles, "2026-06-27")
    assert [x["timestamp"][:10] for x in out] == ["2026-06-27", "2026-06-30"]


def test_wider_entry_serves_narrower_request_filtered():
    c = _reset()
    # Store a 6-day span; a 3-day request must be served, filtered to its window.
    span = _candles(["2026-06-24", "2026-06-25", "2026-06-26", "2026-06-27", "2026-06-29", "2026-06-30"])
    c.put("NSE_FO|1", "minutes/5", "2026-06-24", "2026-06-30", span)
    out = c.get("NSE_FO|1", "minutes/5", "2026-06-29", max_age_sec=60)
    assert out is not None
    assert [x["timestamp"][:10] for x in out] == ["2026-06-29", "2026-06-30"]


def test_narrower_entry_does_not_cover_wider_request():
    c = _reset()
    span = _candles(["2026-06-29", "2026-06-30"])
    c.put("NSE_FO|1", "minutes/5", "2026-06-29", "2026-06-30", span)
    # Request needs data back to the 24th -> cached span starts later -> miss.
    assert c.get("NSE_FO|1", "minutes/5", "2026-06-24", max_age_sec=60) is None


def test_ttl_expiry():
    c = _reset()
    span = _candles(["2026-06-30"])
    c.put("NSE_FO|1", "minutes/5", "2026-06-30", "2026-06-30", span)
    assert c.get("NSE_FO|1", "minutes/5", "2026-06-30", max_age_sec=-1) is None


def test_get_recent_ignores_window():
    c = _reset()
    span = _candles(["2026-06-29", "2026-06-30"])
    c.put("NSE_FO|1", "minutes/5", "2026-06-29", "2026-06-30", span)
    out = c.get_recent("NSE_FO|1", "minutes/5", max_age_sec=60)
    assert out is not None and len(out) == 2
    # Different interval -> miss.
    assert c.get_recent("NSE_FO|1", "minutes/15", max_age_sec=60) is None


def test_empty_put_is_noop():
    c = _reset()
    c.put("NSE_FO|1", "minutes/5", "2026-06-30", "2026-06-30", None)
    c.put("NSE_FO|1", "minutes/5", "2026-06-30", "2026-06-30", [])
    assert c.get_recent("NSE_FO|1", "minutes/5", max_age_sec=60) is None
