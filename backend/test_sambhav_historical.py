"""Sambhav V1 historical 10m tests — parse, chunk, throttle, boundaries, status."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from backend.services.sambhav.candles import to_ist, validate_ohlc
from backend.services.sambhav.config import IST, INSTRUMENT_KEY
from backend.services.sambhav.data_status import calibration_status_payload, compute_data_status
from backend.services.sambhav.historical import (
    SambhavAuthError,
    chunk_date_range,
    expected_10m_starts,
    fetch_10m_chunk_with_retry,
    filter_valid_10m_candles,
    handle_historical_response,
    HistoricalThrottle,
    is_expected_10m_boundary,
    parse_upstox_10m_response,
    parse_upstox_v3_candle_row,
    to_10m_row,
    validate_10m_candle,
)
from backend.services.sambhav.importer import import_historical_10m


def _ist(y, m, d, hh, mm):
    return IST.localize(datetime(y, m, d, hh, mm))


class _Resp:
    def __init__(self, status_code: int, payload: Any):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeDB:
    def __init__(self):
        self.candles: Dict[tuple, Dict[str, Any]] = {}
        self.state: Dict[str, Dict[str, Any]] = {}
        self.commits = 0

    def execute(self, sql, params=None):
        q = str(sql).lower()
        params = params or {}
        if "insert into sambhav_10m_candles" in q:
            key = (params["ik"], params["cs"])
            self.candles[key] = dict(params)
            return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [], scalar=lambda: 0)
        if "insert into sambhav_import_state" in q:
            self.state[params["ik"]] = dict(params)
            return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [], scalar=lambda: 0)
        if "from sambhav_import_state" in q:
            st = self.state.get(params.get("ik"))
            if not st:
                return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [], scalar=lambda: 0)
            row = SimpleNamespace(
                instrument_key=st["ik"],
                last_imported_ts=st.get("lit"),
                last_from_date=st.get("lf"),
                last_to_date=st.get("lt"),
                status=st.get("st"),
                detail=st.get("detail"),
                updated_at=None,
            )
            return SimpleNamespace(fetchone=lambda: row, fetchall=lambda: [row], scalar=lambda: 1)
        if "from sambhav_10m_candles" in q:
            rows = []
            for (ik, _cs), p in sorted(self.candles.items(), key=lambda x: str(x[1]["cs"])):
                if ik != params.get("ik"):
                    continue
                rows.append(
                    SimpleNamespace(
                        candle_start=p["cs"],
                        open=p["o"],
                        high=p["h"],
                        low=p["l"],
                        close=p["c"],
                        volume=p["v"],
                    )
                )
            return SimpleNamespace(fetchone=lambda rows=rows: rows[0] if rows else None, fetchall=lambda rows=rows: rows, scalar=lambda rows=rows: len(rows))
        return SimpleNamespace(fetchone=lambda: None, fetchall=lambda: [], scalar=lambda: 0)

    def commit(self):
        self.commits += 1


class FakeUpstox:
    NIFTY50_KEY = "NSE_INDEX|Nifty 50"
    base_url = "https://api.upstox.com/v3"

    def get_headers(self):
        return {"Authorization": "Bearer test", "Accept": "application/json"}

    def reload_token_from_storage(self):
        return False


def _v3_payload(rows: List[List[Any]]) -> Dict[str, Any]:
    return {"status": "success", "data": {"candles": rows}}


def test_parse_upstox_10m_api_response():
    raw = [
        ["2025-01-02T09:15:00+05:30", 23700.0, 23710.0, 23690.0, 23705.0, 0.0, 0.0],
        ["2025-01-02T09:25:00+05:30", 23705.0, 23720.0, 23700.0, 23712.0, 0.0, 0.0],
    ]
    parsed = parse_upstox_10m_response(_v3_payload(raw))
    assert len(parsed) == 2
    first = parse_upstox_v3_candle_row(raw[0])
    assert first["open"] == 23700.0
    assert first["oi"] == 0.0
    assert to_ist(first["timestamp"]) == _ist(2025, 1, 2, 9, 15)


def test_chunk_date_range_never_multi_year():
    chunks = chunk_date_range(date(2022, 1, 1), date(2026, 8, 13), chunk_days=31)
    assert chunks[0] == (date(2022, 1, 1), date(2022, 1, 31))
    assert all((b - a).days <= 30 for a, b in chunks)
    assert chunks[-1][1] == date(2026, 8, 13)
    # 31-day cap even if caller asks for 400
    huge = chunk_date_range(date(2022, 1, 1), date(2022, 6, 1), chunk_days=400)
    assert all((b - a).days <= 30 for a, b in huge)


def test_duplicate_prevention_in_filter_and_upsert(monkeypatch):
    monkeypatch.setattr("backend.services.sambhav.candles.ensure_sambhav_tables", lambda: None)
    ts = _ist(2025, 1, 2, 9, 15)
    c = {"timestamp": ts, "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 0}
    valid, reasons = filter_valid_10m_candles([c, dict(c), c])
    assert len(valid) == 1
    assert reasons.get("duplicate") == 2

    db = FakeDB()
    from backend.services.sambhav.candles import upsert_10m_candles

    row = to_10m_row(c)
    n1 = upsert_10m_candles(db, [row], instrument_key=INSTRUMENT_KEY)
    n2 = upsert_10m_candles(db, [row], instrument_key=INSTRUMENT_KEY)
    assert n1 == 1 and n2 == 1
    assert len(db.candles) == 1


def test_rate_limit_backoff_and_no_tight_loop():
    sleeps: List[float] = []
    throttle = HistoricalThrottle(delay_seconds=2.0, backoff_cap=60.0, sleep_fn=lambda s: sleeps.append(s))
    calls = {"n": 0}

    def http_get(url, headers=None, timeout=None):
        calls["n"] += 1
        if calls["n"] < 3:
            return _Resp(429, {"status": "error", "message": "rate limit"})
        return _Resp(200, _v3_payload([["2025-01-02T09:15:00+05:30", 1, 2, 0.5, 1.5, 0, 0]]))

    out = fetch_10m_chunk_with_retry(
        url="https://api.upstox.com/v3/historical-candle/x/minutes/10/2025-01-02/2025-01-02",
        headers={},
        throttle=throttle,
        http_get=http_get,
        max_retries=5,
    )
    assert len(out) == 1
    assert calls["n"] == 3
    # first request: inter-request delay not required; 429 → backoff >= 2s, never 0
    assert any(s >= 2.0 for s in sleeps)
    assert all(s > 0 for s in sleeps)


def test_auth_error_stops_without_tight_retry():
    sleeps: List[float] = []
    throttle = HistoricalThrottle(delay_seconds=2.0, sleep_fn=lambda s: sleeps.append(s))

    def http_get(url, headers=None, timeout=None):
        return _Resp(401, {"status": "error", "message": "unauthorized"})

    with pytest.raises(SambhavAuthError):
        fetch_10m_chunk_with_retry(
            url="https://example.test/x",
            headers={},
            throttle=throttle,
            http_get=http_get,
            max_retries=5,
            reload_auth=lambda: False,
        )


def test_5xx_retries_then_ok():
    sleeps: List[float] = []
    throttle = HistoricalThrottle(delay_seconds=2.0, sleep_fn=lambda s: sleeps.append(s))
    n = {"i": 0}

    def http_get(url, headers=None, timeout=None):
        n["i"] += 1
        if n["i"] == 1:
            return _Resp(503, {"status": "error"})
        return _Resp(200, _v3_payload([["2025-01-02T09:25:00+05:30", 10, 11, 9, 10.5, 0, 0]]))

    out = fetch_10m_chunk_with_retry(
        url="https://example.test/x",
        headers={},
        throttle=throttle,
        http_get=http_get,
        max_retries=4,
    )
    assert out[0]["close"] == 10.5
    assert n["i"] == 2


def test_timezone_handling_ist():
    utc = datetime(2025, 1, 2, 3, 45, tzinfo=__import__("datetime").timezone.utc)  # 09:15 IST
    ts = to_ist(utc)
    assert ts.tzinfo is not None
    assert ts.hour == 9 and ts.minute == 15
    assert to_ist("2025-01-02T09:15:00+05:30").hour == 9
    assert to_ist("2025-01-02T03:45:00Z").hour == 9


def test_candle_boundary_convention():
    assert is_expected_10m_boundary(_ist(2025, 1, 2, 9, 15))
    assert is_expected_10m_boundary(_ist(2025, 1, 2, 9, 25))
    assert is_expected_10m_boundary(_ist(2025, 1, 2, 15, 25))
    assert not is_expected_10m_boundary(_ist(2025, 1, 2, 9, 10))
    assert not is_expected_10m_boundary(_ist(2025, 1, 2, 9, 20))
    assert not is_expected_10m_boundary(_ist(2025, 1, 2, 15, 30))
    starts = expected_10m_starts(date(2025, 1, 2))
    assert starts[0] == _ist(2025, 1, 2, 9, 15)
    assert starts[-1] == _ist(2025, 1, 2, 15, 25)
    assert len(starts) == 38
    row = to_10m_row(
        {
            "timestamp": _ist(2025, 1, 2, 9, 15),
            "open": 1,
            "high": 2,
            "low": 0.5,
            "close": 1.5,
            "volume": 0,
        }
    )
    assert row["candle_end"] == _ist(2025, 1, 2, 9, 25)
    assert row["is_complete"] is True


def test_invalid_ohlc_and_overnight_rejected():
    assert validate_ohlc(100, 101, 99, 100.5)
    assert not validate_ohlc(100, 99, 101, 100)
    overnight = {
        "timestamp": _ist(2025, 1, 2, 16, 15),
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100,
        "volume": 0,
    }
    ok, reason = validate_10m_candle(overnight)
    assert ok is False and reason == "outside_session"
    bad = {
        "timestamp": _ist(2025, 1, 2, 9, 15),
        "open": 100,
        "high": 90,
        "low": 99,
        "close": 100,
        "volume": 0,
    }
    ok, reason = validate_10m_candle(bad)
    assert ok is False and reason == "invalid_ohlc"
    kind, candles, _ = handle_historical_response(
        200,
        _v3_payload([["2025-01-02T03:00:00+05:30", 1, 2, 0.5, 1.5, 0, 0]]),
    )
    assert kind == "ok"
    valid, reasons = filter_valid_10m_candles(candles)
    assert valid == []
    assert reasons.get("outside_session") == 1


def test_pilot_import_mocked(monkeypatch):
    monkeypatch.setattr("backend.services.sambhav.importer.ensure_sambhav_tables", lambda: None)
    monkeypatch.setattr("backend.services.sambhav.candles.ensure_sambhav_tables", lambda: None)
    monkeypatch.setattr("backend.services.sambhav.tables.ensure_sambhav_tables", lambda: None)

    starts = expected_10m_starts(date(2025, 1, 2))
    rows = []
    for i, ts in enumerate(starts):
        o = 23000 + i
        rows.append([ts.isoformat(), o, o + 2, o - 2, o + 1, 0.0, 0.0])

    def http_get(url, headers=None, timeout=None):
        assert "minutes/10" in url
        assert "2025-01-31" in url or "2025-01-01" in url
        return _Resp(200, _v3_payload(rows))

    db = FakeDB()
    out = import_historical_10m(
        db,
        from_date=date(2025, 1, 1),
        to_date=date(2025, 1, 31),
        resume=False,
        upstox=FakeUpstox(),
        throttle=HistoricalThrottle(delay_seconds=0, sleep_fn=lambda s: None),
        http_get=http_get,
    )
    assert out["ok"] is True
    assert out["interval"] == "10m"
    assert out["upserted_10m"] == 38
    assert out["chunks"] == 1
    assert len(db.candles) == 38


def test_data_status_not_imported(monkeypatch):
    monkeypatch.setattr("backend.services.sambhav.data_status.ensure_sambhav_tables", lambda: None)
    monkeypatch.setattr("backend.services.sambhav.importer.ensure_sambhav_tables", lambda: None)
    db = FakeDB()
    st = compute_data_status(db)
    assert st["status"] == "NOT_IMPORTED"
    assert st["interval"] == "10m"
    assert st["candle_count"] == 0
    assert st["instrument"] == "NIFTY 50"
    assert "1m" not in st["interval"]


def test_data_status_pass_after_full_session(monkeypatch):
    monkeypatch.setattr("backend.services.sambhav.data_status.ensure_sambhav_tables", lambda: None)
    monkeypatch.setattr("backend.services.sambhav.data_status.get_import_state", lambda db, instrument_key=None: {"status": "done"})
    monkeypatch.setattr("backend.services.sambhav.data_status.load_nse_holiday_dates", lambda a, b: set())
    monkeypatch.setattr(
        "backend.services.sambhav.data_status.iter_trading_days",
        lambda a, b, holiday_dates=None: [date(2025, 1, 2)],
    )
    db = FakeDB()
    for ts in expected_10m_starts(date(2025, 1, 2)):
        db.candles[(INSTRUMENT_KEY, ts)] = {
            "ik": INSTRUMENT_KEY,
            "cs": ts,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 0.0,
        }
    st = compute_data_status(db, start_date=date(2025, 1, 2), end_date=date(2025, 1, 2))
    assert st["candle_count"] == 38
    assert st["trading_days"] == 1
    assert st["missing_candles"] == 0
    assert st["duplicates"] == 0
    assert st["invalid_ohlc"] == 0
    assert st["status"] == "PASS"


def test_calibration_status_n_zero_is_insufficient():
    empty = calibration_status_payload(buckets=None)
    assert empty["status"] == "INSUFFICIENT DATA"
    assert empty["n"] == 0
    assert empty["ece"] is None
    zero = calibration_status_payload(buckets={"status": "OK", "n": 0, "ece": None, "buckets": []})
    assert zero["status"] == "INSUFFICIENT DATA"
    assert zero["n"] == 0
    assert zero["ece"] is None
    ok = calibration_status_payload(buckets={"status": "OK", "n": 80, "ece": 0.04, "buckets": [1]})
    assert ok["status"] == "OK"
    assert ok["n"] == 80
