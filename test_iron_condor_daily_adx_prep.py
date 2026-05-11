"""
Self-test for Iron Condor daily ADX candle prep (no pytest required).
Run: PYTHONPATH=. python3 test_iron_condor_daily_adx_prep.py
"""
from __future__ import annotations

import os
from datetime import datetime

import pytz

IST = pytz.timezone("Asia/Kolkata")


def _run() -> None:
    os.environ.setdefault("IRON_CONDOR_UNIVERSE_ADX_EXCLUDE_INCOMPLETE_DAILY", "1")

    from backend.services.iron_condor_service import (
        _ic_daily_hlc_for_adx,
        _ic_prepare_daily_spot_candles_for_adx,
    )

    raw = [
        {"timestamp": "2026-05-17", "high": 10, "low": 9, "close": 9.5},
        {"timestamp": "2026-05-18T15:31:00+05:30", "high": 11, "low": 9.8, "close": 10.2},  # same day overwrite
        {"timestamp": "2026-05-18", "high": 10.5, "low": 9.9, "close": 10.0},  # earlier same day drops
        {"timestamp": "2026-05-16", "high": 10, "low": 9, "close": 9.4},  # out of chronological input order
    ]
    now = IST.localize(datetime(2026, 5, 19, 12, 0, 0))
    ordered = _ic_prepare_daily_spot_candles_for_adx(raw, now_ist=now)
    hs, ls, cs = _ic_daily_hlc_for_adx(ordered)
    assert [round(x, 2) for x in cs] == [9.4, 9.5, 10.2], (ordered, hs, ls, cs)
    assert len(ordered) == 3

    os.environ["IRON_CONDOR_UNIVERSE_ADX_EXCLUDE_INCOMPLETE_DAILY"] = "1"
    with_partial = ordered + [{"timestamp": "2026-05-19T10:00:00+05:30", "high": 99, "low": 8, "close": 50}]
    clipped = _ic_prepare_daily_spot_candles_for_adx(with_partial, now_ist=now)
    assert len(clipped) == 3 and float(clipped[-1]["close"]) == 10.2, clipped

    raw2 = [{"timestamp": "2026-05-17", "high": 1, "low": 1, "close": 1}] + ordered
    now2 = IST.localize(datetime(2026, 5, 19, 12, 0, 0))
    after = _ic_prepare_daily_spot_candles_for_adx(raw2, now_ist=now2)
    assert len(after) == 3

    raw3 = list(clipped)
    now_evening = IST.localize(datetime(2026, 5, 19, 16, 0, 0))
    keep_today = _ic_prepare_daily_spot_candles_for_adx(
        raw3 + [{"timestamp": "2026-05-19", "high": 12, "low": 10, "close": 11}], now_ist=now_evening
    )
    hs3, ls3, cs3 = _ic_daily_hlc_for_adx(keep_today)
    assert cs3[-1] == 11.0, cs3

    os.environ["IRON_CONDOR_UNIVERSE_ADX_EXCLUDE_INCOMPLETE_DAILY"] = "0"
    raw4 = list(with_partial)
    now_midday = IST.localize(datetime(2026, 5, 19, 11, 0, 0))
    keep_partial = _ic_prepare_daily_spot_candles_for_adx(raw4, now_ist=now_midday)
    assert len(keep_partial) == len(raw4)


if __name__ == "__main__":
    _run()
    print("test_iron_condor_daily_adx_prep: OK")
