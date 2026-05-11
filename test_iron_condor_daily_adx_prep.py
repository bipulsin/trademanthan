"""
Self-test for Iron Condor daily ADX candle prep (no pytest required).
Run: PYTHONPATH=. python3 test_iron_condor_daily_adx_prep.py
"""
from __future__ import annotations

import os
from datetime import datetime
from unittest import mock

import pytz

IST = pytz.timezone("Asia/Kolkata")


def _run() -> None:
    os.environ.setdefault("IRON_CONDOR_UNIVERSE_ADX_EXCLUDE_INCOMPLETE_DAILY", "1")
    os.environ.setdefault("IRON_CONDOR_UNIVERSE_ADX_NSE_REGULAR_CLOSE_HHMM", "1530")
    os.environ.setdefault("IRON_CONDOR_UNIVERSE_ADX_DAILY_SETTLE_BUFFER_MIN", "5")

    from backend.services import iron_condor_service as _ics_mod
    from backend.services.iron_condor_service import (
        _ic_daily_hlc_for_adx,
        _ic_nse_cash_daily_bar_settled_for_adx_ist,
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

    os.environ["IRON_CONDOR_UNIVERSE_ADX_EXCLUDE_INCOMPLETE_DAILY"] = "1"

    # (a)(b) Session-weekday behavior: force "trading day" so local DB holiday rows cannot flip the test.
    patch_trading = mock.patch.object(
        _ics_mod.mh_ic,
        "should_skip_scheduled_market_jobs_ist",
        return_value=False,
    )
    base_tue = [
        {"timestamp": "2026-05-25", "high": 1, "low": 1, "close": 1},
        {"timestamp": "2026-05-26", "high": 2, "low": 1, "close": 1.5},
    ]
    with patch_trading:
        # (a) 10:00 IST → strip today's developing bar
        tue_am = IST.localize(datetime(2026, 5, 26, 10, 0, 0))
        assert not _ic_nse_cash_daily_bar_settled_for_adx_ist(tue_am), tue_am
        stripped_morning = _ic_prepare_daily_spot_candles_for_adx(base_tue, now_ist=tue_am)
        assert len(stripped_morning) == 1 and float(stripped_morning[-1]["close"]) == 1.0, stripped_morning

        # (b) 16:00 IST → keep completed today
        tue_pm = IST.localize(datetime(2026, 5, 26, 16, 0, 0))
        assert _ic_nse_cash_daily_bar_settled_for_adx_ist(tue_pm), tue_pm
        kept_evening = _ic_prepare_daily_spot_candles_for_adx(base_tue, now_ist=tue_pm)
        assert len(kept_evening) == 2 and float(kept_evening[-1]["close"]) == 1.5, kept_evening

        # Just before vs at settle cutoff (15:30 + 5 min buffer)
        tue_1534 = IST.localize(datetime(2026, 5, 26, 15, 34, 0))
        assert not _ic_nse_cash_daily_bar_settled_for_adx_ist(tue_1534)
        assert len(_ic_prepare_daily_spot_candles_for_adx(base_tue, now_ist=tue_1534)) == 1
        tue_1535 = IST.localize(datetime(2026, 5, 26, 15, 35, 0))
        assert _ic_nse_cash_daily_bar_settled_for_adx_ist(tue_1535)
        assert len(_ic_prepare_daily_spot_candles_for_adx(base_tue, now_ist=tue_1535)) == 2

    # (c) Weekend — do not apply intraday strip; keep last row even if dated "today" (Saturday)
    sat = IST.localize(datetime(2026, 5, 23, 16, 0, 0))
    assert sat.weekday() == 5
    weekend_rows = [
        {"timestamp": "2026-05-22", "high": 1, "low": 1, "close": 1},
        {"timestamp": "2026-05-23", "high": 9, "low": 8, "close": 8.5},
    ]
    wk = _ic_prepare_daily_spot_candles_for_adx(weekend_rows, now_ist=sat)
    assert len(wk) == 2 and float(wk[-1]["close"]) == 8.5, wk


if __name__ == "__main__":
    _run()
    print("test_iron_condor_daily_adx_prep: OK")
