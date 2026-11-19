"""Unit tests for Open Low High screener rules — no network."""
from __future__ import annotations

from backend.services.open_low_high.signals import (
    confirm_phase_b,
    evaluate_nifty_cue,
    evaluate_phase_a,
    evaluate_phase_b,
    freeze_tags_immutable,
    next_pivot_targets,
    open_extreme_badge,
    passes_second_candle_wick_filter,
    traditional_pivot_levels,
)


def test_phase_a_bullish_close_above_pdh_vwap_green():
    r = evaluate_phase_a(
        first_open=100.0,
        first_high=103.0,
        first_low=99.5,
        first_close=102.5,
        vwap_0915=101.0,
        prev_high=101.0,
        prev_low=95.0,
        prev_close=100.5,
    )
    assert r is not None
    assert r.side == "BULLISH"
    assert r.first_close == 102.5


def test_phase_a_bullish_rejects_red_candle():
    r = evaluate_phase_a(
        first_open=103.0,
        first_high=104.0,
        first_low=101.0,
        first_close=102.5,  # above PDH + VWAP but red
        vwap_0915=101.0,
        prev_high=101.0,
        prev_low=95.0,
    )
    assert r is None


def test_phase_a_bullish_accepts_doji():
    r = evaluate_phase_a(
        first_open=102.5,
        first_high=103.0,
        first_low=101.5,
        first_close=102.5,  # doji above PDH + VWAP
        vwap_0915=101.0,
        prev_high=101.0,
        prev_low=95.0,
    )
    assert r is not None
    assert r.side == "BULLISH"


def test_phase_a_bullish_rejects_close_not_above_pdh():
    # Wick above PDH but close at/below — live screener uses close, not wick.
    r = evaluate_phase_a(
        first_open=99.0,
        first_high=101.5,
        first_low=98.5,
        first_close=100.5,
        vwap_0915=99.0,
        prev_high=101.0,
        prev_low=95.0,
    )
    assert r is None


def test_phase_a_bearish_close_below_pdl_vwap_red():
    r = evaluate_phase_a(
        first_open=100.0,
        first_high=100.5,
        first_low=96.0,
        first_close=97.0,
        vwap_0915=98.5,
        prev_high=105.0,
        prev_low=98.0,
        prev_close=99.0,
    )
    assert r is not None
    assert r.side == "BEARISH"


def test_phase_a_bearish_accepts_doji():
    # INDIGO-style: close == open below PDL and below VWAP.
    r = evaluate_phase_a(
        first_open=5600.0,
        first_high=5614.2,
        first_low=5587.7,
        first_close=5600.0,
        vwap_0915=5600.633,
        prev_high=5672.2,
        prev_low=5608.7,
    )
    assert r is not None
    assert r.side == "BEARISH"


def test_phase_a_bearish_rejects_green():
    r = evaluate_phase_a(
        first_open=96.0,
        first_high=98.0,
        first_low=95.0,
        first_close=97.0,  # below PDL + VWAP but green
        vwap_0915=98.5,
        prev_high=105.0,
        prev_low=98.0,
    )
    assert r is None


def test_phase_b_bullish_keep_when_second_close_ge_first_close():
    assert (
        confirm_phase_b(
            "BULLISH",
            first_close=102.0,
            second_close=102.5,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BULLISH",
            first_close=102.0,
            second_close=102.0,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BULLISH",
            first_close=102.0,
            second_close=101.5,
        )
        is False
    )


def test_phase_b_bearish_keep_when_second_close_le_first_close():
    assert (
        confirm_phase_b(
            "BEARISH",
            first_close=97.0,
            second_close=96.5,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BEARISH",
            first_close=97.0,
            second_close=97.0,
        )
        is True
    )
    assert (
        confirm_phase_b(
            "BEARISH",
            first_close=97.0,
            second_close=97.2,
        )
        is False
    )


def test_evaluate_phase_b_fail_reasons():
    missing = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=None,
        second_high=None,
        second_low=None,
        second_close=None,
    )
    assert missing.ok is False
    assert missing.fail_reason == "missing 9:20 candle"

    # Red 9:20 + close below 9:15 → reverse-direction wording
    reverse = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=101.0,
        second_low=99.0,
        second_close=99.5,
    )
    assert reverse.ok is False
    assert reverse.fail_reason == "9:20 candle is in reverse direction of 9:15 candle"

    # Green-ish 9:20 but still closes below 9:15
    close_fail = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=99.0,
        second_high=100.5,
        second_low=98.5,
        second_close=99.5,
    )
    assert close_fail.ok is False
    assert close_fail.fail_reason == "9:20 candle closes below 9:15 candle"

    bear_close = evaluate_phase_b(
        "BEARISH",
        first_close=100.0,
        second_open=100.5,
        second_high=101.0,
        second_low=100.0,
        second_close=100.2,
    )
    assert bear_close.ok is False
    assert bear_close.fail_reason == "9:20 candle closes above 9:15 candle"

    wick_fail = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=114.0,
        second_low=99.0,
        second_close=110.0,
    )
    assert wick_fail.ok is False
    assert wick_fail.wait_list is True  # 40% is in Wait band (35–90%)
    assert wick_fail.fail_reason == (
        "9:20 candle has wick longer than 35% of candle body (40%)"
    )
    assert wick_fail.wick_body_ratio == 0.4

    # POLYCAB-like ~71% → Wait List (body=10, lower wick=7.1)
    wait_71 = evaluate_phase_b(
        "BEARISH",
        first_close=100.0,
        second_open=100.0,
        second_high=100.5,
        second_low=82.9,
        second_close=90.0,
    )
    assert wait_71.ok is False
    assert wait_71.wait_list is True
    assert abs(float(wait_71.wick_body_ratio) - 0.71) < 1e-9

    hard_wick = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=120.0,  # body=10 → upper=10 → 100% > 90%
        second_low=99.0,
        second_close=110.0,
    )
    assert hard_wick.ok is False
    assert hard_wick.wait_list is False
    assert hard_wick.wick_body_ratio == 1.0

    ok = evaluate_phase_b(
        "BULLISH",
        first_close=100.0,
        second_open=100.0,
        second_high=113.5,
        second_low=99.0,
        second_close=110.0,
    )
    assert ok.ok is True
    assert ok.fail_reason is None
    assert ok.wait_list is False
    assert ok.wick_body_ratio == 0.35


def test_open_extreme_badge_ol_oh():
    assert (
        open_extreme_badge("BULLISH", first_open=100.0, first_high=105.0, first_low=100.0)
        == "O=L"
    )
    assert (
        open_extreme_badge("BULLISH", first_open=100.0, first_high=105.0, first_low=99.0)
        is None
    )
    assert (
        open_extreme_badge("BEARISH", first_open=105.0, first_high=105.0, first_low=100.0)
        == "O=H"
    )
    assert (
        open_extreme_badge("BEARISH", first_open=104.0, first_high=105.0, first_low=100.0)
        is None
    )


def test_nifty_cue_gap_and_bias():
    # Bias = session close vs PDC (prefer 09:20); gap = 09:15 open vs PDC
    bull = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=101.0,
        first_close=101.5,
        second_close=102.0,
    )
    assert bull["nifty_gap"] == "up"
    assert bull["nifty_bias"] == "bullish"
    assert bull["nifty_show_on"] == "BULLISH"

    bear = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=99.0,
        first_close=99.5,
        second_close=98.5,
    )
    assert bear["nifty_gap"] == "down"
    assert bear["nifty_bias"] == "bearish"
    assert bear["nifty_show_on"] == "BEARISH"

    # Gap up but 09:20 close below PDC → bearish bias, no header badge
    mixed = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=101.0,
        first_close=102.0,
        second_close=99.0,
    )
    assert mixed["nifty_gap"] == "up"
    assert mixed["nifty_bias"] == "bearish"
    assert mixed["nifty_show_on"] is None

    # Before 09:20: bias from 09:15 close vs PDC
    early = evaluate_nifty_cue(
        prev_close=100.0,
        first_open=101.0,
        first_close=101.5,
        second_close=None,
    )
    assert early["nifty_bias"] == "bullish"
    assert early["nifty_show_on"] == "BULLISH"


def test_wick_filter_bullish_upper_wick_ratio():
    # body=10, upper wick=3.5 → 0.35 pass; upper wick=4 → fail
    assert (
        passes_second_candle_wick_filter(
            "BULLISH",
            second_open=100.0,
            second_high=113.5,
            second_low=99.0,
            second_close=110.0,
        )
        is True
    )
    assert (
        passes_second_candle_wick_filter(
            "BULLISH",
            second_open=100.0,
            second_high=114.0,
            second_low=99.0,
            second_close=110.0,
        )
        is False
    )


def test_wick_filter_bearish_lower_wick_ratio():
    # body=17, lower wick=4.4 → ~0.26 (INDIGO REST-like) pass at 0.35; wick=6 → fail
    assert (
        passes_second_candle_wick_filter(
            "BEARISH",
            second_open=5606.9,
            second_high=5606.9,
            second_low=5585.5,
            second_close=5589.9,
        )
        is True
    )
    assert (
        passes_second_candle_wick_filter(
            "BEARISH",
            second_open=5606.9,
            second_high=5606.9,
            second_low=5583.9,
            second_close=5589.9,
        )
        is False
    )


def test_wick_filter_rejects_zero_body():
    assert (
        passes_second_candle_wick_filter(
            "BEARISH",
            second_open=100.0,
            second_high=101.0,
            second_low=99.0,
            second_close=100.0,
        )
        is False
    )
    assert (
        passes_second_candle_wick_filter(
            "BULLISH",
            second_open=100.0,
            second_high=101.0,
            second_low=99.0,
            second_close=100.0,
        )
        is False
    )


def test_freeze_tags_immutable_snapshots_display():
    tags = freeze_tags_immutable(
        {"sector_badge": "S1", "sector_star": True, "nifty_sector_label": "Nifty IT"},
        frozen=True,
    )
    assert tags["frozen"] is True
    assert tags["sector_badge"] == "S1"
    assert tags["sector_badge_display"] == "S1*"
    # Mutating a fresh live dict must not affect the frozen snapshot copy.
    live = {"sector_badge": "W2", "sector_star": False}
    frozen = freeze_tags_immutable(live, frozen=True)
    live["sector_badge"] = "S3"
    assert frozen["sector_badge"] == "W2"
    assert frozen["sector_badge_display"] == "W2"


def test_traditional_pivot_levels_basic():
    # H=110, L=100, C=105 → PP=105; R1=110; S1=100
    lv = traditional_pivot_levels(110.0, 100.0, 105.0)
    assert abs(lv["pp"] - 105.0) < 1e-9
    assert abs(lv["r1"] - 110.0) < 1e-9
    assert abs(lv["s1"] - 100.0) < 1e-9
    assert abs(lv["r2"] - 115.0) < 1e-9
    assert abs(lv["s2"] - 95.0) < 1e-9


def test_next_pivot_targets_bullish_and_bearish():
    # reference between S1 and PP → bullish next = PP, R1; bearish next = S1, S2
    t1, t2 = next_pivot_targets(
        "BULLISH",
        prev_high=110.0,
        prev_low=100.0,
        prev_close=105.0,
        reference=102.0,
    )
    assert t1 == 105.0
    assert t2 == 110.0
    t1b, t2b = next_pivot_targets(
        "BEARISH",
        prev_high=110.0,
        prev_low=100.0,
        prev_close=105.0,
        reference=102.0,
    )
    assert t1b == 100.0
    assert t2b == 95.0


def test_window_status_boundaries():
    from datetime import datetime

    import pytz

    from backend.services.open_low_high.phases import phase_a_window_status, phase_b_window_status
    from backend.services.open_low_high.screener import window_status

    IST = pytz.timezone("Asia/Kolkata")
    # Phase B window drives window_status (Phase A greys can show while waiting).
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 20, 30))) == "waiting"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 25, 0))) == "waiting"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 25, 3))) == "open"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 26, 0))) == "open"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 29, 0))) == "open"
    assert window_status(IST.localize(datetime(2026, 11, 17, 9, 29, 1))) == "past"

    assert phase_a_window_status(IST.localize(datetime(2026, 11, 17, 9, 20, 9))) == "waiting"
    assert phase_a_window_status(IST.localize(datetime(2026, 11, 17, 9, 20, 10))) == "open"
    assert phase_a_window_status(IST.localize(datetime(2026, 11, 17, 9, 23, 0))) == "open"
    assert phase_a_window_status(IST.localize(datetime(2026, 11, 17, 9, 23, 1))) == "past"

    assert phase_b_window_status(IST.localize(datetime(2026, 11, 17, 9, 25, 3))) == "open"
    assert phase_b_window_status(IST.localize(datetime(2026, 11, 17, 9, 29, 0))) == "open"
    assert phase_b_window_status(IST.localize(datetime(2026, 11, 17, 9, 29, 1))) == "past"


def test_data_quality_incomplete_defers_empty_freeze():
    from backend.services.open_low_high.screener import data_quality_incomplete

    # 2026-11-19 style: Phase A=0 because most equities lacked REST bars.
    incomplete, details = data_quality_incomplete(
        phase_a_count=0,
        scanned=145,
        miss_stats={"no_bars": 120, "no_0915": 10, "no_prev": 5, "no_equity_key": 0},
        nifty_ready=False,
    )
    assert incomplete is True
    assert details["missing_rate"] >= 0.35
    assert details["nifty_ready"] is False

    # Legitimate flat day: empty Phase A but equity/Nifty data present.
    ok_empty, details_ok = data_quality_incomplete(
        phase_a_count=0,
        scanned=145,
        miss_stats={"no_bars": 2, "no_0915": 1, "no_prev": 0, "no_equity_key": 0},
        nifty_ready=True,
    )
    assert ok_empty is False
    assert details_ok["missing_rate"] < 0.35

    # Any Phase A qualifiers → never treat as incomplete deferral.
    has_a, _ = data_quality_incomplete(
        phase_a_count=9,
        scanned=145,
        miss_stats={"no_bars": 100, "no_0915": 0, "no_prev": 0, "no_equity_key": 0},
        nifty_ready=False,
    )
    assert has_a is False


def test_nifty_data_ready_requires_bars_and_prev_close():
    from backend.services.open_low_high.screener import _nifty_data_ready

    assert _nifty_data_ready({}) is False
    assert (
        _nifty_data_ready(
            {
                "nifty_prev_close": 24000.0,
                "nifty_0915_open": 24010.0,
                "nifty_0915_close": 24020.0,
            },
            require_0920=True,
        )
        is False
    )
    assert (
        _nifty_data_ready(
            {
                "nifty_prev_close": 24000.0,
                "nifty_0915_open": 24010.0,
                "nifty_0915_close": 24020.0,
                "nifty_0920_close": 24030.0,
            },
            require_0920=True,
        )
        is True
    )


def test_resolve_equity_key_prefers_universe_nse_eq():
    from backend.services.open_low_high.screener import _resolve_equity_key

    class _Ux:
        def get_instrument_key(self, symbol):
            return "NSE_EQ|FALLBACK"

    row = {"underlying_key": "NSE_EQ|INE123A01016"}
    assert _resolve_equity_key(_Ux(), row, "FOO") == "NSE_EQ|INE123A01016"


def test_resolve_equity_key_falls_back_to_upstox():
    from backend.services.open_low_high.screener import _resolve_equity_key

    class _Ux:
        def get_instrument_key(self, symbol):
            assert symbol == "INDIGO"
            return "NSE_EQ:INE646L01027"

    assert _resolve_equity_key(_Ux(), {"underlying_key": ""}, "INDIGO") == "NSE_EQ|INE646L01027"


def test_row_to_dict_does_not_copy_equity_open_as_sl():
    from backend.services.open_low_high.persistence import _row_to_dict

    legacy = _row_to_dict(
        {"first_open": 100.0, "sl": None, "active": True, "phase_b_ok": True}
    )
    assert legacy["sl"] == 100.0

    equity = _row_to_dict(
        {
            "first_open": 100.0,
            "sl": None,
            "equity_instrument_key": "NSE_EQ|INE123A01016",
            "active": True,
            "phase_b_ok": True,
        }
    )
    assert equity["sl"] is None
    assert equity["data_source_logic"] == "equity"
    assert equity["data_source_levels"] == "future"


def test_ohl_log_filename_and_cleanup(tmp_path, monkeypatch):
    from datetime import date, datetime

    import pytz

    from backend.services.open_low_high import ohl_log as mod

    IST = pytz.timezone("Asia/Kolkata")
    monkeypatch.setattr(mod, "_LOG_DIR_CANDIDATES", (tmp_path,))
    assert mod.ohl_log_filename(date(2026, 11, 19)) == "OHL_Log_19Nov2026"
    path = mod.ohl_log_path(date(2026, 11, 19))
    mod.ohl_log("success", action="phase_a_ws", symbol="RELIANCE", source="ws", detail="got 09:15")
    assert path.exists()
    text = path.read_text(encoding="utf-8")
    assert "OHL" in text and "success" in text and "RELIANCE" in text

    # Old file should be deleted; recent kept.
    old = tmp_path / "OHL_Log_01Nov2026"
    old.write_text("old\n", encoding="utf-8")
    recent = tmp_path / "OHL_Log_18Nov2026"
    recent.write_text("recent\n", encoding="utf-8")
    out = mod.cleanup_ohl_logs(keep_days=3, now=IST.localize(datetime(2026, 11, 19, 12, 0)))
    assert "OHL_Log_01Nov2026" in out["deleted"]
    assert recent.exists()
    assert not old.exists()


def test_pdc_column_name():
    from backend.services.open_low_high.pdc import PDC_COLUMN, PDC_DATE_COLUMN

    assert PDC_COLUMN == "equity_prev_close"
    assert PDC_DATE_COLUMN == "equity_prev_close_date"


def test_pdc_resolve_asof_eod_vs_morning():
    """EOD stores today's close; 09:00 uses previous trading day."""
    from datetime import date

    from backend.services.open_low_high.pdc import _resolve_asof

    d = date(2026, 11, 19)
    assert _resolve_asof(d, eod=True) == date(2026, 11, 19)
    assert _resolve_asof(d, eod=False) == date(2026, 11, 18)
    # Next morning (20-Nov) → prior completed session 19-Nov
    assert _resolve_asof(date(2026, 11, 20), eod=False) == date(2026, 11, 19)


def test_pdc_close_on_asof_no_older_hist_mislabel(monkeypatch):
    """Missing asof hist bar must not return an older session close."""
    from datetime import date, datetime
    from types import SimpleNamespace

    import pytz

    from backend.services.open_high_low.bars import Bar
    from backend.services.open_low_high import pdc as pdc_mod

    IST = pytz.timezone("Asia/Kolkata")
    asof = date(2026, 11, 19)
    older = Bar(
        ts=IST.localize(datetime(2026, 11, 18, 15, 30)),
        open=1,
        high=1,
        low=1,
        close=1363.0,
        volume=1,
        contract="NSE_EQ|X",
    )

    class FakeUx:
        def get_historical_candles_by_instrument_key(self, *a, **k):
            return [{"raw": True}]

        def get_ohlc_data(self, ik):
            return {"open": 1, "high": 1, "low": 1, "close": 1375.6, "last_price": 1375.6}

    monkeypatch.setattr(
        "backend.services.open_low_high.screener._bars_from_rows",
        lambda rows, ik: [older],
    )
    monkeypatch.setattr(
        "backend.services.open_low_high.screener._hist_range_end",
        lambda d: d,
    )
    monkeypatch.setattr(
        pdc_mod,
        "_now",
        lambda: IST.localize(datetime(2026, 11, 19, 18, 0)),
    )

    # EOD / same-day: quote fallback
    pc = pdc_mod._close_on_asof(FakeUx(), "NSE_EQ|X", asof, asof, eod=True)
    assert pc == 1375.6

    # Morning of 20-Nov looking for 19-Nov: missing hist → None (no older mislabel)
    monkeypatch.setattr(
        pdc_mod,
        "_now",
        lambda: IST.localize(datetime(2026, 11, 20, 9, 0)),
    )
    pc2 = pdc_mod._close_on_asof(
        FakeUx(), "NSE_EQ|X", date(2026, 11, 19), date(2026, 11, 20), eod=False
    )
    assert pc2 is None


def test_ws_lock_window_and_merge():
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import ws_feed as wf

    IST = pytz.timezone("Asia/Kolkata")
    assert wf.in_ws_lock_window(IST.localize(datetime(2026, 11, 19, 9, 14, 59))) is False
    assert wf.in_ws_lock_window(IST.localize(datetime(2026, 11, 19, 9, 15, 0))) is True
    assert wf.in_ws_lock_window(IST.localize(datetime(2026, 11, 19, 9, 24, 59))) is True
    assert wf.in_ws_lock_window(IST.localize(datetime(2026, 11, 19, 9, 25, 0))) is False

    wf.release_ws_lock()
    with wf._lock:
        wf._locked_eq_index = {"NSE_EQ|A", "NSE_INDEX|Nifty 50"}
        wf._lock_engaged = True
    merged = wf._provider_keys_with_lock(["NSE_FO|1"])
    assert "NSE_EQ|A" in merged
    assert "NSE_INDEX|Nifty 50" in merged
    assert "NSE_FO|1" in merged
    wf.release_ws_lock()


# ---- Harden suite 5.1–5.6 (off-market unit tests) ----


def test_51_ws_down_both_tries_rest_completes(monkeypatch):
    """5.1 WS down both tries → REST completes."""
    from datetime import date, datetime

    import pytz

    from backend.services.open_high_low.bars import Bar
    from backend.services.open_low_high import screener as scr

    IST = pytz.timezone("Asia/Kolkata")
    session = date(2026, 11, 19)
    rest_bar = Bar(
        ts=IST.localize(datetime(2026, 11, 19, 9, 15)),
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=1,
        contract="NSE_EQ|X",
    )

    monkeypatch.setattr(scr, "_ws_try_once", lambda *a, **k: ([], "none"))
    monkeypatch.setattr(scr, "_fetch_rest_5m", lambda *a, **k: [rest_bar])
    monkeypatch.setattr(scr, "_record_attempt", lambda *a, **k: None)

    bars, src = scr._session_5m_bars_try(
        object(), "NSE_EQ|X", session, symbol="FOO", action="t51", prefer="ws"
    )
    assert src == "rest"
    assert len(bars) == 1
    assert bars[0].close == 100.5


def test_52_rest_empty_all_degraded_no_freeze():
    """5.2 REST empty all → DEGRADED + no locked freeze (gate)."""
    from backend.services.open_low_high.screener import freeze_gate

    ok, reason = freeze_gate(coverage=0.0, nifty_present=False)
    assert ok is False
    assert "nifty" in reason or "coverage" in reason


def test_53_nifty_missing_equities_ok_degraded():
    """5.3 Nifty missing, equities OK → DEGRADED (19-Nov scenario)."""
    from backend.services.open_low_high.screener import freeze_gate

    ok, reason = freeze_gate(coverage=0.95, nifty_present=False)
    assert ok is False
    assert reason == "nifty_missing"


def test_54_late_ws_first_tick_untrusted():
    """5.4 Late WS first tick >5s → UNTRUSTED."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high.screener import _ws_bucket_trusted

    IST = pytz.timezone("Asia/Kolkata")
    bucket = IST.localize(datetime(2026, 11, 19, 9, 15, 0))
    late = IST.localize(datetime(2026, 11, 19, 9, 15, 6))
    early = IST.localize(datetime(2026, 11, 19, 9, 15, 2))
    assert _ws_bucket_trusted(bucket, late) is False
    assert _ws_bucket_trusted(bucket, early) is True


def test_55_ws_keepalive_outside_window_skipped():
    """5.5 Socket drop/reconnect companion skips outside 09:05–09:29."""
    from datetime import datetime
    from unittest.mock import patch

    import pytz

    from backend.services.open_low_high import ws_feed as wf

    IST = pytz.timezone("Asia/Kolkata")
    with patch.object(wf, "_now", return_value=IST.localize(datetime(2026, 11, 19, 10, 0))):
        out = wf.resubscribe_olh_ws_keepalive()
    assert out.get("status") == "skipped"


def test_56_null_pdc_crosscheck_flags_still_null(monkeypatch):
    """5.6 Null PDC at 09:00 verify → still_null reported (exclude + alert path)."""
    from datetime import date

    from backend.services.open_low_high import pdc as pdc_mod

    monkeypatch.setattr(pdc_mod, "ensure_pdc_columns", lambda: None)
    monkeypatch.setattr(
        pdc_mod,
        "fill_equity_prev_close",
        lambda **k: {
            "ok": True,
            "status": "filled",
            "ok_n": 1,
            "fail_n": 1,
            "nifty_prev_close": None,
        },
    )
    monkeypatch.setattr(
        pdc_mod,
        "_load_equity_rows",
        lambda only_null=False, expected_asof=None: (
            [("ZZNULL", "NSE_EQ|INE000")] if only_null else []
        ),
    )
    monkeypatch.setattr(pdc_mod, "get_equity_prev_close", lambda symbol: None)
    alerts = []
    monkeypatch.setattr(
        "backend.services.open_low_high.alerts.alert_degraded",
        lambda *a, **k: alerts.append((a, k)) or True,
        raising=False,
    )

    # Patch import inside function by stubbing alerts module path used.
    import backend.services.open_low_high.alerts as al

    monkeypatch.setattr(al, "alert_degraded", lambda *a, **k: alerts.append(k) or True)

    out = pdc_mod.crosscheck_pdc_at_0900(session=date(2026, 11, 19))
    assert out.get("still_null_n") == 1
    assert "ZZNULL" in (out.get("still_null_symbols") or [])
    assert out.get("expected_asof") == "2026-11-18"
    assert alerts, "expected alert on remaining nulls"


def test_freeze_gate_ok_when_coverage_and_nifty():
    from backend.services.open_low_high.screener import freeze_gate

    ok, reason = freeze_gate(coverage=0.91, nifty_present=True)
    assert ok is True
    assert reason == "ok"


def test_deploy_guard_window_constants():
    from backend.services.open_low_high.config import DEPLOY_GUARD_END, DEPLOY_GUARD_START

    assert DEPLOY_GUARD_START.hour == 8 and DEPLOY_GUARD_START.minute == 45
    assert DEPLOY_GUARD_END.hour == 9 and DEPLOY_GUARD_END.minute == 35


# --- Hardening scenarios 5.1–5.6 (off-market unit tests) ---


def test_5_1_ws_down_both_tries_falls_back_to_rest(monkeypatch):
    """5.1 WS try1+try2 empty → REST completes."""
    from datetime import datetime

    import pytz

    from backend.services.open_high_low.bars import Bar
    from backend.services.open_low_high import screener as sc

    IST = pytz.timezone("Asia/Kolkata")
    session = datetime(2026, 11, 19, tzinfo=IST).date()
    rest_bar = Bar(
        ts=IST.localize(datetime(2026, 11, 19, 9, 15)),
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=1,
        contract="NSE_EQ|X",
    )

    monkeypatch.setattr(sc, "_ws_try_once", lambda *a, **k: ([], "none"))
    monkeypatch.setattr(sc, "_fetch_rest_5m", lambda *a, **k: [rest_bar])
    monkeypatch.setattr(sc, "_record_attempt", lambda *a, **k: None)

    bars, src = sc._session_5m_bars_try(
        object(), "NSE_EQ|X", session, symbol="FOO", prefer="ws"
    )
    assert src == "rest"
    assert len(bars) == 1
    assert bars[0].close == 100.5


def test_fetch_rest_5m_today_uses_intraday_only(monkeypatch):
    """Live session must call V3 intraday only — never historical."""
    from datetime import date

    from backend.services.open_low_high import screener as scr

    calls = {"intraday": 0, "historical": 0}

    class FakeUx:
        def _fetch_intraday_candles_v3(self, ik, interval):
            calls["intraday"] += 1
            return []

        def get_historical_candles_by_instrument_key(self, *a, **k):
            calls["historical"] += 1
            return [{"timestamp": "x"}]

    monkeypatch.setattr(scr, "_today", lambda: date(2026, 11, 20))
    monkeypatch.setattr(scr, "_bars_from_rows", lambda rows, ik: [])
    out = scr._fetch_rest_5m(FakeUx(), "NSE_EQ|X", date(2026, 11, 20))
    assert out == []
    assert calls["intraday"] == 1
    assert calls["historical"] == 0


def test_fetch_rest_5m_past_uses_historical(monkeypatch):
    """Previous days use historical (not live intraday-first)."""
    from datetime import date

    from backend.services.open_low_high import screener as scr

    calls = {"intraday": 0, "historical": 0}

    class FakeUx:
        def _fetch_intraday_candles_v3(self, ik, interval):
            calls["intraday"] += 1
            return []

        def get_historical_candles_by_instrument_key(self, *a, **k):
            calls["historical"] += 1
            return []

    monkeypatch.setattr(scr, "_today", lambda: date(2026, 11, 20))
    monkeypatch.setattr(scr, "_bars_from_rows", lambda rows, ik: [])
    monkeypatch.setattr(scr, "_hist_range_end", lambda s: s)
    scr._fetch_rest_5m(FakeUx(), "NSE_EQ|X", date(2026, 11, 18))
    assert calls["historical"] == 1


def test_5_2_freeze_gate_empty_coverage_is_degraded():
    """5.2 REST empty / low coverage → DEGRADED, no locked freeze."""
    from backend.services.open_low_high.screener import freeze_gate

    ok, reason = freeze_gate(coverage=0.0, nifty_present=True)
    assert ok is False
    assert "coverage" in reason

    ok2, reason2 = freeze_gate(coverage=0.95, nifty_present=True)
    assert ok2 is True
    assert reason2 == "ok"


def test_5_3_nifty_missing_equities_ok_degraded():
    """5.3 Nifty missing, equities OK → DEGRADED (19-Nov scenario)."""
    from backend.services.open_low_high.screener import freeze_gate

    ok, reason = freeze_gate(coverage=0.95, nifty_present=False)
    assert ok is False
    assert reason == "nifty_missing"


def test_5_4_late_ws_first_tick_untrusted():
    """5.4 Late WS first tick >5s → UNTRUSTED."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high.screener import _ws_bucket_trusted

    IST = pytz.timezone("Asia/Kolkata")
    bucket = IST.localize(datetime(2026, 11, 19, 9, 15, 0))
    late = IST.localize(datetime(2026, 11, 19, 9, 15, 6))
    early = IST.localize(datetime(2026, 11, 19, 9, 15, 2))
    assert _ws_bucket_trusted(bucket, late) is False
    assert _ws_bucket_trusted(bucket, early) is True


def test_5_5_ws_keepalive_outside_window_skipped():
    """5.5 Socket drop/reconnect helper skips outside 09:05–09:29."""
    from datetime import datetime
    from unittest.mock import patch

    import pytz

    from backend.services.open_low_high import ws_feed as wf

    IST = pytz.timezone("Asia/Kolkata")
    with patch.object(wf, "_now", return_value=IST.localize(datetime(2026, 11, 19, 10, 0))):
        out = wf.resubscribe_olh_ws_keepalive()
    assert out.get("status") == "skipped"


def test_5_6_pdc_null_crosscheck_alerts(monkeypatch):
    """5.6 Null PDC at 09:00 verify → repair path + alert (no silent null)."""
    from backend.services.open_low_high import pdc as pdc_mod

    alerts = []

    monkeypatch.setattr(
        pdc_mod,
        "fill_equity_prev_close",
        lambda **kw: {
            "ok": True,
            "fail_n": 2,
            "nifty_prev_close": None,
            "ok_n": 0,
        },
    )
    monkeypatch.setattr(
        pdc_mod,
        "_load_equity_rows",
        lambda only_null=True, expected_asof=None: [("AAA", "NSE_EQ|1"), ("BBB", "NSE_EQ|2")],
    )
    monkeypatch.setattr(pdc_mod, "get_equity_prev_close", lambda symbol: None)
    monkeypatch.setattr(pdc_mod, "ensure_pdc_columns", lambda: None)
    monkeypatch.setattr(pdc_mod, "ohl_log", lambda *a, **k: None)

    def _fake_alert(reason, *, trade_date, **kw):
        alerts.append((reason, trade_date, kw))
        return True

    import backend.services.open_low_high.alerts as alerts_mod

    monkeypatch.setattr(alerts_mod, "alert_degraded", _fake_alert)
    # Also patch the import site used inside the function.
    monkeypatch.setitem(
        __import__("sys").modules,
        "backend.services.open_low_high.alerts",
        type("M", (), {"alert_degraded": staticmethod(_fake_alert)})(),
    )

    out = pdc_mod.crosscheck_pdc_at_0900(session=__import__("datetime").date(2026, 11, 19))
    assert out.get("still_null_n") == 2
    assert out.get("nifty_prev_close") is None
    assert len(alerts) >= 1


def test_freeze_gate_and_config_defaults():
    from backend.services.open_low_high import config as cfg
    from backend.services.open_low_high.screener import freeze_gate

    assert cfg.FREEZE_COVERAGE_MIN == 0.90
    assert cfg.WS_FIRST_TICK_TRUST_SEC == 5
    assert cfg.CANARY_EQUITY_SHARE == 0.90
    ok, _ = freeze_gate(coverage=0.90, nifty_present=True)
    assert ok is True
    ok2, _ = freeze_gate(coverage=0.899, nifty_present=True)
    assert ok2 is False


def test_fetch_rest_5m_today_uses_intraday_only(monkeypatch):
    """Today session must call V3 intraday and never historical."""
    from datetime import date

    from backend.services.open_low_high import screener as scr

    calls = {"intra": 0, "hist": 0}

    class FakeUx:
        def _fetch_intraday_candles_v3(self, ik, interval):
            calls["intra"] += 1
            assert interval == "minutes/5"
            return [
                {
                    "timestamp": "2026-11-20T09:15:00+05:30",
                    "open": 1,
                    "high": 2,
                    "low": 1,
                    "close": 1.5,
                    "volume": 10,
                }
            ]

        def get_historical_candles_by_instrument_key(self, *a, **k):
            calls["hist"] += 1
            raise AssertionError("historical must not be called for today")

    monkeypatch.setattr(scr, "_today", lambda: date(2026, 11, 20))
    monkeypatch.setattr(scr, "_bars_from_rows", lambda rows, ik: rows)
    out = scr._fetch_rest_5m(FakeUx(), "NSE_EQ|X", date(2026, 11, 20))
    assert calls["intra"] == 1
    assert calls["hist"] == 0
    assert len(out) == 1


def test_fetch_rest_5m_past_uses_historical(monkeypatch):
    """Previous days use historical endpoint."""
    from datetime import date

    from backend.services.open_low_high import screener as scr

    calls = {"intra": 0, "hist": 0}

    class FakeUx:
        def _fetch_intraday_candles_v3(self, *a, **k):
            calls["intra"] += 1
            return None

        def get_historical_candles_by_instrument_key(self, *a, **k):
            calls["hist"] += 1
            return [
                {
                    "timestamp": "2026-11-18T09:15:00+05:30",
                    "open": 1,
                    "high": 2,
                    "low": 1,
                    "close": 1.5,
                    "volume": 10,
                }
            ]

    monkeypatch.setattr(scr, "_today", lambda: date(2026, 11, 20))
    monkeypatch.setattr(scr, "_bars_from_rows", lambda rows, ik: rows)
    monkeypatch.setattr(scr, "_hist_range_end", lambda s: s)
    out = scr._fetch_rest_5m(FakeUx(), "NSE_EQ|X", date(2026, 11, 18))
    assert calls["hist"] == 1
    assert len(out) == 1


def test_rest_canary_date_gate():
    from datetime import datetime

    import pytz

    from backend.services.open_low_high.rest_canary import (
        CANARY_DATE,
        CANARY_TIMES,
        canary_should_run_today,
    )

    IST = pytz.timezone("Asia/Kolkata")
    assert CANARY_DATE.isoformat() == "2026-11-20"
    assert len(CANARY_TIMES) == 5
    assert canary_should_run_today(IST.localize(datetime(2026, 11, 20, 9, 20, 5)))
    assert not canary_should_run_today(IST.localize(datetime(2026, 11, 19, 9, 20, 5)))


def test_drop_unclosed_bars_fetch_092005_excludes_0920():
    """Fetch at 09:20:05 must NOT return the forming 09:20 bar (bar_end 09:25)."""
    from datetime import datetime, time

    import pytz

    from backend.services.open_high_low.bars import Bar
    from backend.services.open_low_high.screener import _drop_unclosed_bars

    IST = pytz.timezone("Asia/Kolkata")
    fetch = IST.localize(datetime(2026, 11, 20, 9, 20, 5))
    b915 = Bar(
        ts=IST.localize(datetime(2026, 11, 20, 9, 15, 0)),
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=10,
        contract="NSE_EQ|X",
    )
    b920 = Bar(
        ts=IST.localize(datetime(2026, 11, 20, 9, 20, 0)),
        open=100.5,
        high=102,
        low=100,
        close=101,
        volume=12,
        contract="NSE_EQ|X",
    )
    out = _drop_unclosed_bars([b915, b920], fetch_time=fetch)
    assert len(out) == 1
    assert out[0].ts.time() == time(9, 15)
    # Exactly at bar_end 09:20 the 09:15 bar is closed; 09:20 still open until 09:25.
    at_close = IST.localize(datetime(2026, 11, 20, 9, 20, 0))
    out2 = _drop_unclosed_bars([b915, b920], fetch_time=at_close)
    assert len(out2) == 1
    assert out2[0].ts.time() == time(9, 15)
    after_920_close = IST.localize(datetime(2026, 11, 20, 9, 25, 0))
    out3 = _drop_unclosed_bars([b915, b920], fetch_time=after_920_close)
    assert len(out3) == 2


def test_drop_unclosed_bars_upstox_ist_offset_tz_aware():
    """Upstox REST returns +05:30 ISO timestamps — both bar.ts and fetch_time must be tz-aware."""
    from datetime import datetime, time, timedelta

    from backend.services.open_high_low.bars import parse_ts
    from backend.services.open_low_high.screener import (
        _bar_end,
        _bars_from_rows,
        _drop_unclosed_bars,
    )

    # Exact shape Upstox historical/intraday candles use.
    rows = [
        {
            "timestamp": "2026-11-20T09:15:00+05:30",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100.5,
            "volume": 10,
        },
        {
            "timestamp": "2026-11-20T09:20:00+05:30",
            "open": 100.5,
            "high": 102,
            "low": 100,
            "close": 101,
            "volume": 12,
        },
    ]
    bars = _bars_from_rows(rows, "NSE_EQ|X")
    assert len(bars) == 2
    for b in bars:
        assert b.ts.tzinfo is not None, "bar timestamps must be tz-aware"
        # Offset must resolve to IST (+05:30), not naive wall clock.
        assert b.ts.utcoffset() == timedelta(hours=5, minutes=30)

    fetch = parse_ts("2026-11-20T09:20:05+05:30")
    assert fetch is not None and fetch.tzinfo is not None
    assert fetch.utcoffset() == timedelta(hours=5, minutes=30)

    # Closed-bar check uses tz-aware bar_end vs tz-aware fetch_time.
    assert _bar_end(bars[0].ts).tzinfo is not None
    assert _bar_end(bars[1].ts) > fetch  # 09:20 bar still open at 09:20:05

    out = _drop_unclosed_bars(bars, fetch_time=fetch)
    assert len(out) == 1
    assert out[0].ts.time() == time(9, 15)
    assert out[0].ts.tzinfo is not None

    # Naive fetch_time is localized to IST inside _drop_unclosed_bars (still comparable).
    naive_fetch = datetime(2026, 11, 20, 9, 20, 5)
    assert naive_fetch.tzinfo is None
    out_naive = _drop_unclosed_bars(bars, fetch_time=naive_fetch)
    assert len(out_naive) == 1
    assert out_naive[0].ts.time() == time(9, 15)


# --- Startup catch-up after restart (Phase A/B) ---


def test_phase_job_misfire_grace_is_short():
    """Document current APScheduler grace: restart past ~90–120s silently skips."""
    import inspect

    from backend.services.open_low_high import scheduler as sch

    src = inspect.getsource(sch.start_open_low_high_scheduler)
    assert "misfire_grace_time=90" in src  # Phase A primary + refill
    assert "misfire_grace_time=120" in src  # Phase B 09:25:03
    assert "misfire_grace_time=60" in src  # Phase B retries
    assert "coalesce=True" in src
    assert "max_instances=1" in src
    assert "replace_existing=True" in src
    assert "_spawn_startup_catchup" in src


def test_startup_catchup_0922_runs_phase_a(monkeypatch):
    """Restart at 09:22 with no Phase A → immediate Phase A; Phase B not yet due."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 22, 0))
    calls = {"a": 0, "b": 0}

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: False)
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: False)
    monkeypatch.setattr(sch, "_run_phase_a", lambda: calls.__setitem__("a", calls["a"] + 1))
    monkeypatch.setattr(sch, "_run_phase_b", lambda: calls.__setitem__("b", calls["b"] + 1))

    # After Phase A runs, treat it as valid so we don't re-query DB.
    def _has_a_after(_d):
        return calls["a"] > 0

    monkeypatch.setattr(sch, "has_valid_phase_a", _has_a_after)

    out = sch.run_startup_catchup(now=now)
    assert out["ok"] is True
    assert out["actions"] == ["phase_a"]
    assert calls["a"] == 1
    assert calls["b"] == 0


def test_startup_catchup_0927_runs_phase_b_and_missing_a(monkeypatch):
    """Restart at 09:27: catch up Phase A if still missing, then Phase B."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 27, 0))
    calls = {"a": 0, "b": 0}
    phase_a_ok = {"v": False}
    phase_b_ok = {"v": False}

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: phase_a_ok["v"])
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: phase_b_ok["v"])

    def _run_a():
        calls["a"] += 1
        phase_a_ok["v"] = True

    def _run_b():
        calls["b"] += 1
        phase_b_ok["v"] = True

    monkeypatch.setattr(sch, "_run_phase_a", _run_a)
    monkeypatch.setattr(sch, "_run_phase_b", _run_b)

    out = sch.run_startup_catchup(now=now)
    assert out["ok"] is True
    assert out["actions"] == ["phase_a", "phase_b"]
    assert calls["a"] == 1
    assert calls["b"] == 1

    # Phase A already valid → only Phase B.
    calls["a"] = calls["b"] = 0
    phase_a_ok["v"] = True
    phase_b_ok["v"] = False
    out2 = sch.run_startup_catchup(now=now)
    assert out2["actions"] == ["phase_b"]
    assert calls["a"] == 0
    assert calls["b"] == 1


def test_startup_catchup_after_0929_marks_degraded(monkeypatch):
    """Restart after 09:29 with missing phases → DEGRADED + alert, no auto-run."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 30, 0))
    calls = {"a": 0, "b": 0, "meta": 0, "alert": 0}

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: False)
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: False)
    monkeypatch.setattr(sch, "_run_phase_a", lambda: calls.__setitem__("a", calls["a"] + 1))
    monkeypatch.setattr(sch, "_run_phase_b", lambda: calls.__setitem__("b", calls["b"] + 1))

    import backend.services.open_low_high.alerts as alerts_mod
    import backend.services.open_low_high.persistence as pers

    monkeypatch.setattr(
        pers,
        "upsert_day_meta",
        lambda *a, **k: calls.__setitem__("meta", calls["meta"] + 1),
    )
    monkeypatch.setattr(
        alerts_mod,
        "alert_degraded",
        lambda *a, **k: calls.__setitem__("alert", calls["alert"] + 1) or True,
    )

    out = sch.run_startup_catchup(now=now)
    assert out["ok"] is True
    assert out["actions"] == ["degraded"]
    assert calls["a"] == 0 and calls["b"] == 0
    assert calls["meta"] == 1 and calls["alert"] == 1


def test_startup_catchup_already_valid_never_reruns(monkeypatch):
    """Valid Phase A+B today → catch-up is a no-op (idempotent vs misfire/restart)."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 27, 0))
    calls = {"a": 0, "b": 0}

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: True)
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: True)
    monkeypatch.setattr(sch, "_run_phase_a", lambda: calls.__setitem__("a", calls["a"] + 1))
    monkeypatch.setattr(sch, "_run_phase_b", lambda: calls.__setitem__("b", calls["b"] + 1))

    out = sch.run_startup_catchup(now=now)
    assert out["ok"] is True
    assert out["actions"] == []
    assert out.get("skipped") == "nothing_to_catch_up"
    assert out["need_phase_a"] is False
    assert out["need_phase_b"] is False
    assert calls["a"] == 0 and calls["b"] == 0


def test_phase_a_misfire_skips_when_already_valid(monkeypatch):
    """Cron/misfire entry ``_run_phase_a`` must not call screener when Phase A is valid."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch
    import backend.services.open_low_high.screener as screener

    IST = pytz.timezone("Asia/Kolkata")
    calls = {"run": 0}
    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: True)
    monkeypatch.setattr(
        sch,
        "_aware_ist",
        lambda now=None: IST.localize(datetime(2026, 11, 20, 9, 21, 0)),
    )
    monkeypatch.setattr(
        screener,
        "run_phase_a",
        lambda **k: calls.__setitem__("run", calls["run"] + 1)
        or {"ok": True, "status": "phase_a"},
    )

    sch._run_phase_a()  # real wrapper (not monkeypatched away)
    assert calls["run"] == 0


def test_phase_b_misfire_skips_when_terminal(monkeypatch):
    """Cron/misfire entry ``_run_phase_b`` skips when Phase B is already frozen/partial."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch
    import backend.services.open_low_high.screener as screener

    IST = pytz.timezone("Asia/Kolkata")
    calls = {"run": 0}
    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "_phase_b_terminal", lambda d: True)
    monkeypatch.setattr(
        sch,
        "_aware_ist",
        lambda now=None: IST.localize(datetime(2026, 11, 20, 9, 26, 0)),
    )
    monkeypatch.setattr(
        screener,
        "run_phase_b",
        lambda **k: calls.__setitem__("run", calls["run"] + 1)
        or {"ok": True, "status": "frozen"},
    )

    sch._run_phase_b()
    assert calls["run"] == 0


def test_startup_catchup_concurrent_blocked(monkeypatch):
    """Second catch-up while first holds the lock returns catchup_busy."""
    from datetime import datetime
    import threading

    import pytz

    from backend.services.open_low_high import scheduler as sch

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 22, 0))
    entered = threading.Event()
    release = threading.Event()
    calls = {"a": 0}

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: False)
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: True)

    def _slow_a():
        calls["a"] += 1
        entered.set()
        release.wait(timeout=2.0)

    monkeypatch.setattr(sch, "_run_phase_a", _slow_a)
    monkeypatch.setattr(sch, "_run_phase_b", lambda: None)

    results = {}

    def _first():
        results["first"] = sch.run_startup_catchup(now=now)

    t = threading.Thread(target=_first)
    t.start()
    assert entered.wait(timeout=2.0)
    second = sch.run_startup_catchup(now=now)
    release.set()
    t.join(timeout=2.0)

    assert second.get("skipped") == "catchup_busy"
    assert second.get("actions") == []
    assert results["first"]["actions"] == ["phase_a"]
    assert calls["a"] == 1


def test_phase_a_concurrent_lock_returns_busy(monkeypatch):
    """Overlapping Phase A calls are blocked by _phase_a_lock (busy)."""
    import threading
    from datetime import date

    import backend.services.open_low_high.phases as phases

    results = []

    monkeypatch.setattr(phases, "ensure_tables", lambda: None)
    monkeypatch.setattr(phases, "is_day_frozen", lambda d: False)
    monkeypatch.setattr(phases, "phase_a_window_status", lambda now=None: "open")
    monkeypatch.setattr(phases, "_today", lambda: date(2026, 11, 20))

    # Hold the lock the same way a live Phase A run would.
    assert phases._phase_a_lock.acquire(blocking=False)

    def _blocked():
        results.append(phases.run_phase_a(force=True, session=date(2026, 11, 20)))

    try:
        t = threading.Thread(target=_blocked)
        t.start()
        t.join(timeout=2.0)
        assert results and results[0].get("status") == "busy"
        assert "already running" in (results[0].get("message") or "").lower()
    finally:
        phases._phase_a_lock.release()


def test_phase_b_concurrent_lock_returns_busy(monkeypatch):
    """Overlapping Phase B calls are blocked by _phase_b_lock (busy)."""
    import threading
    from datetime import date

    import backend.services.open_low_high.phases as phases

    results = []
    monkeypatch.setattr(phases, "ensure_tables", lambda: None)
    monkeypatch.setattr(phases, "is_day_frozen", lambda d: False)
    monkeypatch.setattr(phases, "phase_b_window_status", lambda now=None: "open")
    monkeypatch.setattr(phases, "_today", lambda: date(2026, 11, 20))

    assert phases._phase_b_lock.acquire(blocking=False)
    try:
        t = threading.Thread(
            target=lambda: results.append(
                phases.run_phase_b(force=True, session=date(2026, 11, 20))
            )
        )
        t.start()
        t.join(timeout=2.0)
        assert results and results[0].get("status") == "busy"
        assert "already running" in (results[0].get("message") or "").lower()
    finally:
        phases._phase_b_lock.release()


def test_startup_catchup_092002_respects_phase_a_buffer(monkeypatch):
    """Restart at 09:20:02 — before Phase A buffer (~09:20:10) → do not run Phase A yet."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch
    from backend.services.open_low_high.config import PHASE_A_START, PHASE_B_START

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 20, 2))
    calls = {"a": 0, "b": 0}

    assert PHASE_A_START.hour == 9 and PHASE_A_START.minute == 20
    assert PHASE_A_START.second >= 2  # default buffer 10s → 09:20:10
    assert now.time() < PHASE_A_START
    assert now.time() < PHASE_B_START

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: False)
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: False)
    monkeypatch.setattr(sch, "_run_phase_a", lambda: calls.__setitem__("a", calls["a"] + 1))
    monkeypatch.setattr(sch, "_run_phase_b", lambda: calls.__setitem__("b", calls["b"] + 1))

    out = sch.run_startup_catchup(now=now)
    assert out["ok"] is True
    assert out["actions"] == []
    assert out.get("skipped") == "nothing_to_catch_up"
    assert out.get("buffer_wait_phase_a") == PHASE_A_START.strftime("%H:%M:%S")
    assert calls["a"] == 0 and calls["b"] == 0
    # Scheduled Phase A at PHASE_A_START (+ retries) still apply after the buffer.


def test_startup_catchup_092502_respects_phase_b_buffer(monkeypatch):
    """Restart at 09:25:02 — before Phase B buffer (~09:25:03) → no Phase B; A may catch up."""
    from datetime import datetime

    import pytz

    from backend.services.open_low_high import scheduler as sch
    from backend.services.open_low_high.config import PHASE_A_START, PHASE_B_START

    IST = pytz.timezone("Asia/Kolkata")
    now = IST.localize(datetime(2026, 11, 20, 9, 25, 2))
    calls = {"a": 0, "b": 0}
    phase_a_ok = {"v": False}

    assert PHASE_B_START.hour == 9 and PHASE_B_START.minute == 25
    assert PHASE_B_START.second >= 2  # default buffer 3s → 09:25:03
    assert now.time() >= PHASE_A_START
    assert now.time() < PHASE_B_START

    monkeypatch.setattr(sch, "_skip", lambda: False)
    monkeypatch.setattr(sch, "has_valid_phase_a", lambda d: phase_a_ok["v"])
    monkeypatch.setattr(sch, "has_valid_phase_b", lambda d: False)

    def _run_a():
        calls["a"] += 1
        phase_a_ok["v"] = True

    monkeypatch.setattr(sch, "_run_phase_a", _run_a)
    monkeypatch.setattr(sch, "_run_phase_b", lambda: calls.__setitem__("b", calls["b"] + 1))

    out = sch.run_startup_catchup(now=now)
    assert out["ok"] is True
    assert out["actions"] == ["phase_a"]  # A due; B still in buffer
    assert out.get("buffer_wait_phase_b") == PHASE_B_START.strftime("%H:%M:%S")
    assert calls["a"] == 1 and calls["b"] == 0

    # Phase A already valid → still wait for Phase B buffer (retries at 09:25:03+).
    calls["a"] = calls["b"] = 0
    out2 = sch.run_startup_catchup(now=now)
    assert out2["actions"] == []
    assert out2.get("buffer_wait_phase_b") == PHASE_B_START.strftime("%H:%M:%S")
    assert calls["a"] == 0 and calls["b"] == 0
