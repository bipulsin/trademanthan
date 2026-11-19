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
        lambda only_null=False: [("ZZNULL", "NSE_EQ|INE000")] if only_null else [],
    )
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
    monkeypatch.setattr(pdc_mod, "_load_equity_rows", lambda only_null=True: [("AAA", "NSE_EQ|1"), ("BBB", "NSE_EQ|2")])
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
