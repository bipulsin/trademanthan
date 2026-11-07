"""MTF Swing v2 experiment tests. No network. v1 behaviour must stay intact."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytz

from backend.services.mtf_swing.bars import Bar
from backend.services.mtf_swing.config import (
    ABLATION_VARIANTS,
    MtfSwingConfig,
    TRADE_FIELDS,
    ablation_config,
    months_between,
    resolve_variants,
    v2_config,
)
from backend.services.mtf_swing.csv_store import append_trades, append_trades_v2, read_rows
from backend.services.mtf_swing.engine import run_symbol
from backend.services.mtf_swing.levels import WeekStats, build_levels, candidate_side
from backend.services.mtf_swing.metrics import (
    ablation_vs_baseline,
    build_dashboard,
    build_v2_dashboard,
    compare_v1_equivalent,
    core_trade_signature,
    pick_default_variant,
    robustness_block,
)
from backend.services.mtf_swing.regime import (
    build_regime_series,
    classify_regime,
    ema,
    regime_allows,
    regime_by_screen_date,
)

IST = pytz.timezone("Asia/Kolkata")


def bar(day: date, o, h, l, c, contract: str = "M1", hh: int = 15, mm: int = 15) -> Bar:
    return Bar(
        ts=IST.localize(datetime(day.year, day.month, day.day, hh, mm)),
        open=o,
        high=h,
        low=l,
        close=c,
        contract=contract,
    )


def days_from(start: date, n: int):
    out = []
    d = start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _wk(low, high, close, last_date=date(2025, 11, 14)) -> WeekStats:
    return WeekStats(
        key=(2025, 46),
        low=low,
        high=high,
        close=close,
        last_date=last_date,
        last_ts=IST.localize(datetime(last_date.year, last_date.month, last_date.day, 15, 15)),
        basis_contract="M",
        adjusted=[],
    )


def test_regime_ema_no_lookahead_and_classification():
    # Build rising then falling weekly closes via daily bars (one bar per week Friday).
    fridays = []
    d = date(2025, 9, 5)
    while len(fridays) < 14:
        if d.weekday() == 4:
            fridays.append(d)
        d += timedelta(days=1)
    # Steadily rising for 10 weeks, then drop.
    closes = [100 + i * 2 for i in range(10)] + [115, 110, 100, 90]
    bars = [bar(fridays[i], c, c + 1, c - 1, c) for i, c in enumerate(closes)]
    points = build_regime_series(bars, ma_length=10, ma_type="ema")
    assert points
    # First point requires 10 weeks of data — screen_date is the 10th Friday.
    assert points[0].screen_date == fridays[9]
    # EMA at index 9 is SMA of first 10 closes.
    assert abs(points[0].ma - (sum(closes[:10]) / 10.0)) < 1e-6
    # Rising close > MA → Bullish once prev_ma exists (from week 11 onward).
    assert any(p.regime == "Bullish" for p in points)
    # Late drop should produce Bearish or Neutral, never look ahead past each Friday.
    mapping = regime_by_screen_date(points)
    assert fridays[9].isoformat() in mapping
    # A later week's label must not depend on future closes: rebuild truncated series.
    truncated = build_regime_series(bars[:11], ma_length=10)
    assert truncated[0].regime == points[0].regime
    assert truncated[0].ma == points[0].ma

    assert classify_regime(120, 100, 99)[0] == "Bullish"
    assert classify_regime(80, 100, 101)[0] == "Bearish"
    assert classify_regime(120, 100, 101)[0] == "Neutral"  # above MA but falling
    assert regime_allows("LONG", "Bullish")
    assert not regime_allows("SHORT", "Bullish")
    assert not regime_allows("LONG", "Neutral", "block")
    assert regime_allows("LONG", "Neutral", "allow_both")


def test_pivot_sl_outside_prev_week_and_no_pivot_discard():
    # Trigger swing high before SL pivot; pivot swing low below W2.low.
    w1_days = days_from(date(2025, 11, 17), 5)
    w1_bars = [
        bar(w1_days[0], 100, 101, 100, 100),
        bar(w1_days[1], 100, 105, 99, 102),
        bar(w1_days[2], 102, 102, 98, 100),
        bar(w1_days[3], 100, 103, 93, 97),
        bar(w1_days[4], 97, 104, 96, 106),
    ]
    w2 = _wk(100, 130, 115)
    w1 = WeekStats(
        key=(2025, 47),
        low=93,
        high=105,
        close=106,
        last_date=w1_days[4],
        last_ts=w1_bars[-1].ts,
        basis_contract="M",
        adjusted=w1_bars,
    )
    assert candidate_side(w1, w2) == "LONG"
    levels, reason = build_levels("LONG", w1, w2, sl_basis="pivot_outside_prev_week")
    assert reason is None
    assert levels is not None
    assert levels.sl == 93
    assert levels.trigger == 105
    assert levels.sl_basis == "pivot_outside_prev_week"
    assert levels.opp_extreme == 130

    # Swing lows exist but none below W2.low → NO_PIVOT_SL
    flat = [
        bar(w1_days[0], 102, 105, 101, 103),
        bar(w1_days[1], 103, 106, 100.5, 104),
        bar(w1_days[2], 104, 107, 101, 105),
        bar(w1_days[3], 105, 108, 102, 106),
        bar(w1_days[4], 106, 109, 103, 107),
    ]
    w1b = WeekStats(
        key=(2025, 47),
        low=100.5,
        high=109,
        close=107,
        last_date=w1_days[4],
        last_ts=flat[-1].ts,
        basis_contract="M",
        adjusted=flat,
    )
    levels2, reason2 = build_levels("LONG", w1b, w2, sl_basis="pivot_outside_prev_week")
    assert levels2 is None
    assert reason2 == "NO_PIVOT_SL"


def _long_context_pivot():
    """W2 + W1 that screens LONG with pivot SL at 93, trigger 105, opp 130.

    Trigger swing high must form *before* the SL pivot timestamp.
    """
    bars = []
    for d in days_from(date(2025, 11, 10), 5):
        bars.append(bar(d, 110, 130, 100, 115))
    w1 = days_from(date(2025, 11, 17), 5)
    bars += [
        bar(w1[0], 100, 101, 100, 100),
        bar(w1[1], 100, 105, 99, 102),  # swing high 105 (trigger)
        bar(w1[2], 102, 102, 98, 100),  # confirms swing high
        bar(w1[3], 100, 103, 93, 97),  # swing low 93 (pivot SL below W2.low)
        bar(w1[4], 97, 104, 96, 106),  # confirms swing low; Friday screen close
    ]
    return bars


def test_v2_tp_r_no_time_exit_and_open_mtm():
    entry_days = days_from(date(2025, 11, 24), 5)
    bars = _long_context_pivot() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 106),  # breakout close 106 > trigger 105
        bar(entry_days[2], 107, 109, 106, 108),  # next open fill 107
        bar(entry_days[3], 108, 110, 107, 109),
        bar(entry_days[4], 109, 111, 108, 110),
    ]
    cfg = v2_config(regime_filter=False, timeframe="15m")
    # potential RR at 106: (130-106)/(106-93) = 24/13 ≈ 1.85 >= 1.3
    trades, _, _ = run_symbol(
        bars,
        symbol="V2A",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=10,
        finalize_open=True,
    )
    assert len(trades) == 1
    t = trades[0]
    assert t["exit_reason"] == "OPEN"
    assert t["mtm_price"] == "110.0000"
    # entry 107, sl 93, risk 14 → TP = 107 + 1.5*14 = 128.0
    assert abs(float(t["sl"]) - 93) < 1e-6
    assert abs(float(t["t1"]) - 128.0) < 1e-6
    assert float(t["mtm_pnl"]) == (110 - 107) * 10
    assert t["variant"] == "all"
    # No TIME exit even after many sessions when max_sessions is None.
    more = bars[:]
    d = entry_days[4] + timedelta(days=1)
    for _ in range(15):
        if d.weekday() < 5:
            more.append(bar(d, 110, 112, 109, 111))
        d += timedelta(days=1)
    trades2, _, _ = run_symbol(
        more,
        symbol="V2A",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=10,
        finalize_open=True,
    )
    assert trades2[0]["exit_reason"] == "OPEN"
    assert "TIME" not in {trades2[0]["exit_reason"]}


def test_v2_min_potential_rr_skip_and_tp_hit():
    entry_days = days_from(date(2025, 11, 24), 4)
    # Breakout close very close to opp extreme → potential RR < 1.3
    bars = _long_context_pivot() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 120, 128, 119, 127),  # close 127; RR=(130-127)/(127-93)=3/34≈0.09
    ]
    cfg = v2_config(regime_filter=False, timeframe="15m")
    trades, discards, _ = run_symbol(
        bars,
        symbol="V2B",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=1,
        finalize_open=True,
    )
    assert trades == []
    assert any(d["reason"] == "RR_BELOW_MIN" for d in discards)

    # Good RR then hit TP at 1.5R
    good = _long_context_pivot() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 106),
        bar(entry_days[2], 107, 109, 106, 108),
        bar(entry_days[3], 108, 130, 107, 128),  # TP 128.0 touched
    ]
    trades2, _, _ = run_symbol(
        good,
        symbol="V2C",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=2,
        finalize_open=False,
    )
    assert len(trades2) == 1
    assert trades2[0]["exit_reason"] == "TP"
    assert abs(float(trades2[0]["exit_price"]) - 128.0) < 1e-6
    assert float(trades2[0]["potential_rr"]) >= 1.3


def test_v2_pre_entry_cancel_and_same_bar_sl_first():
    entry_days = days_from(date(2025, 11, 24), 3)
    # Breakout, then next bar gaps through prospective TP before fill.
    bars = _long_context_pivot() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 106),  # signal; prospective TP = 106+1.5*(106-93)=125.5
        bar(entry_days[2], 126, 128, 125, 127),  # open already through TP
    ]
    cfg = v2_config(regime_filter=False, timeframe="15m")
    trades, discards, _ = run_symbol(
        bars,
        symbol="V2D",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=1,
        finalize_open=True,
    )
    assert trades == []
    assert any("GAP_THROUGH_TP" in d["reason"] or d["reason"] == "GAP_THROUGH_TP" for d in discards)

    # Same-bar SL and TP → SL wins
    same = _long_context_pivot() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 106),
        bar(entry_days[2], 107, 140, 80, 100),  # fill at open 107, same bar hits SL and TP
    ]
    trades2, _, _ = run_symbol(
        same,
        symbol="V2E",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=1,
        finalize_open=False,
    )
    assert trades2[0]["exit_reason"] == "SL"
    assert trades2[0]["exit_price"] == "93.0000"


def test_v2_regime_blocks_wrong_side(tmp_path: Path):
    entry_days = days_from(date(2025, 11, 24), 3)
    bars = _long_context_pivot() + [
        bar(entry_days[0], 100, 104, 100, 103),
        bar(entry_days[1], 103, 108, 100, 106),
        bar(entry_days[2], 107, 109, 106, 108),
    ]
    screen = days_from(date(2025, 11, 17), 5)[4].isoformat()
    cfg = v2_config(timeframe="15m")
    # Bearish regime should block LONG.
    trades, discards, _ = run_symbol(
        bars,
        symbol="V2F",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=1,
        finalize_open=True,
        regime_map={screen: "Bearish"},
    )
    assert trades == []
    assert any(d["reason"] == "REGIME_BLOCK" for d in discards)

    trades2, _, _ = run_symbol(
        bars,
        symbol="V2F",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=1,
        finalize_open=True,
        regime_map={screen: "Bullish"},
    )
    assert len(trades2) == 1
    assert trades2[0]["regime"] == "Bullish"


def test_v1_isolation_csv_and_defaults(tmp_path: Path):
    """v2 writes must not rewrite v1 TRADE_FIELDS / default config behaviour."""
    v1_path = tmp_path / "v1_trades.csv"
    v2_path = tmp_path / "v2_trades.csv"
    row_v1 = {
        "trade_id": "ISO|60m|LONG|2025-11-28T15:15:00+05:30",
        "timeframe": "60m",
        "month": "2025-11",
        "symbol": "ISO",
        "side": "LONG",
        "screen_date": "2025-11-21",
        "entry_time": "2025-11-28T15:15:00+05:30",
        "entry_price": "110.0000",
        "entry_type": "direct",
        "lot_size": "1",
        "qty_lots": "1",
        "sl": "90.0000",
        "t1": "130.0000",
        "exit_time": "2025-11-29T15:15:00+05:30",
        "exit_price": "130.0000",
        "exit_reason": "T1",
        "sessions_held": "2",
        "gross_pnl": "40.00",
        "charges": "0.00",
        "net_pnl": "40.00",
        "r_multiple": "1.0000",
        "run_ts": "t",
    }
    assert append_trades(v1_path, [row_v1]) == 1
    before = v1_path.read_bytes()
    # Writing v2 elsewhere must not touch v1.
    row_v2 = dict(row_v1)
    row_v2["trade_id"] = "ISO|60m|LONG|2025-11-28T15:15:00+05:30|all"
    row_v2.update(
        {
            "variant": "all",
            "regime": "Bullish",
            "potential_rr": "1.5000",
            "sl_basis": "pivot_outside_prev_week",
            "tp_r": "1.50",
            "mtm_price": "",
            "mtm_pnl": "",
            "exit_reason": "TP",
        }
    )
    assert append_trades_v2(v2_path, [row_v2]) == 1
    assert v1_path.read_bytes() == before
    header = before.splitlines()[0].decode()
    assert header == ",".join(TRADE_FIELDS)

    # Default config still has v1 semantics.
    cfg = MtfSwingConfig()
    assert cfg.variant == "v1"
    assert cfg.sl_basis == "abs_extreme"
    assert cfg.tp_r is None
    assert cfg.max_sessions == 10
    assert cfg.min_potential_rr is None
    assert cfg.regime_filter is False


def test_v2_dashboard_aggregates_match_csv_and_exclude_open():
    rows = [
        {
            "trade_id": "A|60m|LONG|1|all",
            "timeframe": "60m",
            "month": "2026-06",
            "symbol": "A",
            "side": "LONG",
            "exit_reason": "TP",
            "exit_time": "2026-06-10T15:15:00+05:30",
            "net_pnl": "100.00",
            "variant": "all",
            "mtm_pnl": "",
        },
        {
            "trade_id": "B|60m|SHORT|2|all",
            "timeframe": "60m",
            "month": "2026-06",
            "symbol": "B",
            "side": "SHORT",
            "exit_reason": "SL",
            "exit_time": "2026-06-12T15:15:00+05:30",
            "net_pnl": "-40.00",
            "variant": "all",
            "mtm_pnl": "",
        },
        {
            "trade_id": "C|60m|LONG|3|all",
            "timeframe": "60m",
            "month": "2026-06",
            "symbol": "C",
            "side": "LONG",
            "exit_reason": "OPEN",
            "exit_time": "2026-07-01T15:15:00+05:30",
            "net_pnl": "",
            "variant": "all",
            "mtm_pnl": "25.00",
            "mtm_price": "110",
        },
    ]
    dash = build_v2_dashboard(rows, timeframe="60m", month="2026-06", variant="all")
    assert dash["summary"]["trades"] == 2
    assert dash["summary"]["net_pnl"] == 60
    assert dash["summary"]["open_trades"] == 1
    assert dash["summary"]["mtm_pnl_sum"] == 25
    assert dash["open_mtm"]["open_trades"] == 1
    # Classic dashboard also excludes OPEN from closed counts.
    classic = build_dashboard(rows, timeframe="60m")
    assert classic["summary"]["trades"] == 2
    assert classic["open_trades"] == 1


def test_ablation_configs_toggle_flags():
    v1e = ablation_config("v1_equivalent")
    assert not v1e.regime_filter and v1e.sl_basis == "abs_extreme"
    assert v1e.tp_r is None and v1e.max_sessions == 10 and v1e.min_potential_rr is None
    # Legacy short names still resolve.
    r = ablation_config("regime")
    assert r.ablation_tag == "regime_only" and r.regime_filter and r.sl_basis == "abs_extreme"
    s = ablation_config("sl_only")
    assert s.sl_basis == "pivot_outside_prev_week" and not s.regime_filter
    t = ablation_config("tp_only")
    assert t.tp_r == 1.5 and t.max_sessions is None
    rr = ablation_config("rr_only")
    assert rr.min_potential_rr == 1.3
    allc = ablation_config("all")
    assert allc.regime_filter and allc.sl_basis == "pivot_outside_prev_week"
    assert allc.tp_r == 1.5 and allc.min_potential_rr == 1.3
    lo = ablation_config("all_except_regime")
    assert not lo.regime_filter and lo.tp_r == 1.5


def test_resolve_variants_matrix_and_specific():
    assert resolve_variants(["all"]) == list(ABLATION_VARIANTS)
    assert resolve_variants(["v1_equivalent"]) == ["v1_equivalent"]
    assert resolve_variants(["regime_only", "all"]) == ["regime_only", "all"]
    assert "all_except_sl" in resolve_variants(["v1_equivalent"], leave_one_out=True)
    assert months_between("2025-11", "2026-10")[0] == "2025-11"
    assert months_between("2025-11", "2026-10")[-1] == "2026-10"
    assert len(months_between("2025-11", "2026-10")) == 12


def test_ema_seed_matches_sma():
    vals = [10.0, 11, 12, 13, 14]
    series = ema(vals, 3)
    assert series[0] is None and series[1] is None
    assert abs(series[2] - (10 + 11 + 12) / 3) < 1e-9
    assert series[3] is not None


def test_v1_equivalent_month_batch_matches_continuous():
    """Continuous state carry for v1_equivalent must match a single-window run."""
    # Reuse the proven v1 carry series from test_mtf_swing via identical construction.
    from backend.test_mtf_swing import _carry_series, _sig

    bars = _carry_series()
    cfg = ablation_config("v1_equivalent", timeframe="60m")
    continuous, _, _ = run_symbol(
        bars,
        symbol="V1E",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 12, 31),
        config=cfg,
        lot_size=250,
        finalize_open=True,
    )
    assert len(continuous) == 1
    nov_bars = [b for b in bars if b.session_date.month == 11]
    dec_bars = [b for b in bars if b.session_date >= date(2025, 11, 10)]
    t1, _, state = run_symbol(
        nov_bars,
        symbol="V1E",
        active_from=date(2025, 11, 1),
        active_to=date(2025, 11, 30),
        config=cfg,
        lot_size=250,
        finalize_open=False,
    )
    assert t1 == []
    assert state.get("position") is not None
    t2, _, _ = run_symbol(
        dec_bars,
        symbol="V1E",
        active_from=date(2025, 12, 1),
        active_to=date(2025, 12, 31),
        state=state,
        config=cfg,
        lot_size=250,
        finalize_open=True,
    )
    assert _sig(t1 + t2) == _sig(continuous)
    # Cross-month: one trade, entry month Nov, symbol lock via single position state.
    assert continuous[0]["month"] == "2025-11"
    assert continuous[0]["variant"] == "v1_equivalent"


def test_v1_equivalent_matches_plain_v1_signatures():
    from backend.test_mtf_swing import _carry_series

    bars = _carry_series()
    v1_cfg = MtfSwingConfig(timeframe="60m")
    v2_cfg = ablation_config("v1_equivalent", timeframe="60m")
    t_v1, _, _ = run_symbol(
        bars, symbol="EQ", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31),
        config=v1_cfg, lot_size=250, finalize_open=True,
    )
    t_v2, _, _ = run_symbol(
        bars, symbol="EQ", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31),
        config=v2_cfg, lot_size=250, finalize_open=True,
    )
    # trade_id gains a |v1_equivalent suffix; core fields must still match v1.
    assert [core_trade_signature(r) for r in t_v1] == [core_trade_signature(r) for r in t_v2]
    assert t_v1[0]["net_pnl"] == t_v2[0]["net_pnl"]


def test_pick_default_variant_prefers_full_period_over_pilot_all():
    pilot = [
        {"variant": "all", "month": "2026-06", "timeframe": "60m"},
        {"variant": "all", "month": "2026-10", "timeframe": "60m"},
    ]
    full_v1e = [
        {"variant": "v1_equivalent", "month": f"2026-{m:02d}", "timeframe": "60m"}
        for m in range(1, 11)
    ] + [
        {"variant": "v1_equivalent", "month": "2025-11", "timeframe": "60m"},
        {"variant": "v1_equivalent", "month": "2025-12", "timeframe": "60m"},
    ]
    assert pick_default_variant(pilot + full_v1e) == "v1_equivalent"
    full_all = [
        {"variant": "all", "month": lab, "timeframe": "60m"}
        for lab in (
            "2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04",
            "2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10",
        )
    ]
    assert pick_default_variant(full_all + full_v1e) == "all"
    assert pick_default_variant([]) == "v1_equivalent"


def test_like_for_like_dashboard_scopes_months_and_wr_pp():
    v1 = [
        {"trade_id": "A", "timeframe": "60m", "month": "2026-06", "symbol": "A", "side": "LONG",
         "exit_reason": "T1", "exit_time": "2026-06-10T15:15:00+05:30", "net_pnl": "100", "r_multiple": "1"},
        {"trade_id": "B", "timeframe": "60m", "month": "2026-07", "symbol": "B", "side": "SHORT",
         "exit_reason": "SL", "exit_time": "2026-07-10T15:15:00+05:30", "net_pnl": "-50", "r_multiple": "-1"},
    ]
    v2 = [
        {"trade_id": "A|all", "timeframe": "60m", "month": "2026-06", "symbol": "A", "side": "LONG",
         "exit_reason": "TP", "exit_time": "2026-06-10T15:15:00+05:30", "net_pnl": "80", "variant": "all",
         "r_multiple": "1.5", "regime": "Bullish"},
        {"trade_id": "C|all", "timeframe": "60m", "month": "2026-06", "symbol": "C", "side": "LONG",
         "exit_reason": "OPEN", "exit_time": "2026-06-30T15:15:00+05:30", "net_pnl": "", "variant": "all",
         "mtm_pnl": "10", "regime": "Bullish"},
    ]
    dash = build_v2_dashboard(v2, timeframe="60m", month="2026-06", variant="all", v1_trades=v1)
    assert dash["comparison"]["scope"]
    assert "pp" in dash["comparison"]["scope"] or "closed" in dash["comparison"]["scope"]
    assert dash["comparison"]["v1"]["trades"] == 1
    assert dash["comparison"]["v2"]["trades"] == 1
    assert dash["comparison"]["delta_pp"]["win_rate"] is not None
    # Full month table stays 12 rows; headline like-for-like clips to months the variant has.
    full = build_v2_dashboard(v2, timeframe="60m", month="all", variant="all", v1_trades=v1)
    assert len(full["month_compare"]) == 12
    assert any(m["month"] == "2026-07" for m in full["month_compare"])
    assert full["scope_months"] == ["2026-06"]
    # v1 July must not inflate the headline comparison when variant=all only has June.
    assert full["comparison"]["v1"]["trades"] == 1
    assert full["comparison"]["v2"]["trades"] == 1


def test_concentration_exposure_and_ablation_baseline():
    rows = [
        {"trade_id": "1", "timeframe": "15m", "month": "2026-06", "symbol": "AAA", "side": "LONG",
         "exit_reason": "TP", "exit_time": "2026-06-01", "net_pnl": "100", "variant": "v1_equivalent",
         "r_multiple": "1", "regime": "Bullish"},
        {"trade_id": "2", "timeframe": "15m", "month": "2026-06", "symbol": "AAA", "side": "LONG",
         "exit_reason": "SL", "exit_time": "2026-06-02", "net_pnl": "-40", "variant": "v1_equivalent",
         "r_multiple": "-1", "regime": "Bullish"},
        {"trade_id": "3", "timeframe": "15m", "month": "2026-06", "symbol": "BBB", "side": "SHORT",
         "exit_reason": "TP", "exit_time": "2026-06-03", "net_pnl": "50", "variant": "regime_only",
         "r_multiple": "1.2", "regime": "Bearish"},
        {"trade_id": "4", "timeframe": "15m", "month": "2026-06", "symbol": "CCC", "side": "LONG",
         "exit_reason": "OPEN", "exit_time": "2026-06-30", "net_pnl": "", "variant": "regime_only",
         "mtm_pnl": "5", "regime": "Bullish"},
    ]
    rob = robustness_block([r for r in rows if r["variant"] == "regime_only"])
    assert rob["open_trades"] == 1
    assert rob["unique_symbols"] == 1
    assert "SHORT" in rob["side_breakdown"]
    table = ablation_vs_baseline(rows, timeframe="15m")
    tags = {r["variant"] for r in table}
    assert "v1_equivalent" in tags and "regime_only" in tags
    report = compare_v1_equivalent(
        [{"trade_id": "1", "timeframe": "15m", "month": "2026-06", "symbol": "AAA", "side": "LONG",
          "entry_time": "t", "exit_time": "t2", "exit_reason": "T1", "entry_price": "1",
          "exit_price": "2", "net_pnl": "100"}],
        [{"trade_id": "1|v1_equivalent", "timeframe": "15m", "month": "2026-06", "symbol": "AAA",
          "side": "LONG", "entry_time": "t", "exit_time": "t2", "exit_reason": "T1",
          "entry_price": "1", "exit_price": "2", "net_pnl": "100", "variant": "v1_equivalent"}],
        timeframe="15m",
    )
    assert report["ok"] is True


def test_v1_equivalent_open_exit_time_blank_like_v1():
    from backend.test_mtf_swing import _carry_series

    bars = _carry_series()
    v1_cfg = MtfSwingConfig(timeframe="60m")
    v2_cfg = ablation_config("v1_equivalent", timeframe="60m")
    t_v1, _, _ = run_symbol(
        bars, symbol="EQ", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31),
        config=v1_cfg, lot_size=250, finalize_open=True,
    )
    t_v2, _, _ = run_symbol(
        bars, symbol="EQ", active_from=date(2025, 11, 1), active_to=date(2025, 12, 31),
        config=v2_cfg, lot_size=250, finalize_open=True,
    )
    opens_v1 = [r for r in t_v1 if r["exit_reason"] == "OPEN"]
    opens_v2 = [r for r in t_v2 if r["exit_reason"] == "OPEN"]
    if opens_v1:
        assert opens_v1[0]["exit_time"] == ""
        assert opens_v2[0]["exit_time"] == ""
        assert opens_v2[0]["mtm_price"] == ""


def test_parity_hole_bar_filter_and_compare_open_signature():
    from backend.services.mtf_swing.parity import filter_bars_for_holes, holes_from_events, parse_log_events

    text = (
        # Real hole: never fetched successfully.
        "2026-11-07 03:00:00 INFO mtf_swing ADANIENSOL 2026-04 no candles\n"
        # Cascade overwrite must not turn a prior bars= month into a hole.
        "2026-11-07 03:01:00 INFO mtf_swing 60m 2025-11 ADANIENSOL bars=10\n"
        "2026-11-07 03:02:00 INFO mtf_swing ADANIENSOL 2025-11 no candles\n"
        # Later refill clears a prior hole.
        "2026-11-07 03:03:00 INFO mtf_swing ADANIENSOL 2026-05 no candles\n"
        "2026-11-07 06:00:00 INFO mtf_swing 60m 2026-05 ADANIENSOL bars=20\n"
    )
    events = parse_log_events(text, default_tf="60m")
    holes = holes_from_events(events)
    assert ("60m", "ADANIENSOL", "2026-04") in holes
    assert ("60m", "ADANIENSOL", "2025-11") not in holes
    assert ("60m", "ADANIENSOL", "2026-05") not in holes

    days = days_from(date(2026, 4, 28), 5)
    bars = [bar(d, 100, 101, 99, 100) for d in days]
    filtered = filter_bars_for_holes(bars, holes, timeframe="60m", symbol="ADANIENSOL")
    assert all(b.session_date.month != 4 for b in filtered)

    # Mixed full_run.log: 15m untyped failures must not become 60m holes.
    mixed = (
        "2026-11-07 02:00:00 INFO mtf_swing 60m 2025-11 symbols=2 window=2025-10-11..2025-11-30\n"
        "2026-11-07 02:01:00 INFO mtf_swing 60m 2025-11 RELIANCE bars=10 new_trades=0 new_discards=0 lot=500\n"
        "2026-11-07 03:00:00 INFO mtf_swing 15m 2025-11 symbols=2 window=2025-10-11..2025-11-30\n"
        "2026-11-07 03:01:00 INFO mtf_swing RELIANCE 2025-11 no candles\n"
        "2026-11-07 03:01:01 INFO mtf_swing ONLY15M 2025-11 no candles\n"
    )
    mixed_holes = holes_from_events(parse_log_events(mixed, default_tf="60m"))
    assert ("60m", "RELIANCE", "2025-11") not in mixed_holes
    assert ("60m", "ONLY15M", "2025-11") not in mixed_holes
    assert ("15m", "RELIANCE", "2025-11") in mixed_holes
    assert ("15m", "ONLY15M", "2025-11") in mixed_holes

    # OPEN exit_time blank vs stamped must compare equal.
    a = {
        "timeframe": "60m", "symbol": "X", "side": "LONG", "entry_time": "t",
        "exit_time": "", "exit_reason": "OPEN", "entry_price": "1", "exit_price": "",
        "net_pnl": "", "month": "2026-10",
    }
    b = dict(a, exit_time="2026-10-27T15:15:00+05:30")
    assert core_trade_signature(a) == core_trade_signature(b)
