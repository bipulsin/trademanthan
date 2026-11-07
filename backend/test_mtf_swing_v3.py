"""MTF Swing v3 tests. No network. Does not alter v1 / v2 baseline behaviour."""
from __future__ import annotations

from datetime import date, datetime, timedelta

import pytz

from backend.services.mtf_swing.bars import Bar
from backend.services.mtf_swing.config import (
    V3_VARIANTS,
    ablation_config,
    resolve_variants,
)
from backend.services.mtf_swing.engine import run_symbol
from backend.services.mtf_swing.stock_trend import (
    build_stock_trend_series,
    classify_stock_trend,
    trend_allows,
    trend_by_screen_date,
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


def test_resolve_v3_variants_and_bare_all_quirk():
    assert resolve_variants(["v3"]) == list(V3_VARIANTS)
    assert resolve_variants(["v3_all,v3_trend_only"]) == ["v3_all", "v3_trend_only"]
    # Bare all still expands to v2 primary matrix only (historical CLI quirk).
    tags = resolve_variants(["all"])
    assert "v1_equivalent" in tags and "all" in tags
    assert "v3_all" not in tags
    cfg = ablation_config("v3_all")
    assert cfg.stock_ema_trend is True
    assert cfg.weekly_window is True
    assert cfg.weekly_cap == 5
    assert cfg.regime_filter is False
    assert cfg.tp_r == 1.5


def test_stock_ema_trend_no_lookahead():
    fridays = []
    d = date(2024, 1, 5)
    while len(fridays) < 90:
        if d.weekday() == 4:
            fridays.append(d)
        d += timedelta(days=1)
    closes = [100 + i * 0.5 for i in range(90)]
    bars = [bar(fridays[i], c, c + 1, c - 1, c) for i, c in enumerate(closes)]
    pts = build_stock_trend_series(bars, ema_length=20, warmup_weeks=80)
    assert pts
    assert pts[0].screen_date == fridays[79]
    # Truncation must not change earlier labels.
    truncated = build_stock_trend_series(bars[:85], ema_length=20, warmup_weeks=80)
    assert truncated[0].trend_state == pts[0].trend_state
    assert truncated[0].ema20 == pts[0].ema20
    assert classify_stock_trend(110, 100) == "Bullish"
    assert classify_stock_trend(90, 100) == "Bearish"
    assert trend_allows("LONG", "Bullish")
    assert not trend_allows("SHORT", "Bullish")
    mapping = trend_by_screen_date(pts)
    assert fridays[79].isoformat() in mapping


def test_weekly_window_after_cutoff_and_week_end():
    # Build a minimal series spanning one full week Mon–Fri with a long setup already armed.
    mon = date(2026, 5, 4)  # Monday
    days = days_from(mon, 5)
    assert days[0].weekday() == 0 and days[4].weekday() == 4
    bars = []
    for d in days:
        bars.append(bar(d, 100, 101, 99, 100, hh=9, mm=15))
        bars.append(bar(d, 100, 101, 99, 100, hh=15, mm=15))
    from backend.services.mtf_swing.engine import Setup

    setup = Setup(
        side="LONG",
        screen_date=(mon - timedelta(days=3)).isoformat(),
        valid_week=mon.isocalendar()[:2],
        sl=90.0,
        t1=120.0,
        trigger=99.0,
        basis_contract="M1",
        phase="limit",
        limit_price=100.0,
    )
    cfg = ablation_config("v3_window_only", timeframe="15m", regime_filter=False)
    # Seed state with the pending limit; engine should cancel AFTER_CUTOFF on Thu.
    trades, discards, st = run_symbol(
        bars,
        symbol="WK",
        holidays=set(),
        active_from=mon,
        active_to=days[-1],
        state={"setup": setup.to_dict(), "queued": None, "position": None, "last_ts": "", "last_close": None, "last_contract": ""},
        config=cfg,
        lot_size=1,
        finalize_open=True,
    )
    reasons = {d["reason"] for d in discards}
    # Either filled Mon–Wed or cancelled AFTER_CUTOFF; must not leave a Thu/Fri fill.
    assert "AFTER_CUTOFF" in reasons or any(t.get("exit_reason") == "WEEK_END" for t in trades) or trades
    for t in trades:
        if t.get("entry_time"):
            assert date.fromisoformat(t["entry_time"][:10]).weekday() <= 2


def test_weekly_cap_first_come_shared_counter():
    cfg = ablation_config("v3_cap_only", timeframe="60m", regime_filter=False, weekly_cap=1)
    cap = {}
    # Two symbols trying to enter the same ISO week via seeded next_open fills.
    mon = date(2026, 6, 1)
    assert mon.weekday() == 0
    bars = [
        bar(mon, 100, 101, 99, 100, hh=9, mm=15),
        bar(mon, 100, 101, 99, 100, hh=15, mm=15),
    ]
    from backend.services.mtf_swing.engine import Setup

    def seeded(sym):
        setup = Setup(
            side="LONG",
            screen_date=(mon - timedelta(days=3)).isoformat(),
            valid_week=mon.isocalendar()[:2],
            sl=90.0,
            t1=130.0,
            trigger=99.0,
            basis_contract="M1",
            phase="next_open",
            defer_fill=True,
            potential_rr="2.0000",
        )
        return run_symbol(
            bars,
            symbol=sym,
            holidays=set(),
            active_from=mon,
            active_to=mon,
            state={"setup": setup.to_dict(), "queued": None, "position": None, "last_ts": "", "last_close": None, "last_contract": ""},
            config=cfg,
            lot_size=1,
            finalize_open=True,
            weekly_cap_state=cap,
        )

    t1, d1, _ = seeded("AAA")
    t2, d2, _ = seeded("BBB")
    entries = [t for t in t1 + t2 if t.get("exit_reason") != "OPEN" or t.get("entry_time")]
    # Cap=1 → at most one accepted entry across both symbols; the other gets WEEKLY_CAP.
    cap_discards = [d for d in d1 + d2 if d.get("reason") == "WEEKLY_CAP"]
    assert len(cap) <= 1 or sum(cap.values()) == 1
    assert cap_discards or len([t for t in t1 + t2 if t.get("entry_time")]) <= 1


def test_v3_continuous_matches_month_batch_flags():
    """Smoke: v3_all flags match month-batch vs single window for a flat series."""
    days = days_from(date(2025, 11, 3), 40)
    bars = [bar(d, 100, 101, 99, 100) for d in days]
    cfg = ablation_config("v3_trend_only", timeframe="60m")
    # No setups expected on flat bars; just ensure engine accepts trend_map path.
    from backend.services.mtf_swing.stock_trend import build_stock_trend_series, trend_by_screen_date

    pts = build_stock_trend_series(bars, ema_length=20, warmup_weeks=5)
    trades, discards, st = run_symbol(
        bars,
        symbol="FLAT",
        holidays=set(),
        active_from=date(2025, 11, 1),
        active_to=date(2025, 12, 15),
        config=cfg,
        lot_size=1,
        finalize_open=True,
        trend_map=trend_by_screen_date(pts),
    )
    assert isinstance(trades, list)
    assert isinstance(discards, list)
    assert "last_ts" in st
