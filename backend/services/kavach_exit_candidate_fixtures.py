"""Historical high-R give-back fixtures for exit-candidate shadow replay.

Bars are confirmed 10m OHLC with EMA5/EMA10 at bar close. FEDERALBNK is
reconstructed from the 20-Jul case log (true peak + spike + exit candles).
POLICYBZR / ADANIGREEN / CHOLAFIN are stylized reconstructions that preserve
the documented peak-R → scratch/loss give-back shape for candidate comparison
(not tick-perfect Upstox replays — re-run scripts/shadow_exit_candidates_high_r.py
with --fetch when paperclip Upstox is available).
"""
from __future__ import annotations

from typing import Dict, List, Tuple

from backend.services.kavach_exit_candidate_shadow import Bar, TradeSpec

CaseFixture = Tuple[TradeSpec, List[Bar]]


def federalbnk_20260720() -> CaseFixture:
    """FEDERALBNK 20-Jul — 3.7R spike → scratch on EMA5 close.

    Entry 350.10, risk 0.5 (EMA10 SL 349.60). Spike O/H/L/C 351.40/351.95/
    350.50/350.80 vs EMA5 350.55 (closed ABOVE — baseline correctly silent).
    Later exit close 350.40 vs EMA5 350.50 → fill ~350.10 (₹0).
    """
    trade = TradeSpec(
        symbol="FEDERALBNK",
        direction="LONG",
        entry=350.10,
        risk_pts=0.50,
        session_date="2026-07-20",
        notes="3.7R peak → round-trip scratch; spike closed above EMA5",
    )
    bars = [
        # Build toward peak (R on close still modest)
        Bar(349.90, 350.40, 349.80, 350.30, ema5=350.00, ema10=349.70, bar_at="2026-07-20T10:00:00+05:30"),
        Bar(350.30, 350.80, 350.20, 350.70, ema5=350.20, ema10=349.85, bar_at="2026-07-20T10:10:00+05:30"),
        Bar(350.70, 351.20, 350.60, 351.10, ema5=350.45, ema10=350.05, bar_at="2026-07-20T10:20:00+05:30"),
        # Spike candle — peak 351.95 = +1.85 = 3.7R; close 350.80 = 1.4R; low pierces EMA5
        Bar(351.40, 351.95, 350.50, 350.80, ema5=350.55, ema10=350.20, bar_at="2026-07-20T10:30:00+05:30"),
        # Exit-triggering candle — close below EMA5
        Bar(350.75, 350.90, 350.30, 350.40, ema5=350.50, ema10=350.25, bar_at="2026-07-20T10:40:00+05:30"),
    ]
    return trade, bars


def policybzr_giveback() -> CaseFixture:
    """POLICYBZR-style: ~3.5R MFE then near-scratch on slow trail.

    Anchored to MAE report (entry 1580, risk EMA10 3.61, MFE 12.5≈3.46R,
    exit ≈1580.2). Peak bar spikes to 3.46R then same-bar closes ≤1.5R so
    C2 can fire; fade continues to EMA5 scratch for baseline.
    """
    trade = TradeSpec(
        symbol="POLICYBZR",
        direction="LONG",
        entry=1580.0,
        risk_pts=3.61,
        session_date="2026-07-16",
        notes="MAE MFE 3.46R → exit ~0R; missed ~1590 book",
    )
    # 3.46R * 3.61 ≈ 12.5 → peak ~1592.5; same-bar close at 1.4R = 1585.05
    bars = [
        Bar(1580.0, 1584.0, 1579.0, 1583.0, ema5=1579.5, ema10=1576.5, bar_at="2026-07-16T11:45:00+05:30"),
        Bar(1583.0, 1588.0, 1582.0, 1587.0, ema5=1581.5, ema10=1578.0, bar_at="2026-07-16T11:55:00+05:30"),
        # Spike high 1592.5 (3.46R) → close 1585.0 (1.39R) — C2 same-bar window
        Bar(1587.0, 1592.5, 1584.5, 1585.0, ema5=1584.0, ema10=1580.0, bar_at="2026-07-16T12:05:00+05:30"),
        Bar(1585.0, 1586.0, 1581.0, 1582.0, ema5=1584.5, ema10=1581.0, bar_at="2026-07-16T12:25:00+05:30"),
        Bar(1582.0, 1583.0, 1579.0, 1580.2, ema5=1583.0, ema10=1581.0, bar_at="2026-07-16T13:15:00+05:30"),
    ]
    return trade, bars


def adanigreen_long_giveback() -> CaseFixture:
    """ADANIGREEN LONG give-back shape (user: 1.1–3.5R → scratch on EMA10).

    Uses planned EMA10 risk at entry (not VWAP nearer stop from MAE). Peak
    staged at ~3.5R with same-bar fade into C2 retain band, then EMA10 exit.
    """
    entry = 1564.5
    risk = 5.0  # illustrative planned EMA10 risk for ~3.5R peak narrative
    trade = TradeSpec(
        symbol="ADANIGREEN",
        direction="LONG",
        entry=entry,
        risk_pts=risk,
        session_date="2026-07-13",
        notes="High-R then EMA10 trail round-trip (research narrative)",
    )
    # 3.5R * 5 = 17.5 → peak 1582.0; same-bar close at 1.4R = 1571.5
    bars = [
        Bar(1564.5, 1570.0, 1563.0, 1569.0, ema5=1562.0, ema10=1559.5, bar_at="2026-07-13T10:00:00+05:30"),
        Bar(1569.0, 1576.0, 1568.0, 1575.0, ema5=1565.0, ema10=1561.0, bar_at="2026-07-13T10:10:00+05:30"),
        Bar(1575.0, 1582.0, 1571.0, 1571.5, ema5=1570.0, ema10=1564.0, bar_at="2026-07-13T10:20:00+05:30"),
        Bar(1571.5, 1572.0, 1566.0, 1567.0, ema5=1569.0, ema10=1565.0, bar_at="2026-07-13T10:40:00+05:30"),
        Bar(1567.0, 1568.0, 1562.0, 1563.0, ema5=1567.0, ema10=1565.0, bar_at="2026-07-13T10:50:00+05:30"),
    ]
    return trade, bars


def cholafin_round_trip() -> CaseFixture:
    """CHOLAFIN from MAE: 7.38R MFE → −4.25R PnL (extra high-R round-trip)."""
    trade = TradeSpec(
        symbol="CHOLAFIN",
        direction="LONG",
        entry=1822.1,
        risk_pts=0.8,  # MAE effective risk was tiny; keep small so MFE R is large
        session_date="2026-07-15",
        notes="MAE round_trip peak 7.38R → large loss; stress-test candidates",
    )
    # peak ≈ 1822.1 + 7.38*0.8 ≈ 1828.0; same-bar fade to ≤1.5R for C2
    bars = [
        Bar(1822.1, 1824.0, 1821.5, 1823.5, ema5=1821.0, ema10=1820.5, bar_at="2026-07-15T11:00:00+05:30"),
        Bar(1823.5, 1826.0, 1823.0, 1825.5, ema5=1822.0, ema10=1821.0, bar_at="2026-07-15T11:10:00+05:30"),
        Bar(1825.5, 1828.0, 1822.5, 1823.2, ema5=1823.5, ema10=1821.5, bar_at="2026-07-15T11:20:00+05:30"),
        Bar(1823.2, 1824.0, 1818.0, 1818.7, ema5=1823.0, ema10=1821.5, bar_at="2026-07-15T11:40:00+05:30"),
    ]
    return trade, bars


def tataelxsi_false_stop_20260720() -> CaseFixture:
    """TATAELXSI 20-Jul — stop-out then continuation (control: <2R, should NOT arm).

    Entry 3510, exit 3500.30 on SL; post-exit high 3532.50 is documented in
    notes only (not replayed) so peak stays &lt;2R during the held window.
    """
    entry = 3510.0
    risk = 8.0  # planned risk such that pullback to 3500.30 ≈ −1.2R, peak <2R
    trade = TradeSpec(
        symbol="TATAELXSI",
        direction="LONG",
        entry=entry,
        risk_pts=risk,
        session_date="2026-07-20",
        notes=(
            "False stop then continuation to 3532.50 post-exit — "
            "candidates must stay silent (<2R) on held bars"
        ),
    )
    bars = [
        Bar(3510.0, 3515.0, 3508.0, 3512.0, ema5=3509.0, ema10=3505.0, bar_at="2026-07-20T11:00:00+05:30"),
        Bar(3512.0, 3518.0, 3505.0, 3506.0, ema5=3510.0, ema10=3506.0, bar_at="2026-07-20T11:10:00+05:30"),
        Bar(3506.0, 3508.0, 3499.0, 3500.30, ema5=3508.0, ema10=3505.5, bar_at="2026-07-20T11:20:00+05:30"),
    ]
    return trade, bars


def all_fixtures() -> Dict[str, CaseFixture]:
    return {
        "FEDERALBNK_20260720": federalbnk_20260720(),
        "POLICYBZR_giveback": policybzr_giveback(),
        "ADANIGREEN_long_giveback": adanigreen_long_giveback(),
        "CHOLAFIN_round_trip": cholafin_round_trip(),
        "TATAELXSI_false_stop_20260720": tataelxsi_false_stop_20260720(),
    }
