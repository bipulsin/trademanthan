#!/usr/bin/env python3
"""
Backtest validation: BEL FUT 30 JUN 26 @ 2026-06-23 10:15 scan.

Compares legacy picker assumptions vs hardened rules (#1–#9).
Run: python scripts/validate_bel_jun23_backtest.py
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# Production candle snapshot (5m IST) for BEL 2026-06-23
M5 = [
    ("10:00", 431.2, 431.45),
    ("10:05", 432.2, 432.2),
    ("10:10", 432.7, 432.95),
    ("10:15", 431.7, 432.9),
    ("10:20", 431.35, 431.8),
    ("10:25", 429.85, 431.35),
    ("10:30", 430.0, 430.1),
]

M15_VWAP_AT_1015 = 431.0831  # 5 completed 15m buckets through 10:15 label
SENTIMENT = -0.604


def legacy_long_qualifies(gate_price: float, vwap: float, m15_close: float) -> bool:
    return gate_price > vwap and m15_close > vwap


def new_long_qualifies(
    gate_price: float,
    vwap: float,
    m15_close: float,
    closes_5m: list[float],
    sentiment: float,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if sentiment < -0.4:
        reasons.append("sentiment_blocks_long")
    if not (gate_price > vwap and m15_close > vwap):
        reasons.append("vwap_gate_fail")
    if len(closes_5m) >= 2 and not all(c > vwap for c in closes_5m[-2:]):
        reasons.append("vwap_two_bar_fail")
    if gate_price <= vwap:
        reasons.append("entry_below_vwap")
    return (len(reasons) == 0, reasons)


def short_opportunity_after_break(scan_high: float) -> dict:
    """Afternoon move: price never reclaimed 10:15 high after 10:25 breakdown."""
    post = [c for _, c, h in M5 if h <= scan_high]
    highs_after = [h for _, _, h in M5[4:]]
    max_high_after = max(highs_after) if highs_after else 0
    return {
        "scan_high": scan_high,
        "max_high_after_1025": max_high_after,
        "reclaimed_scan_high": max_high_after >= scan_high,
        "best_short_window": "10:25+",
        "low_by_1030": min(c for t, c, _ in M5 if t >= "10:25"),
    }


def main() -> None:
    gate_legacy = 431.7  # 5m close at 10:15 (picker gate)
    entry_legacy_1m = 430.3  # old 1m persist price
    m15_close_at_scan = 432.7  # last completed 15m at 10:15 scan
    closes = [c for _, c, _ in M5]

    leg_ok = legacy_long_qualifies(gate_legacy, M15_VWAP_AT_1015, m15_close_at_scan)
    new_ok, new_reasons = new_long_qualifies(
        gate_legacy, M15_VWAP_AT_1015, m15_close_at_scan, closes[:4], SENTIMENT
    )

    short_info = short_opportunity_after_break(432.9)

    print("=== BEL Jun-23 10:15 backtest validation ===")
    print(f"Legacy LONG qualifies (gate/m15): {leg_ok}")
    print(f"Legacy entry 1m ({entry_legacy_1m}) < VWAP ({M15_VWAP_AT_1015:.2f}): {entry_legacy_1m < M15_VWAP_AT_1015}")
    print(f"New rules LONG qualifies: {new_ok}  reasons={new_reasons}")
    print(f"Sentiment {SENTIMENT}: blocks LONG under new rules")
    print(f"Short opportunity: {short_info}")
    print()
    if leg_ok and not new_ok:
        print("PASS: New rules would have blocked the false LONG.")
    else:
        print("Review: unexpected qualification outcome — check inputs.")


if __name__ == "__main__":
    main()
