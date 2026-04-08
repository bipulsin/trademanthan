#!/usr/bin/env python3
"""
Sample backtest harness (offline): trend day vs chop day close paths.
Run: python backend/scripts/smart_futures_backtest_sample.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.services.smart_futures.renko_engine import build_traditional_renko, renko_structure_filter_long
from backend.services.smart_futures.scanner import compute_score


def run_case(name: str, closes: list[float], brick: float) -> None:
    bricks = build_traditional_renko(closes, brick)
    ok, reason = renko_structure_filter_long(bricks)
    score = compute_score("LONG", bricks, vol_spike=True, momentum_ok=True)
    print(f"\n=== {name} ===")
    print(f"  bricks: {len(bricks)}  structure_ok={ok} ({reason})  score={score}")


def main() -> None:
    brick = 10.0
    trend = [100 + i * 4 for i in range(60)]
    chop = [100.0]
    x = 100.0
    for i in range(80):
        x += 8 if i % 2 == 0 else -8
        chop.append(x)
    run_case("trend_day", trend, brick)
    run_case("chop_day", chop, brick)


if __name__ == "__main__":
    main()
