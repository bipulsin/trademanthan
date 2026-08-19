#!/usr/bin/env python3
"""Fetch candles → run HA Momentum backtest → write hamoment.html."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.fetch_candles import main as fetch_main
from backtest.generate_report import main as report_main
from backtest.run_backtest import main as backtest_main


def main() -> None:
    fetch_main()
    backtest_main()
    report_main()


if __name__ == "__main__":
    main()
