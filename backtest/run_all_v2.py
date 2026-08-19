#!/usr/bin/env python3
"""Fetch Nifty 15m → run HA Momentum v2 variants → write hamoment.html."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.fetch_nifty_candles import main as nifty_main
from backtest.generate_report_v2 import main as report_main
from backtest.run_backtest_v2 import main as backtest_main


def main() -> None:
    nifty_main()
    backtest_main()
    report_main()


if __name__ == "__main__":
    main()
