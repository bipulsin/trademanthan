#!/usr/bin/env python3
"""Fetch Nifty if needed → run HA Momentum v3 variants → write hamoment.html."""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.fetch_nifty_candles import main as nifty_main
from backtest.generate_report_v3 import main as report_main
from backtest.run_backtest_v3 import main as backtest_main


def main() -> None:
    src = ROOT / "frontend" / "public" / "hamoment.html"
    archive = ROOT / "frontend" / "public" / "hamoment_v2.html"
    if src.exists() and not archive.exists():
        shutil.copy2(src, archive)
    nifty_main()
    backtest_main()
    report_main()


if __name__ == "__main__":
    main()
