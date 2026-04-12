"""Dedicated file logger for Smart Futures backtest (does not alter smart_future_algo logging)."""
from __future__ import annotations

import logging
from pathlib import Path


def get_backtest_logger() -> logging.Logger:
    log_dir = Path(__file__).resolve().parents[3] / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "smart_futures_backtest.log"
    name = "smart_futures_backtest"
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    logger.addHandler(fh)
    logger.propagate = False
    return logger
