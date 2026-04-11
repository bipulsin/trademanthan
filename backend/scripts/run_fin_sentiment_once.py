#!/usr/bin/env python3
"""
Run fin sentiment job once (off schedule) with INFO logs to smart_future_algo.log and stdout.

Usage (repo root):
  PYTHONPATH=. python backend/scripts/run_fin_sentiment_once.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOG_PATH = ROOT / "logs" / "smart_future_algo.log"


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    for name in (
        "backend.services.fin_sentiment_job",
        "backend.services.nse_corporate_client",
    ):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.setLevel(logging.INFO)
        lg.addHandler(fh)
        lg.addHandler(sh)
        lg.propagate = False


def main() -> int:
    _setup_logging()
    log = logging.getLogger("backend.services.fin_sentiment_job")
    log.info("[fin_sentiment][S00] manual_run script=run_fin_sentiment_once.py log_file=%s", LOG_PATH)
    from backend.services.fin_sentiment_job import run_fin_sentiment_job

    out = run_fin_sentiment_job()
    print(json.dumps(out, default=str), flush=True)
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
