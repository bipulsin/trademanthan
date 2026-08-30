#!/usr/bin/env python3
"""Run remaining Breakfast spot-proxy history months (resume batch)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.breakfast_strategy.history import load_history, run_spot_proxy_month

MONTHS = ["2025-12", "2025-11", "2025-10", "2025-09", "2025-08", "2025-07", "2025-06", "2026-05"]
LOG = Path("/home/ubuntu/trademanthan/data/breakfast_remaining_host.log")


def log(msg: str) -> None:
    line = f"{msg}\n"
    print(line, end="")
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line)


def main() -> int:
    log("=== Remaining months batch started ===")
    hist = load_history()
    complete = {
        m.get("period_label")
        for m in (hist.get("months") or [])
        if m.get("status") == "complete"
    }
    for month in MONTHS:
        if month in complete:
            log(f"SKIP {month} (already complete)")
            continue
        log(f"=== Running {month} ===")
        try:
            r = run_spot_proxy_month(month, persist_db=True, force_fetch=False)
            sm = r.get("summary") or {}
            log(
                f"{r.get('period_label')} {r.get('status')} "
                f"trades={sm.get('total_trades')} pnl={sm.get('total_pnl_inr')} "
                f"err={r.get('error') or ''}"
            )
        except Exception as e:
            log(f"EXCEPTION {month}: {e}")
    log("=== Remaining months batch finished ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
