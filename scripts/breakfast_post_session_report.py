#!/usr/bin/env python3
"""Manual Breakfast post-session report (stdout or Telegram). Usage on paperclip:
  docker compose exec app python scripts/breakfast_post_session_report.py
  docker compose exec app python scripts/breakfast_post_session_report.py --send-telegram
  docker compose exec app python scripts/breakfast_post_session_report.py --date 2026-09-01
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.env_bootstrap  # noqa: F401

from backend.services.breakfast_monitor import (
    build_post_session_report,
    collect_breakfast_preflight_status,
    format_preflight_telegram,
    run_breakfast_morning_telegram_ping,
    run_breakfast_post_session_telegram_report,
)


def main() -> int:
    p = argparse.ArgumentParser(description="Breakfast monitor report / preflight")
    p.add_argument("--date", help="Session date YYYY-MM-DD")
    p.add_argument("--send-telegram", action="store_true", help="Send to @TradeWithCTO")
    p.add_argument("--preflight", action="store_true", help="09:00 preflight instead of post-session")
    args = p.parse_args()

    if args.preflight:
        if args.send_telegram:
            run_breakfast_morning_telegram_ping()
            print("Preflight Telegram sent (if token configured).")
            return 0
        status = collect_breakfast_preflight_status()
        print(format_preflight_telegram(status))
        return 0

    if args.send_telegram:
        run_breakfast_post_session_telegram_report()
        print("Post-session Telegram sent (if token configured).")
        return 0

    print(build_post_session_report(args.date))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
