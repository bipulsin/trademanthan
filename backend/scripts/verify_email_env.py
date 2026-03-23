#!/usr/bin/env python3
"""
Verify that .env provides variables needed for:
- Daily health report email (health_monitor.send_daily_report_email)
- ChartInk ranking CSV email (chartink_ranking_email)

Run from repo root:
  PYTHONPATH=. python3 backend/scripts/verify_email_env.py

Exit 0 if all required keys are non-empty; exit 1 otherwise.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import backend.env_bootstrap  # noqa: F401


def main() -> int:
    # SMTP_PORT defaults in code (25 or 587) but server host/user/password are required for TLS auth
    daily = ["ALERT_EMAIL", "SMTP_SERVER", "SMTP_USER", "SMTP_PASSWORD"]
    ranking = ["SMTP_SERVER", "SMTP_USER", "SMTP_PASSWORD"]
    # CHARTINK_RANKING_EMAIL defaults to tradentical@gmail.com if unset

    print("=== TradeManthan email env (values hidden) ===")
    ok = True
    for k in sorted(set(daily + ranking + ["SMTP_FROM_EMAIL", "CHARTINK_RANKING_EMAIL", "SMTP_PORT"])):
        v = os.getenv(k)
        if v is None or not str(v).strip():
            if k in ("CHARTINK_RANKING_EMAIL", "SMTP_FROM_EMAIL", "SMTP_PORT"):
                print(f"  {k}: (optional / has default)")
            elif k in daily:
                print(f"  {k}: MISSING (required for daily report email)")
                ok = False
            elif k in ranking:
                print(f"  {k}: MISSING (required for ranking CSV SMTP)")
                ok = False
        else:
            print(f"  {k}: SET")

    if not ok:
        print(
            "\nFix: edit repo root `.env` (see backend/env.example), uncomment and set "
            "ALERT_EMAIL and SMTP_* then restart: sudo systemctl restart trademanthan-backend",
            file=sys.stderr,
        )
        return 1
    print("\nOK: email-related variables present.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
