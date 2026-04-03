#!/usr/bin/env python3
"""
One-time backfill for users.last_login_* fields from available historical logs.

Best-effort logic:
- Parse app logs for lines containing "Google OAuth verification for user: <email>".
- If an auth request IP appears shortly before that event, attach it.
- Update users table only when parsed timestamp is newer than existing last_login_at.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import backend.env_bootstrap  # noqa: F401
from backend.database import SessionLocal
import backend.models as models


TS_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
EMAIL_LOGIN_RE = re.compile(r"Google OAuth verification for user:\s*([^\s]+)", re.IGNORECASE)
AUTH_IP_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}).*?(POST|GET)\s+/(?:api/)?auth/(google|google-verify|google-code)", re.IGNORECASE)


def _parse_ts(line: str) -> Optional[datetime]:
    m = TS_PREFIX_RE.search(line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _candidate_logs() -> list[Path]:
    paths = [
        Path("/home/ubuntu/trademanthan/logs/trademanthan.log"),
        Path("/home/ubuntu/trademanthan/logs/trademanthan.log.1"),
        Path("/home/ubuntu/trademanthan/logs/trademanthan.log.2"),
        Path("/Users/bipulsahay/TradeManthan/logs/trademanthan.log"),
    ]
    return [p for p in paths if p.exists() and p.is_file()]


def parse_login_events() -> Dict[str, Tuple[datetime, Optional[str]]]:
    # email -> (latest_login_ts, ip_or_none)
    events: Dict[str, Tuple[datetime, Optional[str]]] = {}
    last_auth_ip: Optional[Tuple[datetime, str]] = None

    for log_path in _candidate_logs():
        try:
            with log_path.open("r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    ts = _parse_ts(line)

                    auth_ip_match = AUTH_IP_RE.search(line)
                    if ts and auth_ip_match:
                        last_auth_ip = (ts, auth_ip_match.group(1))

                    m = EMAIL_LOGIN_RE.search(line)
                    if not m or not ts:
                        continue
                    email = m.group(1).strip().lower()
                    ip = None
                    if last_auth_ip:
                        delta = abs((ts - last_auth_ip[0]).total_seconds())
                        if delta <= 120:
                            ip = last_auth_ip[1]

                    prev = events.get(email)
                    if not prev or ts > prev[0]:
                        events[email] = (ts, ip)
        except Exception:
            continue

    return events


def main():
    events = parse_login_events()
    if not events:
        print("No historical login events found in available logs.")
        return

    db = SessionLocal()
    updated = 0
    try:
        for email, (ts, ip) in events.items():
            user = db.query(models.User).filter(models.User.email == email).first()
            if not user:
                continue
            if user.last_login_at and user.last_login_at >= ts:
                continue
            user.last_login_at = ts
            if ip:
                user.last_login_ip = ip[:64]
                user.last_activity_ip = ip[:64]
            if not user.last_page_visited:
                user.last_page_visited = "login.html"
            if not user.last_page_visited_at:
                user.last_page_visited_at = ts
            updated += 1
        db.commit()
        print(f"Backfill complete. Updated users: {updated}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
