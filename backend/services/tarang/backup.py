"""Daily pg_dump of tarang_* tables. Dry-run supported. Never deletes forward-test rows."""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple
from urllib.parse import ParseResult
from urllib.parse import urlparse

from backend.database import DATABASE_URL
from backend.services.tarang.schema import ensure_tarang_tables

BACKUP_DIR = Path(os.getenv("TARANG_BACKUP_DIR") or "/tmp/tarang_backups")
RETENTION_DAYS = int(os.getenv("TARANG_BACKUP_RETENTION_DAYS") or "30")
TABLES = [
    "tarang_settings",
    "tarang_iv_snapshots",
    "tarang_feed_health",
    "tarang_event_calendar",
    "tarang_candidates",
    "tarang_rejections",
    "tarang_trades",
    "tarang_orders",
    "tarang_alerts",
    "tarang_trade_events",
    "tarang_fills",
    "tarang_chain_snapshots",
    "tarang_alert_dedupe",
    "tarang_data_gaps",
    "tarang_liquidity_probes",
    "tarang_hist_imports",
    "tarang_hist_eod",
    "tarang_signal_snapshots",
    "tarang_skipped_signals",
    "tarang_shadow_orders",
    "tarang_live_arming",
    "tarang_heartbeat",
    "tarang_job_runs",
    "tarang_hist_underlying",
    "tarang_option_archive",
]


def _pg_env() -> Tuple[Dict[str, str], ParseResult]:
    u = urlparse(DATABASE_URL.replace("postgresql+psycopg2", "postgresql"))
    env = dict(os.environ)
    if u.password:
        env["PGPASSWORD"] = u.password
    return env, u


def run_backup(*, dry_run: bool = False) -> Dict[str, Any]:
    env, u = _pg_env()
    if not dry_run:
        ensure_tarang_tables()
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = BACKUP_DIR / f"tarang_{stamp}.dump"
    args = [
        "pg_dump",
        "-h",
        u.hostname or "localhost",
        "-p",
        str(u.port or 5432),
        "-U",
        u.username or "postgres",
        "-d",
        (u.path or "/trademanthan").lstrip("/"),
        "-Fc",
    ]
    for t in TABLES:
        args.extend(["-t", t])
    args.extend(["-f", str(dest)])
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "command": args[:-2] + ["-f", str(dest)],
            "dest": str(dest),
            "retention_days": RETENTION_DAYS,
            "restore": f"pg_restore -d $DATABASE_URL --data-only {dest}  # never DROP tarang_trades",
            "note": "Forward-test rows are never deleted by this script.",
        }
    try:
        subprocess.check_call(args, env=env, timeout=300)
        size = dest.stat().st_size if dest.exists() else 0
        # retention
        cutoff = datetime.now(timezone.utc).timestamp() - RETENTION_DAYS * 86400
        removed = []
        for p in BACKUP_DIR.glob("tarang_*.dump"):
            if p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
                removed.append(p.name)
        return {"ok": True, "dest": str(dest), "bytes": size, "removed": removed, "retention_days": RETENTION_DAYS}
    except FileNotFoundError:
        return {"ok": False, "error": "pg_dump_not_found", "dry_run_hint": run_backup(dry_run=True)}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}
