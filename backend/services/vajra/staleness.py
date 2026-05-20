"""Vajra rating staleness detection — post-deploy and schema/engine freshness."""
from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

DEPLOY_EPOCH_FILE = Path("/tmp/trademanthan_deploy_epoch")
VAJRA_STALE_MAX_AGE_SEC = int(os.getenv("VAJRA_STALE_MAX_AGE_SEC", "360"))
VAJRA_MISSING_EVS_RATIO = float(os.getenv("VAJRA_MISSING_EVS_RATIO", "0.05"))


def read_deploy_epoch() -> Optional[datetime]:
    """UTC-aware deploy timestamp written by deploy_backend.sh after restart."""
    try:
        if not DEPLOY_EPOCH_FILE.is_file():
            return None
        raw = DEPLOY_EPOCH_FILE.read_text().strip()
        if not raw:
            return None
        sec = float(raw)
        return datetime.fromtimestamp(sec, tz=IST)
    except (OSError, ValueError, TypeError):
        return None


def write_deploy_epoch(when: Optional[datetime] = None) -> None:
    ts = when or datetime.now(IST)
    DEPLOY_EPOCH_FILE.write_text(str(int(ts.timestamp())))


def _aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _missing_evs_ratio(rows: List[Dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    missing = sum(1 for r in rows if r.get("evs_score") is None)
    return missing / len(rows)


def is_vajra_ratings_stale(
    rows: List[Dict[str, Any]],
    updated_at: Optional[datetime],
    *,
    now: Optional[datetime] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Return (is_stale, reason_code).
    Reasons: no_rows, no_timestamp, age, pre_deploy, missing_evs
    """
    if not rows:
        return True, "no_rows"

    ts_now = _aware(now or datetime.now(IST))

    if updated_at is None:
        return True, "no_timestamp"

    updated = _aware(updated_at)
    age_sec = max(0, int((ts_now - updated).total_seconds()))
    if age_sec > VAJRA_STALE_MAX_AGE_SEC:
        return True, "age"

    deploy_at = read_deploy_epoch()
    if deploy_at is not None and updated < deploy_at:
        return True, "pre_deploy"

    if _missing_evs_ratio(rows) >= VAJRA_MISSING_EVS_RATIO:
        return True, "missing_evs"

    return False, None


def is_vajra_db_snapshot_stale(
    *,
    row_count: int,
    missing_evs_count: int,
    updated_at: Optional[datetime],
    now: Optional[datetime] = None,
) -> Tuple[bool, Optional[str]]:
    """Lightweight staleness check using DB aggregates (no full row load)."""
    if row_count <= 0:
        return True, "no_rows"

    ts_now = _aware(now or datetime.now(IST))

    if updated_at is None:
        return True, "no_timestamp"

    updated = _aware(updated_at)
    age_sec = max(0, int((ts_now - updated).total_seconds()))
    if age_sec > VAJRA_STALE_MAX_AGE_SEC:
        return True, "age"

    deploy_at = read_deploy_epoch()
    if deploy_at is not None and updated < deploy_at:
        return True, "pre_deploy"

    if row_count > 0 and (missing_evs_count / row_count) >= VAJRA_MISSING_EVS_RATIO:
        return True, "missing_evs"

    return False, None


def should_run_post_deploy_refresh(
    rows: List[Dict[str, Any]],
    updated_at: Optional[datetime],
) -> Tuple[bool, Optional[str]]:
    """Background persist job after deploy/startup — same rules as API staleness."""
    return is_vajra_ratings_stale(rows, updated_at)
