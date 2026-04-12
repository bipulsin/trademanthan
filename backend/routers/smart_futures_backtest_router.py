"""
Read-only API for Smart Futures backtest results (table ``backtest_smart_future``).

Does not mount in the left menu; page is ``/backtestsmart.html``.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.smart_futures_backtest.engine import run_backtest_date_range

logger = logging.getLogger(__name__)

router = APIRouter(tags=["smart-futures-backtest"])


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


def _row_to_dict(r: Any) -> Dict[str, Any]:
    out = dict(r)
    sd = out.get("session_date")
    if sd is not None and hasattr(sd, "isoformat"):
        out["session_date"] = sd.isoformat()
    sa = out.get("simulated_asof")
    if sa is not None and hasattr(sa, "isoformat"):
        out["simulated_asof"] = sa.isoformat()
    for k in (
        "obv_slope",
        "volume_surge",
        "adx_14",
        "atr_14",
        "atr5_14_ratio",
        "renko_momentum",
        "ha_trend",
        "macd_div",
        "rsi_div",
        "stoch_div",
        "cms",
        "final_cms",
        "sector_score",
        "combined_sentiment",
        "entry_price",
        "sl_price",
        "target_price",
        "vix_at_scan",
    ):
        v = out.get(k)
        if v is not None:
            out[k] = float(v)
    return out


@router.get("/rows")
def list_backtest_rows(
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
    limit: int = Query(5000, ge=1, le=20000),
):
    """All backtest rows, newest first (no session_date filter)."""
    try:
        rows = db.execute(
            text(
                """
                SELECT id, session_date, simulated_asof, scan_time_label,
                       stock, fut_symbol, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, cms, atr5_14_ratio,
                       vix_at_scan, sentiment_source, sentiment_run_at_match_count
                FROM backtest_smart_future
                ORDER BY simulated_asof DESC NULLS LAST, id DESC
                LIMIT :lim
                """
            ),
            {"lim": limit},
        ).mappings().all()
    except Exception as e:
        logger.warning("backtest /rows failed: %s", e)
        return {"groups": [], "error": str(e)}

    serialized = [_row_to_dict(r) for r in rows]
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in serialized:
        key = str(r.get("simulated_asof") or "")[:16]
        if len(key) < 16:
            key = key or "—"
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(r)
    # Preserve descending simulated_asof order of first appearance
    ordered_keys: List[str] = []
    seen = set()
    for r in serialized:
        k = str(r.get("simulated_asof") or "")[:16] or "—"
        if k not in seen:
            seen.add(k)
            ordered_keys.append(k)
    groups = [{"simulated_asof": k, "rows": buckets[k]} for k in ordered_keys]
    return {"groups": groups, "rows": serialized}


class BacktestRunBody(BaseModel):
    from_date: str = Field(..., description="YYYY-MM-DD")
    to_date: str = Field(..., description="YYYY-MM-DD")
    times: Optional[List[str]] = Field(default=None, description='IST labels e.g. ["09:30","10:30"]')


@router.post("/run")
def post_run_backtest(
    body: BacktestRunBody,
    admin: User = Depends(_require_admin),
    db: Session = Depends(get_db),
):
    """
    Admin-only: run backtest for a date range (synchronous; use CLI for very long ranges).
    """
    from datetime import date as date_cls

    try:
        d0 = date_cls.fromisoformat(body.from_date.strip())
        d1 = date_cls.fromisoformat(body.to_date.strip())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date: {e}") from e
    if d0 > d1:
        raise HTTPException(status_code=400, detail="from_date must be <= to_date")
    times = tuple(body.times) if body.times else ("09:30", "10:30")
    for t in times:
        if len(t) < 4 or ":" not in t:
            raise HTTPException(status_code=400, detail=f"Invalid time label: {t}")
    try:
        out = run_backtest_date_range(db, d0, d1, times, throttle_sec=0.04)
        if out.get("error"):
            raise HTTPException(status_code=400, detail=out["error"])
        return {"success": True, **out}
    except Exception as e:
        logger.exception("backtest /run failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e
