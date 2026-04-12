"""
Smart Futures routes: admin config (JSON file) + smart_futures_daily list / order.

Mounted at /api/smart-futures and /smart-futures for admintwc.js and smartfuture.html.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.smart_futures_picker.job import compute_atr5_14_ratio_for_session
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["smart-futures"])

_CONFIG_LOCK = threading.Lock()
_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
_CONFIG_PATH = _DATA_DIR / "smart_futures_admin_stub.json"

_DEFAULT: Dict[str, Any] = {
    "live_enabled": False,
    "position_size": 1,
    "partial_exit_enabled": False,
    "brick_atr_period": 10,
    "brick_atr_override": None,
}


def _require_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)) -> User:
    return get_user_from_token(token, db)


def _require_admin(user: User = Depends(_require_user)) -> User:
    if (getattr(user, "is_admin", None) or "").strip() != "Yes":
        raise HTTPException(status_code=403, detail="Administrator only")
    return user


def _load_unlocked() -> Dict[str, Any]:
    out = dict(_DEFAULT)
    try:
        if _CONFIG_PATH.is_file():
            raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for k in _DEFAULT:
                    if k in raw:
                        out[k] = raw[k]
    except Exception as e:
        logger.warning("smart_futures_stub: read config failed: %s", e)
    return out


def _read_config() -> Dict[str, Any]:
    with _CONFIG_LOCK:
        return _load_unlocked()


class SmartFuturesConfigUpdate(BaseModel):
    live_enabled: Optional[bool] = None
    position_size: Optional[int] = Field(None, ge=1, le=3)
    partial_exit_enabled: Optional[bool] = None
    brick_atr_period: Optional[int] = Field(None, ge=2, le=99)
    brick_atr_override: Optional[float] = None


@router.get("/config")
def get_sf_config_stub(user: User = Depends(_require_user)):
    """Admin UI + future screener: persisted parameters (file-backed, not DB)."""
    return _read_config()


@router.put("/config")
def put_sf_config_stub(body: SmartFuturesConfigUpdate, admin: User = Depends(_require_admin)):
    patch = body.model_dump(exclude_unset=True)
    with _CONFIG_LOCK:
        cur = _load_unlocked()
        cur.update(patch)
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(cur, indent=2), encoding="utf-8")
    logger.info("smart_futures_stub: config saved by admin id=%s", getattr(admin, "id", None))
    return cur


def _atr_ratio_needs_backfill(v: Any) -> bool:
    if v is None:
        return True
    try:
        f = float(v)
        return not (f == f)  # NaN
    except (TypeError, ValueError):
        return True


def _row_to_dict(r: Any) -> Dict[str, Any]:
    out = dict(r)
    sd = out.get("session_date")
    if sd is not None and hasattr(sd, "isoformat"):
        out["session_date"] = sd.isoformat()
    ea = out.get("entry_at")
    if ea is not None and hasattr(ea, "isoformat"):
        out["entry_at"] = ea.isoformat()
    for k in (
        "cms",
        "final_cms",
        "sector_score",
        "combined_sentiment",
        "entry_price",
        "sl_price",
        "target_price",
        "buy_price",
        "atr5_14_ratio",
    ):
        v = out.get(k)
        if v is not None:
            out[k] = float(v)
    return out


@router.get("/daily")
def get_smart_futures_daily(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    """Today's Trend: rows for effective session_date (IST window 9:00 → next session 08:59)."""
    sd = effective_session_date_ist_for_trend()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, session_date, scan_trigger,
                       cms, atr5_14_ratio
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                """
            ),
            {"sd": sd},
        ).mappings().all()
    except Exception as e:
        logger.warning("smart_futures /daily query failed: %s", e)
        return {
            "session_date": sd.isoformat(),
            "groups": [],
            "rows": [],
            "error": str(e),
        }

    serialized = [_row_to_dict(r) for r in rows]

    # Backfill atr5_14_ratio when NULL (e.g. rows created before the column existed).
    # Same 5m session logic as the picker; persist so this is typically a one-time fill per row.
    need_upstox = any(
        _atr_ratio_needs_backfill(r.get("atr5_14_ratio"))
        and (str(r.get("fut_instrument_key") or "").strip())
        for r in serialized
    )
    if need_upstox:
        try:
            upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        except Exception as e:
            logger.warning("smart_futures /daily ATR backfill: Upstox init failed: %s", e)
            upstox = None
        if upstox is not None:
            for r in serialized:
                if not _atr_ratio_needs_backfill(r.get("atr5_14_ratio")):
                    continue
                ikey = str(r.get("fut_instrument_key") or "").strip()
                if not ikey:
                    continue
                rid = r.get("id")
                if rid is None:
                    continue
                ratio = compute_atr5_14_ratio_for_session(upstox, ikey, sd)
                if ratio is None:
                    continue
                try:
                    db.execute(
                        text(
                            """
                            UPDATE smart_futures_daily
                            SET atr5_14_ratio = :ratio, updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id AND session_date = :sd
                            """
                        ),
                        {"id": int(rid), "sd": sd, "ratio": float(ratio)},
                    )
                    db.commit()
                except Exception as ex:
                    logger.warning("smart_futures /daily ATR backfill update id=%s: %s", rid, ex)
                    db.rollback()
                    continue
                r["atr5_14_ratio"] = float(ratio)

    for r in serialized:
        r.pop("fut_instrument_key", None)

    buckets: OrderedDict[str, List[Dict[str, Any]]] = OrderedDict()
    for r in serialized:
        ea = r.get("entry_at") or ""
        bucket = ea[:16] if isinstance(ea, str) and len(ea) >= 16 else (ea or "—")
        if bucket not in buckets:
            buckets[bucket] = []
        buckets[bucket].append(r)
    groups = [{"entry_at": k, "rows": v} for k, v in buckets.items()]
    return {
        "session_date": sd.isoformat(),
        "groups": groups,
        "rows": serialized,
    }


@router.post("/daily/{row_id}/order")
def post_smart_futures_daily_order(
    row_id: int,
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    """Mark row as bought and store LTP at click time (Upstox quote)."""
    sd = effective_session_date_ist_for_trend()
    row = db.execute(
        text(
            """
            SELECT id, fut_instrument_key, session_date, order_status
            FROM smart_futures_daily
            WHERE id = :id
            """
        ),
        {"id": row_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Row not found")
    rsd = row.get("session_date")
    if hasattr(rsd, "isoformat"):
        rsd = rsd.isoformat()
    if str(rsd) != sd.isoformat():
        raise HTTPException(status_code=400, detail="Pick is outside the current session window")
    if (row.get("order_status") or "").strip().lower() == "bought":
        raise HTTPException(status_code=400, detail="Already marked as bought")

    ikey = (row.get("fut_instrument_key") or "").strip()
    if not ikey:
        raise HTTPException(status_code=400, detail="Missing instrument key")

    try:
        us = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        q = us.get_market_quote_by_key(ikey) or {}
        ltp = float(q.get("last_price") or 0)
    except Exception as e:
        logger.error("smart_futures order LTP failed: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail=f"LTP fetch failed: {e}") from e
    if ltp <= 0:
        raise HTTPException(status_code=502, detail="Could not read last price from broker")

    db.execute(
        text(
            """
            UPDATE smart_futures_daily
            SET order_status = 'bought', buy_price = :ltp, updated_at = CURRENT_TIMESTAMP
            WHERE id = :id AND (order_status IS NULL OR LOWER(TRIM(order_status)) <> 'bought')
            """
        ),
        {"id": row_id, "ltp": ltp},
    )
    db.commit()
    logger.info("smart_futures order: user=%s row=%s buy_price=%s", getattr(user, "id", None), row_id, ltp)
    return {"success": True, "id": row_id, "order_status": "bought", "buy_price": ltp}
