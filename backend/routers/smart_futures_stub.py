"""
Smart Futures routes: admin config (JSON file) + smart_futures_daily list / order.

Mounted at /api/smart-futures and /smart-futures for admintwc.js and smartfuture.html.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services import smart_futures_config as sfc
from backend.services.smart_futures_exit import (
    evaluate_exit_with_profit_protection,
)
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.smart_futures_session_utils import compute_atr5_14_ratio_for_session
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

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
    out = _read_config()
    tn = int(getattr(sfc, "SMART_FUTURES_PICK_SELECTION_TOP_N", 6))
    pl, ps = sfc.buildup_selection_long_short_caps(tn)
    out["pick_selection_top_n"] = tn
    out["pick_selection_long_cap"] = pl
    out["pick_selection_short_cap"] = ps
    out["min_long_buildup_selection"] = int(getattr(sfc, "MIN_LONG_BUILDUP_SELECTION", 3))
    out["pick_selection_rule"] = (
        "Each scan: at least min_long_buildup LONG_BUILDUP (when available), plus SHORT_BUILDUP "
        "up to top_n//2, with long+short trimmed to top_n budget (same as futures backtester). "
        "Rows saved respect MAX_OPEN_POSITIONS free slots."
    )
    out["max_open_positions"] = int(getattr(sfc, "MAX_OPEN_POSITIONS", 3))
    return out


@router.put("/config")
def put_sf_config_stub(body: SmartFuturesConfigUpdate, admin: User = Depends(_require_admin)):
    patch = body.model_dump(exclude_unset=True)
    with _CONFIG_LOCK:
        cur = _load_unlocked()
        cur.update(patch)
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        _CONFIG_PATH.write_text(json.dumps(cur, indent=2), encoding="utf-8")
    logger.info("smart_futures_stub: config saved by admin id=%s", getattr(admin, "id", None))
    # Re-merge picker caps so the response matches GET /config
    tn = int(getattr(sfc, "SMART_FUTURES_PICK_SELECTION_TOP_N", 6))
    pl, ps = sfc.buildup_selection_long_short_caps(tn)
    cur["pick_selection_top_n"] = tn
    cur["pick_selection_long_cap"] = pl
    cur["pick_selection_short_cap"] = ps
    cur["min_long_buildup_selection"] = int(getattr(sfc, "MIN_LONG_BUILDUP_SELECTION", 3))
    cur["pick_selection_rule"] = (
        "Each scan: at least min_long_buildup LONG_BUILDUP (when available), plus SHORT_BUILDUP "
        "up to top_n//2, with long+short trimmed to top_n budget (same as futures backtester). "
        "Rows saved respect MAX_OPEN_POSITIONS free slots."
    )
    cur["max_open_positions"] = int(getattr(sfc, "MAX_OPEN_POSITIONS", 3))
    return cur


def _missing_db_column_error(e: BaseException, column: str) -> bool:
    msg = str(e).lower()
    if column.lower() not in msg:
        return False
    return (
        "does not exist" in msg
        or "unknown column" in msg
        or "no such column" in msg
        or ("column" in msg and "not" in msg)
    )


_SQL_DAILY_FULL = """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, sell_price, sell_time, hold_type, session_date, scan_trigger,
                       cms, atr5_14_ratio,
                       signal_tier, tier_multiplier, calculated_lots, stop_stage, current_stop_price,
                       oi_signal, oi_gate_passed, time_filter_passed, regime_filter_passed, ema_slope_norm,
                       premkt_rank, oi_heat_rank
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                """
_SQL_DAILY_NO_RANK = """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, sell_price, sell_time, hold_type, session_date, scan_trigger,
                       cms, atr5_14_ratio,
                       signal_tier, tier_multiplier, calculated_lots, stop_stage, current_stop_price,
                       oi_signal, oi_gate_passed, time_filter_passed, regime_filter_passed, ema_slope_norm
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                """
_SQL_DAILY_NO_SELL_TIME = """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, sell_price, hold_type, session_date, scan_trigger,
                       cms, atr5_14_ratio
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                """
_SQL_DAILY_NO_SELL_PRICE = """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, hold_type, session_date, scan_trigger,
                       cms, atr5_14_ratio
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                """
_SQL_DAILY_NO_HOLD_TYPE = """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, sell_price, sell_time, session_date, scan_trigger,
                       cms, atr5_14_ratio
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                """


def _fetch_daily_for_session(db: Session, sd: Any) -> List[Any]:
    try:
        return db.execute(text(_SQL_DAILY_FULL), {"sd": sd}).mappings().all()
    except Exception as e:
        if _missing_db_column_error(e, "premkt_rank") or _missing_db_column_error(e, "oi_heat_rank"):
            logger.warning("smart_futures /daily: premkt_rank/oi_heat_rank missing, legacy query: %s", e)
            return db.execute(text(_SQL_DAILY_NO_RANK), {"sd": sd}).mappings().all()
        if _missing_db_column_error(e, "signal_tier") or _missing_db_column_error(e, "ema_slope_norm"):
            logger.warning("smart_futures /daily: extended columns missing, using legacy query: %s", e)
            try:
                return db.execute(
                    text(
                        """
                SELECT id, fut_symbol, fut_instrument_key, side, final_cms, sector_score, combined_sentiment,
                       entry_price, sl_price, target_price, trend_continuation, entry_at,
                       order_status, buy_price, sell_price, sell_time, hold_type, session_date, scan_trigger,
                       cms, atr5_14_ratio
                FROM smart_futures_daily
                WHERE session_date = :sd
                ORDER BY entry_at DESC NULLS LAST, id DESC
                        """
                    ),
                    {"sd": sd},
                ).mappings().all()
            except Exception as e2:
                logger.warning("smart_futures /daily: legacy query failed: %s", e2)
                raise e2
        if _missing_db_column_error(e, "hold_type"):
            logger.warning("smart_futures /daily: hold_type missing, query without it: %s", e)
            return db.execute(text(_SQL_DAILY_NO_HOLD_TYPE), {"sd": sd}).mappings().all()
        if _missing_db_column_error(e, "sell_time"):
            logger.warning("smart_futures /daily: sell_time missing, query without it: %s", e)
            try:
                return db.execute(text(_SQL_DAILY_NO_SELL_TIME), {"sd": sd}).mappings().all()
            except Exception as e2:
                if _missing_db_column_error(e2, "sell_price"):
                    logger.warning("smart_futures /daily: fallback minimal query: %s", e2)
                    return db.execute(text(_SQL_DAILY_NO_SELL_PRICE), {"sd": sd}).mappings().all()
                raise
        if _missing_db_column_error(e, "sell_price"):
            logger.warning("smart_futures /daily: sell_price column missing, using query without it: %s", e)
            return db.execute(text(_SQL_DAILY_NO_SELL_PRICE), {"sd": sd}).mappings().all()
        raise


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
    st = out.get("sell_time")
    if st is not None and hasattr(st, "isoformat"):
        out["sell_time"] = st.isoformat()
    for k in (
        "cms",
        "final_cms",
        "sector_score",
        "combined_sentiment",
        "entry_price",
        "sl_price",
        "target_price",
        "buy_price",
        "sell_price",
        "atr5_14_ratio",
        "tier_multiplier",
        "ema_slope_norm",
        "current_stop_price",
    ):
        v = out.get(k)
        if v is not None:
            out[k] = float(v)
    return out


def _ist_date_from_ts(ts: str):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        dt = dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
        return dt.date()
    except Exception:
        try:
            return datetime.strptime(str(ts)[:10], "%Y-%m-%d").date()
        except Exception:
            return None


@router.get("/daily")
def get_smart_futures_daily(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    """Today's Trend: rows for effective session_date (IST window 9:15 → next session 09:14)."""
    sd = effective_session_date_ist_for_trend()
    try:
        rows = _fetch_daily_for_session(db, sd)
    except Exception as e:
        logger.warning("smart_futures /daily query failed: %s", e)
        return {
            "session_date": sd.isoformat(),
            "groups": [],
            "rows": [],
            "error": str(e),
        }

    now_ist = datetime.now(IST)
    intraday_force_exit_time = now_ist.replace(hour=15, minute=15, second=0, microsecond=0)

    serialized = [_row_to_dict(r) for r in rows]
    for r in serialized:
        r.setdefault("exit_suggested", False)
        r.setdefault("exit_reason", "")

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

    # Exit hints + profit protection state for open positions (multi-timeframe manager).
    try:
        us_exit = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    except Exception as e:
        logger.warning("smart_futures /daily exit enrich: Upstox init failed: %s", e)
        us_exit = None
    if us_exit is not None:
        for r in serialized:
            if str(r.get("order_status") or "").strip().lower() != "bought":
                continue
            try:
                ikey = str(r.get("fut_instrument_key") or "").strip()
                if not ikey:
                    continue
                raw5 = us_exit.get_historical_candles_by_instrument_key(
                    ikey, interval="minutes/5", days_back=5, range_end_date=sd
                )
                m5 = [
                    b
                    for b in sorted(raw5 or [], key=lambda x: str(x.get("timestamp") or ""))
                    if _ist_date_from_ts(str(b.get("timestamp") or "")) == sd
                ]
                if len(m5) < 3:
                    continue
                entry_px = float(r.get("buy_price") or r.get("entry_price") or 0)
                if entry_px <= 0:
                    continue
                side = str(r.get("side") or "").strip().upper()
                if side not in {"LONG", "SHORT"}:
                    continue
                entry_at = str(r.get("entry_at") or "")
                post = [
                    b for b in m5
                    if (str(b.get("timestamp") or "") >= entry_at)
                ] if entry_at else m5
                pre_m5 = [b for b in m5 if (str(b.get("timestamp") or "") < entry_at)] if entry_at else []
                if len(post) < 3:
                    post = m5
                ex = evaluate_exit_with_profit_protection(
                    side=side,
                    entry_price=entry_px,
                    entry_time=entry_at or str(post[0].get("timestamp") or ""),
                    lot_size=1,
                    m5_post_entry=post,
                    m5_pre_entry=pre_m5,
                    force_close_at_end=False,
                )
                state = ex.get("state", {}) if isinstance(ex, dict) else {}
                r["exit_suggested"] = bool(ex.get("exit"))
                r["exit_reason"] = str(ex.get("final_exit_reason") or "")
                r["hard_stop_loss"] = state.get("hard_stop_loss")
                r["breakeven_activated"] = bool(state.get("breakeven_activated"))
                r["breakeven_activation_time"] = state.get("breakeven_activation_time")
                r["profit_locking_activated"] = bool(state.get("profit_locking_activated"))
                r["profit_locking_activation_time"] = state.get("profit_locking_activation_time")
                r["profit_locking_stop_level"] = state.get("profit_locking_stop_level")
                r["trailing_stop_activated"] = bool(state.get("trailing_stop_activated"))
                r["trailing_stop_activation_time"] = state.get("trailing_stop_activation_time")
                r["initial_trailing_stop_level"] = state.get("initial_trailing_stop_level")
                r["current_trailing_stop_level"] = state.get("current_trailing_stop_level")
                r["current_active_stop_loss_level"] = state.get("current_active_stop_loss_level")
                r["max_profit_achieved"] = state.get("max_profit_achieved")
                stage = "INITIAL"
                if r["trailing_stop_activated"]:
                    stage = "TRAILING"
                elif r["profit_locking_activated"]:
                    stage = "PROFIT_LOCK"
                elif r["breakeven_activated"]:
                    stage = "BREAKEVEN"
                r["stop_stage"] = stage
                if r.get("current_active_stop_loss_level") is not None:
                    r["current_stop_price"] = float(r["current_active_stop_loss_level"])
                rid = r.get("id")
                if rid is not None:
                    db.execute(
                        text(
                            """
                            UPDATE smart_futures_daily
                            SET current_stop_price = :csp, stop_stage = :stg, updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id AND session_date = :sd
                            """
                        ),
                        {
                            "id": int(rid),
                            "sd": sd,
                            "csp": float(r.get("current_stop_price") or 0.0),
                            "stg": str(stage),
                        },
                    )
                    db.commit()
            except Exception as ex:
                logger.debug("smart_futures exit/protection enrich id=%s: %s", r.get("id"), ex)

    for r in serialized:
        if str(r.get("order_status") or "").strip().lower() != "bought":
            continue
        hold_type = str(r.get("hold_type") or "").strip().lower()
        if hold_type == "positional":
            continue
        if now_ist >= intraday_force_exit_time:
            if not bool(r.get("exit_suggested")):
                r["exit_suggested"] = True
            if not str(r.get("exit_reason") or "").strip():
                r["exit_reason"] = "Intraday time exit window opened (>= 3:15 PM IST)"

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


@router.post("/daily/{row_id}/sell")
def post_smart_futures_daily_sell(
    row_id: int,
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    """Mark row as sold at current LTP (exit / square-off bookkeeping)."""
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
    ost = str(row.get("order_status") or "").strip().lower()
    if ost != "bought":
        raise HTTPException(status_code=400, detail="Sell only after position is marked bought")

    ikey = (row.get("fut_instrument_key") or "").strip()
    if not ikey:
        raise HTTPException(status_code=400, detail="Missing instrument key")

    try:
        us = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        q = us.get_market_quote_by_key(ikey) or {}
        ltp = float(q.get("last_price") or 0)
    except Exception as e:
        logger.error("smart_futures sell LTP failed: %s", e, exc_info=True)
        raise HTTPException(status_code=502, detail=f"LTP fetch failed: {e}") from e
    if ltp <= 0:
        raise HTTPException(status_code=502, detail="Could not read last price from broker")

    sell_time_iso: Optional[str] = None
    try:
        res = db.execute(
            text(
                """
                UPDATE smart_futures_daily
                SET order_status = 'sold', sell_price = :ltp, sell_time = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id AND LOWER(TRIM(order_status)) = 'bought'
                RETURNING sell_price, sell_time
                """
            ),
            {"id": row_id, "ltp": ltp},
        )
        upd = res.mappings().first()
        if not upd:
            db.rollback()
            raise HTTPException(status_code=400, detail="Could not sell — position not open or already sold")
        st = upd.get("sell_time")
        if st is not None and hasattr(st, "isoformat"):
            sell_time_iso = st.isoformat()
    except HTTPException:
        raise
    except Exception as e:
        if _missing_db_column_error(e, "sell_time"):
            logger.warning("smart_futures sell: sell_time column missing, update without it: %s", e)
            try:
                r2 = db.execute(
                    text(
                        """
                        UPDATE smart_futures_daily
                        SET order_status = 'sold', sell_price = :ltp, updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id AND LOWER(TRIM(order_status)) = 'bought'
                        """
                    ),
                    {"id": row_id, "ltp": ltp},
                )
                if getattr(r2, "rowcount", 1) == 0:
                    db.rollback()
                    raise HTTPException(status_code=400, detail="Could not sell — position not open or already sold")
            except HTTPException:
                raise
            except Exception as e2:
                if _missing_db_column_error(e2, "sell_price"):
                    logger.warning("smart_futures sell: sell_price missing, status-only: %s", e2)
                    r3 = db.execute(
                        text(
                            """
                            UPDATE smart_futures_daily
                            SET order_status = 'sold', updated_at = CURRENT_TIMESTAMP
                            WHERE id = :id AND LOWER(TRIM(order_status)) = 'bought'
                            """
                        ),
                        {"id": row_id},
                    )
                    if getattr(r3, "rowcount", 1) == 0:
                        db.rollback()
                        raise HTTPException(
                            status_code=400, detail="Could not sell — position not open or already sold"
                        )
                    ltp = None
                else:
                    raise
        elif _missing_db_column_error(e, "sell_price"):
            logger.warning("smart_futures sell: sell_price column missing, status-only update: %s", e)
            r4 = db.execute(
                text(
                    """
                    UPDATE smart_futures_daily
                    SET order_status = 'sold', updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id AND LOWER(TRIM(order_status)) = 'bought'
                    """
                ),
                {"id": row_id},
            )
            if getattr(r4, "rowcount", 1) == 0:
                db.rollback()
                raise HTTPException(status_code=400, detail="Could not sell — position not open or already sold")
            ltp = None
        else:
            raise
    db.commit()
    logger.info(
        "smart_futures sell: user=%s row=%s sell_price=%s sell_time=%s",
        getattr(user, "id", None),
        row_id,
        ltp,
        sell_time_iso,
    )
    return {
        "success": True,
        "id": row_id,
        "order_status": "sold",
        "sell_price": ltp,
        "sell_time": sell_time_iso,
    }
