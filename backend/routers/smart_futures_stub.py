"""
Smart Futures routes: admin config (JSON file) + smart_futures_daily list / order.

Mounted at /api/smart-futures and /smart-futures for admintwc.js and smartfuture.html.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import OrderedDict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.database import get_db
from backend.models.user import User
from backend.routers.auth import get_user_from_token, oauth2_scheme
from backend.services import smart_futures_config as sfc
from backend.services.smart_futures_exit import (
    RECLAIM_ENTRY_SCORE_THRESHOLD,
    RECLAIM_SCORE_PANIC_THRESHOLD,
    apply_reclaim_vwap_gate,
    evaluate_exit_with_profit_protection,
    is_entry_permitted,
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


class SmartFuturesSellBody(BaseModel):
    """POST /daily/{id}/sell: optional manual price; if omitted, broker LTP is used."""

    sell_price: Optional[float] = Field(default=None, description="Manual square-off price (bookkeeping)")
    manual_exit_reason: Optional[str] = Field(
        default=None,
        max_length=32,
        description=(
            "Operator's stated reason when exiting before the algo panics "
            "(e.g. 'rules', 'emotion'). Persisted on smart_futures_daily.manual_exit_reason."
        ),
    )


class SmartFuturesOrderBody(BaseModel):
    """POST /daily/{id}/order: manual buy price + lot count, or omit buy_price to use broker LTP."""

    buy_price: Optional[float] = Field(
        default=None,
        description="Manual buy price; if omitted, last traded price from broker is used.",
    )
    calculated_lots: int = Field(default=1, ge=1, le=10000, description="Number of lots (position size).")


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
        "up to top_n//2, with long+short trimmed to top_n budget (same as futures backtester)."
    )
    out["max_publish_per_scan"] = int(getattr(sfc, "SMART_FUTURES_MAX_PUBLISH_PER_SCAN", 5))
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
        "up to top_n//2, with long+short trimmed to top_n budget (same as futures backtester)."
    )
    cur["max_publish_per_scan"] = int(getattr(sfc, "SMART_FUTURES_MAX_PUBLISH_PER_SCAN", 5))
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
                       premkt_rank, oi_heat_rank,
                       reclaim_score_last, reclaim_score_prev, reclaim_score_updated_at,
                       manual_exit_reason, manual_exit_at
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
        if (
            _missing_db_column_error(e, "reclaim_score_last")
            or _missing_db_column_error(e, "reclaim_score_prev")
            or _missing_db_column_error(e, "reclaim_score_updated_at")
            or _missing_db_column_error(e, "manual_exit_reason")
            or _missing_db_column_error(e, "manual_exit_at")
        ):
            logger.warning(
                "smart_futures /daily: entry-gate columns missing, using pre-gate query: %s", e
            )
            try:
                db.rollback()
            except Exception:
                pass
            return db.execute(text(_SQL_DAILY_NO_RANK), {"sd": sd}).mappings().all()
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


def _compute_realized_pnl_rupees_for_row(r: Dict[str, Any]) -> Optional[float]:
    """
    Approximate closed-trade PnL in INR for a sold Smart Futures row:
    price diff × lot size (from instruments) × calculated_lots. LONG: (sell−buy); SHORT: (buy−sell).
    """
    try:
        from backend.services.smart_futures_picker.position_sizing import (
            get_futures_lot_size_by_instrument_key,
        )

        bp = r.get("buy_price")
        sp = r.get("sell_price")
        if bp is None or sp is None:
            return None
        bp_f = float(bp)
        sp_f = float(sp)
        lots = int(r.get("calculated_lots") or 0)
        if lots < 1:
            return None
        ikey = str(r.get("fut_instrument_key") or "").strip()
        if not ikey:
            return None
        lot_size = int(get_futures_lot_size_by_instrument_key(ikey))
        if lot_size <= 0:
            return None
        side = str(r.get("side") or "").strip().upper()
        pts = (bp_f - sp_f) if side == "SHORT" else (sp_f - bp_f)
        return round(pts * float(lot_size) * float(lots), 2)
    except Exception:
        return None


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
    for _ts_col in ("reclaim_score_updated_at", "manual_exit_at"):
        v = out.get(_ts_col)
        if v is not None and hasattr(v, "isoformat"):
            out[_ts_col] = v.isoformat()
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


def _parse_any_ts_to_ist(ts: Any) -> Optional[datetime]:
    """Parse common timestamp forms to timezone-aware IST datetime."""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    s = str(ts).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except Exception:
        return None


@router.get("/daily")
def get_smart_futures_daily(user: User = Depends(_require_user), db: Session = Depends(get_db)):
    """Today's Trend: rows for effective session_date (IST window 9:15 → next session 09:10)."""
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
        m15_snapshot_cache: Dict[str, Dict[str, Any]] = {}
        ltp_cache: Dict[str, Optional[float]] = {}
        m5_session_cache: Dict[str, List[Dict[str, Any]]] = {}

        def _get_m5_session(ikey: str) -> List[Dict[str, Any]]:
            if ikey in m5_session_cache:
                return m5_session_cache[ikey]
            bars: List[Dict[str, Any]] = []
            try:
                raw5 = us_exit.get_historical_candles_by_instrument_key(
                    ikey, interval="minutes/5", days_back=5, range_end_date=sd
                )
                bars = [
                    b
                    for b in sorted(raw5 or [], key=lambda x: str(x.get("timestamp") or ""))
                    if _ist_date_from_ts(str(b.get("timestamp") or "")) == sd
                ]
            except Exception:
                bars = []
            m5_session_cache[ikey] = bars
            return bars

        # Prime LTP in one request (get_market_quote_by_key per row is slow and can fail spuriously).
        uniq_ikeys = sorted(
            {
                str(r.get("fut_instrument_key") or "").strip()
                for r in serialized
                if str(r.get("fut_instrument_key") or "").strip()
            }
        )
        if uniq_ikeys:
            try:
                batch_ltp = us_exit.get_market_quotes_batch_by_keys(uniq_ikeys) or {}
                for k, lp in batch_ltp.items():
                    if lp and float(lp) > 0:
                        ltp_cache[k] = round(float(lp), 2)
            except Exception as e:
                logger.warning("smart_futures /daily batch LTP: %s", e)

        for r in serialized:
            ikey = str(r.get("fut_instrument_key") or "").strip()
            if not ikey:
                r["m15_last_close"] = None
                r["m15_vwap"] = None
                r["m15_vwap_at_scan"] = None
                r["m15_last_close_at_scan"] = None
                r["current_ltp"] = None
                continue
            if ikey not in m15_snapshot_cache:
                snap: Dict[str, Any] = {
                    "m15_last_close": None,
                    "m15_vwap": None,
                    "bars": [],
                }
                try:
                    raw15 = us_exit.get_historical_candles_by_instrument_key(
                        ikey, interval="minutes/15", days_back=6, range_end_date=sd
                    )
                    m15 = [
                        b
                        for b in sorted(raw15 or [], key=lambda x: str(x.get("timestamp") or ""))
                        if _ist_date_from_ts(str(b.get("timestamp") or "")) == sd
                    ]
                    bars = []
                    for b in m15:
                        ts_ist = _parse_any_ts_to_ist(b.get("timestamp"))
                        if ts_ist is None:
                            continue
                        close_v = float(b.get("close") or 0.0)
                        vol_v = float(b.get("volume") or 0.0)
                        bars.append({"ts": ts_ist, "close": close_v, "volume": vol_v})
                    snap["bars"] = bars
                    if bars:
                        closes15 = [float(x["close"]) for x in bars]
                        vols15 = [float(x["volume"]) for x in bars]
                        last_close15 = float(closes15[-1]) if closes15 else 0.0
                        den = sum(v for v in vols15 if v > 0)
                        if den > 0:
                            vwap15 = sum(c * max(v, 0.0) for c, v in zip(closes15, vols15)) / den
                        else:
                            vwap15 = 0.0
                        snap["m15_last_close"] = round(last_close15, 2) if last_close15 > 0 else None
                        snap["m15_vwap"] = round(vwap15, 2) if vwap15 > 0 else None
                except Exception:
                    pass
                m15_snapshot_cache[ikey] = snap
            snap = m15_snapshot_cache.get(ikey, {})
            r["m15_last_close"] = snap.get("m15_last_close")
            r["m15_vwap"] = snap.get("m15_vwap")
            r["m15_vwap_at_scan"] = None
            r["m15_last_close_at_scan"] = None
            if ikey not in ltp_cache:
                try:
                    q = us_exit.get_market_quote_by_key(ikey) or {}
                    lp = float(q.get("last_price") or 0.0)
                    ltp_cache[ikey] = round(lp, 2) if lp > 0 else None
                except Exception:
                    ltp_cache[ikey] = None
            r["current_ltp"] = ltp_cache.get(ikey)
            try:
                entry_dt = _parse_any_ts_to_ist(r.get("entry_at"))
                bars = snap.get("bars") or []
                if entry_dt is not None and bars:
                    upto = [x for x in bars if x.get("ts") is not None and x["ts"] <= entry_dt]
                    if upto:
                        closes_u = [float(x["close"]) for x in upto]
                        vols_u = [float(x["volume"]) for x in upto]
                        last_u = float(closes_u[-1]) if closes_u else 0.0
                        den_u = sum(v for v in vols_u if v > 0)
                        vwap_u = (
                            sum(c * max(v, 0.0) for c, v in zip(closes_u, vols_u)) / den_u
                            if den_u > 0
                            else 0.0
                        )
                        r["m15_last_close_at_scan"] = round(last_u, 2) if last_u > 0 else None
                        r["m15_vwap_at_scan"] = round(vwap_u, 2) if vwap_u > 0 else None
            except Exception:
                pass

        for r in serialized:
            if str(r.get("order_status") or "").strip().lower() != "bought":
                continue
            try:
                ikey = str(r.get("fut_instrument_key") or "").strip()
                if not ikey:
                    continue
                m5 = _get_m5_session(ikey)
                if len(m5) < 3:
                    continue
                # Manual "Order" fill uses buy_price; that is the basis for exit / protection vs candles.
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
                scan_px = r.get("current_ltp")
                try:
                    if scan_px is None or float(scan_px) <= 0:
                        scan_px = r.get("m15_last_close")
                except (TypeError, ValueError):
                    scan_px = r.get("m15_last_close")
                try:
                    sp_f = float(scan_px) if scan_px is not None else None
                except (TypeError, ValueError):
                    sp_f = None
                vw_f = r.get("m15_vwap")
                try:
                    vw_use = float(vw_f) if vw_f is not None else None
                except (TypeError, ValueError):
                    vw_use = None
                new_ex, new_reason, _reclaim = apply_reclaim_vwap_gate(
                    side=side,
                    exit_suggested=bool(r["exit_suggested"]),
                    exit_reason=str(r["exit_reason"] or ""),
                    scan_price=sp_f,
                    vwap15=vw_use,
                    m5_session=m5,
                    sector_score=r.get("sector_score"),
                    entry_at=entry_at,
                    scan_time_ist=now_ist,
                )
                r["exit_suggested"] = new_ex
                r["exit_reason"] = new_reason
                r["reclaim_probability_score"] = _reclaim.get("score")
                r["vwap_adverse_at_scan"] = bool(_reclaim.get("vwap_adverse"))
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

        # Entry-gate enrichment + reclaim-score velocity persistence for every row.
        # Runs inside the `us_exit is not None` block because it needs m5 bars.
        for r in serialized:
            try:
                ikey = str(r.get("fut_instrument_key") or "").strip()
                side = str(r.get("side") or "").strip().upper()
                if not ikey or side not in {"LONG", "SHORT"}:
                    continue
                m5 = _get_m5_session(ikey)
                entry_dt = _parse_any_ts_to_ist(r.get("entry_at"))
                vwap_for_entry_gate = r.get("m15_vwap_at_scan")
                if vwap_for_entry_gate is None:
                    vwap_for_entry_gate = r.get("m15_vwap")
                try:
                    vwap_gate_in = (
                        float(vwap_for_entry_gate) if vwap_for_entry_gate is not None else None
                    )
                except (TypeError, ValueError):
                    vwap_gate_in = None
                try:
                    px_gate_in = (
                        float(r.get("entry_price"))
                        if r.get("entry_price") is not None
                        else None
                    )
                except (TypeError, ValueError):
                    px_gate_in = None
                gate = is_entry_permitted(
                    side=side,
                    entry_price=px_gate_in,
                    vwap15=vwap_gate_in,
                    entry_at_ist=entry_dt,
                    m5_session=m5,
                    sector_score=r.get("sector_score"),
                    now_ist=now_ist,
                )
                r["entry_gate_permitted"] = bool(gate.get("permitted"))
                r["entry_gate_score_pass"] = bool(gate.get("gate_score_pass"))
                r["entry_gate_time_pass"] = bool(gate.get("gate_time_pass"))
                r["entry_gate_vwap_pass"] = bool(gate.get("gate_vwap_pass"))
                r["entry_gate_reasons"] = list(gate.get("reasons") or [])
                r["entry_gate_score_threshold"] = gate.get("score_threshold")
                r["entry_gate_time_cutoff"] = gate.get("time_cutoff_hhmm")
                r["entry_gate_optimal_window_minutes"] = gate.get("optimal_window_minutes")
                r["entry_gate_minutes_since_trigger"] = gate.get("minutes_since_trigger")
                r["score_at_trigger"] = gate.get("score_at_trigger")
                r["vwap_at_trigger"] = gate.get("vwap_at_trigger")
                r["vwap_adverse_at_trigger"] = bool(gate.get("vwap_adverse_at_trigger"))
                r["watchlist_eligible"] = bool(gate.get("eligible_watchlist"))

                prev_last = r.get("reclaim_score_last")
                cur_last = r.get("reclaim_probability_score")
                if cur_last is None and gate.get("score_at_trigger") is not None:
                    cur_last = gate.get("score_at_trigger")
                if cur_last is not None:
                    try:
                        cur_f = float(cur_last)
                    except (TypeError, ValueError):
                        cur_f = None
                else:
                    cur_f = None
                prev_f = None
                if prev_last is not None:
                    try:
                        prev_f = float(prev_last)
                    except (TypeError, ValueError):
                        prev_f = None
                if cur_f is not None:
                    persisted_prev = (
                        prev_f if prev_f is not None else cur_f
                    )
                    velocity = cur_f - persisted_prev
                    r["reclaim_score_prev"] = round(persisted_prev, 2)
                    r["reclaim_score_last"] = round(cur_f, 2)
                    r["reclaim_score_velocity"] = round(velocity, 2)
                    rid = r.get("id")
                    if rid is not None and (prev_f is None or abs(velocity) > 1e-6):
                        try:
                            db.execute(
                                text(
                                    """
                                    UPDATE smart_futures_daily
                                    SET reclaim_score_prev = COALESCE(reclaim_score_last, :cur),
                                        reclaim_score_last = :cur,
                                        reclaim_score_updated_at = CURRENT_TIMESTAMP,
                                        updated_at = CURRENT_TIMESTAMP
                                    WHERE id = :id AND session_date = :sd
                                    """
                                ),
                                {"id": int(rid), "sd": sd, "cur": float(cur_f)},
                            )
                            db.commit()
                        except Exception as persist_ex:
                            logger.debug(
                                "smart_futures reclaim-score persist id=%s: %s",
                                rid,
                                persist_ex,
                            )
                            try:
                                db.rollback()
                            except Exception:
                                pass
                else:
                    r["reclaim_score_velocity"] = None

                if (
                    bool(gate.get("eligible_watchlist"))
                    and str(r.get("order_status") or "").strip().lower() != "bought"
                    and entry_dt is not None
                ):
                    try:
                        db.execute(
                            text(
                                """
                                INSERT INTO smart_futures_watchlist (
                                    trigger_date, daily_id, symbol, fut_symbol, fut_instrument_key,
                                    side, trigger_score, trigger_price, vwap_at_trigger, trigger_at
                                ) VALUES (
                                    :trigger_date, :daily_id, :symbol, :fut_symbol, :fut_instrument_key,
                                    :side, :trigger_score, :trigger_price, :vwap_at_trigger, :trigger_at
                                )
                                ON CONFLICT (trigger_date, fut_instrument_key) DO NOTHING
                                """
                            ),
                            {
                                "trigger_date": sd,
                                "daily_id": int(r.get("id")) if r.get("id") is not None else None,
                                "symbol": str(r.get("fut_symbol") or "")[:64]
                                or ikey.split("|")[-1][:64],
                                "fut_symbol": r.get("fut_symbol"),
                                "fut_instrument_key": ikey,
                                "side": side,
                                "trigger_score": gate.get("score_at_trigger"),
                                "trigger_price": px_gate_in,
                                "vwap_at_trigger": vwap_gate_in,
                                "trigger_at": entry_dt,
                            },
                        )
                        db.commit()
                    except Exception as wl_ex:
                        logger.debug(
                            "smart_futures watchlist insert id=%s: %s",
                            r.get("id"),
                            wl_ex,
                        )
                        try:
                            db.rollback()
                        except Exception:
                            pass
            except Exception as gate_ex:
                logger.debug(
                    "smart_futures entry-gate enrich id=%s: %s", r.get("id"), gate_ex
                )

    for r in serialized:
        if str(r.get("order_status") or "").strip().lower() == "sold":
            r["realized_pnl"] = _compute_realized_pnl_rupees_for_row(r)
        else:
            r.pop("realized_pnl", None)

    # Expose per-contract lot size (shares per lot) for UI; requires fut_instrument_key before pop.
    try:
        from backend.services.smart_futures_picker.position_sizing import (
            get_futures_lot_size_by_instrument_key,
        )
    except Exception:
        get_futures_lot_size_by_instrument_key = None  # type: ignore[assignment]

    for r in serialized:
        ik = str(r.get("fut_instrument_key") or "").strip()
        if not ik or get_futures_lot_size_by_instrument_key is None:
            r["instrument_lot_size"] = None
            continue
        try:
            ls = int(get_futures_lot_size_by_instrument_key(ik))
            r["instrument_lot_size"] = ls if ls > 0 else None
        except Exception:
            r["instrument_lot_size"] = None

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
    body: SmartFuturesOrderBody = Body(default_factory=SmartFuturesOrderBody),
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    """Mark row as bought with manual buy price + lots (JSON) or broker LTP if buy_price omitted."""
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

    lots = int(body.calculated_lots)

    manual_bp = body.buy_price
    if manual_bp is not None:
        try:
            ltp = float(manual_bp)
        except (TypeError, ValueError):
            ltp = 0.0
        if ltp <= 0 or ltp != ltp:
            raise HTTPException(status_code=400, detail="buy_price must be a positive number")
    else:
        try:
            us = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            q = us.get_market_quote_by_key(ikey) or {}
            ltp = float(q.get("last_price") or 0)
        except Exception as e:
            logger.error("smart_futures order LTP failed: %s", e, exc_info=True)
            raise HTTPException(status_code=502, detail=f"LTP fetch failed: {e}") from e
        if ltp <= 0:
            raise HTTPException(status_code=502, detail="Could not read last price from broker")

    try:
        db.execute(
            text(
                """
                UPDATE smart_futures_daily
                SET order_status = 'bought',
                    buy_price = :ltp,
                    calculated_lots = :lots,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id AND (order_status IS NULL OR LOWER(TRIM(order_status)) <> 'bought')
                """
            ),
            {"id": row_id, "ltp": ltp, "lots": lots},
        )
    except Exception as e:
        if _missing_db_column_error(e, "calculated_lots"):
            logger.warning("smart_futures order: calculated_lots missing, update without it: %s", e)
            try:
                db.rollback()
            except Exception:
                pass
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
        else:
            raise
    db.commit()
    logger.info(
        "smart_futures order: user=%s row=%s buy_price=%s calculated_lots=%s manual_price=%s",
        getattr(user, "id", None),
        row_id,
        ltp,
        lots,
        manual_bp is not None,
    )
    return {
        "success": True,
        "id": row_id,
        "order_status": "bought",
        "buy_price": ltp,
        "calculated_lots": lots,
    }


@router.post("/daily/{row_id}/sell")
def post_smart_futures_daily_sell(
    row_id: int,
    body: SmartFuturesSellBody = Body(default_factory=SmartFuturesSellBody),
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    """Mark row as sold at a manual price (JSON body) or current LTP if sell_price omitted."""
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

    ltp: Optional[float] = None
    manual = body.sell_price is not None
    if manual:
        try:
            ltp = float(body.sell_price)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            ltp = None
        if ltp is None or ltp <= 0 or ltp != ltp:
            raise HTTPException(status_code=400, detail="sell_price must be a positive number")
    else:
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
    manual_reason_in = (body.manual_exit_reason or "").strip().lower() or None
    if manual_reason_in is not None and manual_reason_in not in {"rules", "emotion"}:
        manual_reason_in = manual_reason_in[:32]
    try:
        res = db.execute(
            text(
                """
                UPDATE smart_futures_daily
                SET order_status = 'sold', sell_price = :ltp, sell_time = CURRENT_TIMESTAMP,
                    manual_exit_reason = :mer,
                    manual_exit_at = CASE WHEN :mer IS NULL THEN manual_exit_at ELSE CURRENT_TIMESTAMP END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id AND LOWER(TRIM(order_status)) = 'bought'
                RETURNING sell_price, sell_time
                """
            ),
            {"id": row_id, "ltp": ltp, "mer": manual_reason_in},
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
        if _missing_db_column_error(e, "manual_exit_reason") or _missing_db_column_error(
            e, "manual_exit_at"
        ):
            logger.warning(
                "smart_futures sell: manual_exit_reason missing, falling back: %s", e
            )
            try:
                db.rollback()
            except Exception:
                pass
            res_legacy = db.execute(
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
            upd_legacy = res_legacy.mappings().first()
            if not upd_legacy:
                db.rollback()
                raise HTTPException(
                    status_code=400,
                    detail="Could not sell — position not open or already sold",
                )
            st = upd_legacy.get("sell_time")
            if st is not None and hasattr(st, "isoformat"):
                sell_time_iso = st.isoformat()
        elif _missing_db_column_error(e, "sell_time"):
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
        "smart_futures sell: user=%s row=%s sell_price=%s sell_time=%s manual=%s",
        getattr(user, "id", None),
        row_id,
        ltp,
        sell_time_iso,
        manual,
    )
    return {
        "success": True,
        "id": row_id,
        "order_status": "sold",
        "sell_price": ltp,
        "sell_time": sell_time_iso,
        "manual_exit_reason": manual_reason_in,
    }


@router.get("/watchlist")
def get_smart_futures_watchlist(
    days: int = 3,
    user: User = Depends(_require_user),
    db: Session = Depends(get_db),
):
    """
    Carry-forward watch: picks that fired after the 14:00 IST cutoff but still passed the
    reclaim-score and VWAP entry gates. Displayed on the dashboard as pre-market review items.

    The list is sourced from ``smart_futures_watchlist``; the ``today_session_date`` is the
    current effective session so the UI can highlight rows triggered on the previous session.
    """
    try:
        lookback = max(1, min(int(days or 3), 30))
    except Exception:
        lookback = 3
    sd = effective_session_date_ist_for_trend()
    try:
        rows = (
            db.execute(
                text(
                    """
                    SELECT w.id, w.trigger_date, w.daily_id, w.symbol, w.fut_symbol, w.fut_instrument_key,
                           w.side, w.trigger_score, w.trigger_price, w.vwap_at_trigger, w.trigger_at,
                           w.added_at, w.cleared_at,
                           a.currmth_future_symbol AS master_currmth_future_symbol,
                           a.currmth_future_instrument_key AS master_currmth_future_inst_key,
                           a.currmth_future_ltp AS master_currmth_future_ltp
                    FROM smart_futures_watchlist w
                    LEFT JOIN arbitrage_master a
                      ON UPPER(TRIM(a.stock))
                         = UPPER(TRIM(SPLIT_PART(COALESCE(w.symbol, ''), ' ', 1)))
                    WHERE w.trigger_date >= :from_date
                    ORDER BY w.trigger_date DESC, w.trigger_at DESC, w.id DESC
                    """
                ),
                {"from_date": sd - timedelta(days=lookback)},
            )
            .mappings()
            .all()
        )
    except Exception as e:
        logger.warning("smart_futures /watchlist query failed: %s", e)
        return {
            "today_session_date": sd.isoformat(),
            "rows": [],
            "error": str(e),
        }
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        for k in ("trigger_date",):
            v = d.get(k)
            if v is not None and hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        for k in ("trigger_at", "added_at", "cleared_at"):
            v = d.get(k)
            if v is not None and hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        for k in ("trigger_score", "trigger_price", "vwap_at_trigger", "master_currmth_future_ltp"):
            v = d.get(k)
            if v is not None:
                try:
                    d[k] = float(v)
                except (TypeError, ValueError):
                    d[k] = None
        out.append(d)

    # Live LTP + PnL overlay for dashboard carry-forward table.
    # PnL is points since trigger_price, signed by side (LONG/SHORT).
    try:
        keys: List[str] = []
        for r in out:
            ik = str(r.get("fut_instrument_key") or "").strip()
            if ik:
                keys.append(ik)
        uniq_keys = list(dict.fromkeys(keys))
        if uniq_keys:
            upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
            ltp_map = upstox.get_market_quotes_batch_by_keys(uniq_keys) or {}
            for r in out:
                ik = str(r.get("fut_instrument_key") or "").strip()
                ltp = None
                if ik and ik in ltp_map:
                    try:
                        lv = float(ltp_map.get(ik))
                        if lv > 0:
                            ltp = lv
                    except (TypeError, ValueError):
                        ltp = None
                r["ltp"] = ltp
                tp = r.get("trigger_price")
                side = str(r.get("side") or "").strip().upper()
                pnl_points = None
                if ltp is not None and tp is not None:
                    try:
                        entry = float(tp)
                        if side == "SHORT":
                            pnl_points = float(entry) - float(ltp)
                        else:
                            pnl_points = float(ltp) - float(entry)
                    except (TypeError, ValueError):
                        pnl_points = None
                r["pnl_points"] = pnl_points
    except Exception as e:
        logger.warning("smart_futures /watchlist live LTP overlay failed: %s", e)
    return {
        "today_session_date": sd.isoformat(),
        "entry_score_threshold": RECLAIM_ENTRY_SCORE_THRESHOLD,
        "panic_score_threshold": RECLAIM_SCORE_PANIC_THRESHOLD,
        "rows": out,
    }
