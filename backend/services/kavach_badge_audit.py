"""Shadow badge-input audit for Whipsawed / DIR CONFLICT / REGIME / CHURN.

Instrumentation only — does not change badge calculation, display, or gates.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Debounce live enrich polls (UI ~60s) unless badge set changes.
_LOG_MIN_INTERVAL = timedelta(minutes=4)

_ENSURED = False

# Explicit: no badge expiry/decay timer exists in live code — badges clear only
# when the next enrich recompute finds the triggering condition false.
BADGE_DECAY_NOTE = (
    "No dedicated badge expiry/decay timer. Badges are re-evaluated every enrich "
    "poll (~60s UI / 5m refresh). They clear only when the underlying condition "
    "is false on the next evaluation — there is no time-based fade."
)


def ensure_badge_audit_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_badge_input_log (
                    id SERIAL PRIMARY KEY,
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(8),
                    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source VARCHAR(32) NOT NULL DEFAULT 'live',
                    trade_state VARCHAR(32),
                    trade_state_reason TEXT,
                    gate_badges JSONB NOT NULL DEFAULT '[]'::jsonb,
                    -- Whipsawed (UI) ← WHIPSAW · n reversals (price EMA5 path, NOT lock churn)
                    whipsaw_active BOOLEAN NOT NULL DEFAULT FALSE,
                    whipsaw_count INTEGER,
                    whipsaw_threshold INTEGER,
                    whipsaw_basis TEXT,
                    whipsaw_events JSONB NOT NULL DEFAULT '[]'::jsonb,
                    -- DIR CONFLICT: checklist lock dir vs live Trend/ST/MACD (+ Kavach/TS)
                    dir_conflict_active BOOLEAN NOT NULL DEFAULT FALSE,
                    dir_conflict JSONB NOT NULL DEFAULT '{}'::jsonb,
                    -- REGIME UNSTABLE / CHURN (session-level, stamped on each card)
                    regime_unstable_active BOOLEAN NOT NULL DEFAULT FALSE,
                    churn_active BOOLEAN NOT NULL DEFAULT FALSE,
                    regime_context JSONB NOT NULL DEFAULT '{}'::jsonb,
                    -- Continuous display seconds while badge stays active (shadow)
                    persistence JSONB NOT NULL DEFAULT '{}'::jsonb,
                    decay_note TEXT,
                    inputs JSONB NOT NULL DEFAULT '{}'::jsonb
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_badge_input_log_session_sym
                ON kavach_badge_input_log (session_date, symbol, logged_at)
                """
            )
        )
    _ENSURED = True


def _badge_active_set(gate_badges: Optional[List[Any]], trade_state_reason: Optional[str]) -> Set[str]:
    out: Set[str] = set()
    for b in gate_badges or []:
        t = str(b)
        if t.startswith("WHIPSAW") or "WHIPSAW" in t:
            out.add("WHIPSAW")
        elif t.startswith("DIR CONFLICT") or t == "DIR CONFLICT":
            out.add("DIR CONFLICT")
        elif t.startswith("REGIME UNSTABLE") or t == "REGIME UNSTABLE":
            out.add("REGIME UNSTABLE")
        elif t.startswith("CHURN"):
            out.add("CHURN")
    r = (trade_state_reason or "").lower()
    if "whip" in r:
        out.add("WHIPSAW")
    if "direction conflict" in r or "dir conflict" in r:
        out.add("DIR CONFLICT")
    return out


def _last_badge_row(db, session_date: str, symbol: str) -> Optional[Any]:
    return db.execute(
        text(
            """
            SELECT logged_at, gate_badges, persistence,
                   whipsaw_active, dir_conflict_active,
                   regime_unstable_active, churn_active
            FROM kavach_badge_input_log
            WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
            ORDER BY logged_at DESC, id DESC
            LIMIT 1
            """
        ),
        {"d": session_date, "sym": symbol.upper()},
    ).fetchone()


def _persistence_update(
    prev: Optional[Any],
    active: Set[str],
    now: datetime,
) -> Dict[str, Any]:
    """Track continuous active duration per badge family (shadow only)."""
    prev_pers: Dict[str, Any] = {}
    if prev is not None and prev.persistence:
        raw = prev.persistence
        if isinstance(raw, str):
            try:
                prev_pers = json.loads(raw)
            except Exception:
                prev_pers = {}
        elif isinstance(raw, dict):
            prev_pers = dict(raw)

    prev_active: Set[str] = set()
    if prev is not None:
        if prev.whipsaw_active:
            prev_active.add("WHIPSAW")
        if prev.dir_conflict_active:
            prev_active.add("DIR CONFLICT")
        if prev.regime_unstable_active:
            prev_active.add("REGIME UNSTABLE")
        if prev.churn_active:
            prev_active.add("CHURN")

    prev_at = None
    if prev is not None and prev.logged_at is not None:
        prev_at = prev.logged_at
        if getattr(prev_at, "tzinfo", None) is None:
            prev_at = IST.localize(prev_at)
        else:
            prev_at = prev_at.astimezone(IST)
    delta_sec = 0.0
    if prev_at is not None:
        delta_sec = max(0.0, (now - prev_at).total_seconds())

    out: Dict[str, Any] = {}
    for name in ("WHIPSAW", "DIR CONFLICT", "REGIME UNSTABLE", "CHURN"):
        was = name in prev_active
        is_on = name in active
        prior = prev_pers.get(name) if isinstance(prev_pers.get(name), dict) else {}
        if is_on:
            if was:
                cont = float(prior.get("continuous_seconds") or 0) + delta_sec
                first = prior.get("first_active_at") or (prev_at.isoformat() if prev_at else now.isoformat())
            else:
                cont = 0.0
                first = now.isoformat()
            out[name] = {
                "active": True,
                "continuous_seconds": round(cont, 1),
                "first_active_at": first,
                "cleared_at": None,
            }
        else:
            out[name] = {
                "active": False,
                "continuous_seconds": 0.0,
                "first_active_at": prior.get("first_active_at") if was else None,
                "cleared_at": now.isoformat() if was else prior.get("cleared_at"),
                "last_continuous_seconds": (
                    round(float(prior.get("continuous_seconds") or 0) + delta_sec, 1)
                    if was
                    else prior.get("last_continuous_seconds")
                ),
            }
    out["_decay_note"] = BADGE_DECAY_NOTE
    return out


def should_log_badge_audit(
    db,
    session_date: str,
    symbol: str,
    active: Set[str],
    *,
    now: Optional[datetime] = None,
    force: bool = False,
) -> bool:
    if force:
        return True
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    prev = _last_badge_row(db, session_date, symbol)
    if prev is None:
        return True
    prev_active: Set[str] = set()
    if prev.whipsaw_active:
        prev_active.add("WHIPSAW")
    if prev.dir_conflict_active:
        prev_active.add("DIR CONFLICT")
    if prev.regime_unstable_active:
        prev_active.add("REGIME UNSTABLE")
    if prev.churn_active:
        prev_active.add("CHURN")
    if prev_active != active:
        return True
    prev_at = prev.logged_at
    if prev_at is None:
        return True
    if getattr(prev_at, "tzinfo", None) is None:
        prev_at = IST.localize(prev_at)
    else:
        prev_at = prev_at.astimezone(IST)
    return (now - prev_at) >= _LOG_MIN_INTERVAL


def build_badge_audit_payload(
    stock: Dict[str, Any],
    *,
    session_date: str,
    candles: Optional[List[Dict[str, Any]]] = None,
    near_atr: float = 0.35,
    atr: Optional[float] = None,
) -> Dict[str, Any]:
    """Snapshot of badge inputs for one stock (no DB writes)."""
    from backend.services.daily_checklist_chop_gates import (
        WHIPSAW_REVERSALS,
        list_whipsaw_reversal_events,
    )

    direction = (stock.get("direction") or "LONG").upper()
    is_long = direction != "SHORT"
    badges = list(stock.get("gate_badges") or [])
    reason = stock.get("trade_state_reason")
    active = _badge_active_set(badges, reason)

    whip_events: List[Dict[str, Any]] = []
    whip_count = int(stock.get("whipsaw_count") or 0)
    if candles:
        whip_events = list_whipsaw_reversal_events(
            candles,
            session_date=session_date,
            is_long=is_long,
            near_atr=near_atr,
            atr=atr,
        )
        whip_count = len(whip_events)

    dc = stock.get("dir_conflict") if isinstance(stock.get("dir_conflict"), dict) else {}
    # Also capture displayed panel labels used for the comparison
    dc_detail = {
        **dc,
        "comparison": (
            "checklist lock direction vs live panel Trend (price/EMA vs VWAP), "
            "Supertrend, MACD; also live Kavach state opposite lock, "
            "or Trading State HOLD/WATCH"
        ),
        "checklist_direction": dc.get("checklist_direction") or direction,
        "displayed_labels": {
            "trend": stock.get("trend"),
            "ema_vs_vwap": stock.get("ema_vs_vwap"),
            "supertrend": stock.get("supertrend"),
            "macd": stock.get("macd"),
            "trading_state": stock.get("trading_state"),
            "kavach_state": stock.get("dashboard_kavach_live")
            or stock.get("dashboard_kavach")
            or stock.get("kavach_state"),
        },
        "sides": dc.get("sides"),
        "opposing_fields": dc.get("opposing_fields"),
        "agreeing_fields": dc.get("agreeing_fields"),
        "live_lean": dc.get("live_lean"),
        "suppress_ready": dc.get("suppress_ready"),
        "reason": dc.get("reason"),
    }

    regime = stock.get("regime_context") if isinstance(stock.get("regime_context"), dict) else {}
    regime_detail = {
        **regime,
        "note": (
            "REGIME UNSTABLE = session market_regime TRANSITION/CHOP or imbalance lean "
            "(regime_unconfirmed). CHURN n = session-wide lock removals in last hour "
            "(not this symbol's membership churn alone). Symbol cycling in/out of lock "
            "is separate (rs_lock_membership_audit) and is NOT the Whipsawed badge."
        ),
        "whipsaw_vs_lock_churn": (
            "WHIPSAW/Whipsawed is price EMA5 touch→reverse-close count. "
            "Lock membership churn is not used for that badge."
        ),
    }

    return {
        "active": sorted(active),
        "gate_badges": badges,
        "trade_state": stock.get("trade_state"),
        "trade_state_reason": reason,
        "whipsaw_active": "WHIPSAW" in active,
        "whipsaw_count": whip_count,
        "whipsaw_threshold": WHIPSAW_REVERSALS,
        "whipsaw_basis": (
            "price: EMA5 proximity touch on confirmed 10m bar, then close against "
            "lock direction within next 1–2 confirmed 10m bars "
            f"(threshold ≥{WHIPSAW_REVERSALS}). NOT lock-list membership churn."
        ),
        "whipsaw_events": whip_events,
        "dir_conflict_active": "DIR CONFLICT" in active,
        "dir_conflict": dc_detail,
        "regime_unstable_active": "REGIME UNSTABLE" in active,
        "churn_active": "CHURN" in active,
        "regime_context": regime_detail,
        "decay_note": BADGE_DECAY_NOTE,
        "ui_whipsawed_label": (
            "Frontend oneWordReason maps trade_state_reason containing 'whip' → 'whipsawed'"
        ),
    }


def log_badge_inputs(
    db,
    *,
    session_date: str,
    stock: Dict[str, Any],
    candles: Optional[List[Dict[str, Any]]] = None,
    near_atr: float = 0.35,
    atr: Optional[float] = None,
    source: str = "live",
    force: bool = False,
    logged_at: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    ensure_badge_audit_table()
    sym = (stock.get("symbol") or "").upper()
    if not sym:
        return None
    now = logged_at or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)

    payload = build_badge_audit_payload(
        stock,
        session_date=session_date,
        candles=candles,
        near_atr=near_atr,
        atr=atr,
    )
    active = set(payload.get("active") or [])
    if not should_log_badge_audit(db, session_date, sym, active, now=now, force=force):
        return None

    prev = _last_badge_row(db, session_date, sym)
    persistence = _persistence_update(prev, active, now)

    try:
        db.execute(
            text(
                """
                INSERT INTO kavach_badge_input_log (
                    session_date, symbol, direction, logged_at, source,
                    trade_state, trade_state_reason, gate_badges,
                    whipsaw_active, whipsaw_count, whipsaw_threshold,
                    whipsaw_basis, whipsaw_events,
                    dir_conflict_active, dir_conflict,
                    regime_unstable_active, churn_active, regime_context,
                    persistence, decay_note, inputs
                ) VALUES (
                    CAST(:d AS date), :sym, :dir, :lat, :src,
                    :ts, :tsr, CAST(:badges AS jsonb),
                    :wa, :wc, :wt,
                    :wb, CAST(:we AS jsonb),
                    :da, CAST(:dc AS jsonb),
                    :ra, :ca, CAST(:rc AS jsonb),
                    CAST(:pers AS jsonb), :dn, CAST(:inp AS jsonb)
                )
                """
            ),
            {
                "d": session_date,
                "sym": sym,
                "dir": (stock.get("direction") or "LONG").upper(),
                "lat": now,
                "src": source,
                "ts": payload.get("trade_state"),
                "tsr": payload.get("trade_state_reason"),
                "badges": json.dumps(payload.get("gate_badges") or []),
                "wa": bool(payload.get("whipsaw_active")),
                "wc": payload.get("whipsaw_count"),
                "wt": payload.get("whipsaw_threshold"),
                "wb": payload.get("whipsaw_basis"),
                "we": json.dumps(payload.get("whipsaw_events") or []),
                "da": bool(payload.get("dir_conflict_active")),
                "dc": json.dumps(payload.get("dir_conflict") or {}),
                "ra": bool(payload.get("regime_unstable_active")),
                "ca": bool(payload.get("churn_active")),
                "rc": json.dumps(payload.get("regime_context") or {}),
                "pers": json.dumps(persistence),
                "dn": BADGE_DECAY_NOTE,
                "inp": json.dumps(
                    {
                        "active": payload.get("active"),
                        "ui_whipsawed_label": payload.get("ui_whipsawed_label"),
                    }
                ),
            },
        )
    except Exception as exc:
        logger.warning("badge input log failed %s: %s", sym, exc)
        return None

    payload["persistence"] = persistence
    return payload


def log_badge_inputs_for_stocks(
    db,
    *,
    session_date: str,
    stocks: List[Dict[str, Any]],
    candle_cache: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    atr_pct_map: Optional[Dict[str, float]] = None,
    near_atr: float = 0.35,
    source: str = "live",
) -> int:
    """Batch shadow log after enrich; returns rows written."""
    ensure_badge_audit_table()
    candle_cache = candle_cache or {}
    atr_pct_map = atr_pct_map or {}
    n = 0
    for s in stocks:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue
        price = None
        try:
            from backend.services.daily_checklist_trade_state import _f

            price = _f(s.get("trade_entry")) or _f(s.get("price"))
        except Exception:
            price = None
        atr_pct = float(atr_pct_map.get(sym) or 0.0)
        atr = (price * atr_pct / 100.0) if price and atr_pct > 0 else None
        out = log_badge_inputs(
            db,
            session_date=session_date,
            stock=s,
            candles=candle_cache.get(sym),
            near_atr=near_atr,
            atr=atr,
            source=source,
        )
        if out:
            n += 1
    return n


def backfill_badge_audit_symbol(
    symbol: str,
    session_date: str,
    *,
    direction: str = "LONG",
    start_hm: str = "09:45",
    end_hm: Optional[str] = None,
) -> Dict[str, Any]:
    """Replay enrich-like badge inputs for a symbol across the session window."""
    from datetime import date, time, timedelta

    from backend.config import settings
    from backend.services.daily_checklist_chop_gates import count_whipsaw_reversals
    from backend.services.daily_checklist_live import _latest_nifty_pct, _ranking_for_direction
    from backend.services.daily_checklist_trade_state import (
        direction_live_conflict,
        overlay_live_momentum_from_candles,
    )
    from backend.services.daily_checklist_zones import (
        IMBALANCE_THRESHOLD,
        annotate_regime_context,
        regime_research_snapshot,
    )
    from backend.services.kavach_10m import metrics_from_10m_candles
    from backend.services.kavach_universe_vwap_scan import _atr_map
    from backend.services.rs_conviction_candles import load_instrument_atr_maps
    from backend.services.rs_conviction_config import get_config
    from backend.services.relative_strength_scanner import (
        CANDLE_DAYS_BACK,
        CANDLE_INTERVAL,
        MIN_BARS,
        _sorted_candles,
    )
    from backend.services.upstox_service import UpstoxService

    ensure_badge_audit_table()
    sym = symbol.upper()
    d = date.fromisoformat(session_date)
    sh, sm = map(int, start_hm.split(":"))
    start = IST.localize(datetime.combine(d, time(sh, sm)))
    if end_hm:
        eh, em = map(int, end_hm.split(":"))
        end = IST.localize(datetime.combine(d, time(eh, em)))
    else:
        end = datetime.now(IST)

    db = SessionLocal()
    try:
        ikey_map, _ = load_instrument_atr_maps(db, {sym})
        ikey = ikey_map.get(sym)
        if not ikey:
            return {"ok": False, "error": "no_instrument_key"}
        atr_pct = float((_atr_map(db, [sym]) or {}).get(sym) or 1.0)
        nifty_pct = _latest_nifty_pct(db)
        ranking = _ranking_for_direction(direction)
        cfg = get_config()
        near_atr = float(cfg.get("convergence_atr") or 0.35)

        raw = UpstoxService(
            settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET
        ).get_historical_candles_by_instrument_key(
            ikey,
            interval=CANDLE_INTERVAL,
            days_back=max(CANDLE_DAYS_BACK, 5),
            range_end_date=d,
        )
        if not raw or len(raw) < MIN_BARS:
            return {"ok": False, "error": "candle_fetch_failed"}
        candles = _sorted_candles(raw)

        # Removals for CHURN / REGIME context (session-wide)
        from backend.services.daily_checklist_trade_state import _recent_removals
        from backend.services.daily_checklist_chop_gates import compute_market_regime

        removals = _recent_removals(db, session_date)
        mkt = compute_market_regime(session_date)

        db.execute(
            text(
                """
                DELETE FROM kavach_badge_input_log
                WHERE session_date = CAST(:d AS date) AND UPPER(symbol) = :sym
                  AND source = 'backfill'
                """
            ),
            {"d": session_date, "sym": sym},
        )
        db.commit()

        samples: List[Dict[str, Any]] = []
        cur = start
        n = 0
        while cur <= end:
            from backend.services.rs_vwap_quality import _parse_ts

            sliced = [
                c for c in candles if (_parse_ts(c.get("timestamp")) or cur) <= cur
            ]
            stock: Dict[str, Any] = {
                "symbol": sym,
                "direction": direction,
                "trade_state": "WAIT FOR PULLBACK",
                "gate_badges": [],
            }
            overlay_live_momentum_from_candles(stock, sliced, nifty_pct=nifty_pct)
            m = metrics_from_10m_candles(
                sliced, ranking_type=ranking, nifty_pct=nifty_pct, now=cur
            )
            if m:
                stock["dashboard_kavach_live"] = m.get("kavach_state")
                stock["price"] = m.get("price")
                price = float(m.get("price") or 0)
            else:
                price = 0.0
            atr = (price * atr_pct / 100.0) if price and atr_pct > 0 else None
            is_long = direction != "SHORT"
            whip = count_whipsaw_reversals(
                sliced,
                session_date=session_date,
                is_long=is_long,
                near_atr=near_atr,
                atr=atr,
            )
            stock["whipsaw_count"] = whip
            if whip >= 2:
                stock["gate_badges"] = [f"WHIPSAW · {whip} reversals"]
                stock["trade_state_reason"] = f"WAIT · WHIPSAW · {whip} reversals"

            dc = direction_live_conflict(
                direction=direction,
                trend=stock.get("trend"),
                ema_vs_vwap=stock.get("ema_vs_vwap"),
                supertrend=stock.get("supertrend"),
                macd=stock.get("macd"),
                kavach_state=stock.get("dashboard_kavach_live"),
                trading_state=stock.get("trading_state"),
            )
            stock["dir_conflict"] = dc
            if int(dc.get("conflict_count") or 0) >= 1 or dc.get("suppress_ready"):
                badges = list(stock.get("gate_badges") or [])
                if "DIR CONFLICT" not in badges:
                    badges.append("DIR CONFLICT")
                stock["gate_badges"] = badges

            # Regime/CHURN as of "now" using same annotate path
            from backend.services.daily_checklist_zones import build_zone1_obs

            zone1 = build_zone1_obs(rotation_day=None, removals=removals, locked_by=None)
            annotate_regime_context(
                [stock],
                market_regime=mkt.get("market_regime"),
                market_regime_label=mkt.get("market_regime_label"),
                imbalance=zone1.get("direction_imbalance"),
                removals=removals,
                now=cur,
            )

            out = log_badge_inputs(
                db,
                session_date=session_date,
                stock=stock,
                candles=sliced,
                near_atr=near_atr,
                atr=atr,
                source="backfill",
                force=True,
                logged_at=cur,
            )
            if out:
                n += 1
                samples.append(
                    {
                        "at": cur.isoformat(),
                        "active": out.get("active"),
                        "whip": out.get("whipsaw_count"),
                        "whip_events": len(out.get("whipsaw_events") or []),
                        "dir": out.get("dir_conflict_active"),
                        "dir_reason": (out.get("dir_conflict") or {}).get("reason"),
                        "sides": (out.get("dir_conflict") or {}).get("sides"),
                        "labels": (out.get("dir_conflict") or {}).get(
                            "displayed_labels"
                        ),
                        "regime": out.get("regime_unstable_active"),
                        "churn": out.get("churn_active"),
                        "badges": out.get("gate_badges"),
                    }
                )
            cur += timedelta(minutes=10)

        db.commit()
        return {
            "ok": True,
            "symbol": sym,
            "rows": n,
            "imbalance_threshold": IMBALANCE_THRESHOLD,
            "decay_note": BADGE_DECAY_NOTE,
            "samples": samples,
        }
    except Exception as exc:
        logger.exception("badge audit backfill failed")
        try:
            db.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        db.close()
