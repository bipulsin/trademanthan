"""
Vajra discretionary trade validation, lifecycle, health, and management alerts.
No broker execution — structure interpretation only.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.config import settings
from backend.services.market_sentiment_dials import build_dial_rows
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.indicators import cumulative_vwap, ema_series
from backend.services.vajra.job import _fetch_candles_for_tf
from backend.services.vajra.transition import compute_pullback_quality
from backend.services.vajra.validation_engine import evaluate_checklist, extension_risk_level

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

LIFECYCLE_STATES = (
    "Early Transition",
    "Expansion",
    "Stable Consolidation",
    "Rotation",
    "Exhaustion",
    "Breakdown Risk",
    "Failed Structure",
)


def _dir_bull(direction: str) -> bool:
    return str(direction or "").upper().startswith("L")


def _fetch_5m(instrument_key: str) -> List[dict]:
    if not instrument_key:
        return []
    try:
        u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        return _fetch_candles_for_tf(u, instrument_key, "5m")
    except Exception as e:
        logger.debug("discretionary 5m fetch %s: %s", instrument_key, e)
        return []


def _ltp(upstox: UpstoxService, instrument_key: str) -> Optional[float]:
    if not instrument_key:
        return None
    keys = {instrument_key.strip()}
    keys.add(instrument_key.replace("|", ":"))
    keys.add(instrument_key.replace(":", "|"))
    for key in keys:
        if not key:
            continue
        try:
            q = upstox.get_market_quote_by_key(key)
            if q:
                p = float(q.get("last_price") or q.get("ltp") or 0)
                if p > 0:
                    return p
        except Exception:
            pass
    try:
        from backend.services.chart_feed_manager import get_chart_live_quote
        from backend.services.upstox_market_feed import _normalize_ik

        q = get_chart_live_quote(_normalize_ik(instrument_key))
        if q and q.get("ltp") is not None:
            p = float(q["ltp"])
            return p if p > 0 else None
    except Exception:
        pass
    return None


def _existing_current_price(trade: Dict[str, Any]) -> Optional[float]:
    raw = trade.get("current_price")
    if raw is None:
        return None
    try:
        p = float(raw)
        return p if p > 0 else None
    except (TypeError, ValueError):
        return None


def _resolve_current_price(
    trade: Dict[str, Any],
    ltp: Optional[float],
    candles: Sequence[Dict[str, Any]],
) -> Optional[float]:
    """Keep last known price; replace only when a new non-null LTP is available."""
    price = _existing_current_price(trade)
    if ltp is not None and ltp > 0:
        return ltp
    if price is not None:
        return price
    if candles:
        try:
            last_close = float(candles[-1].get("close") or 0)
            if last_close > 0:
                return last_close
        except (TypeError, ValueError, IndexError):
            pass
    try:
        entry = float(trade.get("entry_price") or 0)
        return entry if entry > 0 else None
    except (TypeError, ValueError):
        return None


def _sector_alignment_label(stock: str, bull: bool) -> str:
    try:
        from backend.services.vajra.sector_intelligence import sector_alignment_for_stock

        return sector_alignment_for_stock(stock, bull)
    except Exception:
        return "neutral"


def _market_index_pct() -> Dict[str, float]:
    try:
        u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        rows = build_dial_rows(u, basis="today")
        by_id = {str(r.get("id") or ""): r for r in rows}
        return {
            "nifty_pct": float((by_id.get("nifty50") or {}).get("pct_change") or 0),
            "bank_pct": float((by_id.get("banknifty") or {}).get("pct_change") or 0),
        }
    except Exception:
        return {"nifty_pct": 0.0, "bank_pct": 0.0}


def build_validation_preview(
    *,
    stock: str,
    direction: str,
    instrument_key: str,
    discovery_row: Dict[str, Any],
) -> Dict[str, Any]:
    """Auto-checks and warnings for Trade Validation & Entry (Section B/C/D)."""
    bull = _dir_bull(direction)
    candles = _fetch_5m(instrument_key)
    warnings: List[str] = []

    metrics = {
        "tps_score": discovery_row.get("tps_score"),
        "ecs_score": discovery_row.get("ecs_score") or discovery_row.get("confidence"),
        "extension_risk_score": discovery_row.get("extension_risk_score"),
        "pullback_quality_score": discovery_row.get("pullback_quality_score"),
        "trend_strength": "PASS" if "PASS" in str(discovery_row.get("trend") or "") else "FAIL",
        "momentum_state": discovery_row.get("transition_state") or discovery_row.get("momentum"),
        "market_phase": discovery_row.get("market_phase"),
        "htf_bias": discovery_row.get("htf_bias") or discovery_row.get("reversal_risk"),
    }

    if len(candles) < 30:
        return {
            "checklist": {},
            "checklist_eval": [],
            "warnings": ["Insufficient 5m candle data for auto-validation."],
            "metrics": metrics,
            "auto_available": False,
        }

    sector_align = _sector_alignment_label(stock, bull)
    market_idx = _market_index_pct()
    evaluated = evaluate_checklist(
        candles,
        direction=direction,
        market_index=market_idx,
        sector_alignment=sector_align,
    )

    checklist = evaluated.get("checklist") or {}
    items = evaluated.get("items") or []
    ext_score = float(
        evaluated.get("extension_risk_score")
        or discovery_row.get("extension_risk_score")
        or 50
    )
    ext_level = evaluated.get("extension_risk_level") or extension_risk_level(ext_score)
    vwap_dist_pct = float(evaluated.get("vwap_distance_pct") or 0)
    pullback_depth = float(evaluated.get("pullback_depth") or 0)

    metrics["extension_risk_score"] = ext_score
    metrics["extension_risk_level"] = ext_level
    metrics["pullback_quality_score"] = evaluated.get("pullback_quality_score") or metrics.get(
        "pullback_quality_score"
    )
    metrics["vwap_distance_pct"] = vwap_dist_pct
    metrics["pullback_depth_pct"] = round(pullback_depth * 100, 1)

    pass_n = sum(1 for it in items if it.get("status") == "pass")
    warn_n = sum(1 for it in items if it.get("status") == "warn")
    fail_n = sum(1 for it in items if it.get("status") == "fail")
    metrics["validation_pass_count"] = pass_n
    metrics["validation_warn_count"] = warn_n
    metrics["validation_fail_count"] = fail_n

    for it in items:
        if it.get("status") == "fail":
            warnings.append(f"{it.get('label')}: {it.get('metric')}")
        elif it.get("status") == "warn" and it.get("key") in (
            "no_vertical_exhaustion",
            "not_into_major_level",
            "not_extended_vwap",
        ):
            warnings.append(f"{it.get('label')}: {it.get('metric')}")

    if ext_level == "HIGH":
        warnings.append("Extension risk is HIGH — late entry risk.")
    elif ext_level == "MEDIUM" and "Extension risk is HIGH" not in " ".join(warnings):
        warnings.append("Extension risk is elevated — monitor for chasing.")

    seen = set()
    deduped: List[str] = []
    for w in warnings:
        if w not in seen:
            seen.add(w)
            deduped.append(w)

    plan = discovery_row.get("trade_plan")
    if not plan:
        try:
            from backend.services.vajra.execution_co_pilot import enrich_row_co_pilot

            enriched = enrich_row_co_pilot(dict(discovery_row), active_trades={}, prior_wf=None)
            plan = enriched.get("trade_plan")
        except Exception:
            plan = None

    return {
        "checklist": checklist,
        "checklist_eval": items,
        "trade_plan": plan,
        "execution_workflow_state": discovery_row.get("execution_workflow_state"),
        "setup_type": discovery_row.get("setup_type"),
        "quality_grade": discovery_row.get("quality_grade"),
        "warnings": deduped[:12],
        "metrics": metrics,
        "auto_available": True,
        "extension_risk_level": ext_level,
        "pullback_depth": pullback_depth,
        "vwap_distance_pct": vwap_dist_pct,
    }


def _sector_aligned(stock: str, bull: bool) -> bool:
    return _sector_alignment_label(stock, bull) != "conflicting"


def classify_lifecycle(
    *,
    discovery: Dict[str, Any],
    health: float,
    checklist_flags: Dict[str, bool],
    bull: bool,
) -> str:
    phase = str(discovery.get("market_phase") or "").upper()
    ext = float(discovery.get("extension_risk_score") or 50)
    tt = str(discovery.get("trade_type") or "")
    if health < 20 or not checklist_flags.get("ema_reclaimed", True):
        return "Failed Structure"
    if health < 40:
        return "Breakdown Risk"
    if "EXHAUSTION" in phase or ext >= 70:
        return "Exhaustion"
    if "EXPANSION" in phase and health >= 60:
        return "Expansion"
    if "COMPRESSION" in phase or "ROTATIONAL" in phase:
        return "Rotation" if health < 55 else "Stable Consolidation"
    if "EARLY" in tt:
        return "Early Transition"
    return "Stable Consolidation"


def compute_trade_health(
    *,
    bull: bool,
    close: float,
    vwap: float,
    ema5: float,
    pb_q: float,
    ext_risk: float,
    candles: Sequence[Dict[str, Any]],
) -> float:
    score = 50.0
    if bull:
        if close > ema5:
            score += 12
        if close > vwap:
            score += 12
    else:
        if close < ema5:
            score += 12
        if close < vwap:
            score += 12
    score += (pb_q - 50) * 0.25
    score -= max(0, ext_risk - 40) * 0.3
    if len(candles) >= 5:
        closes = [float(c.get("close") or 0) for c in candles]
        if bull and closes[-1] > closes[-3]:
            score += 8
        elif not bull and closes[-1] < closes[-3]:
            score += 8
    return max(0.0, min(100.0, score))


def health_label(score: float) -> str:
    if score >= 80:
        return "Strong"
    if score >= 60:
        return "Healthy"
    if score >= 40:
        return "Weakening"
    if score >= 20:
        return "High Risk"
    return "Failure Risk"


def refresh_trade_state(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Update price, lifecycle, health, statuses, alerts for an active trade."""
    instrument_key = trade.get("instrument_key") or ""
    bull = _dir_bull(trade.get("direction"))
    discovery = trade.get("discovery_snapshot") or {}
    checklist = trade.get("checklist") or {}
    candles = _fetch_5m(instrument_key)
    alerts: List[Dict[str, str]] = list(trade.get("alerts") or [])

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ltp = _ltp(upstox, instrument_key)
    current_price = _resolve_current_price(trade, ltp, candles)
    structure_status = "Neutral"
    momentum_status = "Neutral"
    ema_status = "—"
    vwap_status = "—"

    hist = list(trade.get("lifecycle_history") or [])
    pb_q = float(discovery.get("pullback_quality_score") or 50)

    if candles and len(candles) >= 20:
        closes = [float(c.get("close") or 0) for c in candles]
        highs = [float(c.get("high") or 0) for c in candles]
        lows = [float(c.get("low") or 0) for c in candles]
        opens = [float(c.get("open") or 0) for c in candles]
        volumes = [float(c.get("volume") or 0) for c in candles]
        i = len(closes) - 1
        close = current_price or closes[i]
        vwap = cumulative_vwap(highs, lows, closes, volumes)[i]
        ema5 = ema_series(closes, 5)[i]
        atr = max(1e-6, sum(highs[j] - lows[j] for j in range(max(0, i - 13), i + 1)) / min(14, i + 1))
        pb_q = compute_pullback_quality(opens, highs, lows, closes, ema_series(closes, 5), atr, bull_dir=bull)
        ext_r = float(discovery.get("extension_risk_score") or 50)

        if bull:
            ema_status = "Above EMA5" if close > ema5 else "Below EMA5"
            vwap_status = "Above VWAP" if close > vwap else "Below VWAP"
            structure_status = "Supportive" if close > ema5 and close > vwap else "Weakening"
        else:
            ema_status = "Below EMA5" if close < ema5 else "Above EMA5"
            vwap_status = "Below VWAP" if close < vwap else "Above VWAP"
            structure_status = "Supportive" if close < ema5 and close < vwap else "Weakening"

        if closes[-1] > closes[-2] if bull else closes[-1] < closes[-2]:
            momentum_status = "Strengthening"
        else:
            momentum_status = "Weakening"

        health = compute_trade_health(
            bull=bull, close=close, vwap=vwap, ema5=ema5, pb_q=pb_q, ext_risk=ext_r, candles=candles
        )
        lifecycle = classify_lifecycle(
            discovery=discovery, health=health, checklist_flags=checklist, bull=bull
        )

        def _add_alert(level: str, msg: str):
            if not any(a.get("message") == msg for a in alerts[-8:]):
                alerts.append({"level": level, "message": msg, "at": datetime.now(IST).isoformat()})

        if close > ema5 and bull:
            _add_alert("positive", "Buyers reclaimed EMA")
        if pb_q >= 55:
            _add_alert("positive", "Pullback remained shallow")
        if momentum_status == "Weakening":
            _add_alert("warning", "Momentum weakening")
        if structure_status == "Weakening":
            _add_alert("warning", "EMA acceptance weakening")
        if health < 40:
            _add_alert("risk", "Structure deterioration increasing")
        if lifecycle == "Failed Structure":
            _add_alert("risk", "Failed reclaim behavior detected")

        if not hist or hist[-1].get("state") != lifecycle:
            hist.append({"state": lifecycle, "at": datetime.now(IST).isoformat()})
    else:
        health = float(trade.get("trade_health") or 50)
        lifecycle = trade.get("lifecycle_state") or "Early Transition"

    entry = float(trade.get("entry_price") or 0)
    lots = int(trade.get("lots") or 1)
    pnl = None
    if current_price and entry:
        mult = 1 if bull else -1
        pnl = (current_price - entry) * lots * mult

    return {
        "current_price": current_price,
        "trade_health": round(health, 1),
        "trade_health_label": health_label(health),
        "lifecycle_state": lifecycle,
        "structure_status": structure_status,
        "momentum_status": momentum_status,
        "ema_status": ema_status,
        "vwap_status": vwap_status,
        "unrealized_pnl": round(pnl, 2) if pnl is not None else None,
        "alerts": alerts[-12:],
        "lifecycle_history": hist,
        "updated_at": datetime.now(IST).isoformat(),
    }
