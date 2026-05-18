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
from backend.services.sector_movers import get_sector_movers_cached
from backend.services.upstox_service import UpstoxService
from backend.services.vajra.indicators import cumulative_vwap, ema_series
from backend.services.vajra.job import _fetch_candles_for_tf
from backend.services.vajra.transition import compute_pullback_quality, compute_extension_risk

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
    try:
        q = upstox.get_market_quote_by_key(instrument_key)
        if q:
            p = float(q.get("last_price") or 0)
            return p if p > 0 else None
    except Exception:
        pass
    return None


def build_validation_preview(
    *,
    stock: str,
    direction: str,
    instrument_key: str,
    discovery_row: Dict[str, Any],
) -> Dict[str, Any]:
    """Auto-checks and warnings for Section B/C/D."""
    bull = _dir_bull(direction)
    candles = _fetch_5m(instrument_key)
    checklist: Dict[str, bool] = {}
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
            "checklist": checklist,
            "warnings": ["Insufficient 5m candle data for auto-validation."],
            "metrics": metrics,
            "auto_available": False,
        }

    opens = [float(c.get("open") or 0) for c in candles]
    highs = [float(c.get("high") or 0) for c in candles]
    lows = [float(c.get("low") or 0) for c in candles]
    closes = [float(c.get("close") or 0) for c in candles]
    volumes = [float(c.get("volume") or 0) for c in candles]
    i = len(closes) - 1
    close = closes[i]
    vwap_s = cumulative_vwap(highs, lows, closes, volumes)
    vwap = vwap_s[i]
    ema5 = ema_series(closes, 5)
    atr = max(1e-6, sum(highs[j] - lows[j] for j in range(max(0, i - 13), i + 1)) / min(14, i + 1))

    checklist["vwap_reclaimed"] = (close > vwap) if bull else (close < vwap)
    checklist["ema_reclaimed"] = (close > ema5[i]) if bull else (close < ema5[i])

    pb_q = compute_pullback_quality(opens, highs, lows, closes, ema5, atr, bull_dir=bull)
    impulse_hi = max(highs[max(0, i - 12) : i + 1])
    impulse_lo = min(lows[max(0, i - 12) : i + 1])
    impulse_rng = max(impulse_hi - impulse_lo, atr)
    pullback_depth = (impulse_hi - lows[i]) / impulse_rng if bull else (highs[i] - impulse_lo) / impulse_rng
    checklist["pullback_shallow"] = pullback_depth < 0.4

    recent = list(range(max(0, i - 4), i + 1))
    strong_bull = sum(1 for j in recent if closes[j] > opens[j] and (closes[j] - opens[j]) > atr * 0.35)
    upper_wicks = sum(
        1
        for j in recent
        if (highs[j] - max(closes[j], opens[j])) / max(highs[j] - lows[j], 1e-6) > 0.45
    )
    checklist["no_vertical_exhaustion"] = not (strong_bull >= 3 and upper_wicks >= 2) if bull else True

    bodies = [abs(closes[j] - opens[j]) for j in recent]
    wicks = [max(highs[j] - lows[j] - abs(closes[j] - opens[j]), 0) for j in recent]
    avg_body = sum(bodies) / len(bodies) if bodies else 0
    checklist["candle_spread_healthy"] = bodies[-1] > wicks[-1] and bodies[-1] >= avg_body * 0.85

    nearest_res = min(
        abs(close - impulse_hi),
        abs(close - max(highs)),
        abs(close - round(close / 50) * 50),
    )
    risk_unit = max(atr, close * 0.002)
    checklist["not_into_major_level"] = nearest_res > 0.7 * risk_unit

    if bull:
        checklist["reclaim_candle_strong"] = close > vwap and close >= highs[i] - (highs[i] - lows[i]) * 0.35
    else:
        checklist["reclaim_candle_strong"] = close < vwap and close <= lows[i] + (highs[i] - lows[i]) * 0.35

    vol_ma = sum(volumes[max(0, i - 19) : i + 1]) / min(20, i + 1)
    checklist["volume_acceptable"] = volumes[i] > vol_ma * 1.2 if vol_ma > 0 else False
    vwap_dist_pct = abs(close - vwap) / close * 100 if close else 99
    checklist["not_extended_vwap"] = vwap_dist_pct < 1.5

    try:
        u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        rows = build_dial_rows(u, basis="today")
        by_id = {str(r.get("id") or ""): r for r in rows}
        nifty_pct = float((by_id.get("nifty50") or {}).get("pct_change") or 0)
        bank_pct = float((by_id.get("banknifty") or {}).get("pct_change") or 0)
        if bull:
            checklist["market_structure_supportive"] = nifty_pct >= 0 and bank_pct >= 0
        else:
            checklist["market_structure_supportive"] = nifty_pct <= 0 and bank_pct <= 0
    except Exception:
        checklist["market_structure_supportive"] = False

    checklist["sector_not_conflicting"] = _sector_aligned(stock, bull)

    ext = float(discovery_row.get("extension_risk_score") or compute_extension_risk(
        close, vwap, ema5[i], atr, 50, highs, lows, closes, opens, bull_dir=bull
    ))
    if ext >= 65:
        warnings.append("Extension risk is elevated — late entry risk.")
    if vwap_dist_pct >= 1.5:
        warnings.append("Price is extended from VWAP.")
    if strong_bull >= 3 and bull:
        warnings.append("Large vertical move may already be in progress.")
    if not checklist.get("not_into_major_level"):
        warnings.append("Entry is close to major resistance/support.")
    if not checklist.get("ema_reclaimed"):
        warnings.append("EMA reclaim is weak or not confirmed.")
    if not checklist.get("pullback_shallow"):
        warnings.append("Pullback depth is deeper than ideal.")

    return {
        "checklist": checklist,
        "warnings": warnings,
        "metrics": metrics,
        "auto_available": True,
        "pullback_depth": round(pullback_depth, 3),
        "vwap_distance_pct": round(vwap_dist_pct, 3),
    }


def _sector_aligned(stock: str, bull: bool) -> bool:
    try:
        movers = get_sector_movers_cached(3)
        gainers = {str(x.get("symbol") or "").upper() for x in (movers.get("top_gainers") or [])}
        losers = {str(x.get("symbol") or "").upper() for x in (movers.get("top_losers") or [])}
        sym = stock.upper().replace(".NS", "")
        if bull:
            return sym in gainers or sym not in losers
        return sym in losers or sym not in gainers
    except Exception:
        return True


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
    current_price = ltp
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
