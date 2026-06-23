"""
Smart Futures signal gates: unified gate price, VWAP confirmation, invalidation, publish status.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from backend.services.smart_futures_config import (
    BOS_INVALIDATION_MINUTES,
    SENTIMENT_LONG_BLOCK_BELOW,
    SENTIMENT_SHORT_BLOCK_ABOVE,
    VWAP_CONFIRM_BARS,
    VOLUME_SURGE_EARLY_CAP_MINUTES,
    VOLUME_SURGE_EARLY_MAX,
)
from backend.services.smart_futures_exit import (
    RECLAIM_ENTRY_SCORE_THRESHOLD,
    compute_reclaim_probability_score,
    is_entry_permitted,
)


def gate_price_from_session_m5(m5_same_session: Sequence[dict]) -> Optional[float]:
    """Last completed same-session 5m close — single price for picker gates and entry."""
    if not m5_same_session:
        return None
    try:
        px = float(m5_same_session[-1].get("close") or 0.0)
    except (TypeError, ValueError):
        return None
    return px if px > 0 else None


def scan_bar_ohlc(m5_same_session: Sequence[dict]) -> tuple[Optional[float], Optional[float]]:
    if not m5_same_session:
        return None, None
    bar = m5_same_session[-1]
    try:
        hi = float(bar.get("high") or 0.0)
        lo = float(bar.get("low") or 0.0)
    except (TypeError, ValueError):
        return None, None
    return (hi if hi > 0 else None, lo if lo > 0 else None)


def cap_early_session_volume_surge(bar_end: datetime, volume_surge: float) -> float:
    """Cap inflated early-session surge ratios (tiny elapsed fraction)."""
    try:
        vs = float(volume_surge)
    except (TypeError, ValueError):
        return volume_surge
    if bar_end.tzinfo is None:
        ist = bar_end
    else:
        ist = bar_end.astimezone(__import__("pytz").timezone("Asia/Kolkata"))
    mins = ist.hour * 60 + ist.minute - (9 * 60 + 15)
    if mins < int(VOLUME_SURGE_EARLY_CAP_MINUTES):
        return min(vs, float(VOLUME_SURGE_EARLY_MAX))
    return vs


def vwap_side_confirmed(side: str, m5_same_session: Sequence[dict], vwap: float, n_bars: int | None = None) -> bool:
    """Require N consecutive 5m closes on the favorable side of session VWAP."""
    n = int(n_bars if n_bars is not None else VWAP_CONFIRM_BARS)
    if n < 1 or not m5_same_session or vwap <= 0:
        return False
    if len(m5_same_session) < n:
        return False
    sd = str(side or "").strip().upper()
    last = list(m5_same_session)[-n:]
    try:
        closes = [float(b.get("close") or 0.0) for b in last]
    except (TypeError, ValueError):
        return False
    if any(c <= 0 for c in closes):
        return False
    if sd == "LONG":
        return all(c > float(vwap) for c in closes)
    if sd == "SHORT":
        return all(c < float(vwap) for c in closes)
    return False


def sentiment_blocks_side(side: str, combined_sentiment: float) -> Optional[str]:
    sd = str(side or "").strip().upper()
    try:
        comb = float(combined_sentiment)
    except (TypeError, ValueError):
        return None
    if sd == "LONG" and comb < float(SENTIMENT_LONG_BLOCK_BELOW):
        return f"sentiment_blocks_long ({comb:.3f} < {SENTIMENT_LONG_BLOCK_BELOW})"
    if sd == "SHORT" and comb > float(SENTIMENT_SHORT_BLOCK_ABOVE):
        return f"sentiment_blocks_short ({comb:.3f} > {SENTIMENT_SHORT_BLOCK_ABOVE})"
    return None


def evaluate_publish_signal_status(
    *,
    side: str,
    entry_price: float,
    vwap15: float,
    entry_at_ist: datetime,
    m5_session: Sequence[dict],
    sector_score: Optional[float] = None,
) -> str:
    """
    Return ACTIONABLE when all entry gates pass at publish; else SUPPRESSED.
  """
    gate = is_entry_permitted(
        side=side,
        entry_price=entry_price,
        vwap15=vwap15,
        entry_at_ist=entry_at_ist,
        m5_session=m5_session,
        sector_score=sector_score,
    )
    return "ACTIONABLE" if bool(gate.get("permitted")) else "SUPPRESSED"


def apply_live_signal_invalidation(
    row: Dict[str, Any],
    m5_session: Sequence[dict],
    now_ist: datetime,
) -> None:
    """
    Mutate row with signal_status / signal_status_reason for unpurchased trend rows.
    """
    ost = str(row.get("order_status") or "").strip().lower()
    if ost in ("bought", "sold"):
        return

    side = str(row.get("side") or "").strip().upper()
    if side not in ("LONG", "SHORT"):
        return

    cur_status = str(row.get("signal_status") or "").strip().upper()
    if cur_status in ("STALE", "INVALIDATED"):
        row["signal_status_reason"] = row.get("signal_status_reason") or f"Signal {cur_status.lower()}"
        return

    try:
        ltp = float(row.get("current_ltp") or 0.0)
    except (TypeError, ValueError):
        ltp = 0.0
    vwap = row.get("m15_vwap_at_scan")
    if vwap is None:
        vwap = row.get("m15_vwap")
    try:
        vw = float(vwap or 0.0)
    except (TypeError, ValueError):
        vw = 0.0

    entry_at = row.get("entry_at")
    entry_dt = None
    if entry_at is not None:
        if isinstance(entry_at, datetime):
            entry_dt = entry_at
            if entry_dt.tzinfo is None:
                entry_dt = __import__("pytz").timezone("Asia/Kolkata").localize(entry_dt)
        else:
            from backend.routers.smart_futures_stub import _parse_any_ts_to_ist

            entry_dt = _parse_any_ts_to_ist(entry_at)

    mins_since: Optional[float] = None
    if entry_dt is not None:
        delta = now_ist - entry_dt.astimezone(now_ist.tzinfo)
        mins_since = delta.total_seconds() / 60.0

    try:
        scan_lo = float(row.get("scan_bar_low") or 0.0)
        scan_hi = float(row.get("scan_bar_high") or 0.0)
    except (TypeError, ValueError):
        scan_lo, scan_hi = 0.0, 0.0

    # Break-of-structure within window after scan.
    if (
        ltp > 0
        and mins_since is not None
        and mins_since <= float(BOS_INVALIDATION_MINUTES)
    ):
        if side == "LONG" and scan_lo > 0 and ltp < scan_lo:
            row["signal_status"] = "INVALIDATED"
            row["signal_status_reason"] = (
                f"Price broke below scan 5m low ({scan_lo:.2f}) within {int(BOS_INVALIDATION_MINUTES)}m"
            )
            row["trend_continuation"] = "INVALIDATED"
            return
        if side == "SHORT" and scan_hi > 0 and ltp > scan_hi:
            row["signal_status"] = "INVALIDATED"
            row["signal_status_reason"] = (
                f"Price broke above scan 5m high ({scan_hi:.2f}) within {int(BOS_INVALIDATION_MINUTES)}m"
            )
            row["trend_continuation"] = "INVALIDATED"
            return

    # Adverse VWAP + weak reclaim → invalidate (was only suppressing Order before).
    if ltp > 0 and vw > 0:
        detail = compute_reclaim_probability_score(side, ltp, vw, m5_session, sector_score=row.get("sector_score"))
        adverse = bool(detail.get("vwap_adverse"))
        score = detail.get("score")
        if adverse and score is not None and float(score) < float(RECLAIM_ENTRY_SCORE_THRESHOLD):
            row["signal_status"] = "INVALIDATED"
            row["signal_status_reason"] = (
                f"Adverse vs VWAP with weak reclaim ({float(score):.0f}/{int(RECLAIM_ENTRY_SCORE_THRESHOLD)})"
            )
            row["trend_continuation"] = "INVALIDATED"
            return

    if not bool(row.get("entry_gate_permitted")):
        row["signal_status"] = "SUPPRESSED"
        reasons = row.get("entry_gate_reasons") or []
        row["signal_status_reason"] = "; ".join(str(x) for x in reasons[:2]) if reasons else "Entry gates not passed"
        row["trend_continuation"] = "SUPPRESSED"
        return

    if not cur_status:
        row["signal_status"] = "ACTIONABLE"
        row["signal_status_reason"] = ""
        row["trend_continuation"] = "Yes"


def signal_status_tooltip(row: Dict[str, Any]) -> str:
    side = str(row.get("side") or "")
    scan = str(row.get("scan_trigger") or "")
    ep = row.get("entry_price")
    vw = row.get("m15_vwap_at_scan") or row.get("vwap_at_trigger") or row.get("m15_vwap")
    status = str(row.get("signal_status") or "ACTIONABLE")
    reason = str(row.get("signal_status_reason") or "")
    parts = [f"{side} from {scan} scan"]
    if ep is not None and vw is not None:
        try:
            parts.append(f"entry {float(ep):.2f} vs VWAP {float(vw):.2f}")
        except (TypeError, ValueError):
            pass
    parts.append(f"status: {status}")
    if reason:
        parts.append(reason)
    return " — ".join(parts)
