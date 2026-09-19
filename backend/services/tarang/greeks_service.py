"""GreeksService — prefer venue Greeks when valid; else Black-76 (MCX) / BS (crypto)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from backend.services.tarang.domain.types import OptionQuote
from backend.services.tarang import greeks as g
from backend.services.tarang.iv_normalize import normalize_iv


def _years_to_expiry(expiry_iso: str, now: Optional[datetime] = None) -> Optional[float]:
    now = now or datetime.now(timezone.utc)
    try:
        # Accept YYYY-MM-DD or full ISO
        s = expiry_iso.strip()
        if len(s) == 10:
            exp = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        else:
            exp = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    secs = (exp - now).total_seconds()
    if secs <= 0:
        return None
    return secs / (365.25 * 24 * 3600)


def _venue_greeks_valid(iv: Optional[float], delta: Optional[float]) -> bool:
    if iv is None or delta is None:
        return False
    if iv <= 0:
        return False
    ad = abs(delta)
    if ad < 0.01 or ad > 0.99:
        return False
    # placeholder ±1
    if ad >= 0.999:
        return False
    return True


def enrich_quote(
    q: OptionQuote,
    *,
    F_or_S: float,
    model: str = "black76",
    r: float = 0.0,
    now: Optional[datetime] = None,
) -> OptionQuote:
    """Mutate/return quote with normalized IV and filled Greeks."""
    iv_dec, iv_raw, iv_unit = normalize_iv(q.iv_raw if q.iv_raw is not None else q.iv, q.iv_unit)
    q.iv_raw = iv_raw if iv_raw is not None else q.iv
    q.iv_unit = iv_unit
    q.iv = iv_dec

    if _venue_greeks_valid(q.iv, q.delta):
        q.greeks_source = q.greeks_source or "venue"
        return q

    T = _years_to_expiry(q.expiry, now=now)
    mid = q.mid
    if mid is None and q.bid is not None and q.ask is not None:
        mid = 0.5 * (q.bid + q.ask)
        q.mid = mid
    if T is None or F_or_S <= 0:
        q.greeks_source = "unavailable"
        return q

    sigma = q.iv
    if (sigma is None or sigma <= 0) and mid and mid > 0:
        if model == "bs":
            sigma = g.implied_vol_bs(mid, F_or_S, q.strike, T, r, q.right)
        else:
            sigma = g.implied_vol_black76(mid, F_or_S, q.strike, T, q.right)
        if sigma:
            q.iv = sigma
            if q.iv_raw is None:
                q.iv_raw = sigma
                q.iv_unit = "decimal"

    if sigma and sigma > 0:
        if model == "bs":
            q.delta = g.bs_delta(F_or_S, q.strike, T, sigma, r, q.right)
            q.greeks_source = "bs"
        else:
            q.delta = g.black76_delta(F_or_S, q.strike, T, sigma, q.right)
            q.greeks_source = "black76"
    else:
        q.greeks_source = "unavailable"
    return q


def summarize_greeks_mix(quotes) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    for q in quotes:
        src = getattr(q, "greeks_source", None) or "unknown"
        counts[src] = counts.get(src, 0) + 1
    return counts
