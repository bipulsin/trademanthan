"""Delta-band strike window for Kosmic Tarang chain snapshots.

Keep every strike with |delta| in [0.03, 0.97], or within ±2.5 σ of ATM
(using ATM IV and years to expiry), and never fewer than ATM±min_atm_window.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from backend.services.tarang.greeks import _norm_cdf

DELTA_ABS_MIN_DEFAULT = 0.03
DELTA_ABS_MAX_DEFAULT = 0.97
SIGMA_MULT_DEFAULT = 2.5
MIN_ATM_WINDOW_DEFAULT = 10
IV_FALLBACK = 0.45


def years_to_expiry(expiry_iso: Optional[str], now: Optional[datetime] = None) -> Optional[float]:
    if not expiry_iso:
        return None
    now = now or datetime.now(timezone.utc)
    try:
        s = str(expiry_iso).strip()[:10]
        exp = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None
    secs = (exp.replace(hour=18, minute=30) - now.astimezone(timezone.utc)).total_seconds()
    if secs <= 0:
        return max(1.0 / 365.25, 1e-6)
    return secs / (365.25 * 24 * 3600)


def _norm_ppf(p: float) -> float:
    p = min(max(float(p), 1e-9), 1.0 - 1e-9)
    lo, hi = -8.0, 8.0
    for _ in range(64):
        mid = 0.5 * (lo + hi)
        if _norm_cdf(mid) < p:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def black76_call_strike_for_delta(F: float, T: float, sigma: float, call_delta: float) -> Optional[float]:
    if F <= 0 or T <= 0 or sigma <= 0:
        return None
    d1 = _norm_ppf(min(max(float(call_delta), 1e-6), 1.0 - 1e-6))
    sqrt_t = math.sqrt(T)
    return F * math.exp(-d1 * sigma * sqrt_t + 0.5 * sigma * sigma * T)


def window_rule_dict(
    *,
    delta_abs_min: float = DELTA_ABS_MIN_DEFAULT,
    delta_abs_max: float = DELTA_ABS_MAX_DEFAULT,
    sigma_mult: float = SIGMA_MULT_DEFAULT,
    min_atm_window: int = MIN_ATM_WINDOW_DEFAULT,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "window_kind": "delta_window",
        "delta_abs_min": delta_abs_min,
        "delta_abs_max": delta_abs_max,
        "sigma_mult": sigma_mult,
        "min_atm_window": min_atm_window,
        "rule": "|delta| in [0.03,0.97] OR ±2.5σ ATM OR at least ATM±10",
    }
    if extra:
        out.update(extra)
    return out


def select_strikes(
    strikes: Sequence[float],
    *,
    F: Optional[float],
    atm_iv: Optional[float] = None,
    years: Optional[float] = None,
    deltas_by_strike: Optional[Dict[float, float]] = None,
    delta_abs_min: float = DELTA_ABS_MIN_DEFAULT,
    delta_abs_max: float = DELTA_ABS_MAX_DEFAULT,
    sigma_mult: float = SIGMA_MULT_DEFAULT,
    min_atm_window: int = MIN_ATM_WINDOW_DEFAULT,
) -> Tuple[Set[float], Dict[str, Any]]:
    """Return the strike set to snapshot plus metadata describing the rule used."""
    uniq = sorted({float(s) for s in strikes if s is not None})
    meta = window_rule_dict(
        delta_abs_min=delta_abs_min,
        delta_abs_max=delta_abs_max,
        sigma_mult=sigma_mult,
        min_atm_window=min_atm_window,
    )
    if not uniq:
        meta["n_strikes"] = 0
        return set(), meta

    keep: Set[float] = set()
    atm = None
    atm_idx = None
    if F and uniq:
        atm = min(uniq, key=lambda s: abs(s - F))
        atm_idx = uniq.index(atm)
        lo = max(0, atm_idx - int(min_atm_window))
        hi = min(len(uniq), atm_idx + int(min_atm_window) + 1)
        keep.update(uniq[lo:hi])
        meta["atm_strike"] = atm
        meta["n_atm_pm"] = hi - lo

    sigma = float(atm_iv) if atm_iv and atm_iv > 0 else IV_FALLBACK
    T = float(years) if years and years > 0 else None
    if F and T and sigma > 0:
        vol = sigma * math.sqrt(T)
        k_lo = F * math.exp(-sigma_mult * vol)
        k_hi = F * math.exp(sigma_mult * vol)
        d_hi = black76_call_strike_for_delta(F, T, sigma, delta_abs_max)  # ~0.97 → ITM, lower K
        d_lo = black76_call_strike_for_delta(F, T, sigma, delta_abs_min)  # ~0.03 → OTM, higher K
        bounds = [k_lo, k_hi]
        if d_hi:
            bounds.append(d_hi)
        if d_lo:
            bounds.append(d_lo)
        lo_k, hi_k = min(bounds), max(bounds)
        keep.update(s for s in uniq if lo_k - 1e-9 <= s <= hi_k + 1e-9)
        meta["sigma"] = sigma
        meta["years_to_expiry"] = T
        meta["k_lo"] = lo_k
        meta["k_hi"] = hi_k

    if deltas_by_strike:
        for s, d in deltas_by_strike.items():
            try:
                ad = abs(float(d))
            except (TypeError, ValueError):
                continue
            if delta_abs_min - 1e-9 <= ad <= delta_abs_max + 1e-9:
                keep.add(float(s))

    if not keep:
        keep = set(uniq)

    meta["n_strikes"] = len(keep)
    meta["n_listed"] = len(uniq)
    return keep, meta


def delta_snapshot_expiries(listed: Iterable[str], today: Optional[date] = None) -> List[str]:
    """3 nearest listed expiries (dailies) plus the next 2 Friday weeklies not in that set."""
    today = today or datetime.now(timezone.utc).date()
    future = sorted({str(e)[:10] for e in listed if str(e)[:10] >= today.isoformat()})
    dailies = future[:3]
    fridays = [e for e in future if date.fromisoformat(e).weekday() == 4]
    extra = [e for e in fridays if e not in dailies][:2]
    return sorted(set(dailies + extra))
