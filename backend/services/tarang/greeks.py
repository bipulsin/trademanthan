"""Black-76 (futures options) and Black-Scholes (spot/crypto) helpers."""
from __future__ import annotations

import math
from typing import Optional


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def black76_price(F: float, K: float, T: float, sigma: float, opt: str, df: float = 1.0) -> Optional[float]:
    if F <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    right = (opt or "").upper()
    if right in ("CE", "C", "CALL"):
        return df * (F * _norm_cdf(d1) - K * _norm_cdf(d2))
    if right in ("PE", "P", "PUT"):
        return df * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))
    return None


def black76_delta(F: float, K: float, T: float, sigma: float, opt: str, df: float = 1.0) -> Optional[float]:
    if F <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt_t)
    right = (opt or "").upper()
    if right in ("CE", "C", "CALL"):
        return df * _norm_cdf(d1)
    if right in ("PE", "P", "PUT"):
        return -df * _norm_cdf(-d1)
    return None


def black76_vega(F: float, K: float, T: float, sigma: float, df: float = 1.0) -> Optional[float]:
    if F <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt_t)
    return df * F * _norm_pdf(d1) * sqrt_t


def implied_vol_black76(
    price: float,
    F: float,
    K: float,
    T: float,
    opt: str,
    df: float = 1.0,
    lo: float = 1e-4,
    hi: float = 5.0,
    tol: float = 1e-6,
    max_iter: int = 80,
) -> Optional[float]:
    if price <= 0 or F <= 0 or K <= 0 or T <= 0:
        return None
    # Intrinsic floor
    right = (opt or "").upper()
    if right in ("CE", "C", "CALL"):
        intrinsic = max(0.0, df * (F - K))
    else:
        intrinsic = max(0.0, df * (K - F))
    if price < intrinsic * 0.999:
        return None
    a, b = lo, hi
    fa = (black76_price(F, K, T, a, opt, df) or 0.0) - price
    fb = (black76_price(F, K, T, b, opt, df) or 0.0) - price
    if fa * fb > 0:
        # expand hi once
        for _ in range(6):
            b *= 1.5
            fb = (black76_price(F, K, T, b, opt, df) or 0.0) - price
            if fa * fb <= 0:
                break
        else:
            return None
    for _ in range(max_iter):
        mid = 0.5 * (a + b)
        fm = (black76_price(F, K, T, mid, opt, df) or 0.0) - price
        if abs(fm) < tol or (b - a) < tol:
            return mid
        if fa * fm <= 0:
            b, fb = mid, fm
        else:
            a, fa = mid, fm
    return 0.5 * (a + b)


def bs_price(S: float, K: float, T: float, sigma: float, r: float, opt: str) -> Optional[float]:
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    df = math.exp(-r * T)
    right = (opt or "").upper()
    if right in ("CE", "C", "CALL"):
        return S * _norm_cdf(d1) - K * df * _norm_cdf(d2)
    if right in ("PE", "P", "PUT"):
        return K * df * _norm_cdf(-d2) - S * _norm_cdf(-d1)
    return None


def bs_delta(S: float, K: float, T: float, sigma: float, r: float, opt: str) -> Optional[float]:
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_t)
    right = (opt or "").upper()
    if right in ("CE", "C", "CALL"):
        return _norm_cdf(d1)
    if right in ("PE", "P", "PUT"):
        return -_norm_cdf(-d1)
    return None


def implied_vol_bs(
    price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    opt: str,
    lo: float = 1e-4,
    hi: float = 5.0,
    tol: float = 1e-6,
    max_iter: int = 80,
) -> Optional[float]:
    if price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    a, b = lo, hi
    fa = (bs_price(S, K, T, a, r, opt) or 0.0) - price
    fb = (bs_price(S, K, T, b, r, opt) or 0.0) - price
    if fa * fb > 0:
        for _ in range(6):
            b *= 1.5
            fb = (bs_price(S, K, T, b, r, opt) or 0.0) - price
            if fa * fb <= 0:
                break
        else:
            return None
    for _ in range(max_iter):
        mid = 0.5 * (a + b)
        fm = (bs_price(S, K, T, mid, r, opt) or 0.0) - price
        if abs(fm) < tol or (b - a) < tol:
            return mid
        if fa * fm <= 0:
            b, fb = mid, fm
        else:
            a, fa = mid, fm
    return 0.5 * (a + b)
