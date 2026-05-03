"""Expanded IV vs realised volatility context for Iron Condor checklist (standalone — no checklist import)."""

from __future__ import annotations

import logging
import math
import statistics
from typing import Any, Dict, List, Optional, Tuple

from backend.services.iron_condor_service import option_chain_underlying
from backend.services.upstox_service import upstox_service as vwap_service

logger = logging.getLogger(__name__)


def _mk(status: str, code: str, message: str, detail: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": status, "code": code, "message": message, "detail": detail or {}}


def _percentile_approx(value: float, population: List[float]) -> Optional[float]:
    if not population:
        return None
    filt = sorted(x for x in population if isinstance(x, (int, float)) and math.isfinite(x))
    if not filt:
        return None
    n = sum(1 for x in filt if x <= float(value))
    return round(float(n) / float(len(filt)) * 100.0, 2)


def _rolling_rv_ann(candles: List[Dict[str, Any]], *, window: int = 10) -> Tuple[Optional[float], List[float]]:
    closes: List[float] = []
    for c in candles or []:
        try:
            lp = float(c.get("close") or c.get("last_price") or 0)
        except Exception:
            continue
        if lp > 0:
            closes.append(lp)
    if len(closes) < window + 30:
        return None, []
    lr: List[float] = []
    for i in range(1, len(closes)):
        try:
            lr.append(math.log(closes[i] / closes[i - 1]))
        except Exception:
            continue
    rvs: List[float] = []
    for i in range(window, len(lr)):
        seg = lr[i - window : i]
        try:
            rvs.append(statistics.pstdev(seg) * math.sqrt(252.0))
        except Exception:
            continue
    if not rvs:
        return None, []
    return rvs[-1], rvs


def atm_iv_metric_from_chain(chain_payload: Any) -> Tuple[Optional[float], List[float]]:
    payload = chain_payload
    if isinstance(chain_payload, dict) and chain_payload.get("status") == "success":
        payload = chain_payload.get("data") or chain_payload
    strikes = payload.get("strikes") if isinstance(payload, dict) else None
    ivs: List[float] = []
    if not isinstance(strikes, list):
        return None, ivs
    mids = []
    for sd in strikes:
        if isinstance(sd, dict) and sd.get("strike_price") is not None:
            try:
                mids.append(float(sd["strike_price"]))
            except Exception:
                continue
    if not mids:
        return None, ivs
    mid = sorted(mids)[len(mids) // 2]
    for sd in strikes:
        if not isinstance(sd, dict):
            continue
        try:
            if abs(float(sd.get("strike_price") or 0) - mid) > mid * 0.035 + 40:
                continue
        except Exception:
            continue
        for side in ("call_options", "put_options"):
            node = sd.get(side)
            od = node.get("market_data", node) if isinstance(node, dict) else None
            if isinstance(od, dict):
                iv_raw = od.get("iv") or od.get("implied_volatility")
                try:
                    if iv_raw is not None:
                        ivs.append(float(iv_raw))
                except (TypeError, ValueError):
                    pass
    if not ivs:
        return None, ivs
    med_iv = sorted(ivs)[len(ivs) // 2]
    return med_iv, ivs


def iv_context_chip(symbol: str) -> Dict[str, Any]:
    """
    Approximates elevated / compressed premium environment:
      • IV dispersion percentile on current chain (IVR-style proxy vs snapshot range)
      • 10‑day realised vol vs trailing distribution (history-aware)
    True 52‑week ATM IV series is broker-dependent; RV history uses spot closes.
    """
    api_sym = option_chain_underlying(symbol)
    chain = vwap_service.get_option_chain(api_sym)
    atm_iv, all_iv = atm_iv_metric_from_chain(chain)

    ivr_dispersion_pct: Optional[float] = None
    if atm_iv is not None and len(all_iv) >= 14:
        lo, hi = min(all_iv), max(all_iv)
        if hi > lo + 1e-9:
            ivr_dispersion_pct = (atm_iv - lo) / (hi - lo) * 100.0

    rv_now: Optional[float] = None
    rv_hist_pct: Optional[float] = None
    eq_key = vwap_service.get_instrument_key(api_sym)
    if eq_key:
        try:
            cnd = (
                vwap_service.get_historical_candles_by_instrument_key(
                    eq_key, interval="days/1", days_back=320
                )
                or []
            )
            rows = [dict(x) for x in cnd if isinstance(x, dict)]
            rv_now, rv_series = _rolling_rv_ann(rows)
            hist_pool = rv_series[:-42] if len(rv_series) > 60 else rv_series[:-10]
            if rv_now is not None and hist_pool:
                rv_hist_pct = _percentile_approx(rv_now, hist_pool)
        except Exception as e:
            logger.info("iron_condor_iv_vol: RV skipped %s: %s", symbol, e)

    detail = {
        "atm_iv_med": atm_iv,
        "ivr_dispersion_proxy_pct": round(ivr_dispersion_pct, 2) if ivr_dispersion_pct is not None else None,
        "rv_ann_10d": round(rv_now, 4) if rv_now else None,
        "rv_hist_percentile": rv_hist_pct,
        "chain_iv_samples": len(all_iv),
    }

    if ivr_dispersion_pct is None and rv_hist_pct is None:
        return _mk(
            "WARN",
            "IV_VOL",
            "IV / volatility context unavailable (chain or equity history incomplete). Review manually.",
            detail,
        )

    warns_msg: List[str] = []

    # Cheap IV wing vs entire chain ⇒ structurally suppressed short-vol payoff.
    if ivr_dispersion_pct is not None and ivr_dispersion_pct < 30:
        warns_msg.append("ATM IV skews LOW vs snapshot chain range (premium selling yield thin).")

    # Quiet coil: RV floor vs own history ⇒ gap / expansion tail risk relative to credited premium.
    if rv_hist_pct is not None and rv_hist_pct < 22:
        warns_msg.append("10d realised vol is subdued vs trailing history — gap / expansion tail risk.")

    # Informational linkage: comparatively rich IV vs realised — not auto PASS/FAIL.
    iv_rv_gap = None
    if atm_iv and rv_now and rv_now > 1e-6:
        iv_rv_gap = atm_iv / rv_now

    detail["iv_to_rv_ratio"] = round(iv_rv_gap, 3) if iv_rv_gap else None

    if warns_msg:
        return _mk("WARN", "IV_VOL", " ".join(warns_msg[:2]), detail)

    extras = []
    if ivr_dispersion_pct is not None:
        extras.append("Chain IV dispersion proxy ~{:.0f}%.".format(ivr_dispersion_pct))
    if rv_hist_pct is not None:
        extras.append("RV percentile ~{:.0f}%.".format(rv_hist_pct))

    return _mk(
        "PASS",
        "IV_VOL",
        " ".join(extras) if extras else "IV / realised-vol posture acceptable at snapshot.",
        detail,
    )
