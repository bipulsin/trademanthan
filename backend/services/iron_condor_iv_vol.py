"""Expanded IV vs realised volatility context for Iron Condor checklist (standalone — no checklist import)."""

from __future__ import annotations

import logging
import math
import statistics
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.iron_condor_service import option_chain_underlying
from backend.services.upstox_service import upstox_service as vwap_service

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


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


def iv_context_chip(
    symbol: str,
    db: Optional[Any] = None,
    *,
    precached_daily: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Realised‑vol percentile vs trailing history (+ optional India VIX / RV linkage).
    Skips option‑chain scrape: uses Iron Condor pre‑market equity close cache when available.
    """
    sym_u = (symbol or "").strip().upper()
    api_sym = option_chain_underlying(sym_u)

    india_v_snap: Optional[float] = None
    closes_cached: Optional[List[float]] = None
    if db is not None:
        try:
            from backend.services.iron_condor_snapshot_cache import (
                read_india_vix_session,
                read_underlying_atr_closes_session,
            )

            td = datetime.now(IST).date()
            india_v_snap = read_india_vix_session(db, td)
            _atr_gap, closes_cached = read_underlying_atr_closes_session(db, td, sym_u)
        except Exception as e:
            logger.debug("iron_condor_iv_vol: cache read skipped %s", e)

    rv_now: Optional[float] = None
    rv_hist_pct: Optional[float] = None
    rows_for_rv: List[Dict[str, Any]] = []
    if closes_cached and len(closes_cached) >= 42:
        rows_for_rv = [{"close": float(c)} for c in closes_cached if c is not None]
    elif precached_daily and len(precached_daily) >= 42:
        rows_for_rv = [dict(x) for x in precached_daily if isinstance(x, dict)]
    eq_key = vwap_service.get_instrument_key(api_sym)
    if len(rows_for_rv) < 42 and eq_key:
        try:
            cnd = (
                vwap_service.get_historical_candles_by_instrument_key(
                    eq_key, interval="days/1", days_back=320
                )
                or []
            )
            rows_for_rv = [dict(x) for x in cnd if isinstance(x, dict)]
        except Exception as e:
            logger.info("iron_condor_iv_vol: RV candles skipped %s: %s", symbol, e)
    if rows_for_rv:
        try:
            rv_now, rv_series = _rolling_rv_ann(rows_for_rv)
            hist_pool = rv_series[:-42] if len(rv_series) > 60 else rv_series[:-10]
            if rv_now is not None and hist_pool:
                rv_hist_pct = _percentile_approx(rv_now, hist_pool)
        except Exception as e:
            logger.info("iron_condor_iv_vol: RV compute skipped %s: %s", symbol, e)

    if india_v_snap is None:
        try:
            vk = getattr(vwap_service, "INDIA_VIX_KEY", "NSE_INDEX|India VIX")
            qq = vwap_service.get_market_quote_by_key(vk) or {}
            ll = qq.get("last_price") or qq.get("ltp")
            india_v_snap = float(ll) if ll is not None and float(ll) > 0 else None
        except Exception:
            pass

    ix_rv = None
    if india_v_snap is not None and rv_now is not None and rv_now > 1e-6:
        ix_rv = float(india_v_snap) / float(rv_now)

    closes_src = "live_or_short"
    if closes_cached and len(closes_cached or []) >= 42:
        closes_src = "prefetch_db"
    elif precached_daily and len(precached_daily) >= 42:
        closes_src = "checklist_bundle"

    detail = {
        "atm_iv_med": None,
        "ivr_dispersion_proxy_pct": None,
        "rv_ann_10d": round(rv_now, 4) if rv_now else None,
        "rv_hist_percentile": rv_hist_pct,
        "chain_iv_samples": 0,
        "india_vix_snapshot": round(india_v_snap, 3) if india_v_snap else None,
        "iv_to_rv_ratio": round(ix_rv, 3) if ix_rv else None,
        "closes_source": closes_src,
    }

    if rv_hist_pct is None and rv_now is None:
        return _mk(
            "WARN",
            "IV_VOL",
            "Realised‑vol context unavailable (cache + live history incomplete). Review manually.",
            detail,
        )

    warns_msg: List[str] = []
    if rv_hist_pct is not None and rv_hist_pct < 22:
        warns_msg.append("10d realised vol is subdued vs trailing history — gap / expansion tail risk.")

    if warns_msg:
        return _mk("WARN", "IV_VOL", " ".join(warns_msg[:2]), detail)

    extras: List[str] = []
    if rv_hist_pct is not None:
        extras.append("RV percentile ~{:.0f}%.".format(rv_hist_pct))
    if ix_rv is not None:
        extras.append("India VIX / 10d RV ~ {:.2f}.".format(ix_rv))

    return _mk(
        "PASS",
        "IV_VOL",
        " ".join(extras) if extras else "Realised‑vol posture acceptable at snapshot (no chain IV scrape).",
        detail,
    )
