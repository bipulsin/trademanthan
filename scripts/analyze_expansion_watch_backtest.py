#!/usr/bin/env python3
"""Expansion Watch full-universe backtest (read-only).

Gate: VWAP slope steepening (THRESHOLD_VWAP_SLOPE) + EMA5/EMA10 aligned for
2+ confirmed 10m closes + not extended beyond ATR multiple from breakout bar.

Compares hit follow-through vs baseline with Wilson CI / credibility labels
(same framework as kavach_momentum_ignition_validate).

Do NOT set EXPANSION_WATCH_LIVE=1 until this clears credible_positive.

Run:
  PYTHONPATH=. python3 scripts/analyze_expansion_watch_backtest.py
  PYTHONPATH=. python3 scripts/analyze_expansion_watch_backtest.py --days 20 --max-symbols 80
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytz

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402
from backend.services.kavach_momentum_ignition_validate import (  # noqa: E402
    _credibility_label,
    _wilson_ci,
)
from backend.services.relative_strength_scanner import _f, _sorted_candles  # noqa: E402
from backend.services.rs_conviction_config import get_config  # noqa: E402
from backend.services.rs_expansion_watch import (  # noqa: E402
    ALERT_TIER,
    DEFAULT_ATR_EXT_MAX,
    EMA_ALIGN_BARS,
    evaluate_candles_for_expansion,
    fno_universe,
)
from backend.services.upstox_service import UpstoxService  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")
THROTTLE_SEC = 0.25
FT_ATR = 0.5  # follow-through: +0.5 ATR in signal direction within remainder of day


def _parse_ist_dt(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if hasattr(ts, "astimezone"):
        return ts.astimezone(IST)
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None
    return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)


def _session_dates(db, days: int) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT scan_time::date AS d
            FROM relative_strength_snapshot
            WHERE scan_time::date >= (CURRENT_DATE - :n)
            ORDER BY d DESC
            """
        ),
        {"n": days},
    ).fetchall()
    return [str(r.d) for r in rows if r.d]


def _atr_pct_guess(candles: List[Dict]) -> float:
    if len(candles) < 20:
        return 1.0
    ranges = []
    for c in candles[-40:]:
        h = _f(c.get("high"))
        l = _f(c.get("low"))
        if h and l and h > l:
            ranges.append(h - l)
    if not ranges:
        return 1.0
    avg = sum(ranges) / len(ranges)
    px = _f(candles[-1].get("close"), 1.0)
    return max(0.2, min(5.0, (avg / px) * 100.0 * 5))  # rough daily-ish


def _load_candles(upx: UpstoxService, ikey: str, day: str) -> List[Dict]:
    try:
        raw = (
            upx.get_historical_candles_by_instrument_key(
                ikey, interval="minutes/5", days_back=3
            )
            or []
        )
    except Exception:
        return []
    # Filter to session day
    out = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        ts = _parse_ist_dt(c.get("timestamp"))
        if ts and ts.strftime("%Y-%m-%d") == day:
            out.append(c)
    return _sorted_candles(out)


def _instrument_map(db) -> Dict[str, str]:
    rows = db.execute(
        text(
            """
            SELECT UPPER(stock) AS symbol, currmth_future_instrument_key AS ikey
            FROM arbitrage_master
            WHERE stock IS NOT NULL AND currmth_future_instrument_key IS NOT NULL
            """
        )
    ).fetchall()
    return {str(r.symbol).upper(): r.ikey for r in rows if r.symbol and r.ikey}


def _follow_through(candles: List[Dict], hit: Dict[str, Any], atr_pct: float) -> bool:
    bar_at = _parse_ist_dt(hit.get("bar_at"))
    if not bar_at:
        return False
    direction = hit.get("direction") or "LONG"
    entry = float(hit.get("confirmed_close") or 0)
    atr = entry * max(atr_pct, 0.001) / 100.0
    if atr <= 0 or entry <= 0:
        return False
    target = entry + FT_ATR * atr if direction == "LONG" else entry - FT_ATR * atr
    for c in candles:
        ts = _parse_ist_dt(c.get("timestamp"))
        if not ts or ts <= bar_at:
            continue
        close = _f(c.get("close"))
        if close is None:
            continue
        if direction == "LONG" and close >= target:
            return True
        if direction == "SHORT" and close <= target:
            return True
    return False


def _metric(hits: int, n: int, baseline: Optional[float]) -> Dict[str, Any]:
    lo, hi = _wilson_ci(hits, n) if n else (None, None)
    rate = (hits / n) if n else None
    lift = (rate - baseline) if rate is not None and baseline is not None else None
    return {
        "n": n,
        "hits": hits,
        "rate": round(rate, 4) if rate is not None else None,
        "wilson_ci_95": [lo, hi],
        "baseline_rate": baseline,
        "lift_vs_baseline": round(lift, 4) if lift is not None else None,
        "credibility": _credibility_label(hits, n, baseline),
    }


def run(days: int, max_symbols: int, atr_ext_max: float) -> Dict[str, Any]:
    db = SessionLocal()
    upx = UpstoxService()
    cfg = get_config()
    try:
        dates = _session_dates(db, days)
        imap = _instrument_map(db)
        syms = fno_universe(db)[:max_symbols]
        signal_hits = 0
        signal_n = 0
        baseline_hits = 0
        baseline_n = 0
        samples: List[Dict[str, Any]] = []

        for day in dates:
            for sym in syms:
                ikey = imap.get(sym)
                if not ikey:
                    continue
                candles = _load_candles(upx, ikey, day)
                time.sleep(THROTTLE_SEC)
                if len(candles) < 40:
                    continue
                atr_pct = _atr_pct_guess(candles)
                # Baseline: mid-session bar follow-through without gates
                mid = candles[len(candles) // 2]
                mid_close = _f(mid.get("close"))
                if mid_close:
                    fake = {
                        "bar_at": mid.get("timestamp"),
                        "confirmed_close": mid_close,
                        "direction": "LONG",
                    }
                    baseline_n += 1
                    if _follow_through(candles, fake, atr_pct):
                        baseline_hits += 1
                    fake_s = dict(fake)
                    fake_s["direction"] = "SHORT"
                    baseline_n += 1
                    if _follow_through(candles, fake_s, atr_pct):
                        baseline_hits += 1

                for side in ("LONG", "SHORT"):
                    hit = evaluate_candles_for_expansion(
                        candles,
                        side=side,
                        atr_daily_pct=atr_pct,
                        atr_ext_max=atr_ext_max,
                        cfg=cfg,
                    )
                    if not hit:
                        continue
                    signal_n += 1
                    ok = _follow_through(candles, hit, atr_pct)
                    if ok:
                        signal_hits += 1
                    samples.append(
                        {
                            "session_date": day,
                            "symbol": sym,
                            "direction": side,
                            "follow_through": ok,
                            "vwap_slope_score": hit.get("vwap_slope_score"),
                            "extension_atr": hit.get("extension_atr"),
                        }
                    )

        baseline = (baseline_hits / baseline_n) if baseline_n else None
        result = {
            "tier": ALERT_TIER,
            "generated_at": datetime.now(IST).isoformat(),
            "params": {
                "days": days,
                "max_symbols": max_symbols,
                "ema_align_bars": EMA_ALIGN_BARS,
                "atr_ext_max": atr_ext_max,
                "follow_through_atr": FT_ATR,
                "default_atr_ext_max": DEFAULT_ATR_EXT_MAX,
            },
            "signal": _metric(signal_hits, signal_n, baseline),
            "baseline": {
                "n": baseline_n,
                "hits": baseline_hits,
                "rate": round(baseline, 4) if baseline is not None else None,
            },
            "live_recommendation": (
                "ENABLE_LIVE"
                if _credibility_label(signal_hits, signal_n, baseline) == "credible_positive"
                else "KEEP_LIVE_OFF"
            ),
            "sample_count": len(samples),
            "samples_head": samples[:40],
        }
        return result
    finally:
        db.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Expansion Watch backtest")
    ap.add_argument("--days", type=int, default=15)
    ap.add_argument("--max-symbols", type=int, default=60)
    ap.add_argument("--atr-ext-max", type=float, default=DEFAULT_ATR_EXT_MAX)
    ap.add_argument(
        "--out",
        type=str,
        default=str(_ROOT / "docs/diagnostics/EXPANSION_WATCH_BACKTEST.json"),
    )
    args = ap.parse_args()
    result = run(args.days, args.max_symbols, args.atr_ext_max)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: result[k] for k in ("live_recommendation", "signal", "baseline", "params")}, indent=2))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
