#!/usr/bin/env python3
"""Post-process Fast Watch follow-through JSON + alternate ENTRY_REF=open sensitivity."""
from __future__ import annotations

import json
import math
import sys
import time
from collections import defaultdict
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.database import SessionLocal  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = dtime(9, 15)
SESSION_CLOSE = dtime(15, 30)
PRE_FLAG_FLOOR_PCT = 0.0005


def _parse_ist(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)


def _f(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _wilson(s, n, z=1.96):
    if n <= 0:
        return (0.0, 0.0)
    p = s / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((centre - margin) / denom, (centre + margin) / denom)


def rate(rows, key):
    n = len(rows)
    s = sum(1 for r in rows if r.get(key))
    lo, hi = _wilson(s, n)
    return {"count": s, "n": n, "pct": round(100 * s / n, 1) if n else None, "wilson95": [round(100 * lo, 1), round(100 * hi, 1)]}


def pctile(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[int(round((len(xs) - 1) * p))], 3)


def main():
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/fast_watch_followthrough.json")
    data = json.loads(src.read_text())
    rows = [r for r in data["rows"] if "remaining_mult" in r]
    core = [r for r in rows if not r.get("near_extreme")]

    print("=== PRIMARY (ENTRY_REF=session extreme to flag) ===")
    print("n", len(rows), "core", len(core))
    print("remaining>=4x", rate(core, "hit_4x"))
    print("remaining>=5x", rate(core, "hit_5x"))
    print("fizzle<1x", rate(core, "fizzle_fp"))
    print("delayed>=3x", rate(core, "captured_ge_3x"))
    print("delayed>=4x", rate(core, "captured_ge_4x"))
    print("med remaining_mult", pctile([r["remaining_mult"] for r in core], 0.5))
    print("med captured_mult", pctile([r["captured_mult"] for r in core], 0.5))

    by = defaultdict(list)
    for r in core:
        by[r["d"]].append(r)
    print("\n=== BY DAY ===")
    for day in sorted(by):
        rs = by[day]
        print(
            day,
            "n",
            len(rs),
            "ge3",
            rate(rs, "captured_ge_3x")["pct"],
            "fizzle",
            rate(rs, "fizzle_fp")["pct"],
            "med_rem",
            pctile([r["remaining_mult"] for r in rs], 0.5),
        )

    # Absolute move diagnostics: remaining as % of price and vs ATR proxy (session range)
    abs_ok_05 = 0
    abs_ok_10 = 0
    for r in core:
        px = r["p_flag"]
        rem_pct = 100.0 * r["remaining"] / px if px else 0
        if rem_pct >= 0.5:
            abs_ok_05 += 1
        if rem_pct >= 1.0:
            abs_ok_10 += 1
    n = len(core)
    print("\n=== ABSOLUTE SAME-SESSION REMAINING (not multiple) ===")
    print("remaining>=0.5% of price", round(100 * abs_ok_05 / n, 1), f"({abs_ok_05}/{n})")
    print("remaining>=1.0% of price", round(100 * abs_ok_10 / n, 1), f"({abs_ok_10}/{n})")

    # Recompute with ENTRY_REF = session open (first bar open)
    print("\n=== SENSITIVITY: ENTRY_REF = session open ===")
    from backend.config import settings
    from backend.services.relative_strength_scanner import CANDLE_INTERVAL, _sorted_candles
    from backend.services.upstox_service import UpstoxService

    db = SessionLocal()
    upx = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    ikeys = {}
    for r in db.execute(text("SELECT stock, currmth_future_instrument_key FROM arbitrage_master")).fetchall():
        if r.currmth_future_instrument_key:
            ikeys[r.stock.upper()] = str(r.currmth_future_instrument_key).strip()

    # Group flags by symbol-day from original payload symbols
    flags_meta = []
    for r in rows:
        flags_meta.append(r)

    candle_cache = {}
    open_rows = []
    for r in flags_meta:
        key = (r["symbol"], r["d"])
        if key not in candle_cache:
            ik = ikeys.get(r["symbol"].upper())
            if not ik:
                candle_cache[key] = []
                continue
            try:
                raw = upx.get_historical_candles_by_instrument_key(
                    ik, interval=CANDLE_INTERVAL, days_back=5, range_end_date=date.fromisoformat(r["d"])
                )
                candle_cache[key] = _sorted_candles(raw) if raw else []
                time.sleep(0.15)
            except Exception as exc:
                print("err", key, exc)
                candle_cache[key] = []
        bars = candle_cache[key]
        sess = []
        for c in bars:
            ts = _parse_ist(c.get("timestamp") or c.get("time"))
            if ts is None or ts.strftime("%Y-%m-%d") != r["d"]:
                continue
            if ts.time() < SESSION_OPEN or ts.time() > SESSION_CLOSE:
                continue
            sess.append({**c, "_ts": ts})
        sess.sort(key=lambda x: x["_ts"])
        if len(sess) < 6:
            continue
        flip_at = _parse_ist(r["first_flip_at"])
        pre = [b for b in sess if b["_ts"] <= flip_at]
        post = [b for b in sess if b["_ts"] >= (pre[-1]["_ts"] if pre else flip_at)]
        if not pre or not post:
            continue
        p_flag = r["p_flag"]
        direction = r["direction"]
        entry_ref = _f(sess[0].get("open"))
        if direction == "SHORT":
            peak = min(_f(b.get("low")) for b in post)
            remaining = p_flag - peak
            pre_flag = abs(entry_ref - p_flag)
            # delayed already in r
            captured = r["p_delayed"] - min(
                (_f(b.get("low")) for b in sess if b["_ts"] >= _parse_ist(r["delayed_at"])),
                default=r["p_delayed"],
            )
        else:
            peak = max(_f(b.get("high")) for b in post)
            remaining = peak - p_flag
            pre_flag = abs(p_flag - entry_ref)
            delayed_at = _parse_ist(r["delayed_at"])
            captured = max(
                (_f(b.get("high")) for b in sess if b["_ts"] >= delayed_at),
                default=r["p_delayed"],
            ) - r["p_delayed"]
        floor = max(p_flag * PRE_FLAG_FLOOR_PCT, 1e-6)
        pre_used = max(pre_flag, floor)
        rem_m = remaining / pre_used
        cap_m = captured / pre_used
        open_rows.append(
            {
                **{k: r[k] for k in ("d", "symbol", "direction", "is_reversal")},
                "pre_flag": round(pre_flag, 4),
                "remaining_mult": round(rem_m, 3),
                "captured_mult": round(cap_m, 3),
                "hit_4x": rem_m >= 4,
                "hit_5x": rem_m >= 5,
                "fizzle_fp": rem_m < 1,
                "captured_ge_3x": cap_m >= 3,
                "captured_ge_4x": cap_m >= 4,
                "near_extreme": pre_flag < floor,
                "remaining_pct": round(100 * remaining / p_flag, 3) if p_flag else 0,
            }
        )

    ocore = [r for r in open_rows if not r.get("near_extreme")]
    print("n_open_ref", len(open_rows), "core", len(ocore))
    print("remaining>=4x", rate(ocore, "hit_4x"))
    print("remaining>=5x", rate(ocore, "hit_5x"))
    print("fizzle<1x", rate(ocore, "fizzle_fp"))
    print("delayed>=3x", rate(ocore, "captured_ge_3x"))
    print("delayed>=4x", rate(ocore, "captured_ge_4x"))
    print("med remaining_mult", pctile([r["remaining_mult"] for r in ocore], 0.5))
    print("med captured_mult", pctile([r["captured_mult"] for r in ocore], 0.5))
    print("remaining>=0.5%px", round(100 * sum(1 for r in ocore if r["remaining_pct"] >= 0.5) / max(1, len(ocore)), 1))
    print("remaining>=1.0%px", round(100 * sum(1 for r in ocore if r["remaining_pct"] >= 1.0) / max(1, len(ocore)), 1))

    out = {
        "primary_session_extreme": data.get("summary"),
        "sensitivity_session_open": {
            "n": len(ocore),
            "remaining_ge_4x": rate(ocore, "hit_4x"),
            "remaining_ge_5x": rate(ocore, "hit_5x"),
            "fizzle": rate(ocore, "fizzle_fp"),
            "delayed_ge_3x": rate(ocore, "captured_ge_3x"),
            "delayed_ge_4x": rate(ocore, "captured_ge_4x"),
            "med_remaining_mult": pctile([r["remaining_mult"] for r in ocore], 0.5),
            "med_captured_mult": pctile([r["captured_mult"] for r in ocore], 0.5),
        },
        "absolute_remaining_primary": {
            "ge_0_5pct": round(100 * abs_ok_05 / n, 1),
            "ge_1_0pct": round(100 * abs_ok_10 / n, 1),
            "n": n,
        },
    }
    Path("/tmp/fast_watch_followthrough_sensitivity.json").write_text(json.dumps(out, indent=2))
    print("\nwrote /tmp/fast_watch_followthrough_sensitivity.json")
    db.close()


if __name__ == "__main__":
    main()
