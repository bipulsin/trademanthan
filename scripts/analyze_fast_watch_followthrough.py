#!/usr/bin/env python3
"""Fast Watch follow-through vs 4-5x movers (read-only Part B research).

Pulls historical rs_fast_watch flags, fetches 5m candles (futures keys — same
as live Kavach), aggregates to 10m for the delayed-entry proxy, and measures
how much of the session move was still ahead of the flag.

Reference definitions (stated explicitly for review)
----------------------------------------------------
ENTRY_REF (leg start):
  LONG  → session low from 09:15 IST through the flag bar (inclusive)
  SHORT → session high from 09:15 IST through the flag bar (inclusive)
  Rationale: matches "day's low/open or wherever the leg started" for an
  intraday ignition. Session extreme up to the flag is the conservative
  already-covered distance; using open alone understates pre-flag move when
  price dipped/rallied first.

PRE_FLAG move:
  |P_flag − ENTRY_REF|  (floor: max(0.05% of P_flag, tiny epsilon) so near-extreme
  flags aren't infinite multiples)

P_flag:
  rs_fast_watch.flip_price when present, else 5m close at/just before first_flip_at

SESSION PEAK (same-session follow-through window):
  LONG  → max high from flag bar through 15:30 IST
  SHORT → min low from flag bar through 15:30 IST
  Window: remainder of the NSE cash session on 10m-compatible 5m bars.
  Multi-day follow-through is out of scope (Fast Watch is a same-day checklist tool).

REMAINING_AT_FLAG:
  |SESSION_PEAK − P_flag|

4x / 5x classification (ticket wording):
  subsequent move ≥ N × distance already covered
  → REMAINING_AT_FLAG ≥ N × PRE_FLAG

1x-DELAYED ENTRY (proxy chosen):
  Entry at the close of the next completed 10m bar after the flag time.
  Defensible because Fast Watch / Kavach live recompute is on closed 10m bars;
  "one more bar" is the natural unit of delay. (Not a price-increment delay.)

CAPTURED (delayed):
  |SESSION_PEAK − P_delayed|
  Success thresholds reported at ≥3× and ≥4× PRE_FLAG.

FALSE POSITIVE:
  REMAINING_AT_FLAG < 1× PRE_FLAG  (fizzle — less ahead than already done)
  Also report adverse: MAE before MFE exceeds 0.5× PRE_FLAG (optional diagnostic).

Run on paperclip (has DB + Upstox):
  docker compose exec -T app python scripts/analyze_fast_watch_followthrough.py
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from backend.database import SessionLocal  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")
SESSION_OPEN = dtime(9, 15)
SESSION_CLOSE = dtime(15, 30)
THROTTLE_SEC = 0.2
PRE_FLAG_FLOOR_PCT = 0.0005  # 5 bps of flag price


def _parse_ist(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None
    return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _session_slice(candles: List[Dict], day: str) -> List[Dict]:
    out = []
    for c in candles:
        ts = _parse_ist(c.get("timestamp") or c.get("time"))
        if ts is None:
            continue
        if ts.strftime("%Y-%m-%d") != day:
            continue
        t = ts.timetz().replace(tzinfo=None) if False else ts.time()
        if t < SESSION_OPEN or t > SESSION_CLOSE:
            continue
        out.append({**c, "_ts": ts})
    out.sort(key=lambda x: x["_ts"])
    return out


def _aggregate_10m(bars_5m: List[Dict]) -> List[Dict]:
    """Bucket 5m bars into 10m closes (09:15-09:25, 09:25-09:35, ...)."""
    buckets: Dict[datetime, List[Dict]] = defaultdict(list)
    for b in bars_5m:
        ts: datetime = b["_ts"]
        # Align to 10m boundary ending: floor minute to even decade from :15 open
        minutes_from_open = (ts.hour * 60 + ts.minute) - (9 * 60 + 15)
        if minutes_from_open < 0:
            continue
        bucket_idx = minutes_from_open // 10
        bucket_end_min = 9 * 60 + 15 + (bucket_idx + 1) * 10
        end_h, end_m = divmod(bucket_end_min, 60)
        end_ts = ts.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
        buckets[end_ts].append(b)
    out = []
    for end_ts in sorted(buckets):
        group = buckets[end_ts]
        highs = [_f(x.get("high")) for x in group]
        lows = [_f(x.get("low")) for x in group]
        out.append(
            {
                "_ts": end_ts,
                "open": _f(group[0].get("open")),
                "high": max(highs) if highs else 0.0,
                "low": min(lows) if lows else 0.0,
                "close": _f(group[-1].get("close")),
            }
        )
    return out


def resolve_ikey(db, symbol: str) -> Optional[str]:
    row = db.execute(
        text(
            """
            SELECT currmth_future_instrument_key AS ikey
            FROM arbitrage_master
            WHERE UPPER(stock) = :s
            LIMIT 1
            """
        ),
        {"s": symbol.upper()},
    ).fetchone()
    return str(row.ikey).strip() if row and row.ikey else None


def fetch_candles(upx, ikey: str, session_date: str) -> List[Dict]:
    from backend.services.relative_strength_scanner import CANDLE_INTERVAL, _sorted_candles

    d = date.fromisoformat(session_date)
    raw = upx.get_historical_candles_by_instrument_key(
        ikey,
        interval=CANDLE_INTERVAL,
        days_back=5,
        range_end_date=d,
    )
    if not raw:
        return []
    return _sorted_candles(raw)


def load_flags(db) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT session_date::text AS d, symbol, direction,
                   first_flip_at, kavach_state, prev_kavach_state,
                   flip_price, trade_score, confidence_grade,
                   COALESCE(is_reversal, false) AS is_reversal,
                   lock_direction
            FROM rs_fast_watch
            ORDER BY first_flip_at ASC
            """
        )
    ).mappings().all()
    return [dict(r) for r in rows]


def analyze_flag(
    flag: Dict[str, Any],
    bars_5m: List[Dict],
) -> Optional[Dict[str, Any]]:
    day = flag["d"]
    direction = (flag["direction"] or "LONG").upper()
    flip_at = _parse_ist(flag["first_flip_at"])
    if flip_at is None:
        return None
    session = _session_slice(bars_5m, day)
    if len(session) < 6:
        return {"skip_reason": "insufficient_session_bars", "symbol": flag["symbol"], "d": day}

    # Flag bar = last 5m bar with ts <= flip_at
    pre = [b for b in session if b["_ts"] <= flip_at]
    post = [b for b in session if b["_ts"] >= (pre[-1]["_ts"] if pre else flip_at)]
    if not pre:
        return {"skip_reason": "no_bars_before_flag", "symbol": flag["symbol"], "d": day}
    flag_bar = pre[-1]
    p_flag = _f(flag.get("flip_price"))
    if p_flag <= 0:
        p_flag = _f(flag_bar.get("close"))
    if p_flag <= 0:
        return {"skip_reason": "no_flag_price", "symbol": flag["symbol"], "d": day}

    if direction == "SHORT":
        entry_ref = max(_f(b.get("high")) for b in pre)
        peak = min(_f(b.get("low")) for b in post) if post else p_flag
        remaining = p_flag - peak  # positive if price fell
        pre_flag = entry_ref - p_flag
    else:
        entry_ref = min(_f(b.get("low")) for b in pre)
        peak = max(_f(b.get("high")) for b in post) if post else p_flag
        remaining = peak - p_flag
        pre_flag = p_flag - entry_ref

    floor = max(p_flag * PRE_FLAG_FLOOR_PCT, 1e-6)
    pre_flag_raw = pre_flag
    pre_flag = max(pre_flag, floor)
    near_extreme = pre_flag_raw < floor

    # 1x-delayed: next 10m bar close after flip
    bars_10 = _aggregate_10m(session)
    delayed_candidates = [b for b in bars_10 if b["_ts"] > flip_at]
    if delayed_candidates:
        delayed_bar = delayed_candidates[0]
        p_delayed = _f(delayed_bar.get("close"))
        delayed_at = delayed_bar["_ts"]
    else:
        p_delayed = p_flag
        delayed_at = flip_at

    if direction == "SHORT":
        # peak after delayed entry
        post_d = [b for b in session if b["_ts"] >= delayed_at]
        peak_d = min(_f(b.get("low")) for b in post_d) if post_d else p_delayed
        captured = p_delayed - peak_d
        # MAE: adverse rise after delayed entry before eventual low
        mae = 0.0
        mfe = 0.0
        best = p_delayed
        for b in post_d:
            h, l = _f(b.get("high")), _f(b.get("low"))
            mae = max(mae, h - p_delayed)
            best = min(best, l)
            mfe = max(mfe, p_delayed - best)
    else:
        post_d = [b for b in session if b["_ts"] >= delayed_at]
        peak_d = max(_f(b.get("high")) for b in post_d) if post_d else p_delayed
        captured = peak_d - p_delayed
        mae = 0.0
        mfe = 0.0
        best = p_delayed
        for b in post_d:
            h, l = _f(b.get("high")), _f(b.get("low"))
            mae = max(mae, p_delayed - l)
            best = max(best, h)
            mfe = max(mfe, best - p_delayed)

    rem_mult = remaining / pre_flag
    cap_mult = captured / pre_flag
    return {
        "d": day,
        "symbol": flag["symbol"],
        "direction": direction,
        "kavach_state": flag.get("kavach_state"),
        "is_reversal": bool(flag.get("is_reversal")),
        "confidence_grade": flag.get("confidence_grade"),
        "first_flip_at": flip_at.isoformat(),
        "p_flag": round(p_flag, 4),
        "entry_ref": round(entry_ref, 4),
        "peak": round(peak, 4),
        "pre_flag": round(pre_flag_raw, 4),
        "pre_flag_used": round(pre_flag, 4),
        "near_extreme": near_extreme,
        "remaining": round(remaining, 4),
        "remaining_mult": round(rem_mult, 3),
        "hit_4x": rem_mult >= 4.0,
        "hit_5x": rem_mult >= 5.0,
        "p_delayed": round(p_delayed, 4),
        "delayed_at": delayed_at.isoformat(),
        "captured": round(captured, 4),
        "captured_mult": round(cap_mult, 3),
        "captured_ge_3x": cap_mult >= 3.0,
        "captured_ge_4x": cap_mult >= 4.0,
        "fizzle_fp": rem_mult < 1.0,
        "mae": round(mae, 4),
        "mfe": round(mfe, 4),
        "mae_vs_pre": round(mae / pre_flag, 3),
    }


def _wilson(successes: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((centre - margin) / denom, (centre + margin) / denom)


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    usable = [r for r in rows if "remaining_mult" in r]
    n = len(usable)
    if n == 0:
        return {"n": 0}

    def rate(key: str) -> Dict[str, Any]:
        s = sum(1 for r in usable if r.get(key))
        lo, hi = _wilson(s, n)
        return {"count": s, "n": n, "pct": round(100.0 * s / n, 1), "wilson95": [round(100 * lo, 1), round(100 * hi, 1)]}

    rem = sorted(r["remaining_mult"] for r in usable)
    cap = sorted(r["captured_mult"] for r in usable)

    def pctile(xs: List[float], p: float) -> float:
        if not xs:
            return 0.0
        i = int(round((len(xs) - 1) * p))
        return round(xs[i], 3)

    # Cohort: exclude near-extreme flags (pre-flag floored) as multiples are unstable
    core = [r for r in usable if not r.get("near_extreme")]
    nc = len(core)

    def rate_core(key: str) -> Dict[str, Any]:
        if nc == 0:
            return {"count": 0, "n": 0, "pct": None}
        s = sum(1 for r in core if r.get(key))
        lo, hi = _wilson(s, nc)
        return {"count": s, "n": nc, "pct": round(100.0 * s / nc, 1), "wilson95": [round(100 * lo, 1), round(100 * hi, 1)]}

    return {
        "n_flags_analyzed": n,
        "n_core_ex_near_extreme": nc,
        "date_span": [usable[0]["d"], usable[-1]["d"]] if usable else None,
        "remaining_mult_dist": {
            "p25": pctile(rem, 0.25),
            "p50": pctile(rem, 0.50),
            "p75": pctile(rem, 0.75),
            "p90": pctile(rem, 0.90),
        },
        "captured_mult_dist_1x_delayed": {
            "p25": pctile(cap, 0.25),
            "p50": pctile(cap, 0.50),
            "p75": pctile(cap, 0.75),
            "p90": pctile(cap, 0.90),
        },
        "all_flags": {
            "remaining_ge_4x": rate("hit_4x"),
            "remaining_ge_5x": rate("hit_5x"),
            "fizzle_fp_remaining_lt_1x": rate("fizzle_fp"),
            "delayed_captured_ge_3x": rate("captured_ge_3x"),
            "delayed_captured_ge_4x": rate("captured_ge_4x"),
        },
        "core_ex_near_extreme": {
            "remaining_ge_4x": rate_core("hit_4x"),
            "remaining_ge_5x": rate_core("hit_5x"),
            "fizzle_fp_remaining_lt_1x": rate_core("fizzle_fp"),
            "delayed_captured_ge_3x": rate_core("captured_ge_3x"),
            "delayed_captured_ge_4x": rate_core("captured_ge_4x"),
        },
        "by_direction": {
            side: {
                "n": sum(1 for r in core if r["direction"] == side),
                "delayed_ge_3x_pct": round(
                    100
                    * sum(1 for r in core if r["direction"] == side and r["captured_ge_3x"])
                    / max(1, sum(1 for r in core if r["direction"] == side)),
                    1,
                ),
                "fizzle_pct": round(
                    100
                    * sum(1 for r in core if r["direction"] == side and r["fizzle_fp"])
                    / max(1, sum(1 for r in core if r["direction"] == side)),
                    1,
                ),
            }
            for side in ("LONG", "SHORT")
        },
        "by_reversal": {
            str(rev): {
                "n": sum(1 for r in core if bool(r["is_reversal"]) == rev),
                "delayed_ge_3x_pct": round(
                    100
                    * sum(1 for r in core if bool(r["is_reversal"]) == rev and r["captured_ge_3x"])
                    / max(1, sum(1 for r in core if bool(r["is_reversal"]) == rev)),
                    1,
                ),
                "fizzle_pct": round(
                    100
                    * sum(1 for r in core if bool(r["is_reversal"]) == rev and r["fizzle_fp"])
                    / max(1, sum(1 for r in core if bool(r["is_reversal"]) == rev)),
                    1,
                ),
            }
            for rev in (False, True)
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-flags", type=int, default=0, help="0 = all")
    ap.add_argument("--out", type=str, default="docs/diagnostics/FAST_WATCH_FOLLOWTHROUGH.md")
    ap.add_argument("--json-out", type=str, default="docs/diagnostics/fast_watch_followthrough.json")
    args = ap.parse_args()

    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    db = SessionLocal()
    upx = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    flags = load_flags(db)
    if args.max_flags:
        flags = flags[: args.max_flags]

    # Cache candles per (symbol, day)
    candle_cache: Dict[Tuple[str, str], List[Dict]] = {}
    ikey_cache: Dict[str, Optional[str]] = {}
    results: List[Dict[str, Any]] = []
    skips: Dict[str, int] = defaultdict(int)

    print(f"flags_loaded={len(flags)}")
    for i, flag in enumerate(flags):
        sym = flag["symbol"]
        day = flag["d"]
        key = (sym, day)
        if key not in candle_cache:
            if sym not in ikey_cache:
                ikey_cache[sym] = resolve_ikey(db, sym)
            ikey = ikey_cache[sym]
            if not ikey:
                skips["no_instrument"] += 1
                candle_cache[key] = []
            else:
                try:
                    candle_cache[key] = fetch_candles(upx, ikey, day)
                    time.sleep(THROTTLE_SEC)
                except Exception as exc:
                    print(f"candle_err {sym} {day}: {exc}")
                    candle_cache[key] = []
                    skips["candle_fetch_err"] += 1
        bars = candle_cache[key]
        if not bars:
            skips["no_candles"] += 1
            continue
        row = analyze_flag(flag, bars)
        if row is None:
            skips["analyze_none"] += 1
            continue
        if "skip_reason" in row:
            skips[row["skip_reason"]] += 1
            continue
        results.append(row)
        if (i + 1) % 25 == 0:
            print(f"progress {i+1}/{len(flags)} analyzed={len(results)}")

    summary = summarize(results)
    summary["skips"] = dict(skips)
    summary["definitions"] = {
        "entry_ref": "session extreme (low for LONG / high for SHORT) from 09:15 through flag bar",
        "follow_through_window": "flag bar through 15:30 IST same session",
        "delayed_entry": "close of next completed 10m bar after first_flip_at",
        "instrument": "currmth futures key from arbitrage_master (Kavach parity)",
        "4x_5x": "remaining_at_flag / pre_flag_move",
        "fp": "remaining_mult < 1.0",
    }

    # Go/no-go heuristic (documented, not auto-enforced)
    core = summary.get("core_ex_near_extreme") or {}
    ge3 = (core.get("delayed_captured_ge_3x") or {}).get("pct")
    fp = (core.get("fizzle_fp_remaining_lt_1x") or {}).get("pct")
    verdict = "INCONCLUSIVE"
    rationale = "Insufficient sample or missing rates."
    if ge3 is not None and fp is not None and (core.get("delayed_captured_ge_3x") or {}).get("n", 0) >= 20:
        if ge3 >= 35 and fp <= 45:
            verdict = "CONDITIONAL_GO"
            rationale = (
                f"Delayed ≥3× captured on {ge3}% of core flags with fizzle (remaining<1×) at {fp}%. "
                "Useful as an early alert, not a standalone auto-entry without risk controls."
            )
        elif ge3 >= 25 and fp <= 55:
            verdict = "WEAK_GO_SHADOW_ONLY"
            rationale = (
                f"Delayed ≥3× at {ge3}%, fizzle {fp}% — edge may exist but false positives are high "
                "for standalone live entries; keep shadow / discretionary."
            )
        else:
            verdict = "NO_GO_STANDALONE"
            rationale = (
                f"Delayed ≥3× only {ge3}% with fizzle {fp}% — not good enough alone vs Votes/Grade lag tradeoff."
            )
    summary["verdict"] = {"call": verdict, "rationale": rationale, "ge3_pct": ge3, "fizzle_pct": fp}

    out_json = Path(args.json_out)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summary, "rows": results}
    out_json.write_text(json.dumps(payload, indent=2, default=str))

    md = Path(args.out)
    md.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Fast Watch follow-through (4–5× research)",
        "",
        f"Generated: {datetime.now(IST).isoformat()}",
        "",
        "## Definitions",
        "",
        f"- **ENTRY_REF:** {summary['definitions']['entry_ref']}",
        f"- **Follow-through window:** {summary['definitions']['follow_through_window']}",
        f"- **1×-delayed entry:** {summary['definitions']['delayed_entry']}",
        f"- **4×/5× test:** {summary['definitions']['4x_5x']}",
        f"- **False positive (fizzle):** {summary['definitions']['fp']}",
        f"- **Instrument:** {summary['definitions']['instrument']}",
        "",
        "## Verdict",
        "",
        f"**{verdict}** — {rationale}",
        "",
        "## Summary",
        "",
        "```json",
        json.dumps(summary, indent=2),
        "```",
        "",
        f"Full row dump: `{out_json}`",
        "",
    ]
    md.write_text("\n".join(lines))
    print(json.dumps(summary, indent=2))
    print(f"wrote {md} and {out_json}")
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
