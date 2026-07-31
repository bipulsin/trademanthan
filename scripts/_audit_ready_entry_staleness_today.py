#!/usr/bin/env python3
"""Audit READY-family entry freshness vs live EMA5 / candle tip (prod diagnostic)."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.daily_checklist_snapshot import _load_candles_for_symbol
from backend.services.kavach_10m import metrics_from_10m_candles
from backend.services.relative_strength_scanner import RANKING_BEARISH, RANKING_BULLISH

IST = timezone(timedelta(hours=5, minutes=30))
# Deploy e9b332e completed ~11:17 IST = 05:47 UTC
DEPLOY_UTC = datetime(2026, 7, 31, 5, 47, 0, tzinfo=timezone.utc)
SESSION = "2026-07-31"


def _f(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _parse_ts(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        dt = ts
    else:
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def candles_behind(entry: float, candles: List[Dict], *, now: datetime) -> Tuple[Optional[int], Optional[str]]:
    """How many 5m bars back does entry best match a close (or EMA5 tip at cut)."""
    today = [c for c in candles if str(c.get("timestamp") or "").startswith(SESSION)]
    if not today:
        return None, None
    best_i = None
    best_err = None
    for i, c in enumerate(today):
        cl = _f(c.get("close"))
        if cl is None:
            continue
        err = abs(cl - entry)
        if best_err is None or err < best_err:
            best_err = err
            best_i = i
    if best_i is None:
        return None, None
    # bars from tip
    behind = len(today) - 1 - best_i
    tip_ts = str(today[best_i].get("timestamp") or "")
    # only treat as match if close within 0.5%
    tip_close = _f(today[best_i].get("close"))
    if tip_close and abs(tip_close - entry) / tip_close > 0.005:
        # try EMA5 match at successive cuts
        hist = [c for c in candles if not str(c.get("timestamp") or "").startswith(SESSION)]
        best = None
        for i in range(len(today)):
            series = hist + today[: i + 1]
            if len(series) < 40:
                continue
            m = metrics_from_10m_candles(
                series, ranking_type=RANKING_BULLISH, nifty_pct=0.0, include_forming=True
            )
            if not m:
                continue
            e5 = _f(m.get("ema5"))
            if e5 is None:
                continue
            err = abs(e5 - entry)
            if best is None or err < best[0]:
                best = (err, i, str(today[i].get("timestamp") or ""), e5)
        if best and best[0] / max(entry, 1e-9) <= 0.002:
            behind = len(today) - 1 - best[1]
            return behind, best[2]
        return None, tip_ts
    return behind, tip_ts


def main() -> None:
    db = SessionLocal()
    rows = db.execute(
        text(
            """
            SELECT symbol, direction, rendered_state, logged_at, inputs
            FROM kavach_ready_consistency_log
            WHERE session_date = CAST(:d AS date)
              AND (
                rendered_state ILIKE '%READY%'
                OR COALESCE((inputs->>'dwell_soft_hold')::boolean, false) = true
              )
            ORDER BY logged_at
            """
        ),
        {"d": SESSION},
    ).mappings().all()
    print(f"READY-family consistency rows: {len(rows)}")
    syms = sorted({str(r["symbol"]).upper() for r in rows})
    print(f"unique symbols: {len(syms)} -> {syms}")

    # Load candles once per symbol
    candle_map: Dict[str, List] = {}
    live_now_map: Dict[str, Dict] = {}
    for sym in syms:
        try:
            c = _load_candles_for_symbol(db, sym) or []
        except Exception as exc:
            print(f"  candle load fail {sym}: {exc}")
            c = []
        candle_map[sym] = c
        if c:
            m = metrics_from_10m_candles(
                c, ranking_type=RANKING_BULLISH, nifty_pct=0.0, include_forming=True
            )
            live_now_map[sym] = {
                "ema5": _f((m or {}).get("ema5")),
                "price": _f((m or {}).get("price")),
                "bar_at": str((m or {}).get("bar_evaluated_at") or ""),
                "n_today": sum(
                    1 for x in c if str(x.get("timestamp") or "").startswith(SESSION)
                ),
                "tip": str(c[-1].get("timestamp") or "") if c else "",
            }
        else:
            live_now_map[sym] = {}

    pre: List[Dict] = []
    post: List[Dict] = []
    by_hour_miss = defaultdict(lambda: [0, 0])

    for r in rows:
        sym = str(r["symbol"]).upper()
        inp = r["inputs"] or {}
        dwell = inp.get("dwell_entry_shadow") or {}
        live = dwell.get("live_levels") or {}
        audit = dwell.get("audit_levels") or {}
        entry = _f(inp.get("trade_entry"))
        live_e5 = _f(live.get("ema5"))
        live_px = _f(live.get("price"))
        audit_e5 = _f(audit.get("ema5"))
        live_missing = live_e5 is None and live_px is None
        gap_live = None
        if entry is not None and live_e5 is not None and live_e5 != 0:
            gap_live = abs(entry - live_e5) / abs(live_e5) * 100.0

        # Reconstruct expected EMA5 at logged_at from candles (truncate by wall clock)
        logged = r["logged_at"]
        if logged.tzinfo is None:
            logged = logged.replace(tzinfo=timezone.utc)
        logged_ist = logged.astimezone(IST)
        expected_e5 = None
        expected_px = None
        candles = candle_map.get(sym) or []
        if candles:
            cut = []
            for c in candles:
                ts = _parse_ts(c.get("timestamp"))
                if ts is None:
                    continue
                # include bar if open time <= logged
                if ts <= logged_ist:
                    cut.append(c)
            if len(cut) >= 40:
                ranking = (
                    RANKING_BEARISH
                    if str(r["direction"]).upper() == "SHORT"
                    else RANKING_BULLISH
                )
                m = metrics_from_10m_candles(
                    cut,
                    ranking_type=ranking,
                    nifty_pct=0.0,
                    include_forming=True,
                    now=logged_ist,
                )
                if m:
                    expected_e5 = _f(m.get("ema5"))
                    expected_px = _f(m.get("price"))

        gap_expected = None
        if entry is not None and expected_e5 is not None and expected_e5 != 0:
            gap_expected = abs(entry - expected_e5) / abs(expected_e5) * 100.0

        behind = None
        match_ts = None
        if entry is not None and candles:
            behind, match_ts = candles_behind(entry, candles, now=logged_ist)

        rec = {
            "sym": sym,
            "at": logged,
            "at_ist": logged_ist.strftime("%H:%M:%S"),
            "state": r["rendered_state"],
            "entry": entry,
            "live_e5": live_e5,
            "live_px": live_px,
            "audit_e5": audit_e5,
            "expected_e5": expected_e5,
            "expected_px": expected_px,
            "live_missing": live_missing,
            "gap_live_pct": gap_live,
            "gap_expected_pct": gap_expected,
            "behind_5m": behind,
            "match_ts": match_ts,
            "dwell": bool(inp.get("dwell_soft_hold")),
            "reason": (inp.get("trade_state_reason") or "")[:100],
            "entry_stale": bool(
                (gap_expected is not None and gap_expected > 0.15)
                or (behind is not None and behind >= 4)
            ),
        }
        (post if logged >= DEPLOY_UTC else pre).append(rec)

        h = logged_ist.hour
        by_hour_miss[h][0] += 1
        if live_missing:
            by_hour_miss[h][1] += 1

    def summarize(label: str, arr: List[Dict]) -> None:
        print(f"\n=== {label} (n={len(arr)}) ===")
        if not arr:
            return
        live_miss = sum(1 for x in arr if x["live_missing"])
        stale = sum(1 for x in arr if x["entry_stale"])
        with_live = sum(1 for x in arr if x["live_e5"] is not None)
        print(
            f"live_missing={live_miss}/{len(arr)} ({100*live_miss/len(arr):.0f}%) "
            f"with_live_e5={with_live} entry_stale(vs reconstructed EMA5 or >=4 bars)="
            f"{stale}/{len(arr)} ({100*stale/len(arr):.0f}%)"
        )
        # latest per symbol
        latest: Dict[str, Dict] = {}
        for x in arr:
            latest[x["sym"]] = x
        print("latest per symbol:")
        for sym, x in sorted(latest.items()):
            print(
                f"  {sym} @{x['at_ist']} entry={x['entry']} "
                f"live_e5={x['live_e5']} exp_e5={round(x['expected_e5'],4) if x['expected_e5'] else None} "
                f"gap_exp%={round(x['gap_expected_pct'],3) if x['gap_expected_pct'] is not None else None} "
                f"behind5m={x['behind_5m']} match={x['match_ts']} "
                f"live_miss={x['live_missing']} stale={x['entry_stale']} dwell={x['dwell']}"
            )

        # affected list with magnitude
        affected = [x for x in latest.values() if x["entry_stale"] or x["live_missing"]]
        print(f"affected latest cards: {len(affected)}/{len(latest)}")

    summarize("PRE-deploy (<11:17 IST)", pre)
    summarize("POST-deploy (>=11:17 IST)", post)

    print("\n=== live_missing by IST hour ===")
    for h in sorted(by_hour_miss):
        n, m = by_hour_miss[h]
        print(f"  {h:02d}: {m}/{n} ({100*m/n:.0f}%)")

    # Current live vs last displayed entry for each READY symbol
    print("\n=== NOW: candle tip EMA5 vs last logged entry ===")
    last: Dict[str, Dict] = {}
    for x in pre + post:
        last[x["sym"]] = x
    for sym, x in sorted(last.items()):
        nowm = live_now_map.get(sym) or {}
        entry = x["entry"]
        e5 = nowm.get("ema5")
        gap = None
        if entry is not None and e5:
            gap = abs(entry - e5) / abs(e5) * 100
        print(
            f"  {sym}: last_entry={entry} now_ema5={round(e5,4) if e5 else None} "
            f"now_px={nowm.get('price')} tip={nowm.get('tip')} "
            f"gap%={round(gap,3) if gap is not None else None} "
            f"n_today={nowm.get('n_today')}"
        )

    # Staleness logger coverage vs consistency
    st = db.execute(
        text(
            """
            SELECT COUNT(*) AS n, COUNT(DISTINCT symbol) AS ns
            FROM kavach_ready_entry_staleness_log
            WHERE session_date = CAST(:d AS date)
            """
        ),
        {"d": SESSION},
    ).mappings().first()
    print(
        f"\nstaleness_log today: rows={st['n']} symbols={st['ns']} "
        f"vs consistency READY symbols={len(syms)}"
    )

    # Historical: last 10 sessions from consistency — live_missing rate + entry frozen
    print("\n=== HISTORICAL (consistency READY rows, last 10 sessions) ===")
    hist = db.execute(
        text(
            """
            SELECT session_date::text AS d,
                   COUNT(*) AS n,
                   COUNT(*) FILTER (
                     WHERE (inputs->'dwell_entry_shadow'->'live_levels'->>'ema5') IS NULL
                       AND (inputs->'dwell_entry_shadow'->'live_levels'->>'price') IS NULL
                   ) AS live_miss,
                   COUNT(DISTINCT symbol) AS ns
            FROM kavach_ready_consistency_log
            WHERE session_date >= CURRENT_DATE - 14
              AND rendered_state ILIKE '%READY%'
            GROUP BY 1
            ORDER BY 1
            """
        )
    ).mappings().all()
    for h in hist:
        n = int(h["n"] or 0)
        m = int(h["live_miss"] or 0)
        print(
            f"  {h['d']}: rows={n} syms={h['ns']} live_miss={m} "
            f"({(100*m/n) if n else 0:.0f}%)"
        )

    db.close()


if __name__ == "__main__":
    main()
