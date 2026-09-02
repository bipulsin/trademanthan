#!/usr/bin/env python3
"""Local REST replay of Breakfast Live unify. No persist_session_lock. No deploy."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.services.breakfast_prev_close import (  # noqa: E402
    classify_daily_wick,
    filter_sector_members_by_first_5m_color,
    filter_sector_members_by_sign_gate,
    filter_sector_members_by_wick,
    first_5m_is_doji,
    latest_settled_daily_ohlc,
)
from backend.services.breakfast_strategy.candles import (  # noqa: E402
    first_5m_bar,
    forming_bar_from_1m_upto,
    move_pct_vs_prev_close,
)
from backend.services.breakfast_strategy.config import STOCK_MOVE_CAP_PCT  # noqa: E402
from backend.services.breakfast_strategy.engine import NIFTY50_KEY  # noqa: E402
from backend.services.breakfast_strategy.engine_prevclose import (  # noqa: E402
    nifty_bias_from_bar_vs_prev_close,
    rank_sectors_vs_prev_close,
)
from backend.services.breakfast_strategy.live_tick import (  # noqa: E402
    LIVE_SECTORS_TO_PICK,
    LIVE_STOCKS_PER_SECTOR,
    live_lock_failure_reason,
    try_one_sector_cascade,
)
from backend.services.breakfast_strategy.universe import (  # noqa: E402
    SECTOR_UNIVERSE,
    StockRow,
    pick_stocks_in_sector,
    sector_index_key_for_label,
)
from backend.services.upstox_service import UpstoxService  # noqa: E402

IST = pytz.timezone("Asia/Kolkata")
SESSION = date(2026, 9, 2)
OUT_PATH = ROOT / "docs" / "diagnostics" / "breakfast_live_replay_20260902.json"


def _sector_keys() -> List[str]:
    keys = []
    for label, _ in SECTOR_UNIVERSE:
        ik = sector_index_key_for_label(label)
        if ik:
            keys.append(ik)
    return keys


def dump_paperclip_prev() -> Dict[str, Any]:
    local = ROOT / "data" / "breakfast_prev_dump.json"
    if local.exists():
        return json.loads(local.read_text())
    py = r"""
import json
from backend.services.breakfast_prev_close import load_stored_prev_closes_and_wicks
from backend.database import SessionLocal
from sqlalchemy import text
bench, stocks, wicks = load_stored_prev_closes_and_wicks()
db = SessionLocal()
try:
    rows = db.execute(text('''
        SELECT UPPER(TRIM(stock)) AS stock, TRIM(sector) AS sector,
               TRIM(sector_index) AS sector_index,
               TRIM(currmth_future_instrument_key) AS fut_key,
               COALESCE(lot_size, 0) AS lot_size,
               prev_session_close_for_date::text AS prev_for_date
        FROM arbitrage_master
        WHERE stock IS NOT NULL AND TRIM(stock) <> ''
    ''')).mappings().all()
    bench_dates = db.execute(text('''
        SELECT instrument_key, prev_session_close_for_date::text AS prev_for_date
        FROM nifty_benchmark_reference
    ''')).mappings().all()
finally:
    db.close()
members = []
for r in rows:
    members.append({
        "stock": r["stock"], "sector": r.get("sector") or "",
        "sector_index": r.get("sector_index") or "",
        "fut_key": r.get("fut_key") or "",
        "lot_size": int(r.get("lot_size") or 0),
    })
print(json.dumps({
    "bench": bench, "stock_prev": stocks, "wicks": wicks, "members": members,
    "bench_prev_for_date": {r["instrument_key"]: r.get("prev_for_date") for r in bench_dates},
    "stock_prev_for_date": {r["stock"]: r.get("prev_for_date") for r in rows},
}))
"""
    cmd = [
        str(ROOT / "scripts" / "paperclip-ssh.sh"),
        "cd /home/ubuntu/twcto && docker compose exec -T app python3 -c "
        + json.dumps(py),
    ]
    raw = subprocess.check_output(cmd, text=True)
    # strip ssh banner
    line = [ln for ln in raw.splitlines() if ln.startswith("{")][-1]
    return json.loads(line)


def paperclip_token() -> str:
    cmd = [
        str(ROOT / "scripts" / "paperclip-ssh.sh"),
        "cd /home/ubuntu/twcto && docker compose exec -T app printenv UPSTOX_ACCESS_TOKEN",
    ]
    tok = subprocess.check_output(cmd, text=True).strip().splitlines()[-1].strip()
    if not tok:
        raise SystemExit("no Upstox token on paperclip")
    return tok


def _asof_now(session: date) -> datetime:
    return IST.localize(datetime(session.year, session.month, session.day, 9, 15, 0))


def apply_asof_prev_close(
    ux: UpstoxService,
    session: date,
    dump: Dict[str, Any],
    extra_fut_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Replace bench/stock_prev/wicks with settled daily as-of 09:15 on session.

    Paperclip stores a single current snapshot (prev_session_close_for_date + wick).
    Wick has no history table — reconstructed from that session's prior daily OHLC.
    """
    now = _asof_now(session)
    stored_dates = [d for d in (dump.get("bench_prev_for_date") or {}).values() if d]
    stored_date = stored_dates[0] if stored_dates else None
    bench = dict(dump.get("bench") or {})
    stocks = dict(dump.get("stock_prev") or {})
    wicks = dict(dump.get("wicks") or {})
    asof_bench_date = None
    asof_stock_n = 0
    index_keys = [NIFTY50_KEY] + _sector_keys()
    print(f"as-of daily prev for {len(index_keys)} indexes @ {session}", file=sys.stderr)
    daily_idx = fetch_daily(ux, index_keys, session)
    for ik, candles in daily_idx.items():
        row = latest_settled_daily_ohlc(candles, now_ist=now)
        if not row:
            continue
        d, _o, _h, _l, px = row
        bench[ik] = float(px)
        asof_bench_date = d.isoformat()
    needed = list(extra_fut_keys or [])
    by_fut = {m["fut_key"]: m["stock"] for m in dump.get("members") or [] if m.get("fut_key")}
    asof_stock_n = 0
    if needed:
        print(f"as-of daily prev for {len(needed)} stocks @ {session}", file=sys.stderr)
        daily_st = fetch_daily(ux, needed, session)
        for ik, candles in daily_st.items():
            row = latest_settled_daily_ohlc(candles, now_ist=now)
            if not row:
                continue
            d, o, h, lo, px = row
            sym = by_fut.get(ik)
            if not sym:
                continue
            stocks[sym] = float(px)
            wicks[sym] = classify_daily_wick(o, h, lo, px)
            asof_stock_n += 1
    dump = dict(dump)
    dump["bench"] = bench
    dump["stock_prev"] = stocks
    dump["wicks"] = wicks
    dump["asof_prev"] = {
        "session_date": session.isoformat(),
        "paperclip_bench_prev_for_date": stored_date,
        "asof_prev_session_date": asof_bench_date,
        "used_reconstructed_daily": True,
        "wick_source": "reconstructed_from_daily_ohlc_asof_session",
        "wick_limitation": "arbitrage_master.wick is a current snapshot only; no as-of wick history in DB",
        "n_stock_prev_asof": asof_stock_n,
        "stored_matches_asof": stored_date == asof_bench_date,
    }
    return dump


def fetch_daily(ux: UpstoxService, keys: List[str], session: date) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for i, ik in enumerate(keys):
        try:
            raw = ux.get_historical_candles_by_instrument_key(
                ik, interval="days/1", days_back=12, range_end_date=session
            ) or []
        except Exception as e:
            print(f"daily fail {ik}: {e}", file=sys.stderr)
            raw = []
        out[ik] = list(raw)
        if (i + 1) % 20 == 0:
            print(f"  fetched {i+1}/{len(keys)} days/1", file=sys.stderr)
    return out


def _coverage(candles: Dict[str, List], session: date, minutes_1m: bool) -> Dict[str, int]:
    return {k: len(_session_bars(v, session, minutes_1m)) for k, v in candles.items()}


def _session_bars(candles: List[Dict[str, Any]], session: date, minutes_1m: bool) -> List[Dict[str, Any]]:
    out = []
    for c in candles or []:
        ts = str(c.get("timestamp") or "")
        if session.isoformat() not in ts:
            continue
        if "T09:" not in ts:
            continue
        hhmm = ts[11:16]
        if minutes_1m:
            if hhmm < "09:15" or hhmm > "09:20":
                continue
        else:
            if hhmm != "09:15":
                # keep first 5m if labeled 09:15; skip later
                if hhmm >= "09:20":
                    continue
        out.append(c)
    return out


def fetch_candles(ux: UpstoxService, keys: List[str], session: date, interval: str) -> Dict[str, List[Dict[str, Any]]]:
    from urllib.parse import quote

    from backend.services.upstox_service import _candles_rows_to_structured

    out: Dict[str, List[Dict[str, Any]]] = {}
    for i, ik in enumerate(keys):
        try:
            raw = ux.get_historical_candles_by_instrument_key(
                ik, interval=interval, days_back=3, range_end_date=session
            ) or []
            # Same as 02-Sep: always merge V3 intraday; session filter drops other days.
            key_enc = quote(ik, safe="")
            url = f"{ux.base_url}/historical-candle/intraday/{key_enc}/{interval}"
            data = ux.make_api_request(url, method="GET", timeout=15, max_retries=2)
            intra_rows = ((data or {}).get("data") or {}).get("candles") or []
            intra = _candles_rows_to_structured(intra_rows) if intra_rows else []
            by_ts = {str(c.get("timestamp")): c for c in list(raw)}
            for c in intra:
                by_ts[str(c.get("timestamp"))] = c
            raw = list(by_ts.values())
        except Exception as e:
            print(f"fetch fail {ik} {interval}: {e}", file=sys.stderr)
            raw = []
        out[ik] = list(raw)
        if (i + 1) % 20 == 0:
            print(f"  fetched {i+1}/{len(keys)} {interval} n={len(raw)}", file=sys.stderr)
    return out


def _nifty_side(bar, prev) -> Tuple[str, Optional[float], bool]:
    bias, pct = nifty_bias_from_bar_vs_prev_close(bar or {}, prev, missing="unknown")
    long_side = bias != "negative"
    return bias, pct, long_side and bias != "unknown"


def _members_by_sector(dump: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    from backend.services.sector_movers import normalize_sector_instrument_key

    out: Dict[str, List[Dict[str, str]]] = {}
    for m in dump["members"]:
        skey = normalize_sector_instrument_key(m.get("sector_index"))
        if not skey:
            continue
        out.setdefault(skey, []).append(
            {"stock": m["stock"], "sector": m.get("sector") or "", "sector_index": skey}
        )
    return out


def _stock_rows(dump: Dict[str, Any]) -> Dict[str, StockRow]:
    from backend.services.sector_movers import normalize_sector_instrument_key

    rows = {}
    for m in dump["members"]:
        sym = m["stock"]
        if not m.get("fut_key"):
            continue
        skey = normalize_sector_instrument_key(m.get("sector_index")) or ""
        rows[sym] = StockRow(
            stock=sym,
            display_symbol=sym,
            instrument_label=sym,
            sector=m.get("sector") or "",
            sector_index=skey,
            instrument_key=m["fut_key"],
            lot_size=1,
            price_source="futures",
        )
    return rows


def _sector_rank(
    candles: Dict[str, List],
    session: date,
    dump: Dict[str, Any],
    stocks_by_sector: Dict[str, List],
    upto: Tuple[int, int],
    freeze: bool,
    candles_5m: Dict[str, List],
) -> Tuple[List[Tuple[str, float, float]], bool, Optional[float], str]:
    nifty_bars = candles_5m.get(NIFTY50_KEY, []) if freeze else candles.get(NIFTY50_KEY, [])
    nifty_bar = first_5m_bar(nifty_bars, session) if freeze else forming_bar_from_1m_upto(
        candles.get(NIFTY50_KEY, []), session, upto
    )
    prev = dump["bench"].get(NIFTY50_KEY)
    bias, pct = nifty_bias_from_bar_vs_prev_close(nifty_bar or {}, prev, missing="unknown")
    long_side = bias != "negative"
    eligible = set(stocks_by_sector)
    sector_bars = {}
    sector_prev = dict(dump["bench"])
    for skey in eligible:
        src = candles_5m.get(skey, []) if freeze else candles.get(skey, [])
        bar = first_5m_bar(src, session) if freeze else forming_bar_from_1m_upto(src, session, upto)
        if bar:
            sector_bars[skey] = bar
    ranked = rank_sectors_vs_prev_close(
        sector_bars, sector_prev, eligible_keys=eligible, descending=long_side
    )
    return ranked, long_side and bias != "unknown", pct, bias


def _bar_for_stock(candles_1m, candles_5m, ik, session, upto, freeze):
    if freeze:
        return first_5m_bar(candles_5m.get(ik, []), session)
    return forming_bar_from_1m_upto(candles_1m.get(ik, []), session, upto)


def simulate_tick(
    *,
    session: date,
    minute: int,
    freeze: bool,
    dump: Dict[str, Any],
    stocks_by_sector: Dict[str, List],
    rows: Dict[str, StockRow],
    candles_1m: Dict[str, List],
    candles_5m: Dict[str, List],
) -> Dict[str, Any]:
    upto = (9, 20) if freeze else (9, minute)
    ranked, have_side, nifty_pct, bias = _sector_rank(
        candles_1m, session, dump, stocks_by_sector, upto, freeze, candles_5m
    )
    unknown = bias == "unknown" or nifty_pct is None
    long_side = have_side and bias != "negative"
    if bias == "negative":
        long_side = False
    ranked_keys = [k for k, _, _ in ranked]
    picked = ranked_keys[:LIVE_SECTORS_TO_PICK]
    wicks = dump["wicks"]
    wick_members = filter_sector_members_by_wick(
        {k: stocks_by_sector.get(k, []) for k in picked}, wicks, long_side=long_side
    )
    top2 = list(picked)
    top2_wick = [len(wick_members.get(k, [])) for k in top2]

    def qualify(members_map):
        bar_map = {}
        pcts = {}
        details = {}
        for skey, mems in members_map.items():
            before = [m["stock"] for m in mems]
            after_sign_syms = []
            after_color = []
            dojis = []
            for m in mems:
                sym = m["stock"]
                row = rows.get(sym)
                if not row:
                    continue
                bar = _bar_for_stock(candles_1m, candles_5m, row.instrument_key, session, upto, freeze)
                if not bar:
                    continue
                prev = dump["stock_prev"].get(sym)
                if not prev:
                    continue
                pct = move_pct_vs_prev_close(float(bar.get("close") or 0), float(prev))
                if pct is None:
                    continue
                bar_map[sym] = bar
                pcts[sym] = float(pct)
            signed = filter_sector_members_by_sign_gate(
                {skey: mems}, pcts, long_side=long_side, move_cap=STOCK_MOVE_CAP_PCT
            )
            colored = filter_sector_members_by_first_5m_color(signed, bar_map, long_side=long_side)
            after_sign_syms = [m["stock"] for m in signed.get(skey, [])]
            for m in colored.get(skey, []):
                b = bar_map.get(m["stock"]) or {}
                flag = first_5m_is_doji(b.get("open"), b.get("close"))
                after_color.append(m["stock"])
                if flag:
                    dojis.append(m["stock"])
            picks = pick_stocks_in_sector(
                colored.get(skey, []),
                bar_map,
                pcts,
                session_date=session,
                fut_by_und={},
                eq_by_symbol={},
                long_side=long_side,
                move_cap=STOCK_MOVE_CAP_PCT,
                top_n=LIVE_STOCKS_PER_SECTOR,
                session_rows=rows,
            )
            details[skey] = {
                "before_wick": [m["stock"] for m in stocks_by_sector.get(skey, [])],
                "after_wick": before,
                "after_sign": after_sign_syms,
                "after_color": after_color,
                "doji": dojis,
                "final_top3": [p.stock for p in picks],
                "pcts": {s: round(pcts[s], 3) for s in after_color if s in pcts},
            }
        return details, {k: [{"stock": s} for s in d["after_color"]] for k, d in details.items()}

    details, after_color = qualify(wick_members)
    new_picked, cascade_from, cascade_to, swapped = try_one_sector_cascade(picked, ranked_keys, after_color)
    if swapped and cascade_to:
        extra = filter_sector_members_by_wick(
            {cascade_to: stocks_by_sector.get(cascade_to, [])}, wicks, long_side=long_side
        )
        extra_d, extra_c = qualify(extra)
        details.update(extra_d)
        after_color[cascade_to] = extra_c.get(cascade_to, [])
        if cascade_from:
            after_color.pop(cascade_from, None)
        picked = new_picked

    n_with = sum(1 for k in picked if details.get(k, {}).get("final_top3"))
    fail = live_lock_failure_reason(
        nifty_unknown=unknown,
        nifty_bar_missing=unknown,
        swapped=swapped,
        n_sectors_with_stocks=n_with,
        top2_wick_counts=top2_wick,
        top2_after_color_counts=[len(details.get(k, {}).get("after_color") or []) for k in top2],
    )
    ranked_view = [
        {"sector": k, "pct": round(p, 3), "selected": k in picked, "cascaded": k == cascade_to}
        for k, p, _ in ranked[:8]
    ]
    return {
        "minute": minute,
        "freeze": freeze,
        "upto": f"09:{minute:02d}" if not freeze else "09:20:05",
        "nifty": {"bias": bias, "pct": None if nifty_pct is None else round(nifty_pct, 3), "long_side": long_side},
        "ranked_sectors": ranked_view,
        "selected": picked,
        "cascade": {"swapped": swapped, "from": cascade_from, "to": cascade_to},
        "per_sector": {k: details.get(k) for k in picked},
        "would_be_lock_status": "failed" if fail else "locked",
        "would_be_failure_reason": fail,
    }


def replay_day(session: date, dump: Dict[str, Any], ux: UpstoxService) -> Dict[str, Any]:
    stocks_by_sector = _members_by_sector(dump)
    rows = _stock_rows(dump)
    index_keys = [NIFTY50_KEY] + _sector_keys()
    dump = apply_asof_prev_close(ux, session, dump, extra_fut_keys=[])
    print(f"fetching 1m indexes {len(index_keys)} for {session}", file=sys.stderr)
    candles_1m = fetch_candles(ux, index_keys, session, "minutes/1")
    n50_sess = len(_session_bars(candles_1m.get(NIFTY50_KEY) or [], session, True))
    print(f"nifty 1m session bars={n50_sess} raw={len(candles_1m.get(NIFTY50_KEY) or [])}", file=sys.stderr)
    print("fetching 5m indexes", file=sys.stderr)
    candles_5m = fetch_candles(ux, index_keys, session, "minutes/5")
    ranked0, _, _, _ = _sector_rank(
        candles_1m, session, dump, stocks_by_sector, (9, 16), False, candles_5m
    )
    if not ranked0:
        ranked0, _, _, _ = _sector_rank(
            candles_1m, session, dump, stocks_by_sector, (9, 20), True, candles_5m
        )
    top_sector_keys = [k for k, _, _ in ranked0[:8]]
    fut_needed = []
    for skey in top_sector_keys:
        for m in stocks_by_sector.get(skey, []):
            row = rows.get(m["stock"])
            if row:
                fut_needed.append(row.instrument_key)
    dump = apply_asof_prev_close(ux, session, dump, extra_fut_keys=fut_needed)

    ticks = []
    needed_iks = set()
    for minute in (16, 17, 18, 19, 20):
        freeze = minute == 20
        # rank first to know wick survivors then fetch those stocks
        upto = (9, 20) if freeze else (9, minute)
        ranked, have, *_rest = _sector_rank(
            candles_1m, session, dump, stocks_by_sector, upto, freeze, candles_5m
        )
        bias = _rest[-1] if _rest else "unknown"
        long_side = have and bias != "negative"
        if _rest and _rest[-1] == "negative":
            long_side = False
        ranked_keys = [k for k, _, _ in ranked]
        picked = ranked_keys[:LIVE_SECTORS_TO_PICK]
        wick_members = filter_sector_members_by_wick(
            {k: stocks_by_sector.get(k, []) for k in picked + ranked_keys[2:3]},
            dump["wicks"],
            long_side=long_side,
        )
        for mems in wick_members.values():
            for m in mems:
                row = rows.get(m["stock"])
                if row:
                    needed_iks.add(row.instrument_key)
        missing_1m = [ik for ik in needed_iks if ik not in candles_1m]
        missing_5m = [ik for ik in needed_iks if ik not in candles_5m]
        if missing_1m:
            print(f"fetch 1m stocks n={len(missing_1m)} min={minute}", file=sys.stderr)
            candles_1m.update(fetch_candles(ux, missing_1m, session, "minutes/1"))
        if freeze and missing_5m:
            print(f"fetch 5m stocks n={len(missing_5m)}", file=sys.stderr)
            candles_5m.update(fetch_candles(ux, missing_5m, session, "minutes/5"))
        ticks.append(
            simulate_tick(
                session=session,
                minute=minute,
                freeze=freeze,
                dump=dump,
                stocks_by_sector=stocks_by_sector,
                rows=rows,
                candles_1m=candles_1m,
                candles_5m=candles_5m,
            )
        )
    cov_1m = _coverage(candles_1m, session, True)
    cov_5m = _coverage(candles_5m, session, False)
    gap = []
    if cov_1m.get(NIFTY50_KEY, 0) == 0:
        gap.append("no_nifty_1m_0915_0920")
    if cov_5m.get(NIFTY50_KEY, 0) == 0:
        gap.append("no_nifty_5m_first_bar")
    return {
        "session_date": session.isoformat(),
        "index_keys": index_keys,
        "ticks": ticks,
        "coverage": {
            "nifty_1m_session_bars": cov_1m.get(NIFTY50_KEY, 0),
            "nifty_5m_session_bars": cov_5m.get(NIFTY50_KEY, 0),
            "index_1m_with_bars": sum(1 for k in index_keys if cov_1m.get(k, 0) > 0),
            "index_5m_with_bars": sum(1 for k in index_keys if cov_5m.get(k, 0) > 0),
            "index_count": len(index_keys),
            "gaps": gap,
        },
        "asof_prev": dump.get("asof_prev"),
        "persist_session_lock": False,
    }


def main() -> None:
    sessions = [date.fromisoformat(a) for a in sys.argv[1:]] or [SESSION]
    dump = dump_paperclip_prev()
    tok = paperclip_token()
    ux = UpstoxService("", "", access_token=tok)
    ux.access_token = tok
    for session in sessions:
        out_path = ROOT / "docs" / "diagnostics" / f"breakfast_live_replay_{session.strftime('%Y%m%d')}.json"
        primary = replay_day(session, dump, ux)
        out = {"primary": primary, "cleaner_day": None, "persist_session_lock": False}
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(out, indent=2))
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
