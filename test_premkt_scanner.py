#!/usr/bin/env python3
"""
Pre-market scanner test harness: simulate run at a fixed IST time using Upstox historical data.

Usage:
  PYTHONPATH=. python test_premkt_scanner.py --demo-one RELIANCE
  PYTHONPATH=. python test_premkt_scanner.py --limit 30
  PYTHONPATH=. python test_premkt_scanner.py --sample
  PYTHONPATH=. python test_premkt_scanner.py --date 2026-04-13 --validate

Environment / backend.config: TEST_SIMULATION_DATE, TEST_SIMULATION_TIME, TEST_SYMBOL_COUNT
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Repo root on path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.smart_futures_picker.indicators import compute_obv_slope_daily, ema_slope_norm_m5

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("premkt_test")

IST = pytz.timezone("Asia/Kolkata")

# Weighted composite (same as product spec)
W_OBV = 0.30
W_GAP = 0.25
W_RANGE = 0.25
W_MOM = 0.20


def _parse_candle_date(ts: Any) -> Optional[date]:
    """
    Calendar date in Asia/Kolkata for an Upstox candle timestamp (often UTC ISO).
    Using the raw UTC date breaks NSE daily alignment (evening UTC = previous IST date).
    """
    if ts is None:
        return None
    s = str(ts).strip()
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = IST.localize(dt)
            else:
                dt = dt.astimezone(IST)
            return dt.date()
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def prev_trading_day(d: date) -> date:
    x = d - timedelta(days=1)
    while x.weekday() >= 5:
        x -= timedelta(days=1)
    return x


def default_simulation_date() -> date:
    """Yesterday, or last Friday when today is Monday."""
    today = date.today()
    if today.weekday() == 0:
        return today - timedelta(days=3)
    d = today - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def parse_sim_date(s: Optional[str]) -> date:
    if s and str(s).strip():
        return date.fromisoformat(str(s).strip()[:10])
    ds = os.getenv("TEST_SIMULATION_DATE", "").strip()
    if ds:
        return date.fromisoformat(ds[:10])
    try:
        from backend.config import settings

        ds2 = (getattr(settings, "TEST_SIMULATION_DATE", None) or "").strip()
        if ds2:
            return date.fromisoformat(ds2[:10])
    except Exception:
        pass
    return default_simulation_date()


def parse_sim_time(cli: Optional[str]) -> str:
    if cli and str(cli).strip():
        return str(cli).strip()
    t = os.getenv("TEST_SIMULATION_TIME", "").strip()
    if t:
        return t
    try:
        from backend.config import settings

        return (getattr(settings, "TEST_SIMULATION_TIME", None) or "09:10:00").strip()
    except Exception:
        return "09:10:00"


def parse_symbol_limit(cli: Optional[int]) -> int:
    if cli is not None:
        return max(1, int(cli))
    try:
        return max(1, int(os.getenv("TEST_SYMBOL_COUNT", "200")))
    except ValueError:
        return 200


def demo_historical_fetch_one(upstox: Any, instrument_key: str, sim_date: date) -> None:
    """
    Minimal Upstox historical-candle demo for one equity key (same endpoint as production).

    GET /v2/historical-candle/{instrument_key}/days/1/{to_date}/{from_date}
    Anchors ``to_date`` to the last completed session before sim_date for a stable window.
    """
    pre = prev_trading_day(sim_date)
    print("\n--- Demo: Upstox historical daily candles (one symbol) ---")
    print(f"instrument_key: {instrument_key}")
    print(f"simulation_date: {sim_date.isoformat()} (premarket uses prior session through {pre.isoformat()})")
    daily = upstox.get_historical_candles_by_instrument_key(
        instrument_key,
        interval="days/1",
        days_back=14,
        range_end_date=pre,
    )
    daily = _sort_candles(daily)
    print(f"candles returned: {len(daily)}")
    for c in daily[-5:]:
        print(f"  ts={c.get('timestamp')} O={c.get('open')} H={c.get('high')} L={c.get('low')} C={c.get('close')} V={c.get('volume')}")
    print("--- end demo ---\n")


@dataclass
class SymbolRow:
    stock: str
    instrument_key: str
    obv_slope: float = 0.0
    gap_pct_signed: float = 0.0
    gap_strength: float = 0.0
    range_position: float = 0.0
    momentum: float = 0.0
    obv_norm: float = 0.0
    gap_norm: float = 0.0
    range_norm: float = 0.0
    mom_norm: float = 0.0
    composite_score: float = 0.0
    error: Optional[str] = None


def _min_max_norm(values: Sequence[float]) -> List[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    span = hi - lo
    if span <= 1e-12:
        return [0.5 for _ in values]
    return [(v - lo) / span for v in values]


class PremktTester:
    def __init__(
        self,
        simulation_date: date,
        simulation_time: str = "09:10:00",
        symbol_limit: int = 200,
    ):
        self.simulation_date = simulation_date
        self.simulation_time = simulation_time
        self.symbol_limit = symbol_limit
        self._rows: List[SymbolRow] = []
        self._upstox: Optional[Any] = None

    def _ux(self) -> Any:
        if self._upstox is None:
            from backend.config import settings
            from backend.services.upstox_service import UpstoxService

            self._upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        return self._upstox

    def load_test_instruments(self) -> List[Tuple[str, str]]:
        """(stock, stock_instrument_key) from arbitrage_master, cap symbol_limit."""
        out: List[Tuple[str, str]] = []
        db = SessionLocal()
        try:
            q = db.execute(
                text(
                    """
                    SELECT stock, stock_instrument_key
                    FROM arbitrage_master
                    WHERE stock_instrument_key IS NOT NULL
                      AND TRIM(stock_instrument_key) <> ''
                    ORDER BY stock
                    LIMIT :lim
                    """
                ),
                {"lim": int(self.symbol_limit)},
            ).fetchall()
            for stock, ikey in q:
                st = str(stock or "").strip().upper()
                ik = str(ikey or "").strip()
                if st and ik:
                    out.append((st, ik))
        except Exception as e:
            logger.error("DB arbitrage_master: %s", e)
        finally:
            db.close()
        if not out:
            logger.warning("No rows from arbitrage_master; use --sample or check DATABASE_URL")
        return out

    def fetch_historical_data(self, stock: str, instrument_key: str) -> Dict[str, Any]:
        """
        prev_5_days_ohlcv: last 5 completed daily bars ending prev_trading_day(sim) (for display)
        OBV uses 10 sessions ending prev_trading_day (matches production compute_obv_slope_daily input)
        prev_day_close: previous session close vs simulation_date
        52w_high / 52w_low: from ~270 calendar days of daily bars ending prev_trading_day
        recent_5min_bars: prior session 5m closes for momentum
        """
        ux = self._ux()
        sim = self.simulation_date
        pre = prev_trading_day(sim)

        out: Dict[str, Any] = {"stock": stock.upper(), "instrument_key": instrument_key, "error": None}

        # Long history for 52w proxy (trading days in window)
        daily_raw = ux.get_historical_candles_by_instrument_key(
            instrument_key, interval="days/1", days_back=320, range_end_date=pre
        )
        daily = _sort_candles(daily_raw)
        if len(daily) < 30:
            out["error"] = f"insufficient daily history ({len(daily)})"
            return out

        by_d = {}
        for c in daily:
            dd = _parse_candle_date(c.get("timestamp"))
            if dd:
                by_d[dd] = c

        if pre not in by_d:
            out["error"] = f"no daily bar for prev session {pre}"
            return out

        prev_close = float(by_d[pre]["close"])
        if prev_close <= 0:
            out["error"] = "bad prev_close"
            return out

        # OBV slope: last 10 daily bars ending last session in `daily` (range_end_date=pre)
        tail10 = daily[-10:]
        if len(tail10) < 10:
            out["error"] = "need 10 daily bars for OBV"
            return out
        closes = [float(x["close"]) for x in tail10]
        vols = [float(x.get("volume") or 0) for x in tail10]
        obv_slope = compute_obv_slope_daily(closes, vols)

        prev5 = daily[-5:]
        out["prev_5_days_ohlcv"] = [
            {
                "date": str(_parse_candle_date(x.get("timestamp"))),
                "o": float(x["open"]),
                "h": float(x["high"]),
                "l": float(x["low"]),
                "c": float(x["close"]),
                "v": float(x.get("volume") or 0),
            }
            for x in prev5
        ]

        # 52w range from history through `pre` (completed sessions only)
        highs = [float(x["high"]) for x in daily]
        lows = [float(x["low"]) for x in daily]
        w52_hi = max(highs)
        w52_lo = min(lows)
        if w52_hi - w52_lo <= 1e-9:
            out["error"] = "degenerate 52w range"
            return out

        # Simulation session bar (open vs prev close = gap); requires completed day in API
        daily_to_sim = ux.get_historical_candles_by_instrument_key(
            instrument_key, interval="days/1", days_back=5, range_end_date=sim
        )
        daily_to_sim = _sort_candles(daily_to_sim)
        sim_bar = None
        for c in daily_to_sim:
            if _parse_candle_date(c.get("timestamp")) == sim:
                sim_bar = c
                break

        day_open = float(sim_bar.get("open") or 0) if sim_bar else 0.0
        day_open_source = "daily"
        if day_open <= 0:
            # Same calendar day before EOD: Upstox often has no finalized daily yet — use first cash 5m open (≥9:15 IST).
            m5today = ux.get_historical_candles_by_instrument_key(
                instrument_key, interval="minutes/5", days_back=2, range_end_date=sim
            )
            m5today = _sort_candles(m5today)
            for c in m5today:
                ts = str(c.get("timestamp") or "")
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = IST.localize(dt)
                    else:
                        dt = dt.astimezone(IST)
                except Exception:
                    continue
                if dt.date() != sim:
                    continue
                if dt.hour < 9 or (dt.hour == 9 and dt.minute < 15):
                    continue
                o = float(c.get("open") or 0)
                if o > 0:
                    day_open = o
                    day_open_source = "first_5m_open"
                    break

        if day_open <= 0:
            out["error"] = f"no session open for {sim} (no daily or intraday 5m yet)"
            return out
        out["day_open_source"] = day_open_source

        gap_pct = (day_open - prev_close) / prev_close * 100.0
        gap_strength = abs(gap_pct)

        # Range position at open vs ~52w band
        range_pos = (day_open - w52_lo) / (w52_hi - w52_lo + 1e-12)
        range_pos = max(0.0, min(1.0, float(range_pos)))
        out["prev_day_close"] = prev_close
        out["day_open"] = day_open
        out["52w_high"] = w52_hi
        out["52w_low"] = w52_lo
        out["gap_pct_signed"] = gap_pct
        out["gap_strength"] = gap_strength
        out["range_position"] = range_pos
        out["obv_slope"] = obv_slope

        # Momentum: prior session 5m closes → ema_slope_norm_m5
        m5 = ux.get_historical_candles_by_instrument_key(
            instrument_key, interval="minutes/5", days_back=5, range_end_date=pre
        )
        m5 = _sort_candles(m5)
        closes_m5: List[float] = []
        for c in m5:
            ts = str(c.get("timestamp") or "")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = IST.localize(dt)
                else:
                    dt = dt.astimezone(IST)
            except Exception:
                continue
            if dt.date() != pre:
                continue
            if dt.hour < 9 or (dt.hour == 9 and dt.minute < 15):
                continue
            if dt.hour > 15 or (dt.hour == 15 and dt.minute > 35):
                continue
            closes_m5.append(float(c["close"]))

        out["recent_5min_bars"] = len(closes_m5)
        if len(closes_m5) >= 20:
            mom = ema_slope_norm_m5(closes_m5)
        else:
            mom = 0.0
        out["momentum"] = float(mom)

        return out

    def compute_premkt_score(self, raw: Dict[str, Any]) -> SymbolRow:
        r = SymbolRow(
            stock=str(raw.get("stock", "")),
            instrument_key=str(raw.get("instrument_key", "")),
        )
        if raw.get("error"):
            r.error = str(raw["error"])
            return r
        r.obv_slope = float(raw.get("obv_slope", 0))
        r.gap_pct_signed = float(raw.get("gap_pct_signed", 0))
        r.gap_strength = float(raw.get("gap_strength", 0))
        r.range_position = float(raw.get("range_position", 0))
        r.momentum = float(raw.get("momentum", 0))
        return r

    def rank_all_symbols(self, symbols: List[Tuple[str, str]]) -> List[SymbolRow]:
        raw_rows: List[SymbolRow] = []
        for stock, ikey in symbols:
            d = self.fetch_historical_data(stock, ikey)
            row = self.compute_premkt_score(d)
            if row.error:
                logger.warning("%s skipped: %s", stock, row.error)
                continue
            raw_rows.append(row)
            time.sleep(0.05)

        if len(raw_rows) < 3:
            return raw_rows

        obv_v = [r.obv_slope for r in raw_rows]
        gap_v = [r.gap_strength for r in raw_rows]
        rng_v = [r.range_position for r in raw_rows]
        mom_v = [r.momentum for r in raw_rows]

        obv_n = _min_max_norm(obv_v)
        gap_n = _min_max_norm(gap_v)
        rng_n = _min_max_norm(rng_v)
        mom_n = _min_max_norm(mom_v)

        for i, r in enumerate(raw_rows):
            r.obv_norm = obv_n[i]
            r.gap_norm = gap_n[i]
            r.range_norm = rng_n[i]
            r.mom_norm = mom_n[i]
            r.composite_score = (
                W_OBV * r.obv_norm
                + W_GAP * r.gap_norm
                + W_RANGE * r.range_norm
                + W_MOM * r.mom_norm
            )

        raw_rows.sort(key=lambda x: x.composite_score, reverse=True)
        self._rows = raw_rows
        return raw_rows

    def print_results(self, top_n: int = 10) -> None:
        sim = self.simulation_date
        tm = self.simulation_time
        print(f"\nPRE-MARKET SCANNER TEST — {sim.isoformat()} {tm} IST")
        print("=" * 90)
        hdr = f"{'Rank':<4} | {'Symbol':<12} | {'Score':>7} | {'OBV Sl':>8} | {'Gap%':>7} | {'RngPos':>7} | {'Mom':>7}"
        print(hdr)
        print("-" * len(hdr))
        for i, r in enumerate(self._rows[:top_n], start=1):
            print(
                f"{i:<4} | {r.stock:<12} | {r.composite_score:7.4f} | {r.obv_slope:+8.4f} | {r.gap_pct_signed:+7.2f} | {r.range_position:7.4f} | {r.momentum:+7.4f}"
            )
        top = self._rows[:top_n]
        if top:
            avg = sum(x.composite_score for x in top) / len(top)
            pos_gaps = sum(1 for x in top if x.gap_pct_signed > 0)
            near_52 = sum(1 for x in top if x.range_position >= 0.85)
            print("-" * len(hdr))
            print(f"Average score (Top {top_n}): {avg:.4f}")
            print(f"Positive gaps: {pos_gaps}/{len(top)}")
            print(f"Range position >= 0.85 (near 52w high proxy): {near_52}/{len(top)}")

    def run_validation(self, top_n: int = 10) -> None:
        """Compare Top N to same-day 5m session (open vs close)."""
        ux = self._ux()
        sim = self.simulation_date
        print(f"\nSCANNER VALIDATION — {sim.isoformat()}")
        print("=" * 70)
        print(f"{'Symbol':<12} | {'Score':>7} | {'Open→Close%':>12} | {'Hit/Miss':>8}")
        print("-" * 70)
        hits = 0
        total = 0
        for r in self._rows[:top_n]:
            if r.error:
                continue
            total += 1
            m5 = ux.get_historical_candles_by_instrument_key(
                r.instrument_key, interval="minutes/5", days_back=2, range_end_date=sim
            )
            m5 = _sort_candles(m5)
            day_op = None
            day_cl = None
            for c in m5:
                ts = str(c.get("timestamp") or "")
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = IST.localize(dt)
                    else:
                        dt = dt.astimezone(IST)
                except Exception:
                    continue
                if dt.date() != sim:
                    continue
                if dt.hour < 9 or (dt.hour == 9 and dt.minute < 15):
                    continue
                if day_op is None:
                    day_op = float(c["open"])
                day_cl = float(c["close"])
            if not day_op or not day_cl or day_op <= 0:
                print(f"{r.stock:<12} | {r.composite_score:7.4f} | {'n/a':>12} | {'—':>8}")
                continue
            pct = (day_cl - day_op) / day_op * 100.0
            bullish = r.composite_score >= 0.5
            ok = (bullish and pct >= 0) or ((not bullish) and pct <= 0)
            if ok:
                hits += 1
            tag = "HIT" if ok else "miss"
            print(f"{r.stock:<12} | {r.composite_score:7.4f} | {pct:+11.2f}% | {tag:>8}")

        if total:
            print("-" * 70)
            print(f"Heuristic accuracy (score vs same-day return sign): {hits}/{total} ({100.0 * hits / total:.0f}%)")


def load_sample_rows(path: Path) -> Tuple[Optional[date], List[SymbolRow]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    sd = data.get("simulation_date")
    sim_d: Optional[date] = None
    if sd:
        try:
            sim_d = date.fromisoformat(str(sd)[:10])
        except ValueError:
            sim_d = None
    rows = []
    for x in data.get("rows") or []:
        rows.append(
            SymbolRow(
                stock=str(x.get("stock", "")),
                instrument_key=str(x.get("instrument_key", "")),
                obv_slope=float(x.get("obv_slope", 0)),
                gap_pct_signed=float(x.get("gap_pct_signed", 0)),
                gap_strength=float(x.get("gap_strength", 0)),
                range_position=float(x.get("range_position", 0)),
                momentum=float(x.get("momentum", 0)),
                composite_score=float(x.get("composite_score", 0)),
            )
        )
    if len(rows) >= 3:
        obv_v = [r.obv_slope for r in rows]
        gap_v = [r.gap_strength for r in rows]
        rng_v = [r.range_position for r in rows]
        mom_v = [r.momentum for r in rows]
        obv_n = _min_max_norm(obv_v)
        gap_n = _min_max_norm(gap_v)
        rng_n = _min_max_norm(rng_v)
        mom_n = _min_max_norm(mom_v)
        for i, r in enumerate(rows):
            r.obv_norm = obv_n[i]
            r.gap_norm = gap_n[i]
            r.range_norm = rng_n[i]
            r.mom_norm = mom_n[i]
            r.composite_score = (
                W_OBV * r.obv_norm + W_GAP * r.gap_norm + W_RANGE * r.range_norm + W_MOM * r.mom_norm
            )
        rows.sort(key=lambda x: x.composite_score, reverse=True)
    return sim_d, rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Pre-market scanner test harness")
    ap.add_argument("--date", help="Simulation session date YYYY-MM-DD", default=None)
    ap.add_argument("--time", help="Label time HH:MM:SS", default=None)
    ap.add_argument("--limit", type=int, default=None, help="Max symbols from arbitrage_master")
    ap.add_argument("--top", type=int, default=10, help="Rows to print")
    ap.add_argument("--sample", action="store_true", help="Use sample_data.json only (no Upstox)")
    ap.add_argument("--sample-path", type=Path, default=ROOT / "sample_data.json")
    ap.add_argument("--demo-one", metavar="STOCK", help="Print one-symbol historical fetch and exit")
    ap.add_argument("--validate", action="store_true", help="Optional same-day validation")
    args = ap.parse_args()

    sim_date = parse_sim_date(args.date)
    sim_time = parse_sim_time(args.time)
    lim = parse_symbol_limit(args.limit)

    if args.demo_one:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        sym = args.demo_one.strip().upper()
        db = SessionLocal()
        try:
            row = db.execute(
                text(
                    "SELECT stock_instrument_key FROM arbitrage_master WHERE UPPER(TRIM(stock)) = :s LIMIT 1"
                ),
                {"s": sym},
            ).first()
        finally:
            db.close()
        if not row or not row[0]:
            print(f"Symbol {sym} not in arbitrage_master; pass a valid stock.")
            return 1
        demo_historical_fetch_one(ux, str(row[0]).strip(), sim_date)
        return 0

    tester = PremktTester(sim_date, simulation_time=sim_time, symbol_limit=lim)

    if args.sample:
        if not args.sample_path.is_file():
            print(f"Missing {args.sample_path}")
            return 1
        sd_meta, trows = load_sample_rows(args.sample_path)
        tester = PremktTester(
            sd_meta or sim_date,
            simulation_time=sim_time,
            symbol_limit=lim,
        )
        tester._rows = trows
        tester.print_results(top_n=args.top)
        return 0

    syms = tester.load_test_instruments()
    if not syms:
        print("No instruments; try --sample or fix DB.")
        return 1

    tester.rank_all_symbols(syms)
    tester.print_results(top_n=args.top)
    if args.validate and tester._rows:
        tester.run_validation(top_n=args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main())
