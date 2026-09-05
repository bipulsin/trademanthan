"""Month-loop HA-VWAP backtest from CSV times, current-month stock futures."""
from __future__ import annotations

import json
import logging
from calendar import monthrange
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.ha_vwap.candles import default_cache_dir, default_out_dir, fetch_session_10m
from backend.services.ha_vwap.config import (
    ARTIFACT_COMBINED,
    CANDLE_DAYS_BACK,
    EMA_PERIOD,
    HISTORY_SESSIONS,
    PUBLIC_ARTIFACT,
    ST_MULTIPLIER,
    ST_PERIOD,
)
from backend.services.ha_vwap.indicators import ema_series, heikin_ashi, session_vwap, supertrend_series
from backend.services.ha_vwap.signals import CsvSignal, date_span, default_signals_path, load_csv_signals, signals_by_session
from backend.services.ha_vwap.simulate import annotate_bars, simulate_session
from backend.services.ha_vwap.universe import HaVwapName, load_universe
from backend.services.market_holiday import refresh_holiday_dates_from_db
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

NOTES = {
    "entry": "CSV Date/Time → session-aligned 10m bar_start (floor from 09:15). Fill = raw 10m close × 1.0003. Exits start on the next bar.",
    "instrument": "Current-month stock FUT from arbitrage_master.currmth_future_instrument_key. Qty = 1 FUT lot.",
    "vwap": "session VWAP from raw 10m typical price × volume from 09:15",
    "ha": "standard HA recursion on session-aligned 10m OHLC",
    "st": "SuperTrend(10, 3) on RAW 10m OHLC; SL if raw close < ST",
    "exits": "first hit after entry bar: TP 0.8% (10m high), HA close < VWAP AND EMA20 (reason=vwap_ema), raw close < ST (reason=supertrend), else 15:15 (reason=time)",
    "filters": "none — entries are CSV-driven, not HA-cross / MACD / top-2 volume",
}


def _iter_trading_days(d0: date, d1: date, holidays: set) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


def _month_chunks_backward(d0: date, d1: date) -> List[Tuple[int, int, date, date]]:
    chunks = []
    y, m = d1.year, d1.month
    while date(y, m, 1) >= date(d0.year, d0.month, 1):
        last = monthrange(y, m)[1]
        a = max(d0, date(y, m, 1))
        b = min(d1, date(y, m, last))
        if a <= b:
            chunks.append((y, m, a, b))
        if m == 1:
            y, m = y - 1, 12
        else:
            m -= 1
    return chunks


def _prior_sessions(session: date, holidays: set, n: int) -> List[date]:
    out: List[date] = []
    d = session - timedelta(days=1)
    while len(out) < n and (session - d).days < 40:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d -= timedelta(days=1)
    out.reverse()
    return out


def _annotate_full(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not bars:
        return []
    o = [float(b["open"]) for b in bars]
    h = [float(b["high"]) for b in bars]
    l = [float(b["low"]) for b in bars]
    c = [float(b["close"]) for b in bars]
    v = [float(b.get("volume") or 0) for b in bars]
    sids = []
    for b in bars:
        start = b.get("bar_start")
        sids.append(start.date() if hasattr(start, "date") else str(b.get("timestamp") or "")[:10])
    _, _, _, ha_c = heikin_ashi(o, h, l, c)
    vwap = session_vwap(h, l, c, v, sids)
    ema20 = ema_series(ha_c, EMA_PERIOD)
    st = supertrend_series(h, l, c, ST_PERIOD, ST_MULTIPLIER)
    return annotate_bars(bars, ha_c, vwap, ema20, st)


def _filter_session(annotated: List[Dict[str, Any]], session_date: date) -> List[Dict[str, Any]]:
    out = []
    for b in annotated:
        start = b.get("bar_start")
        if start is not None and hasattr(start, "date") and start.date() == session_date:
            out.append(b)
    return out


def summarize(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    wins = [t for t in trades if float(t.get("pnl") or 0) > 0]
    pnl = sum(float(t.get("pnl") or 0) for t in trades)
    by_month: Dict[str, Dict[str, Any]] = {}
    by_reason: Dict[str, int] = {}
    for t in trades:
        m = str(t.get("date") or "")[:7]
        bucket = by_month.setdefault(m, {"trades": 0, "wins": 0, "pnl": 0.0})
        bucket["trades"] += 1
        if float(t.get("pnl") or 0) > 0:
            bucket["wins"] += 1
        bucket["pnl"] += float(t.get("pnl") or 0)
        rsn = str(t.get("reason") or "unknown")
        by_reason[rsn] = by_reason.get(rsn, 0) + 1
    for m, b in by_month.items():
        b["win_pct"] = (100.0 * b["wins"] / b["trades"]) if b["trades"] else 0.0
        b["pnl"] = round(b["pnl"], 2)
    return {
        "trades": n,
        "wins": len(wins),
        "win_pct": (100.0 * len(wins) / n) if n else 0.0,
        "pnl": round(pnl, 2),
        "by_month": by_month,
        "by_reason": by_reason,
        "entry_fill": "CSV time → 10m bar start (floor from 09:15); raw close × 1.0003",
        "bars": "session-aligned 10m from Upstox minutes/5 paired (09:15–09:25…)",
        "tp": "entry × 1.008, fill if raw 10m high ≥ TP (after entry bar)",
        "exit": "HA close < VWAP AND EMA20 (vwap_ema); else raw close < SuperTrend(10,3) (supertrend); else 15:15 (time)",
        "no_sl": False,
    }


def _write_json(path: Path, doc: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _month_path(out_dir: Path, year: int, month: int, mode: str) -> Path:
    return out_dir / f"{mode}_{year:04d}-{month:02d}.json"


def _copy_public(combined: Dict[str, Any], repo_root: Path) -> None:
    pub = repo_root / "frontend" / "public" / PUBLIC_ARTIFACT
    slim = {
        "ok": True,
        "summary": combined.get("summary"),
        "trades": combined.get("trades") or [],
        "months_status": combined.get("months_status") or {},
        "notes": combined.get("notes"),
    }
    _write_json(pub, slim)


def drop_json_artifacts(out_dir: Path, repo_root: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for p in list(out_dir.glob("futures_*.json")) + list(out_dir.glob("cash_*.json")):
        p.unlink(missing_ok=True)
    comb = out_dir / ARTIFACT_COMBINED
    if comb.is_file():
        comb.unlink()
    pub = repo_root / "frontend" / "public" / PUBLIC_ARTIFACT
    if pub.is_file():
        pub.unlink()
    logger.info("ha_vwap dropped existing JSON under %s", out_dir)


def run_ha_vwap_backtest(
    upstox: UpstoxService,
    *,
    mode: str = "futures",
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    out_dir: Optional[Path] = None,
    cache_dir: Optional[Path] = None,
    resume: bool = True,
    symbol_pause_sec: float = 0.12,
    limit_symbols: Optional[int] = None,
    copy_public: bool = True,
    csv_path: Optional[Path] = None,
    drop_json: bool = False,
) -> Dict[str, Any]:
    mode = (mode or "futures").strip().lower()
    if mode != "futures":
        raise ValueError("CSV HA-VWAP rebuild is current-month FUT only (mode=futures)")

    csv_rows = load_csv_signals(Path(csv_path) if csv_path else default_signals_path())
    by_day = signals_by_session(csv_rows)
    span0, span1 = date_span(csv_rows)
    if span0 is None:
        raise ValueError("No CSV signals")
    date_from = date_from or span0.date()
    date_to = date_to or span1.date()

    out_dir = out_dir or default_out_dir()
    cache_dir = cache_dir or default_cache_dir()
    repo_root = Path(__file__).resolve().parents[3]
    if drop_json or not resume:
        drop_json_artifacts(out_dir, repo_root)
        resume = False

    holidays = refresh_holiday_dates_from_db() or set()
    universe = {nm.symbol: nm for nm in load_universe("futures")}
    if limit_symbols:
        keep = sorted(universe.keys())[: int(limit_symbols)]
        universe = {k: universe[k] for k in keep}

    chunks = _month_chunks_backward(date_from, date_to)
    months_status: Dict[str, str] = {}
    all_trades: List[Dict[str, Any]] = []

    combined_path = out_dir / ARTIFACT_COMBINED
    existing = _load_json(combined_path) if resume else {}
    if existing.get("trades"):
        all_trades = list(existing["trades"])
    months_status = dict(existing.get("months_status") or {})

    for year, month, a, b in chunks:
        key = f"{mode}_{year:04d}-{month:02d}"
        mpath = _month_path(out_dir, year, month, mode)
        if resume and months_status.get(key) == "complete" and mpath.is_file():
            logger.info("ha_vwap skip complete month %s", key)
            continue
        days = [d for d in _iter_trading_days(a, b, holidays) if d.isoformat() in by_day]
        days.sort(reverse=True)
        month_trades: List[Dict[str, Any]] = list((_load_json(mpath).get("trades") or []) if resume else [])
        month_done = {str(t.get("date")) for t in month_trades}

        for session in days:
            if session.isoformat() in month_done:
                continue
            day_sigs: List[CsvSignal] = by_day.get(session.isoformat()) or []
            symbols = sorted({s.symbol for s in day_sigs})
            logger.info("ha_vwap %s %s csv_rows=%d symbols=%d", mode, session, len(day_sigs), len(symbols))
            by_symbol: Dict[str, List[Dict[str, Any]]] = {}
            lots: Dict[str, int] = {}
            instruments: Dict[str, str] = {}
            keys: Dict[str, str] = {}
            csv_entries = []
            priors = _prior_sessions(session, holidays, HISTORY_SESSIONS)
            for sym in symbols:
                nm: Optional[HaVwapName] = universe.get(sym)
                if nm is None:
                    logger.warning("ha_vwap skip %s %s: no current-month FUT in universe", sym, session)
                    continue
                hist_bars: List[Dict[str, Any]] = []
                for d in [session] + list(priors):
                    try:
                        hist_bars.extend(
                            fetch_session_10m(
                                upstox,
                                nm.instrument_key,
                                d,
                                cache_dir=cache_dir,
                                symbol_pause_sec=symbol_pause_sec if d == session else 0.0,
                                days_back=CANDLE_DAYS_BACK,
                            )
                        )
                    except Exception as e:
                        logger.warning("ha_vwap fetch %s %s: %s", nm.symbol, d, e)
                hist_bars.sort(key=lambda b: b.get("bar_start") or b.get("timestamp"))
                annotated = _annotate_full(hist_bars)
                today = _filter_session(annotated, session)
                if not today:
                    logger.warning("ha_vwap skip %s %s: no 10m candles", sym, session)
                    continue
                by_symbol[nm.symbol] = today
                lots[nm.symbol] = nm.lot_size
                instruments[nm.symbol] = nm.instrument
                keys[nm.symbol] = nm.instrument_key
            for sig in day_sigs:
                csv_entries.append((sig.symbol, sig.bar_start))
            day_trades = simulate_session(
                by_symbol,
                lots=lots,
                instruments=instruments,
                keys=keys,
                session_date=session,
                csv_entries=csv_entries,
            )
            month_trades.extend(day_trades)
            month_doc = {
                "ok": True,
                "mode": mode,
                "month": f"{year:04d}-{month:02d}",
                "from": a.isoformat(),
                "to": b.isoformat(),
                "status": "in_progress",
                "summary": summarize(month_trades),
                "trades": month_trades,
            }
            _write_json(mpath, month_doc)

        status = "complete"
        months_status[key] = status
        month_doc = {
            "ok": True,
            "mode": mode,
            "month": f"{year:04d}-{month:02d}",
            "from": a.isoformat(),
            "to": b.isoformat(),
            "status": status,
            "summary": summarize(month_trades),
            "trades": month_trades,
        }
        _write_json(mpath, month_doc)
        keep = [
            t
            for t in all_trades
            if not (
                str(t.get("date") or "").startswith(f"{year:04d}-{month:02d}")
                and str(t.get("instrument") or "") == "fut"
            )
        ]
        all_trades = keep + month_trades

        combined = {
            "ok": True,
            "generated_at": datetime.now(IST).isoformat(),
            "notes": NOTES,
            "csv_rows": len(csv_rows),
            "months_status": months_status,
            "summary": summarize(all_trades),
            "trades": all_trades,
        }
        _write_json(combined_path, combined)
        if copy_public:
            _copy_public(combined, repo_root)

    combined = _load_json(combined_path)
    combined["summary"] = summarize(all_trades)
    combined["trades"] = all_trades
    combined["months_status"] = months_status
    combined["notes"] = NOTES
    combined["csv_rows"] = len(csv_rows)
    _write_json(combined_path, combined)
    if copy_public:
        _copy_public(combined, repo_root)
    return combined
