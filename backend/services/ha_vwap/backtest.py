"""Month-loop HA-VWAP backtest with resume/append JSON."""
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
    CASH_FROM,
    CASH_TO,
    EMA_PERIOD,
    FUTURES_FROM,
    FUTURES_TO,
    HISTORY_SESSIONS,
    MACD_FAST,
    MACD_SIGNAL,
    MACD_SLOW,
    PUBLIC_ARTIFACT,
)
from backend.services.ha_vwap.indicators import ema_series, heikin_ashi, macd_hist_series, session_vwap
from backend.services.ha_vwap.simulate import annotate_bars, simulate_session
from backend.services.ha_vwap.universe import HaVwapName, load_universe
from backend.services.market_holiday import refresh_holiday_dates_from_db
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _iter_trading_days(d0: date, d1: date, holidays: set) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5 and d not in holidays:
            out.append(d)
        d += timedelta(days=1)
    return out


def _month_chunks_backward(d0: date, d1: date) -> List[Tuple[int, int, date, date]]:
    """[(year, month, from, to), ...] newest month first."""
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


def _flatten_history(hist: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    bars: List[Dict[str, Any]] = []
    for day in hist:
        bars.extend(day)
    return bars


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
    macd_h = macd_hist_series(ha_c, MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    return annotate_bars(bars, ha_c, vwap, ema20, macd_h)


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
        "entry_fill": "raw 10m close × 1.0003 (market fill, not HA close)",
        "bars": "session-aligned 10m from Upstox minutes/5 paired (09:15–09:25…)",
        "tp": "entry × 1.008, fill if raw 10m high ≥ TP",
        "exit": "HA close < VWAP AND HA close < EMA20; else 15:15 close (reason=time)",
        "no_sl": True,
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
) -> Dict[str, Any]:
    mode = (mode or "futures").strip().lower()
    if mode == "cash":
        date_from = date_from or CASH_FROM
        date_to = date_to or CASH_TO
    else:
        date_from = date_from or FUTURES_FROM
        date_to = date_to or FUTURES_TO
        mode = "futures"

    out_dir = out_dir or default_out_dir()
    cache_dir = cache_dir or default_cache_dir()
    holidays = refresh_holiday_dates_from_db() or set()
    names = load_universe(mode)
    if limit_symbols:
        names = names[: int(limit_symbols)]
    chunks = _month_chunks_backward(date_from, date_to)
    repo_root = Path(__file__).resolve().parents[3]
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
            doc = _load_json(mpath)
            logger.info("ha_vwap skip complete month %s", key)
            continue
        days = _iter_trading_days(a, b, holidays)
        days.sort(reverse=True)
        month_trades: List[Dict[str, Any]] = list((_load_json(mpath).get("trades") or []) if resume else [])
        month_done = {str(t.get("date")) for t in month_trades}

        for session in days:
            if session.isoformat() in month_done:
                continue
            logger.info("ha_vwap %s %s symbols=%d", mode, session, len(names))
            by_symbol: Dict[str, List[Dict[str, Any]]] = {}
            lots: Dict[str, int] = {}
            instruments: Dict[str, str] = {}
            keys: Dict[str, str] = {}
            priors = _prior_sessions(session, holidays, HISTORY_SESSIONS)
            for nm in names:
                hist_days = [session] + list(priors)
                hist_bars: List[Dict[str, Any]] = []
                for d in hist_days:
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
                if len(today) < 4:
                    continue
                by_symbol[nm.symbol] = today
                lots[nm.symbol] = nm.lot_size
                instruments[nm.symbol] = nm.instrument
                keys[nm.symbol] = nm.instrument_key
            day_trades = simulate_session(
                by_symbol, lots=lots, instruments=instruments, keys=keys, session_date=session
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
        # replace this month's trades in combined
        keep = [t for t in all_trades if not (str(t.get("date") or "").startswith(f"{year:04d}-{month:02d}") and str(t.get("instrument") or "") == ("cash" if mode == "cash" else "fut"))]
        all_trades = keep + month_trades

        combined = {
            "ok": True,
            "generated_at": datetime.now(IST).isoformat(),
            "notes": {
                "entry": "raw 10m close × 1.0003 slippage (not HA close)",
                "vwap": "session VWAP from raw 10m typical price × volume from 09:15",
                "ha": "standard HA recursion on session-aligned 10m OHLC",
                "filters": "HA crossed above VWAP, HA close > EMA20, MACD hist(104,48,36) > 0, 09:45–12:45",
                "size": "1 FUT lot (futures) or cash shares = that symbol's FUT lot size",
            },
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
    _write_json(combined_path, combined)
    if copy_public:
        _copy_public(combined, repo_root)
    return combined
