"""Month-by-month backtest orchestration with in-memory job progress."""
from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.services.divtest.candles import (
    fetch_month_candles_upstox,
    generate_demo_candles,
    months_back,
)
from backend.services.divtest.config import TIMEFRAMES, get_settings, update_settings
from backend.services.divtest.instruments import pick_future_for_month, resolve_instrument
from backend.services.divtest.results_store import (
    append_month_trades,
    month_exists,
    write_month,
)
from backend.services.divtest.strategy import run_strategy

logger = logging.getLogger(__name__)

_jobs: Dict[str, Dict[str, Any]] = {}
_lock = threading.Lock()


def _prev_yyyy_mm(yyyy_mm: str) -> str:
    y, m = [int(x) for x in yyyy_mm.split("-")]
    m -= 1
    if m == 0:
        m = 12
        y -= 1
    return f"{y:04d}-{m:02d}"


def _merge_candles(*series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for chunk in series:
        for c in chunk or []:
            k = str(c.get("timestamp") or c.get("datetime") or "")
            if not k or k in seen:
                continue
            seen.add(k)
            out.append(c)
    out.sort(key=lambda x: str(x.get("timestamp") or x.get("datetime") or ""))
    return out


def _trades_in_month(trades: List[Dict[str, Any]], yyyy_mm: str) -> List[Dict[str, Any]]:
    """Keep only trades whose entry timestamp falls in the target calendar month."""
    kept: List[Dict[str, Any]] = []
    for t in trades or []:
        entry = str(t.get("entry_datetime") or "")
        if len(entry) >= 7 and entry[:7] == yyyy_mm:
            kept.append(t)
    return kept


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _lock:
        job = _jobs.get(job_id)
        return dict(job) if job else None


def _log(job: Dict[str, Any], message: str, level: str = "info") -> None:
    entry = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, "message": message}
    job.setdefault("logs", []).append(entry)
    if len(job["logs"]) > 2000:
        job["logs"] = job["logs"][-1500:]


def start_backtest(
    instruments: str,
    period_months: int = 6,
    force_refresh: bool = False,
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if settings:
        update_settings(settings)
    cfg = get_settings()
    symbols = [s.strip().upper() for s in str(instruments or "").split(",") if s.strip()]
    if not symbols:
        raise ValueError("At least one instrument is required")
    months = months_back(12 if int(period_months) == 12 else 6)
    job_id = str(uuid.uuid4())
    total = len(symbols) * len(months) * len(TIMEFRAMES)
    job: Dict[str, Any] = {
        "id": job_id,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "symbols": symbols,
        "months": months,
        "force_refresh": bool(force_refresh),
        "progress": {
            "total": total,
            "done": 0,
            "current": None,
            "processed_months": [],
            "message": "Starting…",
        },
        "logs": [],
        "partial_trades": 0,
        "error": None,
    }
    with _lock:
        _jobs[job_id] = job
    threading.Thread(target=_execute, args=(job_id, cfg), daemon=True).start()
    return job


def _execute(job_id: str, cfg: Dict[str, Any]) -> None:
    with _lock:
        job = _jobs[job_id]

    def log(msg: str, level: str = "info") -> None:
        _log(job, msg, level)

    try:
        demo = bool(cfg.get("demo_mode"))
        if demo:
            log("DEMO MODE: synthetic candles (no Upstox calls)")
        else:
            log("Using Upstox candles + instrument master")

        for symbol in job["symbols"]:
            if demo:
                resolved = {
                    "symbol": symbol,
                    "found": True,
                    "cash": {"instrument_key": f"DEMO|{symbol}", "lot_size": 1},
                    "futures": [],
                    "is_commodity": symbol in {"CRUDEOIL", "GOLD", "SILVER", "NATGAS"},
                    "lot_size": 65 if symbol in {"NIFTY", "BANKNIFTY"} else 1,
                    "notes": [],
                }
            else:
                try:
                    resolved = resolve_instrument(symbol)
                except Exception as exc:
                    log(f"Resolve failed for {symbol}: {exc}", "error")
                    job["progress"]["done"] += len(job["months"]) * len(TIMEFRAMES)
                    continue
                if not resolved.get("found"):
                    log(f"Instrument not found: {symbol} — skipping", "warn")
                    job["progress"]["done"] += len(job["months"]) * len(TIMEFRAMES)
                    continue
                log(
                    f"Resolved {symbol}: cash={(resolved.get('cash') or {}).get('instrument_key')}, "
                    f"futures={len(resolved.get('futures') or [])}, commodity={resolved.get('is_commodity')}"
                )

            for yyyy_mm in job["months"]:
                job["progress"]["current"] = f"{symbol} {yyyy_mm}"
                processed = ", ".join(job["progress"]["processed_months"]) or "—"
                job["progress"]["message"] = f"Processed: {processed} … fetching {yyyy_mm}"

                for tf in TIMEFRAMES:
                    step = f"{symbol} {tf['id']} {yyyy_mm}"
                    try:
                        if not job["force_refresh"] and month_exists(symbol, tf["id"], yyyy_mm):
                            log(f"Skip {step} (cached; use Force Refresh to re-run)")
                            job["progress"]["done"] += 1
                            continue

                        candles, source, ikey, notes = _fetch_month(
                            symbol, resolved, yyyy_mm, tf, demo, log
                        )
                        if not candles:
                            log(f"No candle data for {step}", "warn")
                            write_month(
                                symbol,
                                tf["id"],
                                yyyy_mm,
                                {
                                    "trades": [],
                                    "candles": 0,
                                    "source": source,
                                    "instrument_key": ikey,
                                    "notes": notes + ["no_data"],
                                    "meta": {"empty": True},
                                },
                            )
                            job["progress"]["done"] += 1
                            continue

                        # Prior calendar month as indicator/pivot warmup so early-month
                        # MACD + divergences are not computed on a cold series.
                        warmup_ym = _prev_yyyy_mm(yyyy_mm)
                        warmup, w_source, _, w_notes = _fetch_month(
                            symbol, resolved, warmup_ym, tf, demo, log
                        )
                        if warmup:
                            notes = list(notes) + [
                                f"warmup:{warmup_ym}:{len(warmup)}:{w_source}"
                            ]
                        elif w_notes:
                            notes = list(notes) + [f"warmup_empty:{warmup_ym}"] + list(w_notes)

                        series = _merge_candles(warmup, candles)
                        qty = int(resolved.get("lot_size") or cfg["equity_default_qty"])
                        entry_mode = "same_close" if cfg.get("entry_mode") == "same_close" else "next_open"
                        result = run_strategy(
                            series,
                            {
                                "macd_fast": cfg["macd_fast"],
                                "macd_slow": cfg["macd_slow"],
                                "macd_signal": cfg["macd_signal"],
                                "divergence_lookback": cfg["divergence_lookback"],
                                "histogram_flip_window": cfg["histogram_flip_window"],
                                "entry_mode": entry_mode,
                                "exit_opposite_flip": cfg["exit_opposite_flip"],
                                "exit_stop_loss": cfg["exit_stop_loss"],
                                "exit_time_based": cfg["exit_time_based"],
                                "atr_period": cfg["atr_period"],
                                "atr_stop_multiplier": cfg["atr_stop_multiplier"],
                                "atr_target_multiplier": cfg["atr_target_multiplier"],
                                "max_holding_bars": cfg["max_holding_bars"],
                                "swing_order": cfg["swing_order"],
                                "instrument": symbol,
                                "timeframe": tf["id"],
                                "qty_per_lot": qty,
                                "brokerage_per_side": cfg["brokerage_per_side"],
                                "brokerage_enabled": cfg["brokerage_enabled"],
                            },
                        )
                        trades = _trades_in_month(result.get("trades") or [], yyyy_mm)
                        payload = {
                            "trades": trades,
                            "candles": len(candles),
                            "warmup_candles": len(warmup or []),
                            "source": source,
                            "instrument_key": ikey,
                            "notes": notes,
                            "meta": {
                                **(result.get("meta") or {}),
                                "series_candles": len(series),
                                "month_trade_count": len(trades),
                            },
                        }
                        if job["force_refresh"]:
                            write_month(symbol, tf["id"], yyyy_mm, payload)
                        else:
                            append_month_trades(symbol, tf["id"], yyyy_mm, trades, payload)
                        job["partial_trades"] += len(trades)
                        log(
                            f"{step}: {len(candles)} month + {len(warmup or [])} warmup "
                            f"via {source}, {len(trades)} trades"
                        )
                    except Exception as exc:
                        log(f"Error on {step}: {exc} — continuing", "error")
                        write_month(
                            symbol,
                            tf["id"],
                            yyyy_mm,
                            {
                                "trades": [],
                                "candles": 0,
                                "source": "error",
                                "notes": [str(exc)],
                                "meta": {"error": True},
                            },
                        )
                    job["progress"]["done"] += 1

                if yyyy_mm not in job["progress"]["processed_months"]:
                    job["progress"]["processed_months"].append(yyyy_mm)
                job["progress"]["message"] = "Processed: " + ", ".join(job["progress"]["processed_months"])

        job["status"] = "completed"
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
        job["progress"]["message"] = f"Done. {job['partial_trades']} new trades this run."
        log("Backtest job completed")
    except Exception as exc:
        logger.exception("divtest job failed")
        job["status"] = "failed"
        job["error"] = str(exc)
        job["finished_at"] = datetime.now(timezone.utc).isoformat()
        log(f"Job failed: {exc}", "error")


def _fetch_month(symbol, resolved, yyyy_mm, tf, demo, log):
    notes: List[str] = []
    if demo:
        seed = sum(ord(c) for c in symbol)
        return (
            generate_demo_candles(yyyy_mm, tf["id"], seed),
            "demo",
            f"DEMO|{symbol}",
            ["demo_synthetic"],
        )

    prefer_fut = bool(resolved.get("is_commodity") or (resolved.get("futures") or []))
    if prefer_fut:
        fut = pick_future_for_month(resolved, yyyy_mm)
        if fut:
            try:
                candles = fetch_month_candles_upstox(fut["instrument_key"], tf["interval"], yyyy_mm)
                if candles:
                    return candles, f"future:{fut.get('trading_symbol')}", fut["instrument_key"], notes
                notes.append(f"Empty future candles for {fut.get('trading_symbol')} {yyyy_mm}")
                log(
                    f"No future contract data for {symbol} {yyyy_mm} ({fut.get('trading_symbol')}), falling back to cash",
                    "warn",
                )
            except Exception as exc:
                notes.append(f"Future fetch failed: {exc}")
                log(f"No future contract data for {symbol} {yyyy_mm}, falling back to cash ({exc})", "warn")
        else:
            notes.append(f"No future contract mapped for {yyyy_mm}")
            log(f"No future contract data for {symbol} {yyyy_mm}, falling back to cash", "warn")

    cash = resolved.get("cash") or {}
    if cash.get("instrument_key"):
        try:
            candles = fetch_month_candles_upstox(cash["instrument_key"], tf["interval"], yyyy_mm)
            return (
                candles,
                f"cash:{cash.get('trading_symbol') or cash['instrument_key']}",
                cash["instrument_key"],
                notes,
            )
        except Exception as exc:
            notes.append(f"Cash fetch failed: {exc}")
            log(f"Cash segment fetch failed for {symbol} {yyyy_mm}: {exc}", "error")
    else:
        notes.append("No cash instrument available")
        log(f"No cash/equity fallback for {symbol} {yyyy_mm}", "warn")
    return [], "none", None, notes
