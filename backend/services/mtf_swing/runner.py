"""Resumable month-by-month MTF Swing backtest.

Completion is recorded per timeframe and month in data/mtf_swing/progress.json.
Re-running a finished month is a no-op. CSV appends skip trade_id / discard_id
values that are already on disk, and never rewrite earlier bytes.

    python3 scripts/run_mtf_swing_backtest.py
    python3 scripts/run_mtf_swing_backtest.py --timeframe 15m --symbol RELIANCE
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.mtf_swing.config import TIMEFRAMES, MtfSwingConfig, month_windows
from backend.services.mtf_swing.csv_store import append_discards, append_trades
from backend.services.mtf_swing.data import (
    data_dir,
    discards_path,
    known_holidays,
    load_universe,
    lot_size_for,
    progress_path,
    state_path,
    stitch_symbol_bars,
    trades_path,
    window_for_month,
)
from backend.services.mtf_swing.engine import run_symbol

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _load_json(path, default):
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("mtf_swing could not read %s: %s", path, exc)
        return default


def _save_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(path)


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def run_backtest(
    *,
    timeframes: Sequence[str] = TIMEFRAMES,
    symbols: Optional[Sequence[str]] = None,
    resume: bool = True,
    upstox: Any = None,
) -> Dict[str, Any]:
    """Run missing month/timeframe batches. Safe to call again after a stop."""
    hol = known_holidays()
    universe = load_universe()
    if symbols:
        wanted = {s.strip().upper() for s in symbols if s and str(s).strip()}
        universe = [row for row in universe if row["symbol"] in wanted]
    if not universe:
        raise RuntimeError("arbitrage_master returned no symbols")
    client = upstox if upstox is not None else _upstox()
    progress = _load_json(progress_path(), {"completed": []})
    done = set(progress.get("completed") or [])
    summary = {
        "symbols": len(universe),
        "completed_now": [],
        "skipped": [],
        "trades_appended": 0,
        "discards_appended": 0,
        "failed": [],
    }
    run_ts = datetime.now(IST).isoformat()
    months = month_windows()

    for tf in timeframes:
        if tf not in TIMEFRAMES:
            raise ValueError(f"timeframe must be one of {TIMEFRAMES}")
        state = _load_json(state_path(tf), {"symbols": {}})
        sym_state: Dict[str, Any] = state.get("symbols") or {}
        cfg = MtfSwingConfig(timeframe=tf)
        for idx, (m_start, m_end, label) in enumerate(months):
            token = f"{tf}:{label}"
            if resume and token in done:
                summary["skipped"].append(token)
                logger.info("mtf_swing skip %s (already complete)", token)
                continue
            is_last = idx == len(months) - 1
            fetch_start, fetch_end = window_for_month(m_start, m_end, tail_days=20 if is_last else 0)
            logger.info(
                "mtf_swing %s %s symbols=%s window=%s..%s",
                tf,
                label,
                len(universe),
                fetch_start,
                fetch_end,
            )
            screened = candidates = setups = entries = exits = 0
            for row in universe:
                symbol = row["symbol"]
                try:
                    bars, lots = stitch_symbol_bars(
                        client,
                        symbol,
                        fetch_start,
                        fetch_end,
                        tf,
                        hol,
                        underlying_key=row.get("underlying_key") or "",
                    )
                except Exception as exc:
                    logger.warning("mtf_swing fetch %s %s failed: %s", symbol, label, exc)
                    time.sleep(0.25)
                    continue
                if not bars:
                    logger.info("mtf_swing %s %s no candles", symbol, label)
                    time.sleep(0.15)
                    continue
                # Light pacing so a full universe pass does not trip Upstox 429s
                # immediately after a heavy 60m run (which left 15m with empty months).
                time.sleep(0.05)
                lot = lot_size_for(symbol, bars[-1].contract) or next(iter(lots.values()), 0)
                trades, discards, new_state = run_symbol(
                    bars,
                    symbol=symbol,
                    holidays=hol,
                    active_from=m_start,
                    active_to=m_end,
                    state=sym_state.get(symbol),
                    config=cfg,
                    lot_size=lot,
                    lot_by_contract=lots,
                    run_ts=run_ts,
                    finalize_open=is_last,
                    exit_only_after=m_end,
                )
                n_tr = append_trades(trades_path(), trades)
                n_ds = append_discards(discards_path(), discards)
                summary["trades_appended"] += n_tr
                summary["discards_appended"] += n_ds
                sym_state[symbol] = new_state
                screened += 1
                candidates += sum(1 for d in discards if d.get("reason") in ("NO_SWING", "LEVELS_ILLOGICAL"))
                setups += sum(1 for d in discards if d.get("reason") == "WEEK_EXPIRED") + len(trades)
                entries += len(trades)
                exits += sum(1 for t in trades if t.get("exit_reason") != "OPEN")
                logger.info(
                    "mtf_swing %s %s %s bars=%s new_trades=%s new_discards=%s lot=%s",
                    tf,
                    label,
                    symbol,
                    len(bars),
                    n_tr,
                    n_ds,
                    lot,
                )
            if screened == 0:
                logger.error("mtf_swing %s had no candles; leaving the month incomplete", token)
                summary["failed"].append(token)
                continue
            state["symbols"] = sym_state
            _save_json(state_path(tf), state)
            done.add(token)
            progress["completed"] = sorted(done)
            progress["updated"] = run_ts
            progress.pop("note", None)
            _save_json(progress_path(), progress)
            summary["completed_now"].append(token)
            logger.info(
                "mtf_swing progress %s screened=%s structural_discards=%s week_expired=%s entries=%s exits=%s",
                token,
                screened,
                candidates,
                setups,
                entries,
                exits,
            )
    summary["trades_csv"] = str(trades_path())
    summary["discards_csv"] = str(discards_path())
    summary["data_dir"] = str(data_dir())
    return summary
