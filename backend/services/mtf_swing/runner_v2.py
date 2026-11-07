"""MTF Swing v2 experiment runner.

Runs independent flat-start months (default 2026-06 and 2026-10) on 60m and 15m.
Writes only under data/mtf_swing_v2/ — never touches v1 trades.csv.

    python3 scripts/run_mtf_swing_backtest.py --v2 --months 2026-06,2026-10 --timeframes 60m,15m
    python3 scripts/run_mtf_swing_backtest.py --v2 --ablation
"""
from __future__ import annotations

import copy
import json
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.mtf_swing.config import (
    ABLATION_VARIANTS,
    TIMEFRAMES,
    V2_MONTHS,
    ablation_config,
    month_window_for,
    v2_config,
)
from backend.services.mtf_swing.csv_store import (
    append_discards_v2,
    append_regime_log,
    append_trades_v2,
    write_variant_summary,
)
from backend.services.mtf_swing.data import (
    NIFTY50_KEY,
    data_dir_v2,
    discards_path_v2,
    fetch_index_day_bars,
    known_holidays,
    load_universe,
    lot_size_for,
    progress_path_v2,
    regime_log_path,
    state_path_v2,
    stitch_symbol_bars,
    trades_path_v2,
    variant_summary_path,
    window_for_month,
)
from backend.services.mtf_swing.engine import run_symbol
from backend.services.mtf_swing.metrics import ablation_table
from backend.services.mtf_swing.regime import build_regime_series, regime_by_screen_date, regime_log_rows

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_FULL_UNIVERSE = 20
_MIN_SCREENED_RATIO = 0.70
_MIN_ENTRIES_FULL = 1  # v2 months are thinner; require at least one screened pass
_MONTH_ATTEMPTS = 3
_EMPTY_STREAK_COOLDOWN = 45.0
_MONTH_RETRY_COOLDOWN = 90.0
# Follow trades past the month end through latest available data.
_FORWARD_TAIL_DAYS = 120
# Regime MA needs ~10 weekly closes + buffer before the first screen.
_REGIME_WARMUP_DAYS = 120


def _load_json(path, default):
    if not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("mtf_swing_v2 could not read %s: %s", path, exc)
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


def _month_thresholds(universe_size: int) -> tuple[int, int]:
    if universe_size >= _FULL_UNIVERSE:
        return max(1, int(_MIN_SCREENED_RATIO * universe_size)), _MIN_ENTRIES_FULL
    return max(1, universe_size), 0


def _parse_months(raw: Optional[Sequence[str]]) -> List[str]:
    if not raw:
        return list(V2_MONTHS)
    out: List[str] = []
    for item in raw:
        for part in str(item).split(","):
            lab = part.strip()
            if not lab:
                continue
            month_window_for(lab)  # validate
            out.append(lab)
    return out or list(V2_MONTHS)


def _parse_timeframes(raw: Optional[Sequence[str]]) -> List[str]:
    if not raw:
        return list(TIMEFRAMES)
    out: List[str] = []
    for item in raw:
        for part in str(item).split(","):
            tf = part.strip().lower()
            if not tf:
                continue
            if tf not in TIMEFRAMES:
                raise ValueError(f"timeframe must be one of {TIMEFRAMES}")
            out.append(tf)
    return out or list(TIMEFRAMES)


def _build_regime_map(upstox: Any, cfg, hol: set, fetch_start: date, fetch_end: date, run_ts: str) -> Dict[str, str]:
    if not cfg.regime_filter:
        return {}
    regime_start = fetch_start - timedelta(days=_REGIME_WARMUP_DAYS)
    bars = fetch_index_day_bars(
        upstox,
        instrument_key=NIFTY50_KEY,
        start=regime_start,
        end=fetch_end,
    )
    points = build_regime_series(
        bars,
        ma_length=int(cfg.regime_ma_length),
        ma_type=cfg.regime_ma_type,
    )
    append_regime_log(
        regime_log_path(),
        regime_log_rows(
            points,
            symbol=cfg.regime_symbol,
            ma_type=cfg.regime_ma_type,
            ma_length=int(cfg.regime_ma_length),
            run_ts=run_ts,
        ),
    )
    mapping = regime_by_screen_date(points)
    logger.info(
        "mtf_swing_v2 regime points=%s span=%s..%s sample=%s",
        len(points),
        regime_start,
        fetch_end,
        list(mapping.items())[:3],
    )
    return mapping


def _run_one_variant(
    *,
    cfg,
    timeframes: Sequence[str],
    months: Sequence[str],
    universe: Sequence[Dict[str, str]],
    client: Any,
    hol: set,
    resume: bool,
    run_ts: str,
) -> Dict[str, Any]:
    progress = _load_json(progress_path_v2(), {"completed": []})
    done = set(progress.get("completed") or [])
    summary: Dict[str, Any] = {
        "variant": cfg.ablation_tag or cfg.variant,
        "completed_now": [],
        "skipped": [],
        "trades_appended": 0,
        "discards_appended": 0,
        "failed": [],
    }
    min_screened, min_entries = _month_thresholds(len(universe))
    tag = cfg.ablation_tag or "all"

    for tf in timeframes:
        # Independent flat starts: no cross-month state carry for v2 months.
        for label in months:
            m_start, m_end, _ = month_window_for(label)
            token = f"v2:{tag}:{tf}:{label}"
            if resume and token in done:
                summary["skipped"].append(token)
                logger.info("mtf_swing_v2 skip %s", token)
                continue
            fetch_start, fetch_end = window_for_month(m_start, m_end, tail_days=_FORWARD_TAIL_DAYS)
            # Cap forward at today-ish; Upstox will return what exists.
            today = datetime.now(IST).date()
            if fetch_end > today:
                fetch_end = today
            month_ok = False
            for attempt in range(1, _MONTH_ATTEMPTS + 1):
                logger.info(
                    "mtf_swing_v2 %s %s %s symbols=%s window=%s..%s attempt=%s/%s",
                    tag,
                    tf,
                    label,
                    len(universe),
                    fetch_start,
                    fetch_end,
                    attempt,
                    _MONTH_ATTEMPTS,
                )
                regime_map = _build_regime_map(client, cfg, hol, fetch_start, fetch_end, run_ts)
                screened = entries = 0
                empty_streak = 0
                pending_trades: List[Dict[str, Any]] = []
                pending_discards: List[Dict[str, Any]] = []
                # Fresh flat state each month/variant/tf.
                sym_state: Dict[str, Any] = {}
                for row in universe:
                    symbol = row["symbol"]
                    bars: List[Any] = []
                    lots: Dict[str, int] = {}
                    for _fetch_try in range(2):
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
                            logger.warning("mtf_swing_v2 fetch %s %s: %s", symbol, label, exc)
                            time.sleep(1.0)
                            bars = []
                            lots = {}
                        if bars:
                            empty_streak = 0
                            break
                        empty_streak += 1
                        if empty_streak >= 8:
                            time.sleep(_EMPTY_STREAK_COOLDOWN)
                            empty_streak = 0
                        else:
                            time.sleep(0.5)
                    if not bars:
                        continue
                    time.sleep(0.4)
                    lot = lot_size_for(symbol, bars[-1].contract) or next(iter(lots.values()), 0)
                    month_cfg = copy.copy(cfg)
                    month_cfg.timeframe = tf
                    trades, discards, new_state = run_symbol(
                        bars,
                        symbol=symbol,
                        holidays=hol,
                        active_from=m_start,
                        active_to=m_end,
                        state=None,  # flat start each month
                        config=month_cfg,
                        lot_size=lot,
                        lot_by_contract=lots,
                        run_ts=run_ts,
                        # Bars include a long forward tail; exits keep managing after
                        # active_to while new screens/entries stop at month end.
                        finalize_open=True,
                        exit_only_after=m_end,
                        regime_map=regime_map,
                    )
                    # Entries only count when the entry month matches the batch month.
                    month_trades = [t for t in trades if t.get("month") == label]
                    # Keep discards that belong to this screen window.
                    pending_trades.extend(month_trades)
                    # Also keep OPEN/closed that exited later but entered this month — already filtered.
                    pending_discards.extend(discards)
                    sym_state[symbol] = new_state
                    screened += 1
                    entries += len(month_trades)
                    logger.info(
                        "mtf_swing_v2 %s %s %s %s bars=%s trades=%s discards=%s",
                        tag,
                        tf,
                        label,
                        symbol,
                        len(bars),
                        len(month_trades),
                        len(discards),
                    )
                if screened >= min_screened and entries >= min_entries:
                    n_tr = append_trades_v2(trades_path_v2(), pending_trades)
                    n_ds = append_discards_v2(discards_path_v2(), pending_discards)
                    summary["trades_appended"] += n_tr
                    summary["discards_appended"] += n_ds
                    _save_json(state_path_v2(tf, tag), {"symbols": sym_state, "month": label})
                    done.add(token)
                    progress["completed"] = sorted(done)
                    progress["updated"] = run_ts
                    _save_json(progress_path_v2(), progress)
                    summary["completed_now"].append(token)
                    month_ok = True
                    logger.info(
                        "mtf_swing_v2 progress %s screened=%s entries=%s appended_trades=%s",
                        token,
                        screened,
                        entries,
                        n_tr,
                    )
                    break
                logger.error(
                    "mtf_swing_v2 %s incomplete screened=%s (need >=%s) entries=%s (need >=%s) attempt=%s",
                    token,
                    screened,
                    min_screened,
                    entries,
                    min_entries,
                    attempt,
                )
                if attempt < _MONTH_ATTEMPTS:
                    time.sleep(_MONTH_RETRY_COOLDOWN)
            if not month_ok:
                summary["failed"].append(token)
                logger.error("mtf_swing_v2 %s failed after %s attempts", token, _MONTH_ATTEMPTS)
    return summary


def run_backtest_v2(
    *,
    timeframes: Optional[Sequence[str]] = None,
    months: Optional[Sequence[str]] = None,
    symbols: Optional[Sequence[str]] = None,
    resume: bool = True,
    ablation: bool = False,
    upstox: Any = None,
) -> Dict[str, Any]:
    """Run the v2 experiment. Safe to resume; never rewrites v1 CSVs."""
    hol = known_holidays()
    universe = load_universe()
    if symbols:
        wanted = {s.strip().upper() for s in symbols if s and str(s).strip()}
        universe = [row for row in universe if row["symbol"] in wanted]
    if not universe:
        raise RuntimeError("arbitrage_master returned no symbols")
    client = upstox if upstox is not None else _upstox()
    tfs = _parse_timeframes(timeframes)
    month_labels = _parse_months(months)
    run_ts = datetime.now(IST).isoformat()
    variants = list(ABLATION_VARIANTS) if ablation else ["all"]

    prefetch_mode = None
    try:
        from backend.services.upstox_rate_limiter import set_backtest_bulk_prefetch_mode as prefetch_mode

        prefetch_mode(True)
    except Exception:
        prefetch_mode = None

    summaries: List[Dict[str, Any]] = []
    try:
        for name in variants:
            cfg = v2_config() if (name == "all" and not ablation) else ablation_config(name)
            logger.info("mtf_swing_v2 starting variant=%s months=%s tfs=%s", cfg.ablation_tag, month_labels, tfs)
            summaries.append(
                _run_one_variant(
                    cfg=cfg,
                    timeframes=tfs,
                    months=month_labels,
                    universe=universe,
                    client=client,
                    hol=hol,
                    resume=resume,
                    run_ts=run_ts,
                )
            )
    finally:
        if prefetch_mode is not None:
            try:
                prefetch_mode(False)
            except Exception:
                pass

    # Ablation / overall roll-up from the trades CSV.
    from backend.services.mtf_swing.csv_store import read_rows

    trades = read_rows(trades_path_v2())
    table = ablation_table(trades)
    summary_rows = []
    for row in table:
        summary_rows.append(
            {
                **{k: ("" if row.get(k) is None else row.get(k)) for k in row},
                "win_rate": "" if row.get("win_rate") is None else f"{row['win_rate']:.4f}",
                "profit_factor": ""
                if row.get("profit_factor") is None
                else ("inf" if row["profit_factor"] == float("inf") else f"{row['profit_factor']:.4f}"),
                "avg_pnl": "" if row.get("avg_pnl") is None else f"{row['avg_pnl']:.2f}",
                "net_pnl": f"{float(row.get('net_pnl') or 0):.2f}",
                "mtm_pnl_sum": f"{float(row.get('mtm_pnl_sum') or 0):.2f}",
                "run_ts": run_ts,
            }
        )
    write_variant_summary(variant_summary_path(), summary_rows)

    return {
        "ok": True,
        "variants": summaries,
        "trades_csv": str(trades_path_v2()),
        "discards_csv": str(discards_path_v2()),
        "regime_log": str(regime_log_path()),
        "variant_summary": str(variant_summary_path()),
        "data_dir": str(data_dir_v2()),
        "months": month_labels,
        "timeframes": list(tfs),
        "ablation": ablation,
    }
