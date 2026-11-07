"""MTF Swing v2 experiment runner.

Month-by-month forward with continuous state carry-over (open trades, pending /
limits, watchlist, regime) — equivalent to one continuous run. Writes only under
data/mtf_swing_v2/ and never touches v1 trades.csv.

    python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \\
        --timeframes 60m,15m --variants v1_equivalent
    python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \\
        --timeframes 60m,15m --variants all
    python3 scripts/run_mtf_swing_backtest.py --v2 --ablation --leave-one-out

Pilot June/Oct flat ``all`` rows used trade_id suffix ``|all``. Full-period
continuous runs use the same suffix for the ``all`` variant; idempotent append
skips duplicate keys. Before a full-period ``all`` run, archive the pilot CSV
(see README) so stale flat-start rows do not mix with continuous results.
Progress tokens are ``v2:cont:{variant}:{tf}:{YYYY-MM}`` so legacy flat pilot
tokens (``v2:all:…``) do not mark continuous months complete.
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
    TIMEFRAMES,
    V2_FULL_END,
    V2_FULL_START,
    V2_MONTHS,
    ablation_config,
    month_window_for,
    months_between,
    resolve_variants,
)
from backend.services.mtf_swing.csv_store import (
    append_discards_v2,
    append_regime_log,
    append_rows,
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
from backend.services.mtf_swing.parity import filter_bars_for_holes, is_hole, load_holes
from backend.services.mtf_swing.regime import build_regime_series, regime_by_screen_date, regime_log_rows
from backend.services.mtf_swing.config import COVERAGE_LOG_FIELDS

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_FULL_UNIVERSE = 20
_MIN_SCREENED_RATIO = 0.70
# Feature-filtered months can be thin; v1_equivalent uses v1's higher bar.
_MIN_ENTRIES_DEFAULT = 1
_MIN_ENTRIES_V1_EQUIV = 10
_MONTH_ATTEMPTS = 3
_EMPTY_STREAK_COOLDOWN = 45.0
_MONTH_RETRY_COOLDOWN = 90.0
# Follow trades past the final month end through latest available data.
_FORWARD_TAIL_DAYS = 120
# v1_equivalent must match v1's last-batch tail (~3 weeks) for OPEN parity.
_V1_EQUIV_TAIL_DAYS = 20
# ≥20 weekly index closes before 1-Nov-2025 (~154 calendar days + buffer).
_REGIME_WARMUP_DAYS = 160


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


def _month_thresholds(universe_size: int, variant: str) -> tuple[int, int]:
    min_entries = _MIN_ENTRIES_V1_EQUIV if variant == "v1_equivalent" else _MIN_ENTRIES_DEFAULT
    if universe_size >= _FULL_UNIVERSE:
        return max(1, int(_MIN_SCREENED_RATIO * universe_size)), min_entries
    return max(1, universe_size), 0


def _parse_months(
    raw: Optional[Sequence[str]] = None,
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[str]:
    if start or end:
        return months_between(start or V2_FULL_START, end or V2_FULL_END)
    if not raw:
        return months_between(V2_FULL_START, V2_FULL_END)
    out: List[str] = []
    for item in raw:
        for part in str(item).split(","):
            lab = part.strip()
            if not lab:
                continue
            month_window_for(lab)  # validate
            out.append(lab)
    return out or months_between(V2_FULL_START, V2_FULL_END)


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


def coverage_log_path():
    return data_dir_v2() / "coverage_log.csv"


def _append_coverage(rows: List[Dict[str, Any]]) -> int:
    return append_rows(coverage_log_path(), COVERAGE_LOG_FIELDS, rows, "coverage_id")


def _progress_token(tag: str, tf: str, label: str) -> str:
    return f"v2:cont:{tag}:{tf}:{label}"


def _build_regime_map(
    upstox: Any,
    cfg,
    fetch_start: date,
    fetch_end: date,
    run_ts: str,
    *,
    force_warmup_from: Optional[date] = None,
) -> Dict[str, str]:
    if not cfg.regime_filter:
        return {}
    regime_start = (force_warmup_from or fetch_start) - timedelta(days=_REGIME_WARMUP_DAYS)
    # Always warm from well before the first backtest screen when covering Nov'25+.
    floor = date(2025, 5, 1)
    if regime_start > floor and (force_warmup_from or fetch_start) <= date(2025, 11, 30):
        regime_start = floor
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


def _bar_ts(bar: Any) -> str:
    ts = getattr(bar, "ts", None)
    if ts is None:
        return ""
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


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
    tag = cfg.ablation_tag or "all"
    summary: Dict[str, Any] = {
        "variant": tag,
        "completed_now": [],
        "skipped": [],
        "trades_appended": 0,
        "discards_appended": 0,
        "failed": [],
        "last_bars": {},
    }
    min_screened, min_entries = _month_thresholds(len(universe), tag)
    month_labels = list(months)
    today = datetime.now(IST).date()
    # Replay v1's historical fetch holes so v1_equivalent matches the frozen v1 CSV.
    parity_holes = load_holes() if tag == "v1_equivalent" else set()
    if parity_holes:
        logger.info(
            "mtf_swing_v2 %s applying %s v1 fetch-hole keys for candle-gap parity",
            tag,
            len(parity_holes),
        )

    for tf in timeframes:
        # Continuous carry: one state map per timeframe × variant across months.
        state_blob = _load_json(state_path_v2(tf, tag), {"symbols": {}})
        sym_state: Dict[str, Any] = state_blob.get("symbols") or {}
        for idx, label in enumerate(month_labels):
            m_start, m_end, _ = month_window_for(label)
            token = _progress_token(tag, tf, label)
            if resume and token in done:
                summary["skipped"].append(token)
                logger.info("mtf_swing_v2 skip %s", token)
                continue
            is_last = idx == len(month_labels) - 1
            if is_last:
                tail = _V1_EQUIV_TAIL_DAYS if tag == "v1_equivalent" else _FORWARD_TAIL_DAYS
            else:
                tail = 0
            fetch_start, fetch_end = window_for_month(
                m_start,
                m_end,
                tail_days=tail,
            )
            if fetch_end > today:
                fetch_end = today
                month_ok = False
            # With v1 fetch-hole replay, many symbols are intentionally skipped;
            # thresholds apply to the remaining (non-hole) universe for this month.
            if parity_holes:
                eligible_n = sum(
                    1 for row in universe if not is_hole(parity_holes, tf, row["symbol"], label)
                )
                month_min_screened, month_min_entries = _month_thresholds(eligible_n, tag)
            else:
                eligible_n = len(universe)
                month_min_screened, month_min_entries = min_screened, min_entries
            for attempt in range(1, _MONTH_ATTEMPTS + 1):
                snap = copy.deepcopy(sym_state)
                logger.info(
                    "mtf_swing_v2 %s %s %s symbols=%s eligible=%s window=%s..%s attempt=%s/%s "
                    "carry=%s finalize_open=%s min_screened=%s",
                    tag,
                    tf,
                    label,
                    len(universe),
                    eligible_n,
                    fetch_start,
                    fetch_end,
                    attempt,
                    _MONTH_ATTEMPTS,
                    bool(sym_state),
                    is_last,
                    month_min_screened,
                )
                regime_map = _build_regime_map(
                    client,
                    cfg,
                    fetch_start,
                    fetch_end,
                    run_ts,
                    force_warmup_from=date(2025, 11, 1) if label <= "2025-11" else None,
                )
                screened = entries = 0
                empty_streak = 0
                pending_trades: List[Dict[str, Any]] = []
                pending_discards: List[Dict[str, Any]] = []
                coverage_rows: List[Dict[str, Any]] = []
                month_last_ts = ""
                for row in universe:
                    symbol = row["symbol"]
                    # Month marked as a v1 fetch hole → skip entirely (state carries).
                    if parity_holes and is_hole(parity_holes, tf, symbol, label):
                        logger.info(
                            "mtf_swing_v2 %s %s %s %s skip v1_fetch_hole",
                            tag,
                            tf,
                            label,
                            symbol,
                        )
                        continue
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
                    if parity_holes and bars:
                        before = len(bars)
                        bars = filter_bars_for_holes(bars, parity_holes, timeframe=tf, symbol=symbol)
                        if len(bars) != before:
                            logger.info(
                                "mtf_swing_v2 %s %s %s %s stripped_hole_bars %s→%s",
                                tag,
                                tf,
                                label,
                                symbol,
                                before,
                                len(bars),
                            )
                    if not bars:
                        continue
                    time.sleep(0.4)
                    last_ts = _bar_ts(bars[-1])
                    if last_ts > month_last_ts:
                        month_last_ts = last_ts
                    coverage_rows.append(
                        {
                            "coverage_id": f"{tag}|{tf}|{label}|{symbol}",
                            "variant": tag,
                            "timeframe": tf,
                            "month": label,
                            "symbol": symbol,
                            "last_bar_ts": last_ts,
                            "bars": str(len(bars)),
                            "run_ts": run_ts,
                        }
                    )
                    lot = lot_size_for(symbol, bars[-1].contract) or next(iter(lots.values()), 0)
                    month_cfg = copy.copy(cfg)
                    month_cfg.timeframe = tf
                    trades, discards, new_state = run_symbol(
                        bars,
                        symbol=symbol,
                        holidays=hol,
                        active_from=m_start,
                        active_to=m_end,
                        state=sym_state.get(symbol),
                        config=month_cfg,
                        lot_size=lot,
                        lot_by_contract=lots,
                        run_ts=run_ts,
                        # Only finalize on the last month; earlier months carry
                        # open/pending state forward (continuous run).
                        finalize_open=is_last,
                        exit_only_after=m_end,
                        regime_map=regime_map,
                    )
                    # Like v1: append every emitted trade/discards. Cross-month
                    # exits keep their entry month; trade_id idempotency dedupes.
                    pending_trades.extend(trades)
                    pending_discards.extend(discards)
                    sym_state[symbol] = new_state
                    screened += 1
                    entries += len(trades)
                    logger.info(
                        "mtf_swing_v2 %s %s %s %s bars=%s last_bar=%s trades=%s discards=%s",
                        tag,
                        tf,
                        label,
                        symbol,
                        len(bars),
                        last_ts,
                        len(trades),
                        len(discards),
                    )
                summary["last_bars"][f"{tag}:{tf}:{label}"] = month_last_ts
                if month_last_ts:
                    logger.info(
                        "mtf_swing_v2 coverage %s %s %s last_available_bar=%s "
                        "(feed may end before month calendar end — not a silent stop)",
                        tag,
                        tf,
                        label,
                        month_last_ts,
                    )
                if screened >= month_min_screened and entries >= month_min_entries:
                    n_tr = append_trades_v2(trades_path_v2(), pending_trades)
                    n_ds = append_discards_v2(discards_path_v2(), pending_discards)
                    _append_coverage(coverage_rows)
                    summary["trades_appended"] += n_tr
                    summary["discards_appended"] += n_ds
                    state_blob["symbols"] = sym_state
                    state_blob["month"] = label
                    state_blob["last_bar"] = month_last_ts
                    _save_json(state_path_v2(tf, tag), state_blob)
                    done.add(token)
                    progress["completed"] = sorted(done)
                    progress["updated"] = run_ts
                    progress["mode"] = "continuous"
                    _save_json(progress_path_v2(), progress)
                    summary["completed_now"].append(token)
                    month_ok = True
                    logger.info(
                        "mtf_swing_v2 progress %s screened=%s entries=%s appended_trades=%s last_bar=%s",
                        token,
                        screened,
                        entries,
                        n_tr,
                        month_last_ts,
                    )
                    break
                logger.error(
                    "mtf_swing_v2 %s incomplete screened=%s (need >=%s) entries=%s (need >=%s) attempt=%s",
                    token,
                    screened,
                    month_min_screened,
                    entries,
                    month_min_entries,
                    attempt,
                )
                sym_state = snap
                if attempt < _MONTH_ATTEMPTS:
                    time.sleep(_MONTH_RETRY_COOLDOWN)
            if not month_ok:
                summary["failed"].append(token)
                logger.error("mtf_swing_v2 %s failed after %s attempts; stopping further %s months", token, _MONTH_ATTEMPTS, tf)
                break
    return summary


def run_backtest_v2(
    *,
    timeframes: Optional[Sequence[str]] = None,
    months: Optional[Sequence[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    symbols: Optional[Sequence[str]] = None,
    resume: bool = True,
    ablation: bool = False,
    variants: Optional[Sequence[str]] = None,
    leave_one_out: bool = False,
    upstox: Any = None,
) -> Dict[str, Any]:
    """Run the v2 experiment with continuous state. Safe to resume; never rewrites v1 CSVs."""
    hol = known_holidays()
    universe = load_universe()
    if symbols:
        wanted = {s.strip().upper() for s in symbols if s and str(s).strip()}
        universe = [row for row in universe if row["symbol"] in wanted]
    if not universe:
        raise RuntimeError("arbitrage_master returned no symbols")
    client = upstox if upstox is not None else _upstox()
    tfs = _parse_timeframes(timeframes)
    month_labels = _parse_months(months, start=start, end=end)
    run_ts = datetime.now(IST).isoformat()
    variant_names = resolve_variants(variants, ablation=ablation, leave_one_out=leave_one_out)

    prefetch_mode = None
    try:
        from backend.services.upstox_rate_limiter import set_backtest_bulk_prefetch_mode as prefetch_mode

        prefetch_mode(True)
    except Exception:
        prefetch_mode = None

    summaries: List[Dict[str, Any]] = []
    try:
        for name in variant_names:
            cfg = ablation_config(name)
            logger.info(
                "mtf_swing_v2 starting variant=%s months=%s..%s (%s) tfs=%s continuous=1",
                cfg.ablation_tag,
                month_labels[0] if month_labels else "",
                month_labels[-1] if month_labels else "",
                len(month_labels),
                tfs,
            )
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
                "avg_r": "" if row.get("avg_r") is None else f"{row['avg_r']:.4f}",
                "net_pnl": f"{float(row.get('net_pnl') or 0):.2f}",
                "mtm_pnl_sum": f"{float(row.get('mtm_pnl_sum') or 0):.2f}",
                "run_ts": run_ts,
            }
        )
    write_variant_summary(variant_summary_path(), summary_rows)

    return {
        "ok": True,
        "variants": summaries,
        "variant_names": variant_names,
        "trades_csv": str(trades_path_v2()),
        "discards_csv": str(discards_path_v2()),
        "regime_log": str(regime_log_path()),
        "coverage_log": str(coverage_log_path()),
        "variant_summary": str(variant_summary_path()),
        "data_dir": str(data_dir_v2()),
        "months": month_labels,
        "timeframes": list(tfs),
        "ablation": ablation or (len(variant_names) > 1),
        "leave_one_out": leave_one_out,
        "mode": "continuous",
        "pilot_months_legacy": list(V2_MONTHS),
    }
