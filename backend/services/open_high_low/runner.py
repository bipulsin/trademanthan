"""Orchestrate OpenHighLow backtest: load → signal → features → simulate → select → report."""
from __future__ import annotations

import json
import logging
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.services.open_high_low.config import (
    ATR_PERIOD,
    SL_MODES,
    OHLConfig,
)
from backend.services.open_high_low.data import (
    daily_atr_asof,
    daily_hl_for,
    data_dir,
    fetch_contract_bars,
    front_month_expiry,
    known_holidays,
    load_universe,
    previous_trading_day,
    resolve_contract,
    resolve_lot,
    trading_days,
)
from backend.services.open_high_low.features import (
    annotate_same_side_counts,
    build_entry_features,
)
from backend.services.open_high_low.metrics import summarize_study
from backend.services.open_high_low.report import (
    write_html,
    write_selection_summary,
    write_sl_path_dump,
    write_trades_csv,
)
from backend.services.open_high_low.selection import run_selection_study
from backend.services.open_high_low.signals import detect_setup
from backend.services.open_high_low.simulate import simulate_trade

logger = logging.getLogger(__name__)


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _months_in_range(start: date, end: date) -> List[Tuple[date, date]]:
    """Inclusive calendar-month windows clipped to [start, end]."""
    out: List[Tuple[date, date]] = []
    y, m = start.year, start.month
    while date(y, m, 1) <= end:
        if m == 12:
            next_first = date(y + 1, 1, 1)
        else:
            next_first = date(y, m + 1, 1)
        last = next_first - timedelta(days=1)
        m_start = max(start, date(y, m, 1))
        m_end = min(end, last)
        if m_start <= m_end:
            out.append((m_start, m_end))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _probe_earliest_feasible(
    upstox: Any,
    universe: List[Dict[str, str]],
    holidays: Set[date],
    *,
    requested_start: date,
    end: date,
    use_cache: bool,
) -> date:
    """Try to confirm data exists near ``requested_start``; walk forward by month if empty.

    Uses a liquid name (RELIANCE / first available) front-month contract.
    """
    probe_sym = None
    probe_row = None
    for prefer in ("RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"):
        for row in universe:
            if row["symbol"] == prefer:
                probe_sym, probe_row = prefer, row
                break
        if probe_sym:
            break
    if probe_row is None and universe:
        probe_row = universe[0]
        probe_sym = probe_row["symbol"]
    if probe_row is None:
        return requested_start

    cur = requested_start
    while cur <= end:
        # Probe first trading day of this month window
        days = trading_days(cur, min(end, cur + timedelta(days=10)), holidays)
        if not days:
            if cur.month == 12:
                cur = date(cur.year + 1, 1, 1)
            else:
                cur = date(cur.year, cur.month + 1, 1)
            continue
        session = days[0]
        exp = front_month_expiry(session, holidays)
        resolved = resolve_contract(
            upstox,
            probe_sym,
            exp,
            probe_row.get("underlying_key") or "",
            universe_row=probe_row,
        )
        key = str(resolved.get("instrument_key") or "")
        if not key:
            logger.info("ohl probe no contract %s %s — try next month", probe_sym, exp)
        else:
            bars = fetch_contract_bars(
                upstox,
                key,
                session - timedelta(days=3),
                session,
                "5m",
                use_cache=use_cache,
            )
            if bars:
                logger.info(
                    "ohl earliest feasible start=%s (probed %s on %s, bars=%s)",
                    cur.isoformat(),
                    probe_sym,
                    session.isoformat(),
                    len(bars),
                )
                return cur
            logger.info(
                "ohl probe empty bars %s %s — advance month", probe_sym, session
            )
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    return requested_start


def _fetch_nifty_bars(
    upstox: Any,
    start: date,
    end: date,
    *,
    nifty_key: str,
    use_cache: bool,
) -> List[Any]:
    # Warm a few sessions before start for robustness
    return fetch_contract_bars(
        upstox,
        nifty_key,
        start - timedelta(days=10),
        end,
        "5m",
        use_cache=use_cache,
    )


def _run_month(
    cfg: OHLConfig,
    month_start: date,
    month_end: date,
    *,
    universe: List[Dict[str, str]],
    holidays: Set[date],
    upstox: Any,
    nifty_bars: List[Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    days = trading_days(month_start, month_end, holidays)
    trades: List[Dict[str, Any]] = []
    skips: List[Dict[str, Any]] = []
    if not days:
        return trades, skips

    fetch_start = previous_trading_day(month_start, holidays)
    fetch_end = month_end

    expiries_needed: Set[date] = set()
    for d in days:
        expiries_needed.add(front_month_expiry(d, holidays))
        prev = previous_trading_day(d, holidays)
        expiries_needed.add(front_month_expiry(prev, holidays))

    # Collect setups first (for same_side_count), then simulate all modes.
    pending: List[Dict[str, Any]] = []

    for i, row in enumerate(universe, start=1):
        symbol = row["symbol"]
        underlying = row.get("underlying_key") or ""
        if i % 25 == 1 or i == len(universe):
            logger.info(
                "ohl month %s..%s progress %s/%s %s",
                month_start,
                month_end,
                i,
                len(universe),
                symbol,
            )

        contracts: Dict[date, Dict[str, Any]] = {}
        for exp in sorted(expiries_needed):
            resolved = resolve_contract(
                upstox, symbol, exp, underlying, universe_row=row
            )
            key = str(resolved.get("instrument_key") or "")
            if not key:
                continue
            lot = resolve_lot(resolved, cfg.lot_fallback)
            # Warm daily for ATR + 5m for EMA30/rvol(~20 sessions). Keep windows
            # close to prior cache shards to maximize reuse.
            c_start_5m = fetch_start - timedelta(days=40)
            c_start_day = fetch_start - timedelta(days=45)
            bars_5m = fetch_contract_bars(
                upstox, key, c_start_5m, fetch_end, "5m", use_cache=cfg.use_cache
            )
            bars_day = fetch_contract_bars(
                upstox, key, c_start_day, fetch_end, "day", use_cache=cfg.use_cache
            )
            contracts[exp] = {
                "instrument_key": key,
                "lot_size": lot,
                "bars_5m": bars_5m,
                "bars_day": bars_day,
                "expiry": exp,
            }

        for session in days:
            exp = front_month_expiry(session, holidays)
            cinfo = contracts.get(exp)
            if not cinfo:
                skips.append(
                    {
                        "date": session.isoformat(),
                        "symbol": symbol,
                        "reason": f"no contract resolved for expiry {exp.isoformat()}",
                    }
                )
                continue

            hl = daily_hl_for(cinfo["bars_day"], session, holidays)
            if hl is None:
                skips.append(
                    {
                        "date": session.isoformat(),
                        "symbol": symbol,
                        "reason": (
                            f"missing same-contract previous-day daily for "
                            f"{cinfo['instrument_key']} (expiry {exp.isoformat()})"
                        ),
                    }
                )
                continue

            prev_high, prev_low = hl
            setup = detect_setup(cinfo["bars_5m"], session, prev_high, prev_low)
            if setup is None:
                continue

            atr = daily_atr_asof(
                cinfo["bars_day"],
                session,
                period=int(cfg.atr_period or ATR_PERIOD),
                holidays=holidays,
            )
            feats = build_entry_features(
                setup,
                cinfo["bars_5m"],
                cinfo["bars_day"],
                atr=atr,
                nifty_bars=nifty_bars,
                holidays=holidays,
                ema_period=int(cfg.ema_period),
                rvol_lookback=int(cfg.rvol_lookback),
            )
            pending.append(
                {
                    "setup": setup,
                    "cinfo": cinfo,
                    "symbol": symbol,
                    "atr": atr,
                    "features": feats,
                    "date": session.isoformat(),
                    "side": setup.side,
                }
            )

    # same_side_count across the month's pending signals (by calendar day)
    feature_rows = [
        {"date": p["date"], "side": p["side"], **p["features"]} for p in pending
    ]
    annotate_same_side_counts(feature_rows)
    for p, fr in zip(pending, feature_rows):
        p["features"]["same_side_count_0925"] = fr.get("same_side_count_0925")

    modes = list(cfg.sl_modes) or list(SL_MODES)
    for p in pending:
        setup = p["setup"]
        cinfo = p["cinfo"]
        for mode in modes:
            trade = simulate_trade(
                setup,
                cinfo["bars_5m"],
                symbol=p["symbol"],
                contract=cinfo["instrument_key"],
                expiry=cinfo["expiry"],
                lot_size=int(cinfo["lot_size"]),
                cfg=cfg,
                atr=p["atr"],
                sl_mode=mode,
                features=p["features"],
            )
            trades.append(trade)

    return trades, skips


def _pick_sl_path_samples(
    cfg: OHLConfig,
    universe: List[Dict[str, str]],
    holidays: Set[date],
    upstox: Any,
    days: List[date],
    *,
    n: int = 5,
) -> List[Dict[str, Any]]:
    """Re-simulate candle_trail with record_sl_path until ``n`` trails that moved."""
    samples: List[Dict[str, Any]] = []
    if not days or n <= 0:
        return samples

    # Walk a subset of liquid symbols / recent days for proof artifacts.
    prefer = ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN", "ICICIBANK", "BHARTIARTL"]
    ordered_univ = sorted(
        universe,
        key=lambda r: (0 if r["symbol"] in prefer else 1, r["symbol"]),
    )[:40]

    for session in reversed(days):
        if len(samples) >= n:
            break
        exp = front_month_expiry(session, holidays)
        for row in ordered_univ:
            if len(samples) >= n:
                break
            symbol = row["symbol"]
            resolved = resolve_contract(
                upstox,
                symbol,
                exp,
                row.get("underlying_key") or "",
                universe_row=row,
            )
            key = str(resolved.get("instrument_key") or "")
            if not key:
                continue
            bars_5m = fetch_contract_bars(
                upstox,
                key,
                session - timedelta(days=5),
                session,
                "5m",
                use_cache=cfg.use_cache,
            )
            bars_day = fetch_contract_bars(
                upstox,
                key,
                session - timedelta(days=45),
                session,
                "day",
                use_cache=cfg.use_cache,
            )
            hl = daily_hl_for(bars_day, session, holidays)
            if hl is None:
                continue
            setup = detect_setup(bars_5m, session, hl[0], hl[1])
            if setup is None:
                continue
            atr = daily_atr_asof(
                bars_day, session, period=int(cfg.atr_period or ATR_PERIOD), holidays=holidays
            )
            trade = simulate_trade(
                setup,
                bars_5m,
                symbol=symbol,
                contract=key,
                expiry=exp,
                lot_size=resolve_lot(resolved, cfg.lot_fallback),
                cfg=cfg,
                atr=atr,
                sl_mode="candle_trail",
                record_sl_path=True,
            )
            path = trade.get("sl_path") or []
            moved = any(
                abs(float(p.get("sl_after", 0)) - float(p.get("sl_before", 0))) > 1e-9
                for p in path
            )
            if not moved and abs(float(trade["final_sl"]) - float(trade["initial_sl"])) < 1e-9:
                continue
            samples.append(
                {
                    "date": trade["date"],
                    "symbol": trade["symbol"],
                    "side": trade["side"],
                    "entry_price": trade["entry_price"],
                    "initial_sl": trade["initial_sl"],
                    "final_sl": trade["final_sl"],
                    "exit_reason": trade["exit_reason"],
                    "exit_price": trade["exit_price"],
                    "sl_path": path,
                }
            )
            logger.info(
                "ohl sl-path sample %s %s initial=%.2f final=%.2f steps=%s",
                trade["date"],
                trade["symbol"],
                trade["initial_sl"],
                trade["final_sl"],
                len(path),
            )
    return samples


def run_backtest(
    cfg: Optional[OHLConfig] = None,
    *,
    out_dir: Optional[Path] = None,
    copy_public: bool = True,
    copy_downloads: bool = True,
) -> Dict[str, Any]:
    cfg = cfg or OHLConfig()
    holidays = known_holidays()
    out = Path(out_dir) if out_dir else data_dir()
    out.mkdir(parents=True, exist_ok=True)
    month_cache = out / "month_cache"
    month_cache.mkdir(parents=True, exist_ok=True)

    universe = load_universe(exclude=cfg.exclude_symbols)
    if cfg.symbols:
        want = {s.upper() for s in cfg.symbols}
        universe = [u for u in universe if u["symbol"] in want]
    if cfg.max_symbols:
        universe = universe[: int(cfg.max_symbols)]

    upstox = _upstox()

    # Probe earliest feasible start within requested window.
    actual_start = _probe_earliest_feasible(
        upstox,
        universe,
        holidays,
        requested_start=cfg.start,
        end=cfg.end,
        use_cache=cfg.use_cache,
    )
    if actual_start > cfg.start:
        logger.info(
            "ohl adjusting start %s → %s (Upstox history)",
            cfg.start,
            actual_start,
        )

    days = trading_days(actual_start, cfg.end, holidays)
    modes = list(cfg.sl_modes) or list(SL_MODES)

    logger.info(
        "ohl backtest %s..%s days=%s symbols=%s modes=%s",
        actual_start,
        cfg.end,
        len(days),
        len(universe),
        modes,
    )

    nifty_bars = _fetch_nifty_bars(
        upstox,
        actual_start,
        cfg.end,
        nifty_key=str(cfg.nifty_key),
        use_cache=cfg.use_cache,
    )
    logger.info("ohl nifty 5m bars=%s key=%s", len(nifty_bars), cfg.nifty_key)

    all_trades: List[Dict[str, Any]] = []
    all_skips: List[Dict[str, Any]] = []

    for m_start, m_end in _months_in_range(actual_start, cfg.end):
        mk = _month_key(m_start)
        cache_file = month_cache / f"{mk}.json"
        if cfg.resume_months and cache_file.is_file():
            try:
                payload = json.loads(cache_file.read_text(encoding="utf-8"))
                cached_trades = payload.get("trades") or []
                cached_skips = payload.get("skips") or []
                # Invalidate if SL mode set changed
                if set(payload.get("sl_modes") or []) == set(modes) and cached_trades:
                    logger.info(
                        "ohl resume month %s trades=%s", mk, len(cached_trades)
                    )
                    all_trades.extend(cached_trades)
                    all_skips.extend(cached_skips)
                    continue
            except Exception as exc:
                logger.info("ohl month cache read fail %s: %s", mk, exc)

        m_trades, m_skips = _run_month(
            cfg,
            m_start,
            m_end,
            universe=universe,
            holidays=holidays,
            upstox=upstox,
            nifty_bars=nifty_bars,
        )
        try:
            cache_file.write_text(
                json.dumps(
                    {"sl_modes": list(modes), "trades": m_trades, "skips": m_skips},
                    default=str,
                ),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.info("ohl month cache write fail %s: %s", mk, exc)
        all_trades.extend(m_trades)
        all_skips.extend(m_skips)
        logger.info(
            "ohl month done %s trades=%s skips=%s", mk, len(m_trades), len(m_skips)
        )

    all_trades.sort(
        key=lambda t: (
            t.get("date") or "",
            t.get("symbol") or "",
            t.get("sl_mode") or "",
            t.get("entry_time") or "",
        )
    )

    selection = run_selection_study(
        all_trades,
        sl_modes=modes,
        limit_pct=float(cfg.move_filter_pct),
        top_n=int(cfg.selection_top_n),
        n_samples=int(cfg.random_baseline_samples),
        pnl_key="rupee_pnl",
        seed=42,
    )

    summary = summarize_study(
        all_trades, all_skips, selection_study=selection, sl_modes=modes
    )
    summary["stocks_scanned"] = len(universe)
    summary["trading_days"] = len(days)
    summary["actual_start"] = actual_start.isoformat()
    summary["requested_start"] = cfg.start.isoformat()

    # SL path proof for candle_trail
    sl_samples = _pick_sl_path_samples(
        cfg, universe, holidays, upstox, days, n=int(cfg.sl_path_samples)
    )
    sl_path_file = write_sl_path_dump(sl_samples, out / "sl_path_samples.json")

    meta = {
        "start": actual_start.isoformat(),
        "requested_start": cfg.start.isoformat(),
        "end": cfg.end.isoformat(),
        "square_off": cfg.square_off.strftime("%H:%M"),
        "brokerage_per_lot": cfg.brokerage_per_lot,
        "slippage_points": cfg.slippage_points,
        "lot_fallback": cfg.lot_fallback,
        "stocks_scanned": len(universe),
        "trading_days": len(days),
        "sl_modes": list(modes),
        "atr_period": int(cfg.atr_period or ATR_PERIOD),
        "atr_trail_mult": float(cfg.atr_trail_mult),
        "risk_rupees": float(cfg.risk_rupees),
        "move_filter_pct": float(cfg.move_filter_pct),
        "selection_top_n": int(cfg.selection_top_n),
        "random_baseline_samples": int(cfg.random_baseline_samples),
        "note": (
            "Three SL modes (fixed / candle_trail / atr_trail); "
            "entry rules unchanged; selection study top-2 bull+bear after "
            f"signed move filter ≤{cfg.move_filter_pct}%; "
            f"fixed-risk sizing ₹{cfg.risk_rupees:.0f}/trade; "
            "lot size from Upstox when available."
        ),
        "sl_path_samples": len(sl_samples),
    }

    csv_path = write_trades_csv(all_trades, out / "trades.csv")
    sel_path = write_selection_summary(
        selection, summary, out / "selection_summary.json", meta=meta
    )
    html_path = write_html(all_trades, summary, out / "report.html", meta=meta)

    public_path = None
    downloads_path = None
    if copy_public:
        try:
            from backend.services.open_high_low.data import repo_root

            pub = repo_root() / "frontend" / "public" / "open-high-low.html"
            pub.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(html_path, pub)
            public_path = str(pub)
        except Exception as exc:
            logger.info("ohl copy public failed: %s", exc)

    if copy_downloads:
        for candidate in (
            Path.home() / "Downloads" / "open-high-low.html",
            Path("/Users/bipulsahay/Downloads/open-high-low.html"),
        ):
            try:
                candidate.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(html_path, candidate)
                downloads_path = str(candidate)
                shutil.copy2(csv_path, candidate.with_name("open_high_low_trades.csv"))
                shutil.copy2(
                    sl_path_file, candidate.with_name("open_high_low_sl_path_samples.json")
                )
                shutil.copy2(
                    sel_path, candidate.with_name("open_high_low_selection_summary.json")
                )
                break
            except Exception:
                continue

    result = {
        "ok": True,
        "trades": len(all_trades),
        "signals": len({(t.get("date"), t.get("symbol")) for t in all_trades}),
        "skips": len(all_skips),
        "stocks_scanned": len(universe),
        "trading_days": len(days),
        "actual_start": actual_start.isoformat(),
        "end": cfg.end.isoformat(),
        "win_rate": summary.get("win_rate"),
        "net_pnl": summary.get("net_pnl"),
        "csv": str(csv_path),
        "html": str(html_path),
        "selection_summary": str(sel_path),
        "sl_path_samples": str(sl_path_file),
        "public": public_path,
        "downloads": downloads_path,
        "summary": summary,
        "meta": meta,
        "sl_path_proof": [
            {
                "date": s["date"],
                "symbol": s["symbol"],
                "side": s["side"],
                "initial_sl": s["initial_sl"],
                "final_sl": s["final_sl"],
                "moved": abs(float(s["final_sl"]) - float(s["initial_sl"])) > 1e-9,
                "path_len": len(s.get("sl_path") or []),
            }
            for s in sl_samples
        ],
    }

    slim_out = {k: v for k, v in result.items() if k not in ("summary",)}
    # Compact summary for JSON (drop bulky equity curves per mode)
    compact_summary = {
        k: v
        for k, v in summary.items()
        if k not in ("equity", "skips", "daily_pnl")
    }
    if "by_sl_mode" in compact_summary:
        compact_modes = {}
        for mode, block in (compact_summary.get("by_sl_mode") or {}).items():
            compact_modes[mode] = _compact_mode_block(block)
        compact_summary["by_sl_mode"] = compact_modes
    slim_out["summary"] = compact_summary
    slim_out["skip_sample"] = all_skips[:50]
    (out / "summary.json").write_text(
        json.dumps(slim_out, indent=2, default=str), encoding="utf-8"
    )
    return result


def _compact_mode_block(block: Dict[str, Any]) -> Dict[str, Any]:
    def slim_metrics(m: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not m:
            return m
        keep = (
            "trades",
            "wins",
            "win_rate",
            "net_pnl",
            "profit_factor",
            "avg_r",
            "max_drawdown",
            "trades_per_day",
            "by_side",
            "by_exit",
            "by_month",
        )
        return {k: m.get(k) for k in keep}

    out: Dict[str, Any] = {
        "baseline_one_lot": slim_metrics(block.get("baseline_one_lot")),
        "baseline_fixed_risk": slim_metrics(block.get("baseline_fixed_risk")),
        "filtered_one_lot": slim_metrics(block.get("filtered_one_lot")),
        "rules": {},
    }
    for rule, rb in (block.get("rules") or {}).items():
        out["rules"][rule] = {
            "one_lot": slim_metrics(rb.get("one_lot")),
            "fixed_risk": slim_metrics(rb.get("fixed_risk")),
            "by_side": {
                s: slim_metrics(v) for s, v in (rb.get("by_side") or {}).items()
            },
        }
    return out
