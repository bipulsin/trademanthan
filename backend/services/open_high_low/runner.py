"""Orchestrate OpenHighLow backtest: load → signal → simulate → report."""
from __future__ import annotations

import logging
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from backend.services.open_high_low.config import ATR_PERIOD, OHLConfig
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
from backend.services.open_high_low.metrics import summarize
from backend.services.open_high_low.report import write_html, write_trades_csv
from backend.services.open_high_low.signals import detect_setup
from backend.services.open_high_low.simulate import simulate_trade

logger = logging.getLogger(__name__)


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def run_backtest(
    cfg: Optional[OHLConfig] = None,
    *,
    out_dir: Optional[Path] = None,
    copy_public: bool = True,
    copy_downloads: bool = True,
) -> Dict[str, Any]:
    cfg = cfg or OHLConfig()
    holidays = known_holidays()
    days = trading_days(cfg.start, cfg.end, holidays)
    out = Path(out_dir) if out_dir else data_dir()
    out.mkdir(parents=True, exist_ok=True)

    universe = load_universe(exclude=cfg.exclude_symbols)
    if cfg.symbols:
        want = {s.upper() for s in cfg.symbols}
        universe = [u for u in universe if u["symbol"] in want]
    if cfg.max_symbols:
        universe = universe[: int(cfg.max_symbols)]

    upstox = _upstox()
    trades: List[Dict[str, Any]] = []
    skips: List[Dict[str, Any]] = []

    # Precompute expiries needed across the window (+ prev day for first session).
    fetch_start = previous_trading_day(cfg.start, holidays) if days else cfg.start
    fetch_end = cfg.end

    expiries_needed: Set[date] = set()
    for d in days:
        expiries_needed.add(front_month_expiry(d, holidays))
        # Previous trading day may still be on prior month contract.
        prev = previous_trading_day(d, holidays)
        expiries_needed.add(front_month_expiry(prev, holidays))

    logger.info(
        "ohl backtest %s..%s days=%s symbols=%s expiries=%s",
        cfg.start,
        cfg.end,
        len(days),
        len(universe),
        sorted(e.isoformat() for e in expiries_needed),
    )

    for i, row in enumerate(universe, start=1):
        symbol = row["symbol"]
        underlying = row.get("underlying_key") or ""
        if i % 25 == 1 or i == len(universe):
            logger.info("ohl progress %s/%s %s", i, len(universe), symbol)

        # Resolve contracts once per expiry (expired API + live currmth/nextmth).
        contracts: Dict[date, Dict[str, Any]] = {}
        for exp in sorted(expiries_needed):
            resolved = resolve_contract(
                upstox, symbol, exp, underlying, universe_row=row
            )
            key = str(resolved.get("instrument_key") or "")
            if not key:
                continue
            lot = resolve_lot(resolved, cfg.lot_fallback)
            # Fetch 5m + daily covering prev-day through end for this contract.
            # Warm daily history for ATR(14) (~1 month calendar lookback).
            c_start_5m = fetch_start - timedelta(days=5)
            c_start_day = fetch_start - timedelta(days=45)
            c_end = fetch_end
            bars_5m = fetch_contract_bars(
                upstox, key, c_start_5m, c_end, "5m", use_cache=cfg.use_cache
            )
            bars_day = fetch_contract_bars(
                upstox, key, c_start_day, c_end, "day", use_cache=cfg.use_cache
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
                period=ATR_PERIOD,
                holidays=holidays,
            )
            trade = simulate_trade(
                setup,
                cinfo["bars_5m"],
                symbol=symbol,
                contract=cinfo["instrument_key"],
                expiry=exp,
                lot_size=int(cinfo["lot_size"]),
                cfg=cfg,
                atr=atr,
            )
            trades.append(trade)

    trades.sort(key=lambda t: (t.get("date") or "", t.get("symbol") or "", t.get("entry_time") or ""))
    summary = summarize(trades, skips)
    summary["stocks_scanned"] = len(universe)
    summary["trading_days"] = len(days)

    meta = {
        "start": cfg.start.isoformat(),
        "end": cfg.end.isoformat(),
        "square_off": cfg.square_off.strftime("%H:%M"),
        "brokerage_per_lot": cfg.brokerage_per_lot,
        "slippage_points": cfg.slippage_points,
        "lot_fallback": cfg.lot_fallback,
        "stocks_scanned": len(universe),
        "trading_days": len(days),
        "note": (
            "ATR-step trailing SL from daily ATR(14) as of prior session close; "
            "initial SL = first-candle extreme; lot size from Upstox when available."
        ),
        "sl_mode": "atr_step_trail",
        "atr_period": ATR_PERIOD,
    }

    csv_path = write_trades_csv(trades, out / "trades.csv")
    html_path = write_html(trades, summary, out / "report.html", meta=meta)

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
                # Also CSV beside it
                shutil.copy2(csv_path, candidate.with_name("open_high_low_trades.csv"))
                break
            except Exception:
                continue

    # Always mirror CSV next to HTML in data dir
    result = {
        "ok": True,
        "trades": len(trades),
        "skips": len(skips),
        "stocks_scanned": len(universe),
        "trading_days": len(days),
        "win_rate": summary.get("win_rate"),
        "net_pnl": summary.get("net_pnl"),
        "csv": str(csv_path),
        "html": str(html_path),
        "public": public_path,
        "downloads": downloads_path,
        "summary": summary,
        "meta": meta,
    }
    import json

    slim_out = {k: v for k, v in result.items() if k not in ("summary",)}
    slim_out["summary"] = {
        k: v
        for k, v in summary.items()
        if k not in ("equity", "skips")  # keep summary.json smaller
    }
    slim_out["skip_sample"] = skips[:50]
    (out / "summary.json").write_text(json.dumps(slim_out, indent=2, default=str), encoding="utf-8")
    return result
