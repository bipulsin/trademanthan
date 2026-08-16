"""Rocket CLI — fetch historical data and run event-driven backtests."""

from __future__ import annotations

import logging
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.logging import RichHandler

from rocket.analytics.tearsheet import export_html, print_tearsheet
from rocket.config.settings import PROJECT_ROOT, get_settings
from rocket.engine.backtester import RocketBacktester

app = typer.Typer(add_completion=False, no_args_is_help=True, help="Rocket — ML Institutional Futures backtester")
console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _mirror_html(path: Path) -> None:
    public = PROJECT_ROOT / "frontend" / "public" / "rocket.html"
    try:
        public.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, public)
        console.print(f"[green]Wrote[/green] {path} and {public}")
    except Exception:
        console.print(f"[green]Wrote[/green] {path}")


@app.command("fetch-data")
def fetch_data(
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    interval: str = typer.Option("5minute", "--interval"),
    limit: int = typer.Option(200, "--limit", help="Max symbols from arbitrage_master"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Pull Upstox candles for the active futures universe into Parquet cache."""
    _setup_logging(verbose)
    bt = RocketBacktester(interval=interval, max_symbols=limit)
    n = len(bt.load_universe())
    console.print(f"Universe: {n} current-month futures")
    series = bt.fetch_data(_parse_date(start_date), _parse_date(end_date))
    console.print(f"Cached/fetched series for {len(series)} symbols → {get_settings().rocket_cache_dir}")


@app.command("backtest")
def backtest(
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    interval: str = typer.Option("5minute", "--interval"),
    capital: float = typer.Option(10_000_000.0, "--capital"),
    limit: int = typer.Option(200, "--limit"),
    output: Optional[Path] = typer.Option(None, "--output", help="HTML tear sheet path"),
    skip_fetch: bool = typer.Option(False, "--skip-fetch", help="Use cache when present; still fetches missing"),
    meta_filter: bool = typer.Option(
        True,
        "--meta-filter/--no-meta-filter",
        help="Run ML meta-filter (breadth+CLV+HTF+RVOL, 0–3/day, P≥0.34)",
    ),
    min_prob: float = typer.Option(0.34, "--min-prob", help="Continuous-bulk floor (spikes >0.85 discarded)"),
    max_per_day: int = typer.Option(3, "--max-per-day", help="Max selected trades per day"),
    min_per_day: int = typer.Option(0, "--min-per-day", help="Min trades/day (0 = allow empty days)"),
    kelly_factor: float = typer.Option(0.35, "--kelly-factor", help="Fractional Kelly multiplier"),
    clv_threshold: float = typer.Option(0.20, "--clv-threshold", help="|CLV| minimum for directional close"),
    breadth_long_min: float = typer.Option(0.50, "--breadth-long-min", help="Min universe breadth for BUY"),
    breadth_short_max: float = typer.Option(0.50, "--breadth-short-max", help="Max universe breadth for SELL"),
    rvol_min: float = typer.Option(1.15, "--rvol-min", help="Minimum relative volume"),
    time_exit_bars: Optional[int] = typer.Option(
        4, "--time-exit-bars", help="Stagnation exit bar count (None/0 disables)"
    ),
    time_exit_atr_min: float = typer.Option(0.5, "--time-exit-atr-min"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run Rocket backtest and write rocket.html tear sheet."""
    _setup_logging(verbose)
    settings = get_settings()
    out = Path(output) if output else settings.rocket_output_html
    te_bars = int(time_exit_bars) if time_exit_bars and int(time_exit_bars) > 0 else None
    bt = RocketBacktester(
        interval=interval,
        capital=capital,
        max_symbols=limit,
        time_exit_bars=te_bars,
        time_exit_atr_min=time_exit_atr_min,
    )
    bt.load_universe()
    _ = skip_fetch
    bt.fetch_data(_parse_date(start_date), _parse_date(end_date))

    if meta_filter:
        from rocket.ml.pipeline import print_comparison, run_comparative_meta_backtest
        from rocket.ml.trade_selector import ConfluenceGatesConfig

        gates = ConfluenceGatesConfig(
            p_min=float(min_prob),
            clv_threshold=float(clv_threshold),
            breadth_long_min=float(breadth_long_min),
            breadth_short_max=float(breadth_short_max),
            rvol_min=float(rvol_min),
        )
        result = run_comparative_meta_backtest(
            bt,
            _parse_date(start_date),
            _parse_date(end_date),
            min_probability=min_prob,
            min_per_day=min_per_day,
            max_per_day=max_per_day,
            kelly_factor=kelly_factor,
            gates=gates,
        )
        metrics = dict(result["filtered"])
        metrics["comparison"] = result["comparison"]
        metrics["baseline"] = result["baseline"]
        metrics["raw_signal_count"] = result.get("raw_signal_count")
        metrics["selected_count"] = result.get("selected_count")
        metrics["meta_metrics"] = result.get("meta_metrics")
        metrics["confluence_gates"] = gates.as_dict()
        print_tearsheet(metrics, console)
        print_comparison(result["comparison"], console)
        title = (
            f"ML Meta + Kelly · {start_date} → {end_date} · {interval} · "
            f"{metrics.get('universe_size', 0)} symbols"
        )
    else:
        metrics = bt.run(_parse_date(start_date), _parse_date(end_date))
        print_tearsheet(metrics, console)
        title = (
            f"ML Institutional Futures · {start_date} → {end_date} · {interval} · "
            f"{metrics.get('universe_size', 0)} symbols"
        )

    path = export_html(metrics, out, title=title)
    _mirror_html(path)


@app.command("compare-timeframes")
def compare_timeframes(
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    intervals: str = typer.Option(
        "5minute,15minute",
        "--intervals",
        help="Comma-separated intervals (e.g. 5minute,15minute)",
    ),
    capital: float = typer.Option(10_000_000.0, "--capital"),
    limit: int = typer.Option(200, "--limit"),
    output: Optional[Path] = typer.Option(None, "--output", help="HTML tear sheet path"),
    min_prob: float = typer.Option(0.34, "--min-prob"),
    max_per_day: int = typer.Option(3, "--max-per-day"),
    min_per_day: int = typer.Option(0, "--min-per-day"),
    kelly_factor: float = typer.Option(0.35, "--kelly-factor"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Comparative meta+Kelly backtest across candle intervals; writes rocket.html."""
    _setup_logging(verbose)
    from rocket.ml.pipeline import print_timeframe_comparison, run_timeframe_comparison

    ivs: List[str] = [x.strip() for x in intervals.split(",") if x.strip()]
    if not ivs:
        raise typer.BadParameter("Provide at least one interval")

    settings = get_settings()
    out = Path(output) if output else settings.rocket_output_html
    result = run_timeframe_comparison(
        start=_parse_date(start_date),
        end=_parse_date(end_date),
        intervals=ivs,
        capital=capital,
        limit=limit,
        min_probability=min_prob,
        kelly_factor=kelly_factor,
        min_per_day=min_per_day,
        max_per_day=max_per_day,
    )
    metrics = result["primary_metrics"]
    print_tearsheet(metrics, console)
    print_timeframe_comparison(result["timeframe_comparison"], ivs, console)
    title = (
        f"ML Meta + Kelly · {start_date} → {end_date} · "
        f"{'+'.join(ivs)} · {metrics.get('universe_size', 0)} symbols"
    )
    path = export_html(metrics, out, title=title)
    _mirror_html(path)


@app.command("compare-time-exits")
def compare_time_exits(
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    interval: str = typer.Option("5minute", "--interval"),
    bars: str = typer.Option("2,4,6", "--bars", help="Comma-separated stagnation horizons in bars"),
    capital: float = typer.Option(10_000_000.0, "--capital"),
    limit: int = typer.Option(200, "--limit"),
    output: Optional[Path] = typer.Option(None, "--output", help="HTML tear sheet path"),
    meta_filter: bool = typer.Option(
        True,
        "--meta-filter/--no-meta-filter",
        help="Use ML meta-filter selected entries for the sweep",
    ),
    min_prob: float = typer.Option(0.34, "--min-prob"),
    max_per_day: int = typer.Option(3, "--max-per-day"),
    min_per_day: int = typer.Option(0, "--min-per-day"),
    kelly_factor: float = typer.Option(0.35, "--kelly-factor"),
    time_exit_atr_min: float = typer.Option(0.5, "--time-exit-atr-min"),
    include_none: bool = typer.Option(
        True,
        "--include-none/--no-include-none",
        help="Include a no-time-exit control column",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Compare dynamic stagnation exits (N bars) on the same meta-selected book."""
    _setup_logging(verbose)
    if not meta_filter:
        raise typer.BadParameter("compare-time-exits currently requires --meta-filter")

    bar_list: List[int] = []
    for part in bars.split(","):
        part = part.strip()
        if not part:
            continue
        bar_list.append(int(part))
    if not bar_list:
        raise typer.BadParameter("Provide at least one bar horizon via --bars")

    from rocket.ml.pipeline import print_time_exit_comparison, run_time_exit_comparison

    settings = get_settings()
    out = Path(output) if output else settings.rocket_output_html
    result = run_time_exit_comparison(
        start=_parse_date(start_date),
        end=_parse_date(end_date),
        bars=bar_list,
        interval=interval,
        capital=capital,
        limit=limit,
        min_probability=min_prob,
        kelly_factor=kelly_factor,
        min_per_day=min_per_day,
        max_per_day=max_per_day,
        time_exit_atr_min=time_exit_atr_min,
        include_none=include_none,
    )
    metrics = result["primary_metrics"]
    horizons = metrics.get("time_exit_horizons") or []
    print_tearsheet(metrics, console)
    print_time_exit_comparison(result["time_exit_comparison"], horizons, console)
    title = (
        f"ML Meta + Time Exits · {start_date} → {end_date} · {interval} · "
        f"bars={','.join(str(b) for b in bar_list)} · {metrics.get('universe_size', 0)} symbols"
    )
    path = export_html(metrics, out, title=title)
    _mirror_html(path)


@app.command("sweep-confluence")
def sweep_confluence(
    start_date: str = typer.Option(..., "--start-date", help="YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="YYYY-MM-DD"),
    interval: str = typer.Option("5minute", "--interval"),
    capital: float = typer.Option(10_000_000.0, "--capital"),
    limit: int = typer.Option(200, "--limit"),
    output: Optional[Path] = typer.Option(None, "--output", help="HTML tear sheet path"),
    max_per_day: int = typer.Option(3, "--max-per-day"),
    min_per_day: int = typer.Option(0, "--min-per-day"),
    kelly_factor: float = typer.Option(0.35, "--kelly-factor"),
    time_exit_bars: Optional[int] = typer.Option(4, "--time-exit-bars"),
    time_exit_atr_min: float = typer.Option(0.5, "--time-exit-atr-min"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Grid-search confluence gates; pick 18–35 trades with highest profit factor."""
    _setup_logging(verbose)
    from rocket.ml.pipeline import print_confluence_sweep, run_confluence_sweep

    settings = get_settings()
    out = Path(output) if output else settings.rocket_output_html
    te_bars = int(time_exit_bars) if time_exit_bars and int(time_exit_bars) > 0 else None
    result = run_confluence_sweep(
        start=_parse_date(start_date),
        end=_parse_date(end_date),
        interval=interval,
        capital=capital,
        limit=limit,
        kelly_factor=kelly_factor,
        min_per_day=min_per_day,
        max_per_day=max_per_day,
        time_exit_bars=te_bars,
        time_exit_atr_min=time_exit_atr_min,
    )
    print_confluence_sweep(result.get("rows") or [], console)
    metrics = dict(result.get("primary_metrics") or {})
    metrics["confluence_sweep_rows"] = [
        {k: v for k, v in r.items() if k not in ("filtered", "gates", "comparison")}
        for r in (result.get("rows") or [])
    ]
    print_tearsheet(metrics, console)
    best = result.get("best") or {}
    title = (
        f"Confluence Sweep · {start_date} → {end_date} · {interval} · "
        f"best={best.get('label', 'n/a')}"
    )
    path = export_html(metrics, out, title=title)
    _mirror_html(path)


def main() -> None:
    # Allow `python -m rocket.main` and `python rocket/main.py`
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    app()


if __name__ == "__main__":
    main()
