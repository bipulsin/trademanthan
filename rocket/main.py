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
        help="Run ML meta-filter (soft-floor 0.55, max 3/day) and comparative report",
    ),
    min_prob: float = typer.Option(0.55, "--min-prob", help="Soft-floor P(win) threshold"),
    max_per_day: int = typer.Option(3, "--max-per-day", help="Max selected trades per day"),
    min_per_day: int = typer.Option(2, "--min-per-day", help="Soft minimum trades per day"),
    kelly_factor: float = typer.Option(0.35, "--kelly-factor", help="Fractional Kelly multiplier"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run Rocket backtest and write rocket.html tear sheet."""
    _setup_logging(verbose)
    settings = get_settings()
    out = Path(output) if output else settings.rocket_output_html
    bt = RocketBacktester(interval=interval, capital=capital, max_symbols=limit)
    bt.load_universe()
    _ = skip_fetch
    bt.fetch_data(_parse_date(start_date), _parse_date(end_date))

    if meta_filter:
        from rocket.ml.pipeline import print_comparison, run_comparative_meta_backtest

        result = run_comparative_meta_backtest(
            bt,
            _parse_date(start_date),
            _parse_date(end_date),
            min_probability=min_prob,
            min_per_day=min_per_day,
            max_per_day=max_per_day,
            kelly_factor=kelly_factor,
        )
        metrics = dict(result["filtered"])
        metrics["comparison"] = result["comparison"]
        metrics["baseline"] = result["baseline"]
        metrics["raw_signal_count"] = result.get("raw_signal_count")
        metrics["selected_count"] = result.get("selected_count")
        metrics["meta_metrics"] = result.get("meta_metrics")
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
    min_prob: float = typer.Option(0.55, "--min-prob"),
    max_per_day: int = typer.Option(3, "--max-per-day"),
    min_per_day: int = typer.Option(2, "--min-per-day"),
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


def main() -> None:
    # Allow `python -m rocket.main` and `python rocket/main.py`
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    app()


if __name__ == "__main__":
    main()
