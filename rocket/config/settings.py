"""Rocket settings — loads project-root `.env` then pydantic BaseSettings."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from rocket.config.constants import (
    DEFAULT_BROKERAGE_PER_ORDER,
    DEFAULT_INITIAL_MARGIN_PCT,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class RocketSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    DATABASE_URL: str = Field(
        default="postgresql://trademanthan:trademanthan123@127.0.0.1:15432/trademanthan"
    )
    UPSTOX_API_KEY: str = ""
    UPSTOX_API_SECRET: str = ""
    UPSTOX_ACCESS_TOKEN: Optional[str] = None

    # Backtest defaults
    rocket_initial_capital: float = 10_000_000.0
    rocket_brokerage_per_order: float = DEFAULT_BROKERAGE_PER_ORDER
    rocket_initial_margin_pct: float = DEFAULT_INITIAL_MARGIN_PCT
    rocket_max_margin_utilization_pct: float = 0.85
    rocket_slippage_ticks: float = 1.0
    # Primary slippage model: basis points of price (tick used only for rounding).
    # Avoids pathological costs when instruments.json stores large FO tick sizes.
    rocket_slippage_bps: float = 2.0
    rocket_max_positions: int = 10
    rocket_rate_limit_per_sec: float = 3.0
    rocket_cache_dir: Path = PROJECT_ROOT / ".cache" / "parquet"
    rocket_instruments_path: Path = PROJECT_ROOT / "data" / "instruments" / "nse_instruments.json"
    rocket_output_html: Path = PROJECT_ROOT / "rocket.html"


@lru_cache(maxsize=1)
def get_settings() -> RocketSettings:
    return RocketSettings()
