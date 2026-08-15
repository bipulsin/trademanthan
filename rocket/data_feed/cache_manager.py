"""Local Parquet cache for Upstox historical candles."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _safe_key(instrument_key: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", instrument_key)


class CacheManager:
    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def path_for(self, instrument_key: str, interval: str, from_date: str, to_date: str) -> Path:
        name = f"{_safe_key(instrument_key)}_{interval}_{from_date}_{to_date}.parquet"
        return self.cache_dir / name

    def load(
        self, instrument_key: str, interval: str, from_date: str, to_date: str
    ) -> Optional[pd.DataFrame]:
        path = self.path_for(instrument_key, interval, from_date, to_date)
        if not path.exists():
            return None
        try:
            df = pd.read_parquet(path)
            if df.empty:
                return None
            logger.debug("cache hit %s", path.name)
            return df
        except Exception as exc:
            logger.warning("cache read failed %s: %s", path, exc)
            return None

    def save(
        self,
        instrument_key: str,
        interval: str,
        from_date: str,
        to_date: str,
        rows: List[dict],
    ) -> Path:
        path = self.path_for(instrument_key, interval, from_date, to_date)
        df = pd.DataFrame(rows)
        if not df.empty and "timestamp" in df.columns:
            df = df.sort_values("timestamp").reset_index(drop=True)
        df.to_parquet(path, index=False)
        return path
