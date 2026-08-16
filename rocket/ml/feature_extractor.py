"""Technical / session-context features for Rocket meta-filtering.

Builds on the Upstox OHLCV schema used by ``RocketBacktester``
(``timestamp, open, high, low, close, volume, oi``) and adds trend,
volatility, and session-phase features at the trigger bar.
"""

from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd


FEATURE_COLUMNS = (
    "side_code",
    "directional_ema20_dist",
    "directional_ema50_dist",
    "directional_ema200_dist",
    "directional_vwap_dist",
    "ema_spread_atr",
    "bb_width",
    "pct_b",
    "rsi_14",
    "rsi_slope_3",
    "atr_pct",
    "rvol",
    "vol_surge",
    "range_pos",
    "high_breakout",
    "low_breakout",
    "dist_pdh_atr",
    "dist_pdl_atr",
    "dist_pdc_atr",
    "session_progress",
    "time_sin",
    "time_cos",
    "is_open_drive",
    "is_midday_chop",
    "is_power_hour",
    "dow",
)

# Default confluence thresholds (enforced via trade_selector.ConfluenceGatesConfig)
DEFAULT_CLV_THRESHOLD = 0.20
DEFAULT_BREADTH_LONG_MIN = 0.50
DEFAULT_BREADTH_SHORT_MAX = 0.50
DEFAULT_RVOL_MIN = 1.15


class RocketFeatureExtractor:
    """Computes technical, structural, and session-context features for 5m candles."""

    @staticmethod
    def _attach_htf_15m(out: pd.DataFrame) -> pd.DataFrame:
        """Attach completed 15m EMA20 + VWAP onto each 5m bar (backward as-of)."""
        if out.empty:
            out["ema_20_15m"] = np.nan
            out["vwap_15m"] = np.nan
            out["close_15m"] = np.nan
            return out

        indexed = out.set_index("timestamp").sort_index()
        htf = (
            indexed.resample("15min", label="left", closed="left")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna(subset=["close"])
        )
        if htf.empty:
            out["ema_20_15m"] = np.nan
            out["vwap_15m"] = np.nan
            out["close_15m"] = np.nan
            return out

        htf["ema_20_15m"] = htf["close"].ewm(span=20, adjust=False).mean()
        ts_ist = htf.index.tz_convert("Asia/Kolkata")
        day15 = pd.Series(ts_ist.date, index=htf.index)
        tp15 = (htf["high"] + htf["low"] + htf["close"]) / 3.0
        vol15 = htf["volume"].fillna(0.0).clip(lower=0.0)
        cum_vol15 = vol15.groupby(day15).cumsum()
        cum_pv15 = (tp15 * vol15).groupby(day15).cumsum()
        htf["vwap_15m"] = np.where(cum_vol15 > 0, cum_pv15 / cum_vol15, htf["close"])

        # Values become available only after the 15m bucket closes
        avail = htf[["ema_20_15m", "vwap_15m", "close"]].copy()
        avail.index = avail.index + pd.Timedelta(minutes=15)
        avail = avail.rename(columns={"close": "close_15m"}).reset_index()
        avail = avail.rename(columns={"timestamp": "timestamp"})

        base = out.sort_values("timestamp").reset_index(drop=True)
        merged = pd.merge_asof(
            base,
            avail.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
        )
        return merged

    @staticmethod
    def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """Enrich raw OHLCV with indicators (no lookahead beyond current bar)."""
        if df.empty:
            return df
        out = df.copy()
        if not pd.api.types.is_datetime64_any_dtype(out["timestamp"]):
            out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)

        high_low = out["high"] - out["low"]
        high_close = (out["high"] - out["close"].shift(1)).abs()
        low_close = (out["low"] - out["close"].shift(1)).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        out["atr_14"] = tr.rolling(window=14, min_periods=5).mean()
        out["safe_atr"] = np.where(out["atr_14"] > 0, out["atr_14"], out["close"] * 0.002)
        out["atr"] = out["atr_14"]  # compatibility with MLInstitutionalStrategy
        out["atr_pct"] = (out["safe_atr"] / out["close"].replace(0, np.nan)) * 100.0

        out["ema_5"] = out["close"].ewm(span=5, adjust=False).mean()
        out["ema_10"] = out["close"].ewm(span=10, adjust=False).mean()
        out["ema_20"] = out["close"].ewm(span=20, adjust=False).mean()
        out["ema_50"] = out["close"].ewm(span=50, adjust=False).mean()
        out["ema_200"] = out["close"].ewm(span=200, adjust=False).mean()

        out["dist_ema20_atr"] = (out["close"] - out["ema_20"]) / out["safe_atr"]
        out["dist_ema50_atr"] = (out["close"] - out["ema_50"]) / out["safe_atr"]
        out["dist_ema200_atr"] = (out["close"] - out["ema_200"]) / out["safe_atr"]
        out["ema_spread_atr"] = (out["ema_5"] - out["ema_20"]) / out["safe_atr"]
        out["ema5_dist_atr"] = (out["close"] - out["ema_5"]).abs() / out["safe_atr"]
        out["ema20_dist_atr"] = (out["close"] - out["ema_20"]).abs() / out["safe_atr"]

        # Session VWAP (reset each calendar day in IST)
        ts_ist = out["timestamp"].dt.tz_convert("Asia/Kolkata")
        day = ts_ist.dt.date
        tp = (out["high"] + out["low"] + out["close"]) / 3.0
        vol = out["volume"].fillna(0.0).clip(lower=0.0)
        cum_vol = vol.groupby(day).cumsum()
        cum_pv = (tp * vol).groupby(day).cumsum()
        out["vwap"] = np.where(cum_vol > 0, cum_pv / cum_vol, out["close"])
        out["dist_vwap_atr"] = (out["close"] - out["vwap"]) / out["safe_atr"]

        mid = out["close"].rolling(20, min_periods=10).mean()
        std = out["close"].rolling(20, min_periods=10).std()
        upper = mid + 2.0 * std
        lower = mid - 2.0 * std
        band = (upper - lower).replace(0, np.nan)
        out["bb_width"] = ((upper - lower) / mid.replace(0, np.nan)).fillna(0.0)
        out["pct_b"] = ((out["close"] - lower) / band).fillna(0.5)

        delta = out["close"].diff()
        gain = delta.clip(lower=0).rolling(window=14, min_periods=5).mean()
        loss = (-delta.clip(upper=0)).rolling(window=14, min_periods=5).mean()
        rs = gain / loss.replace(0, np.nan)
        out["rsi_14"] = (100.0 - (100.0 / (1.0 + rs))).fillna(50.0)
        out["rsi_slope_3"] = out["rsi_14"].diff(3).fillna(0.0)

        out["vol_sma20"] = vol.rolling(window=20, min_periods=5).mean()
        out["rvol"] = np.where(out["vol_sma20"] > 0, vol / out["vol_sma20"], 1.0)
        out["vol_surge"] = np.where(out["rvol"] >= 2.0, 1, 0)

        # Close Location Value: +1 close at high, -1 close at low
        bar_range = (out["high"] - out["low"]).replace(0, np.nan)
        out["clv"] = (
            ((out["close"] - out["low"]) - (out["high"] - out["close"])) / bar_range
        ).fillna(0.0).clip(-1.0, 1.0)

        # --- 15-minute HTF structure (no lookahead: only completed 15m bars) ---
        out = RocketFeatureExtractor._attach_htf_15m(out)

        out["day_high"] = out["high"].groupby(day).cummax()
        out["day_low"] = out["low"].groupby(day).cummin()
        day_range = (out["day_high"] - out["day_low"]).replace(0, np.nan)
        out["range_pos"] = ((out["close"] - out["day_low"]) / day_range).fillna(0.5)

        # Prior-bar breakout vs rolling 20-bar high/low (excludes current bar)
        roll_high = out["high"].shift(1).rolling(20, min_periods=5).max()
        roll_low = out["low"].shift(1).rolling(20, min_periods=5).min()
        out["high_breakout"] = (out["close"] > roll_high).astype(int)
        out["low_breakout"] = (out["close"] < roll_low).astype(int)

        # Previous session H/L/C (strictly prior calendar day)
        daily = (
            out.assign(_day=day)
            .groupby("_day", sort=True)
            .agg(pdh=("high", "max"), pdl=("low", "min"), pdc=("close", "last"))
        )
        daily[["pdh", "pdl", "pdc"]] = daily[["pdh", "pdl", "pdc"]].shift(1)
        mapped = day.map(daily.to_dict("index"))
        out["pdh"] = [m.get("pdh") if isinstance(m, dict) else np.nan for m in mapped]
        out["pdl"] = [m.get("pdl") if isinstance(m, dict) else np.nan for m in mapped]
        out["pdc"] = [m.get("pdc") if isinstance(m, dict) else np.nan for m in mapped]
        out["dist_pdh_atr"] = (out["close"] - out["pdh"]) / out["safe_atr"]
        out["dist_pdl_atr"] = (out["close"] - out["pdl"]) / out["safe_atr"]
        out["dist_pdc_atr"] = (out["close"] - out["pdc"]) / out["safe_atr"]

        minute_of_day = ts_ist.dt.hour * 60 + ts_ist.dt.minute
        # 09:15 = 555, 15:30 = 930
        out["session_progress"] = np.clip((minute_of_day - 555) / (930 - 555), 0.0, 1.0)
        out["time_sin"] = np.sin(2 * np.pi * out["session_progress"])
        out["time_cos"] = np.cos(2 * np.pi * out["session_progress"])
        out["is_open_drive"] = ((minute_of_day >= 555) & (minute_of_day <= 630)).astype(int)
        out["is_midday_chop"] = ((minute_of_day > 630) & (minute_of_day < 810)).astype(int)
        out["is_power_hour"] = (minute_of_day >= 810).astype(int)
        out["dow"] = ts_ist.dt.dayofweek.astype(int)

        return out

    @classmethod
    def extract_trade_features(
        cls,
        candle_df: pd.DataFrame,
        trigger_idx: int,
        side: str,
    ) -> Dict[str, float]:
        """Feature vector at ``trigger_idx``; distances signed toward trade direction."""
        row = candle_df.iloc[trigger_idx]
        side_u = side.upper()
        direction_mult = 1.0 if side_u in ("BUY", "LONG") else -1.0

        def _f(key: str, default: float = 0.0) -> float:
            val = row.get(key, default)
            try:
                v = float(val)
            except (TypeError, ValueError):
                return default
            return default if not np.isfinite(v) else v

        rsi = _f("rsi_14", 50.0)
        range_pos = _f("range_pos", 0.5)
        return {
            "side_code": 1.0 if direction_mult > 0 else -1.0,
            "directional_ema20_dist": _f("dist_ema20_atr") * direction_mult,
            "directional_ema50_dist": _f("dist_ema50_atr") * direction_mult,
            "directional_ema200_dist": _f("dist_ema200_atr") * direction_mult,
            "directional_vwap_dist": _f("dist_vwap_atr") * direction_mult,
            "ema_spread_atr": _f("ema_spread_atr") * direction_mult,
            "bb_width": _f("bb_width"),
            "pct_b": _f("pct_b", 0.5) if direction_mult > 0 else (1.0 - _f("pct_b", 0.5)),
            "rsi_14": rsi if direction_mult > 0 else (100.0 - rsi),
            "rsi_slope_3": _f("rsi_slope_3") * direction_mult,
            "atr_pct": _f("atr_pct"),
            "rvol": _f("rvol", 1.0),
            "vol_surge": _f("vol_surge"),
            "range_pos": range_pos if direction_mult > 0 else (1.0 - range_pos),
            "high_breakout": _f("high_breakout") if direction_mult > 0 else _f("low_breakout"),
            "low_breakout": _f("low_breakout") if direction_mult > 0 else _f("high_breakout"),
            "dist_pdh_atr": _f("dist_pdh_atr") * direction_mult,
            "dist_pdl_atr": _f("dist_pdl_atr") * direction_mult,
            "dist_pdc_atr": _f("dist_pdc_atr") * direction_mult,
            "session_progress": _f("session_progress"),
            "time_sin": _f("time_sin"),
            "time_cos": _f("time_cos"),
            "is_open_drive": _f("is_open_drive"),
            "is_midday_chop": _f("is_midday_chop"),
            "is_power_hour": _f("is_power_hour"),
            "dow": _f("dow"),
            # Gate / structural fields (not in meta FEATURE_COLUMNS)
            "ema5_dist_atr": _f("ema5_dist_atr"),
            "ema20_dist_atr": _f("ema20_dist_atr"),
            "raw_rsi_14": rsi,
            "ema_5": _f("ema_5"),
            "ema_10": _f("ema_10"),
            "ema_20": _f("ema_20"),
            "vwap": _f("vwap"),
            "safe_atr": _f("safe_atr"),
            "rvol_raw": _f("rvol", 1.0),
            "ema_20_15m": _f("ema_20_15m"),
            "vwap_15m": _f("vwap_15m"),
            "close_15m": _f("close_15m"),
            "clv": _f("clv", 0.0),
            "market_breadth": _f("market_breadth", 0.5),
        }

    @classmethod
    def attach_market_breadth(cls, series: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Cross-sectional share of symbols with close > session VWAP at each timestamp.

        ``market_breadth = mean(I(close > vwap))`` over the enriched universe.
        """
        if not series:
            return series
        frames: list[pd.DataFrame] = []
        for sym, df in series.items():
            if df is None or df.empty or "vwap" not in df.columns:
                continue
            tmp = df[["timestamp", "close", "vwap"]].copy()
            tmp["above_vwap"] = (tmp["close"].astype(float) > tmp["vwap"].astype(float)).astype(float)
            frames.append(tmp)
        if not frames:
            return series
        panel = pd.concat(frames, ignore_index=True)
        breadth = (
            panel.groupby("timestamp", sort=True)["above_vwap"]
            .mean()
            .rename("market_breadth")
            .reset_index()
        )
        out: Dict[str, pd.DataFrame] = {}
        for sym, df in series.items():
            if df is None or df.empty:
                out[sym] = df
                continue
            merged = df.drop(columns=["market_breadth"], errors="ignore").merge(
                breadth, on="timestamp", how="left"
            )
            out[sym] = merged
        return out

    @classmethod
    def enrich_universe(cls, series: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Return a new dict of symbol → indicator-enriched frames (+ market breadth)."""
        enriched = {sym: cls.calculate_indicators(df) for sym, df in series.items() if not df.empty}
        return cls.attach_market_breadth(enriched)
