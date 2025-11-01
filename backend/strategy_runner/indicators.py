"""
Indicators Module

This module provides functions to compute technical indicators using pandas_ta.
It takes strategy configuration and applies the appropriate indicators to
candlestick data.
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
import pandas_ta as ta


def apply_indicators(df: pd.DataFrame, indicators: List[str], parameters: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply technical indicators to the DataFrame based on strategy configuration.
    
    Args:
        df: DataFrame with OHLCV data (must have columns: open, high, low, close, volume)
        indicators: List of indicator names to compute
        parameters: Dictionary of parameters for each indicator
        
    Returns:
        DataFrame with original data plus computed indicator columns
        
    Raises:
        ValueError: If required columns are missing or invalid parameters
    """
    logger = logging.getLogger(__name__)
    
    # Validate input DataFrame
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"DataFrame missing required columns: {missing_columns}")
    
    # Make a copy to avoid modifying original
    result_df = df.copy()
    
    logger.debug("Applying indicators: %s", indicators)
    logger.debug("Parameters: %s", parameters)
    
    for indicator in indicators:
        try:
            result_df = _apply_single_indicator(result_df, indicator, parameters.get(indicator, {}))
        except Exception as e:
            logger.error("Failed to apply indicator %s: %s", indicator, e)
            # Continue with other indicators instead of failing completely
    
    return result_df


def _apply_single_indicator(df: pd.DataFrame, indicator: str, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply a single indicator to the DataFrame.
    
    Args:
        df: DataFrame with OHLCV data
        indicator: Name of the indicator to apply
        params: Parameters for the indicator
        
    Returns:
        DataFrame with the indicator columns added
    """
    logger = logging.getLogger(__name__)
    
    if indicator == "rsi":
        return _apply_rsi(df, params)
    elif indicator == "ema":
        return _apply_ema(df, params)
    elif indicator == "sma":
        return _apply_sma(df, params)
    elif indicator == "supertrend":
        return _apply_supertrend(df, params)
    elif indicator == "bb_squeeze":
        return _apply_bollinger_bands_squeeze(df, params)
    elif indicator == "bollinger_bands":
        return _apply_bollinger_bands(df, params)
    elif indicator == "macd":
        return _apply_macd(df, params)
    elif indicator == "stochastic":
        return _apply_stochastic(df, params)
    elif indicator == "atr":
        return _apply_atr(df, params)
    elif indicator == "triple_ema":
        return _apply_triple_ema(df, params)
    elif indicator == "adx":
        return _apply_adx(df, params)
    elif indicator == "cci":
        return _apply_cci(df, params)
    elif indicator == "williams_r":
        return _apply_williams_r(df, params)
    elif indicator == "obv":
        return _apply_obv(df, params)
    elif indicator == "vwap":
        return _apply_vwap(df, params)
    else:
        logger.warning("Unknown indicator: %s", indicator)
        return df


def _apply_rsi(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply RSI indicator."""
    length = params.get('length', 14)
    result = ta.rsi(df['close'], length=length)
    df[f'rsi_{length}'] = result
    return df


def _apply_ema(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply EMA indicator."""
    length = params.get('length', 20)
    result = ta.ema(df['close'], length=length)
    df[f'ema_{length}'] = result
    return df


def _apply_sma(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply SMA indicator."""
    length = params.get('length', 20)
    result = ta.sma(df['close'], length=length)
    df[f'sma_{length}'] = result
    return df


def _apply_triple_ema(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply Triple EMA indicator."""
    short_period = params.get('short_ema_period', 9)
    medium_period = params.get('medium_ema_period', 21)
    long_period = params.get('long_ema_period', 50)
    
    df[f'ema_short_{short_period}'] = ta.ema(df['close'], length=short_period)
    df[f'ema_medium_{medium_period}'] = ta.ema(df['close'], length=medium_period)
    df[f'ema_long_{long_period}'] = ta.ema(df['close'], length=long_period)
    
    return df


def _apply_supertrend(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply Supertrend indicator."""
    atr_length = params.get('atr_period', 10)
    multiplier = params.get('multiplier', 3)
    
    # Calculate ATR first
    atr = ta.atr(df['high'], df['low'], df['close'], length=atr_length)
    
    # Calculate Supertrend
    hl2 = (df['high'] + df['low']) / 2
    
    # Upper and Lower bands
    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)
    
    # Initialize Supertrend
    supertrend = pd.Series(index=df.index, dtype=float)
    trend = pd.Series(index=df.index, dtype=str)
    
    # First value
    supertrend.iloc[0] = lower_band.iloc[0]
    trend.iloc[0] = 'UP'
    
    for i in range(1, len(df)):
        if df['close'].iloc[i] > upper_band.iloc[i-1]:
            trend.iloc[i] = 'UP'
        elif df['close'].iloc[i] < lower_band.iloc[i-1]:
            trend.iloc[i] = 'DOWN'
        else:
            trend.iloc[i] = trend.iloc[i-1]
        
        if trend.iloc[i] == 'UP':
            if lower_band.iloc[i] < supertrend.iloc[i-1]:
                supertrend.iloc[i] = lower_band.iloc[i]
            else:
                supertrend.iloc[i] = supertrend.iloc[i-1]
        else:
            if upper_band.iloc[i] > supertrend.iloc[i-1]:
                supertrend.iloc[i] = upper_band.iloc[i]
            else:
                supertrend.iloc[i] = supertrend.iloc[i-1]
    
    # Add columns
    df['st_trend'] = supertrend
    df['st_upper'] = upper_band
    df['st_lower'] = lower_band
    df['st_direction'] = trend
    
    return df


def _apply_bollinger_bands(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply Bollinger Bands indicator."""
    length = params.get('length', 20)
    std_dev = params.get('std_dev', 2)
    
    bb = ta.bbands(df['close'], length=length, std=std_dev)
    
    if bb is not None and not bb.empty:
        df[f'bb_upper_{length}'] = bb[f'BBU_{length}_{std_dev}']
        df[f'bb_middle_{length}'] = bb[f'BBM_{length}_{std_dev}']
        df[f'bb_lower_{length}'] = bb[f'BBL_{length}_{std_dev}']
        df[f'bb_width_{length}'] = bb[f'BBW_{length}_{std_dev}']
        df[f'bb_percent_{length}'] = bb[f'BBP_{length}_{std_dev}']
    
    return df


def _apply_bollinger_bands_squeeze(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply Bollinger Bands Squeeze indicator."""
    bb_length = params.get('bb_length', 20)
    bb_std = params.get('bb_std', 2)
    kc_length = params.get('kc_length', 20)
    kc_std = params.get('kc_std', 1.5)
    squeeze_threshold = params.get('squeeze_threshold', 0.5)
    
    # Calculate Bollinger Bands
    bb = ta.bbands(df['close'], length=bb_length, std=bb_std)
    
    # Calculate Keltner Channels
    atr = ta.atr(df['high'], df['low'], df['close'], length=kc_length)
    hl2 = (df['high'] + df['low']) / 2
    
    kc_upper = hl2 + (kc_std * atr)
    kc_lower = hl2 - (kc_std * atr)
    
    # Calculate Squeeze
    bb_width = bb[f'BBW_{bb_length}_{bb_std}']
    kc_width = kc_upper - kc_lower
    
    # Squeeze is when BB width is less than KC width
    squeeze = bb_width < kc_width
    
    # Squeeze Momentum (when squeeze releases)
    squeeze_momentum = pd.Series(0.0, index=df.index)
    for i in range(1, len(df)):
        if squeeze.iloc[i-1] and not squeeze.iloc[i]:
            # Squeeze released
            squeeze_momentum.iloc[i] = 1.0
        elif not squeeze.iloc[i-1] and squeeze.iloc[i]:
            # Squeeze started
            squeeze_momentum.iloc[i] = -1.0
    
    # Add columns
    if bb is not None and not bb.empty:
        df[f'bb_upper_{bb_length}'] = bb[f'BBU_{bb_length}_{bb_std}']
        df[f'bb_middle_{bb_length}'] = bb[f'BBM_{bb_length}_{bb_std}']
        df[f'bb_lower_{bb_length}'] = bb[f'BBL_{bb_length}_{bb_std}']
        df[f'bb_width_{bb_length}'] = bb[f'BBW_{bb_length}_{bb_std}']
    
    df[f'kc_upper_{kc_length}'] = kc_upper
    df[f'kc_lower_{kc_length}'] = kc_lower
    df[f'kc_width_{kc_length}'] = kc_width
    df[f'squeeze_{bb_length}_{kc_length}'] = squeeze
    df[f'squeeze_momentum_{bb_length}_{kc_length}'] = squeeze_momentum
    
    return df


def _apply_macd(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply MACD indicator."""
    fast = params.get('fast', 12)
    slow = params.get('slow', 26)
    signal = params.get('signal', 9)
    
    macd = ta.macd(df['close'], fast=fast, slow=slow, signal=signal)
    
    if macd is not None and not macd.empty:
        df[f'macd_{fast}_{slow}_{signal}'] = macd[f'MACD_{fast}_{slow}_{signal}']
        df[f'macd_signal_{fast}_{slow}_{signal}'] = macd[f'MACDs_{fast}_{slow}_{signal}']
        df[f'macd_hist_{fast}_{slow}_{signal}'] = macd[f'MACDh_{fast}_{slow}_{signal}']
    
    return df


def _apply_stochastic(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply Stochastic indicator."""
    k_length = params.get('k_length', 14)
    d_length = params.get('d_length', 3)
    
    stoch = ta.stoch(df['high'], df['low'], df['close'], k=k_length, d=d_length)
    
    if stoch is not None and not stoch.empty:
        df[f'stoch_k_{k_length}'] = stoch[f'STOCHk_{k_length}_{d_length}']
        df[f'stoch_d_{k_length}'] = stoch[f'STOCHd_{k_length}_{d_length}']
    
    return df


def _apply_atr(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply ATR indicator."""
    length = params.get('length', 14)
    result = ta.atr(df['high'], df['low'], df['close'], length=length)
    df[f'atr_{length}'] = result
    return df


def _apply_adx(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply ADX indicator."""
    length = params.get('length', 14)
    result = ta.adx(df['high'], df['low'], df['close'], length=length)
    
    if result is not None and not result.empty:
        df[f'adx_{length}'] = result[f'ADX_{length}']
        df[f'di_plus_{length}'] = result[f'DMP_{length}']
        df[f'di_minus_{length}'] = result[f'DMN_{length}']
    
    return df


def _apply_cci(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply CCI indicator."""
    length = params.get('length', 20)
    result = ta.cci(df['high'], df['low'], df['close'], length=length)
    df[f'cci_{length}'] = result
    return df


def _apply_williams_r(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply Williams %R indicator."""
    length = params.get('length', 14)
    result = ta.willr(df['high'], df['low'], df['close'], length=length)
    df[f'williams_r_{length}'] = result
    return df


def _apply_obv(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply OBV indicator."""
    result = ta.obv(df['close'], df['volume'])
    df['obv'] = result
    return df


def _apply_vwap(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Apply VWAP indicator."""
    result = ta.vwap(df['high'], df['low'], df['close'], df['volume'])
    df['vwap'] = result
    return df


def get_indicator_columns(indicators: List[str], parameters: Dict[str, Any]) -> List[str]:
    """
    Get the list of column names that will be created by the indicators.
    
    Args:
        indicators: List of indicator names
        parameters: Dictionary of parameters for each indicator
        
    Returns:
        List of column names that will be added
    """
    column_names = []
    
    for indicator in indicators:
        if indicator == "rsi":
            length = parameters.get(indicator, {}).get('length', 14)
            column_names.append(f'rsi_{length}')
        elif indicator == "ema":
            length = parameters.get(indicator, {}).get('length', 20)
            column_names.append(f'ema_{length}')
        elif indicator == "sma":
            length = parameters.get(indicator, {}).get('length', 20)
            column_names.append(f'sma_{length}')
        elif indicator == "triple_ema":
            short_period = parameters.get(indicator, {}).get('short_ema_period', 9)
            medium_period = parameters.get(indicator, {}).get('medium_ema_period', 21)
            long_period = parameters.get(indicator, {}).get('long_ema_period', 50)
            column_names.extend([
                f'ema_short_{short_period}',
                f'ema_medium_{medium_period}',
                f'ema_long_{long_period}'
            ])
        elif indicator == "supertrend":
            column_names.extend(['st_trend', 'st_upper', 'st_lower', 'st_direction'])
        elif indicator == "bb_squeeze":
            bb_length = parameters.get(indicator, {}).get('bb_length', 20)
            kc_length = parameters.get(indicator, {}).get('kc_length', 20)
            column_names.extend([
                f'bb_upper_{bb_length}', f'bb_middle_{bb_length}', f'bb_lower_{bb_length}',
                f'bb_width_{bb_length}', f'kc_upper_{kc_length}', f'kc_lower_{kc_length}',
                f'kc_width_{kc_length}', f'squeeze_{bb_length}_{kc_length}',
                f'squeeze_momentum_{bb_length}_{kc_length}'
            ])
        elif indicator == "bollinger_bands":
            length = parameters.get(indicator, {}).get('length', 20)
            column_names.extend([
                f'bb_upper_{length}', f'bb_middle_{length}', f'bb_lower_{length}',
                f'bb_width_{length}', f'bb_percent_{length}'
            ])
        elif indicator == "macd":
            fast = parameters.get(indicator, {}).get('fast', 12)
            slow = parameters.get(indicator, {}).get('slow', 26)
            signal = parameters.get(indicator, {}).get('signal', 9)
            column_names.extend([
                f'macd_{fast}_{slow}_{signal}',
                f'macd_signal_{fast}_{slow}_{signal}',
                f'macd_hist_{fast}_{slow}_{signal}'
            ])
        elif indicator == "stochastic":
            k_length = parameters.get(indicator, {}).get('k_length', 14)
            column_names.extend([f'stoch_k_{k_length}', f'stoch_d_{k_length}'])
        elif indicator == "atr":
            length = parameters.get(indicator, {}).get('length', 14)
            column_names.append(f'atr_{length}')
        elif indicator == "adx":
            length = parameters.get(indicator, {}).get('length', 14)
            column_names.extend([f'adx_{length}', f'di_plus_{length}', f'di_minus_{length}'])
        elif indicator == "cci":
            length = parameters.get(indicator, {}).get('length', 20)
            column_names.append(f'cci_{length}')
        elif indicator == "williams_r":
            length = parameters.get(indicator, {}).get('length', 14)
            column_names.append(f'williams_r_{length}')
        elif indicator == "obv":
            column_names.append('obv')
        elif indicator == "vwap":
            column_names.append('vwap')
    
    return column_names

