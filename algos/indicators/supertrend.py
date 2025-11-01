"""
SuperTrend Indicator Implementation
Implements SuperTrend with configurable length and factor for Bitcoin options strategy
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)

class SuperTrend:
    """
    SuperTrend indicator implementation for Bitcoin options strategy
    
    SuperTrend is a trend-following indicator that combines ATR (Average True Range)
    with price action to determine trend direction and support/resistance levels.
    
    Parameters:
    - length: Period for ATR calculation (default: 16)
    - factor: Multiplier for ATR (default: 1.5)
    """
    
    def __init__(self, length: int = 16, factor: float = 1.5):
        self.length = length
        self.factor = factor
        self.name = f"SuperTrend_{length}_{factor}"
        
    def calculate_atr(self, high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
        """Calculate Average True Range (ATR)"""
        high_low = high - low
        high_close = np.abs(high - close.shift())
        low_close = np.abs(low - close.shift())
        
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=self.length).mean()
        
        return atr
    
    def calculate_supertrend(self, high: pd.Series, low: pd.Series, close: pd.Series) -> Dict[str, pd.Series]:
        """
        Calculate SuperTrend indicator values
        
        Args:
            high: High prices
            low: Low prices  
            close: Close prices
            
        Returns:
            Dictionary containing:
            - 'supertrend': SuperTrend line values
            - 'direction': Trend direction (1 for uptrend, -1 for downtrend)
            - 'atr': ATR values
            - 'upper_band': Upper SuperTrend band
            - 'lower_band': Lower SuperTrend band
        """
        try:
            # Calculate ATR
            atr = self.calculate_atr(high, low, close)
            
            # Calculate basic upper and lower bands
            hl2 = (high + low) / 2
            upper_band = hl2 + (self.factor * atr)
            lower_band = hl2 - (self.factor * atr)
            
            # Initialize arrays
            supertrend = pd.Series(index=close.index, dtype=float)
            direction = pd.Series(index=close.index, dtype=int)
            final_upper_band = pd.Series(index=close.index, dtype=float)
            final_lower_band = pd.Series(index=close.index, dtype=float)
            
            # Find first valid ATR index
            first_valid_atr = atr.first_valid_index()
            if first_valid_atr is None:
                # No valid ATR data
                return {
                    'supertrend': supertrend,
                    'direction': direction,
                    'atr': atr,
                    'upper_band': final_upper_band,
                    'lower_band': final_lower_band
                }
            
            first_valid_idx = atr.index.get_loc(first_valid_atr)
            
            # Initialize first valid values
            supertrend.iloc[first_valid_idx] = lower_band.iloc[first_valid_idx]
            direction.iloc[first_valid_idx] = 1
            final_upper_band.iloc[first_valid_idx] = upper_band.iloc[first_valid_idx]
            final_lower_band.iloc[first_valid_idx] = lower_band.iloc[first_valid_idx]
            
            # Calculate SuperTrend for each period starting from first valid ATR
            for i in range(first_valid_idx + 1, len(close)):
                # Skip if ATR is NaN (not enough data)
                if pd.isna(atr.iloc[i]):
                    supertrend.iloc[i] = np.nan
                    direction.iloc[i] = direction.iloc[i-1] if i > first_valid_idx else 1
                    final_upper_band.iloc[i] = final_upper_band.iloc[i-1] if i > first_valid_idx else upper_band.iloc[i]
                    final_lower_band.iloc[i] = final_lower_band.iloc[i-1] if i > first_valid_idx else lower_band.iloc[i]
                    continue
                
                # Current upper and lower bands
                curr_upper = upper_band.iloc[i]
                curr_lower = lower_band.iloc[i]
                
                # Previous values
                prev_upper = final_upper_band.iloc[i-1]
                prev_lower = final_lower_band.iloc[i-1]
                prev_close = close.iloc[i-1]
                prev_direction = direction.iloc[i-1]
                
                # Final upper band
                if curr_upper < prev_upper or prev_close > prev_upper:
                    final_upper_band.iloc[i] = curr_upper
                else:
                    final_upper_band.iloc[i] = prev_upper
                
                # Final lower band
                if curr_lower > prev_lower or prev_close < prev_lower:
                    final_lower_band.iloc[i] = curr_lower
                else:
                    final_lower_band.iloc[i] = prev_lower
                
                # Determine direction and SuperTrend value
                if prev_direction == 1 and close.iloc[i] <= final_lower_band.iloc[i]:
                    direction.iloc[i] = -1
                    supertrend.iloc[i] = final_upper_band.iloc[i]
                elif prev_direction == -1 and close.iloc[i] >= final_upper_band.iloc[i]:
                    direction.iloc[i] = 1
                    supertrend.iloc[i] = final_lower_band.iloc[i]
                else:
                    direction.iloc[i] = prev_direction
                    if prev_direction == 1:
                        supertrend.iloc[i] = final_lower_band.iloc[i]
                    else:
                        supertrend.iloc[i] = final_upper_band.iloc[i]
            
            result = {
                'supertrend': supertrend,
                'direction': direction,
                'atr': atr,
                'upper_band': final_upper_band,
                'lower_band': final_lower_band
            }
            
            logger.debug(f"SuperTrend calculated for {len(close)} periods")
            return result
            
        except Exception as e:
            logger.error(f"Error calculating SuperTrend: {e}")
            raise
    
    def get_signal(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Get SuperTrend trading signal
        
        Args:
            data: DataFrame with OHLC data
            
        Returns:
            Dictionary with signal information
        """
        try:
            if len(data) < self.length + 1:
                return {
                    'signal': 'HOLD',
                    'direction': 0,
                    'supertrend_value': None,
                    'confidence': 0.0
                }
            
            # Calculate SuperTrend
            st_data = self.calculate_supertrend(data['high'], data['low'], data['close'])
            
            # Get current and previous values
            valid_direction = st_data['direction'].dropna()
            if len(valid_direction) > 0:
                current_direction = valid_direction.iloc[-1]
                previous_direction = valid_direction.iloc[-2] if len(valid_direction) > 1 else 0
            else:
                current_direction = 0
                previous_direction = 0
            
            # Find the last valid SuperTrend value
            valid_supertrend = st_data['supertrend'].dropna()
            if len(valid_supertrend) > 0:
                current_supertrend = valid_supertrend.iloc[-1]
            else:
                current_supertrend = None
                
            current_price = data['close'].iloc[-1]
            
            # Determine signal
            signal = 'HOLD'
            confidence = 0.5
            
            if current_direction != previous_direction:
                if current_direction == 1:  # Uptrend (Green)
                    signal = 'BUY'  # Sell Put
                    confidence = 0.8
                elif current_direction == -1:  # Downtrend (Red)
                    signal = 'SELL'  # Sell Call
                    confidence = 0.8
            
            return {
                'signal': signal,
                'direction': current_direction,
                'supertrend_value': current_supertrend,
                'current_price': current_price,
                'confidence': confidence,
                'trend_change': current_direction != previous_direction
            }
            
        except Exception as e:
            logger.error(f"Error getting SuperTrend signal: {e}")
            return {
                'signal': 'HOLD',
                'direction': 0,
                'supertrend_value': None,
                'confidence': 0.0
            }
    
    def is_trend_change(self, data: pd.DataFrame) -> bool:
        """
        Check if SuperTrend trend has changed
        
        Args:
            data: DataFrame with OHLC data
            
        Returns:
            True if trend has changed, False otherwise
        """
        try:
            if len(data) < self.length + 2:
                return False
            
            st_data = self.calculate_supertrend(data['high'], data['low'], data['close'])
            current_direction = st_data['direction'].iloc[-1]
            previous_direction = st_data['direction'].iloc[-2]
            
            return current_direction != previous_direction
            
        except Exception as e:
            logger.error(f"Error checking trend change: {e}")
            return False
