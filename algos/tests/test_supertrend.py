"""
Unit tests for SuperTrend indicator
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add parent directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from indicators.supertrend import SuperTrend

class TestSuperTrend:
    """Test cases for SuperTrend indicator"""
    
    def setup_method(self):
        """Setup test data"""
        # Create sample OHLC data
        dates = pd.date_range('2024-01-01', periods=100, freq='90min')
        np.random.seed(42)  # For reproducible tests
        
        # Generate realistic price data
        base_price = 40000
        price_changes = np.random.normal(0, 0.02, 100)  # 2% volatility
        prices = [base_price]
        
        for change in price_changes[1:]:
            new_price = prices[-1] * (1 + change)
            prices.append(new_price)
        
        # Create OHLC data
        self.test_data = pd.DataFrame({
            'open': prices,
            'high': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
            'low': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
            'close': prices,
            'volume': np.random.randint(1000, 10000, 100)
        }, index=dates)
        
        # Ensure high >= low
        self.test_data['high'] = np.maximum(self.test_data['high'], self.test_data['low'])
        
        self.supertrend = SuperTrend(length=16, factor=1.5)
    
    def test_supertrend_initialization(self):
        """Test SuperTrend initialization"""
        assert self.supertrend.length == 16
        assert self.supertrend.factor == 1.5
        assert self.supertrend.name == "SuperTrend_16_1.5"
    
    def test_calculate_atr(self):
        """Test ATR calculation"""
        atr = self.supertrend.calculate_atr(
            self.test_data['high'],
            self.test_data['low'],
            self.test_data['close']
        )
        
        assert len(atr) == len(self.test_data)
        assert not atr.isna().all()  # Should have some valid values
        assert (atr >= 0).all()  # ATR should be non-negative
    
    def test_calculate_supertrend(self):
        """Test SuperTrend calculation"""
        result = self.supertrend.calculate_supertrend(
            self.test_data['high'],
            self.test_data['low'],
            self.test_data['close']
        )
        
        # Check required keys
        required_keys = ['supertrend', 'direction', 'atr', 'upper_band', 'lower_band']
        for key in required_keys:
            assert key in result
            assert len(result[key]) == len(self.test_data)
        
        # Check direction values
        directions = result['direction'].dropna()
        assert (directions.isin([1, -1])).all()
        
        # Check SuperTrend values are positive
        supertrend_values = result['supertrend'].dropna()
        assert (supertrend_values > 0).all()
    
    def test_get_signal_insufficient_data(self):
        """Test signal generation with insufficient data"""
        small_data = self.test_data.head(10)  # Less than required length
        signal = self.supertrend.get_signal(small_data)
        
        assert signal['signal'] == 'HOLD'
        assert signal['direction'] == 0
        assert signal['confidence'] == 0.0
    
    def test_get_signal_sufficient_data(self):
        """Test signal generation with sufficient data"""
        signal = self.supertrend.get_signal(self.test_data)
        
        # Check required keys
        required_keys = ['signal', 'direction', 'supertrend_value', 'confidence', 'trend_change']
        for key in required_keys:
            assert key in signal
        
        # Check signal values
        assert signal['signal'] in ['HOLD', 'BUY', 'SELL']
        assert signal['direction'] in [0, 1, -1]
        assert 0 <= signal['confidence'] <= 1
    
    def test_is_trend_change(self):
        """Test trend change detection"""
        # Test with sufficient data
        trend_change = self.supertrend.is_trend_change(self.test_data)
        assert isinstance(trend_change, bool)
        
        # Test with insufficient data
        small_data = self.test_data.head(10)
        trend_change_small = self.supertrend.is_trend_change(small_data)
        assert trend_change_small == False
    
    def test_different_parameters(self):
        """Test SuperTrend with different parameters"""
        # Test with different length and factor
        st_custom = SuperTrend(length=10, factor=2.0)
        result = st_custom.calculate_supertrend(
            self.test_data['high'],
            self.test_data['low'],
            self.test_data['close']
        )
        
        assert len(result['supertrend']) == len(self.test_data)
        assert st_custom.length == 10
        assert st_custom.factor == 2.0
    
    def test_edge_cases(self):
        """Test edge cases"""
        # Test with all same prices
        same_prices = pd.DataFrame({
            'open': [40000] * 50,
            'high': [40000] * 50,
            'low': [40000] * 50,
            'close': [40000] * 50
        })
        
        result = self.supertrend.calculate_supertrend(
            same_prices['high'],
            same_prices['low'],
            same_prices['close']
        )
        
        # Should still return valid results
        assert len(result['supertrend']) == len(same_prices)
        
        # Test with NaN values
        data_with_nan = self.test_data.copy()
        data_with_nan.loc[10:15, 'close'] = np.nan
        
        result_nan = self.supertrend.calculate_supertrend(
            data_with_nan['high'],
            data_with_nan['low'],
            data_with_nan['close']
        )
        
        # Should handle NaN values gracefully
        assert len(result_nan['supertrend']) == len(data_with_nan)

if __name__ == "__main__":
    pytest.main([__file__])
