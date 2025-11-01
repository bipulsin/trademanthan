#!/usr/bin/env python3
"""
Test script for the Strategy Runner + Generator system.

This script tests the basic functionality without requiring a database connection.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add the current directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent))

from utils import timeframe_to_seconds, mask_secret, setup_strategy_logger
from indicators import apply_indicators, get_indicator_columns
from conditions import evaluate_entry_exit
import pandas as pd


def test_timeframe_conversion():
    """Test timeframe conversion utilities."""
    print("🧪 Testing timeframe conversion...")
    
    test_cases = [
        ("1m", 60),
        ("5m", 300),
        ("15m", 900),
        ("1h", 3600),
        ("4h", 14400),
        ("1d", 86400)
    ]
    
    for timeframe, expected_seconds in test_cases:
        result = timeframe_to_seconds(timeframe)
        status = "✅" if result == expected_seconds else "❌"
        print(f"  {status} {timeframe} -> {result}s (expected: {expected_seconds}s)")
    
    print()


def test_secret_masking():
    """Test secret masking utilities."""
    print("🔐 Testing secret masking...")
    
    test_secrets = [
        ("secret123", "*****123"),
        ("api_key", "*****key"),
        ("short", "****"),
        ("", ""),
        ("a", "*")
    ]
    
    for secret, expected_masked in test_secrets:
        masked = mask_secret(secret)
        status = "✅" if masked == expected_masked else "❌"
        print(f"  {status} '{secret}' -> '{masked}' (expected: '{expected_masked}')")
    
    print()


def test_indicators():
    """Test indicator computation."""
    print("📊 Testing indicators...")
    
    # Create sample OHLCV data
    data = {
        'open': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114],
        'high': [102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116],
        'low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113],
        'close': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
        'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400]
    }
    
    df = pd.DataFrame(data)
    
    # Test RSI indicator
    print("  Testing RSI indicator...")
    indicators = ["rsi"]
    parameters = {"rsi": {"length": 14}}
    
    try:
        result_df = apply_indicators(df, indicators, parameters)
        rsi_column = f"rsi_{parameters['rsi']['length']}"
        
        if rsi_column in result_df.columns:
            print(f"    ✅ RSI indicator computed successfully")
            print(f"       RSI values: {result_df[rsi_column].dropna().tolist()}")
        else:
            print(f"    ❌ RSI indicator failed - column not found")
            
    except Exception as e:
        print(f"    ❌ RSI indicator failed with error: {e}")
    
    # Test Triple EMA indicator
    print("  Testing Triple EMA indicator...")
    indicators = ["triple_ema"]
    parameters = {"triple_ema": {"short_ema_period": 5, "medium_ema_period": 10, "long_ema_period": 15}}
    
    try:
        result_df = apply_indicators(df, indicators, parameters)
        ema_columns = [f"ema_short_5", f"ema_medium_10", f"ema_long_15"]
        
        if all(col in result_df.columns for col in ema_columns):
            print(f"    ✅ Triple EMA indicator computed successfully")
            for col in ema_columns:
                values = result_df[col].dropna().tolist()
                print(f"       {col}: {values[-3:]} (last 3 values)")
        else:
            print(f"    ❌ Triple EMA indicator failed - columns not found")
            
    except Exception as e:
        print(f"    ❌ Triple EMA indicator failed with error: {e}")
    
    # Test Bollinger Bands Squeeze indicator
    print("  Testing Bollinger Bands Squeeze indicator...")
    indicators = ["bb_squeeze"]
    parameters = {"bb_squeeze": {"bb_length": 10, "kc_length": 10}}
    
    try:
        result_df = apply_indicators(df, indicators, parameters)
        bb_columns = ["bb_upper_10", "bb_middle_10", "bb_lower_10", "squeeze_10_10"]
        
        if all(col in result_df.columns for col in bb_columns):
            print(f"    ✅ BB Squeeze indicator computed successfully")
            print(f"       Squeeze values: {result_df['squeeze_10_10'].dropna().tolist()}")
        else:
            print(f"    ❌ BB Squeeze indicator failed - columns not found")
            
    except Exception as e:
        print(f"    ❌ BB Squeeze indicator failed with error: {e}")
    
    # Test Supertrend indicator
    print("  Testing Supertrend indicator...")
    indicators = ["supertrend"]
    parameters = {"supertrend": {"atr_period": 5, "multiplier": 2}}
    
    try:
        result_df = apply_indicators(df, indicators, parameters)
        st_columns = ["st_trend", "st_upper", "st_lower", "st_direction"]
        
        if all(col in result_df.columns for col in st_columns):
            print(f"    ✅ Supertrend indicator computed successfully")
            print(f"       Direction values: {result_df['st_direction'].dropna().tolist()}")
        else:
            print(f"    ❌ Supertrend indicator failed - columns not found")
            
    except Exception as e:
        print(f"    ❌ Supertrend indicator failed with error: {e}")
    
    # Test indicator column names
    expected_columns = get_indicator_columns(indicators, parameters)
    print(f"  📋 Expected indicator columns: {expected_columns}")
    
    print()


def test_conditions():
    """Test condition evaluation."""
    print("🔍 Testing condition evaluation...")
    
    # Create sample data with indicators
    data = {
        'open': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114],
        'high': [102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116],
        'low': [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113],
        'close': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
        'volume': [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400],
        'rsi_14': [30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100],
        'ema_20': [100, 100.5, 101, 101.5, 102, 102.5, 103, 103.5, 104, 104.5, 105, 105.5, 106, 106.5, 107],
        'st_direction': ['UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP', 'UP']
    }
    
    df = pd.DataFrame(data)
    
    # Test simple conditions
    trade_conditions = {
        "rsi_14": {"below": 35}
    }
    
    entry_criteria = {
        "threshold": {
            "indicator": "rsi_14",
            "threshold": 30,
            "operator": "below"
        }
    }
    
    exit_criteria = {
        "threshold": {
            "indicator": "rsi_14",
            "threshold": 70,
            "operator": "above"
        }
    }
    
    try:
        result = evaluate_entry_exit(
            df, trade_conditions, "AND", entry_criteria, exit_criteria
        )
        
        print(f"  ✅ Condition evaluation successful")
        print(f"     Entry signal: {result['entry']}")
        print(f"     Exit signal: {result['exit']}")
        print(f"     Confidence: {result['confidence']:.2f}")
        
    except Exception as e:
        print(f"  ❌ Condition evaluation failed with error: {e}")
    
    print()


def test_logging():
    """Test logging setup."""
    print("📝 Testing logging setup...")
    
    try:
        logger = setup_strategy_logger(999, "Test Strategy", "DEBUG")
        logger.info("Test log message")
        logger.warning("Test warning message")
        logger.error("Test error message")
        
        print("  ✅ Logging setup successful")
        print("  📁 Check logs/strategy_999.log for log output")
        
    except Exception as e:
        print(f"  ❌ Logging setup failed with error: {e}")
    
    print()


def test_pandas_ta_compatibility():
    """Test pandas_ta compatibility and functionality."""
    print("🔧 Testing pandas_ta compatibility...")
    
    try:
        import pandas_ta as ta
        print(f"  ✅ pandas_ta imported successfully (version: {ta.__version__})")
        
        # Test basic pandas_ta functionality
        data = {
            'open': [100, 101, 102, 103, 104],
            'high': [102, 103, 104, 105, 106],
            'low': [99, 100, 101, 102, 103],
            'close': [101, 102, 103, 104, 105],
            'volume': [1000, 1100, 1200, 1300, 1400]
        }
        
        df = pd.DataFrame(data)
        
        # Test RSI
        rsi = ta.rsi(df['close'], length=3)
        if rsi is not None:
            print(f"    ✅ pandas_ta RSI working: {rsi.tolist()}")
        else:
            print(f"    ❌ pandas_ta RSI failed")
        
        # Test EMA
        ema = ta.ema(df['close'], length=3)
        if ema is not None:
            print(f"    ✅ pandas_ta EMA working: {ema.tolist()}")
        else:
            print(f"    ❌ pandas_ta EMA failed")
        
        # Test Bollinger Bands
        bb = ta.bbands(df['close'], length=3, std=2)
        if bb is not None and not bb.empty:
            print(f"    ✅ pandas_ta Bollinger Bands working")
        else:
            print(f"    ❌ pandas_ta Bollinger Bands failed")
            
    except ImportError as e:
        print(f"  ❌ pandas_ta import failed: {e}")
    except Exception as e:
        print(f"  ❌ pandas_ta test failed: {e}")
    
    print()


def test_numpy_compatibility():
    """Test numpy compatibility."""
    print("🔢 Testing numpy compatibility...")
    
    try:
        import numpy as np
        print(f"  ✅ numpy imported successfully (version: {np.__version__})")
        
        # Test basic numpy functionality
        arr = np.array([1, 2, 3, 4, 5])
        mean_val = np.mean(arr)
        std_val = np.std(arr)
        
        print(f"    ✅ numpy basic functions working")
        print(f"       Array: {arr}")
        print(f"       Mean: {mean_val}")
        print(f"       Std: {std_val}")
        
        # Test pandas_ta with numpy
        import pandas_ta as ta
        data = pd.DataFrame({
            'close': [100, 101, 102, 103, 104]
        })
        
        rsi = ta.rsi(data['close'], length=3)
        if rsi is not None:
            print(f"    ✅ pandas_ta + numpy integration working")
        else:
            print(f"    ❌ pandas_ta + numpy integration failed")
            
    except ImportError as e:
        print(f"  ❌ numpy import failed: {e}")
    except Exception as e:
        print(f"  ❌ numpy test failed: {e}")
    
    print()


async def main():
    """Main test function."""
    print("🚀 Starting Strategy Runner + Generator Tests")
    print("=" * 50)
    
    # Run tests
    test_timeframe_conversion()
    test_secret_masking()
    test_indicators()
    test_conditions()
    test_logging()
    test_pandas_ta_compatibility()
    test_numpy_compatibility()
    
    print("🎉 All tests completed!")
    print("\n📋 Test Summary:")
    print("  - Timeframe conversion utilities")
    print("  - Secret masking for security")
    print("  - Technical indicator computation (pandas_ta)")
    print("  - Trading condition evaluation")
    print("  - Logging system setup")
    print("  - pandas_ta compatibility")
    print("  - numpy compatibility")
    
    print("\n💡 Next steps:")
    print("  1. Set up your database connection")
    print("  2. Configure your Delta Exchange API credentials")
    print("  3. Create strategies in the database")
    print("  4. Run: python main.py --strategy-id <ID>")


if __name__ == "__main__":
    asyncio.run(main())
