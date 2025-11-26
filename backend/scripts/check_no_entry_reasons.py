#!/usr/bin/env python3
"""
Script to check why LTF and RELIANCE stocks are still showing "No Entry" status
"""

import sys
import os

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, parent_dir)

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from backend.services.upstox_service import upstox_service
from datetime import datetime
import pytz

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find LTF and RELIANCE trades
    trades = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= today,
        IntradayStockOption.stock_name.in_(['LTF', 'RELIANCE']),
        IntradayStockOption.status == 'no_entry'
    ).all()
    
    print('=' * 80)
    print('CHECKING WHY LTF AND RELIANCE ARE STILL NO ENTRY')
    print('=' * 80)
    print()
    
    for trade in trades:
        print(f'Stock: {trade.stock_name}')
        print(f'  Alert Time: {trade.alert_time}')
        print(f'  Status: {trade.status}')
        print(f'  Option Contract: {trade.option_contract}')
        print(f'  Option Type: {trade.option_type}')
        print(f'  Instrument Key: {trade.instrument_key}')
        print(f'  Qty: {trade.qty}')
        print()
        
        # Check current time
        print(f'  Current Time: {now.strftime("%Y-%m-%d %H:%M:%S")}')
        is_before_3pm = now.hour < 15
        print(f'  Time Check (before 3:00 PM): {"✅" if is_before_3pm else "❌"} {is_before_3pm}')
        print()
        
        # Check index trends
        index_trends = upstox_service.check_index_trends()
        nifty_trend = index_trends.get('nifty_trend', 'unknown')
        banknifty_trend = index_trends.get('banknifty_trend', 'unknown')
        print(f'  Index Trends:')
        print(f'    NIFTY: {nifty_trend}')
        print(f'    BANKNIFTY: {banknifty_trend}')
        
        option_type = trade.option_type or 'PE'
        both_bullish = (nifty_trend == 'bullish' and banknifty_trend == 'bullish')
        both_bearish = (nifty_trend == 'bearish' and banknifty_trend == 'bearish')
        opposite_directions = not both_bullish and not both_bearish
        
        can_enter_by_index = False
        if option_type == 'PE':
            if both_bullish or both_bearish:
                can_enter_by_index = True
            elif opposite_directions:
                can_enter_by_index = False
        elif option_type == 'CE':
            if both_bullish:
                can_enter_by_index = True
            elif both_bearish or opposite_directions:
                can_enter_by_index = False
        
        print(f'    Can Enter by Index: {"✅" if can_enter_by_index else "❌"} {can_enter_by_index}')
        print()
        
        # Fetch current stock data
        stock_data = upstox_service.get_stock_ltp_and_vwap(trade.stock_name)
        if stock_data:
            current_stock_ltp = stock_data.get('ltp', 0)
            current_stock_vwap = stock_data.get('vwap', 0)
            print(f'  Current Stock Data:')
            print(f'    LTP: ₹{current_stock_ltp:.2f}')
            print(f'    VWAP: ₹{current_stock_vwap:.2f}')
        else:
            print(f'  ⚠️ Could not fetch current stock data')
            current_stock_vwap = 0
        print()
        
        # Check VWAP slope
        stock_vwap_prev = trade.stock_vwap_previous_hour
        stock_vwap_prev_time = trade.stock_vwap_previous_hour_time
        print(f'  VWAP Slope Check:')
        print(f'    Previous Hour VWAP: ₹{stock_vwap_prev if stock_vwap_prev else "N/A"}')
        if stock_vwap_prev_time:
            print(f'    Previous Hour Time: {stock_vwap_prev_time.strftime("%Y-%m-%d %H:%M:%S")}')
        else:
            print(f'    Previous Hour Time: N/A')
        print(f'    Current VWAP: ₹{current_stock_vwap:.2f}')
        
        vwap_slope_passed = False
        if stock_vwap_prev and stock_vwap_prev > 0 and stock_vwap_prev_time and current_stock_vwap > 0:
            try:
                slope_result = upstox_service.vwap_slope(
                    vwap1=stock_vwap_prev,
                    time1=stock_vwap_prev_time,
                    vwap2=current_stock_vwap,
                    time2=now
                )
                if isinstance(slope_result, dict):
                    slope_status = slope_result.get('status', 'No')
                    slope_angle = slope_result.get('angle', 0.0)
                    slope_direction = slope_result.get('direction', 'flat')
                    vwap_slope_passed = (slope_status == 'Yes')
                    print(f'    VWAP Slope: {"✅" if vwap_slope_passed else "❌"} {slope_status} ({slope_angle:.2f}° {slope_direction})')
                else:
                    vwap_slope_passed = (slope_result == 'Yes')
                    print(f'    VWAP Slope: {"✅" if vwap_slope_passed else "❌"} {slope_result}')
            except Exception as e:
                print(f'    ⚠️ Error calculating VWAP slope: {e}')
        else:
            print(f'    ⚠️ Missing VWAP data for slope calculation')
        print()
        
        # Check candle size
        print(f'  Candle Size Check:')
        candle_size_passed = False
        if trade.instrument_key:
            try:
                option_candles = upstox_service.get_option_daily_candles_current_and_previous(trade.instrument_key)
                if option_candles:
                    current_day_candle = option_candles.get('current_day_candle', {})
                    previous_day_candle = option_candles.get('previous_day_candle', {})
                    
                    if current_day_candle and previous_day_candle:
                        current_size = abs(current_day_candle.get('high', 0) - current_day_candle.get('low', 0))
                        previous_size = abs(previous_day_candle.get('high', 0) - previous_day_candle.get('low', 0))
                        
                        if previous_size > 0:
                            size_ratio = current_size / previous_size
                            candle_size_passed = (size_ratio < 7.5)
                            print(f'    Current Day Size: ₹{current_size:.2f}')
                            print(f'    Previous Day Size: ₹{previous_size:.2f}')
                            print(f'    Ratio: {size_ratio:.2f}x')
                            print(f'    Candle Size: {"✅" if candle_size_passed else "❌"} {"Pass" if candle_size_passed else "Fail"} ({size_ratio:.2f}x)')
                        else:
                            print(f'    ⚠️ Previous day size is 0')
                    else:
                        print(f'    ⚠️ Missing candle data')
                else:
                    print(f'    ⚠️ Could not fetch option candles')
            except Exception as e:
                print(f'    ⚠️ Error fetching candles: {e}')
        else:
            print(f'    ⚠️ No instrument_key')
        print()
        
        # Check option contract
        has_option_data = bool(trade.option_contract and trade.instrument_key)
        print(f'  Option Data: {"✅" if has_option_data else "❌"} {"Valid" if has_option_data else "Missing"}')
        print()
        
        # Summary
        print(f'  ENTRY CONDITIONS SUMMARY:')
        print(f'    Time Check (< 3:00 PM): {"✅" if is_before_3pm else "❌"}')
        print(f'    Index Trends: {"✅" if can_enter_by_index else "❌"}')
        print(f'    VWAP Slope: {"✅" if vwap_slope_passed else "❌"}')
        print(f'    Candle Size: {"✅" if candle_size_passed else "❌"}')
        print(f'    Option Data: {"✅" if has_option_data else "❌"}')
        print()
        
        all_conditions_met = (is_before_3pm and can_enter_by_index and vwap_slope_passed and candle_size_passed and has_option_data)
        print(f'  ALL CONDITIONS MET: {"✅ YES" if all_conditions_met else "❌ NO"}')
        
        if not all_conditions_met:
            print()
            print('  FAILING CONDITIONS:')
            if not is_before_3pm:
                print('    - Time >= 3:00 PM')
            if not can_enter_by_index:
                print(f'    - Index trends not aligned (NIFTY: {nifty_trend}, BANKNIFTY: {banknifty_trend})')
            if not vwap_slope_passed:
                print('    - VWAP slope < 45°')
            if not candle_size_passed:
                print('    - Candle size >= 7.5× previous day')
            if not has_option_data:
                print('    - Missing option contract or instrument_key')
        
        print()
        print('=' * 80)
        print()
    
    db.close()

if __name__ == "__main__":
    main()

