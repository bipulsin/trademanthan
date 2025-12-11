#!/usr/bin/env python3
"""
Script to check why today's stock options are showing "No Entry" status
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
from datetime import datetime, timedelta
import pytz
from collections import defaultdict

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find all trades for today
    trades = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= today,
        IntradayStockOption.trade_date < today + timedelta(days=1)
    ).order_by(IntradayStockOption.alert_time.desc()).all()
    
    print('=' * 80)
    print(f'CHECKING TODAY\'S STOCK OPTIONS - {today.strftime("%Y-%m-%d")}')
    print('=' * 80)
    print()
    
    if not trades:
        print('❌ No trades found for today!')
        print()
        print('Possible reasons:')
        print('  1. No webhook alerts received today')
        print('  2. Market is closed (weekend/holiday)')
        print('  3. Chartink scan did not trigger any alerts')
        db.close()
        return
    
    # Group by status
    status_counts = defaultdict(int)
    for trade in trades:
        status_counts[trade.status] += 1
    
    print(f'Total trades found: {len(trades)}')
    print(f'Status breakdown:')
    for status, count in sorted(status_counts.items()):
        print(f'  - {status}: {count}')
    print()
    
    # Check no_entry trades
    no_entry_trades = [t for t in trades if t.status == 'no_entry']
    
    if not no_entry_trades:
        print('✅ No "No Entry" trades found - all trades were entered!')
        db.close()
        return
    
    print(f'Found {len(no_entry_trades)} "No Entry" trades')
    print()
    
    # Group reasons for no_entry
    reasons = defaultdict(list)
    
    for trade in no_entry_trades:
        print('=' * 80)
        print(f'Stock: {trade.stock_name} | Alert Type: {trade.alert_type}')
        print(f'  Alert Time: {trade.alert_time.strftime("%Y-%m-%d %H:%M:%S") if trade.alert_time else "N/A"}')
        print(f'  Status: {trade.status}')
        print(f'  Option Contract: {trade.option_contract or "N/A"}')
        print(f'  Option Type: {trade.option_type or "N/A"}')
        print(f'  Qty: {trade.qty}')
        print(f'  Buy Price: {trade.buy_price if trade.buy_price else "N/A"}')
        print()
        
        # Check current time vs alert time
        alert_time = trade.alert_time
        if alert_time:
            is_after_3pm = alert_time.hour >= 15
            print(f'  ⏰ Time Check:')
            print(f'    Alert Time: {alert_time.strftime("%H:%M:%S")}')
            print(f'    Before 3:00 PM: {"✅" if not is_after_3pm else "❌"}')
            if is_after_3pm:
                reasons['Time >= 3:00 PM'].append(trade.stock_name)
        else:
            print(f'  ⏰ Time Check: ⚠️ No alert_time')
        print()
        
        # Check index trends (at alert time, but we'll check current)
        print(f'  📊 Index Trends Check:')
        index_trends = upstox_service.check_index_trends()
        nifty_trend = index_trends.get('nifty_trend', 'unknown')
        banknifty_trend = index_trends.get('banknifty_trend', 'unknown')
        print(f'    NIFTY: {nifty_trend}')
        print(f'    BANKNIFTY: {banknifty_trend}')
        
        option_type = trade.option_type or 'PE'
        alert_type = trade.alert_type or 'Bearish'
        
        # Determine expected option type from alert type
        if alert_type == 'Bullish':
            expected_option_type = 'CE'
        else:
            expected_option_type = 'PE'
        
        # Check if trends align
        both_bullish = (nifty_trend == 'bullish' and banknifty_trend == 'bullish')
        both_bearish = (nifty_trend == 'bearish' and banknifty_trend == 'bearish')
        opposite_directions = not both_bullish and not both_bearish
        
        can_enter_by_index = False
        if expected_option_type == 'PE':
            if both_bullish or both_bearish:
                can_enter_by_index = True
            elif opposite_directions:
                can_enter_by_index = False
        elif expected_option_type == 'CE':
            if both_bullish:
                can_enter_by_index = True
            elif both_bearish or opposite_directions:
                can_enter_by_index = False
        
        print(f'    Expected Option Type: {expected_option_type} (from {alert_type} alert)')
        print(f'    Can Enter by Index: {"✅" if can_enter_by_index else "❌"}')
        if not can_enter_by_index:
            reasons['Index trends not aligned'].append(f"{trade.stock_name} (NIFTY:{nifty_trend}, BANKNIFTY:{banknifty_trend})")
        print()
        
        # Check VWAP slope
        print(f'  📈 VWAP Slope Check:')
        stock_vwap_prev = trade.stock_vwap_previous_hour
        stock_vwap_prev_time = trade.stock_vwap_previous_hour_time
        stock_vwap_current = trade.stock_vwap
        
        if stock_vwap_prev and stock_vwap_prev > 0 and stock_vwap_prev_time and stock_vwap_current and stock_vwap_current > 0:
            try:
                slope_result = upstox_service.vwap_slope(
                    vwap1=stock_vwap_prev,
                    time1=stock_vwap_prev_time,
                    vwap2=stock_vwap_current,
                    time2=alert_time if alert_time else now
                )
                if isinstance(slope_result, dict):
                    slope_status = slope_result.get('status', 'No')
                    slope_angle = slope_result.get('angle', 0.0)
                    slope_direction = slope_result.get('direction', 'flat')
                    vwap_slope_passed = (slope_status == 'Yes')
                    print(f'    Previous Hour VWAP: ₹{stock_vwap_prev:.2f} at {stock_vwap_prev_time.strftime("%H:%M:%S") if stock_vwap_prev_time else "N/A"}')
                    print(f'    Current VWAP: ₹{stock_vwap_current:.2f}')
                    print(f'    VWAP Slope: {"✅" if vwap_slope_passed else "❌"} {slope_status} ({slope_angle:.2f}° {slope_direction})')
                    if not vwap_slope_passed:
                        reasons['VWAP slope < 45°'].append(f"{trade.stock_name} ({slope_angle:.2f}°)")
                else:
                    vwap_slope_passed = (slope_result == 'Yes')
                    print(f'    VWAP Slope: {"✅" if vwap_slope_passed else "❌"} {slope_result}')
                    if not vwap_slope_passed:
                        reasons['VWAP slope < 45°'].append(trade.stock_name)
            except Exception as e:
                print(f'    ⚠️ Error calculating VWAP slope: {e}')
                reasons['VWAP slope error'].append(trade.stock_name)
        else:
            print(f'    ⚠️ Missing VWAP data for slope calculation')
            print(f'      Previous Hour VWAP: {stock_vwap_prev if stock_vwap_prev else "N/A"}')
            print(f'      Previous Hour Time: {stock_vwap_prev_time.strftime("%H:%M:%S") if stock_vwap_prev_time else "N/A"}')
            print(f'      Current VWAP: {stock_vwap_current if stock_vwap_current else "N/A"}')
            reasons['Missing VWAP data'].append(trade.stock_name)
        print()
        
        # Check candle size
        print(f'  🕯️ Candle Size Check:')
        candle_size_status = trade.candle_size_status
        candle_size_ratio = trade.candle_size_ratio
        
        if candle_size_status:
            candle_size_passed = (candle_size_status == 'Pass')
            print(f'    Status: {candle_size_status}')
            if candle_size_ratio:
                print(f'    Ratio: {candle_size_ratio:.2f}x')
            print(f'    Candle Size: {"✅" if candle_size_passed else "❌"}')
            if not candle_size_passed:
                reasons['Candle size >= 7.5×'].append(f"{trade.stock_name} ({candle_size_ratio:.2f}x)" if candle_size_ratio else trade.stock_name)
        else:
            print(f'    ⚠️ Candle size status not available')
            reasons['Candle size not calculated'].append(trade.stock_name)
        print()
        
        # Check option data
        print(f'  💰 Option Data Check:')
        has_option_contract = bool(trade.option_contract)
        has_instrument_key = bool(trade.instrument_key)
        has_option_ltp = bool(trade.option_ltp and trade.option_ltp > 0)
        has_lot_size = bool(trade.qty and trade.qty > 0)
        
        print(f'    Option Contract: {"✅" if has_option_contract else "❌"} {trade.option_contract or "N/A"}')
        print(f'    Instrument Key: {"✅" if has_instrument_key else "❌"} {trade.instrument_key or "N/A"}')
        print(f'    Option LTP: {"✅" if has_option_ltp else "❌"} {trade.option_ltp if trade.option_ltp else "N/A"}')
        print(f'    Lot Size/Qty: {"✅" if has_lot_size else "❌"} {trade.qty if trade.qty else "N/A"}')
        
        if not has_option_contract or not has_instrument_key or not has_option_ltp or not has_lot_size:
            reasons['Missing option data'].append(trade.stock_name)
        print()
        
        print()
    
    # Summary
    print('=' * 80)
    print('SUMMARY OF "NO ENTRY" REASONS:')
    print('=' * 80)
    print()
    
    if reasons:
        for reason, stocks in reasons.items():
            print(f'{reason}:')
            for stock in stocks:
                print(f'  - {stock}')
            print()
    else:
        print('⚠️ Could not determine specific reasons')
    
    print()
    print('=' * 80)
    
    db.close()

if __name__ == "__main__":
    main()

