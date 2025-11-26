"""
Script to check VWAP slope calculation for LTF trade at 12:15 PM
Shows why it's showing 0 degrees and what values are used
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
import math

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Find LTF trade at 12:15 PM
    trade = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= today,
        IntradayStockOption.alert_time >= today.replace(hour=12, minute=15),
        IntradayStockOption.alert_time < today.replace(hour=12, minute=16),
        IntradayStockOption.stock_name == 'LTF'
    ).first()
    
    if not trade:
        print("❌ LTF trade at 12:15 PM not found")
        return
    
    print("=" * 80)
    print("VWAP SLOPE CALCULATION ANALYSIS FOR LTF (12:15 PM Trade Entry)")
    print("=" * 80)
    print()
    
    print("📊 DATABASE VALUES:")
    print(f"  Stock Name: {trade.stock_name}")
    print(f"  Alert Time: {trade.alert_time}")
    print(f"  Stock VWAP (Current): ₹{trade.stock_vwap}")
    print(f"  Stock VWAP Previous Hour: ₹{trade.stock_vwap_previous_hour}")
    print(f"  Stock VWAP Previous Hour Time: {trade.stock_vwap_previous_hour_time}")
    print(f"  Status: {trade.status}")
    print()
    
    # Check if previous hour time is invalid (1970-01-01)
    invalid_time = datetime(1970, 1, 1, 5, 30, 0)
    if trade.stock_vwap_previous_hour_time and trade.stock_vwap_previous_hour_time.date() == invalid_time.date():
        print("⚠️  PROBLEM DETECTED: Previous Hour Time is INVALID (1970-01-01)")
        print("   This is causing the VWAP slope to show 0 degrees!")
        print()
    
    # Calculate with stored (wrong) values
    print("=" * 80)
    print("CALCULATION WITH STORED VALUES (WRONG - Shows 0 degrees)")
    print("=" * 80)
    
    vwap1 = trade.stock_vwap_previous_hour
    time1 = trade.stock_vwap_previous_hour_time
    vwap2 = trade.stock_vwap
    time2 = trade.alert_time
    
    if time1.tzinfo is None:
        time1 = ist.localize(time1)
    elif time1.tzinfo != ist:
        time1 = time1.astimezone(ist)
    
    if time2.tzinfo is None:
        time2 = ist.localize(time2)
    elif time2.tzinfo != ist:
        time2 = time2.astimezone(ist)
    
    time_diff = time2 - time1
    time_diff_hours = time_diff.total_seconds() / 3600.0
    vwap_change = vwap2 - vwap1
    scaling_factor_per_hour = vwap1 * 0.002
    normalized_time = time_diff_hours * scaling_factor_per_hour
    
    print(f"VWAP1 (Previous Hour): ₹{vwap1:.2f}")
    print(f"Time1 (Previous Hour Time): {time1.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"VWAP2 (Current): ₹{vwap2:.2f}")
    print(f"Time2 (Alert Time): {time2.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()
    print(f"Time Difference: {time_diff_hours:.2f} hours ({time_diff.days} days)")
    print(f"Price Change: ₹{vwap_change:.2f}")
    print(f"Scaling Factor (0.2% of ₹{vwap1:.2f}): ₹{scaling_factor_per_hour:.4f} per hour")
    print(f"Normalized Time: ₹{normalized_time:.4f}")
    
    if normalized_time > 0:
        slope_ratio = abs(vwap_change) / normalized_time
        angle_radians = math.atan(slope_ratio)
        angle_degrees = math.degrees(angle_radians)
        print(f"Slope Ratio: {slope_ratio:.6f}")
        print(f"Angle: {angle_degrees:.2f} degrees")
        print()
        print(f"❌ RESULT: {angle_degrees:.2f}° (WRONG - due to invalid time1)")
    else:
        print("Normalized time is 0 or negative - angle cannot be calculated")
    
    print()
    print("=" * 80)
    print("CALCULATION WITH CORRECT VALUES (Should be ~85 degrees)")
    print("=" * 80)
    
    # Correct previous hour time should be 11:15 AM
    time1_correct = ist.localize(datetime(2025, 11, 26, 11, 15, 0))
    time_diff_correct = time2 - time1_correct
    time_diff_hours_correct = time_diff_correct.total_seconds() / 3600.0
    normalized_time_correct = time_diff_hours_correct * scaling_factor_per_hour
    
    print(f"VWAP1 (Previous Hour): ₹{vwap1:.2f}")
    print(f"Time1 (Correct Previous Hour Time): {time1_correct.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"VWAP2 (Current): ₹{vwap2:.2f}")
    print(f"Time2 (Alert Time): {time2.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print()
    print(f"Time Difference: {time_diff_hours_correct:.2f} hours")
    print(f"Price Change: ₹{vwap_change:.2f}")
    print(f"Scaling Factor: ₹{scaling_factor_per_hour:.4f} per hour")
    print(f"Normalized Time: ₹{normalized_time_correct:.4f}")
    
    if normalized_time_correct > 0:
        slope_ratio_correct = abs(vwap_change) / normalized_time_correct
        angle_degrees_correct = math.degrees(math.atan(slope_ratio_correct))
        direction = "upward" if vwap_change > 0 else "downward" if vwap_change < 0 else "flat"
        print(f"Slope Ratio: {slope_ratio_correct:.6f}")
        print(f"Angle: {angle_degrees_correct:.2f} degrees ({direction})")
        print()
        print(f"✅ CORRECT RESULT: {angle_degrees_correct:.2f}° {direction} (>= 45° threshold)")
    
    print()
    print("=" * 80)
    print("RECALCULATION AT 13:15 PM")
    print("=" * 80)
    print()
    print("The scheduler runs `update_vwap_for_all_open_positions()` at 13:15 PM.")
    print("This function:")
    print("  1. Re-evaluates 'no_entry' trades")
    print("  2. Updates VWAP for all open positions")
    print()
    print("However, it does NOT update `stock_vwap_previous_hour_time` if it's already stored.")
    print("The function only fetches previous hour VWAP if it's missing:")
    print("  ```python")
    print("  if not stock_vwap_prev or not stock_vwap_prev_time:")
    print("      prev_vwap_data = vwap_service.get_stock_vwap_for_previous_hour(stock_name)")
    print("  ```")
    print()
    print("Since `stock_vwap_previous_hour` (295.58) is already stored, it won't be updated.")
    print("The invalid `stock_vwap_previous_hour_time` (1970-01-01) will remain.")
    print()
    print("SOLUTION: Need to fix the invalid timestamp in the database.")
    
    db.close()

if __name__ == "__main__":
    main()

