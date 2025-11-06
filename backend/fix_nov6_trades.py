"""
Fix November 6, 2025 trades based on correct index trend logic

Based on user's analysis:
- 10:15 AM: Both indices BEARISH → Bullish alerts = NO TRADE
- 11:15 AM: Both indices BULLISH → Bearish alerts = NO TRADE
- 12:15 PM: Both indices BEARISH → Bullish alerts = NO TRADE
- 1:15 PM: Both indices BULLISH → Bearish alerts = NO TRADE
- 2:15 PM: NIFTY BEARISH, BANKNIFTY BULLISH (Mixed) → Both alerts = NO TRADE
"""

import sys
import os
from datetime import datetime
import pytz

sys.path.insert(0, '/home/ubuntu/trademanthan/backend')

from database import SessionLocal
from models.trading import IntradayStockOption
from sqlalchemy import and_

# Historical index trends for November 6, 2025 (provided by user)
INDEX_TRENDS_NOV6 = {
    "10:15": {"nifty": "bearish", "banknifty": "bearish"},
    "11:15": {"nifty": "bullish", "banknifty": "bullish"},
    "12:15": {"nifty": "bearish", "banknifty": "bearish"},
    "13:15": {"nifty": "bullish", "banknifty": "bullish"},  # 1:15 PM
    "14:15": {"nifty": "bearish", "banknifty": "bullish"},  # 2:15 PM (mixed)
}

def should_enter_trade(alert_type: str, alert_hour: int, alert_minute: int) -> tuple[bool, str]:
    """
    Determine if trade should be entered based on alert type and index trends
    
    Returns:
        (can_enter, reason)
    """
    time_key = f"{alert_hour:02d}:{alert_minute:02d}"
    
    if time_key not in INDEX_TRENDS_NOV6:
        return (False, f"No index data for {time_key}")
    
    trends = INDEX_TRENDS_NOV6[time_key]
    nifty = trends["nifty"]
    banknifty = trends["banknifty"]
    
    # Bullish alert: Both indices must be bullish
    if alert_type == "Bullish":
        if nifty == "bullish" and banknifty == "bullish":
            return (True, f"{time_key}: Both indices bullish - TRADE ALLOWED")
        else:
            return (False, f"{time_key}: NIFTY {nifty}, BANKNIFTY {banknifty} - NO TRADE")
    
    # Bearish alert: Both indices must be bearish
    elif alert_type == "Bearish":
        if nifty == "bearish" and banknifty == "bearish":
            return (True, f"{time_key}: Both indices bearish - TRADE ALLOWED")
        else:
            return (False, f"{time_key}: NIFTY {nifty}, BANKNIFTY {banknifty} - NO TRADE")
    
    return (False, "Unknown alert type")


def fix_nov6_trades():
    """Apply correct index trend logic to November 6, 2025 trades"""
    
    db = SessionLocal()
    try:
        ist = pytz.timezone('Asia/Kolkata')
        nov6_start = datetime(2025, 11, 6, 0, 0, 0, tzinfo=ist)
        nov6_end = datetime(2025, 11, 7, 0, 0, 0, tzinfo=ist)
        
        print("=" * 80)
        print("🔧 Fixing November 6, 2025 trades based on index trend logic")
        print("=" * 80)
        print("")
        
        # Get all positions from Nov 6th
        all_positions = db.query(IntradayStockOption).filter(
            and_(
                IntradayStockOption.trade_date >= nov6_start,
                IntradayStockOption.trade_date < nov6_end
            )
        ).all()
        
        if not all_positions:
            print("No positions found for November 6, 2025")
            return
        
        print(f"Found {len(all_positions)} positions to analyze")
        print("")
        
        corrected_count = 0
        unchanged_count = 0
        
        for pos in all_positions:
            alert_time = pos.alert_time
            alert_hour = alert_time.hour
            alert_minute = alert_time.minute
            alert_type = pos.alert_type  # 'Bullish' or 'Bearish'
            stock_name = pos.stock_name
            
            # Determine if this trade should have been entered
            should_enter, reason = should_enter_trade(alert_type, alert_hour, alert_minute)
            
            # Check current status
            is_currently_no_trade = (pos.qty == 0 or pos.qty is None or 
                                     pos.buy_price is None or pos.buy_price == 0)
            
            if should_enter and is_currently_no_trade:
                # SHOULD have entered but marked as no trade - INCORRECT
                # This shouldn't happen if we had proper logic, but keep as is
                print(f"⚠️  {stock_name} ({alert_hour:02d}:{alert_minute:02d} {alert_type}): {reason}")
                print(f"    Currently NO TRADE but SHOULD HAVE ENTERED - Keeping as is (data may be correct)")
                unchanged_count += 1
                
            elif not should_enter and not is_currently_no_trade:
                # Should NOT have entered but was marked as trade - INCORRECT, FIX IT
                print(f"🔧 {stock_name} ({alert_hour:02d}:{alert_minute:02d} {alert_type}): {reason}")
                print(f"    Was ENTERED but should be NO TRADE - CORRECTING NOW")
                
                # Mark as no_entry
                pos.status = 'no_entry'
                pos.qty = 0
                pos.buy_price = None
                pos.buy_time = None
                pos.sell_price = None
                pos.stop_loss = None
                pos.pnl = None
                pos.exit_reason = None
                
                corrected_count += 1
                
            elif not should_enter and is_currently_no_trade:
                # Correctly marked as no trade
                print(f"✅ {stock_name} ({alert_hour:02d}:{alert_minute:02d} {alert_type}): {reason}")
                print(f"    Correctly marked as NO TRADE")
                unchanged_count += 1
                
            else:
                # should_enter and not is_currently_no_trade
                # Correctly entered trade
                buy_price_display = f"₹{pos.buy_price:.2f}" if pos.buy_price else "₹0"
                print(f"✅ {stock_name} ({alert_hour:02d}:{alert_minute:02d} {alert_type}): {reason}")
                print(f"    Correctly ENTERED (Buy: {buy_price_display}, Qty: {pos.qty})")
                unchanged_count += 1
        
        # Commit all corrections
        if corrected_count > 0:
            db.commit()
            print("")
            print("=" * 80)
            print(f"✅ CORRECTIONS APPLIED:")
            print(f"   - Corrected: {corrected_count} trades")
            print(f"   - Unchanged: {unchanged_count} trades")
            print(f"   - Total: {len(all_positions)} trades")
            print("=" * 80)
        else:
            print("")
            print("=" * 80)
            print(f"✅ NO CORRECTIONS NEEDED - All {len(all_positions)} trades are correctly marked")
            print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    fix_nov6_trades()

