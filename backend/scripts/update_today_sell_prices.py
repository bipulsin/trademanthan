#!/usr/bin/env python3
"""
Script to update sell_price for today's trades
Fetches current price from Upstox using instrument_key and sets sell_time to 3:25 PM
Only updates trades with status != 'no_entry'
"""

import sys
import os
from datetime import datetime
import pytz

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption

def update_today_sell_prices():
    """
    Update sell_price for today's trades using stored instrument_key
    Sets sell_time to 3:25 PM today
    """
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    
    try:
        # Get today's date range
        today = datetime.now(ist).date()
        today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=ist)
        today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=ist)
        
        # Set sell_time to 3:25 PM today
        sell_time = datetime.combine(today, datetime.strptime("15:25", "%H:%M").time()).replace(tzinfo=ist)
        
        print(f"📅 Target Date: {today.strftime('%Y-%m-%d')}")
        print(f"⏰ Sell Time: {sell_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print()
        
        # Import Upstox service (pre-initialized instance)
        try:
            from services.upstox_service import upstox_service as vwap_service
        except ImportError:
            print("❌ Could not import upstox_service")
            return False
        
        if not vwap_service:
            print("❌ ERROR: Upstox service not available")
            return False
        
        print("✅ Upstox service available")
        print()
        
        # Query all trades for today that are NOT 'no_entry' and have instrument_key
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today_start,
            IntradayStockOption.trade_date < today_end,
            IntradayStockOption.status != 'no_entry',
            IntradayStockOption.instrument_key.isnot(None),
            IntradayStockOption.instrument_key != ''
        ).all()
        
        total_count = len(records)
        print(f"📊 Found {total_count} records to update for today")
        print(f"   (Status != 'no_entry' and have instrument_key)")
        print()
        
        if total_count == 0:
            print("✅ No records to update")
            return True
        
        updated_count = 0
        failed_count = 0
        skipped_count = 0
        
        for record in records:
            stock_name = record.stock_name
            option_contract = record.option_contract
            instrument_key = record.instrument_key
            record_id = record.id
            current_sell_price = record.sell_price
            buy_price = record.buy_price
            status = record.status
            
            print(f"Processing ID {record_id}: {stock_name} - {option_contract}")
            print(f"   Status: {status}, Instrument Key: {instrument_key}")
            current_sell_display = current_sell_price if current_sell_price else 0.0
            buy_price_display = buy_price if buy_price else 0.0
            print(f"   Current Sell Price: ₹{current_sell_display:.2f}, Buy Price: ₹{buy_price_display:.2f}")
            
            try:
                # Fetch current LTP using instrument_key
                print(f"   Fetching LTP from Upstox API...")
                option_quote = vwap_service.get_market_quote_by_key(instrument_key)
                
                if not option_quote:
                    print(f"   ❌ No quote data returned from API")
                    failed_count += 1
                    continue
                
                if 'last_price' not in option_quote:
                    print(f"   ❌ No 'last_price' in quote data: {option_quote}")
                    failed_count += 1
                    continue
                
                new_ltp = option_quote['last_price']
                
                if not new_ltp or new_ltp <= 0:
                    print(f"   ⚠️  Invalid LTP: {new_ltp}")
                    failed_count += 1
                    continue
                
                print(f"   ✅ Fetched LTP: ₹{new_ltp:.2f}")
                
                # Sanity check: If LTP is >3x buy_price, it's likely wrong
                if buy_price and buy_price > 0:
                    ratio = new_ltp / buy_price
                    if ratio > 3.0:
                        print(f"   🚨 UNREALISTIC LTP DETECTED!")
                        print(f"      Buy Price: ₹{buy_price:.2f}")
                        print(f"      New LTP: ₹{new_ltp:.2f} ({ratio:.2f}x buy price)")
                        print(f"      ⚠️  Skipping update due to unrealistic value")
                        skipped_count += 1
                        continue
                
                # Update sell_price and sell_time
                old_sell_price = current_sell_price or 0.0
                record.sell_price = new_ltp
                record.sell_time = sell_time
                
                # Recalculate PnL if buy_price and qty are available
                if buy_price and record.qty:
                    record.pnl = (new_ltp - buy_price) * record.qty
                    print(f"   ✅ Updated: Sell Price: ₹{old_sell_price:.2f} → ₹{new_ltp:.2f}")
                    print(f"      Sell Time: {sell_time.strftime('%H:%M:%S')}")
                    print(f"      PnL: ₹{record.pnl:.2f}")
                else:
                    print(f"   ✅ Updated: Sell Price: ₹{old_sell_price:.2f} → ₹{new_ltp:.2f}")
                    print(f"      Sell Time: {sell_time.strftime('%H:%M:%S')}")
                    print(f"      (PnL not calculated - missing buy_price or qty)")
                
                updated_count += 1
                
            except Exception as e:
                print(f"   ❌ Error fetching/updating: {str(e)}")
                import traceback
                traceback.print_exc()
                failed_count += 1
            
            print()  # Empty line between records
        
        # Commit all updates
        if updated_count > 0:
            db.commit()
            print(f"✅ Successfully updated {updated_count} records")
        
        if skipped_count > 0:
            print(f"⚠️  Skipped {skipped_count} records (unrealistic LTP values)")
        
        if failed_count > 0:
            print(f"❌ Failed to update {failed_count} records")
        
        print()
        print(f"📊 Summary:")
        print(f"   Total records: {total_count}")
        print(f"   Updated: {updated_count}")
        print(f"   Skipped (unrealistic): {skipped_count}")
        print(f"   Failed: {failed_count}")
        
        return True
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error updating sell prices: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    print("=" * 70)
    print("Update Sell Prices for Today's Trades")
    print("=" * 70)
    print()
    
    success = update_today_sell_prices()
    
    if success:
        print()
        print("✅ Update completed")
        sys.exit(0)
    else:
        print()
        print("❌ Update failed")
        sys.exit(1)

