#!/usr/bin/env python3
"""
One-time script to update sell_price for all trades from 18-Nov-2025
Fetches current LTP using instrument_key and recalculates PnL
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption
from datetime import datetime
import pytz
from services.upstox_service import upstox_service as vwap_service

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    
    # Target date: 18-Nov-2025
    target_date = datetime(2025, 11, 18, 0, 0, 0).replace(tzinfo=ist)
    target_date_end = datetime(2025, 11, 19, 0, 0, 0).replace(tzinfo=ist)
    
    # Check Upstox service
    print("Checking Upstox service...")
    if not vwap_service:
        print("❌ ERROR: Upstox service not available")
        db.close()
        return
    print("✅ Upstox service available")
    
    # Get all trades from 18-Nov-2025 that have instrument_key
    trades = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= target_date,
        IntradayStockOption.trade_date < target_date_end,
        IntradayStockOption.instrument_key.isnot(None),
        IntradayStockOption.instrument_key != '',
        IntradayStockOption.buy_price.isnot(None),
        IntradayStockOption.buy_price > 0
    ).all()
    
    print(f"\n{'='*80}")
    print(f"Found {len(trades)} trades on 18-Nov-2025 with instrument_key")
    print(f"{'='*80}\n")
    
    updated_count = 0
    failed_count = 0
    
    for trade in trades:
        print(f"\n[{updated_count + failed_count + 1}/{len(trades)}] {trade.stock_name}: {trade.option_contract}")
        print(f"  Instrument Key: {trade.instrument_key}")
        print(f"  Buy Price: ₹{trade.buy_price}")
        print(f"  Current Sell Price: ₹{trade.sell_price if trade.sell_price else 'None'}")
        print(f"  Current PnL: ₹{trade.pnl if trade.pnl else 'None'}")
        
        # Fetch current LTP using instrument_key
        try:
            quote_data = vwap_service.get_market_quote_by_key(trade.instrument_key)
            
            if quote_data and quote_data.get('last_price'):
                new_sell_price = float(quote_data.get('last_price', 0))
                
                if new_sell_price > 0:
                    # Update sell_price
                    trade.sell_price = new_sell_price
                    
                    # Recalculate PnL
                    if trade.buy_price and trade.qty:
                        trade.pnl = (new_sell_price - trade.buy_price) * trade.qty
                    else:
                        print(f"  ⚠️  Warning: Cannot calculate PnL (buy_price or qty missing)")
                        trade.pnl = None
                    
                    updated_count += 1
                    print(f"  ✅ Updated Sell Price: ₹{new_sell_price:.2f}")
                    print(f"  ✅ Updated PnL: ₹{trade.pnl:.2f}" if trade.pnl else "  ✅ PnL: None")
                else:
                    failed_count += 1
                    print(f"  ❌ Invalid LTP received: {new_sell_price}")
            else:
                failed_count += 1
                print(f"  ❌ No quote data received for instrument_key {trade.instrument_key}")
                
        except Exception as e:
            failed_count += 1
            print(f"  ❌ Error fetching LTP: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Commit changes
    if updated_count > 0:
        print(f"\n{'='*80}")
        print(f"Committing {updated_count} updates to database...")
        db.commit()
        print(f"✅ Successfully updated {updated_count} trades")
    else:
        print(f"\n{'='*80}")
        print(f"No updates made")
    
    if failed_count > 0:
        print(f"⚠️  Failed to update {failed_count} trades")
    
    print(f"{'='*80}\n")
    
    db.close()

if __name__ == '__main__':
    main()

