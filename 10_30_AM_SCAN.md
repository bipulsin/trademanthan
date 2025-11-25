# 10:30 AM Special Scan for 10:15 AM Alert Stocks

## Overview
A special scheduled scan runs at **10:30 AM IST** to capture market data for all stocks that were alerted in the **10:15 AM webhook**. This provides a 15-minute price movement snapshot after the initial alert.

## Schedule
- **Time**: 10:30 AM IST (daily)
- **Trigger**: Automatic via APScheduler CronTrigger
- **Function**: `update_10_15_alert_stocks_at_10_30()`

## What It Does

### 1. Query Stocks from 10:15 AM Alert
- Queries database for all stocks with `alert_time` at exactly **10:15 AM** today
- Includes both entered trades and "no_entry" stocks
- Matches stocks regardless of their current status

### 2. Fetch Market Data
For each stock found, the system fetches:
- **Stock LTP** - Current Last Traded Price
- **Stock VWAP** - Current Volume Weighted Average Price  
- **Option LTP** - Current Option Last Traded Price (if instrument_key is available)

### 3. Save to Historical Table
All data is saved to `historical_market_data` table with:
- Stock name
- Stock VWAP
- Stock LTP
- Option contract name
- Option instrument key
- Option LTP
- Scan date/time (10:30 AM)
- Scan time (human-readable format)

## Implementation Details

### Function Location
`backend/services/vwap_updater.py` - `update_10_15_alert_stocks_at_10_30()`

### Scheduler Configuration
```python
self.scheduler.add_job(
    update_10_15_alert_stocks_at_10_30,
    trigger=CronTrigger(hour=10, minute=30, timezone='Asia/Kolkata'),
    id='update_10_15_stocks_10_30',
    name='Update 10:15 AM Alert Stocks at 10:30 AM',
    replace_existing=True
)
```

### Query Logic
```python
# Target alert time: 10:15 AM
target_alert_time = today.replace(hour=10, minute=15, second=0, microsecond=0)

# Query with 1-minute window to account for timezone differences
alert_time_start = target_alert_time
alert_time_end = target_alert_time + timedelta(minutes=1)

stocks_from_10_15 = db.query(IntradayStockOption).filter(
    and_(
        IntradayStockOption.trade_date >= today,
        IntradayStockOption.alert_time >= alert_time_start,
        IntradayStockOption.alert_time < alert_time_end
    )
).all()
```

## Benefits

1. **Early Price Movement Tracking**: Captures price changes 15 minutes after initial alert
2. **Historical Analysis**: Provides data point for analyzing early-morning price movements
3. **Pattern Recognition**: Helps identify which stocks move quickly after alerts
4. **Performance Metrics**: Compare 10:15 AM vs 10:30 AM prices to measure initial momentum

## Example Use Cases

### Query 10:30 AM Data for 10:15 AM Stocks
```python
from backend.database import SessionLocal
from backend.models.trading import HistoricalMarketData
from datetime import datetime
import pytz

db = SessionLocal()
ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist).date()
scan_time = today.replace(hour=10, minute=30, second=0)

# Get 10:30 AM scan data
data_10_30 = db.query(HistoricalMarketData).filter(
    HistoricalMarketData.scan_date >= scan_time,
    HistoricalMarketData.scan_date < scan_time.replace(minute=31)
).all()

for record in data_10_30:
    print(f"{record.stock_name}: LTP={record.stock_ltp}, VWAP={record.stock_vwap}")
```

### Compare 10:15 AM vs 10:30 AM Prices
```python
from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption, HistoricalMarketData
from datetime import datetime
import pytz

db = SessionLocal()
ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist).date()

# Get stocks from 10:15 AM alert
stocks_10_15 = db.query(IntradayStockOption).filter(
    IntradayStockOption.alert_time >= today.replace(hour=10, minute=15),
    IntradayStockOption.alert_time < today.replace(hour=10, minute=16)
).all()

# Get 10:30 AM historical data
scan_10_30 = today.replace(hour=10, minute=30)
historical_10_30 = db.query(HistoricalMarketData).filter(
    HistoricalMarketData.scan_date >= scan_10_30,
    HistoricalMarketData.scan_date < scan_10_30.replace(minute=31)
).all()

# Compare prices
for stock in stocks_10_15:
    alert_price = stock.stock_ltp or stock.option_ltp
    hist_record = next((h for h in historical_10_30 if h.stock_name == stock.stock_name), None)
    
    if hist_record and alert_price:
        price_10_30 = hist_record.stock_ltp or hist_record.option_ltp
        if price_10_30:
            change = price_10_30 - alert_price
            change_pct = (change / alert_price) * 100
            print(f"{stock.stock_name}: {alert_price:.2f} → {price_10_30:.2f} ({change_pct:+.2f}%)")
```

## Logging

The function logs:
- Number of stocks found from 10:15 AM alert
- Processing status for each stock
- Success/failure counts
- Any errors encountered

Example log output:
```
📊 Starting 10:30 AM scan for 10:15 AM alert stocks at 2025-11-25 10:30:00 IST
📋 Found 5 stocks from 10:15 AM alert
📊 Processing RELIANCE (from 10:15 AM alert)
   Stock LTP: ₹2500.50
   Stock VWAP: ₹2498.75
   Option LTP: ₹125.50
   ✅ Saved historical data for RELIANCE at 10:30 AM
📊 10:30 AM Scan Complete: 5 stocks saved, 0 failed
```

## Notes

- Runs automatically every trading day at 10:30 AM IST
- Only processes stocks from the 10:15 AM webhook alert
- Saves data even if option LTP is not available (stock data is still valuable)
- Historical records are permanent and can be queried anytime
- Works for both entered trades and "no_entry" stocks

