# Historical Market Data Storage

## Overview
The system now automatically captures and stores historical market data snapshots during hourly updates. This provides a complete historical record of stock VWAP, stock LTP, and option LTP for analysis and backtesting.

## Database Table: `historical_market_data`

### Schema
```sql
CREATE TABLE historical_market_data (
    id INTEGER PRIMARY KEY,
    stock_name VARCHAR(100) NOT NULL,
    stock_vwap FLOAT,
    stock_ltp FLOAT,
    option_contract VARCHAR(255),
    option_instrument_key VARCHAR(255),
    option_ltp FLOAT,
    scan_date TIMESTAMP NOT NULL,
    scan_time VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Fields Description

| Field | Type | Description |
|-------|------|-------------|
| `id` | Integer | Primary key (auto-increment) |
| `stock_name` | String(100) | Name of the underlying stock (indexed) |
| `stock_vwap` | Float | Volume Weighted Average Price of the stock |
| `stock_ltp` | Float | Last Traded Price of the stock |
| `option_contract` | String(255) | Option contract name (e.g., "RELIANCE-Nov2024-2500-CE") (indexed) |
| `option_instrument_key` | String(255) | Upstox instrument key (e.g., "NSE_FO\|104500") (indexed) |
| `option_ltp` | Float | Last Traded Price of the option contract |
| `scan_date` | DateTime | Date and time of the scan/update (indexed) |
| `scan_time` | String(20) | Human-readable time (e.g., "10:15 am") |
| `created_at` | DateTime | Record creation timestamp |

## When Data is Captured

Historical data is automatically saved during **hourly market data updates**:

- **9:15 AM** - First update
- **9:30 AM** - Market open update
- **10:15 AM** - Hourly update
- **11:15 AM** - Hourly update
- **12:15 PM** - Hourly update
- **1:15 PM** - Hourly update
- **2:15 PM** - Hourly update
- **3:15 PM** - Hourly update

## What Gets Saved

For each **open position** (status = 'bought'), the system saves:

1. **Stock Information**
   - Stock name
   - Current Stock VWAP
   - Current Stock LTP

2. **Option Information**
   - Option contract name
   - Option instrument key
   - Current Option LTP

3. **Timestamp**
   - Scan date/time (precise timestamp)
   - Scan time (human-readable format)

## Implementation Details

### Model Location
`backend/models/trading.py` - `HistoricalMarketData` class

### Save Logic Location
`backend/services/vwap_updater.py` - `update_vwap_for_all_open_positions()` function

### Code Flow
1. Hourly scheduler triggers `update_vwap_for_all_open_positions()`
2. For each open position:
   - Fetches fresh Stock VWAP
   - Fetches fresh Stock LTP
   - Fetches fresh Option LTP
   - Updates position record
   - **Saves historical snapshot** ← New functionality
3. Commits all changes (including historical records)

## Usage Examples

### Query Historical Data for a Stock
```python
from backend.database import SessionLocal
from backend.models.trading import HistoricalMarketData
from datetime import datetime, timedelta
import pytz

db = SessionLocal()
ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist).date()

# Get all historical data for a specific stock today
historical_data = db.query(HistoricalMarketData).filter(
    HistoricalMarketData.stock_name == 'RELIANCE',
    HistoricalMarketData.scan_date >= datetime.combine(today, datetime.min.time())
).order_by(HistoricalMarketData.scan_date).all()

for record in historical_data:
    print(f"{record.scan_time}: Stock LTP={record.stock_ltp}, VWAP={record.stock_vwap}, Option LTP={record.option_ltp}")
```

### Query Historical Data for an Option Contract
```python
# Get historical data for a specific option contract
historical_data = db.query(HistoricalMarketData).filter(
    HistoricalMarketData.option_contract == 'RELIANCE-Nov2024-2500-CE'
).order_by(HistoricalMarketData.scan_date).all()
```

### Analyze Price Movements Over Time
```python
# Get price movement for a stock throughout the day
historical_data = db.query(HistoricalMarketData).filter(
    HistoricalMarketData.stock_name == 'TATAMOTORS',
    HistoricalMarketData.scan_date >= datetime.combine(today, datetime.min.time())
).order_by(HistoricalMarketData.scan_date).all()

# Calculate price changes
if len(historical_data) > 1:
    first_price = historical_data[0].stock_ltp
    last_price = historical_data[-1].stock_ltp
    change = last_price - first_price
    change_pct = (change / first_price) * 100
    print(f"Price change: ₹{change:.2f} ({change_pct:.2f}%)")
```

## Benefits

1. **Historical Analysis**: Track how prices moved throughout the day
2. **Backtesting**: Use historical data to test trading strategies
3. **Performance Tracking**: Analyze which stocks/options performed best
4. **Pattern Recognition**: Identify patterns in price movements
5. **Debugging**: Review historical data to debug trading decisions

## Data Retention

- Historical data is stored permanently in the database
- No automatic deletion (can be manually cleaned if needed)
- Indexes on `stock_name`, `option_contract`, `option_instrument_key`, and `scan_date` for fast queries

## Notes

- Historical data is saved even if option LTP fetch fails (stock data is still valuable)
- Each hourly update creates a new record for each open position
- Records are linked to positions via `stock_name` and `option_contract`
- The `scan_date` field uses precise timestamps for accurate time-series analysis

