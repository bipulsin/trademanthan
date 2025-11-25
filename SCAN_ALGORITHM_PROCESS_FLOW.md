# Scan Algorithm Process Flow

## Overview
This document details the complete process flow for the Chartink Scan Algorithm, from webhook reception to trade entry, monitoring, and exit.

---

## Phase 1: Webhook Reception

### 1.1 Webhook Endpoints
The system has two dedicated endpoints for receiving alerts:

- **`POST /scan/chartink-webhook-bullish`** - Receives bullish alerts (CALL options)
- **`POST /scan/chartink-webhook-bearish`** - Receives bearish alerts (PUT options)

### 1.2 Webhook Processing Flow

```
┌─────────────────────────────────┐
│ Chartink sends webhook alert    │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ FastAPI receives webhook        │
│ - Parse JSON payload            │
│ - Log full payload for debugging│
│ - Respond immediately (202)     │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ Queue for background processing │
│ (BackgroundTasks)               │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│ process_webhook_data()          │
│ (Async background task)         │
└─────────────────────────────────┘
```

### 1.3 Webhook Data Format
Expected JSON payload from Chartink:
```json
{
    "stocks": "STOCK1,STOCK2,STOCK3",
    "trigger_prices": "100.5,200.75,300.25",
    "triggered_at": "10:15 AM",
    "scan_name": "Bullish Breakout",
    "scan_url": "bullish-breakout",
    "alert_name": "Alert for Bullish Breakout"
}
```

### 1.4 Time Normalization
- Parses `triggered_at` time from webhook
- Maps to standard Chartink schedule times:
  - **10:15 AM** (if time < 10:15)
  - **11:15 AM** (if time < 11:15)
  - **12:15 PM** (if time < 12:15)
  - **1:15 PM** (if time < 13:15)
  - **2:15 PM** (if time < 14:15)
  - **3:15 PM** (if time >= 14:15)

---

## Phase 2: Data Processing & Enrichment

### 2.1 Stock Parsing
For each stock in the webhook:

1. **Parse stock name** from comma-separated list
2. **Parse trigger price** from comma-separated list
3. **Determine alert type**:
   - Bullish webhook → `option_type = 'CE'` (CALL)
   - Bearish webhook → `option_type = 'PE'` (PUT)

### 2.2 Stock Data Enrichment

For each stock, the system fetches:

#### **2.2.1 Current Stock Data**
- **Stock LTP** (Last Traded Price)
- **Stock VWAP** (Volume Weighted Average Price) - Current hour

#### **2.2.2 Previous Hour Stock Data**
- **Stock VWAP** (Previous 1-hour candle)
- **Stock VWAP Time** (Timestamp of previous hour)

#### **2.2.3 Option Contract Selection**

**Step 1: Determine Expiry Month & Year**
Before selecting the option contract, the system first determines the target expiry month and year based on the current date:

- **If current day > 17**: Use **next month's expiry**
  - Example: If today is November 18, use December expiry
  - Special case: If current month is December (month 12), use January of next year
- **If current day ≤ 17**: Use **current month's expiry**
  - Example: If today is November 15, use November expiry

**Logic:**
```python
if current_day > 17:
    if current_month == 12:
        target_expiry_month = 1
        target_expiry_year = current_year + 1
    else:
        target_expiry_month = current_month + 1
        target_expiry_year = current_year
else:
    target_expiry_month = current_month
    target_expiry_year = current_year
```

**Step 2: Find Strike Price**
The system finds the best option strike using this logic:

1. **Get Option Chain** from Upstox API
2. **Identify OTM Strikes**:
   - For **CE**: Strikes > Stock LTP
   - For **PE**: Strikes < Stock LTP
3. **Sort by Distance** from LTP to identify OTM-1, OTM-2, ..., OTM-5
4. **Select Strike** with **HIGHEST volume × OI** among OTM-1 to OTM-5

**Important**: If option chain is **not available** or **strike cannot be determined**, the function returns `None` and the trade is marked as **`no_entry`**. No fallback strike calculation is performed.

**Step 3: Query MasterStock Table**
With the determined expiry month/year and strike price, query the `master_stock` table:

1. **Filter Criteria**:
   - `underlying_symbol` = Stock name
   - `option_type` = CE or PE
   - `strike_price` = Selected strike
   - `expiry_flag` = 'M' (Monthly expiry)
   - `sm_expiry_date` year = Target expiry year
   - `sm_expiry_date` month = Target expiry month

2. **Exact Match**: If exact strike found → Return option contract

3. **Closest Match**: If exact strike not found:
   - For **CE**: Find closest strike >= target_strike
   - For **PE**: Find closest strike <= target_strike
   - If no match in direction, get highest/lowest available strike for target expiry

#### **2.2.4 Option Data Fetching**
- **Option Contract Name** (e.g., "RELIANCE-Dec2025-2500-CE")
- **Option LTP** (Last Traded Price)
- **Option VWAP** (Volume Weighted Average Price)
- **Instrument Key** (for future API calls)
- **Lot Size** (from MasterStock table)

#### **2.2.5 Option OHLC Candles**
- **Current 1-hour Candle**:
  - Open, High, Low, Close
  - Timestamp
- **Previous 1-hour Candle**:
  - Open, High, Low, Close
  - Timestamp

### 2.3 Index Trend Check
Fetches current trend for both indices:
- **NIFTY50** trend (bullish/bearish)
- **BANKNIFTY** trend (bullish/bearish)

---

## Phase 3: Trade Entry Decision

### 3.1 Entry Conditions (ALL must be met)

#### **3.1.1 Time Check**
- ✅ Alert time must be **before 3:00 PM IST**
- ❌ Alerts at or after 3:00 PM are rejected (no new trades allowed)

#### **3.1.2 Index Trend Check**
Rules based on NIFTY and BANKNIFTY trends:

**For Bullish Alerts (CE options):**
- ✅ **Both indices Bullish** → Can enter
- ❌ **Both indices Bearish** → Cannot enter
- ❌ **Opposite directions** → Cannot enter

**For Bearish Alerts (PE options):**
- ✅ **Both indices Bullish** → Can enter
- ✅ **Both indices Bearish** → Can enter
- ❌ **Opposite directions** → Cannot enter

#### **3.1.3 VWAP Slope Filter**
- **Requirement**: VWAP slope must be **≥ 45 degrees**
- **Calculation**: Uses `vwap_slope()` method:
  - Input: Previous hour VWAP + time, Current hour VWAP + time
  - Method calculates angle using price-range normalized scaling
  - Returns: **"Yes"** if angle >= 45°, **"No"** otherwise
- **Both Directions**: Both upward and downward slopes are considered

#### **3.1.4 Candle Size Filter**
- **Requirement**: Current option candle size must be **< 7.5×** previous candle size
- **Calculation**:
  - Current candle size = `High - Low` of current 1-hour candle
  - Previous candle size = `High - Low` of previous 1-hour candle
  - Ratio = `current_size / previous_size`
- **Passes if**: Ratio < 7.5 (threshold is middle of 7-8 range)

#### **3.1.5 Option Data Validation**
- ✅ Option LTP must be > 0
- ✅ Lot size must be > 0
- ✅ Option contract must be found
- ✅ Instrument key must be available

### 3.2 Trade Entry Result

#### **✅ ENTRY (Status: `bought`)**
If ALL conditions met:

**At Entry Moment:**
1. **Fetch Fresh Option LTP**: Option LTP is fetched again at the exact moment of entry (not from enrichment phase)
2. **Set Buy Price**: `buy_price` = Current option LTP (fetched at entry moment)
3. **Set Buy Time**: `buy_time` = Current system time (IST), **NOT** alert time
4. **Set Stop Loss**: `stop_loss` = Low price of **previous option candle** (not calculated)
   - If previous candle low not available, defaults to ₹0.05
5. **Set Quantity**: `qty` = Lot size
6. **Set Status**: `status` = `'bought'`
7. **Initialize P&L**: `pnl` = 0.0
8. **Sell Price**: `sell_price` = NULL (will be updated hourly)

**Stored Data:**
- `stock_vwap_previous_hour` = Previous hour VWAP
- `stock_vwap_previous_hour_time` = Previous hour VWAP timestamp
- `option_current_candle_*` = Current option OHLC data
- `option_previous_candle_*` = Previous option OHLC data

#### **❌ NO ENTRY (Status: `no_entry`)**
If ANY condition fails:
- `buy_price` = Option LTP at alert time (for reference)
- `buy_time` = NULL (trade not executed)
- `qty` = Lot size (for reference)
- `stop_loss` = Previous option candle low (for reference, same as entry logic)
- `status` = `'no_entry'`
- `pnl` = NULL
- `sell_price` = NULL
- All enrichment data stored for later re-evaluation

**Reasons for No Entry**:
- Time >= 3:00 PM
- Index trends not aligned
- VWAP slope < 45 degrees
- Candle size >= 7.5× previous candle
- Missing option data

### 3.3 Database Storage
All trade records (both `bought` and `no_entry`) are stored in `intraday_stock_options` table with:
- Alert metadata (scan_name, alert_name, triggered_at)
- Stock data (LTP, VWAP, previous hour VWAP)
- Option data (contract, LTP, VWAP, OHLC candles)
- Trade status and entry criteria results

---

## Phase 4: Hourly Market Data Updates

### 4.1 Update Schedule
Runs every hour at **:15 minutes** (e.g., 9:15, 10:15, 11:15, etc.) during market hours (9:30 AM - 3:30 PM IST)

### 4.2 Update Process

#### **4.2.1 Re-evaluate "no_entry" Trades**
For trades with `status = 'no_entry'`:

1. **Fetch Current Data**:
   - Current stock LTP and VWAP
   - Previous hour VWAP (if not stored)
   - Current and previous option OHLC candles

2. **Re-check Entry Conditions**:
   - Time < 3:00 PM?
   - Index trends aligned?
   - VWAP slope >= 45 degrees?
   - Candle size < 7.5× previous?
   - Option data available?

3. **If Conditions Met**:
   - Change `status` from `'no_entry'` to `'bought'`
   - **Fetch Fresh Option LTP**: Option LTP is fetched again at the exact moment of entry
   - Set `buy_price` = Current option LTP (fetched at entry moment)
   - Set `buy_time` = Current system time (IST), **NOT** alert time
   - Set `stop_loss` = Low price of **previous option candle** (not calculated)
   - Set `pnl` = 0.0
   - **Trade is now active!**

#### **4.2.2 Update Open Positions**
For trades with `status = 'bought'` and `exit_reason = NULL`:

1. **Fetch Fresh Market Data**:
   - Stock VWAP (current)
   - Stock LTP (current)
   - Option LTP (current) - using stored `instrument_key`

2. **Update Database**:
   - `stock_vwap` = New VWAP
   - `stock_ltp` = New stock LTP
   - `sell_price` = New option LTP (for monitoring)
   - `pnl` = (sell_price - buy_price) × qty

3. **Save Historical Snapshot**:
   - Saves to `historical_market_data` table
   - Includes: stock_name, stock_vwap, stock_ltp, option_contract, option_ltp, scan_date, scan_time

#### **4.2.3 Check Exit Conditions**

**Priority Order** (highest to lowest):

1. **Stop Loss** (Highest Priority)
   - If `option_ltp <= stop_loss` → Exit immediately
   - `exit_reason` = `'stop_loss'`
   - `sell_price` = Current option LTP
   - `sell_time` = Current time
   - `status` = `'sold'`

2. **VWAP Cross** (After 11:15 AM)
   - For **CE**: If `stock_ltp < stock_vwap` → Exit
   - For **PE**: If `stock_ltp > stock_vwap` → Exit
   - `exit_reason` = `'stock_vwap_cross'`
   - `sell_price` = Current option LTP
   - `sell_time` = Current time
   - `status` = `'sold'`

3. **Profit Target** (1.5× buy price)
   - If `option_ltp >= (buy_price × 1.5)` → Exit
   - `exit_reason` = `'profit_target'`
   - `sell_price` = Current option LTP
   - `sell_time` = Current time
   - `status` = `'sold'`

4. **Time-Based Exit** (3:25 PM)
   - All open trades closed automatically
   - `exit_reason` = `'time_based'`
   - `sell_price` = Current option LTP (with retry logic)
   - `sell_time` = 3:25 PM
   - `status` = `'sold'`

---

## Phase 5: Special Scheduled Tasks

### 5.1 10:30 AM Scan
**Purpose**: Capture market data snapshot for stocks from 10:15 AM webhook alert

**Process**:
1. Query all trades from 10:15 AM alerts
2. Fetch current market data:
   - Stock LTP
   - Stock VWAP
   - Option LTP
3. Store in `historical_market_data` table

### 5.2 End of Day Processing

#### **5.2.1 Close All Trades (3:25 PM)**
- Closes ALL open positions (`status = 'bought'`)
- Uses retry logic (3 attempts) to fetch option LTP
- Fallback priority:
  1. Fetched current LTP
  2. Last known `sell_price` from hourly updates
  3. `buy_price` (last resort)
- `exit_reason` = `'time_based'`

#### **5.2.2 Final VWAP Update (3:30 PM & 3:35 PM)**
- Updates ALL positions (including exited ones) with final day VWAP
- Ensures complete market data for analysis

---

## Phase 6: Data Retrieval (Frontend)

### 6.1 API Endpoint
**`GET /scan/latest`**

### 6.2 Response Structure
Returns data grouped by alert time:

```json
{
    "status": "success",
    "data": {
        "bullish": {
            "date": "2025-11-25",
            "alerts": [
                {
                    "scan_name": "Bullish Breakout",
                    "alert_name": "Bullish Alert",
                    "triggered_at": "2025-11-25T10:15:00",
                    "stocks": [
                        {
                            "stock_name": "RELIANCE",
                            "stock_vwap": 2500.50,
                            "stock_vwap_previous_hour": 2495.25,
                            "option_contract": "RELIANCE-Dec2025-2500-CE",
                            "vwap_slope_status": "Yes",
                            "candle_size_status": "Pass",
                            "candle_size_ratio": 2.5,
                            "buy_price": 15.50,
                            "status": "bought",
                            "pnl": 250.00
                        }
                    ]
                }
            ]
        },
        "bearish": { ... },
        "index_check": {
            "nifty_trend": "bullish",
            "banknifty_trend": "bullish",
            "allow_trading": true
        }
    }
}
```

### 6.3 Frontend Display
- Shows all alerts grouped by time
- Displays entry criteria status (VWAP slope, candle size)
- Shows real-time P&L
- Color-coded status indicators

---

## Key Components

### Services
- **`upstox_service`**: Fetches market data (LTP, VWAP, OHLC, index trends)
- **`vwap_updater`**: Hourly updates and exit condition checks
- **`health_monitor`**: Tracks webhook success/failure

### Database Tables
- **`intraday_stock_options`**: Main trade records
- **`historical_market_data`**: Hourly snapshots
- **`master_stock`**: Option contract metadata

### Key Methods
- **`process_webhook_data()`**: Main webhook processing logic
- **`find_strike_from_option_chain()`**: Option strike selection
- **`find_option_contract_from_master_stock()`**: Option contract lookup with expiry determination
- **`vwap_slope()`**: VWAP slope calculation
- **`update_vwap_for_all_open_positions()`**: Hourly update logic
- **`close_all_open_trades()`**: End-of-day closure

### Key Entry Logic Details
- **Buy Price**: Always fetched fresh at entry moment using `instrument_key` via `get_market_quote_by_key()`
- **Buy Time**: Always set to current system time (`datetime.now(ist)`), not alert time
- **Stop Loss**: Always set to previous option candle low price, not calculated
- **Fallback**: If fresh LTP fetch fails, uses enrichment-phase LTP; if previous candle low unavailable, defaults to ₹0.05

---

## Error Handling

### Webhook Errors
- **Client Disconnect**: Logged, returns 499 status
- **Timeout**: Logged, returns 408 status
- **Processing Error**: Logged with full traceback, returns 500 status

### Data Fetch Errors
- **Missing Stock Data**: Trade marked as `no_entry`
- **Missing Option Data**: Trade marked as `no_entry`
- **API Failures**: Retry logic with fallbacks

### Database Errors
- **Connection Issues**: Logged, transaction rolled back
- **Constraint Violations**: Logged, duplicate entries skipped

---

## Performance Considerations

### Background Processing
- Webhooks processed asynchronously to prevent timeouts
- Immediate 202 response to Chartink
- Processing happens in background tasks

### Database Optimization
- Indexes on frequently queried columns
- Batch updates for hourly processing
- Efficient queries with proper filters

### API Rate Limiting
- Upstox API calls batched where possible
- Caching of option chain data
- Retry logic with exponential backoff

---

## Monitoring & Logging

### Log Levels
- **INFO**: Normal operations (webhook received, trade entered)
- **WARNING**: Non-critical issues (missing data, skipped trades)
- **ERROR**: Critical failures (API errors, database errors)

### Key Log Messages
- `📥 Received bullish/bearish webhook`
- `✅ TRADE ENTERED`
- `❌ NO ENTRY`
- `📊 Starting hourly market data update`
- `🛑 EXIT: Stop Loss hit`
- `📉 EXIT: VWAP Cross`

### Health Monitoring
- Webhook success/failure tracking
- API health checks
- Database connection monitoring

---

## Summary Flow Diagram

```
Webhook Reception
    ↓
Parse & Normalize Time
    ↓
For Each Stock:
    ├─ Fetch Stock LTP & VWAP (current)
    ├─ Fetch Stock VWAP (previous hour)
    ├─ Find Option Contract
    ├─ Fetch Option LTP & VWAP (enrichment phase)
    ├─ Fetch Option OHLC (current & previous)
    └─ Check Index Trends
    ↓
Entry Decision:
    ├─ Time Check (< 3:00 PM?)
    ├─ Index Trends Aligned?
    ├─ VWAP Slope >= 45°?
    ├─ Candle Size < 7.5×?
    └─ Option Data Valid?
    ↓
    ├─ YES → Enter Trade:
    │   ├─ Fetch Fresh Option LTP (at entry moment)
    │   ├─ Set buy_price = Current Option LTP
    │   ├─ Set buy_time = Current System Time
    │   ├─ Set stop_loss = Previous Candle Low
    │   └─ status='bought'
    └─ NO → Mark as No Entry (status='no_entry')
    ↓
Hourly Updates:
    ├─ Re-evaluate no_entry trades
    │   └─ If conditions met: Fetch fresh LTP, set buy_time=now, SL=prev candle low
    ├─ Update open positions
    └─ Check exit conditions
    ↓
Exit Conditions:
    ├─ Stop Loss (option_ltp <= stop_loss)
    ├─ VWAP Cross
    ├─ Profit Target (1.5× buy_price)
    └─ Time-Based (3:25 PM)
    ↓
End of Day:
    ├─ Close all trades (3:25 PM)
    └─ Final VWAP update (3:30 PM)
```

---

*Last Updated: November 25, 2025*

