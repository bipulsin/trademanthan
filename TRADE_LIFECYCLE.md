# Trade Lifecycle: How Stocks from Webhook Alerts are Handled Throughout Market Duration

This document explains the complete lifecycle of a trade from the moment a stock alert arrives via webhook until the trade is closed.

---

## 📥 **Phase 1: Webhook Alert Reception**

### **Entry Point**
- **Endpoint**: `/scan/chartink-webhook-bullish` or `/scan/chartink-webhook-bearish`
- **File**: `backend/routers/scan.py`
- **Function**: `receive_bullish_webhook()` / `receive_bearish_webhook()`

### **What Happens**
1. **Immediate Response**: Backend responds with HTTP 202 (Accepted) to prevent webhook timeout
2. **Background Processing**: Webhook data is queued for background processing using FastAPI `BackgroundTasks`
3. **Payload Logging**: Full webhook payload is logged for debugging

### **Webhook Data Format**
```json
{
  "stocks": "STOCK1,STOCK2,STOCK3",
  "trigger_prices": "100.5,200.75,50.25",
  "triggered_at": "10:15 am",
  "scan_name": "Bullish Breakout",
  "scan_url": "bullish-breakout",
  "alert_name": "Alert for Bullish Breakout"
}
```

---

## 🔍 **Phase 2: Webhook Data Processing**

### **Function**: `process_webhook_data()` in `backend/routers/scan.py`

### **Step-by-Step Processing**

#### **2.1 Data Parsing**
- Parses stock names and trigger prices from webhook payload
- Determines trading date (today if trading day, else last trading date)
- Parses alert time (`triggered_at`)

#### **2.2 Stock Enrichment**
For each stock in the alert:

1. **Fetch Stock Data**:
   - Stock LTP (Last Traded Price)
   - Stock VWAP (Volume Weighted Average Price)
   - Option contract details (strike, expiry, type)

2. **Find Option Contract**:
   - Searches for OTM-1 to OTM-5 strikes
   - Selects strike with highest volume × OI
   - Determines lot size and option type (CE for bullish, PE for bearish)

3. **Fetch Option Data**:
   - Option LTP (Last Traded Price)
   - Instrument key (for future API calls)
   - Option VWAP

#### **2.3 Stock Ranking (if needed)**
- If more than 15 stocks in alert, applies ranking algorithm
- Selects top 15 stocks based on momentum, volume, and other factors
- Rejected stocks are still saved but marked as `no_entry`

---

## ✅ **Phase 3: Trade Entry Decision**

### **Entry Conditions (ALL must be met)**

#### **3.1 Time Check**
- ✅ Alert time must be **before 3:00 PM IST**
- ❌ Alerts at or after 3:00 PM are rejected (no new trades allowed)

#### **3.2 Index Trend Check**
The system checks NIFTY 50 and BANKNIFTY trends:

**Rules**:
- **Both indices Bullish** → ✅ Both bullish AND bearish alerts can enter
- **Both indices Bearish** → ✅ Only bearish alerts can enter
- **Indices in opposite directions** → ❌ No trades allowed

**Logic**:
```python
both_bullish = (nifty_trend == "bullish" and banknifty_trend == "bullish")
both_bearish = (nifty_trend == "bearish" and banknifty_trend == "bearish")
opposite_directions = not both_bullish and not both_bearish

# Bullish alerts
if both_bullish: ✅ Can enter
elif both_bearish: ❌ Cannot enter
elif opposite_directions: ❌ Cannot enter

# Bearish alerts
if both_bullish: ✅ Can enter
elif both_bearish: ✅ Can enter
elif opposite_directions: ❌ Cannot enter
```

#### **3.3 VWAP Slope Filter**
- **Requirement**: VWAP slope must be **≥ 45 degrees**
- **Calculation**: Uses `vwap_slope()` method with:
  - Previous hour VWAP and time
  - Current hour VWAP and time
- **Returns**: "Yes" if angle >= 45°, "No" otherwise
- **Both Directions**: Both upward and downward slopes are considered

#### **3.4 Candle Size Filter**
- **Requirement**: Current option candle size must be **< 7.5×** previous candle size
- **Calculation**: 
  - Current candle size = `High - Low` of current 1-hour candle
  - Previous candle size = `High - Low` of previous 1-hour candle
  - Ratio = `current_size / previous_size`
- **Passes if**: Ratio < 7.5 (threshold is middle of 7-8 range)

#### **3.5 Option Data Validation**
- ✅ Option LTP must be > 0
- ✅ Lot size must be > 0
- ✅ Option contract must be found

### **Trade Entry Result**

#### **✅ ENTRY (Status: `bought`)**
If ALL conditions met:
- `buy_price` = Current option LTP
- `buy_time` = Alert time
- `qty` = Lot size
- `stop_loss` = Calculated (buy_price - ₹3100/qty, rounded down to nearest 10 paise)
- `status` = `'bought'`
- `pnl` = 0.0
- `sell_price` = NULL (will be updated hourly)
- `stock_vwap_previous_hour` = Previous hour VWAP
- `stock_vwap_previous_hour_time` = Previous hour VWAP timestamp
- `option_current_candle_*` = Current option OHLC data
- `option_previous_candle_*` = Previous option OHLC data

#### **❌ NO ENTRY (Status: `no_entry`)**
If ANY condition fails:
- `buy_price` = Option LTP at alert time (for reference)
- `buy_time` = NULL (or alert time for display)
- `qty` = Lot size (for reference)
- `stop_loss` = Calculated (for reference)
- `status` = `'no_entry'`
- `pnl` = NULL
- `sell_price` = NULL

**Reasons for No Entry**:
- Time >= 3:00 PM
- Index trends not aligned
- VWAP slope < 45 degrees
- Candle size >= 7.5× previous candle
- Missing option data

---

## 💾 **Phase 4: Database Storage**

### **Database Record Created**
Every stock from webhook alert is saved to `intraday_stock_options` table:

**Fields Saved**:
- `alert_time` - When webhook was received
- `alert_type` - "Bullish" or "Bearish"
- `stock_name` - Stock symbol
- `stock_ltp` - Stock LTP at alert time
- `stock_vwap` - Stock VWAP at alert time
- `option_contract` - Option contract name (e.g., "RELIANCE-Nov2024-2500-CE")
- `option_type` - "CE" or "PE"
- `option_strike` - Strike price
- `option_ltp` - Option LTP at alert time
- `qty` - Lot size
- `buy_price` - Entry price (if entered) or reference price (if no_entry)
- `buy_time` - Entry time (if entered) or NULL
- `stop_loss` - Stop loss price
- `status` - `'bought'` or `'no_entry'`
- `instrument_key` - Upstox instrument key (for fetching option LTP)
- `trade_date` - Trading date
- `exit_reason` - NULL (will be set on exit)
- `sell_price` - NULL (will be updated hourly)
- `sell_time` - NULL (will be set on exit)
- `pnl` - Current P&L (0.0 if entered, NULL if no_entry)

---

## 🔄 **Phase 5: Hourly Monitoring (During Market Hours)**

### **Schedule**
Updates run at:
- **9:15 AM** - First update
- **9:30 AM** - Market open update
- **10:15 AM** - Hourly update
- **10:30 AM** - Special scan for 10:15 AM alert stocks
- **11:15 AM** - Hourly update
- **12:15 PM** - Hourly update
- **1:15 PM** - Hourly update
- **2:15 PM** - Hourly update
- **3:15 PM** - Hourly update

### **Function**: `update_vwap_for_all_open_positions()` in `backend/services/vwap_updater.py`

### **What Happens Each Hour**

#### **5.1 Re-evaluate "no_entry" Trades**
For trades with `status = 'no_entry'`:

1. **Check Current Conditions**:
   - Time < 3:00 PM?
   - Index trends aligned?
   - VWAP slope >= 45 degrees?
   - Candle size < 7.5× previous?
   - Option data available?

2. **If Conditions Met**:
   - Change `status` from `'no_entry'` to `'bought'`
   - Set `buy_price` = Current option LTP
   - Set `buy_time` = Current time (NOT alert time)
   - Set `stock_ltp` and `stock_vwap` = Current values
   - Calculate `stop_loss`
   - Set `pnl` = 0.0
   - **Trade is now active!**

#### **5.2 Update Open Positions**
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

#### **5.3 Check Exit Conditions**

**Priority Order** (highest to lowest):
1. **Stop Loss** (Highest Priority)
2. **VWAP Cross** (After 11:15 AM)
3. **Profit Target** (1.5x buy price)

##### **5.3.1 Stop Loss Check**
```python
if option_ltp <= stop_loss:
    exit_reason = 'stop_loss'
    status = 'sold'
    sell_price = option_ltp
    sell_time = now
    pnl = (sell_price - buy_price) × qty
```

##### **5.3.2 VWAP Cross Check** (Only after 11:15 AM)
```python
if now.hour >= 11 and now.minute >= 15:
    if option_type == 'CE' and stock_ltp < stock_vwap:
        exit_reason = 'stock_vwap_cross'
        status = 'sold'
        sell_price = option_ltp
        sell_time = now
        pnl = (sell_price - buy_price) × qty
    
    elif option_type == 'PE' and stock_ltp > stock_vwap:
        exit_reason = 'stock_vwap_cross'
        status = 'sold'
        sell_price = option_ltp
        sell_time = now
        pnl = (sell_price - buy_price) × qty
```

##### **5.3.3 Profit Target Check**
```python
profit_target = buy_price × 1.5
if option_ltp >= profit_target:
    exit_reason = 'profit_target'
    status = 'sold'
    sell_price = option_ltp
    sell_time = now
    pnl = (sell_price - buy_price) × qty
```

#### **5.4 After Exit**
Once `exit_reason` is set:
- ✅ Trade is **CLOSED**
- ✅ `sell_price` is **FROZEN** at exit price
- ✅ `sell_time` is **FROZEN** at exit time
- ✅ Trade is **excluded** from future hourly updates
- ✅ Final P&L is calculated and stored

---

## 🏁 **Phase 6: End of Day (3:25 PM)**

### **Function**: `close_all_open_trades()` in `backend/services/vwap_updater.py`

### **What Happens**
1. **Find All Open Trades**:
   - `status != 'sold'`
   - `exit_reason = NULL`
   - `trade_date = today`

2. **For Each Open Trade**:
   - Fetch current option LTP (with retry logic - 3 attempts)
   - Set `sell_price` = Current option LTP (or last known sell_price, or buy_price as fallback)
   - Set `sell_time` = 3:25 PM
   - Set `exit_reason` = `'time_based'`
   - Set `status` = `'sold'`
   - Calculate final P&L: `pnl = (sell_price - buy_price) × qty`

3. **Commit All Changes**

### **Fallback Logic for Sell Price**
If option LTP fetch fails:
1. ✅ Use **last known sell_price** from hourly updates (best option)
2. ⚠️ Use **buy_price** as fallback (results in 0 P&L)
3. ❌ Use **0.0** as last resort (worst case)

---

## 📊 **Phase 7: End of Day VWAP Update (3:30 PM & 3:35 PM)**

### **Function**: `update_end_of_day_vwap()` in `backend/services/vwap_updater.py`

### **What Happens**
- Updates ALL positions (both open and exited) with final day VWAP
- This is the complete VWAP from 9:15 AM to 3:30 PM
- Used for analysis and reporting

---

## 📈 **Summary: Trade States Throughout the Day**

### **State Transitions**

```
Webhook Alert Received
    ↓
[Status: 'no_entry' or 'bought']
    ↓
Hourly Updates (if 'bought')
    ↓
[Check Exit Conditions]
    ↓
[Exit Triggered?]
    ├─ Yes → [Status: 'sold', exit_reason: 'stop_loss'/'vwap_cross'/'profit_target']
    └─ No → Continue monitoring
    ↓
3:25 PM - Time-based Exit
    ↓
[Status: 'sold', exit_reason: 'time_based']
```

### **Status Values**
- `'alert_received'` - Minimal data saved (API failure)
- `'no_entry'` - Conditions not met at alert time (may be re-evaluated hourly)
- `'bought'` - Trade entered, actively monitored
- `'sold'` - Trade closed (exited)

### **Exit Reasons**
- `'stop_loss'` - Option LTP hit stop loss
- `'stock_vwap_cross'` - Stock LTP crossed VWAP (after 11:15 AM)
- `'profit_target'` - Option LTP reached 1.5x buy price
- `'time_based'` - End of day exit at 3:25 PM

---

## 🔍 **Key Features**

### **1. Re-entry Mechanism**
- Trades with `'no_entry'` status are re-evaluated every hour
- If conditions become favorable, trade is entered with current prices/time

### **2. Historical Data Tracking**
- Every hourly update saves a snapshot to `historical_market_data` table
- Tracks: stock_vwap, stock_ltp, option_ltp, scan_time

### **3. Robust Exit Handling**
- Multiple exit conditions checked independently
- Highest priority exit is applied
- Sell price fallback ensures trades always have an exit price

### **4. Data Integrity**
- `instrument_key` stored at entry for reliable option LTP fetching
- Retry logic for API failures
- Sanity checks for unrealistic price movements

---

## 📝 **Example Timeline**

**10:15 AM** - Webhook alert received for "RELIANCE"
- ✅ Time check: Before 3:00 PM
- ✅ Index trends: Both bullish
- ✅ Momentum: 0.5% above VWAP
- ✅ Option data: Valid
- **Result**: Trade entered (`status = 'bought'`, `buy_price = ₹50.25`)

**11:15 AM** - First hourly update
- Stock VWAP: ₹2450 → ₹2455
- Stock LTP: ₹2452 → ₹2458
- Option LTP: ₹50.25 → ₹52.30
- P&L: ₹0 → ₹+205
- Exit check: None triggered
- **Result**: Continue monitoring

**12:15 PM** - Second hourly update
- Stock VWAP: ₹2455 → ₹2460
- Stock LTP: ₹2458 → ₹2465
- Option LTP: ₹52.30 → ₹55.75
- P&L: ₹+205 → ₹+550
- Exit check: Profit target (₹75.38) not reached
- **Result**: Continue monitoring

**1:15 PM** - Third hourly update
- Stock VWAP: ₹2460 → ₹2458
- Stock LTP: ₹2465 → ₹2455 (crossed below VWAP!)
- Option LTP: ₹55.75 → ₹48.20
- P&L: ₹+550 → ₹-205
- Exit check: **VWAP Cross detected** (CE: stock LTP < VWAP)
- **Result**: **Trade exited** (`exit_reason = 'stock_vwap_cross'`, `sell_price = ₹48.20`, `pnl = ₹-205`)

**3:25 PM** - End of day
- Trade already exited, skipped

---

## 🎯 **Key Takeaways**

1. **Every webhook alert is saved** to database, even if trade is not entered
2. **Trades are monitored hourly** during market hours
3. **Multiple exit conditions** protect profits and limit losses
4. **Re-entry mechanism** allows trades to enter later if conditions improve
5. **All trades exit at 3:25 PM** if not already closed
6. **Historical data** is tracked for analysis and backtesting

