# Scheduled Tasks and Data Fetching Schedule

This document lists all scheduled tasks, their execution times, and what data they fetch.

## 📅 Daily Tasks (Once per day)

### 1. Master Stock Data Download
- **Time**: 9:00 AM IST
- **File**: `backend/services/master_stock_scheduler.py`
- **Function**: `download_and_update_master_stock()`
- **What it fetches**:
  - Downloads Dhan API scrip master CSV file
  - Filters NSE options data
  - Stores in PostgreSQL `master_stock` table
- **Purpose**: Provides option contract metadata (strikes, expiries, lot sizes)

### 2. Upstox Instruments Download
- **Time**: 9:05 AM IST (5 minutes after master stock)
- **File**: `backend/services/instruments_downloader.py`
- **Function**: `download_daily_instruments()`
- **What it fetches**:
  - Downloads Upstox instruments JSON file
  - Stores in `/home/ubuntu/trademanthan/data/instruments/nse_instruments.json`
  - Contains instrument keys for option contracts
- **Purpose**: Provides instrument keys for fetching option LTPs

---

## ⏰ Hourly Tasks (During Market Hours)

### 3. Market Data Update (VWAP, Stock LTP, Option LTP)
- **Times**: 
  - **9:15 AM** - First update
  - **9:30 AM** - Market open update
  - **10:15 AM** - Hourly update
  - **11:15 AM** - Hourly update
  - **12:15 PM** - Hourly update
  - **1:15 PM** - Hourly update
  - **2:15 PM** - Hourly update
  - **3:15 PM** - Hourly update
- **File**: `backend/services/vwap_updater.py`
- **Function**: `update_vwap_for_all_open_positions()`
- **What it fetches** (for each open position):
  1. **Stock VWAP** (`stock_vwap`)
     - Fetched via `upstox_service.get_stock_vwap(stock_name)`
     - Uses hourly candles to calculate Volume Weighted Average Price
   
  2. **Stock LTP** (`stock_ltp`)
     - Fetched via `upstox_service.get_stock_ltp_from_market_quote(stock_name)`
     - Current Last Traded Price of underlying stock
   
  3. **Option LTP** (`sell_price`)
     - Fetched via `upstox_service.get_market_quote_by_key(instrument_key)`
     - Current Last Traded Price of option contract
     - Uses stored `instrument_key` from trade entry
   
  4. **Index Trends** (for re-evaluating "no_entry" trades)
     - NIFTY 50 trend (bullish/bearish)
     - BANKNIFTY trend (bullish/bearish)
     - Fetched via `upstox_service.check_index_trends()`

- **Additional Processing**:
  - Re-evaluates "no_entry" trades to check if conditions are now met
  - Checks for VWAP cross conditions (exit signal)
  - Checks for stop loss triggers
  - Checks for profit target hits
  - Updates PnL calculations

---

## 🏁 End of Day Tasks

### 4. Close All Open Trades
- **Time**: 3:25 PM IST (5 minutes before market close)
- **File**: `backend/services/vwap_updater.py`
- **Function**: `close_all_open_trades()`
- **What it does**:
  - Closes all open positions that haven't exited yet
  - Sets `exit_reason = 'time_exit'`
  - Sets `status = 'sold'`
  - Calculates final PnL

### 5. End of Day VWAP Update
- **Times**: 
  - **3:30 PM IST** - First EOD update
  - **3:35 PM IST** - Final EOD update (5 minutes later)
- **File**: `backend/services/vwap_updater.py`
- **Function**: `update_end_of_day_vwap()`
- **What it fetches**:
  - Final VWAP for ALL positions (including exited ones)
  - Ensures complete market data for the day
  - Updates historical records

---

## 🔍 Health Monitoring Tasks

### 6. Health Checks (Every 15 minutes)
- **Times**: 
  - Every 15 minutes from **9:15 AM to 3:45 PM**
  - Schedule: 9:15, 9:30, 9:45, 10:00, 10:15, 10:30, 10:45, 11:00, 11:15, 11:30, 11:45, 12:00, 12:15, 12:30, 12:45, 1:00, 1:15, 1:30, 1:45, 2:00, 2:15, 2:30, 2:45, 3:00, 3:15, 3:30, 3:45
- **File**: `backend/services/health_monitor.py`
- **Function**: `perform_health_check()`
- **What it checks**:
  1. **Database connectivity**
     - Tests database connection
   
  2. **Webhook reception**
     - Counts alerts received today
     - Alerts if no webhooks after 11 AM on weekdays
   
  3. **Upstox API token status**
     - Fetches index prices to verify token validity
     - Checks NIFTY and BANKNIFTY data availability
   
  4. **Instruments file freshness**
     - Verifies instruments JSON file exists
     - Checks file age (alerts if > 7 days old)

### 7. Hourly Health Checks
- **Times**: 
  - **10:00 AM, 11:00 AM, 12:00 PM, 1:00 PM, 2:00 PM, 3:00 PM**
- **File**: `backend/services/health_monitor.py`
- **Function**: `perform_health_check()`
- **Same checks as above** (redundant coverage)

### 8. Daily Health Report
- **Time**: 4:00 PM IST (after market close)
- **File**: `backend/services/health_monitor.py`
- **Function**: `send_daily_health_report()`
- **What it does**:
  - Generates summary of system health
  - Reports any issues detected during the day
  - Sends notifications if configured

---

## 📊 Summary Table

| Task | Frequency | Time(s) | Data Fetched |
|------|-----------|---------|--------------|
| Master Stock Download | Daily | 9:00 AM | Dhan API scrip master CSV |
| Instruments Download | Daily | 9:05 AM | Upstox instruments JSON |
| Market Data Update | Hourly | 9:15, 9:30, 10:15, 11:15, 12:15, 1:15, 2:15, 3:15 | Stock VWAP, Stock LTP, Option LTP, Index Trends |
| Close Open Trades | Daily | 3:25 PM | None (closes positions) |
| EOD VWAP Update | Daily | 3:30 PM, 3:35 PM | Final VWAP for all positions |
| Health Check | Every 15 min | 9:15 AM - 3:45 PM | Database, Webhooks, API Token, Instruments file |
| Hourly Health Check | Hourly | 10:00 AM - 3:00 PM | Same as above |
| Daily Health Report | Daily | 4:00 PM | Summary report |

---

## 🔄 Backend Monitoring

### 9. Backend Process Monitor
- **Frequency**: Every 5 minutes
- **File**: `backend/scripts/monitor_backend.sh`
- **What it does**:
  - Checks if backend process is running
  - Verifies health endpoint responds
  - Auto-restarts backend if down
  - Logs to `/tmp/backend_monitor.log`

---

## 📝 Notes

- All times are in **IST (Asia/Kolkata)** timezone
- Market hours: 9:15 AM - 3:30 PM IST
- Hourly updates run at **:15 minutes** past each hour
- Health checks run more frequently for better monitoring
- All scheduled tasks use **APScheduler** with CronTrigger
- Tasks are started when backend application starts (via `lifespan` context manager)

