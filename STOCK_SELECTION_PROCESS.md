# Stock Selection Process - From Webhook to Trade

## Complete Workflow Overview

When alerts arrive from the Chartink webhook, stocks go through a rigorous 7-step selection process:

1. **Webhook Alert Received** - Parse incoming data
2. **Data Enrichment** - Fetch market data (LTP, VWAP, options)
3. **Stock Ranking & Selection** - Select best 15 if >15 stocks (momentum-based)
4. **Index Trend Check** - Verify NIFTY & BANKNIFTY alignment
5. **Momentum Filter** - Validate 0.3% minimum momentum
6. **Trade Entry Decision** - 3-layer validation
7. **Database Storage** - Save all records

---

## Step 1: Webhook Alert Received

**Source:** Chartink (TradingView webhook)

**Data Received:**
```json
{
  "scan_name": "Bearish Momentum",
  "triggered": "2025-11-07 10:15:03",
  "stocks": [
    {"name": "ZYDUSLIFE", "trigger": 933.80},
    {"name": "ABB", "trigger": 7350.25},
    {"name": "KAYNES", "trigger": 5210.00}
    // ... 40 more stocks
  ]
}
```

**Initial Processing:**
- Parse alert type (Bullish/Bearish) from scan_name
- Extract timestamp
- Count total stocks (e.g., 43 stocks)

---

## Step 2: Data Enrichment

For EACH stock in the alert, additional data is fetched:

### A. Stock Data (Upstox API)
- **Stock LTP**: Current market price via `get_stock_ltp_and_vwap()`
- **Stock VWAP**: Volume weighted average price for momentum calculation

### B. Option Contract Identification
- **Strike Calculation**:
  - Bearish (PE): Round DOWN to nearest 50/100
  - Bullish (CE): Round UP to nearest 50/100
- **Contract Name**: Format `{STOCK}-{MONTH}{YEAR}-{STRIKE}-{TYPE}`
  - Example: `ZYDUSLIFE-Nov2025-900-PE`

### C. Option Data (Master Stock Database)
- **Lot Size**: Shares per lot from master_stock table
- **Option LTP**: Premium price via Upstox instrument key

**Enriched Data Structure:**
```python
{
  "stock_name": "ZYDUSLIFE",
  "trigger_price": 933.80,
  "last_traded_price": 933.80,
  "stock_vwap": 935.50,
  "option_type": "PE",
  "option_contract": "ZYDUSLIFE-Nov2025-900-PE",
  "otm1_strike": 900.0,
  "option_ltp": 4.90,
  "qty": 100
}
```

---

## Step 3: Stock Ranking & Selection

**Trigger:** Only if alert contains > 15 stocks

**Maximum Stocks Per Alert:** 15

### Ranking Algorithm (`services/stock_ranker.py`)

#### Scoring Breakdown (100 points max):

**1. MOMENTUM STRENGTH (40 points) - Highest Weight**
```python
momentum_pct = abs((stock_ltp - stock_vwap) / stock_vwap) × 100

Direction Check:
- PE (Bearish): Stock must be BELOW VWAP
- CE (Bullish): Stock must be ABOVE VWAP
- Wrong direction = 0 points

Scoring:
- ≥3.0% momentum: 40 points
- ≥2.0% momentum: 35 points
- ≥1.5% momentum: 30 points
- ≥1.0% momentum: 25 points
- ≥0.5% momentum: 18 points
- <0.5% momentum: 10 points
```

**2. LIQUIDITY/EXECUTABILITY (25 points)**
```python
Based on lot size:
- ≥1000 qty: 25 points
- ≥500 qty:  22 points
- ≥300 qty:  20 points
- ≥150 qty:  17 points
- ≥75 qty:   15 points
- <75 qty:   10 points
```

**3. OPTION PREMIUM QUALITY (20 points)**
```python
Optimal range: ₹2-30
- ₹2-30:      20 points (optimal)
- ₹1-2:       18 points
- ₹30-60:     17 points
- ₹0.50-1:    15 points (penny options)
- ₹60-100:    12 points
- >₹100:      8 points
```

**4. STRIKE SELECTION (10 points)**
```python
OTM percentage check:
- 0.5-4% OTM:  10 points (optimal)
- 4-7% OTM:    8 points
- <0.5% OTM:   7 points
- >7% OTM:     4 points
```

**5. DATA COMPLETENESS (5 points)**
```python
All required fields present: 5 points
```

#### Bonus Points:

**BONUS 1: Extreme Momentum Multiplier (+10 points)**
- Triggered when momentum ≥ 5% in correct direction

**BONUS 2: "Likely to Hold" Characteristics (+10 points max)**
- Premium in stable range (₹10-60): +5 points
- Not penny options (≥₹2): +3 points
- Moderate liquidity (150-800 lot): +2 points

### Selection Process:
1. Calculate score for all 43 stocks
2. Sort by score (highest to lowest)
3. Select TOP 15 stocks
4. Reject remaining 28 stocks

---

## Step 4: Index Trend Check

**Purpose:** Ensure market conditions support the trade direction

**API Call:** `vwap_service.check_index_trends()`

**Returns:**
```python
{
  "nifty_trend": "bullish" | "bearish" | "neutral",
  "banknifty_trend": "bullish" | "bearish" | "neutral",
  "allow_trading": true | false
}
```

### Trade Entry Rules:

**For BULLISH Alerts (CE options):**
- ✅ ALLOW: Both NIFTY & BANKNIFTY = bullish
- ❌ BLOCK: Either index is bearish or neutral

**For BEARISH Alerts (PE options):**
- ✅ ALLOW: Both NIFTY & BANKNIFTY = bearish
- ❌ BLOCK: Either index is bullish or neutral

**Example (Bearish Alert):**
```
NIFTY:      Open: 24,150 → LTP: 24,050 → Trend: bearish ✅
BANKNIFTY:  Open: 51,800 → LTP: 51,650 → Trend: bearish ✅
Result:     BOTH bearish → can_enter_trade_by_index = TRUE ✅
```

---

## Step 5: Momentum Filter (0.3% Threshold)

**Purpose:** Validate stock has minimum momentum in correct direction

**Calculation:**
```python
momentum_pct = abs((stock_ltp - stock_vwap) / stock_vwap) × 100
```

**Direction Validation:**
- **PE (Bearish):** Stock LTP < Stock VWAP ✅
- **CE (Bullish):** Stock LTP > Stock VWAP ✅

**Threshold Check:**
```python
MINIMUM_MOMENTUM_PCT = 0.3%

If momentum_pct >= 0.3% AND correct direction:
   has_strong_momentum = TRUE ✅
Else:
   has_strong_momentum = FALSE ❌
```

**Why 0.3%?**
Based on Nov 7 analysis:
- Winners had momentum: 0.18% - 1.05%
- Setting too high (1.5%) would block all winners
- 0.3% ensures direction validation without being too strict

**Example Checks:**
```
STOCK: ABB (PE option)
LTP: ₹7,350, VWAP: ₹7,385
Momentum: -0.47%
Direction: LTP < VWAP ✅ (Correct for PE)
Absolute: 0.47% (ABOVE 0.3% threshold)
Result: ✅ Strong bearish momentum: 0.47% below VWAP

STOCK: ZYDUSLIFE (PE option)
LTP: ₹933.80, VWAP: ₹935.50
Momentum: -0.18%
Direction: LTP < VWAP ✅ (Correct for PE)
Absolute: 0.18% (BELOW 0.3% threshold)
Result: 🚫 NO ENTRY - Weak momentum: 0.18% (need ≥0.3%)
```

---

## Step 6: Trade Entry Decision

### 3-Layer Protection System:

**Layer 1: Index Trend Alignment**
- ✅ `can_enter_trade_by_index = TRUE`

**Layer 2: Momentum Direction & Strength**
- ✅ `has_strong_momentum = TRUE` (≥0.3% in correct direction)

**Layer 3: Valid Option Data**
- ✅ `option_ltp > 0`
- ✅ `lot_size > 0`

### IF ALL 3 LAYERS PASS → ENTER TRADE ✅

**Trade Entry Details:**
```python
qty = lot_size                    # Example: 100
buy_price = option_ltp            # Example: ₹4.90
buy_time = alert_datetime         # Example: 2025-11-07 10:15:03
status = 'bought'

# Stop Loss Calculation
SL_LOSS_TARGET = ₹3,100
calculated_sl = option_ltp - (3100 / qty)
stop_loss = max(0.05, floor(calculated_sl / 0.10) × 0.10)

# Initial Exit Values (updated later)
sell_price = NULL
sell_time = NULL
exit_reason = NULL
pnl = 0.0
```

**Output:**
```
✅ TRADE ENTERED: ABB - Strong bearish momentum: 0.47% below VWAP
   Buy: ₹4.90, Qty: 100, SL: ₹0.05, LTP: ₹7350, VWAP: ₹7385
```

### IF ANY LAYER FAILS → NO ENTRY ❌

**No Entry Details:**
```python
qty = 0
buy_price = NULL
buy_time = NULL
status = 'no_entry'
```

**Output Examples:**
```
⚠️ NO ENTRY: ZYDUSLIFE - Index trends not aligned (NIFTY: bullish, BANKNIFTY: bearish)
🚫 NO ENTRY: KAYNES - Weak momentum: 0.18% (need ≥0.3%)
🚫 NO ENTRY: ABC - WRONG direction: PE but stock above VWAP
⚠️ NO ENTRY: XYZ - Missing option data (option_ltp=0, qty=0)
```

---

## Step 7: Database Storage

**ALL stocks are saved** to database (both entered and not entered)

**Table:** `intraday_stock_options`

**Record Structure:**
```python
{
  # Alert Information
  "alert_time": datetime,
  "alert_type": "Bullish" | "Bearish",
  "scan_name": string,
  "trade_date": date,
  
  # Stock Information
  "stock_name": string,
  "stock_ltp": float,
  "stock_vwap": float,
  
  # Option Information
  "option_contract": string,
  "option_type": "CE" | "PE",
  "option_strike": float,
  "option_ltp": float,
  
  # Trade Execution (if entered)
  "qty": int,
  "buy_time": datetime | NULL,
  "buy_price": float | NULL,
  "stop_loss": float | NULL,
  "status": "bought" | "no_entry",
  
  # Trade Exit (updated by hourly service)
  "sell_time": datetime | NULL,
  "sell_price": float | NULL,
  "exit_reason": string | NULL,  # profit_target, stop_loss, time_based, vwap_cross
  "pnl": float
}
```

---

## Real-World Example: November 7, 2025

**Incoming Webhook:**
- Alert: Bearish Momentum scan
- Stocks in alert: 43 stocks
- Triggered: 10:15 AM

**Step-by-Step Results:**

1. **Enrichment:** ✅ 43 stocks enriched with market data

2. **Ranking (43 > 15):**
   - ✅ Top 15 selected based on scores
   - ❌ 28 stocks rejected (lower scores)

3. **Index Check:**
   - ✅ NIFTY: bearish, BANKNIFTY: bearish → PASS

4. **Momentum Filter (15 stocks):**
   - ✅ 8 stocks: momentum ≥ 0.3% in correct direction
   - ❌ 7 stocks: weak momentum or wrong direction

5. **Valid Data Check (8 stocks):**
   - ✅ 8 stocks: have option_ltp > 0 and qty > 0
   - ❌ 0 stocks: missing data

**Final Result:**
```
✅ TRADES ENTERED: 8 stocks
❌ NO ENTRY: 35 stocks (28 rejected by ranking + 7 failed momentum)

DATABASE RECORDS: 43 records saved
• 8 with status='bought'
• 35 with status='no_entry'
```

---

## Key Decision Factors

| Factor | Weight | Description |
|--------|--------|-------------|
| **Volume Limit** | - | Max 15 stocks per alert |
| **Momentum** | 40% | Distance from VWAP (if ranking) |
| **Liquidity** | 25% | Lot size for executability |
| **Premium Quality** | 20% | Option price in tradeable range |
| **Strike Selection** | 10% | OTM percentage optimal range |
| **Completeness** | 5% | All data fields present |
| **Index Alignment** | Required | Both indexes must agree |
| **Minimum Momentum** | 0.3% | Direction + strength validation |
| **Data Availability** | Required | Must have LTP and lot size |

---

## Benefits of This System

✅ **Quality Over Quantity**
- Selects best 15 instead of all 43
- Focuses on high-probability setups

✅ **Momentum-Based**
- Prioritizes stocks with strong price action
- 40% weight on momentum ensures best movers selected

✅ **Risk Management**
- Index alignment prevents counter-trend trades
- Momentum filter validates direction
- Stop loss calculated for each trade

✅ **Executable Trades**
- Checks for valid option data
- Ensures liquidity (lot size check)
- Premium in tradeable range

✅ **Comprehensive Tracking**
- All alerts saved (entered or not)
- Detailed reasoning for each decision
- Audit trail for performance analysis

---

## Related Files

- **Webhook Handler:** `backend/routers/scan.py` (process_webhook_data function)
- **Stock Ranker:** `backend/services/stock_ranker.py`
- **Upstox Service:** `backend/services/upstox_service.py`
- **Database Model:** `backend/models/trading.py` (IntradayStockOption)
- **Master Stock Data:** `backend/models/trading.py` (MasterStock)

---

*Last Updated: November 9, 2025*

