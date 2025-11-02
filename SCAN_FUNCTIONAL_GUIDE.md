# TradeManthan Scan Page - Functional Guide

**URL:** https://trademanthan.in/scan.html  
**Purpose:** Real-time stock scanning and intraday options trading alerts based on Chartink webhooks

---

## Table of Contents

1. [Overview](#overview)
2. [Page Initialization Flow](#page-initialization-flow)
3. [Index Price Monitoring](#index-price-monitoring)
4. [Webhook Processing](#webhook-processing)
5. [Trading Logic & Conditions](#trading-logic--conditions)
6. [Authentication Flows](#authentication-flows)
7. [API Endpoints](#api-endpoints)
8. [Data Structures](#data-structures)
9. [UI Components](#ui-components)
10. [Error Handling](#error-handling)

---

## Overview

The scan page is an intelligent trading alert system that:
- Receives real-time webhook alerts from Chartink.com
- Monitors NIFTY 50 and BANKNIFTY index trends
- Calculates optimal option contracts (OTM-1 strikes)
- Determines entry/exit signals based on VWAP
- Manages Upstox API authentication
- Provides downloadable CSV reports

---

## Page Initialization Flow

### 1. Page Load Sequence

```
DOMContentLoaded Event
    ↓
checkOAuthSuccess()           // Check for auth callback
    ↓
loadIndexPrices()            // Fetch NIFTY & BANKNIFTY prices
    ↓
loadLatestData()             // Fetch webhook alerts
    ↓
startAutoRefresh()           // Begin 30-second auto-refresh
```

### 2. Initialization Code

**File:** `scan.js` (Lines 13-22)

```javascript
document.addEventListener('DOMContentLoaded', function() {
    checkOAuthSuccess();      // Handle OAuth returns
    loadIndexPrices();        // Load index data
    loadLatestData();         // Load alerts
    startAutoRefresh();       // Start refresh timer
});
```

**Timing:**
- Auto-refresh interval: 30 seconds
- OAuth success reload delay: 3 seconds
- Index price polling: Every 30 seconds

---

## Index Price Monitoring

### 1. Index Price Fetch Flow

```
loadIndexPrices()
    ↓
API Call: GET /scan/index-prices
    ↓
Check Response Status
    ├─ 401/token_expired → showTokenExpiredMessage()
    ├─ Success → updateIndexDisplay(data)
    └─ Error → showIndexError()
```

### 2. Index Trend Determination

**Backend Logic** (`services/upstox_service.py`):

**IMPORTANT:** Index trends are determined by **candle color** (LTP vs Open Price), NOT VWAP.

```python
def check_index_trends():
    # Get NIFTY and BANKNIFTY OHLC data from Upstox
    nifty_ohlc = get_ohlc_data("NIFTY 50")
    banknifty_ohlc = get_ohlc_data("BANKNIFTY")
    
    # Extract day's open price and current LTP
    nifty_day_open = nifty_ohlc['open']
    nifty_ltp = nifty_ohlc['last_price']
    
    banknifty_day_open = banknifty_ohlc['open']
    banknifty_ltp = banknifty_ohlc['last_price']
    
    # Determine trend based on CANDLE COLOR (LTP vs Day Open)
    nifty_trend = 'bullish' if nifty_ltp > nifty_day_open else 'bearish'
    banknifty_trend = 'bullish' if banknifty_ltp > banknifty_day_open else 'bearish'
```

**Candle Color Logic:**
- **Green Candle (Bullish):** LTP > Day Open Price → ↑ (green arrow)
- **Red Candle (Bearish):** LTP < Day Open Price → ↓ (red arrow)
- **Neutral:** LTP = Day Open (rare) → → (gray arrow)

**Note:** This logic applies **ONLY to indices** (NIFTY50 & BANKNIFTY). Individual stocks use VWAP for Hold/Exit signals.

### 3. Index Display Update

**File:** `scan.js` (Lines 92-138)

**Data Flow:**
```
updateIndexDisplay(data)
    ↓
For NIFTY:
    - Display price (LTP or close_price if market closed)
    - Show trend arrow (↑/↓/→)
    - Apply color class (up/down/neutral)
    ↓
For BANKNIFTY:
    - Display price (LTP or close_price if market closed)
    - Show trend arrow (↑/↓/→)
    - Apply color class (up/down/neutral)
```

**Market Status:**
- **Open:** Uses `ltp` (Last Traded Price)
- **Closed:** Uses `close_price` (Previous close)

---

## Webhook Processing

### 1. Webhook Endpoints

| Endpoint | Purpose | Option Type |
|----------|---------|-------------|
| `/scan/chartink-webhook-bullish` | Bullish alerts only | CALL (CE) |
| `/scan/chartink-webhook-bearish` | Bearish alerts only | PUT (PE) |
| `/scan/chartink-webhook` | Auto-detect | Based on scan name |

### 2. Webhook Data Format (Chartink Input)

```json
{
    "stocks": "SEPOWER,ASTEC,EDUCOMP",
    "trigger_prices": "3.75,541.8,2.1",
    "triggered_at": "2:34 pm",
    "scan_name": "Bullish Breakout",
    "scan_url": "bullish-breakout",
    "alert_name": "Alert for Bullish Breakout"
}
```

### 3. Webhook Processing Flow

```
Chartink sends webhook
    ↓
Backend: process_webhook_data()
    ↓
For each stock:
    ├─ Parse stock name and trigger price
    ├─ Fetch stock LTP from Upstox API
    ├─ Calculate stock VWAP
    ├─ Determine option type (CE/PE)
    ├─ Find OTM-1 strike from option chain
    ├─ Get option LTP and VWAP
    ├─ Calculate quantity based on margin
    ├─ Set entry price (buy_price)
    ├─ Set exit price (sell_price)
    ├─ Calculate potential PnL
    └─ Store in database (IntradayStockOption)
    ↓
Group by triggered_at timestamp
    ↓
Store in memory (bullish_data or bearish_data)
    ↓
Return success response
```

### 4. Stock Processing Details

**File:** `backend/routers/scan.py` (Lines 246-670)

**Key Calculations:**

**A. Stock Data Fetching:**
```python
# Get LTP (Last Traded Price)
stock_ltp = vwap_service.get_ltp(stock_name)

# Calculate VWAP (Volume Weighted Average Price)
stock_vwap = vwap_service.get_vwap(stock_name)
```

**B. Option Contract Selection:**
```python
# For Bullish (CE): Find OTM-1 strike > stock_ltp
# For Bearish (PE): Find OTM-1 strike < stock_ltp

strike = find_strike_from_option_chain(
    vwap_service, 
    stock_name, 
    option_type,  # 'CE' or 'PE'
    stock_ltp
)
```

**C. Quantity Calculation:**
```python
# Based on ₹10,000 margin per trade
MARGIN_PER_TRADE = 10000
option_ltp = strike['ltp']
qty = MARGIN_PER_TRADE / option_ltp
qty = round(qty)  # Round to nearest integer
```

**D. Entry/Exit Prices:**
```python
buy_price = option_ltp      # Current option price
sell_price = option_ltp * 1.5  # 50% profit target
```

**E. PnL Calculation:**
```python
pnl = (sell_price - buy_price) * qty
```

---

## Trading Logic & Conditions

### 1. Index Alignment Check

**Condition:** Both NIFTY and BANKNIFTY must have the SAME trend direction

```javascript
// Trading allowed conditions:
if (nifty_trend === 'bullish' && banknifty_trend === 'bullish') {
    allow_trading = true;
    show_section = 'bullish';
}
else if (nifty_trend === 'bearish' && banknifty_trend === 'bearish') {
    allow_trading = true;
    show_section = 'bearish';
}
else {
    allow_trading = false;  // Indices in opposite directions
    show_warning = "No Trade Applicable";
}
```

**UI Behavior:**
- ✅ **Aligned (both bullish):** Show Bullish alerts only
- ✅ **Aligned (both bearish):** Show Bearish alerts only
- ⚠️ **Opposite:** Show warning banner, display both sections

### 2. Hold/Exit Signal Logic

**File:** `scan.js` (Lines 385-389)

**NOTE:** Hold/Exit signals for **individual stocks** use VWAP, NOT Open Price.

```javascript
const stock_ltp = stock.last_traded_price;
const stock_vwap = stock.stock_vwap;

if (stock_ltp > stock_vwap) {
    signal = 'Hold';    // Stock above VWAP - continue holding
} else {
    signal = 'Exit';    // Stock below VWAP - exit position
}
```

**Signal Display:**
- 🟢 **Hold:** Green badge (Stock LTP > Stock VWAP)
- 🔴 **Exit:** Red badge (Stock LTP ≤ Stock VWAP)

**KEY DISTINCTION:**
- **Index Trends (NIFTY/BANKNIFTY):** Use LTP vs Day Open (candle color)
- **Stock Signals (Hold/Exit):** Use LTP vs VWAP (momentum indicator)

### 3. Section Display Logic

**File:** `scan.js` (Lines 510-588)

```
displaySectionsBasedOnTrends(index_check, bullish, bearish)
    ↓
IF allow_trading === true:
    ├─ Bullish market → Show ONLY bullish alerts
    ├─ Bearish market → Show ONLY bearish alerts
    └─ Hide opposite trends warning
    ↓
ELSE (opposite trends):
    ├─ Show "No Trade Applicable" warning
    ├─ Display BOTH sections (bullish & bearish)
    └─ Enable all data visibility
```

---

## Authentication Flows

### 1. OAuth Authentication Flow

```
User clicks "Login with Upstox"
    ↓
Frontend: initiateUpstoxOAuth()
    ↓
Redirect: /scan/upstox/login
    ↓
Backend: Generate state token for CSRF
    ↓
Redirect: https://api.upstox.com/v2/login/authorization/dialog
          ?response_type=code
          &client_id={UPSTOX_API_KEY}
          &redirect_uri={CALLBACK_URL}
          &state={STATE_TOKEN}
    ↓
User authorizes on Upstox
    ↓
Upstox redirects: /scan/upstox/callback?code={AUTH_CODE}&state={STATE}
    ↓
Backend: Exchange code for access_token
    ↓
Backend: Update token in upstox_service.py file
    ↓
Backend: Update token in memory
    ↓
Redirect: /scan.html?auth=success
    ↓
Frontend: checkOAuthSuccess() → Show success message → Reload page
```

**File References:**
- Frontend: `scan.js` (Lines 896-911)
- Backend: `scan.py` (Lines 1443-1581)

### 2. Manual Token Update Flow

```
User pastes token in popup
    ↓
Click "Update Token Manually"
    ↓
Frontend: updateTokenFromPopup()
    ↓
POST /scan/update-upstox-token
    Body: { "access_token": "{TOKEN}" }
    ↓
Backend: Update token in service file
    ↓
Backend: Restart backend service
    ↓
Frontend: Show success → Reload page after 3s
```

**File:** `scan.js` (Lines 913-942)

### 3. Token Expiry Detection

**Triggers:**
- HTTP 401 status from any API call
- Response contains `error_type: 'token_expired'`

**Actions:**
```javascript
showTokenExpiredMessage()
    ↓
Display warning banner
    ↓
Show authentication popup modal
    ↓
Offer two options:
    ├─ OAuth Login (recommended)
    └─ Manual Token Entry
```

---

## API Endpoints

### Frontend API Calls

| Endpoint | Method | Purpose | Frequency |
|----------|--------|---------|-----------|
| `/scan/index-prices` | GET | Get NIFTY & BANKNIFTY prices and trends | Every 30s |
| `/scan/latest` | GET | Get latest bullish/bearish webhook alerts | Every 30s |
| `/scan/update-upstox-token` | POST | Update access token manually | On-demand |
| `/scan/upstox/login` | GET | Initiate OAuth flow | On-demand |
| `/scan/upstox/callback` | GET | Handle OAuth callback | OAuth flow |
| `/scan/upstox/status` | GET | Check token validity | On-demand |

### Backend Webhook Receivers

| Endpoint | Method | Purpose | Called By |
|----------|--------|---------|-----------|
| `/scan/chartink-webhook-bullish` | POST | Receive bullish alerts | Chartink.com |
| `/scan/chartink-webhook-bearish` | POST | Receive bearish alerts | Chartink.com |
| `/scan/chartink-webhook` | POST | Auto-detect alerts (legacy) | Chartink.com |

---

## Data Structures

### 1. Index Data Structure

```javascript
{
    "status": "success",
    "data": {
        "market_status": "open|closed",
        "nifty": {
            "ltp": 24500.50,
            "close_price": 24450.00,
            "vwap": 24480.25,
            "trend": "bullish|bearish|neutral"
        },
        "banknifty": {
            "ltp": 52300.75,
            "close_price": 52250.00,
            "vwap": 52275.50,
            "trend": "bullish|bearish|neutral"
        }
    }
}
```

### 2. Alert Data Structure

```javascript
{
    "status": "success",
    "data": {
        "bullish": {
            "date": "2025-11-02",
            "alerts": [
                {
                    "triggered_at": "2025-11-02 14:34:00",
                    "scan_name": "Bullish Breakout",
                    "stocks": [
                        {
                            "stock_name": "RELIANCE",
                            "trigger_price": 2450.50,
                            "last_traded_price": 2452.00,
                            "stock_vwap": 2448.75,
                            "option_contract": "RELIANCE 02NOV25 2500 CE",
                            "option_type": "CE",
                            "otm1_strike": 2500.0,
                            "option_ltp": 25.50,
                            "option_vwap": 24.80,
                            "qty": 392,
                            "buy_price": 25.50,
                            "sell_price": 38.25,
                            "pnl": 4998.0
                        }
                    ]
                }
            ]
        },
        "bearish": {
            // Same structure for bearish alerts
        },
        "index_check": {
            "nifty_trend": "bullish|bearish|neutral",
            "banknifty_trend": "bullish|bearish|neutral",
            "allow_trading": true|false,
            "show_section": "bullish|bearish|both"
        },
        "allow_trading": true|false
    }
}
```

### 3. Stock Alert Object (Individual Stock)

```javascript
{
    "stock_name": "TATAMOTORS",
    "trigger_price": 950.50,          // Chartink trigger price
    "last_traded_price": 952.00,      // Current LTP from Upstox
    "stock_vwap": 948.75,             // Stock VWAP
    "option_contract": "TATAMOTORS 02NOV25 980 CE",
    "option_type": "CE",              // CE (Call) or PE (Put)
    "otm1_strike": 980.0,             // OTM-1 strike price
    "option_ltp": 15.25,              // Option premium (current)
    "option_vwap": 14.80,             // Option VWAP
    "qty": 656,                       // Calculated quantity
    "buy_price": 15.25,               // Entry price
    "sell_price": 22.88,              // Exit target (50% profit)
    "pnl": 5004.0                     // Potential profit
}
```

---

## Trading Logic & Conditions

### 1. OTM-1 Strike Selection Algorithm

**File:** `backend/routers/scan.py` (Lines 45-165)

```python
def find_strike_from_option_chain(vwap_service, stock_name, option_type, stock_ltp):
    """
    Find Out-of-The-Money (OTM-1) strike based on volume and OI
    """
    
    # Step 1: Get option chain from Upstox
    option_chain = vwap_service.get_option_chain(stock_name)
    
    # Step 2: Filter for OTM strikes
    if option_type == 'CE':  # Call options
        otm_strikes = [s for s in strikes if s['strike_price'] > stock_ltp]
    else:  # PE - Put options
        otm_strikes = [s for s in strikes if s['strike_price'] < stock_ltp]
    
    # Step 3: Sort by distance from LTP (closest first)
    otm_strikes.sort(key=lambda x: abs(x['strike_price'] - stock_ltp))
    
    # Step 4: Select OTM-1 (first OTM strike)
    if len(otm_strikes) > 0:
        return otm_strikes[0]  # OTM-1
    
    return None
```

**OTM-1 Definition:**
- **For CALL (CE):** First strike price ABOVE current LTP
- **For PUT (PE):** First strike price BELOW current LTP

**Selection Criteria:**
1. Strike must be OTM (out of the money)
2. Sorted by proximity to LTP
3. First option is OTM-1 (most liquid)

### 2. Quantity Calculation Logic

**File:** `backend/routers/scan.py` (Lines 400-450)

```python
MARGIN_PER_TRADE = 10000  # ₹10,000 per trade

def calculate_quantity(option_ltp):
    """
    Calculate quantity based on fixed margin
    """
    if option_ltp <= 0:
        return 0
    
    qty = MARGIN_PER_TRADE / option_ltp
    qty = round(qty)  # Round to nearest lot
    
    return max(qty, 1)  # Minimum 1 quantity
```

**Example:**
- Option LTP: ₹25.50
- Margin: ₹10,000
- Quantity: 10000 / 25.50 = 392 shares/contracts

### 3. Entry/Exit Price Logic

**Entry (Buy Price):**
```python
buy_price = option_ltp  # Current market price
```

**Exit (Sell Price):**
```python
sell_price = option_ltp * 1.5  # 50% profit target
```

**PnL Calculation:**
```python
pnl = (sell_price - buy_price) * qty
# Example: (38.25 - 25.50) * 392 = ₹4,998
```

### 4. Hold/Exit Decision Logic

**File:** `scan.js` (Lines 385-389)

```javascript
const shouldHold = stock_ltp > stock_vwap;

if (shouldHold) {
    signal = 'Hold';   // Continue holding position
    icon_class = 'hold-icon';  // Green badge
} else {
    signal = 'Exit';   // Exit/book profits
    icon_class = 'exit-icon';  // Red badge
}
```

**Decision Matrix:**

| Condition | LTP vs VWAP | Signal | Action |
|-----------|-------------|--------|--------|
| Stock momentum positive | LTP > VWAP | 🟢 Hold | Continue holding |
| Stock momentum negative | LTP ≤ VWAP | 🔴 Exit | Book profits/exit |

---

## Conditional Flows

### 1. Trading Permission Flow

```
Check Index Trends
    ↓
IF (nifty_trend === banknifty_trend):
    ├─ Both Bullish → allow_trading = true, show_section = 'bullish'
    ├─ Both Bearish → allow_trading = true, show_section = 'bearish'
    └─ Display corresponding section only
    ↓
ELSE (opposite trends):
    ├─ allow_trading = false
    ├─ show_section = 'both'
    ├─ Display warning: "No Trade Applicable"
    ├─ Show both bullish and bearish sections
    └─ Highlight: "Wait for indices to align"
```

### 2. Data Display Flow

```
displaySectionsBasedOnTrends(index_check, bullish, bearish)
    ↓
Check index_check.allow_trading
    ↓
IF allow_trading === true:
    ├─ Hide warning banner
    ├─ IF show_section === 'bullish':
    │   ├─ Display bullish section
    │   └─ Hide bearish section
    ├─ ELSE IF show_section === 'bearish':
    │   ├─ Display bearish section
    │   └─ Hide bullish section
    └─ Update trend display
    ↓
ELSE (opposite trends):
    ├─ Show warning banner
    ├─ Display both sections
    └─ Allow user to see all data (informational)
```

**File:** `scan.js` (Lines 510-588)

### 3. Token Validation Flow

```
Any API Call
    ↓
Check Response
    ↓
IF status === 401 OR error_type === 'token_expired':
    ├─ Stop data loading
    ├─ Display expired token banner
    ├─ Open authentication popup
    └─ Offer OAuth or manual token entry
    ↓
User authenticates
    ↓
Token updated
    ↓
Page reloads
    ↓
Resume normal operation
```

---

## UI Components

### 1. Title Bar Components

**Location:** Top of page (fixed position)

- **Logo:** TradeManthan logo (tm_logo.png, 90px height)
- **Title:** "TradeManthan - Intraday Stock Options Algo"
- **Subtitle:** "by Bipul Sahay"
- **NIFTY Index:** Price + Trend Arrow
- **BANKNIFTY Index:** Price + Trend Arrow

**Update Frequency:** Every 30 seconds

### 2. Warning Banners

**A. Token Expired Banner**
```html
Display: When Upstox token is expired/invalid
Message: "Access Token Expired - Please update your Upstox API access token"
Action: "Update Token" button → Opens popup
Color: Orange/Yellow warning
```

**B. Opposite Trends Banner**
```html
Display: When NIFTY and BANKNIFTY have opposite trends
Message: "No Trade Applicable - Both NIFTY 50 & BANKNIFTY are in opposite direction"
Guidance: "Wait for indices to align"
Color: Red warning
Shows: Current trend of both indices
```

### 3. Alert Sections

**Bullish Section (Green Theme):**
- Title: 📈 BULLISH ALERTS (CALL)
- Download CSV button
- Time-grouped alerts
- Stock table with option details

**Bearish Section (Red Theme):**
- Title: 📉 BEARISH ALERTS (PUT)
- Download CSV button
- Time-grouped alerts
- Stock table with option details

### 4. Stock Alert Table Columns

| Column | Description | Source |
|--------|-------------|--------|
| # | Serial number | Frontend counter |
| Stock Name | Company symbol | Chartink webhook |
| Stock LTP | Last traded price | Upstox API |
| Stock VWAP | Volume weighted avg | Upstox calculation |
| Option Contract | Full contract name | Constructed (symbol+expiry+strike+type) |
| Qty | Number of contracts | Calculated (₹10k/option_ltp) |
| Buy Price | Entry price | Option LTP at alert time |
| Sell Price | Exit target | Buy price × 1.5 (50% profit) |
| PnL | Potential profit/loss | (Sell - Buy) × Qty |

### 5. Authentication Popup

**Trigger:** Token expired or manual click

**Components:**
- **OAuth Login Section (Recommended)**
  - Title: "Recommended: OAuth Login"
  - Button: "Login with Upstox"
  - Action: Redirect to OAuth flow
  
- **Manual Token Section (Alternative)**
  - Textarea: For pasting access token
  - Button: "Update Token Manually"
  - Action: POST token to backend

---

## Error Handling

### 1. API Error Scenarios

| Error Type | Trigger | Handler | User Action |
|------------|---------|---------|-------------|
| **401 Unauthorized** | Expired/invalid token | showTokenExpiredMessage() | Re-authenticate |
| **Network Error** | Connection failure | showIndexError() | Auto-retry in 30s |
| **No Data** | Empty webhook response | displayNoData() | Wait for alerts |
| **Invalid Response** | Malformed JSON | console.error() | Auto-retry in 30s |

### 2. Error Recovery Flow

```
Error Detected
    ↓
IF token_expired (401):
    ├─ Stop auto-refresh
    ├─ Show authentication UI
    └─ Wait for user action
    ↓
ELSE (network/temporary):
    ├─ Log error to console
    ├─ Display error state (--/?)
    ├─ Continue auto-refresh
    └─ Retry after 30 seconds
```

### 3. Graceful Degradation

**When index prices fail:**
- Display: `--` for prices
- Display: `?` for trend arrows
- Continue showing webhook alerts
- Auto-retry every 30 seconds

**When webhook data fails:**
- Display: "No Alerts Yet" message
- Display: Empty sections
- Continue showing index prices
- Auto-retry every 30 seconds

---

## Computational Workflows

### 1. Complete Alert Processing Workflow

```
┌─────────────────────────────────────────────────┐
│  CHARTINK WEBHOOK RECEIVED                      │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  PARSE WEBHOOK DATA                             │
│  - Extract stocks list                          │
│  - Extract trigger prices                       │
│  - Extract timestamp                            │
│  - Extract scan name                            │
└─────────────────────────────────────────────────┘
                    ↓
        FOR EACH STOCK IN LIST:
                    ↓
┌─────────────────────────────────────────────────┐
│  FETCH STOCK DATA (Upstox API)                  │
│  - Get current LTP                              │
│  - Calculate VWAP                               │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  DETERMINE OPTION TYPE                          │
│  IF alert is Bullish → CE (Call)                │
│  IF alert is Bearish → PE (Put)                 │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  FIND OTM-1 STRIKE                              │
│  - Get option chain from Upstox                 │
│  - Filter OTM strikes (CE: >LTP, PE: <LTP)      │
│  - Sort by proximity to LTP                     │
│  - Select first strike (OTM-1)                  │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  GET OPTION MARKET DATA                         │
│  - Fetch option LTP                             │
│  - Calculate option VWAP                        │
│  - Get volume and OI                            │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  CALCULATE TRADING PARAMETERS                   │
│  - Qty = ₹10,000 / option_ltp                   │
│  - Buy Price = option_ltp                       │
│  - Sell Price = option_ltp × 1.5                │
│  - PnL = (Sell - Buy) × Qty                     │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  STORE IN DATABASE                              │
│  - Table: IntradayStockOption                   │
│  - Store all calculated values                  │
│  - Timestamp: triggered_at                      │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  GROUP BY TIMESTAMP                             │
│  - Group stocks by triggered_at                 │
│  - Maintain chronological order                 │
│  - Store in memory (bullish_data/bearish_data)  │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  FRONTEND DISPLAYS DATA                         │
│  - Check index alignment                        │
│  - Display relevant section(s)                  │
│  - Show Hold/Exit signals                       │
│  - Enable CSV download                          │
└─────────────────────────────────────────────────┘
```

### 2. Index Trend Check Workflow

**IMPORTANT:** Index trends use **Candle Color** (LTP vs Day Open), not VWAP!

```
┌─────────────────────────────────────────────────┐
│  GET INDEX OHLC DATA (Upstox API)               │
│  - NIFTY 50: Day Open, LTP, High, Low          │
│  - BANKNIFTY: Day Open, LTP, High, Low         │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  DETERMINE CANDLE COLOR (TREND)                 │
│  nifty_trend = 'bullish' if LTP > Day Open     │
│               else 'bearish'                    │
│  (Green candle if LTP > Open, Red if LTP < Open)│
│                                                 │
│  banknifty_trend = 'bullish' if LTP > Day Open  │
│                    else 'bearish'               │
│  (Green candle if LTP > Open, Red if LTP < Open)│
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  CHECK ALIGNMENT                                │
│  IF nifty_trend === banknifty_trend:            │
│     allow_trading = TRUE                        │
│     show_section = trend (bullish/bearish)      │
│  ELSE:                                          │
│     allow_trading = FALSE                       │
│     show_section = 'both'                       │
└─────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────┐
│  UPDATE UI                                      │
│  - Display trend arrows                         │
│  - Show/hide warning banner                     │
│  - Filter alert sections                        │
└─────────────────────────────────────────────────┘
```

### 3. Auto-Refresh Workflow

```
startAutoRefresh()
    ↓
Set Interval: 30 seconds
    ↓
Every 30 seconds:
    ├─ loadIndexPrices()
    └─ loadLatestData()
    ↓
Continue until page closed or token expired
```

**File:** `scan.js` (Lines 130-150)

---

## Key Conditional Logic

### 1. Market Status Conditions

```javascript
if (market_status === 'open') {
    price = data.ltp;           // Use live trading price
} else {
    price = data.close_price;   // Use previous close
}
```

### 2. Trend-Based Display Conditions

```javascript
// Condition 1: Both Bullish
if (nifty_trend === 'bullish' && banknifty_trend === 'bullish') {
    - Display: Bullish section only
    - Hide: Bearish section
    - Hide: Warning banner
}

// Condition 2: Both Bearish
else if (nifty_trend === 'bearish' && banknifty_trend === 'bearish') {
    - Display: Bearish section only
    - Hide: Bullish section
    - Hide: Warning banner
}

// Condition 3: Opposite Trends
else {
    - Display: Both sections
    - Show: Warning banner
    - Message: "No Trade Applicable"
}
```

### 3. Hold/Exit Signal Conditions

```javascript
// For each stock in alert:
const ltp = stock.last_traded_price;
const vwap = stock.stock_vwap;

if (ltp > vwap) {
    // Positive momentum - stock moving up
    Signal: HOLD (green)
    Action: Continue position
}
else {
    // Negative momentum - stock moving down
    Signal: EXIT (red)
    Action: Book profits/exit
}
```

### 4. Token Validation Conditions

```javascript
// Check 1: HTTP Status
if (response.status === 401) {
    → Token expired
}

// Check 2: Error Type
if (result.error_type === 'token_expired') {
    → Token expired
}

// Check 3: Error String
if (error.includes('401') || error.includes('Unauthorized')) {
    → Token expired
}
```

---

## Integration Points

### 1. External Services

| Service | Purpose | API Used |
|---------|---------|----------|
| **Chartink.com** | Stock scanning & alerts | Webhook POST |
| **Upstox API** | Market data, option chains | REST API v2 |
| **Yahoo Finance** | VWAP calculation (fallback) | yfinance library |

### 2. Database Tables

**IntradayStockOption Model:**
```python
- id (Primary Key)
- stock_name (String)
- trigger_price (Float)
- last_traded_price (Float)
- stock_vwap (Float)
- option_contract (String)
- option_type (CE/PE)
- option_strike (Float)
- option_ltp (Float)
- option_vwap (Float)
- qty (Integer)
- buy_price (Float)
- sell_price (Float)
- pnl (Float)
- triggered_at (DateTime)
- created_at (DateTime)
- scan_name (String)
```

**Purpose:**
- Persist all webhook alerts
- Historical tracking
- Analytics and reporting
- CSV export functionality

---

## CSV Download Functionality

### 1. CSV Generation Flow

```
User clicks "Download CSV"
    ↓
downloadCSV(type)  // type = 'bullish' or 'bearish'
    ↓
Get current data (currentBullishData or currentBearishData)
    ↓
Flatten alerts array
    ↓
Format as CSV rows:
    - Header: Stock Name, LTP, VWAP, Option, Qty, Buy, Sell, PnL
    - Data rows: One per stock
    ↓
Create Blob with CSV content
    ↓
Generate download link
    ↓
Trigger download
    ↓
Filename: bullish_alerts_YYYYMMDD_HHMMSS.csv
          or bearish_alerts_YYYYMMDD_HHMMSS.csv
```

**File:** `scan.js` (Lines 600-700)

---

## Performance Optimizations

### 1. Caching Strategy

- **No-cache headers:** All API calls use Cache-Control: no-cache
- **Version control:** scan.js?v=timestamp prevents browser caching
- **In-memory storage:** Webhook data stored in backend memory for fast retrieval

### 2. Auto-Refresh Optimization

```javascript
// Avoid duplicate calls
if (tokenExpiredBanner.display === 'shown') {
    skip data loading;  // Don't call API if token expired
}

// Interval: 30 seconds (not too frequent, not too slow)
setInterval(() => {
    loadIndexPrices();
    loadLatestData();
}, 30000);
```

### 3. Conditional Loading

- Skip webhook data load if token expired
- Use cached data for CSV downloads
- Batch stock processing in backend

---

## Security Features

### 1. CSRF Protection (OAuth)

```python
# Generate random state token
state = secrets.token_urlsafe(32)
oauth_states[state] = {"timestamp": datetime.utcnow()}

# Validate on callback
if state not in oauth_states:
    return error("Invalid state parameter")
```

### 2. Token Storage

- **Never exposed to frontend:** Token stored in backend only
- **File-based persistence:** `upstox_service.py`
- **Memory update:** Immediate effect without restart

### 3. Input Validation

```python
# Webhook data validation
if not stocks or not trigger_prices:
    return error("Missing required fields")

# Stock name sanitization
stock_name = stock_name.strip().upper()

# Price validation
if trigger_price <= 0:
    skip stock
```

---

## Troubleshooting Guide

### 1. No Data Showing

**Check:**
1. Token status: Is banner showing "Token Expired"?
2. Console errors: Check for 401/network errors
3. Webhook configuration: Are Chartink webhooks set up?
4. Backend service: Is trademanthan-backend running?

**Solutions:**
- Re-authenticate with Upstox
- Check backend logs: `sudo journalctl -u trademanthan-backend -f`
- Verify webhook URLs in Chartink

### 2. Opposite Trends Warning

**Reason:** NIFTY and BANKNIFTY have different trend directions

**Understanding:**
- NIFTY bullish (↑) + BANKNIFTY bearish (↓) = Conflicting signals
- System suggests: Wait for alignment
- Data still visible: For informational purposes

**Action:** Wait for indices to align in same direction

### 3. Hold/Exit Signals

**Hold Signal (🟢):**
- Meaning: Stock LTP > VWAP (positive momentum)
- Action: Continue holding the position

**Exit Signal (🔴):**
- Meaning: Stock LTP ≤ VWAP (negative momentum)
- Action: Consider booking profits or exiting

---

## Configuration

### 1. Backend Configuration

**File:** `backend/routers/scan.py`

```python
MARGIN_PER_TRADE = 10000  # ₹10,000 per trade
PROFIT_TARGET = 1.5        # 50% profit target
OTM_LEVEL = 1              # OTM-1 strike selection
```

### 2. Frontend Configuration

**File:** `scan.js`

```javascript
API_BASE_URL = 'https://trademanthan.in'  // Production
             = 'http://localhost:8000'     // Local dev

AUTO_REFRESH_INTERVAL = 30000  // 30 seconds
OAUTH_SUCCESS_RELOAD_DELAY = 3000  // 3 seconds
```

### 3. Upstox API Configuration

**Required Environment Variables:**
```bash
UPSTOX_API_KEY=your_api_key
UPSTOX_API_SECRET=your_api_secret
UPSTOX_REDIRECT_URI=https://trademanthan.in/scan/upstox/callback
```

---

## Data Flow Summary

```
┌──────────────┐
│  Chartink    │ ──webhook──→ Backend
└──────────────┘              ↓
                         Process & Store
                              ↓
┌──────────────┐         In-Memory Cache
│   Upstox     │ ←──API calls──┘
│   API        │              ↓
└──────────────┘         Calculate Options
                              ↓
┌──────────────┐         Return JSON
│  Database    │ ←──persist── ↓
└──────────────┘              ↓
                         Frontend polls
                              ↓
                         Display to User
                              ↓
                         Auto-refresh (30s)
```

---

## Monitoring & Logging

### 1. Console Logging

**Frontend (scan.js):**
- Page load confirmation
- API responses
- Error messages
- OAuth button events
- Data loading status

**Backend (scan.py):**
- Webhook receipts
- Stock processing status
- Option chain lookups
- Token updates
- Authentication events

### 2. Log Locations

**Frontend:**
- Browser console (F12 → Console)

**Backend:**
```bash
# Service logs
sudo journalctl -u trademanthan-backend -f

# Nginx access logs
sudo tail -f /var/log/nginx/access.log

# Nginx error logs
sudo tail -f /var/log/nginx/error.log
```

---

## Best Practices

### 1. For Users

1. **Keep Token Fresh:** Re-authenticate when expired
2. **Monitor Index Trends:** Trade only when aligned
3. **Use Hold/Exit Signals:** Follow VWAP-based guidance
4. **Download CSV:** Keep records of daily alerts
5. **Hard Refresh:** Use Ctrl+Shift+R if data seems stale

### 2. For Developers

1. **Check Console First:** Errors are logged
2. **Monitor Auto-Refresh:** Ensure 30s interval is working
3. **Validate Webhooks:** Test with Chartink webhook tester
4. **Update Dependencies:** Keep Upstox SDK updated
5. **Test OAuth Flow:** Verify redirect URIs match

---

## Quick Reference

### Frontend Functions

| Function | Purpose | Returns |
|----------|---------|---------|
| `loadIndexPrices()` | Fetch NIFTY/BANKNIFTY data | Promise |
| `loadLatestData()` | Fetch webhook alerts | Promise |
| `checkOAuthSuccess()` | Handle OAuth return | void |
| `initiateUpstoxOAuth()` | Start OAuth flow | void (redirects) |
| `updateTokenFromPopup()` | Manual token update | Promise |
| `downloadCSV(type)` | Export alerts to CSV | void |
| `displaySectionsBasedOnTrends()` | Show/hide sections | void |
| `renderAlertGroup()` | Render time-grouped alerts | HTML string |

### Backend Endpoints Quick Reference

| Endpoint | Method | Auth | Purpose |
|----------|--------|------|---------|
| `/scan/index-prices` | GET | No | Get NIFTY/BANKNIFTY trends |
| `/scan/latest` | GET | No | Get latest alerts |
| `/scan/chartink-webhook-bullish` | POST | No | Receive bullish webhooks |
| `/scan/chartink-webhook-bearish` | POST | No | Receive bearish webhooks |
| `/scan/upstox/login` | GET | No | Initiate OAuth |
| `/scan/upstox/callback` | GET | No | Handle OAuth callback |
| `/scan/update-upstox-token` | POST | No | Manual token update |

---

## Version History

- **v1.0:** Initial scan page with webhook support
- **v2.0:** Added OAuth authentication
- **v3.0:** Added index trend checking
- **v4.0:** Added Hold/Exit signals based on VWAP
- **v5.0:** Current version with optimized UI and caching

---

## Support & Maintenance

**For Issues:**
1. Check browser console for errors
2. Verify backend service is running
3. Check Upstox token validity
4. Review Chartink webhook configuration

**Backend Service Commands:**
```bash
# Check status
sudo systemctl status trademanthan-backend

# View logs
sudo journalctl -u trademanthan-backend -f

# Restart service
sudo systemctl restart trademanthan-backend
```

**Contact:** Bipul Sahay  
**Repository:** https://github.com/bipulsin/trademanthan

---

*Last Updated: November 2, 2025*

