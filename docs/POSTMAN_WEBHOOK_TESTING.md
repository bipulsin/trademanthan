# Postman Webhook Testing Guide

## Overview
This guide explains how to manually test the Chartink webhook endpoints using Postman to verify the full enrichment flow.

## Server Information
- **Base URL**: `http://13.234.119.21:8000`
- **Endpoints**:
  - Bullish: `/scan/chartink-webhook-bullish`
  - Bearish: `/scan/chartink-webhook-bearish`
  - Auto-detect: `/scan/chartink-webhook`

---

## Step-by-Step Instructions

### Step 1: Open Postman
1. Launch Postman application
2. Create a new request (or use existing collection)

### Step 2: Configure Request
1. **Method**: Select `POST`
2. **URL**: Enter one of the following:
   - For Bullish alerts: `http://13.234.119.21:8000/scan/chartink-webhook-bullish`
   - For Bearish alerts: `http://13.234.119.21:8000/scan/chartink-webhook-bearish`
   - For Auto-detect: `http://13.234.119.21:8000/scan/chartink-webhook`

### Step 3: Set Headers
1. Click on **Headers** tab
2. Add the following header:
   - **Key**: `Content-Type`
   - **Value**: `application/json`

### Step 4: Set Request Body
1. Click on **Body** tab
2. Select **raw** radio button
3. Select **JSON** from the dropdown (on the right)
4. Paste one of the example payloads below

### Step 5: Send Request
1. Click **Send** button
2. You should receive a `202 Accepted` response immediately (processing happens in background)

### Step 6: Verify Results
1. Check the response - should show:
   ```json
   {
     "status": "accepted",
     "message": "Bullish webhook received and queued for processing",
     "alert_type": "bullish",
     "timestamp": "2025-12-11T..."
   }
   ```
2. Check backend logs: `ssh ubuntu@13.234.119.21 "tail -f /tmp/uvicorn.log"`
3. Check database: Query `intraday_stock_options` table for the stocks you sent
4. Check frontend: Visit `http://13.234.119.21:8000/scan.html` to see the stocks

---

## Example Payloads

### Example 1: Bullish Alert (Single Stock)
```json
{
  "stocks": "RELIANCE",
  "trigger_prices": "2450.50",
  "triggered_at": "10:15 am",
  "scan_name": "Bullish Breakout",
  "scan_url": "bullish-breakout",
  "alert_name": "Alert for Bullish Breakout"
}
```

### Example 2: Bullish Alert (Multiple Stocks)
```json
{
  "stocks": "RELIANCE,TATAMOTORS,INFY",
  "trigger_prices": "2450.50,850.25,1850.75",
  "triggered_at": "11:15 am",
  "scan_name": "Bullish Intraday Stock",
  "scan_url": "bullish-intraday-stock",
  "alert_name": "Alert for Bullish Intraday Stock"
}
```

### Example 3: Bearish Alert (Single Stock)
```json
{
  "stocks": "HDFCBANK",
  "trigger_prices": "1650.00",
  "triggered_at": "2:15 pm",
  "scan_name": "Bearish Breakdown",
  "scan_url": "bearish-breakdown",
  "alert_name": "Alert for Bearish Breakdown"
}
```

### Example 4: Bearish Alert (Multiple Stocks)
```json
{
  "stocks": "ICICIBANK,AXISBANK,KOTAKBANK",
  "trigger_prices": "950.50,1150.25,2180.00",
  "triggered_at": "12:15 pm",
  "scan_name": "Bearish Intraday Stock",
  "scan_url": "bearish-intraday-stock",
  "alert_name": "Alert for Bearish Intraday Stock"
}
```

### Example 5: Test with Today's Stocks (Real Data)
```json
{
  "stocks": "MOTHERSON,HINDZINC,KOTAKBANK",
  "trigger_prices": "118.47,529.8,2184.9",
  "triggered_at": "10:15 am",
  "scan_name": "Test Bullish Alert",
  "scan_url": "test-bullish",
  "alert_name": "Test Alert for Bullish"
}
```

---

## Field Descriptions

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `stocks` | String | Yes | Comma-separated list of stock symbols (e.g., "RELIANCE,TATAMOTORS") |
| `trigger_prices` | String | Yes | Comma-separated list of trigger prices matching stock order (e.g., "2450.50,850.25") |
| `triggered_at` | String | Yes | Time when alert was triggered (e.g., "10:15 am", "2:34 pm") |
| `scan_name` | String | Yes | Name of the scan (e.g., "Bullish Breakout") |
| `scan_url` | String | No | URL slug for the scan (e.g., "bullish-breakout") |
| `alert_name` | String | No | Name of the alert (e.g., "Alert for Bullish Breakout") |

---

## What to Verify After Sending

### 1. Immediate Response (202 Accepted)
- Status code should be `202`
- Response should indicate webhook was accepted

### 2. Backend Processing (Check Logs)
```bash
ssh -i /path/to/TradeM.pem ubuntu@13.234.119.21 "tail -100 /tmp/uvicorn.log | grep -A 5 'Processing stock'"
```

Look for:
- ✅ Stock LTP fetched
- ✅ Stock VWAP fetched
- ✅ Option contract found
- ✅ Instrument key found
- ✅ Option LTP fetched

### 3. Database Verification
```bash
ssh -i /path/to/TradeM.pem ubuntu@13.234.119.21 "cd /home/ubuntu/trademanthan && python3 -c \"
from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from datetime import datetime
import pytz

db = SessionLocal()
ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)

trades = db.query(IntradayStockOption).filter(
    IntradayStockOption.trade_date >= today
).order_by(IntradayStockOption.alert_time.desc()).limit(5).all()

for t in trades:
    print(f'{t.stock_name}: stock_vwap={t.stock_vwap}, option_contract={t.option_contract}, instrument_key={t.instrument_key}, status={t.status}')
    
db.close()
\""
```

### 4. Frontend Verification
- Visit: `http://13.234.119.21:8000/scan.html`
- Check if stocks appear in the dashboard
- Verify:
  - Stock LTP is populated
  - Stock VWAP is populated (should not be 0.0)
  - Option contract is shown
  - Option LTP is populated (if instrument_key is available)
  - Status is correct (not "Enrichment failed")

---

## Common Issues & Troubleshooting

### Issue 1: 500 Internal Server Error
**Cause**: Invalid JSON or missing required fields
**Solution**: Verify JSON syntax and ensure all required fields are present

### Issue 2: Stocks show "Enrichment failed"
**Possible Causes**:
- Upstox token expired
- Network connectivity issues
- Stock symbol not found in instruments file

**Check**:
```bash
# Check Upstox token status
curl http://13.234.119.21:8000/scan/upstox/status

# Check backend logs
ssh -i /path/to/TradeM.pem ubuntu@13.234.119.21 "tail -200 /tmp/uvicorn.log | grep -i 'error\|enrichment\|failed'"
```

### Issue 3: Stock VWAP = 0.0
**Possible Causes**:
- Import path issue (should be fixed now)
- Upstox API failure
- Stock not found in instruments file

**Check**:
```bash
# Test VWAP fetch directly
ssh -i /path/to/TradeM.pem ubuntu@13.234.119.21 "cd /home/ubuntu/trademanthan && python3 -c \"
from backend.services.upstox_service import upstox_service
result = upstox_service.get_stock_ltp_and_vwap('RELIANCE')
print(result)
\""
```

### Issue 4: instrument_key = None
**Possible Causes**:
- Option contract not found
- Instruments JSON file outdated
- Option contract format mismatch

**Check**:
```bash
# Verify option contract exists
ssh -i /path/to/TradeM.pem ubuntu@13.234.119.21 "cd /home/ubuntu/trademanthan && python3 -c \"
import json
from pathlib import Path

instruments_file = Path('/home/ubuntu/trademanthan/data/instruments/nse_instruments.json')
with open(instruments_file, 'r') as f:
    data = json.load(f)
    
# Search for RELIANCE options
reliance = [i for i in data if 'RELIANCE' in str(i.get('underlying_symbol', '')) and i.get('instrument_type') == 'CE']
print(f'Found {len(reliance)} RELIANCE CE options')
if reliance:
    print(f'Sample: {reliance[0].get(\"trading_symbol\")}, instrument_key={reliance[0].get(\"instrument_key\")}')
\""
```

---

## Testing Checklist

- [ ] Request sent successfully (202 Accepted)
- [ ] Backend logs show processing started
- [ ] Stock LTP is populated (not trigger price)
- [ ] Stock VWAP is populated (not 0.0)
- [ ] Option contract is found and populated
- [ ] instrument_key is populated (not None)
- [ ] Option LTP is populated (if instrument_key available)
- [ ] Status is correct (not "Enrichment failed")
- [ ] Stock appears in frontend dashboard
- [ ] Historical market data is saved (check `historical_market_data` table)

---

## Quick Test Commands

### Test Bullish Webhook via cURL
```bash
curl -X POST "http://13.234.119.21:8000/scan/chartink-webhook-bullish" \
  -H "Content-Type: application/json" \
  -d '{
    "stocks": "RELIANCE",
    "trigger_prices": "2450.50",
    "triggered_at": "10:15 am",
    "scan_name": "Test Bullish",
    "scan_url": "test-bullish",
    "alert_name": "Test Alert"
  }'
```

### Test Bearish Webhook via cURL
```bash
curl -X POST "http://13.234.119.21:8000/scan/chartink-webhook-bearish" \
  -H "Content-Type: application/json" \
  -d '{
    "stocks": "HDFCBANK",
    "trigger_prices": "1650.00",
    "triggered_at": "2:15 pm",
    "scan_name": "Test Bearish",
    "scan_url": "test-bearish",
    "alert_name": "Test Alert"
  }'
```

---

## Notes

1. **Processing is Asynchronous**: The webhook returns immediately (202 Accepted) and processes in the background. Allow 10-30 seconds for full enrichment.

2. **Time Format**: The `triggered_at` field accepts various formats:
   - "10:15 am"
   - "2:34 pm"
   - "10:15 AM"
   - "14:34"
   The system will normalize it to the nearest Chartink schedule time (10:15 AM, 11:15 AM, 12:15 PM, 1:15 PM, 2:15 PM, 3:15 PM)

3. **Stock Symbols**: Use NSE stock symbols (e.g., "RELIANCE", "TATAMOTORS", not "RELIANCE.NS")

4. **Price Matching**: Ensure the number of stocks matches the number of trigger prices (comma-separated counts must match)

5. **Market Hours**: For best results, test during market hours (9:15 AM - 3:30 PM IST) when live data is available.

