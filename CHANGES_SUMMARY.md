# Major Changes: Replacing Momentum Filter with VWAP Slope + Candle Size Filters

## Overview
This document summarizes the major changes made to replace the momentum filter with VWAP slope and candle size filters for trade entry decisions.

## Changes Made

### 1. Backend Services (`backend/services/upstox_service.py`)
**Added Methods:**
- `get_stock_vwap_for_previous_hour(stock_symbol)` - Fetches stock VWAP from previous 1-hour candle
- `get_option_candles_current_and_previous(instrument_key)` - Fetches current and previous 1-hour OHLC candles for options

### 2. Database Model (`backend/models/trading.py`)
**New Fields Added to `IntradayStockOption`:**
- `stock_vwap_previous_hour` - Float, nullable
- `stock_vwap_previous_hour_time` - DateTime, nullable
- `option_current_candle_open` - Float, nullable
- `option_current_candle_high` - Float, nullable
- `option_current_candle_low` - Float, nullable
- `option_current_candle_close` - Float, nullable
- `option_current_candle_time` - DateTime, nullable
- `option_previous_candle_open` - Float, nullable
- `option_previous_candle_high` - Float, nullable
- `option_previous_candle_low` - Float, nullable
- `option_previous_candle_close` - Float, nullable
- `option_previous_candle_time` - DateTime, nullable

### 3. Webhook Processing (`backend/routers/scan.py`)
**Changes:**
- **Removed**: Momentum filter (0.3% minimum momentum check)
- **Added**: 
  - Fetches option OHLC candles (current and previous) during stock enrichment
  - Fetches previous hour stock VWAP during stock enrichment
  - VWAP slope filter: Uses `vwap_slope()` method to check if slope >= 45 degrees
  - Candle size filter: Checks if current candle size < 7.5× previous candle size

**New Entry Conditions:**
1. Time check (before 3:00 PM)
2. Index trends alignment
3. **VWAP slope >= 45 degrees** (NEW)
4. **Current candle size < 7.5× previous candle** (NEW)
5. Valid option data

### 4. Hourly Updater (`backend/services/vwap_updater.py`)
**Changes:**
- Updated re-evaluation logic for `no_entry` trades to use new filters
- Fetches previous hour VWAP if not stored
- Fetches option candles and checks size
- Updates database with OHLC data

## Entry Filter Logic

### VWAP Slope Filter
- Uses `vwap_slope()` method with:
  - Previous hour VWAP and time
  - Current hour VWAP and time
- Returns "Yes" if angle >= 45 degrees
- Both upward and downward slopes are considered

### Candle Size Filter
- Calculates candle size: `High - Low`
- Compares current candle size to previous candle size
- Passes if: `current_size / previous_size < 7.5`
- Threshold: 7.5 (middle of 7-8 range)

## Database Migration Required

**IMPORTANT**: Database schema needs to be updated. Run migration or ALTER TABLE statements:

```sql
ALTER TABLE intraday_stock_options 
ADD COLUMN stock_vwap_previous_hour FLOAT,
ADD COLUMN stock_vwap_previous_hour_time TIMESTAMP,
ADD COLUMN option_current_candle_open FLOAT,
ADD COLUMN option_current_candle_high FLOAT,
ADD COLUMN option_current_candle_low FLOAT,
ADD COLUMN option_current_candle_close FLOAT,
ADD COLUMN option_current_candle_time TIMESTAMP,
ADD COLUMN option_previous_candle_open FLOAT,
ADD COLUMN option_previous_candle_high FLOAT,
ADD COLUMN option_previous_candle_low FLOAT,
ADD COLUMN option_previous_candle_close FLOAT,
ADD COLUMN option_previous_candle_time TIMESTAMP;
```

## Frontend Updates Needed

1. **Display Entry Criteria**:
   - Replace "Momentum" with "VWAP Slope" and "Candle Size"
   - Show VWAP slope angle and decision
   - Show candle size ratio and decision

2. **Trade Details Page**:
   - Display previous hour VWAP and time
   - Display option OHLC candles (current and previous)
   - Show candle size calculation

3. **No Entry Reasons**:
   - Update to show VWAP slope and candle size reasons instead of momentum

## Testing Checklist

- [ ] Test webhook processing with new filters
- [ ] Verify OHLC data is fetched correctly
- [ ] Verify previous hour VWAP is fetched correctly
- [ ] Test VWAP slope calculation with various scenarios
- [ ] Test candle size filter with various ratios
- [ ] Test re-evaluation of no_entry trades
- [ ] Verify database fields are populated correctly
- [ ] Test frontend display of new criteria

## Notes

- The momentum filter has been completely removed
- Both VWAP slope AND candle size filters must pass for entry
- Previous hour VWAP is fetched if not already stored
- Option candles are fetched using stored `instrument_key`
- All OHLC data is stored in database for analysis

