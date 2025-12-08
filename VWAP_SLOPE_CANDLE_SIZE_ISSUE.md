# Issue: VWAP Slope & Candle Size Missing for Today's Trades

## Root Causes

### 1. **Candle Size Missing**

**Problem**: Candle size is only calculated during webhook processing when `option_candles` is available.

**Why it fails**:
- Candle size calculation requires `instrument_key` to fetch option candles
- If option chain API fails (e.g., for IGL), `instrument_key` is `None`
- Without `instrument_key`, `get_option_daily_candles_current_and_previous()` cannot be called
- Result: `option_candles = None` → No candle size calculation → `candle_size_ratio = None`, `candle_size_status = None`

**Code Flow**:
```python
# In process_webhook_data():
if option_contract and instrument_key:
    option_candles = vwap_service.get_option_daily_candles_current_and_previous(instrument_key)
    # Calculate candle_size_ratio from option_candles
else:
    option_candles = None  # No candle size calculation possible
```

**When it happens**:
- Option chain API returns empty data (e.g., IGL)
- Option chain API fails or times out
- Stock doesn't have options available

### 2. **VWAP Slope Missing**

**Problem**: VWAP slope is calculated by cycle-based scheduler, not during webhook processing.

**Why it might be missing**:
- Cycle scheduler runs at specific times: 10:30, 11:15, 12:15, 13:15, 14:15
- If trades are created after these times, VWAP slope won't be calculated until next cycle
- If cycle scheduler fails or errors occur, VWAP slope won't be calculated
- Trades need specific status (`no_entry` or from current cycle's alert time) to be processed

**Code Flow**:
```python
# In process_webhook_data():
vwap_slope_reason = "VWAP slope will be calculated in cycle-based scheduler"
# VWAP slope is NOT calculated here

# In calculate_vwap_slope_for_cycle():
# Only processes trades that match cycle criteria
# Updates: vwap_slope_status, vwap_slope_angle, vwap_slope_direction
```

## Solutions

### Solution 1: Calculate Candle Size in Cycle Scheduler (Recommended)

**Approach**: Add candle size calculation to `calculate_vwap_slope_for_cycle()` function.

**Benefits**:
- Retries candle size calculation even if it failed during webhook
- Can fetch option candles once `instrument_key` becomes available
- Ensures candle size is calculated for all trades

**Implementation**:
```python
# In calculate_vwap_slope_for_cycle():
# After determining option_contract and instrument_key:
if trade.instrument_key and not trade.candle_size_ratio:
    option_candles = vwap_service.get_option_daily_candles_current_and_previous(trade.instrument_key)
    if option_candles:
        # Calculate and save candle_size_ratio and candle_size_status
```

### Solution 2: Fallback Candle Size Calculation

**Approach**: Try to fetch option candles even when option chain fails, using alternative methods.

**Implementation**:
- If option chain fails, try to find option contract from master_stock table
- Use master_stock data to construct instrument_key
- Fetch candles using constructed instrument_key

### Solution 3: Manual Trigger Endpoint

**Approach**: Create endpoint to manually trigger VWAP slope and candle size calculation for today's trades.

**Implementation**:
```python
@router.post("/recalculate-filters-today")
async def recalculate_filters_today(db: Session = Depends(get_db)):
    """Recalculate VWAP slope and candle size for all today's trades"""
    # Fetch all today's trades
    # For each trade:
    #   - Calculate VWAP slope if missing
    #   - Calculate candle size if missing
```

## Immediate Actions

1. **Check Scheduler Status**: ✅ Already verified - schedulers are running
2. **Check Today's Trades**: Query database to see which trades are missing VWAP slope/candle size
3. **Check Logs**: Look for errors in cycle scheduler execution
4. **Implement Solution 1**: Add candle size calculation to cycle scheduler

## Verification

After implementing fixes, verify:
- All today's trades have `candle_size_ratio` and `candle_size_status`
- All today's trades have `vwap_slope_angle` and `vwap_slope_status` (after cycle runs)
- Cycle scheduler logs show successful execution
- No errors in fetching option candles

