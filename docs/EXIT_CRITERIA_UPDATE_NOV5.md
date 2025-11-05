# Exit Criteria Update - Directional VWAP Cross

**Date:** November 5, 2025  
**Feature:** New directional exit condition based on underlying stock VWAP  
**Priority:** 3 (Between Stop Loss and Profit Target)

---

## 📋 Summary

A new **4th exit condition** has been added to the scan algorithm. The system now automatically exits option trades when the underlying stock crosses VWAP in the **opposite direction** of the trade:

- **Bullish trades (CE):** Exit when stock closes **BELOW** VWAP
- **Bearish trades (PE):** Exit when stock closes **ABOVE** VWAP

---

## 🎯 What Changed?

### Before (3 Exit Conditions)
1. ⏰ **Time-based Exit** - At 3:25 PM
2. 🛑 **Stop Loss** - Option price drops to stop loss level
3. 🎯 **Profit Target** - Option price reaches 50% gain

### After (4 Exit Conditions)
1. ⏰ **Time-based Exit** - At 3:25 PM
2. 🛑 **Stop Loss** - Option price drops to stop loss level
3. 📉 **VWAP Cross (Directional)** - ⭐ **NEW!** Stock crosses VWAP opposite to trade direction
4. 🎯 **Profit Target** - Option price reaches 50% gain

---

## 📊 How It Works

### Exit Logic (Directional)

The system now checks if the **underlying stock's LTP** (Last Traded Price) crosses its **1-hour VWAP** in the direction **opposite** to the option trade during the hourly refresh cycle.

```python
# TIME RESTRICTION: Only check from 11:15 AM onwards (10:15 AM is entry time)
IF current_time >= 11:15 AM:
    
    # For Bullish trades (CE/CALL options)
    IF option_type == 'CE' AND stock_ltp < stock_vwap:
        → Exit the option position (lost bullish momentum)
        → Mark exit_reason as 'stock_vwap_cross'
        → Record P&L at exit time

    # For Bearish trades (PE/PUT options)
    IF option_type == 'PE' AND stock_ltp > stock_vwap:
        → Exit the option position (lost bearish momentum)
        → Mark exit_reason as 'stock_vwap_cross'
        → Record P&L at exit time
```

### Example Scenarios

#### Scenario 1: Bullish Trade (CE/CALL Option)

**Stock Alert:** RELIANCE (Bullish - CALL option)

| Time | Stock LTP | Stock VWAP | Option LTP | Action |
|------|-----------|------------|------------|--------|
| 10:15 AM | ₹2,450 | ₹2,448 | ₹25.50 | Entry (Stock > VWAP ✅ Bullish) |
| 11:15 AM | ₹2,455 | ₹2,450 | ₹28.00 | Hold (Stock > VWAP ✅ Bullish) |
| 12:15 PM | ₹2,445 | ₹2,448 | ₹26.50 | **EXIT** (Stock < VWAP ❌ Lost bullish momentum) |

**Result:**
- Exit triggered at 12:15 PM
- Reason: Stock closed **below** VWAP (₹2,445 < ₹2,448)
- Option Type: CE (Bullish)
- Exit price: ₹26.50
- P&L: qty × (₹26.50 - ₹25.50) = Small profit preserved

#### Scenario 2: Bearish Trade (PE/PUT Option)

**Stock Alert:** TATAMOTORS (Bearish - PUT option)

| Time | Stock LTP | Stock VWAP | Option LTP | Action |
|------|-----------|------------|------------|--------|
| 10:15 AM | ₹945 | ₹948 | ₹18.50 | Entry (Stock < VWAP ✅ Bearish) |
| 11:15 AM | ₹940 | ₹947 | ₹22.00 | Hold (Stock < VWAP ✅ Bearish) |
| 12:15 PM | ₹952 | ₹948 | ₹16.00 | **EXIT** (Stock > VWAP ❌ Lost bearish momentum) |

**Result:**
- Exit triggered at 12:15 PM
- Reason: Stock closed **above** VWAP (₹952 > ₹948)
- Option Type: PE (Bearish)
- Exit price: ₹16.00
- P&L: qty × (₹16.00 - ₹18.50) = Small loss, but prevented larger loss

---

## 🔍 Why This Directional Exit Condition?

### Rationale

**Momentum Indicator:** VWAP represents the average price weighted by volume throughout the day. It acts as a dynamic support/resistance level.

#### For Bullish Trades (CE/CALL)
- **Entry Logic:** You buy CALL when stock shows bullish momentum (typically above VWAP)
- **Expected:** Stock should stay **above VWAP** (buyers in control)
- **Exit Signal:** If stock closes **below VWAP** → Bullish momentum lost → Exit

#### For Bearish Trades (PE/PUT)
- **Entry Logic:** You buy PUT when stock shows bearish momentum (typically below VWAP)
- **Expected:** Stock should stay **below VWAP** (sellers in control)
- **Exit Signal:** If stock closes **above VWAP** → Bearish momentum lost → Exit

### Risk Management Benefits

**Directional Consistency:** The exit condition matches the trade direction:
- Bullish trade expects bullish momentum → Exit when momentum turns bearish
- Bearish trade expects bearish momentum → Exit when momentum turns bullish

**Early Warning System:** Exits when the underlying stock's behavior contradicts your trade thesis:
- **CE holders:** Stock weakness (below VWAP) indicates potential call option decay
- **PE holders:** Stock strength (above VWAP) indicates potential put option decay

**Prevents Counter-Momentum Holding:**
- Don't hold CALLs when stock turns weak
- Don't hold PUTs when stock turns strong
- Exit before larger losses occur

---

## 🔄 When Is This Checked?

### Hourly Refresh Cycle

The exit conditions are evaluated during the **hourly refresh** at these times:

- **10:15 AM** - Entry time, VWAP exit **NOT CHECKED** ⏭️
- **11:15 AM** - VWAP exit check **STARTS** ✅
- **12:15 PM** - VWAP exit check active ✅
- **1:15 PM** - VWAP exit check active ✅
- **2:15 PM** - VWAP exit check active ✅
- **3:15 PM** - Last VWAP exit check ✅
- **3:25 PM** - Time-based exit (all remaining positions) ⏰

**Endpoint:** `POST /scan/refresh-hourly`

**Time Window for VWAP Exit:**
- VWAP exit is **ONLY checked from 11:15 AM onwards**
- 10:15 AM is the first trade entry time, so we skip VWAP exit check at 10:15 AM
- This prevents exiting trades immediately after entry

### What Gets Updated

1. ✅ Fetches latest **stock LTP** from Upstox
2. ✅ Fetches latest **stock VWAP** (1-hour candle)
3. ✅ Fetches latest **option LTP**
4. ✅ Checks all 4 exit conditions in priority order
5. ✅ Updates P&L and exit reason if condition met

---

## 📁 Technical Implementation

### Files Modified

#### 1. Backend Router (`backend/routers/scan.py`)

**Function:** `refresh_hourly_prices()`

**Location:** Lines 1298-1306

**Code Added:**
```python
# 3. Check if underlying stock crosses VWAP (directional - based on option type)
# For CE (Bullish): Exit when stock closes BELOW VWAP (lost bullish momentum)
# For PE (Bearish): Exit when stock closes ABOVE VWAP (lost bearish momentum)
elif record.stock_ltp and record.stock_vwap and record.option_type:
    should_exit_vwap = False
    exit_direction = ""
    
    if record.option_type == 'CE' and record.stock_ltp < record.stock_vwap:
        # Bullish trade: stock went below VWAP (bearish signal)
        should_exit_vwap = True
        exit_direction = "below"
    elif record.option_type == 'PE' and record.stock_ltp > record.stock_vwap:
        # Bearish trade: stock went above VWAP (bullish signal)
        should_exit_vwap = True
        exit_direction = "above"
    
    if should_exit_vwap:
        record.sell_price = new_option_ltp
        record.sell_time = now
        record.exit_reason = 'stock_vwap_cross'
        record.status = 'sold'
        if record.buy_price and record.qty:
            record.pnl = (new_option_ltp - record.buy_price) * record.qty
        print(f"📉 VWAP CROSS EXIT for {record.stock_name} ({record.option_type}): Stock LTP=₹{record.stock_ltp} {exit_direction} VWAP=₹{record.stock_vwap}, Option PnL=₹{record.pnl}")
```

#### 2. Database Model (`backend/models/trading.py`)

**Line:** 62

**Updated Comment:**
```python
exit_reason = Column(String(50), nullable=True)  # 'profit_target', 'stop_loss', 'time_based', 'stock_vwap_cross', 'manual'
```

#### 3. Documentation (`docs/SCAN_FUNCTIONAL_GUIDE.md`)

**Section Added:** "3. Automated Exit Conditions (Hourly Refresh)"

Complete documentation of all 4 exit conditions with examples and priority order.

---

## 🎚️ Exit Priority Order

The system evaluates exit conditions in this **strict order**:

```
1. Time-based (3:25 PM)
   ↓ (if not triggered)
2. Stop Loss (Option LTP ≤ Stop Loss)
   ↓ (if not triggered)
3. VWAP Cross (Directional) ⭐ NEW!
   - CE: Stock LTP < Stock VWAP
   - PE: Stock LTP > Stock VWAP
   ↓ (if not triggered)
4. Profit Target (Option LTP ≥ 1.5 × Buy Price)
   ↓ (if not triggered)
5. Continue holding (update current P&L only)
```

**Important:** Only the **first matching condition** triggers the exit.

---

## 💾 Database Storage

### Exit Reason Values

The `exit_reason` column in `intraday_stock_options` table now accepts:

| Exit Reason | Description |
|-------------|-------------|
| `time_based` | Exited at 3:25 PM (market close) |
| `stop_loss` | Stop loss price hit |
| `stock_vwap_cross` | ⭐ **NEW!** Stock crossed VWAP opposite to trade direction |
| `profit_target` | 50% profit target achieved |
| `manual` | Manual exit by user |
| `null` | Position still open |

---

## 📊 Reporting & Analytics

### CSV Export

When downloading CSV from the scan page, the `exit_reason` column will show:

- `stock_vwap_cross` for positions exited due to this new condition
- Clear visibility of why each trade was exited
- Better analytics for strategy performance

### Example CSV Rows

```csv
Stock Name,Option Contract,Option Type,Buy Price,Sell Price,Exit Reason,PnL
RELIANCE,RELIANCE-Nov2025-2500-CE,CE,25.50,26.50,stock_vwap_cross,250.00
TATAMOTORS,TATAMOTORS-Nov2025-940-PE,PE,18.50,16.00,stock_vwap_cross,-625.00
```

---

## 🧪 Testing Scenarios

### Test Case 1: CE Trade - Stock Above VWAP (Hold)
- Option Type: CE (Bullish)
- Stock LTP: ₹2,450
- Stock VWAP: ₹2,448
- **Result:** No exit (Stock > VWAP, bullish momentum intact)

### Test Case 2: CE Trade - Stock Below VWAP (Exit)
- Option Type: CE (Bullish)
- Stock LTP: ₹2,445
- Stock VWAP: ₹2,448
- **Result:** Exit triggered with reason `stock_vwap_cross` (lost bullish momentum)

### Test Case 3: PE Trade - Stock Below VWAP (Hold)
- Option Type: PE (Bearish)
- Stock LTP: ₹945
- Stock VWAP: ₹948
- **Result:** No exit (Stock < VWAP, bearish momentum intact)

### Test Case 4: PE Trade - Stock Above VWAP (Exit)
- Option Type: PE (Bearish)
- Stock LTP: ₹952
- Stock VWAP: ₹948
- **Result:** Exit triggered with reason `stock_vwap_cross` (lost bearish momentum)

### Test Case 5: Priority Test (Stop Loss vs VWAP Cross)
- Scenario: Both stop loss AND VWAP cross conditions met
- Option Type: CE
- Stop Loss: Option LTP = ₹20 (Stop Loss = ₹22)
- VWAP Cross: Stock LTP = ₹2,445 (VWAP = ₹2,448)
- **Result:** Exit with reason `stop_loss` (higher priority)

### Test Case 6: Edge Case (Missing Data)
- Option Type: CE
- Stock LTP: ₹2,450
- Stock VWAP: 0.0 (not available)
- **Result:** No exit (condition requires stock_ltp, stock_vwap, and option_type)

---

## 🚀 Deployment

### No Migration Required

- ✅ `exit_reason` column already exists as `String(50)`
- ✅ No database schema changes needed
- ✅ Code changes are backward compatible
- ✅ Existing data remains valid

### Deployment Steps

1. ✅ Code updated in `scan.py`
2. ✅ Documentation updated
3. ✅ Model comment updated
4. ✅ Ready to deploy

**Deployment Command:**
```bash
cd /home/ubuntu/trademanthan
git pull origin main
sudo systemctl restart trademanthan-backend
```

---

## 📈 Expected Impact

### Benefits

1. **Better Risk Management**
   - Exits positions early when momentum turns negative
   - Protects gains before they become losses

2. **Improved Win Rate**
   - Avoids holding through reversals
   - Exits weak positions proactively

3. **Cleaner Trade Management**
   - Clear exit signal based on underlying strength
   - Complements existing exit conditions

### Metrics to Monitor

After deployment, track:

1. **Exit Reason Distribution**
   - How many trades exit due to `stock_below_vwap`?
   - Is it triggering appropriately?

2. **P&L by Exit Reason**
   - Average P&L for `stock_below_vwap` exits
   - Compare with other exit reasons

3. **Win Rate Impact**
   - Overall win rate before vs after
   - False exit rate (exited but stock recovered)

---

## 🔧 Configuration

### Current Settings (Hardcoded)

```python
# No configuration needed - uses real-time stock LTP and VWAP
# Condition: stock_ltp < stock_vwap
```

### Future Enhancement Options

If needed, could add:
- Threshold: `stock_ltp < (stock_vwap * 0.99)` (1% buffer)
- Time delay: Only exit if below VWAP for 2 consecutive checks
- Option-type specific: Apply only to certain option types

---

## 📝 User Communication

### What Users Will See

1. **Exit Logs:**
   ```
   📉 VWAP CROSS EXIT for RELIANCE (CE): Stock LTP=₹2,445 below VWAP=₹2,448, Option PnL=₹250.00
   📉 VWAP CROSS EXIT for TATAMOTORS (PE): Stock LTP=₹952 above VWAP=₹948, Option PnL=₹-625.00
   ```

2. **Database Records:**
   - `exit_reason = 'stock_vwap_cross'`
   - `sell_price` = Option LTP at exit time
   - `sell_time` = Timestamp of exit
   - `option_type` = CE or PE (determines direction)

3. **CSV Downloads:**
   - Clear exit reason in exported data
   - Option type visible for context
   - P&L recorded at exit time

---

## 🎓 Training & Documentation

### For Users

**Key Message:**
> "The system now automatically exits your option positions based on directional VWAP momentum:
> - **CALL options (CE)**: Exits when stock closes below VWAP (lost bullish momentum)
> - **PUT options (PE)**: Exits when stock closes above VWAP (lost bearish momentum)
> 
> This helps protect your gains and avoid holding positions when the underlying stock moves against your trade direction."

### For Support Team

**Common Questions:**

**Q: Why did my CALL option exit even though it was profitable?**  
A: The underlying stock closed below VWAP, indicating the stock lost bullish momentum. For CALL options, we want the stock to stay strong (above VWAP). This is a protective exit to preserve gains before potential reversal.

**Q: Why did my PUT option exit when the stock went up?**  
A: The underlying stock closed above VWAP, indicating the stock gained strength. For PUT options, we want the stock to stay weak (below VWAP). This exit prevents holding PUTs when the stock reverses upward.

**Q: Can I disable the VWAP exit condition?**  
A: Currently, this is a core part of the algorithm and cannot be disabled. It's designed to improve overall risk management by aligning exits with momentum direction.

**Q: How often is VWAP checked?**  
A: Every hour during the hourly refresh cycle (10:15 AM, 11:15 AM, 12:15 PM, etc.)

**Q: What's the difference between this and the Hold/Exit signal on the UI?**  
A: The UI shows real-time Hold/Exit signals for information. The VWAP cross exit is the actual automated exit that closes your position in the system.

---

## ✅ Verification Checklist

- [x] Code implemented in `scan.py`
- [x] Database model comment updated
- [x] Documentation updated in `SCAN_FUNCTIONAL_GUIDE.md`
- [x] Exit condition properly prioritized (3rd in order)
- [x] Logging added for tracking
- [x] No linter errors
- [x] No database migration required
- [x] Backward compatible with existing data

---

## 📞 Support

For questions or issues with this feature:

1. Check logs: `sudo journalctl -u trademanthan-backend -f | grep "VWAP CROSS EXIT"`
2. Verify VWAP data is being fetched: Check `stock_vwap` column in database
3. Verify option type is correct: Check `option_type` column (CE or PE)
4. Test with sample data: Manually trigger hourly refresh endpoint

**Sample Log Query:**
```bash
# View all VWAP cross exits
sudo journalctl -u trademanthan-backend -f | grep "VWAP CROSS EXIT"

# View CE exits specifically
sudo journalctl -u trademanthan-backend -f | grep "VWAP CROSS EXIT.*CE"

# View PE exits specifically
sudo journalctl -u trademanthan-backend -f | grep "VWAP CROSS EXIT.*PE"
```

**Database Query:**
```sql
-- Check exits due to VWAP cross
SELECT stock_name, option_type, stock_ltp, stock_vwap, sell_price, pnl, exit_reason
FROM intraday_stock_options
WHERE exit_reason = 'stock_vwap_cross'
ORDER BY sell_time DESC;
```

**Contact:** Bipul Sahay  
**Repository:** https://github.com/bipulsin/trademanthan

---

*Feature Implementation Date: November 5, 2025*
*Updated: Directional VWAP logic (CE/PE specific)*

