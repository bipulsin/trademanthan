# ICICIBANK & KOTAKBANK Futures — Jun 11 2026 Deep Dive

**Session:** 11 Jun 2026, 9:15 AM – 12:00 PM IST  
**Symbols:** ICICIBANK, KOTAKBANK (current-month futures)  
**Charts:** Strong bullish continuation — price above VWAP, EMA5 cross, volume expansion (especially ICICI from open; KOTAK reclaim ~11:15).

---

## Executive summary

On 11-Jun-2026 **none of the four futures algos** surfaced ICICIBANK or KOTAKBANK as strong LONG candidates in the 9:15–12:00 window. That was **by design of legacy filters**, not missing market data:

| Algo | Missed because | Fix implemented? |
|------|----------------|------------------|
| **Volume Mismatch** | 9:30 scan only accepts **gap-down + below lower BB** LONGs; banks gapped up / flat | **Yes** — momentum-open + intraday discovery |
| **Smart Futures** | **ADX regime** (must be rising) + **daily sector gate** blocked before CMS tier | **Yes** — opening momentum regime + sector waiver |
| **Vajra Futures** | **30m TPS bear-biased** early; 5m VWAP reclaim not weighted until late | **Yes** — opening 5m bull bias through noon |
| **Daily Futures** | **ChartInk webhook only** — banks not in bullish scan batch | **No** (architecturally external) |

---

## Per-algo root cause (11-Jun-2026)

### 1. Volume Mismatch Futures

**Observed:** Scan at 9:31 produced LONG for AUBANK, IREDA, IEX — not ICICI/KOTAK.

**Root cause:** `evaluate_vm_signal()` in `signal_rules.py` required ALL of:
- Open **≥1% below** previous close (`MIN_GAP_PCT_LONG = -1`)
- Open **below lower Bollinger Band**
- Green 15m candle + positive net volume

ICICIBANK opened **slightly below** prior close (−0.35%) with **2.98× relative volume** and close well above BB mid — but failed classic gap-down (needed ≤−1% + below lower BB). KOTAKBANK first 15m rel vol **0.44×** — failed any volume gate; bullish structure emerged only after **~11:15** VWAP reclaim.

**Monitor path** (`monitor.py`) already supports VWAP/EMA5 breakout — but **only for symbols already in DB**. Banks never entered DB.

### 2. Smart Futures

**Observed:** No `smart_futures_daily` rows for 11-Jun; KOTAKBANK log shows `BLOCKED: regime filter failed` (ADX ~17–26, not rising).

**Root cause:** `market_regime_ok()` requires `atr5 > atr14`, `adx > 20`, **and** `adx > adx_prev`. At the open ADX often flat/falling even when price > VWAP. Additional gate: `sector_score > 0.05` uses **daily** sector returns — Nifty Bank can lag while individual names lead.

### 3. Vajra Futures

**Observed:** Session 11-Jun ratings SHORT-biased; ICICIBANK on prior session tagged **REJECT** (conf 36). Banks absent from top LONG set.

**Root cause:** TPS on **30m** uses DI+/DI−, RSI, structure from prior bars — often **bear_pts > bull_pts** on first hours while **5m** is already above VWAP. Shortlist caps (5–15 names) hide mid-tier banks. Opening logic skipped strict 5m validation before 9:35 but did not **boost** 5m continuation.

### 4. Daily Futures

**Observed:** No `daily_futures_screening` rows for ICICI/KOTAK; RBLBANK LONG at 9:45 from ChartInk.

**Root cause:** Entirely **webhook-driven**. If ChartInk bullish screener does not include a symbol, Daily Futures cannot list it regardless of Upstox momentum.

**Why not fixed in code:** Would require a parallel Upstox-native scanner duplicating ChartInk semantics — different product contract. Operational fix: adjust ChartInk screener filters for large-cap banks.

---

## Implemented fixes (this release)

### Volume Mismatch — two layers

1. **`momentum_open` scan path** (`signal_rules.py`)
   - LONG when: **−1% < gap < +1%**, green 15m candle, net vol > 0, close ≥ BB mid, **rel vol ≥ 1.25×**
   - Preserves existing gap-down / BB LONG (`gap_bb`) unchanged

2. **Intraday momentum discovery** (`momentum_discovery.py` + `monitor.py`)
   - Every 5m monitor cycle (9:45–12:00): scan universe symbols **not yet in DB**
   - LONG when: price > VWAP & EMA5, price ≥ first-15m high, rising 5m volume, `assess_session_trend == BULLISH`
   - Catches **KOTAK-style late VWAP reclaim** (~11:15)

### Smart Futures — opening continuation

- `opening_momentum_regime_ok()` — 9:30–11:30 IST bypass when price > VWAP, vol surge ≥ 1.25, ADX ≥ 18, relaxed ATR ratio
- `opening_long_sector_waiver()` — 9:30–12:00 waives daily sector gate when price > VWAP with vol surge ≥ 1.5 and VWAP deviation ≥ 0.12 ATR

### Vajra Futures — 5m opening bias

- `opening_session_5m_bull_bias()` — live 5m: close > VWAP & EMA5, rising volume, higher high
- `apply_opening_5m_bias_to_tps()` — +18 TPS bull points, can flip `bull_dir` before noon
- Wired in `pipeline.rate_symbol_transition()`

---

## Algos where high-performer detection remains infeasible

| Algo | Feasibility | Why |
|------|-------------|-----|
| **Daily Futures** | **Not feasible without product change** | Source of truth is ChartInk webhook batch, not intraday Upstox momentum. Adding fallback scanner is a **new subsystem**, not a filter tweak. |
| **Smart Futures (late day)** | Partial | Picker runs 9:30–15:00 but publish cap = 5/scan; banks can still lose rank to higher CMS names after noon. |
| **Vajra (EXECUTABLE tier)** | Partial | EARLY LONG / WATCH can appear; full EXECUTABLE still needs qualification score ≥ 75 + structure — by design to reduce false entries. |

---

## Verification — Jun 11 replay

Run on production (has Upstox token + historical candles):

```bash
docker compose exec -T app python3 scripts/replay_jun11_bank_momentum.py
```

Unit tests:

```bash
python3 -m pytest backend/services/volume_mismatch/test_momentum_signal.py \
  backend/services/vajra/test_opening_5m_bias.py -q
```

**Expected after fix (11-Jun production candles):**
- **ICICIBANK:** `VM scan: LONG via momentum_open` (gap −0.35%, rel_vol 2.98, close 1302.8 > BB mid 1264.8)
- **KOTAKBANK:** VM scan still no signal at 9:30 (rel_vol 0.44); **intraday discovery** after 11:15 VWAP breakout
- Vajra: `5m bias` true at 11:15/11:30; trade_type moves toward EARLY LONG TRANSITION
- Smart Futures: regime/sector gates pass during opening window when re-scored with Jun-11 candles

---

## Files changed

| File | Change |
|------|--------|
| `backend/services/volume_mismatch/constants.py` | Momentum thresholds |
| `backend/services/volume_mismatch/signal_rules.py` | `momentum_open` LONG path |
| `backend/services/volume_mismatch/momentum_discovery.py` | **New** intraday discovery |
| `backend/services/volume_mismatch/monitor.py` | Hook discovery into monitor |
| `backend/services/smart_futures_picker/indicators.py` | Opening regime + sector waiver |
| `backend/services/smart_futures_picker/job.py` | Apply opening gates |
| `backend/services/vajra/transition.py` | 5m bull bias helpers |
| `backend/services/vajra/pipeline.py` | Apply bias in rating pipeline |
| `scripts/replay_jun11_bank_momentum.py` | Jun-11 replay script |
| `backend/services/volume_mismatch/test_momentum_signal.py` | Unit tests |
| `backend/services/vajra/test_opening_5m_bias.py` | Unit tests |

---

## Deployment

Standard paperclip deploy after push to `main`:

```bash
REBUILD=1 NO_CACHE=1 TRADEMANTHAN_REF=main ./scripts/trigger-paperclip-deploy.sh
```

Verify health: `curl -s https://www.tradewithcto.com/scan/health`
