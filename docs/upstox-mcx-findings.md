# Tarang Phase 0 — Upstox MCX + Delta India findings

Spike scripts:

- `scripts/tarang_phase0_upstox_mcx_greeks.py`
- `scripts/tarang_phase0_delta_india_proof.py`

Raw outputs (no secrets): `docs/_spike_upstox_mcx_raw.json`, `docs/_spike_delta_india_raw.json`.

---

## Upstox MCX Option Greek spike

### Method

1. Load public instrument master `https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz`.
2. Pick MCX_FO CE/PE rows for Crude Oil / Natural Gas (name/symbol match; no hard-coded keys).
3. Call `GET https://api.upstox.com/v3/market-quote/option-greek?instrument_key=…` (≤50 keys) with Bearer token from `UPSTOX_ACCESS_TOKEN` or deploy `upstox_token.json`.

### Instrument master (PASS)

Resolved live MCX option rows (sample from spike run on paperclip-vm, 2026-09-19 UTC):

| Bucket | instrument_key | trading_symbol | type | strike | lot_size |
|---|---|---|---|---|---|
| CL | `MCX_FO\|584953` | CRUDEOILM 12700 CE 15 OCT 26 | CE | 12700 | 10 |
| CL | `MCX_FO\|584954` | CRUDEOILM 12750 CE 15 OCT 26 | CE | 12750 | 10 |
| CL | `MCX_FO\|584955` | CRUDEOILM 12800 CE 15 OCT 26 | CE | 12800 | 10 |
| NG | `MCX_FO\|584827` | NATURALGAS 130 CE 20 NOV 26 | CE | 130 | 1250 |
| NG | `MCX_FO\|584828` | NATURALGAS 130 PE 20 NOV 26 | PE | 130 | 1250 |
| NG | `MCX_FO\|584872` | NATGASMINI 130 PE 20 NOV 26 | PE | 130 | 250 |

Notes:

- Confirms **chain must be built from master** (Upstox documents put/call option-chain as **not available for MCX**).
- Spike matched **CRUDEOILM** / **NATURALGAS** / **NATGASMINI** — Phase 1 profiles must define whether CL/NG use mini vs full contracts (from master `lot_size`, never hard-coded).
- Expiry arrives as epoch ms on master rows (`expiry` field).

### Option Greek API result (BLOCKED — expired token)

| Field | Value |
|---|---|
| Verdict | `BLOCKED_EXPIRED_UPSTOX_TOKEN` |
| HTTP | **401** |
| Upstox error | `UDAPI100050` — Invalid token used to access API |
| JWT | `token_expired: true` on paperclip `/home/ubuntu/twcto/data/upstox_token.json` |
| `iv` / `delta` non-null | **0** (no usable body) |

**Interpretation:** This is an **auth lifecycle** failure, not evidence that MCX Greeks are unsupported. Docs for `/v3/market-quote/option-greek` return `iv`, `delta`, `gamma`, `theta`, `vega`, `oi`, `volume`, `last_price` for instrument keys (incl. MCX_FO examples in Upstox samples). Re-run the spike after Broker OAuth refresh; expect `PASS_IV_AND_GREEKS_PRESENT` if MCX Greeks are populated, or `PARTIAL_RESPONSE_BUT_NULLS` if fields come back null (then Black-76 fallback becomes required for Phase 1).

**Secondary note:** Host `urllib` without a normal User-Agent hit Cloudflare 1010 on `api.upstox.com`; spike uses `requests` + Accept/User-Agent. Prefer `UpstoxService.get_headers()` in product code.

### Implications for Tarang

- Build MCX chain from daily master refresh (reuse `divtest.instruments.ensure_instrument_master`).
- Batch Greeks ≤50 keys; Black-76 fallback when native IV/Greeks null.
- **Blocker for Phase 1 live reads:** refresh Upstox OAuth on paperclip (token was expired at spike time).

---

## Delta Exchange India proof

**Base:** `https://api.india.delta.exchange/v2` (public products/tickers; no API key for this spike).  
**Testnet (not called):** `https://cdn-ind.testnet.deltaex.org`

### Symbol shape

Pattern: `{C|P}-{BTC|ETH}-{strike}-{DDMMYY}`

Examples:

- `C-BTC-83000-190926`, `P-BTC-83000-190926`
- `C-ETH-2760-210926`, `P-ETH-2720-190926`

### contract_value

| Underlying | Observed `contract_value` (live options page) |
|---|---|
| BTC | `"0.001"` |
| ETH | `"0.01"` |

`lot_size` on products was `null` in samples — size Tarang risk off `contract_value` × premium, not a hard-coded lot.

### Expiry fields

- **Products:** `settlement_time` ISO8601 UTC (e.g. `2026-09-19T12:00:00Z`, daily/weekly ladder present).
- **Tickers:** no reliable `settlement_time` on ticker objects in this sample — join ticker → product (or parse DDMMYY from `symbol`) for DTE. Prefer **live products** as expiry source (never hard-code calendars).

### Greeks / IV on tickers

Public tickers include:

- `mark_vol` (IV proxy)
- `greeks`: `delta`, `gamma`, `theta`, `vega`, `rho`, `spot`
- `mark_price`, `quotes` (bid/ask), `spot_price`, `strike_price`, `contract_value`

So Delta native IV/Greeks are available without auth for screening; Black-Scholes remains fallback if fields missing.

### preferNativeSpreads

Spike did not exercise Delta multi-leg/native spread order APIs. Product default remains `preferNativeSpreads=false` (leg-by-leg) until Phase 4 design review.

---

## Re-run commands

```bash
# Delta (local, public)
python3 scripts/tarang_phase0_delta_india_proof.py

# Upstox MCX Greeks (needs valid token)
UPSTOX_ACCESS_TOKEN=… python3 scripts/tarang_phase0_upstox_mcx_greeks.py
# or on paperclip after OAuth refresh:
# TARANG_SPIKE_OUT=/tmp/_spike_upstox_mcx_raw.json python3 scripts/tarang_phase0_upstox_mcx_greeks.py
```
