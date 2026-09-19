# Kosmic Tarang Phase 0 — Upstox MCX + Delta India findings

**Product (user-facing):** Kosmic Tarang  
**Internal prefix:** `tarang_` / `/tarang`  
Spike scripts:

- `scripts/tarang_phase0_upstox_mcx_greeks.py` (ATM + Black-76 + liquidity + sizing)
- `scripts/tarang_phase0_delta_india_proof.py` (public market data)
- `scripts/tarang_phase0_delta_creds_check.py` (read-only auth)
- `scripts/tarang_phase0_delta_sizing.py` (BTC/ETH min max-loss)

Raw outputs (no secrets): `docs/_spike_upstox_mcx_raw.json`, `docs/_spike_delta_india_raw.json`, `docs/_spike_delta_creds_raw.json`, `docs/_spike_delta_sizing_raw.json`.  
Sizing summary: [`docs/tarang-sizing-min-max-loss.md`](./tarang-sizing-min-max-loss.md).

---

## Upstox MCX Option Greek spike (follow-up, OAuth refreshed)

### Method

1. Load public instrument master; filter **`segment=MCX_FO`** and **`underlying_symbol` exact** `CRUDEOIL` / `NATURALGAS` (excludes `CRUDEOILM` / `NATGASMINI` / `NSE_COM`). Note: Upstox `name` is often `CRUDE OIL` (spaced); NATGASMINI rows also carry `name=NATURALGAS` — always key off `underlying_symbol`.
2. Nearest **two option expiries** per family; futures LTP from closest matching `underlying_symbol` FUT.
3. ATM: nearest CE+PE strike pairs around futures LTP.
4. `GET /v3/market-quote/option-greek` (≤50 keys) + `GET /v2/market-quote/quotes` for bid/ask, via headers matching `UpstoxService.get_headers()` (+ UA; Cloudflare blocks bare urllib).
5. Black-76 on futures price: implied IV from mid, model delta; flag |Δnative−Δmodel| > 0.12 or large IV relative gap.
6. Liquidity: bid–ask as % of mid for strikes with |δ| in **10–16**.

### Verdict

| Field | Value |
|---|---|
| Verdict | **`PARTIAL_RESPONSE_BUT_NULLS`** |
| Reliable ATM IV+δ | **23 / 32** rows (IV>0 and \|δ\|∈(0.01,0.99)) |
| Unreliable | **9** rows with IV=`0` and/or placeholder δ (`0` / `±1`) — mostly farther / thinner strikes |
| Black-76 in Phase 1 | **Required as fill** when native is zero/placeholder; near-term ATM CL/NG natives track Black-76 well when F is the matching futures |
| Auth | Token valid (paperclip OAuth refresh) — prior `BLOCKED_EXPIRED_UPSTOX_TOKEN` cleared |

### ATM samples (reliable natives)

**CRUDEOIL** — expiry 2026-10-15, lot **100**, strike step **50**, FUT `CRUDEOIL FUT 19 OCT 26` LTP **9230**

| Symbol | IV | δ | Black-76 δ | OI | Vol | Last | Bid/Ask | Spr% mid |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| CRUDEOIL 9250 CE | 0.507 | +0.520 | +0.521 | 3648 | 10046 | 495.5 | 495 / 502 | 1.4% |
| CRUDEOIL 9250 PE | 0.494 | −0.481 | −0.480 | 59 | 10913 | 507.1 | 506 / 516 | 2.0% |
| CRUDEOIL 9300 CE | 0.513 | +0.505 | +0.506 | 3648 | 34624 | 478.2 | 473 / 480 | 1.5% |
| CRUDEOIL 9300 PE | 0.501 | −0.497 | −0.496 | 1242 | 26592 | 541.4 | 540 / 543 | 0.5% |

**NATURALGAS** — expiry 2026-09-23, lot **1250**, strike step **5**, FUT `NATURALGAS FUT 25 SEP 26` LTP **279.6**

| Symbol | IV | δ | Black-76 δ | OI | Vol | Last | Bid/Ask | Spr% mid |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| NATURALGAS 280 CE | 0.430 | +0.502 | +0.497 | 18100 | 305929 | 5.50 | 5.50 / 5.55 | 0.9% |
| NATURALGAS 280 PE | 0.430 | −0.498 | −0.503 | 12697 | 260479 | 5.75 | 5.75 / 5.80 | 0.9% |
| NATURALGAS 285 CE | 0.430 | +0.364 | +0.352 | 11207 | 193536 | 3.45 | 3.45 / 3.50 | 1.4% |
| NATURALGAS 285 PE | 0.430 | −0.636 | −0.648 | 3144 | 95362 | 8.70 | 8.65 / 8.70 | 0.6% |

Puts are negative; ATM ≈ ±0.5 as expected.

### Liquidity gate (10–16 δ band)

Strategy gate: bid–ask ≤ **15% of mid**.

| Metric | Value (this run) |
|---|---|
| Sample size in band | 5 (NG-heavy; CL 10–16δ sparsely in near-ATM window) |
| Median spread % of mid | **~4.4%** |
| Passing ≤15% gate | **60%** (3/5) |
| Failures | Wider books on farther NG expiry (e.g. ~68%, ~123% of mid) |

**Implication:** Liquidity gate will reject a material share of 10–16δ candidates on thinner expiries; keep the 15% rule.

### Implications for Kosmic Tarang

- Build MCX chain from master (`underlying_symbol` + `MCX_FO`); profile `contractFamily=full`.
- Batch Greeks ≤50 keys; **Black-76 fill** when IV=0 / placeholder δ; use matching-expiry futures as F.
- Share Upstox rate limiter / WS with CommDiv in Phase 1 (Tarang lower priority, capped) — plan only this turn; do not refactor CommDiv yet. Confirm WS subscription limits before dual subscribe.
- Prior auth blocker is cleared.

---

## Delta Exchange India

### Public market data (unchanged proof)

Base `https://api.india.delta.exchange/v2`. Symbols `C|P-{BTC|ETH}-{strike}-{DDMMYY}`; `contract_value` BTC `0.001`, ETH `0.01`; tickers expose `mark_vol` + greeks. Testnet only Phase 4. Client order ids: `tarang-` prefix. Paper: public India + PaperBroker.

### Credential check (read-only)

Script: `scripts/tarang_phase0_delta_creds_check.py`  
Calls: `GET /v2/wallet/balances`, `GET /v2/positions` (and margined) only (no orders).

**Phase 1 update:** Tarang-specific keys stored as `DELTA_INDIA_API_*` in paperclip `/home/ubuntu/twcto/.env` + local `.env` (never git). From paperclip egress `140.245.14.17`: **balances HTTP 200 success** (7 assets); **positions/margined HTTP 200** (0 positions). Plain `/v2/positions` may return `bad_schema` without product filter — use margined for health. Local workstation may still see `ip_not_whitelisted_for_api_key` if home IP is not whitelisted.

## Crypto sizing (public)

See [`tarang-sizing-min-max-loss.md`](./tarang-sizing-min-max-loss.md). Per-contract max loss at profile widths is **well under** ₹3k–10k budget (often &lt; $1 / contract) because `contract_value` is 0.001 / 0.01.

---

## Phase 1 notes

- Tables: all `tarang_*` via `ensure_*`; IV history table name **`tarang_iv_snapshots`**.
- Nav: **Kosmic Tarang** next to Iron Condor / CommDiv; **admin-only** through Phase 2.
- Profiles: full CL/NG only (`contractFamily` default `full`).
- Upstox infra: Tarang REST rate budget; shared WS via capped `set_feed_provider_keys('tarang')` later — CommDiv unchanged.
- Delta: `DELTA_INDIA_API_*` env; paper + public India; authenticated read OK on paperclip; testnet Phase 4.

## Re-run commands

```bash
UPSTOX_TOKEN_FILE=/path/to/upstox_token.json python3 scripts/tarang_phase0_upstox_mcx_greeks.py
python3 scripts/tarang_phase0_delta_creds_check.py --from-algo-defaults
python3 scripts/tarang_phase0_delta_sizing.py
python3 scripts/tarang_phase0_delta_india_proof.py
```
