# Kosmic Tarang — Min max-loss per lot / contract (Phase 0 follow-up)

**Purpose:** Floor risk per unit size for the data-health page (Phase 1).  
**Formula (credit spreads):** `max_loss = (width − net_credit) × multiplier`  
**Zero-credit ceiling:** `width × multiplier` (conservative floor of “how bad can one lot be”).  
**Widths:** strategy profile strike steps — CL **5–8**, NG **3–5**, BTC/ETH **2–4**; also show **1-step** theoretical minimum.

Budget reference: **₹3,000** default / **₹10,000** hard cap (strategy doc).  
Crypto USD↔INR rough: $36 ≈ ₹3k, $120 ≈ ₹10k (illustrative).

Source spikes: `docs/_spike_upstox_mcx_raw.json`, `docs/_spike_delta_sizing_raw.json` (ran 2026-09-19 UTC).

---

## MCX full contracts (Upstox)

| Underlying | Expiry | Lot | Strike step | Width | Width (pts) | Max loss / lot (₹), zero credit | Est. w/ mid credit (₹) | Fits ₹3k? | Fits ₹10k? |
|---|---|---:|---:|---|---:|---:|---:|:---:|:---:|
| CRUDEOIL | 2026-10-15 | 100 | 50 | 1 step | 50 | **5,000** | ~5,000 | No | Yes |
| CRUDEOIL | 2026-10-15 | 100 | 50 | profile min 5 | 250 | **25,000** | ~25,000 | No | No |
| CRUDEOIL | 2026-10-15 | 100 | 50 | profile max 8 | 400 | **40,000** | ~40,000 | No | No |
| CRUDEOIL | 2026-11-17 | 100 | 50 | 1 step | 50 | **5,000** | ~5,000 | No | Yes |
| CRUDEOIL | 2026-11-17 | 100 | 50 | profile min 5 | 250 | **25,000** | ~25,000 | No | No |
| CRUDEOIL | 2026-11-17 | 100 | 50 | profile max 8 | 400 | **40,000** | ~40,000 | No | No |
| NATURALGAS | 2026-09-23 | 1250 | 5 | 1 step | 5 | **6,250** | ~5,625 | No | Yes |
| NATURALGAS | 2026-09-23 | 1250 | 5 | profile min 3 | 15 | **18,750** | ~17,400 | No | No |
| NATURALGAS | 2026-09-23 | 1250 | 5 | profile max 5 | 25 | **31,250** | ~29,900 | No | No |
| NATURALGAS | 2026-10-23 | 1250 | 5 | 1 step | 5 | **6,250** | ~6,250 | No | Yes |
| NATURALGAS | 2026-10-23 | 1250 | 5 | profile min 3 | 15 | **18,750** | ~18,750 | No | No |
| NATURALGAS | 2026-10-23 | 1250 | 5 | profile max 5 | 25 | **31,250** | ~31,250 | No | No |

### Read-out

- **Even the narrowest 1-step full-lot CL/NG spread exceeds the ₹3k default budget** (CL ₹5k, NG ₹6.25k zero-credit).
- **Profile widths (5–8 CL / 3–5 NG) are ₹18k–40k per lot** — above the ₹10k hard cap.
- Strategy rule “never round up to one lot” means **full CL/NG will skip under current budgets** unless budget is raised, widths are narrowed below profile, or **mini contracts** (`CRUDEOILM` lot 10 / `NATGASMINI` lot 250) are added later (out of Phase 0 full-only scope).

---

## Delta India (public tickers)

Max loss / contract (USD) = `(width_points − credit) × contract_value`  
(Do **not** multiply by spot again — premiums/width are already USD per coin.)

| Asset | Expiry code | `contract_value` | Strike step | Width | Max loss / contract (USD), est. | Zero-credit USD | Fits ~$36 / ~$120? |
|---|---|---:|---:|---|---:|---:|---|
| BTC | 190926 | 0.001 | 200 | 1 / 2 / 4 steps | ~0.18 / 0.38 / 0.77 | 0.20 / 0.40 / 0.80 | Yes / Yes |
| BTC | 200926 | 0.001 | 200 | 1 / 2 / 4 steps | ~0.17 / 0.37 / 0.77 | 0.20 / 0.40 / 0.80 | Yes / Yes |
| ETH | 190926 | 0.01 | 10 | 1 / 2 / 4 steps | ~0.10 / 0.20 / 0.40 | same | Yes / Yes |
| ETH | 200926 | 0.01 | 20 | 1 / 2 / 4 steps | ~0.18 / 0.38 / 0.78 | 0.20 / 0.40 / 0.80 | Yes / Yes |

Crypto can size **many contracts** inside ₹3k–10k; MCX full lots cannot at profile widths.

---

## Phase 1 implications

1. Surface this table on the data-health page.
2. Decide budget raise vs allow 1-step widths vs enable mini later — **blocking product question**.
3. Delta sizing is fine under current budget once IP whitelist / keys work.
