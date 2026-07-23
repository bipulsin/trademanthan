# Item D2 — VWAP close-confirmation entry filter (shadow only)

**Status:** Instrumentation live as of deploy of this change.  
**Table:** `kavach_vwap_close_confirm_shadow`  
**Hook:** `enrich_stocks_trade_state` → `update_vwap_close_confirm_shadow` (never gates).  
**Also mirrored** under `kavach_ready_consistency_log.inputs.vwap_close_confirm_shadow` when a consistency row is written.

## Flag

| Side | `vwap_close_confirmed` |
|---|---|
| LONG | sticky True after a **closed** 10m candle closes **above** session VWAP |
| SHORT | sticky True after a **closed** 10m candle closes **below** session VWAP |

Wick/touch alone does **not** confirm. Sticky until episode ends (left READY / expired / EOD / direction change). New READY episode resets.

## Fields

`ts_ready_first_flagged`, `price_at_ready`, `vwap_at_ready`, `ts_vwap_close_confirmed`, `price_at_vwap_confirm`, `candles_to_confirm`, `bars_since_ready_at_eod_or_expiry`, `episode_ended_at`, `episode_end_reason`.

## Explicit non-goals (this ticket)

No Take Trade badge, no Confidence/TS change, no live gate, no required UI.

## Sign-off

Same as other shadow threads: review full session(s), quantify confirm-rate + cost-of-waiting, checkpoint decision before any live filter.
