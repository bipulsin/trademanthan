# Kavach Momentum Ignition — Phase 1

## Scope confirmation (vs codebase)

| Requirement | Status | Notes |
|---|---|---|
| WS parsing extension (D5 depth, tbq/tsq, I1) | **Implemented** | `upstox_market_feed.py` — `_extract_feed_fields`, `get_ws_feed_row`, persists to `upstox_ws_orderflow_latest` + extended `upstox_ws_intraday_1m` |
| 5m baseline (no 1m REST) | **Confirmed** | Detection uses `candle_cache` / 5m via `rs_conviction_candles` |
| Manual FII/DII only | **Implemented** | `fii_dii_flow` page field on checklist (same pattern as `dcNiftyDir`) |
| No RRG / delivery % | **Confirmed** | Not built |
| Reuse `accumulation_signal` / `normalized_vwap_slope` | **Confirmed** | Called from `kavach_momentum_ignition.py` |
| Pullback-depth contraction (new) | **Implemented** | `pullback_depth_contraction()` using `ema10_10min` + ATR |
| OI triangulation from WS ticks | **Implemented** | Uses `get_ws_feed_row().oi_change` + 5m price/volume |
| Composite scoring | **Implemented** | `compute_ignition()` with tier weights in `rs_conviction_config` |
| Conviction board input | **Implemented** | `W_ignition_conviction` when `ignition_conviction_enabled=true` |
| UI flag | **Implemented but gated** | `ignition_ui_enabled=false` by default — flag hidden until validation passes |
| Validation before live | **Script provided** | `scripts/validate_momentum_ignition.py` |

### Conflicts / caveats

1. **`nifty_open_direction` is not part of the 9-condition gate** — only a gap-reversal banner. FII/DII follows the same pattern (context multiplier only), as specified.
2. **WS I1 bars may not arrive for every tick** — LTP-synthetic 1m candles remain fallback; I1 overwrites when present.
3. **Order-flow has no historical backfill** — forward validation via `rs_momentum_ignition_log` after deploy.
4. **`arbitrage_master` row count** — confirm on production with `SELECT COUNT(*) FROM arbitrage_master` (open item from Phase 0).

## Validation gate

Before setting `ignition_ui_enabled=true` in Settings / `rs_conviction_config`:

1. Run `PYTHONPATH=. python3 scripts/validate_momentum_ignition.py --days 10 --symbols 20`
2. Run 5–10 live sessions logging to `rs_momentum_ignition_log` with order-flow active
3. Target: 5m signal precision ≥ 55% on 3-bar forward move; order-flow forward hit-rate TBD

## Config keys (`rs_conviction_config`)

- `ignition_ui_enabled` — checklist "Ignition Building" flag (default **false**)
- `ignition_conviction_enabled` — adds `W_ignition_conviction` to board composite (default **false**)
- `ignition_flag_threshold` — default 65
- `W_ignition_orderflow`, `W_ignition_oi_tri`, `W_ignition_absorption`, `W_ignition_slope`, `W_ignition_pullback`, `W_ignition_confirm`, `W_ignition_fii_context`, `W_ignition_conviction`
