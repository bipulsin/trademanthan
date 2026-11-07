# MTF Swing

Weekly swing screen on continuous NSE futures, executed independently on 15-minute and 60-minute bars. One lot per trade. Research backtest for 1 Nov 2025 through 31 Oct 2026.

The page at `/mtf-swing.html` reads `data/mtf_swing/trades.csv` through `GET /api/mtf-swing/results`. It does not embed trades. The **v2 Experiment** tab reads `data/mtf_swing_v2/` through `GET /api/mtf-swing/v2/results`.

v2 defaults (`variant=auto`) pick **`all` (v2 experiment / all filters) only when that tag covers the full Nov'25–Oct'26 window**; otherwise **`v1_equivalent` (validation / v1 flags)**. The UI labels those two tags explicitly so the Experiment tab does not silently look like experimental rules while showing the validation baseline. Legacy June+Oct `variant=all` pilot remains selectable as a partial-month pilot; like-for-like v1 comparison clips to months the selected variant actually has.

## Resume (v1)

From the repo root (Upstox credentials and `arbitrage_master` required):

```bash
python3 scripts/run_mtf_swing_backtest.py
python3 scripts/run_mtf_swing_backtest.py --timeframe 60m
python3 scripts/run_mtf_swing_backtest.py --symbol RELIANCE --timeframe 15m
```

Each finished month and timeframe is stored in `data/mtf_swing/progress.json` and skipped on the next run. Carried state (open trade, pending or limit setup, next-week watchlist) is in `data/mtf_swing/state_15m.json` and `state_60m.json`. Trades append to `data/mtf_swing/trades.csv`. Cancelled and discarded setups append to `data/mtf_swing/discards.csv`. A re-run does not rewrite a row that is already there. The unique trade key is `symbol + timeframe + side + entry timestamp` (`trade_id`).

Candle responses are cached under `data/mtf_swing/cache/` (not committed).

## v2 Experiment

Configurable flags layered on the v1 engine. Writes **only** to `data/mtf_swing_v2/` and never rewrites v1 CSVs. Month-by-month forward runs with **continuous state carry-over** (open trades, pending/limits, watchlist) — equivalent to one continuous run.

```bash
# Validate first: flags off must match v1
python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \
  --timeframes 60m,15m --variants v1_equivalent

# Full primary ablation matrix
python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \
  --timeframes 60m,15m --variants all

# Optional leave-one-out
python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \
  --timeframes 60m,15m --variants all --leave-one-out
```

| variant | regime | SL pivot | TP 1.5R / no time | RR ≥ 1.3 |
|---------|--------|----------|-------------------|----------|
| v1_equivalent | off | off | off | off |
| regime_only | on | off | off | off |
| sl_only | off | on | off | off |
| tp_only | off | off | on | off |
| rr_only | off | off | off | on |
| all | on | on | on | on |

OPEN rows get MTM at the last available bar (`mtm_price` / `mtm_pnl`). Regime warm-up pulls ≥20 weeks of Nifty history before 1-Nov-2025. Coverage timestamps are logged in `coverage_log.csv` — if the feed ends ~27-Oct-2026 15:15, later calendar days are missing bars (not a silent strategy stop).

Outputs: `trades.csv`, `discards.csv`, `regime_log.csv`, `coverage_log.csv`, `variant_summary.csv`.

### Idempotency / pilot June+Oct rows

Progress tokens are `v2:cont:{variant}:{tf}:{YYYY-MM}` so legacy flat pilot tokens (`v2:all:…`) do not mark continuous months complete. Trade IDs append `|{variant}`; append-only skips duplicates. The June+Oct pilot wrote `variant=all` with **flat** month starts (71 trades). A full-period continuous `all` run can share entry-time keys with those rows — **remove** flat `all` trade/discard rows before the continuous run (keep `v1_equivalent` untouched), otherwise idempotent append would skip continuous Jun/Oct results.

```bash
# on paperclip, inside data/mtf_swing_v2/
# 1) Archive already at trades_pilot_jun_oct_flat.csv (or refresh a dated copy)
# 2) Strip variant=all from live trades.csv + discards.csv; reset state_*_all.json
# 3) Start continuous all (does NOT wipe v1_equivalent):
flock -n /tmp/mtf_swing_v2.lock -c \
  'python3 scripts/run_mtf_swing_backtest.py --v2 --start 2025-11 --end 2026-10 \
     --timeframes 60m,15m --variants all'
```

Prefer flocked runs: `flock -n /tmp/mtf_swing_v2.lock -c 'python3 scripts/run_mtf_swing_backtest.py --v2 …'`.

### v1_equivalent candle-gap parity

v1’s frozen `trades.csv` was produced under partial Upstox fetches (many `SYMBOL YYYY-MM no candles` months). A naïve continuous `v1_equivalent` re-run with full history therefore diverges — e.g. **ADANIENSOL SHORT** screened on **2026-04-30** (Thursday; 2026-05-01 Labour Day holiday) and filled 2026-05-04 (+₹86.8k on 60m). v1 had no April candles for that symbol, so the screen never ran; a later May refill could not recover it because bars before `active_from` are warm-up only (no new screens). 15m diverged from Feb onward for the same reason (hundreds of hole months).

`scripts/build_mtf_swing_v1_holes.py` writes `data/mtf_swing/v1_fetch_holes.json` from v1 run logs. The continuous `v1_equivalent` runner loads that file, skips hole months, and strips hole-month bars from warm-up windows so carry-over replays the same gaps. `variant=all` and other experimental tags do **not** apply the hole mask. OPEN rows for `v1_equivalent` keep blank `exit_time` (like v1); other variants still stamp MTM.

### Assumptions (v2)

- Regime is computed once per screening Friday from Nifty 50 daily→weekly closes with no look-ahead; the label applies to the following week's trades only.
- Pivot SL uses confirmed swing highs/lows on the execution timeframe (same 3-bar definition as v1 triggers). If none sit outside the previous week's range, the setup is discarded (`NO_PIVOT_SL`).
- Prospective TP before entry is 1.5R from the breakout close; after fill it is recomputed from the actual entry. Same-bar SL-before-TP and gap fills match v1.
- State carries across month boundaries (one open/pending setup per symbol). Follow trades past 31-Oct-2026 until SL/TP; else OPEN+MTM.
- Universe, 1-lot sizing, and zero default costs match v1.

## Assumptions

- Calendar weeks are ISO weeks (Monday start) in Asia/Kolkata. Screening runs on the last trading session of the week. A Friday holiday means Thursday is that session. A week with no sessions is not a W1. W2 is the previous ISO week; if that week has no bars the screen is discarded as `NO_W2` rather than skipping back to an older week.
- Long and short inequalities are strict where the spec uses `<` or `>`, and inclusive where it uses `<=` or `>=`. The two sides cannot both match.
- The sweep timestamp is the last bar inside W1 that prints the extreme (lowest low / highest high).
- A swing high is a bar whose high is strictly above the previous and next bar. It is not usable until that next bar exists. At the Friday screen, only swings confirmed inside W1 are used. The same rule applies to swing lows.
- The 0.3% cluster compares the most recent swing with the single swing before it, using `|a-b| / min(|a|,|b|)`. On a tie at the boundary the more extreme price wins (higher high, lower low). Only that pair is clustered, not a longer chain.
- Reference levels are computed on a Panama back-adjust of W1+W2 so a roll inside the two weeks is not a weekly high, low, or swing. The last W1 bar stays on its raw contract price, and SL / trigger / T1 are in that basis.
- While a trade or resting order is open, a contract roll shifts entry, SL, T1, trigger, and limit by `new_open - previous_close`. The roll gap is therefore not a stop or target fill. Fills after the roll are on the new contract's raw prices plus that shift, so point P&L matches the continuous series.
- The active contract is the front month through its expiry (last Tuesday, or the previous session if that Tuesday is a weekend or an `NSE_KNOWN_HOLIDAYS` date). The runner resolves it with Upstox `expired-instruments/future/contract`, passing `arbitrage_master.stock_instrument_key` (`NSE_EQ|ISIN` or `NSE_INDEX|...`). The returned key looks like `NSE_FO|49078|25-11-2025` and includes the lot size. Candles for that key come from `expired-instruments/historical-candle` through `UpstoxService.make_api_request`. A symbol with no underlying key is skipped. A month that returns candles for no symbol is left incomplete so a later run can retry it. 60-minute history is 30-minute expired candles paired into 60-minute bars. 15-minute history uses `15minute` (with a `1minute`→15m aggregate fallback when a chunk is empty, e.g. after rate limits). A roll keeps the entry contract's lot size for rupee P&L.
- A breakout or breakdown is the bar close. The limit or next-open order starts on the following bar. A limit that is still working on the last bar of the watchlist week is cancelled (`WEEK_EXPIRED`). A next-open armed on that last bar is allowed to fill the next session, which can be Monday, because that is the configured entry rather than a new signal. If that fill is skipped, the screen taken on Friday becomes the new week's watchlist.
- `rr >= 1` enters at the breakout close or the next bar's open (`entry_timing`, default `next_open`). Otherwise the limit is `(T1 + SL) / 2`.
- Before entry, long cancels on `low < SL` or `high >= T1`. Short cancels on `high > SL` or `low <= T1`.
- Same bar touching SL and T1 exits at SL. A gap through SL or T1 (open already beyond the level) exits at the open.
- The entry session counts as session 1 of 10. TIME exits at the close of the 10th session if SL and T1 have not been touched. An entry at the close is not stopped out on that same bar. An entry at the open can be.
- One open position or pending setup per symbol. State is carried across month boundaries. The trade's `month` is the entry month.
- Still open after 31 Oct 2026: the last batch loads about three extra weeks and keeps managing the position for up to 10 sessions. If it is still open, the row is `exit_reason=OPEN` with blank P&L. OPEN rows are listed and excluded from closed-trade cards, month totals, and profit factor.
- Quantity is 1 lot. Lot size comes from the existing futures instrument master. If the expired contract is absent, P&L uses lot size 0 and is logged.
- Brokerage, charges, and slippage default to 0. Slippage worsens entry and exit by `slippage_points` inside gross P&L. The `charges` column is brokerage plus other charges, in INR per round turn. `r_multiple` uses unsipped prices against the original risk `(entry - SL)` so a stop at SL is -1 when there is no gap.
- 15m and 60m do not share state.
- The live ticker algo list is only for desks with a live snapshot (Stock Options, Kavach, and the others already registered there). MTF Swing is a backtest page, same as HA-VWAP, and is not added to that ticker list.
- Weeks that fail the price pattern are not written to the discard file. `NO_SWING`, `LEVELS_ILLOGICAL`, `NO_W2`, cancellations, and cutoff drops are.
