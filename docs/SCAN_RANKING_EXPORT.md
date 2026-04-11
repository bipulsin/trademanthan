# Scan ranking export (all symbols + scores)

When a Chartink webhook is processed, the stock ranker scores **every** enriched symbol. Exports are written for:

- **More than 15 symbols**: full ranking of all symbols is logged and saved **before** trimming to the top 15 for DB/UI.
- **15 or fewer symbols**: the same full ranking export still runs (no trimming).

## Where to find files

On the server (project root), after each qualifying webhook:

| Output | Path |
|--------|------|
| **JSON** (full detail + `breakdown_raw` per row) | `logs/scan_rankings/scan_ranking_<UTC_ts>_<time_label>_<CE\|PE>.json` |
| **CSV** (Excel-friendly) | Same basename, `.csv` |
| **ASCII table** | `logs/smart_future_algo.log` — search for `FULL STOCK RANKING` |

## Email (CSV attachment)

After each successful webhook run with **at least one** enriched stock, the same CSV is sent **asynchronously** (daemon thread — does not delay trade/order processing) to **`tradentical@gmail.com`** by default.

- Override recipient: env **`CHARTINK_RANKING_EMAIL`**
- Uses existing SMTP settings: **`SMTP_SERVER`**, **`SMTP_PORT`**, **`SMTP_USER`**, **`SMTP_PASSWORD`**, **`SMTP_FROM_EMAIL`**
- **Subject:** `ChartInk webhook stock score for <YYYY-MM-DD HH:MM:SS IST>` (time = **when the email is sent**)
- If SMTP is misconfigured, failure is logged; webhook processing still completes.

## CSV / table columns

| Column | Meaning |
|--------|---------|
| `stock_symbol` | **First column** — NSE symbol |
| `rank` | 1 = highest composite score |
| `composite_score` | Total score (sum of factors + bonuses) |
| `momentum` | VWAP momentum factor (0–40+ bonus) |
| `liquidity` | Lot/qty factor |
| `premium` | Option premium quality |
| `strike` | Strike vs spot reasonableness |
| `completeness` | Data completeness |
| `extreme_bonus` | Extra points for very strong VWAP distance |
| `hold_bonus` | “Hold” characteristic bonus (premium/liquidity bands) |

See `backend/services/stock_ranker.py` → `calculate_score()` for exact rules.

## Past alerts

Exports exist only for webhooks **after** this feature is deployed. Older runs (e.g. a specific past 10:15 batch) are **not** reconstructed unless you still have the matching log lines or a saved JSON.
