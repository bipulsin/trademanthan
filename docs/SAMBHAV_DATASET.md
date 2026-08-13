# Sambhav V1 Dataset

**Status:** FINALIZED  
**Dataset version:** `sambhav_dataset_v1_20260813`  
**Purpose:** ML training, validation, backtesting and future research (NIFTY 10m → 30m probability)

---

## Definition

| Field | Value |
|-------|--------|
| Instrument | NIFTY 50 |
| Instrument key | `NSE_INDEX\|Nifty 50` |
| Exchange | NSE |
| Session | **Regular NSE sessions only** |
| Interval | 10 minutes |
| Historical period | 2022-01-03 → 2026-08-12 |
| Regular sessions | 1,137 |
| Regular candles | 1,137 × 38 = 43,206 |
| Source | Upstox V3 historical candles (`minutes/10`) |

### Included

- Regular weekday NSE sessions with the full 38-bar grid: **09:15 … 15:25** IST

### Excluded (preserved in raw table, not used in V1)

| Kind | Session type | Examples |
|------|--------------|----------|
| NSE holidays | `EXCLUDED_HOLIDAY` | Republic Day, Diwali holiday, etc. |
| Muhurat | `EXCLUDED_MUHURAT` | 2025-10-21 |
| Special / Saturday vendor bars | `EXCLUDED_SPECIAL` | 2024-03-02, 2024-05-18 |
| Unreviewed anomalies | `UNKNOWN` | Do not use until reviewed |

Excluded-session candles remain in `sambhav_10m_candles` for future research. They are **not** deleted.

---

## Data architecture (immutability)

```
RAW / SOURCE DATA          sambhav_10m_candles   (OHLC never mutated by ML)
        ↓
SESSION CLASSIFICATION     sambhav_sessions
        ↓
FEATURE DATASET            sambhav_features      (separate table; not yet populated)
        ↓
MODEL DATASET / TRAINING   sambhav_models (+ dataset_version, feature_version, model_version)
        ↓
VALIDATION / CALIBRATION
```

**Rule:** Feature engineering and training must never modify source OHLC. Use `sambhav_features`.

---

## Quality (V1)

V1 PASS/FAIL is based **only** on REGULAR sessions:

- Expected: 38 candles/session on the Upstox-validated 09:15 grid
- Duplicates / invalid OHLC / timestamp anomalies on regular sessions → FAIL
- Holidays and special sessions are **excluded**, not “missing candles”

Do **not** treat the older “1,970 missing candles” figure (holiday calendar noise) as a V1 failure.

---

## Reproducibility

Every future model must record:

- `dataset_version` (e.g. `sambhav_dataset_v1_20260813`)
- `feature_version` (e.g. `sambhav_features_v1`)
- `model_version` (e.g. `sambhav_xgb_v1`)
- training / validation / out-of-sample periods
- calibration method

---

## Incremental updates (after 2026-08-12)

Use `import_incremental_10m` (or admin import with a from-date after the last stored candle):

1. Read last stored regular/source candle date  
2. Request only the new range from Upstox  
3. Upsert (duplicate-safe)  
4. Classify session  
5. Exclude non-regular  
6. Validate new regular sessions  

Do **not** re-download the full historical archive.

---

## Related docs

- Architecture: `docs/SAMBHAV_ARCHITECTURE.md`
- Backup / restore: `docs/SAMBHAV_DATA_BACKUP.md`
