# Vajra ARMED Ranking v2 — Setup Quality First

## 1. Architecture

| Layer | Role |
|-------|------|
| **Confidence** (`conviction_score` / `confidence_score`) | Continuation probability, trend sustainability |
| **Setup quality** (`setup_quality_score`) | Execution attractiveness *right now* — ignition, RR, extension decay |
| **Institutional participation** | OBV, RVOL/TPS, EVS, early trade type |
| **ARMED rank** (`armed_rank_score`) | Top-8 sort key for ARMED section only |

Pipeline: `trade_quality` → `score_layers` → `setup_quality.enrich_setup_quality_fields` → `qualify_trade` → screener `rank_armed`.

Module: `backend/services/vajra/setup_quality.py`.

## 2. `setup_quality_score`

Weighted blend (then extension decay + phase penalties):

| Component | Weight |
|-----------|--------|
| Ignition quality | 22% |
| Expansion velocity (EVS) | 20% |
| VWAP acceptance | 14% |
| Momentum acceleration | 12% |
| Compression / breakout initiation | 10% |
| Institutional participation | 8% |
| Breakout score | 6% |
| RR efficiency (pullback + inverse extension risk) | 8% |

**Bonuses:** +8 when `breakout_initiated` / `expansion` and EVS ≥ 55.

**Penalties:** mature trend (−14), extended/exhausted (−22), high extension outside ignition (−10 to −18).

## 3. `expansion_velocity_score`

Primary: persisted `evs_score`. Fallback: `48 + 0.25×breakout + 0.12×tps`.

## 4. `institutional_participation_score`

`0.30×OBV + 0.25×volume + 0.20×TPS + 0.15×EVS + 0.10×early trade type`.

## 5. Extension decay

`extension_decay_multiplier(extension_risk, breakout_phase, …)` — multiplicative on setup quality; **not** a hard reject when `is_ignition_context()` (EVS ≥ 55, compression broken, VWAP, or breakout_initiated/expansion).

Qualification `_hard_reject` skips `over_extended` when ignition context is true.

## 6. `armed_rank_score`

```
armed_rank_score =
    setup_quality_score × 0.65
  + confidence_score      × 0.25
  + institutional_participation_score × 0.10
```

ARMED Top-8 sorts by `-armed_rank_score`, then `-setup_quality_score`, `-ignition_quality_score`.

## 7. State-specific ranking

| State | Primary sort keys |
|-------|-------------------|
| DISCOVERY | institutional participation, discovery, TPS, EVS |
| ARMED | armed_rank_score (setup-heavy) |
| EXECUTABLE | setup_quality, confidence, volume |
| ACTIVE / EXTENDED | confidence / continuation (unchanged in screener tiers) |

## 8. POWERINDIA simulation (illustrative)

`simulate_powerindia_profiles()` in `setup_quality.py`:

| Time | Confidence | Setup quality | ARMED rank |
|------|------------|---------------|------------|
| 09:55 ignition | ~70 | **higher** | **ranks high** |
| 15:15 mature | ~95 | **lower** | **ranks low** |

Run: `pytest test_setup_quality_ranking.py -q`.

## 9. Migration

- **No DB migration required** — scores recomputed in `build_trade_state_dict` and `enrich_execution_scores` on API read.
- Stale rows: first GET after deploy gets fresh `setup_quality_score` / `armed_rank_score` when enrichment runs.
- **Backward compatible:** missing fields fall back to legacy ARMED formula inside `rank_armed`.

## 10. Edge cases

| Case | Behavior |
|------|----------|
| Missing EVS | Proxy from breakout/TPS |
| High extension at ignition | Decay softened; no hard reject if ignition context |
| EXECUTABLE with low setup | Ranks below high-setup ARMED in respective sections |
| REJECT / Compression phase | Still excluded by `_section_eligible` |
| DB row without new columns | `enrich_execution_scores` rebuilds layers from persisted sub-scores |
