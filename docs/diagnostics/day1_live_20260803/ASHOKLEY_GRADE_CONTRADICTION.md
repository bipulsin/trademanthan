# ASHOKLEY grade contradiction — 2026-08-03 12:25

## Verdict

**Same enrich cycle, two different grade sources — not genuine sequential decay of one metric.**

SQ qualified on `rs_universe_score_snapshot` grade **A / TS 85**. The live card / `entry_staleness` logged **D! / TS 35** from `stock.confidence` after the live 10m overlay path. Wall-clock gap is ~1s because both writes happen in the same `enrich_stocks_trade_state` pass — not because grade collapsed in the market between them.

## Evidence

| Source | Time (IST) | Grade | Score |
|---|---|---|---:|
| Universe snapshot | 12:25:08 | **A** | 85 |
| SQ promotion log | 12:25:34 | **A** (grade_bonus 20) | rs 85 |
| Entry staleness | 12:25:35 | **D!** | 35 |
| Entry staleness recheck | 12:30:18 | A | 85 |

Code path in `evaluate_sq_for_stock`:

```python
grade = (
    (rs_meta or {}).get("confidence_grade")  # ← universe snapshot preferred
    or stock.get("confidence")
    or stock.get("dashboard_kavach")
)
```

Staleness reads `stock.get("confidence")` — set earlier by `metrics_from_10m_candles(..., include_forming=True)` overlay (or sticky checklist levels when overlay disagrees / lags).

SQ `score_breakdown.stretch_pct` (2.89 on bar 12:15) is the SQ OW-path stretch, **not** the confidence stretch-cliff letter. Grade letter for the gate is taken from the snapshot, so A + high SQ stretch can coexist in the breakdown without contradiction inside SQ’s own math.

## Which user hypothesis?

1. **Snapshot timing lag (yes, in the dual-source sense):** SQ decides on the latest universe snapshot while the displayed card uses the live enrich grade. Not “a fresher scan landed 1s later” — both are read in one cycle from different stores/paths.
2. **Genuine fast decay (no, as the primary explanation):** The D! at 12:25:35 is not the same series as SQ’s A one second earlier. Later flap (A at 12:30, then weaker) is real stretch/grade volatility, but it does not explain the A-vs-D! pair at promote.

## Recurrence / trust

**Can recur** whenever snapshot grade (A/B) disagrees with live `stock.confidence` (e.g. D!). A trader can see a worse grade on the card than the grade SQ used to promote. No live trading fix in this change set — diagnostic only — but any future “trust SQ card grade” work should either (a) stamp SQ’s decision grade onto the card at promote, or (b) gate SQ on the same live grade the UI shows.

## Follow-through note

ASHOKLEY was the clearest 30–60m fade among Day-1 SQ promotes; dual-source promote-on-A while live showed D! is consistent with a weak/stretched tape at the moment of promotion.
