# SQ Additive Composite — threshold sensitivity (2026-07-27..31 clean-10m)

Formula: `Total = 0.15*(RS_trade_score + Garuda + OW + VW + EW) + Grade_Bonus`
Candidates: garuda_top6_rank NOT NULL AND grade A/B AND trade_score+garuda present
Actual READY first episodes: **71** · candidate rows: **1833**

| thr | proposed | TP | FP | FN | precision | recall |
|----:|---------:|---:|---:|---:|----------:|-------:|
| 70 | 120 | 39 | 81 | 32 | 0.325 | 0.5493 |
| 72 | 115 | 39 | 76 | 32 | 0.3391 | 0.5493 |
| 75 | 102 | 37 | 65 | 34 | 0.3627 | 0.5211 |
| 78 | 90 | 33 | 57 | 38 | 0.3667 | 0.4648 |
| 80 | 74 | 28 | 46 | 43 | 0.3784 | 0.3944 |
| 82 | 52 | 22 | 30 | 49 | 0.4231 | 0.3099 |
| 85 | 23 | 10 | 13 | 61 | 0.4348 | 0.1408 |

## thr=75 by day

| day | proposed | TP | FP | FN | precision |
|-----|---------:|---:|---:|---:|----------:|
| 2026-07-27 | 27 | 5 | 22 | 5 | 0.1852 |
| 2026-07-28 | 22 | 8 | 14 | 9 | 0.3636 |
| 2026-07-29 | 17 | 10 | 7 | 8 | 0.5882 |
| 2026-07-30 | 12 | 4 | 8 | 7 | 0.3333 |
| 2026-07-31 | 24 | 10 | 14 | 5 | 0.4167 |

**2026-07-31 @ thr=75:** precision=0.4167 (TP 10 / proposed 24).

Informational only — live deploy proceeds regardless.
