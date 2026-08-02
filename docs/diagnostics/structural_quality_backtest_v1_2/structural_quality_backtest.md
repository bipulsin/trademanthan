# Structural Quality Score v1.2 — Corrected VWAP/EMA + EW/grade LOCF

**LIVE PROMOTION NOT WIRED.**

## Data integrity fixes

1. **VWAP:** session-anchored from **09:15 IST**, typical price `(H+L+C)/3 × volume` accumulated on **1m** bars, sampled at each 10m `bar_end`. (Prior v1.1 used 10m-aggregated H/L/C — correct formula family, but incomplete `n1m` buckets skewed early bars; 1m path is the fix.)
2. **EMA5/EMA10:** computed on **close price only** (unchanged definition). Seeded from **prior session final EMA** (carry-forward), not reset-to-close on bar 1. `ema_reliable` from bar 1 when seeded (6-bar buffer removed 2026-08-02).

## Formula fixes

- **EW start-aligned:** if EMA5 already on qualifying side of VWAP at first evaluated bar → `EW=100`.
- **RS grade/trade_score LOCF** with `rs_score_stale_minutes`.

## Verification (before → after)

Manual targets: TVSMOTOR 2026-07-31 VWAP ~4255–4257 at 09:35/09:45; EMA5 must not equal bar-1 close after seeding fix.

### TVSMOTOR 2026-07-31

- EMA seed5=4217.6148 seed10=4208.4845 (`close_only`)
- VWAP input: `typical_price_(H+L+C)/3 * volume from 1m, session-anchored 09:15 IST`

| hhmm | n1m | close | VWAP old→new | EMA5 old→new | old EMA5==close? |
|-----:|----:|------:|--------------|--------------|:----------------:|
| 09:35 | 1 | 4258.5 | 4255.27→4255.27 | 4258.5→4231.24 | True |
| 09:45 | 5 | 4257.7 | 4256.99→4256.22 | 4258.23→4240.06 | False |
| 09:55 | 10 | 4258.7 | 4260.19→4258.65 | 4258.39→4246.27 | False |
| 10:05 | 6 | 4246.7 | 4257.18→4255.64 | 4254.49→4246.42 | False |
| 10:15 | 4 | 4257.1 | 4257.32→4256.0 | 4255.36→4249.98 | False |
| 10:25 | 10 | 4274.6 | 4259.8→4258.53 | 4261.77→4258.19 | False |

### M&M 2026-07-31

- EMA seed5=3299.117 seed10=3293.6637 (`close_only`)
- VWAP input: `typical_price_(H+L+C)/3 * volume from 1m, session-anchored 09:15 IST`

| hhmm | n1m | close | VWAP old→new | EMA5 old→new | old EMA5==close? |
|-----:|----:|------:|--------------|--------------|:----------------:|
| 09:25 | 10 | 3350.1 | 3320.17→3341.33 | 3350.1→3316.11 | True |
| 09:35 | 10 | 3376.3 | 3342.52→3350.74 | 3358.83→3336.17 | False |
| 09:45 | 6 | 3352.7 | 3345.22→3352.87 | 3356.79→3341.68 | False |
| 09:55 | 10 | 3367.4 | 3353.92→3360.47 | 3360.33→3350.26 | False |
| 10:05 | 9 | 3363.0 | 3356.11→3362.16 | 3361.22→3354.5 | False |
| 10:15 | 7 | 3361.9 | 3356.4→3362.12 | 3361.44→3356.97 | False |

### BAJFINANCE 2026-07-31

- EMA seed5=1058.0865 seed10=1054.4999 (`close_only`)
- VWAP input: `typical_price_(H+L+C)/3 * volume from 1m, session-anchored 09:15 IST`

| hhmm | n1m | close | VWAP old→new | EMA5 old→new | old EMA5==close? |
|-----:|----:|------:|--------------|--------------|:----------------:|
| 09:25 | 10 | 1106.6 | 1100.5→1098.24 | 1106.6→1074.26 | True |
| 09:35 | 10 | 1118.7 | 1107.66→1105.6 | 1110.63→1089.07 | False |
| 09:45 | 6 | 1121.5 | 1109.89→1108.18 | 1114.26→1099.88 | False |
| 09:55 | 10 | 1125.7 | 1113.35→1111.91 | 1118.07→1108.49 | False |
| 10:05 | 10 | 1128.8 | 1115.79→1114.63 | 1121.65→1115.26 | False |
| 10:15 | 7 | 1126.4 | 1116.27→1115.14 | 1123.23→1118.97 | False |

### SIEMENS 2026-07-31

- EMA seed5=3631.364 seed10=3625.5129 (`close_only`)
- VWAP input: `typical_price_(H+L+C)/3 * volume from 1m, session-anchored 09:15 IST`

| hhmm | n1m | close | VWAP old→new | EMA5 old→new | old EMA5==close? |
|-----:|----:|------:|--------------|--------------|:----------------:|
| 09:25 | 1 | 3680.0 | 3680.0→3680.0 | 3680.0→3647.58 | True |
| 09:35 | 1 | 3726.3 | 3722.89→3722.89 | 3695.43→3673.82 | False |
| 09:55 | 10 | 3755.6 | 3749.58→3750.68 | 3715.49→3701.08 | False |
| 10:05 | 6 | 3755.7 | 3751.31→3752.19 | 3728.89→3719.29 | False |
| 10:25 | 10 | 3770.2 | 3757.4→3757.56 | 3742.66→3736.26 | False |
| 10:35 | 6 | 3772.3 | 3759.85→3759.83 | 3752.54→3748.27 | False |

### LODHA 2026-07-28

- EMA seed5=1156.4199 seed10=1152.7329 (`close_only`)
- VWAP input: `typical_price_(H+L+C)/3 * volume from 1m, session-anchored 09:15 IST`

| hhmm | n1m | close | VWAP old→new | EMA5 old→new | old EMA5==close? |
|-----:|----:|------:|--------------|--------------|:----------------:|
| 09:25 | 10 | 1194.0 | 1190.78→1191.64 | 1194.0→1168.95 | True |
| 09:35 | 10 | 1201.0 | 1195.53→1196.03 | 1196.33→1179.63 | False |
| 09:45 | 10 | 1215.45 | 1200.94→1200.04 | 1202.71→1191.57 | False |
| 09:55 | 10 | 1203.3 | 1202.68→1202.42 | 1202.9→1195.48 | False |
| 10:05 | 10 | 1202.0 | 1202.92→1202.93 | 1202.6→1197.65 | False |
| 10:15 | 10 | 1210.95 | 1203.7→1203.5 | 1205.38→1202.09 | False |

## Threshold sensitivity (v1.2 primary: LOCF rank × OW×VW, EW badge)

- Best F1: thr=5.0 F1=0.287 P=0.168 R=0.986 (TP 70/FP 346/FN 1)

| thr | proposed | TP | FP | FN | P | R | F1 | vs v1.1@10 ΔP | ΔR |
|----:|---------:|---:|---:|---:|--:|--:|---:|-------------:|---:|
| 5.0 | 416 | 70 | 346 | 1 | 0.168 | 0.986 | 0.287 |  |  |
| 8.0 | 411 | 67 | 344 | 4 | 0.163 | 0.944 | 0.278 |  |  |
| 10.0 | 409 | 67 | 342 | 4 | 0.164 | 0.944 | 0.279 | -0.003 | 0.0 |
| 12.0 | 402 | 65 | 337 | 6 | 0.162 | 0.915 | 0.275 |  |  |
| 12.2 | 402 | 65 | 337 | 6 | 0.162 | 0.915 | 0.275 |  |  |
| 15.0 | 391 | 63 | 328 | 8 | 0.161 | 0.887 | 0.273 |  |  |
| 18.0 | 379 | 60 | 319 | 11 | 0.158 | 0.845 | 0.266 |  |  |
| 19.0 | 373 | 60 | 313 | 11 | 0.161 | 0.845 | 0.27 |  |  |
| 20.0 | 370 | 58 | 312 | 13 | 0.157 | 0.817 | 0.263 |  |  |
| 25.0 | 324 | 46 | 278 | 25 | 0.142 | 0.648 | 0.233 |  |  |
| 28.5 | 294 | 41 | 253 | 30 | 0.139 | 0.577 | 0.224 |  |  |
| 30.0 | 283 | 37 | 246 | 34 | 0.131 | 0.521 | 0.209 |  |  |
| 35.0 | 236 | 32 | 204 | 39 | 0.136 | 0.451 | 0.209 |  |  |
| 40.0 | 200 | 26 | 174 | 45 | 0.13 | 0.366 | 0.192 |  |  |
| 41.2 | 189 | 25 | 164 | 46 | 0.132 | 0.352 | 0.192 |  |  |
| 45.0 | 156 | 20 | 136 | 51 | 0.128 | 0.282 | 0.176 |  |  |
| 50.0 | 116 | 15 | 101 | 56 | 0.129 | 0.211 | 0.16 |  |  |
| 51.1 | 107 | 14 | 93 | 57 | 0.131 | 0.197 | 0.157 |  |  |
| 55.0 | 74 | 11 | 63 | 60 | 0.149 | 0.155 | 0.152 |  |  |
| 56.7 | 59 | 9 | 50 | 62 | 0.153 | 0.127 | 0.139 |  |  |
| 60.0 | 41 | 7 | 34 | 64 | 0.171 | 0.099 | 0.125 |  |  |

## Ablation at best-F1 threshold

- **v12_full_ow_vw:** TP=70 FP=346 FN=1 P=0.168 R=0.986
- **v12_plus_ew_unlock:** TP=68 FP=312 FN=3 P=0.179 R=0.958
- **OW_alone:** TP=70 FP=347 FN=1 P=0.168 R=0.986
- **VW_alone:** TP=71 FP=349 FN=0 P=0.169 R=1.0

## Before/after @ threshold 10

| config | TP | FP | FN | P | R |
|--------|---:|---:|---:|--:|--:|
| v1.1 (prior) | 67 | 335 | 4 | 0.167 | 0.944 |
| v1.2 corrected | 67 | 342 | 4 | 0.164 | 0.944 |

## Non-requirements

- No live wiring, dashboard, or deploy
