# Item D — VWAP touch-reject win-rate + baseline (B2-style bar)

**Decision:** `NO_GO_EVEN_SHADOW_RULE` — **thread CLOSED 2026-07-23** (no further backtest). Forward log may continue.

## Verdict reasons

- Fails B2-style go bar vs baseline / win-rate / median
- LONG: win_rate edge vs all-bars only 2.2pp (<3pp)
- LONG: mean heavily outlier-pulled (mean-median=1.8842)
- SHORT: mean heavily outlier-pulled (mean-median=2.5124)

**Horizon:** n3 × 10m bars (~30m). **Near-VWAP baseline:** |close−VWAP|/close ≤ 0.15%.

## 1. Win-rate + distribution (reject events)

| Side | n | Win-rate | Mean | Median | Mean−Median | p10 | p90 |
|---|---:|---:|---:|---:|---:|---:|---:|
| LONG | 1118 | 53.58% | 2.1842 | 0.3 | 1.8842 | -8.0 | 12.6 |
| SHORT | 887 | 59.41% | 2.9224 | 0.41 | 2.5124 | -7.1 | 14.25 |
| COMBINED | 2005 | 56.16% | 2.5108 | 0.39 | 2.1208 | -7.85 | 13.3 |

### Buckets (pts)

**LONG:** <0=498 (44.5%), 0-1=151 (13.5%), 1-3=154 (13.8%), 3-5=69 (6.2%), >5=246 (22.0%)
**SHORT:** <0=338 (38.1%), 0-1=166 (18.7%), 1-3=123 (13.9%), 3-5=60 (6.8%), >5=200 (22.5%)

## 2. Baseline comparison

| Cohort | Side | n | Win-rate | Mean | Median |
|---|---|---:|---:|---:|---:|
| Reject | LONG | 1118 | 53.58% | 2.1842 | 0.3 |
| Reject | SHORT | 887 | 59.41% | 2.9224 | 0.41 |
| All non-reject bars | LONG | 7782 | 51.37% | 2.3282 | 0.1 |
| All non-reject bars | SHORT | 6517 | 54.3% | 1.5622 | 0.2 |
| Near-VWAP non-reject | LONG | 1498 | 55.14% | 2.6027 | 0.3 |
| Near-VWAP non-reject | SHORT | 1093 | 53.89% | 3.9223 | 0.2 |

### Edge (reject − baseline)

| vs | Side | Δ win-rate (pp) | Δ mean | Δ median |
|---|---|---:|---:|---:|
| all non-reject | LONG | 2.21 | -0.144 | 0.2 |
| all non-reject | SHORT | 5.11 | 1.3602 | 0.21 |
| near-VWAP non-reject | LONG | -1.56 | -0.4185 | 0.0 |
| near-VWAP non-reject | SHORT | 5.52 | -0.9999 | 0.21 |

## 3. Splits (AM/PM + wick tercile)

### AM vs PM

**AM**
- LONG: n=777 win=57.66% mean=3.8268 median=0.8
- SHORT: n=661 win=62.93% mean=3.5574 median=0.7
**PM**
- LONG: n=341 win=44.28% mean=-1.5586 median=-0.1
- SHORT: n=226 win=49.12% mean=1.0651 median=0.0

### Wick terciles (cuts p33=0.6964, p66=2.8790 pts)

**wick_low**
- LONG: n=348 win=54.6% mean=2.2673 median=0.195
- SHORT: n=321 win=58.57% mean=0.4976 median=0.17
**wick_mid**
- LONG: n=403 win=53.1% mean=1.4938 median=0.5
- SHORT: n=265 win=60.0% mean=1.7026 median=0.8
**wick_high**
- LONG: n=367 win=53.13% mean=2.8636 median=0.7
- SHORT: n=301 win=59.8% mean=6.5823 median=2.5

## Conclusion (shadow)

1. Decision: **NO_GO_EVEN_SHADOW_RULE**.
2. Same bar as B2: averages alone are insufficient; win-rate + baseline edge required.
3. Do **not** wire live / Pine from this slice.

