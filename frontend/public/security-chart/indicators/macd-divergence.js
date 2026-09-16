/**
 * MACD Divergence pane — port of TWCTO Commodity Divergence (Pine v6).
 * MACD(12/26/9 EMA) + regular bull/bear pivots + GO/EXIT state machine.
 * Separate synced Lightweight Charts instance below the main price chart.
 *
 * Caveats vs Pine:
 * - BUY/SELL/X markers are applied on the main candle series by the chart engine
 *   (Pine force_overlay); Bull/Bear labels sit on this MACD pane.
 * - Divergence "lines" are approximated as markers at pivot bars (plus optional
 *   short line segments between the two pivots that form a divergence).
 * - Hidden bull/bear divergences are computed but not plotted (Pine defaults off).
 */
(function (global) {
    'use strict';

    const FAST = 12;
    const SLOW = 26;
    const SIGNAL = 9;
    const LB_L = 5;
    const LB_R = 1;
    const RANGE_UPPER = 60;
    const RANGE_LOWER = 5;
    const DONT_TOUCH_ZERO = true;
    const REENTRY_MAX_BARS = 5;
    const LOOKBACK_ZERO = LB_L + LB_R + 5;

    const COL_MACD = '#2962FF';
    const COL_SIGNAL = '#FF6D00';
    const COL_GROW_ABOVE = '#26A69A';
    const COL_FALL_ABOVE = '#B2DFDB';
    const COL_GROW_BELOW = '#FFCDD2';
    const COL_FALL_BELOW = '#FF5252';
    const BULL_COLOR = '#22c55e';
    const BEAR_COLOR = '#ef4444';
    const EXIT_COLOR = '#94a3b8';

    function mdTheme(isDark) {
        if (isDark) {
            return {
                grid: '#334155',
                text: '#94a3b8',
                bg: '#0f172a',
                zero: '#64748b',
            };
        }
        return {
            grid: '#e2e8f0',
            text: '#64748b',
            bg: '#ffffff',
            zero: '#94a3b8',
        };
    }

    /** SMA-seeded EMA matching TradingView ta.ema / chart engine computeEma. */
    function emaSeries(values, period) {
        const n = values.length;
        const out = new Array(n).fill(null);
        const p = Math.max(1, Math.floor(period));
        if (n < p) return out;
        let sum = 0;
        for (let i = 0; i < p; i++) {
            const v = values[i];
            if (v == null || !Number.isFinite(v)) return out;
            sum += v;
        }
        let ema = sum / p;
        out[p - 1] = ema;
        const k = 2 / (p + 1);
        for (let i = p; i < n; i++) {
            const v = values[i];
            if (v == null || !Number.isFinite(v)) {
                out[i] = null;
                continue;
            }
            ema = (v - ema) * k + ema;
            out[i] = ema;
        }
        return out;
    }

    function isPivotLow(osc, i, lbL, lbR) {
        const pivotIdx = i - lbR;
        if (pivotIdx - lbL < 0 || i >= osc.length) return false;
        const pv = osc[pivotIdx];
        if (pv == null || !Number.isFinite(pv)) return false;
        for (let j = pivotIdx - lbL; j <= pivotIdx + lbR; j++) {
            if (j === pivotIdx) continue;
            const v = osc[j];
            // TradingView: neighbors must be strictly higher than the pivot low
            if (v == null || !Number.isFinite(v) || v <= pv) return false;
        }
        return true;
    }

    function isPivotHigh(osc, i, lbL, lbR) {
        const pivotIdx = i - lbR;
        if (pivotIdx - lbL < 0 || i >= osc.length) return false;
        const pv = osc[pivotIdx];
        if (pv == null || !Number.isFinite(pv)) return false;
        for (let j = pivotIdx - lbL; j <= pivotIdx + lbR; j++) {
            if (j === pivotIdx) continue;
            const v = osc[j];
            // TradingView: neighbors must be strictly lower than the pivot high
            if (v == null || !Number.isFinite(v) || v >= pv) return false;
        }
        return true;
    }

    /** ta.barssince — bars since condition was true (0 if true on i). */
    function barsSinceTrue(flags, i) {
        for (let j = i; j >= 0; j--) {
            if (flags[j]) return i - j;
        }
        return null;
    }

    function inRangeBars(bars) {
        return bars != null && RANGE_LOWER <= bars && bars <= RANGE_UPPER;
    }

    function highestOsc(osc, endIdx, len) {
        let hi = -Infinity;
        const start = Math.max(0, endIdx - len + 1);
        for (let j = start; j <= endIdx; j++) {
            const v = osc[j];
            if (v != null && Number.isFinite(v) && v > hi) hi = v;
        }
        return hi === -Infinity ? null : hi;
    }

    function lowestOsc(osc, endIdx, len) {
        let lo = Infinity;
        const start = Math.max(0, endIdx - len + 1);
        for (let j = start; j <= endIdx; j++) {
            const v = osc[j];
            if (v != null && Number.isFinite(v) && v < lo) lo = v;
        }
        return lo === Infinity ? null : lo;
    }

    /**
     * Full bar-by-bar computation matching Pine TWCTO Commodity Divergence.
     * @param {{time:*, open:number, high:number, low:number, close:number}[]} bars
     */
    function buildDataset(bars) {
        const n = bars ? bars.length : 0;
        const empty = {
            hist: [],
            macd: [],
            signal: [],
            zero: [],
            paneMarkers: [],
            priceMarkers: [],
            bullLines: [],
            bearLines: [],
            macdArr: [],
            histArr: [],
        };
        if (!n) return empty;

        const closes = bars.map(function (b) {
            return Number(b.close);
        });
        const highs = bars.map(function (b) {
            return Number(b.high);
        });
        const lows = bars.map(function (b) {
            return Number(b.low);
        });

        const fastMa = emaSeries(closes, FAST);
        const slowMa = emaSeries(closes, SLOW);
        const macdArr = new Array(n).fill(null);
        for (let i = 0; i < n; i++) {
            if (fastMa[i] != null && slowMa[i] != null) {
                macdArr[i] = fastMa[i] - slowMa[i];
            }
        }
        // Signal EMA over MACD — skip leading nulls by seeding on first contiguous valid stretch
        const signalArr = new Array(n).fill(null);
        {
            const valid = [];
            for (let i = 0; i < n; i++) {
                if (macdArr[i] != null) valid.push(i);
            }
            if (valid.length >= SIGNAL) {
                let sum = 0;
                for (let k = 0; k < SIGNAL; k++) sum += macdArr[valid[k]];
                let ema = sum / SIGNAL;
                signalArr[valid[SIGNAL - 1]] = ema;
                const kMul = 2 / (SIGNAL + 1);
                for (let k = SIGNAL; k < valid.length; k++) {
                    const idx = valid[k];
                    ema = (macdArr[idx] - ema) * kMul + ema;
                    signalArr[idx] = ema;
                }
            }
        }

        const histArr = new Array(n).fill(null);
        for (let i = 0; i < n; i++) {
            if (macdArr[i] != null && signalArr[i] != null) {
                histArr[i] = macdArr[i] - signalArr[i];
            }
        }

        const plFound = new Array(n).fill(false);
        const phFound = new Array(n).fill(false);
        for (let i = 0; i < n; i++) {
            plFound[i] = isPivotLow(macdArr, i, LB_L, LB_R);
            phFound[i] = isPivotHigh(macdArr, i, LB_L, LB_R);
        }

        // plFound[1] series for barssince
        const plFoundPrev = new Array(n).fill(false);
        const phFoundPrev = new Array(n).fill(false);
        for (let i = 1; i < n; i++) {
            plFoundPrev[i] = plFound[i - 1];
            phFoundPrev[i] = phFound[i - 1];
        }

        const bullCond = new Array(n).fill(false);
        const bearCond = new Array(n).fill(false);
        const paneMarkers = [];
        const bullLines = [];
        const bearLines = [];

        // Track prior pivot confirmation indices for valuewhen(..., 1)
        const plConfirmIdxs = [];
        const phConfirmIdxs = [];

        for (let i = 0; i < n; i++) {
            const pivotBar = i - LB_R;
            if (plFound[i] && pivotBar >= 0) {
                const inRangePl1 = inRangeBars(barsSinceTrue(plFoundPrev, i));
                const oscNow = macdArr[pivotBar];
                let prevOsc = null;
                let prevPivotBar = null;
                if (plConfirmIdxs.length >= 1) {
                    const prevConfirm = plConfirmIdxs[plConfirmIdxs.length - 1];
                    prevPivotBar = prevConfirm - LB_R;
                    prevOsc = macdArr[prevPivotBar];
                }
                const oscHL =
                    prevOsc != null &&
                    oscNow != null &&
                    oscNow > prevOsc &&
                    inRangePl1 &&
                    oscNow < 0;
                const priceLL =
                    prevPivotBar != null &&
                    lows[pivotBar] < lows[prevPivotBar];
                const hh = highestOsc(macdArr, i, LOOKBACK_ZERO);
                const blowzero = DONT_TOUCH_ZERO ? hh != null && hh < 0 : true;
                const isBull = oscHL && priceLL && blowzero;
                bullCond[i] = isBull;

                if (isBull && prevPivotBar != null && oscNow != null && prevOsc != null) {
                    paneMarkers.push({
                        time: bars[pivotBar].time,
                        position: 'belowBar',
                        color: BULL_COLOR,
                        shape: 'arrowUp',
                        text: 'Bull',
                    });
                    bullLines.push([
                        { time: bars[prevPivotBar].time, value: prevOsc },
                        { time: bars[pivotBar].time, value: oscNow },
                    ]);
                }
                plConfirmIdxs.push(i);
            }

            if (phFound[i] && pivotBar >= 0) {
                const inRangePh1 = inRangeBars(barsSinceTrue(phFoundPrev, i));
                const oscNow = macdArr[pivotBar];
                let prevOsc = null;
                let prevPivotBar = null;
                if (phConfirmIdxs.length >= 1) {
                    const prevConfirm = phConfirmIdxs[phConfirmIdxs.length - 1];
                    prevPivotBar = prevConfirm - LB_R;
                    prevOsc = macdArr[prevPivotBar];
                }
                const oscLH =
                    prevOsc != null &&
                    oscNow != null &&
                    oscNow < prevOsc &&
                    inRangePh1 &&
                    oscNow > 0;
                const priceHH =
                    prevPivotBar != null &&
                    highs[pivotBar] > highs[prevPivotBar];
                const ll = lowestOsc(macdArr, i, LOOKBACK_ZERO);
                const bearzero = DONT_TOUCH_ZERO ? ll != null && ll > 0 : true;
                const isBear = oscLH && priceHH && bearzero;
                bearCond[i] = isBear;

                if (isBear && prevPivotBar != null && oscNow != null && prevOsc != null) {
                    paneMarkers.push({
                        time: bars[pivotBar].time,
                        position: 'aboveBar',
                        color: BEAR_COLOR,
                        shape: 'arrowDown',
                        text: 'Bear',
                    });
                    bearLines.push([
                        { time: bars[prevPivotBar].time, value: prevOsc },
                        { time: bars[pivotBar].time, value: oscNow },
                    ]);
                }
                phConfirmIdxs.push(i);
            }
        }

        // GO / EXIT state machine (bar order)
        let divState = null;
        let tradeState = null;
        let reentryArmed = null;
        let reentryBarIdx = null;
        const priceMarkers = [];

        for (let i = 0; i < n; i++) {
            const hist = histArr[i];
            const histPrev = i > 0 ? histArr[i - 1] : null;
            const histFlipUp =
                hist != null &&
                histPrev != null &&
                hist > 0 &&
                histPrev <= 0;
            const histFlipDown =
                hist != null &&
                histPrev != null &&
                hist < 0 &&
                histPrev >= 0;

            if (bullCond[i]) divState = 'BULL';
            if (bearCond[i]) divState = 'BEAR';

            if (reentryArmed === 'BULL' && bearCond[i]) reentryArmed = null;
            if (reentryArmed === 'BEAR' && bullCond[i]) reentryArmed = null;

            if (reentryArmed != null && reentryBarIdx != null && i - reentryBarIdx > REENTRY_MAX_BARS) {
                reentryArmed = null;
            }

            const freshGoBull = divState === 'BULL' && hist != null && hist > 0 && tradeState == null;
            const freshGoBear = divState === 'BEAR' && hist != null && hist < 0 && tradeState == null;
            const reentryGoBull =
                reentryArmed === 'BULL' && hist != null && hist > 0 && tradeState == null;
            const reentryGoBear =
                reentryArmed === 'BEAR' && hist != null && hist < 0 && tradeState == null;

            const goBull = freshGoBull || reentryGoBull;
            const goBear = freshGoBear || reentryGoBear;

            if (goBull) {
                tradeState = 'BULL';
                divState = null;
                reentryArmed = null;
                priceMarkers.push({
                    time: bars[i].time,
                    position: 'belowBar',
                    color: BULL_COLOR,
                    shape: 'arrowUp',
                    text: 'BUY',
                });
            }
            if (goBear) {
                tradeState = 'BEAR';
                divState = null;
                reentryArmed = null;
                priceMarkers.push({
                    time: bars[i].time,
                    position: 'aboveBar',
                    color: BEAR_COLOR,
                    shape: 'arrowDown',
                    text: 'SELL',
                });
            }

            const exitBull = tradeState === 'BULL' && histFlipDown;
            const exitBear = tradeState === 'BEAR' && histFlipUp;

            if (exitBull) {
                tradeState = null;
                reentryArmed = 'BULL';
                reentryBarIdx = i;
                priceMarkers.push({
                    time: bars[i].time,
                    position: 'aboveBar',
                    color: EXIT_COLOR,
                    shape: 'circle',
                    text: 'X',
                });
            }
            if (exitBear) {
                tradeState = null;
                reentryArmed = 'BEAR';
                reentryBarIdx = i;
                priceMarkers.push({
                    time: bars[i].time,
                    position: 'belowBar',
                    color: EXIT_COLOR,
                    shape: 'circle',
                    text: 'X',
                });
            }
        }

        const hist = [];
        const macd = [];
        const signal = [];
        const zero = [];
        for (let i = 0; i < n; i++) {
            const t = bars[i].time;
            if (histArr[i] != null) {
                const h = histArr[i];
                const prev = i > 0 ? histArr[i - 1] : null;
                let color;
                if (h >= 0) {
                    color = prev != null && prev < h ? COL_GROW_ABOVE : COL_FALL_ABOVE;
                } else {
                    color = prev != null && prev < h ? COL_GROW_BELOW : COL_FALL_BELOW;
                }
                hist.push({ time: t, value: h, color: color });
            }
            if (macdArr[i] != null) macd.push({ time: t, value: macdArr[i] });
            if (signalArr[i] != null) signal.push({ time: t, value: signalArr[i] });
            zero.push({ time: t, value: 0 });
        }

        return {
            hist: hist,
            macd: macd,
            signal: signal,
            zero: zero,
            paneMarkers: paneMarkers,
            priceMarkers: priceMarkers,
            bullLines: bullLines,
            bearLines: bearLines,
            macdArr: macdArr,
            histArr: histArr,
        };
    }

    function MacdDivergenceIndicator() {
        this.chart = null;
        this.series = {};
        this._lineSeries = [];
        this._syncUnsub = null;
        this._cacheKey = '';
        this._dataset = null;
        this._lastOpts = null;
    }

    MacdDivergenceIndicator.prototype._cacheKeyFor = function (bars) {
        if (!bars || !bars.length) return '';
        const last = bars[bars.length - 1];
        return bars.length + ':' + (last.utcTime || last.time) + ':' + last.close;
    };

    MacdDivergenceIndicator.prototype.getDataset = function (bars, force) {
        const key = this._cacheKeyFor(bars);
        if (!force && key && key === this._cacheKey && this._dataset) {
            return this._dataset;
        }
        this._cacheKey = key;
        this._dataset = buildDataset(bars);
        return this._dataset;
    };

    MacdDivergenceIndicator.prototype.destroy = function (containerEl) {
        if (this._syncUnsub) {
            try {
                this._syncUnsub();
            } catch (e) {
                /* ignore */
            }
            this._syncUnsub = null;
        }
        if (this.chart) {
            try {
                this.chart.remove();
            } catch (e) {
                /* ignore */
            }
            this.chart = null;
        }
        this.series = {};
        this._lineSeries = [];
        this._cacheKey = '';
        this._dataset = null;
        this._lastOpts = null;
        if (containerEl) containerEl.innerHTML = '';
    };

    MacdDivergenceIndicator.prototype.syncFromMain = function (mainChart) {
        const self = this;
        if (this._syncUnsub) {
            this._syncUnsub();
            this._syncUnsub = null;
        }
        if (!mainChart || !this.chart) return;

        let syncing = false;
        function copyRange(from, to) {
            if (syncing) return;
            syncing = true;
            try {
                const range = from.timeScale().getVisibleLogicalRange();
                if (range) to.timeScale().setVisibleLogicalRange(range);
            } catch (e) {
                /* ignore */
            }
            syncing = false;
        }

        copyRange(mainChart, this.chart);

        const onMain = function () {
            copyRange(mainChart, self.chart);
        };
        mainChart.timeScale().subscribeVisibleLogicalRangeChange(onMain);

        this._syncUnsub = function () {
            try {
                mainChart.timeScale().unsubscribeVisibleLogicalRangeChange(onMain);
            } catch (e) {
                /* ignore */
            }
        };
    };

    MacdDivergenceIndicator.prototype._clearDivLines = function () {
        const self = this;
        (this._lineSeries || []).forEach(function (s) {
            try {
                if (self.chart) self.chart.removeSeries(s);
            } catch (e) {
                /* ignore */
            }
        });
        this._lineSeries = [];
    };

    MacdDivergenceIndicator.prototype._drawDivLines = function (ds) {
        if (!this.chart) return;
        this._clearDivLines();
        const self = this;
        function addSegs(segs, color) {
            (segs || []).forEach(function (pair) {
                if (!pair || pair.length < 2) return;
                const s = self.chart.addLineSeries({
                    color: color,
                    lineWidth: 2,
                    priceLineVisible: false,
                    lastValueVisible: false,
                    crosshairMarkerVisible: false,
                });
                s.setData(pair);
                self._lineSeries.push(s);
            });
        }
        addSegs(ds.bullLines, BULL_COLOR);
        addSegs(ds.bearLines, BEAR_COLOR);
    };

    MacdDivergenceIndicator.prototype.render = function (opts) {
        const LWC = global.LightweightCharts;
        const container = opts && opts.container;
        const bars = (opts && opts.bars) || [];
        const isDark = opts && opts.isDark !== false;
        const mainChart = opts && opts.mainChart;
        const timeFormatter = opts && opts.timeFormatter;

        if (!LWC || !container) return;

        this.destroy(container);
        if (!bars.length) return;

        const theme = mdTheme(isDark);
        const ds = this.getDataset(bars, true);

        this.chart = LWC.createChart(container, {
            layout: { background: { color: theme.bg }, textColor: theme.text },
            grid: { vertLines: { color: theme.grid }, horzLines: { color: theme.grid } },
            crosshair: { mode: LWC.CrosshairMode.Normal },
            rightPriceScale: {
                borderColor: theme.grid,
                scaleMargins: { top: 0.1, bottom: 0.1 },
            },
            timeScale: {
                borderColor: theme.grid,
                visible: true,
                timeVisible: true,
                secondsVisible: false,
                tickMarkFormatter: opts && opts.tickMarkFormatter,
            },
            localization: {
                locale: 'en-IN',
                timeFormatter: timeFormatter || undefined,
            },
            handleScroll: false,
            handleScale: false,
        });

        this.series.hist = this.chart.addHistogramSeries({
            priceLineVisible: false,
            lastValueVisible: false,
            title: 'Hist',
        });
        this.series.macd = this.chart.addLineSeries({
            color: COL_MACD,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: 'MACD',
        });
        this.series.signal = this.chart.addLineSeries({
            color: COL_SIGNAL,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: 'Signal',
        });
        this.series.zero = this.chart.addLineSeries({
            color: theme.zero,
            lineWidth: 1,
            lineStyle: LWC.LineStyle ? LWC.LineStyle.Dashed : 2,
            priceLineVisible: false,
            lastValueVisible: false,
        });

        this.series.hist.setData(ds.hist);
        this.series.macd.setData(ds.macd);
        this.series.signal.setData(ds.signal);
        this.series.zero.setData(ds.zero);
        if (this.series.macd.setMarkers) {
            this.series.macd.setMarkers(ds.paneMarkers || []);
        }
        this._drawDivLines(ds);

        this._lastOpts = {
            container: container,
            isDark: isDark,
            mainChart: mainChart,
            timeFormatter: timeFormatter,
            tickMarkFormatter: opts && opts.tickMarkFormatter,
        };

        if (mainChart) this.syncFromMain(mainChart);

        const w = container.clientWidth;
        const h = container.clientHeight;
        if (w && h) {
            this.chart.applyOptions({ width: w, height: h });
        }
    };

    MacdDivergenceIndicator.prototype.updateLastBar = function (bars) {
        if (!this.chart || !this.series.macd || !bars || !bars.length) return;
        // Full recompute — divergence/state machine is not incremental-safe
        if (this._lastOpts) {
            this.render(
                Object.assign({}, this._lastOpts, {
                    bars: bars,
                    mainChart: this._lastOpts.mainChart,
                })
            );
        }
    };

    MacdDivergenceIndicator.prototype.resize = function (width, height) {
        if (!this.chart || !width || !height) return;
        try {
            this.chart.applyOptions({ width: width, height: height });
        } catch (e) {
            /* ignore */
        }
    };

    /** Price-pane markers (BUY/SELL/X) for the main chart candle series. */
    MacdDivergenceIndicator.prototype.getPriceMarkers = function (bars) {
        const ds = this.getDataset(bars || [], false);
        return (ds && ds.priceMarkers) || [];
    };

    MacdDivergenceIndicator.prototype.computeDataset = buildDataset;
    MacdDivergenceIndicator.FAST = FAST;
    MacdDivergenceIndicator.SLOW = SLOW;
    MacdDivergenceIndicator.SIGNAL = SIGNAL;

    global.MacdDivergenceIndicator = MacdDivergenceIndicator;
})(typeof window !== 'undefined' ? window : globalThis);
