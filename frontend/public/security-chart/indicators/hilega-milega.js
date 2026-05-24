/**
 * Hilega-Milega oscillator panel — RSI(9) + EMA(3) + WMA(21) on RSI.
 * Separate synced Lightweight Charts instance below the main price chart.
 */
(function (global) {
    'use strict';

    const RSI_PERIOD = 9;
    const EMA_PERIOD = 3;
    const WMA_PERIOD = 21;
    const MEDIAN = 50;

    function hmTheme(isDark) {
        if (isDark) {
            return {
                rsiLine: '#22d3ee',
                emaLine: '#fbbf24',
                wmaLine: '#e879f9',
                fillAbove: 'rgba(34, 197, 94, 0.38)',
                fillBelow: 'rgba(56, 189, 248, 0.38)',
                medianLine: '#64748b',
                grid: '#334155',
                text: '#94a3b8',
                bg: '#0f172a',
            };
        }
        return {
            rsiLine: '#0284c7',
            emaLine: '#d97706',
            wmaLine: '#a21caf',
            fillAbove: 'rgba(22, 163, 74, 0.32)',
            fillBelow: 'rgba(37, 99, 235, 0.28)',
            medianLine: '#94a3b8',
            grid: '#e2e8f0',
            text: '#64748b',
            bg: '#ffffff',
        };
    }

    function buildDataset(bars) {
        const CI = global.ChartIndicators;
        if (!CI || !bars || !bars.length) {
            return { rsi: [], ema: [], wma: [], median: [] };
        }
        const closes = bars.map(function (b) {
            return Number(b.close);
        });
        const rsiArr = CI.rsiWilder(closes, RSI_PERIOD);
        const emaArr = CI.emaOverSeries(rsiArr, EMA_PERIOD);
        const wmaArr = CI.wmaOverSeries(rsiArr, WMA_PERIOD);

        const rsi = [];
        const ema = [];
        const wma = [];
        const median = [];

        for (let i = 0; i < bars.length; i++) {
            const t = bars[i].time;
            if (rsiArr[i] != null && Number.isFinite(rsiArr[i])) {
                rsi.push({ time: t, value: rsiArr[i] });
            }
            if (emaArr[i] != null && Number.isFinite(emaArr[i])) {
                ema.push({ time: t, value: emaArr[i] });
            }
            if (wmaArr[i] != null && Number.isFinite(wmaArr[i])) {
                wma.push({ time: t, value: wmaArr[i] });
            }
            median.push({ time: t, value: MEDIAN });
        }

        return { rsi: rsi, ema: ema, wma: wma, median: median, rsiArr: rsiArr, emaArr: emaArr, wmaArr: wmaArr };
    }

    function HilegaMilegaIndicator() {
        this.chart = null;
        this.series = {};
        this._syncUnsub = null;
        this._cacheKey = '';
        this._dataset = null;
        this._lastOpts = null;
    }

    HilegaMilegaIndicator.prototype._cacheKeyFor = function (bars) {
        if (!bars || !bars.length) return '';
        const last = bars[bars.length - 1];
        return bars.length + ':' + (last.utcTime || last.time) + ':' + last.close;
    };

    HilegaMilegaIndicator.prototype.getDataset = function (bars, force) {
        const key = this._cacheKeyFor(bars);
        if (!force && key && key === this._cacheKey && this._dataset) {
            return this._dataset;
        }
        this._cacheKey = key;
        this._dataset = buildDataset(bars);
        return this._dataset;
    };

    HilegaMilegaIndicator.prototype.destroy = function (containerEl) {
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
        this._cacheKey = '';
        this._dataset = null;
        this._lastOpts = null;
        if (containerEl) containerEl.innerHTML = '';
    };

    HilegaMilegaIndicator.prototype.syncFromMain = function (mainChart) {
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

    HilegaMilegaIndicator.prototype.render = function (opts) {
        const LWC = global.LightweightCharts;
        const container = opts && opts.container;
        const bars = (opts && opts.bars) || [];
        const isDark = opts && opts.isDark !== false;
        const mainChart = opts && opts.mainChart;
        const timeFormatter = opts && opts.timeFormatter;

        if (!LWC || !container) return;

        this.destroy(container);

        if (!bars.length) return;

        const theme = hmTheme(isDark);
        const ds = this.getDataset(bars, true);

        this.chart = LWC.createChart(container, {
            layout: { background: { color: theme.bg }, textColor: theme.text },
            grid: { vertLines: { color: theme.grid }, horzLines: { color: theme.grid } },
            crosshair: { mode: LWC.CrosshairMode.Normal },
            rightPriceScale: {
                borderColor: theme.grid,
                scaleMargins: { top: 0.08, bottom: 0.08 },
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

        this.series.rsiBaseline = this.chart.addBaselineSeries({
            baseValue: { type: 'price', price: MEDIAN },
            topLineColor: theme.rsiLine,
            bottomLineColor: theme.rsiLine,
            topFillColor1: theme.fillAbove,
            topFillColor2: theme.fillAbove,
            bottomFillColor1: theme.fillBelow,
            bottomFillColor2: theme.fillBelow,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: 'RSI(9)',
        });

        this.series.ema = this.chart.addLineSeries({
            color: theme.emaLine,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: 'EMA(3)',
        });

        this.series.wma = this.chart.addLineSeries({
            color: theme.wmaLine,
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            title: 'WMA(21)',
        });

        this.series.median = this.chart.addLineSeries({
            color: theme.medianLine,
            lineWidth: 1,
            lineStyle: LWC.LineStyle ? LWC.LineStyle.Dashed : 2,
            priceLineVisible: false,
            lastValueVisible: false,
            title: '50',
        });

        this.series.rsiBaseline.setData(ds.rsi);
        this.series.ema.setData(ds.ema);
        this.series.wma.setData(ds.wma);
        this.series.median.setData(ds.median);

        this.chart.priceScale('right').applyOptions({
            autoScale: true,
        });

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

    HilegaMilegaIndicator.prototype.updateLastBar = function (bars) {
        if (!this.chart || !this.series.rsiBaseline || !bars || !bars.length) return;
        const ds = this.getDataset(bars, false);
        const lastRsi = ds.rsi[ds.rsi.length - 1];
        const lastEma = ds.ema[ds.ema.length - 1];
        const lastWma = ds.wma[ds.wma.length - 1];
        const lastMed = ds.median[ds.median.length - 1];
        try {
            if (lastRsi) this.series.rsiBaseline.update(lastRsi);
            if (lastEma) this.series.ema.update(lastEma);
            if (lastWma) this.series.wma.update(lastWma);
            if (lastMed) this.series.median.update(lastMed);
        } catch (e) {
            if (this._lastOpts) {
                this.render(
                    Object.assign({}, this._lastOpts, {
                        bars: bars,
                        mainChart: this._lastOpts.mainChart,
                    })
                );
            }
        }
    };

    HilegaMilegaIndicator.prototype.resize = function (width, height) {
        if (!this.chart || !width || !height) return;
        try {
            this.chart.applyOptions({ width: width, height: height });
        } catch (e) {
            /* ignore */
        }
    };

    HilegaMilegaIndicator.prototype.computeDataset = buildDataset;
    HilegaMilegaIndicator.RSI_PERIOD = RSI_PERIOD;
    HilegaMilegaIndicator.EMA_PERIOD = EMA_PERIOD;
    HilegaMilegaIndicator.WMA_PERIOD = WMA_PERIOD;

    global.HilegaMilegaIndicator = HilegaMilegaIndicator;
})(typeof window !== 'undefined' ? window : globalThis);
