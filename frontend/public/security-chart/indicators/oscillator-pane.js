/**
 * Reusable oscillator sub-pane (RSI or Williams %R) with upper/lower bands.
 */
(function (global) {
    'use strict';

    function theme(isDark) {
        if (isDark) {
            return {
                line: '#a78bfa',
                band: 'rgba(148, 163, 184, 0.55)',
                fill: 'rgba(167, 139, 250, 0.12)',
                grid: 'rgba(180, 210, 190, 0.14)',
                text: '#8fa398',
                bg: '#0c1210',
            };
        }
        return {
            line: '#7c3aed',
            band: 'rgba(100, 116, 139, 0.7)',
            fill: 'rgba(124, 58, 237, 0.1)',
            grid: 'rgba(40, 70, 55, 0.16)',
            text: '#3d5248',
            bg: '#f4f7f5',
        };
    }

    function buildRsi(bars, period) {
        const CI = global.ChartIndicators;
        if (!CI || !CI.rsiWilder || !bars || !bars.length) return [];
        const closes = bars.map(function (b) { return Number(b.close); });
        const arr = CI.rsiWilder(closes, period);
        const out = [];
        for (let i = 0; i < bars.length; i++) {
            if (arr[i] != null && Number.isFinite(arr[i])) {
                out.push({ time: bars[i].time, value: arr[i] });
            }
        }
        return out;
    }

    function buildWilliams(bars, period) {
        const CI = global.ChartIndicators;
        if (!CI || !CI.williamsR || !bars || !bars.length) return [];
        const highs = bars.map(function (b) { return Number(b.high); });
        const lows = bars.map(function (b) { return Number(b.low); });
        const closes = bars.map(function (b) { return Number(b.close); });
        const arr = CI.williamsR(highs, lows, closes, period);
        const out = [];
        for (let i = 0; i < bars.length; i++) {
            if (arr[i] != null && Number.isFinite(arr[i])) {
                out.push({ time: bars[i].time, value: arr[i] });
            }
        }
        return out;
    }

    function OscillatorPaneIndicator(kind) {
        this.kind = kind === 'williams' ? 'williams' : 'rsi';
        this.chart = null;
        this.series = {};
        this._syncUnsub = null;
        this._cacheKey = '';
        this._lineData = null;
        this._lastOpts = null;
    }

    OscillatorPaneIndicator.prototype._cacheKeyFor = function (bars, period, upper, lower) {
        if (!bars || !bars.length) return '';
        const last = bars[bars.length - 1];
        return (
            this.kind +
            ':' +
            bars.length +
            ':' +
            (last.utcTime || last.time) +
            ':' +
            last.close +
            ':' +
            period +
            ':' +
            upper +
            ':' +
            lower
        );
    };

    OscillatorPaneIndicator.prototype.destroy = function (containerEl) {
        if (this._syncUnsub) {
            try { this._syncUnsub(); } catch (e) { /* */ }
            this._syncUnsub = null;
        }
        if (this.chart) {
            try { this.chart.remove(); } catch (e) { /* */ }
            this.chart = null;
        }
        this.series = {};
        this._cacheKey = '';
        this._lineData = null;
        this._lastOpts = null;
        if (containerEl) containerEl.innerHTML = '';
    };

    OscillatorPaneIndicator.prototype.syncFromMain = function (mainChart) {
        if (!this.chart || !mainChart || !mainChart.timeScale) return;
        try {
            const range = mainChart.timeScale().getVisibleLogicalRange();
            if (range) this.chart.timeScale().setVisibleLogicalRange(range);
        } catch (e) { /* */ }
    };

    OscillatorPaneIndicator.prototype.render = function (opts) {
        const LWC = global.LightweightCharts;
        if (!LWC || !opts || !opts.container) return;
        const bars = opts.bars || [];
        const period = Math.max(2, Math.floor(Number(opts.period) || 14));
        const upper = Number(opts.upper);
        const lower = Number(opts.lower);
        const isDark = !!opts.isDark;
        const colors = theme(isDark);
        const key = this._cacheKeyFor(bars, period, upper, lower);
        if (key !== this._cacheKey || !this._lineData) {
            this._cacheKey = key;
            this._lineData =
                this.kind === 'williams' ? buildWilliams(bars, period) : buildRsi(bars, period);
        }
        const lineData = this._lineData || [];
        const bandUpper = [];
        const bandLower = [];
        for (let i = 0; i < bars.length; i++) {
            const t = bars[i].time;
            if (Number.isFinite(upper)) bandUpper.push({ time: t, value: upper });
            if (Number.isFinite(lower)) bandLower.push({ time: t, value: lower });
        }

        const el = opts.container;
        if (!this.chart) {
            el.innerHTML = '';
            this.chart = LWC.createChart(el, {
                layout: { background: { color: colors.bg }, textColor: colors.text },
                grid: {
                    vertLines: { color: colors.grid },
                    horzLines: { color: colors.grid },
                },
                rightPriceScale: { borderColor: colors.grid },
                timeScale: {
                    borderColor: colors.grid,
                    timeVisible: true,
                    secondsVisible: false,
                    tickMarkFormatter: opts.tickMarkFormatter,
                },
                localization: { timeFormatter: opts.timeFormatter },
                crosshair: { mode: LWC.CrosshairMode.Normal },
                width: el.clientWidth || 400,
                height: el.clientHeight || 120,
            });
            this.series.line = this.chart.addLineSeries({
                color: this.kind === 'williams' ? '#f472b6' : colors.line,
                lineWidth: 2,
                priceLineVisible: false,
                lastValueVisible: true,
            });
            this.series.upper = this.chart.addLineSeries({
                color: colors.band,
                lineWidth: 1,
                lineStyle: LWC.LineStyle.Dashed,
                priceLineVisible: false,
                lastValueVisible: false,
            });
            this.series.lower = this.chart.addLineSeries({
                color: colors.band,
                lineWidth: 1,
                lineStyle: LWC.LineStyle.Dashed,
                priceLineVisible: false,
                lastValueVisible: false,
            });
            if (this.kind === 'rsi') {
                this.chart.priceScale('right').applyOptions({
                    scaleMargins: { top: 0.08, bottom: 0.08 },
                });
            }
        }

        this.series.line.setData(lineData);
        this.series.upper.setData(bandUpper);
        this.series.lower.setData(bandLower);

        // Autosize scale range
        if (this.kind === 'rsi') {
            this.chart.priceScale('right').applyOptions({ autoScale: true });
        } else {
            this.chart.priceScale('right').applyOptions({ autoScale: true });
        }

        this.chart.applyOptions({
            width: el.clientWidth || 400,
            height: el.clientHeight || 120,
        });

        if (opts.mainChart && !this._syncUnsub) {
            const self = this;
            const main = opts.mainChart;
            const handler = function () {
                self.syncFromMain(main);
            };
            main.timeScale().subscribeVisibleLogicalRangeChange(handler);
            this._syncUnsub = function () {
                try {
                    main.timeScale().unsubscribeVisibleLogicalRangeChange(handler);
                } catch (e) { /* */ }
            };
            this.syncFromMain(main);
        }
        this._lastOpts = opts;
    };

    OscillatorPaneIndicator.prototype.updateLast = function (bars, opts) {
        if (!this.chart || !this.series.line) {
            this.render(Object.assign({}, opts, { bars: bars }));
            return;
        }
        const period = Math.max(2, Math.floor(Number((opts && opts.period) || 14)));
        const data =
            this.kind === 'williams' ? buildWilliams(bars, period) : buildRsi(bars, period);
        if (data.length) {
            this.series.line.update(data[data.length - 1]);
        }
    };

    global.OscillatorPaneIndicator = OscillatorPaneIndicator;
})(typeof window !== 'undefined' ? window : globalThis);
