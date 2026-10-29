/**
 * Reusable oscillator sub-pane (RSI or Williams %R) with upper/lower bands + zone fill.
 */
(function (global) {
    'use strict';

    function theme(isDark, kind) {
        if (isDark) {
            return {
                line: kind === 'williams' ? '#f472b6' : '#a78bfa',
                band: 'rgba(148, 163, 184, 0.7)',
                fillTop: kind === 'williams' ? 'rgba(244, 114, 182, 0.22)' : 'rgba(167, 139, 250, 0.22)',
                fillBottom: kind === 'williams' ? 'rgba(244, 114, 182, 0.06)' : 'rgba(167, 139, 250, 0.06)',
                mask: '#0c1210',
                grid: 'rgba(180, 210, 190, 0.14)',
                text: '#8fa398',
                bg: '#0c1210',
            };
        }
        return {
            line: kind === 'williams' ? '#db2777' : '#7c3aed',
            band: 'rgba(100, 116, 139, 0.75)',
            fillTop: kind === 'williams' ? 'rgba(219, 39, 119, 0.18)' : 'rgba(124, 58, 237, 0.18)',
            fillBottom: kind === 'williams' ? 'rgba(219, 39, 119, 0.05)' : 'rgba(124, 58, 237, 0.05)',
            mask: '#f4f7f5',
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

    OscillatorPaneIndicator.prototype.resize = function (w, h) {
        if (!this.chart) return;
        try {
            this.chart.applyOptions({
                width: w || 400,
                height: h || 120,
            });
        } catch (e) { /* */ }
    };

    OscillatorPaneIndicator.prototype.render = function (opts) {
        const LWC = global.LightweightCharts;
        if (!LWC || !opts || !opts.container) return;
        const bars = opts.bars || [];
        const period = Math.max(2, Math.floor(Number(opts.period) || 14));
        let upper = Number(opts.upper);
        let lower = Number(opts.lower);
        if (Number.isFinite(upper) && Number.isFinite(lower) && upper < lower) {
            const tmp = upper;
            upper = lower;
            lower = tmp;
        }
        const isDark = !!opts.isDark;
        const colors = theme(isDark, this.kind);
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
            // Zone fill: shade from upper down, then mask everything below lower with bg.
            this.series.zone = this.chart.addAreaSeries({
                topColor: colors.fillTop,
                bottomColor: colors.fillBottom,
                lineColor: 'transparent',
                lineWidth: 1,
                priceLineVisible: false,
                lastValueVisible: false,
                crosshairMarkerVisible: false,
            });
            this.series.zoneMask = this.chart.addAreaSeries({
                topColor: colors.mask,
                bottomColor: colors.mask,
                lineColor: 'transparent',
                lineWidth: 1,
                priceLineVisible: false,
                lastValueVisible: false,
                crosshairMarkerVisible: false,
            });
            this.series.upper = this.chart.addLineSeries({
                color: colors.band,
                lineWidth: 1,
                lineStyle: LWC.LineStyle.Dashed,
                priceLineVisible: false,
                lastValueVisible: false,
                crosshairMarkerVisible: false,
            });
            this.series.lower = this.chart.addLineSeries({
                color: colors.band,
                lineWidth: 1,
                lineStyle: LWC.LineStyle.Dashed,
                priceLineVisible: false,
                lastValueVisible: false,
                crosshairMarkerVisible: false,
            });
            this.series.line = this.chart.addLineSeries({
                color: colors.line,
                lineWidth: 2,
                priceLineVisible: false,
                lastValueVisible: true,
            });
            this.chart.priceScale('right').applyOptions({
                scaleMargins: { top: 0.08, bottom: 0.08 },
            });
        }

        if (this.series.zone && bandUpper.length) this.series.zone.setData(bandUpper);
        if (this.series.zoneMask && bandLower.length) this.series.zoneMask.setData(bandLower);
        this.series.upper.setData(bandUpper);
        this.series.lower.setData(bandLower);
        this.series.line.setData(lineData);

        this.chart.priceScale('right').applyOptions({ autoScale: true });
        this.chart.applyOptions({
            width: el.clientWidth || 400,
            height: el.clientHeight || 120,
            layout: { background: { color: colors.bg }, textColor: colors.text },
        });
        if (this.series.zoneMask) {
            this.series.zoneMask.applyOptions({
                topColor: colors.mask,
                bottomColor: colors.mask,
            });
        }
        if (this.series.zone) {
            this.series.zone.applyOptions({
                topColor: colors.fillTop,
                bottomColor: colors.fillBottom,
            });
        }

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
        if (bars.length && this.series.zone && opts) {
            const upper = Number(opts.upper);
            const lower = Number(opts.lower);
            const t = bars[bars.length - 1].time;
            if (Number.isFinite(upper)) this.series.zone.update({ time: t, value: Math.max(upper, lower) });
            if (this.series.zoneMask && Number.isFinite(lower)) {
                this.series.zoneMask.update({ time: t, value: Math.min(upper, lower) });
            }
        }
    };

    global.OscillatorPaneIndicator = OscillatorPaneIndicator;
})(typeof window !== 'undefined' ? window : globalThis);
