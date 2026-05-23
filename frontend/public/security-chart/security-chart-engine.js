/**
 * Universal Security Chart — TradingView Lightweight Charts + Upstox REST/WS.
 * Lazy-loaded; no chart instances until openSecurityChart() is called.
 */
(function (global) {
    'use strict';

    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;

    const TF_OPTIONS = ['5m', '15m', '30m', '1hr', '1d'];
    const LWC_URL =
        'https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js';
    const CSS_HREF = 'security-chart/security-chart-modal.css?v=3';
    const INTEL_JS = 'security-chart/trade-intelligence-panel.js?v=2';
    const INTEL_PANEL_MAX_PX = 380;
    const INTEL_PANEL_MIN_PX = 200;
    const INTEL_PANEL_DEFAULT_PX = 320;
    const LIVE_POLL_MS = 1000;

    const CANDLE_UP = '#38bdf8';
    const CANDLE_DOWN = '#ef4444';
    const EMA5_COLOR = '#eab308';
    const VWAP_COLOR = '#ffffff';
    const EMA_PERIOD = 5;
    const INITIAL_VISIBLE_BARS = 100;

    function sessionDayKey(unixSec) {
        return new Date(unixSec * 1000).toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
    }

    function computeEma5(bars) {
        if (!bars || bars.length < EMA_PERIOD) return [];
        const k = 2 / (EMA_PERIOD + 1);
        let sum = 0;
        for (let i = 0; i < EMA_PERIOD; i++) sum += bars[i].close;
        let ema = sum / EMA_PERIOD;
        const out = [{ time: bars[EMA_PERIOD - 1].time, value: ema }];
        for (let i = EMA_PERIOD; i < bars.length; i++) {
            ema = (bars[i].close - ema) * k + ema;
            out.push({ time: bars[i].time, value: ema });
        }
        return out;
    }

    function computeSessionVwap(bars) {
        if (!bars || !bars.length) return [];
        const out = [];
        let day = null;
        let cumTpV = 0;
        let cumV = 0;
        bars.forEach(function (b) {
            const d = sessionDayKey(b.time);
            if (d !== day) {
                day = d;
                cumTpV = 0;
                cumV = 0;
            }
            const tp = (b.high + b.low + b.close) / 3;
            const vol = b.volume || 0;
            cumTpV += tp * vol;
            cumV += vol;
            out.push({ time: b.time, value: cumV > 0 ? cumTpV / cumV : tp });
        });
        return out;
    }

    /** Show only the trailing N bars in the viewport (user can scroll/zoom to see more). */
    function setInitialVisibleBars(chart, barCount) {
        if (!chart || barCount < 1) return;
        const visible = Math.min(INITIAL_VISIBLE_BARS, barCount);
        if (visible >= barCount) {
            chart.timeScale().fitContent();
            return;
        }
        chart.timeScale().setVisibleLogicalRange({
            from: barCount - visible,
            to: barCount - 1,
        });
    }

    let lwcPromise = null;
    let intelPromise = null;
    let cssLoaded = false;
    let modalRoot = null;

    function authHeaders() {
        const t = global.localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            Accept: 'application/json',
            'Content-Type': 'application/json',
        };
    }

    function loadCss() {
        if (cssLoaded) return Promise.resolve();
        return new Promise(function (resolve, reject) {
            const link = document.createElement('link');
            link.rel = 'stylesheet';
            link.href = CSS_HREF;
            link.onload = function () {
                cssLoaded = true;
                resolve();
            };
            link.onerror = reject;
            document.head.appendChild(link);
        });
    }

    function loadLwc() {
        if (global.LightweightCharts) return Promise.resolve(global.LightweightCharts);
        if (lwcPromise) return lwcPromise;
        lwcPromise = new Promise(function (resolve, reject) {
            const s = document.createElement('script');
            s.src = LWC_URL;
            s.async = true;
            s.onload = function () {
                resolve(global.LightweightCharts);
            };
            s.onerror = function () {
                reject(new Error('Failed to load Lightweight Charts'));
            };
            document.head.appendChild(s);
        });
        return lwcPromise;
    }

    function loadIntelPanel() {
        if (global.TradeIntelligencePanel) return Promise.resolve(global.TradeIntelligencePanel);
        if (intelPromise) return intelPromise;
        intelPromise = new Promise(function (resolve, reject) {
            const s = document.createElement('script');
            s.src = INTEL_JS;
            s.async = true;
            s.onload = function () {
                resolve(global.TradeIntelligencePanel);
            };
            s.onerror = function () {
                reject(new Error('Failed to load Trade Intelligence Panel'));
            };
            document.head.appendChild(s);
        });
        return intelPromise;
    }

    function ensureAssets() {
        return Promise.all([loadCss(), loadLwc(), loadIntelPanel()]);
    }

    function dirBadgeHtml(direction) {
        const d = String(direction || '').toUpperCase();
        if (d.indexOf('S') === 0) {
            return '<span class="uscm-dir uscm-dir--short">SHORT</span>';
        }
        if (d.indexOf('L') === 0) {
            return '<span class="uscm-dir uscm-dir--long">LONG</span>';
        }
        return '';
    }

    function headerPnlHtml(screenerData) {
        const sd = screenerData || {};
        const pct = sd.livePnlPct != null ? sd.livePnlPct : sd.pnlPct;
        if (pct == null || pct === '') return '';
        const n = parseFloat(pct);
        if (!Number.isFinite(n)) return '';
        const cls = n >= 0 ? 'uscm-header-pnl--pos' : 'uscm-header-pnl--neg';
        const txt = (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
        return '<span class="uscm-header-pnl ' + cls + '">' + txt + '</span>';
    }

    /** Centralized live quote polling (one timer per subscribed symbol). */
    const ChartWebSocketManager = (function () {
        const subs = new Map();
        let timer = null;

        function apiPost(path, params) {
            const q = new URLSearchParams(params).toString();
            return fetch(API_BASE + path + '?' + q, {
                method: 'POST',
                headers: authHeaders(),
                credentials: 'same-origin',
            }).then(function (r) {
                return r.json();
            });
        }

        function apiGet(path, params) {
            const q = new URLSearchParams(params).toString();
            return fetch(API_BASE + path + '?' + q, {
                headers: authHeaders(),
                credentials: 'same-origin',
                cache: 'no-store',
            }).then(function (r) {
                return r.json();
            });
        }

        function tick() {
            subs.forEach(function (entry, ik) {
                if (!entry.listeners.size) return;
                apiGet('/api/chart/live', { instrument_key: ik })
                    .then(function (data) {
                        if (!data || !data.success) return;
                        entry.listeners.forEach(function (fn) {
                            try {
                                fn(data);
                            } catch (e) {
                                /* ignore */
                            }
                        });
                    })
                    .catch(function () {});
            });
        }

        function ensureTimer() {
            if (timer) return;
            timer = global.setInterval(tick, LIVE_POLL_MS);
        }

        function clearTimerIfEmpty() {
            if (subs.size) return;
            if (timer) {
                global.clearInterval(timer);
                timer = null;
            }
        }

        return {
            subscribe: function (instrumentKey, listener) {
                const ik = (instrumentKey || '').trim();
                if (!ik) return function () {};
                let entry = subs.get(ik);
                if (!entry) {
                    entry = { listeners: new Set(), refcount: 0 };
                    subs.set(ik, entry);
                    apiPost('/api/chart/subscribe', { instrument_key: ik }).catch(function () {});
                }
                entry.refcount += 1;
                entry.listeners.add(listener);
                ensureTimer();
                return function unsubscribe() {
                    const e = subs.get(ik);
                    if (!e) return;
                    e.listeners.delete(listener);
                    e.refcount = Math.max(0, e.refcount - 1);
                    if (e.refcount <= 0 && !e.listeners.size) {
                        subs.delete(ik);
                        apiPost('/api/chart/unsubscribe', { instrument_key: ik }).catch(function () {});
                    }
                    clearTimerIfEmpty();
                };
            },
        };
    })();

    function fetchCandles(config) {
        const params = {
            symbol: config.symbol,
            instrument_type: config.instrumentType || 'FUT',
            timeframe: config.timeframe || '5m',
        };
        if (config.instrumentKey) params.instrument_key = config.instrumentKey;
        if (config.exchange) params.exchange = config.exchange;
        const q = new URLSearchParams(params).toString();
        return fetch(API_BASE + '/api/chart/candles?' + q, {
            headers: authHeaders(),
            credentials: 'same-origin',
            cache: 'no-store',
        }).then(function (r) {
            return r.json();
        });
    }

    function SecurityChartModal() {
        this.config = null;
        this.timeframe = '5m';
        this.instrumentKey = null;
        this.displaySymbol = '';
        this.chart = null;
        this.candleSeries = null;
        this.volumeSeries = null;
        this.emaSeries = null;
        this.vwapSeries = null;
        this._barsCache = [];
        this.resizeObs = null;
        this.unsubLive = null;
        this.abortLoad = null;
        this._open = false;
        this._lastBarTime = null;
        this._lastOhlc = null;
        this._screenerData = null;
        this._direction = '';
    }

    SecurityChartModal.prototype._renderHeader = function () {
        const root = modalRoot;
        if (!root || !this.config) return;
        root.querySelector('[data-uscm-symbol]').textContent = this.displaySymbol;
        root.querySelector('[data-uscm-meta]').textContent =
            this.config.exchange +
            ' · ' +
            this.config.instrumentType +
            (this.instrumentKey ? ' · ' + this.instrumentKey : '');
        const dirHost = root.querySelector('[data-uscm-dir]');
        if (dirHost) dirHost.innerHTML = dirBadgeHtml(this._direction);
        const pnlHost = root.querySelector('[data-uscm-header-pnl]');
        if (pnlHost) pnlHost.innerHTML = headerPnlHtml(this._screenerData);
        const qual =
            this.config.metadata && this.config.metadata.qualification
                ? 'Qual: ' + this.config.metadata.qualification
                : '';
        root.querySelector('[data-uscm-footer-meta]').textContent = qual;
    };

    SecurityChartModal.prototype._renderIntelligence = function () {
        const root = modalRoot;
        if (!root) return;
        const host = root.querySelector('[data-uscm-intel]');
        const panel = global.TradeIntelligencePanel;
        if (!host || !panel) return;
        const data = this._screenerData;
        const hasData =
            data &&
            typeof data === 'object' &&
            (Object.keys(data).length > 0 || Array.isArray(data.sections));
        if (!hasData) {
            panel.clear(host);
            root.classList.remove('uscm-panel--with-intel');
            root.classList.add('uscm-intel-empty');
            return;
        }
        root.classList.add('uscm-panel--with-intel');
        root.classList.remove('uscm-intel-empty');
        panel.render(host, {
            screenerData: data,
            insight: data.insight || data.insightBanner || '',
        });
        this._resetIntelPanelWidth();
    };

    SecurityChartModal.prototype._resetIntelPanelWidth = function () {
        const root = modalRoot;
        if (!root) return;
        const split = root.querySelector('[data-uscm-split]');
        if (!split) return;
        const w = Math.min(INTEL_PANEL_MAX_PX, INTEL_PANEL_DEFAULT_PX);
        split.style.setProperty('--uscm-intel-w', w + 'px');
    };

    SecurityChartModal.prototype._notifyChartResize = function () {
        const root = modalRoot;
        if (!this.chart || !root) return;
        const chartEl = root.querySelector('[data-uscm-chart]');
        if (!chartEl || !chartEl.clientWidth) return;
        try {
            this.chart.applyOptions({
                width: chartEl.clientWidth,
                height: chartEl.clientHeight,
            });
        } catch (e) {
            /* ignore */
        }
    };

    SecurityChartModal.prototype._ensureDom = function () {
        if (modalRoot) return modalRoot;
        const backdrop = document.createElement('div');
        backdrop.className = 'uscm-backdrop uscm-hidden';
        backdrop.setAttribute('role', 'dialog');
        backdrop.setAttribute('aria-modal', 'true');
        backdrop.innerHTML =
            '<div class="uscm-panel">' +
            '<header class="uscm-header">' +
            '<div class="uscm-title-block">' +
            '<div class="uscm-title-row">' +
            '<div class="uscm-symbol" data-uscm-symbol>—</div>' +
            '<span data-uscm-dir></span>' +
            '</div>' +
            '<div class="uscm-meta" data-uscm-meta>—</div>' +
            '</div>' +
            '<div class="uscm-ltp-block">' +
            '<div class="uscm-ltp-row">' +
            '<div class="uscm-ltp" data-uscm-ltp>—</div>' +
            '<span data-uscm-header-pnl></span>' +
            '</div>' +
            '<div class="uscm-chg" data-uscm-chg></div>' +
            '</div>' +
            '<div class="uscm-tf-group" data-uscm-tf></div>' +
            '<button type="button" class="uscm-close" data-uscm-close aria-label="Close">&times;</button>' +
            '</header>' +
            '<div class="uscm-body uscm-body--split" data-uscm-split>' +
            '<div class="uscm-chart-wrap">' +
            '<div class="uscm-skeleton" data-uscm-skeleton>Loading chart…</div>' +
            '<div class="uscm-chart-root" data-uscm-chart></div>' +
            '</div>' +
            '<div class="uscm-splitter" data-uscm-splitter role="separator" aria-orientation="vertical" aria-label="Resize statistics panel" tabindex="0"></div>' +
            '<aside class="uscm-intel-wrap tip-panel--empty" data-uscm-intel aria-label="Trade intelligence"></aside>' +
            '</div>' +
            '<footer class="uscm-footer">' +
            '<span class="uscm-footer-slot" data-uscm-footer-meta></span>' +
            '</footer>' +
            '</div>';
        document.body.appendChild(backdrop);
        const self = this;
        backdrop.querySelector('[data-uscm-close]').addEventListener('click', function () {
            self.close();
        });
        backdrop.addEventListener('click', function (e) {
            if (e.target === backdrop) self.close();
        });
        global.addEventListener('keydown', function (e) {
            if (e.key === 'Escape' && self._open) self.close();
        });
        const tfHost = backdrop.querySelector('[data-uscm-tf]');
        TF_OPTIONS.forEach(function (tf) {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'uscm-tf-btn' + (tf === '5m' ? ' active' : '');
            btn.textContent = tf;
            btn.dataset.tf = tf;
            btn.addEventListener('click', function () {
                self.setTimeframe(tf);
            });
            tfHost.appendChild(btn);
        });
        bindIntelSplitter(backdrop, self);
        modalRoot = backdrop;
        return backdrop;
    };

    function bindIntelSplitter(root, modalInstance) {
        const splitter = root.querySelector('[data-uscm-splitter]');
        const splitEl = root.querySelector('[data-uscm-split]');
        const panelEl = root.querySelector('.uscm-panel');
        if (!splitter || !splitEl || !panelEl || splitter._uscmBound) return;
        splitter._uscmBound = true;

        let dragging = false;

        function clampIntelWidth(px) {
            return Math.max(INTEL_PANEL_MIN_PX, Math.min(INTEL_PANEL_MAX_PX, px));
        }

        function setIntelWidth(px) {
            splitEl.style.setProperty('--uscm-intel-w', clampIntelWidth(px) + 'px');
            modalInstance._notifyChartResize();
        }

        splitter.addEventListener('pointerdown', function (e) {
            if (!root.classList.contains('uscm-panel--with-intel')) return;
            dragging = true;
            splitter.classList.add('uscm-splitter--active');
            try {
                splitter.setPointerCapture(e.pointerId);
            } catch (err) {
                /* ignore */
            }
            document.body.classList.add('uscm-col-resize');
            e.preventDefault();
        });

        splitter.addEventListener('pointermove', function (e) {
            if (!dragging) return;
            const pr = panelEl.getBoundingClientRect();
            setIntelWidth(pr.right - e.clientX);
        });

        function endDrag(e) {
            if (!dragging) return;
            dragging = false;
            splitter.classList.remove('uscm-splitter--active');
            document.body.classList.remove('uscm-col-resize');
            try {
                splitter.releasePointerCapture(e.pointerId);
            } catch (err) {
                /* ignore */
            }
            modalInstance._notifyChartResize();
        }

        splitter.addEventListener('pointerup', endDrag);
        splitter.addEventListener('pointercancel', endDrag);

        splitter.addEventListener('keydown', function (e) {
            if (!root.classList.contains('uscm-panel--with-intel')) return;
            const cur = parseInt(
                getComputedStyle(splitEl).getPropertyValue('--uscm-intel-w'),
                10
            );
            const base = Number.isFinite(cur) ? cur : INTEL_PANEL_DEFAULT_PX;
            if (e.key === 'ArrowLeft') {
                setIntelWidth(base + 12);
                e.preventDefault();
            } else if (e.key === 'ArrowRight') {
                setIntelWidth(base - 12);
                e.preventDefault();
            }
        });
    }

    SecurityChartModal.prototype._destroyChart = function () {
        if (this.resizeObs) {
            this.resizeObs.disconnect();
            this.resizeObs = null;
        }
        if (this.chart) {
            try {
                this.chart.remove();
            } catch (e) {
                /* ignore */
            }
            this.chart = null;
        }
        this.candleSeries = null;
        this.volumeSeries = null;
        this.emaSeries = null;
        this.vwapSeries = null;
        this._barsCache = [];
        const el = modalRoot && modalRoot.querySelector('[data-uscm-chart]');
        if (el) el.innerHTML = '';
    };

    SecurityChartModal.prototype.close = function () {
        this._open = false;
        if (this.abortLoad) {
            this.abortLoad.aborted = true;
            this.abortLoad = null;
        }
        if (this.unsubLive) {
            this.unsubLive();
            this.unsubLive = null;
        }
        this._destroyChart();
        if (modalRoot) {
            modalRoot.classList.remove('uscm-open');
            modalRoot.classList.remove('uscm-panel--with-intel');
            modalRoot.classList.add('uscm-intel-empty');
            modalRoot.classList.add('uscm-hidden');
            const intel = modalRoot.querySelector('[data-uscm-intel]');
            if (intel && global.TradeIntelligencePanel) {
                global.TradeIntelligencePanel.clear(intel);
            }
        }
        this.instrumentKey = null;
        this._screenerData = null;
        this._direction = '';
    };

    SecurityChartModal.prototype.setTimeframe = function (tf) {
        if (!this.config || tf === this.timeframe) return;
        this.timeframe = tf;
        const root = this._ensureDom();
        root.querySelectorAll('.uscm-tf-btn').forEach(function (b) {
            b.classList.toggle('active', b.dataset.tf === tf);
        });
        this._loadHistorical();
    };

    SecurityChartModal.prototype._updateLtp = function (data) {
        const root = modalRoot;
        if (!root) return;
        const ltpEl = root.querySelector('[data-uscm-ltp]');
        const chgEl = root.querySelector('[data-uscm-chg]');
        if (data.ltp == null) {
            ltpEl.textContent = '—';
            chgEl.textContent = '';
            return;
        }
        ltpEl.textContent = Number(data.ltp).toLocaleString('en-IN', {
            maximumFractionDigits: 2,
        });
        const chg = data.change;
        const pct = data.change_pct;
        ltpEl.classList.remove('up', 'down');
        let chgText = '';
        if (chg != null && pct != null) {
            chgText =
                (chg >= 0 ? '+' : '') +
                Number(chg).toFixed(2) +
                ' (' +
                (pct >= 0 ? '+' : '') +
                Number(pct).toFixed(2) +
                '%)';
            ltpEl.classList.add(chg >= 0 ? 'up' : 'down');
        }
        chgEl.textContent = chgText;
    };

    SecurityChartModal.prototype._applyBars = function (bars) {
        const LWC = global.LightweightCharts;
        const root = this._ensureDom();
        const chartEl = root.querySelector('[data-uscm-chart]');
        const sk = root.querySelector('[data-uscm-skeleton]');
        if (!bars || !bars.length) {
            sk.textContent = 'No candle data';
            sk.style.display = 'flex';
            return;
        }
        sk.style.display = 'none';
        this._destroyChart();
        const isDark =
            !document.body.getAttribute('data-theme') ||
            document.body.getAttribute('data-theme') === 'dark';
        const grid = isDark ? '#334155' : '#e2e8f0';
        const text = isDark ? '#94a3b8' : '#64748b';
        this.chart = LWC.createChart(chartEl, {
            layout: {
                background: { color: isDark ? '#0f172a' : '#ffffff' },
                textColor: text,
            },
            grid: { vertLines: { color: grid }, horzLines: { color: grid } },
            crosshair: { mode: LWC.CrosshairMode.Normal },
            rightPriceScale: { borderColor: grid },
            timeScale: { borderColor: grid, timeVisible: true, secondsVisible: false },
        });
        this.candleSeries = this.chart.addCandlestickSeries({
            upColor: CANDLE_UP,
            downColor: CANDLE_DOWN,
            borderVisible: false,
            wickUpColor: CANDLE_UP,
            wickDownColor: CANDLE_DOWN,
        });
        this.volumeSeries = this.chart.addHistogramSeries({
            color: '#64748b',
            priceFormat: { type: 'volume' },
            priceScaleId: '',
        });
        this.emaSeries = this.chart.addLineSeries({
            color: EMA5_COLOR,
            lineWidth: 2,
            title: 'EMA(5)',
            priceLineVisible: false,
            lastValueVisible: true,
        });
        this.vwapSeries = this.chart.addLineSeries({
            color: VWAP_COLOR,
            lineWidth: 2,
            title: 'VWAP',
            priceLineVisible: false,
            lastValueVisible: true,
        });
        this.chart.priceScale('').applyOptions({
            scaleMargins: { top: 0.82, bottom: 0 },
        });
        this._barsCache = bars.map(function (b) {
            return {
                time: b.time,
                open: b.open,
                high: b.high,
                low: b.low,
                close: b.close,
                volume: b.volume || 0,
            };
        });
        const candles = this._barsCache.map(function (b) {
            return { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close };
        });
        const vols = this._barsCache.map(function (b) {
            const up = b.close >= b.open;
            return {
                time: b.time,
                value: b.volume,
                color: up ? 'rgba(56,189,248,0.45)' : 'rgba(239,68,68,0.45)',
            };
        });
        this.candleSeries.setData(candles);
        this.volumeSeries.setData(vols);
        const emaData = computeEma5(this._barsCache);
        const vwapData = computeSessionVwap(this._barsCache);
        if (emaData.length) this.emaSeries.setData(emaData);
        if (vwapData.length) this.vwapSeries.setData(vwapData);
        const last = candles[candles.length - 1];
        this._lastBarTime = last ? last.time : null;
        this._lastOhlc = last
            ? { open: last.open, high: last.high, low: last.low, close: last.close }
            : null;
        setInitialVisibleBars(this.chart, candles.length);
        const self = this;
        if (typeof ResizeObserver !== 'undefined') {
            this.resizeObs = new ResizeObserver(function () {
                if (!self.chart || !chartEl.clientWidth) return;
                self.chart.applyOptions({
                    width: chartEl.clientWidth,
                    height: chartEl.clientHeight,
                });
            });
            this.resizeObs.observe(chartEl);
        }
        self.chart.applyOptions({
            width: chartEl.clientWidth,
            height: chartEl.clientHeight,
        });
    };

    SecurityChartModal.prototype._loadHistorical = function () {
        const self = this;
        const token = { aborted: false };
        this.abortLoad = token;
        const root = this._ensureDom();
        root.querySelector('[data-uscm-skeleton]').style.display = 'flex';
        root.querySelector('[data-uscm-skeleton]').textContent = 'Loading ' + self.timeframe + '…';
        fetchCandles({
            symbol: self.config.symbol,
            instrumentType: self.config.instrumentType,
            instrumentKey: self.instrumentKey,
            exchange: self.config.exchange,
            timeframe: self.timeframe,
        })
            .then(function (res) {
                if (token.aborted || !res.success) {
                    if (!token.aborted && res.error) {
                        root.querySelector('[data-uscm-skeleton]').textContent = res.error;
                    }
                    return;
                }
                self.instrumentKey = res.instrument_key || self.instrumentKey;
                self._applyBars(res.bars || []);
                if (self.unsubLive) {
                    self.unsubLive();
                    self.unsubLive = null;
                }
                self.unsubLive = ChartWebSocketManager.subscribe(self.instrumentKey, function (q) {
                    self._updateLtp(q);
                    self._tickBar(q);
                });
            })
            .catch(function (err) {
                if (!token.aborted) {
                    root.querySelector('[data-uscm-skeleton]').textContent =
                        err.message || 'Chart load failed';
                }
            });
    };

    SecurityChartModal.prototype._refreshOverlays = function () {
        if (!this._barsCache.length) return;
        const emaData = computeEma5(this._barsCache);
        const vwapData = computeSessionVwap(this._barsCache);
        if (this.emaSeries && emaData.length) {
            const lastEma = emaData[emaData.length - 1];
            this.emaSeries.update(lastEma);
        }
        if (this.vwapSeries && vwapData.length) {
            const lastVwap = vwapData[vwapData.length - 1];
            this.vwapSeries.update(lastVwap);
        }
    };

    SecurityChartModal.prototype._tickBar = function (quote) {
        if (!this.candleSeries || quote.ltp == null || this._lastBarTime == null || !this._lastOhlc)
            return;
        const ltp = Number(quote.ltp);
        const o = this._lastOhlc;
        o.close = ltp;
        o.high = Math.max(o.high, ltp);
        o.low = Math.min(o.low, ltp);
        const lastBar = this._barsCache[this._barsCache.length - 1];
        if (lastBar && lastBar.time === this._lastBarTime) {
            lastBar.close = o.close;
            lastBar.high = o.high;
            lastBar.low = o.low;
        }
        try {
            this.candleSeries.update({
                time: this._lastBarTime,
                open: o.open,
                high: o.high,
                low: o.low,
                close: o.close,
            });
            this._refreshOverlays();
        } catch (e) {
            /* ignore occasional time mismatch */
        }
    };

    SecurityChartModal.prototype.open = function (config) {
        const self = this;
        config = config || {};
        this.config = {
            symbol: (config.symbol || '').trim(),
            instrumentType: (config.instrumentType || 'FUT').toUpperCase(),
            exchange: config.exchange || 'NSE',
            instrumentKey: config.instrumentKey || config.instrument_key || '',
            displaySymbol: config.displaySymbol || config.symbol,
            metadata: config.metadata || {},
        };
        this.timeframe = config.timeframe || '5m';
        this.instrumentKey = this.config.instrumentKey;
        this.displaySymbol = this.config.displaySymbol || this.config.symbol;
        this._screenerData = config.screenerData || config.screener || null;
        this._direction =
            config.direction ||
            (this._screenerData && this._screenerData.direction) ||
            '';

        return ensureAssets().then(function () {
            const root = self._ensureDom();
            self._renderHeader();
            self._renderIntelligence();
            root.querySelectorAll('.uscm-tf-btn').forEach(function (b) {
                b.classList.toggle('active', b.dataset.tf === self.timeframe);
            });
            root.classList.remove('uscm-hidden');
            requestAnimationFrame(function () {
                root.classList.add('uscm-open');
            });
            self._open = true;
            self._loadHistorical();
        });
    };

    const singletonModal = new SecurityChartModal();

    function openSecurityChart(config) {
        return singletonModal.open(config || {});
    }

    function closeSecurityChart() {
        singletonModal.close();
    }

    global.SecurityChartEngine = {
        openSecurityChart: openSecurityChart,
        closeSecurityChart: closeSecurityChart,
        ChartWebSocketManager: ChartWebSocketManager,
        ensureAssets: ensureAssets,
    };

    /** Alias for scanner modules expecting openChartModal(config). */
    global.openChartModal = openSecurityChart;
})(typeof window !== 'undefined' ? window : globalThis);
