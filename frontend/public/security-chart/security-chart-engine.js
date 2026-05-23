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
    const CSS_HREF = 'security-chart/security-chart-modal.css?v=5';
    const INTEL_JS = 'security-chart/trade-intelligence-panel.js?v=2';
    const EMA_PERIOD_MIN = 2;
    const EMA_PERIOD_MAX = 200;
    const EMA_PERIOD_DEFAULT = 5;
    const INTEL_PANEL_MAX_PX = 380;
    const INTEL_PANEL_MIN_PX = 200;
    const INTEL_PANEL_DEFAULT_PX = 320;
    const LIVE_POLL_MS = 1000;

    const CANDLE_UP = '#38bdf8';
    const CANDLE_DOWN = '#ef4444';
    const EMA_COLOR = '#eab308';
    const VWAP_COLOR = '#ffffff';
    const INITIAL_VISIBLE_BARS = 100;
    const CHART_TZ = 'Asia/Kolkata';

    function istDateParts(utcSec) {
        const parts = new Intl.DateTimeFormat('en-US', {
            timeZone: CHART_TZ,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        }).formatToParts(new Date(utcSec * 1000));
        function part(type) {
            const p = parts.find(function (x) {
                return x.type === type;
            });
            return p ? parseInt(p.value, 10) : 0;
        }
        return {
            year: part('year'),
            month: part('month'),
            day: part('day'),
            hour: part('hour'),
            minute: part('minute'),
            second: part('second'),
        };
    }

    /** LWC reads UTC fields for labels — shift so axis shows IST wall clock (NSE). */
    function utcUnixToChartTime(utcSec, daily) {
        const p = istDateParts(utcSec);
        if (daily) {
            const mm = String(p.month).padStart(2, '0');
            const dd = String(p.day).padStart(2, '0');
            return String(p.year) + '-' + mm + '-' + dd;
        }
        return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) / 1000;
    }

    function sessionDayKeyUtc(utcSec) {
        return new Date(utcSec * 1000).toLocaleDateString('en-CA', { timeZone: CHART_TZ });
    }

    function isDailyTimeframe(tf) {
        return String(tf || '').toLowerCase() === '1d';
    }

    function normalizeBarFromApi(bar, timeframe) {
        const utcTime = bar.time;
        const daily = isDailyTimeframe(timeframe);
        return {
            utcTime: utcTime,
            time: utcUnixToChartTime(utcTime, daily),
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume || 0,
        };
    }

    /** Format chart time values (IST wall clock stored in UTC fields). */
    function formatChartDisplayTime(time, withSuffix) {
        const LWC = global.LightweightCharts;
        if (LWC && LWC.isBusinessDay && LWC.isBusinessDay(time)) {
            return (
                time.year +
                '-' +
                String(time.month).padStart(2, '0') +
                '-' +
                String(time.day).padStart(2, '0')
            );
        }
        const sec = typeof time === 'number' ? time : time && time.timestamp;
        if (!Number.isFinite(sec)) return '';
        const d = new Date(sec * 1000);
        const hh = String(d.getUTCHours()).padStart(2, '0');
        const mi = String(d.getUTCMinutes()).padStart(2, '0');
        return withSuffix ? hh + ':' + mi + ' IST' : hh + ':' + mi;
    }

    function formatChartTimeIst(time) {
        return formatChartDisplayTime(time, true);
    }

    function clampEmaPeriod(n) {
        const p = Math.floor(Number(n));
        if (!Number.isFinite(p)) return EMA_PERIOD_DEFAULT;
        return Math.max(EMA_PERIOD_MIN, Math.min(EMA_PERIOD_MAX, p));
    }

    function computeEma(bars, period) {
        const p = clampEmaPeriod(period);
        if (!bars || bars.length < p) return [];
        const k = 2 / (p + 1);
        let sum = 0;
        for (let i = 0; i < p; i++) sum += bars[i].close;
        let ema = sum / p;
        const out = [{ time: bars[p - 1].time, value: ema }];
        for (let i = p; i < bars.length; i++) {
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
            const d = sessionDayKeyUtc(b.utcTime != null ? b.utcTime : b.time);
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

    function extractLtp(data) {
        if (!data || typeof data !== 'object') return null;
        const raw =
            data.ltp != null
                ? data.ltp
                : data.last_price != null
                  ? data.last_price
                  : data.close;
        const n = Number(raw);
        return Number.isFinite(n) ? n : null;
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

        function notifyListeners(ik, data) {
            const entry = subs.get(ik);
            if (!entry || !data || !data.success) return;
            entry.listeners.forEach(function (fn) {
                try {
                    fn(data);
                } catch (e) {
                    /* ignore */
                }
            });
        }

        function fetchLiveNow(ik) {
            apiGet('/api/chart/live', { instrument_key: ik })
                .then(function (data) {
                    notifyListeners(ik, data);
                })
                .catch(function () {});
        }

        function tick() {
            subs.forEach(function (entry, ik) {
                if (!entry.listeners.size) return;
                fetchLiveNow(ik);
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
                fetchLiveNow(ik);
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
        this.emaEnabled = true;
        this.vwapEnabled = true;
        this.emaPeriod = EMA_PERIOD_DEFAULT;
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
            '<div class="uscm-header-prices">' +
            '<div class="uscm-indicator-wrap" data-uscm-indicator-wrap>' +
            '<button type="button" class="uscm-indicator-btn" data-uscm-indicator-toggle aria-haspopup="true" aria-expanded="false">Indicator</button>' +
            '<div class="uscm-indicator-menu uscm-hidden" data-uscm-indicator-menu role="menu">' +
            '<div class="uscm-indicator-menu-inner" data-uscm-overlays>' +
            '<label class="uscm-ov-label">' +
            '<input type="checkbox" data-uscm-ema-on checked> EMA' +
            '</label>' +
            '<input type="number" class="uscm-ema-period" data-uscm-ema-period min="' +
            EMA_PERIOD_MIN +
            '" max="' +
            EMA_PERIOD_MAX +
            '" value="' +
            EMA_PERIOD_DEFAULT +
            '" title="EMA period" aria-label="EMA period">' +
            '<label class="uscm-ov-label">' +
            '<input type="checkbox" data-uscm-vwap-on checked> VWAP' +
            '</label>' +
            '</div></div></div>' +
            '<div class="uscm-ltp-block">' +
            '<div class="uscm-ltp-row">' +
            '<div class="uscm-ltp" data-uscm-ltp>—</div>' +
            '<span data-uscm-header-pnl></span>' +
            '</div>' +
            '<div class="uscm-chg" data-uscm-chg></div>' +
            '</div></div>' +
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
        bindOverlayControls(backdrop, self);
        bindIndicatorDropdown(backdrop);
        modalRoot = backdrop;
        return backdrop;
    };

    function bindIndicatorDropdown(root) {
        const wrap = root.querySelector('[data-uscm-indicator-wrap]');
        const btn = root.querySelector('[data-uscm-indicator-toggle]');
        const menu = root.querySelector('[data-uscm-indicator-menu]');
        if (!wrap || !btn || !menu || wrap._uscmIndicatorBound) return;
        wrap._uscmIndicatorBound = true;

        function setOpen(open) {
            wrap.classList.toggle('uscm-indicator-wrap--open', open);
            menu.classList.toggle('uscm-hidden', !open);
            btn.setAttribute('aria-expanded', open ? 'true' : 'false');
        }

        btn.addEventListener('click', function (e) {
            e.stopPropagation();
            setOpen(menu.classList.contains('uscm-hidden'));
        });

        menu.addEventListener('click', function (e) {
            e.stopPropagation();
        });

        root.addEventListener('click', function () {
            setOpen(false);
        });

        global.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') setOpen(false);
        });
    }

    function bindOverlayControls(root, modalInstance) {
        const host = root.querySelector('[data-uscm-overlays]');
        if (!host || host._uscmOverlayBound) return;
        host._uscmOverlayBound = true;

        const emaCb = host.querySelector('[data-uscm-ema-on]');
        const vwapCb = host.querySelector('[data-uscm-vwap-on]');
        const emaIn = host.querySelector('[data-uscm-ema-period]');

        function onOverlayChange() {
            modalInstance._readOverlayPrefs();
            modalInstance._rebuildOverlays();
        }

        if (emaCb) {
            emaCb.addEventListener('change', onOverlayChange);
        }
        if (vwapCb) {
            vwapCb.addEventListener('change', onOverlayChange);
        }
        if (emaIn) {
            emaIn.addEventListener('change', onOverlayChange);
            emaIn.addEventListener('keydown', function (e) {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    emaIn.blur();
                    onOverlayChange();
                }
            });
        }
    }

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

    SecurityChartModal.prototype._readOverlayPrefs = function () {
        const root = modalRoot;
        if (!root) return;
        const emaCb = root.querySelector('[data-uscm-ema-on]');
        const vwapCb = root.querySelector('[data-uscm-vwap-on]');
        const emaIn = root.querySelector('[data-uscm-ema-period]');
        if (emaCb) this.emaEnabled = emaCb.checked;
        if (vwapCb) this.vwapEnabled = vwapCb.checked;
        if (emaIn) {
            this.emaPeriod = clampEmaPeriod(emaIn.value);
            emaIn.value = String(this.emaPeriod);
            emaIn.disabled = !this.emaEnabled;
        }
    };

    SecurityChartModal.prototype._syncOverlayUi = function () {
        const root = modalRoot;
        if (!root) return;
        const emaCb = root.querySelector('[data-uscm-ema-on]');
        const vwapCb = root.querySelector('[data-uscm-vwap-on]');
        const emaIn = root.querySelector('[data-uscm-ema-period]');
        if (emaCb) emaCb.checked = this.emaEnabled;
        if (vwapCb) vwapCb.checked = this.vwapEnabled;
        if (emaIn) {
            emaIn.value = String(this.emaPeriod);
            emaIn.disabled = !this.emaEnabled;
        }
    };

    SecurityChartModal.prototype._rebuildOverlays = function () {
        if (!this.chart || !this._barsCache.length) return;
        const emaData = computeEma(this._barsCache, this.emaPeriod);
        const vwapData = computeSessionVwap(this._barsCache);
        if (this.emaSeries) {
            this.emaSeries.applyOptions({
                visible: this.emaEnabled,
                title: 'EMA(' + this.emaPeriod + ')',
            });
            this.emaSeries.setData(this.emaEnabled && emaData.length ? emaData : []);
        }
        if (this.vwapSeries) {
            this.vwapSeries.applyOptions({ visible: this.vwapEnabled });
            this.vwapSeries.setData(this.vwapEnabled && vwapData.length ? vwapData : []);
        }
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

    SecurityChartModal.prototype._resetLtpDisplay = function () {
        const root = modalRoot;
        if (!root) return;
        const ltpEl = root.querySelector('[data-uscm-ltp]');
        const chgEl = root.querySelector('[data-uscm-chg]');
        if (ltpEl) {
            ltpEl.textContent = '—';
            ltpEl.classList.remove('up', 'down');
        }
        if (chgEl) chgEl.textContent = '';
    };

    SecurityChartModal.prototype._seedLtpFromBars = function () {
        if (!this._barsCache.length) return;
        const last = this._barsCache[this._barsCache.length - 1];
        const close = last && Number(last.close);
        if (!Number.isFinite(close)) return;
        this._updateLtp({ ltp: close, change: null, change_pct: null });
    };

    SecurityChartModal.prototype._updateLtp = function (data) {
        const root = modalRoot;
        if (!root) return;
        const ltpEl = root.querySelector('[data-uscm-ltp]');
        const chgEl = root.querySelector('[data-uscm-chg]');
        if (!ltpEl) return;
        const ltp = extractLtp(data);
        if (ltp == null) return;
        ltpEl.textContent = ltp.toLocaleString('en-IN', {
            maximumFractionDigits: 2,
        });
        const chg = data.change;
        const pct = data.change_pct;
        if (chg == null || pct == null) return;
        ltpEl.classList.remove('up', 'down');
        const chgText =
            (chg >= 0 ? '+' : '') +
            Number(chg).toFixed(2) +
            ' (' +
            (pct >= 0 ? '+' : '') +
            Number(pct).toFixed(2) +
            '%)';
        ltpEl.classList.add(chg >= 0 ? 'up' : 'down');
        if (chgEl) chgEl.textContent = chgText;
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
            localization: {
                locale: 'en-IN',
                timeFormatter: formatChartTimeIst,
            },
            timeScale: {
                borderColor: grid,
                timeVisible: true,
                secondsVisible: false,
                tickMarkFormatter: function (time) {
                    return formatChartDisplayTime(time, false);
                },
            },
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
        this._readOverlayPrefs();
        this.emaSeries = this.chart.addLineSeries({
            color: EMA_COLOR,
            lineWidth: 2,
            title: 'EMA(' + this.emaPeriod + ')',
            priceLineVisible: false,
            lastValueVisible: true,
            visible: this.emaEnabled,
        });
        this.vwapSeries = this.chart.addLineSeries({
            color: VWAP_COLOR,
            lineWidth: 2,
            title: 'VWAP',
            priceLineVisible: false,
            lastValueVisible: true,
            visible: this.vwapEnabled,
        });
        this.chart.priceScale('').applyOptions({
            scaleMargins: { top: 0.82, bottom: 0 },
        });
        const tf = this.timeframe;
        this._barsCache = bars.map(function (b) {
            return normalizeBarFromApi(b, tf);
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
        this._rebuildOverlays();
        const last = candles[candles.length - 1];
        this._lastBarTime = last ? last.time : null;
        this._lastOhlc = last
            ? { open: last.open, high: last.high, low: last.low, close: last.close }
            : null;
        setInitialVisibleBars(this.chart, candles.length);
        this._seedLtpFromBars();
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
        this._resetLtpDisplay();
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
        if (this.emaEnabled && this.emaSeries) {
            const emaData = computeEma(this._barsCache, this.emaPeriod);
            if (emaData.length) {
                this.emaSeries.update(emaData[emaData.length - 1]);
            }
        }
        if (this.vwapEnabled && this.vwapSeries) {
            const vwapData = computeSessionVwap(this._barsCache);
            if (vwapData.length) {
                this.vwapSeries.update(vwapData[vwapData.length - 1]);
            }
        }
    };

    SecurityChartModal.prototype._tickBar = function (quote) {
        const ltpVal = extractLtp(quote);
        if (!this.candleSeries || ltpVal == null || this._lastBarTime == null || !this._lastOhlc)
            return;
        const ltp = ltpVal;
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
            self._syncOverlayUi();
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
