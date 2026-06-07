/**
 * Volume Mismatch Futures — grid, filters, entry alerts, chart popup.
 */
(function (global) {
    'use strict';

    const POLL_MS = 60000;
    const state = {
        rows: [],
        filter: 'ALL',
        search: '',
        prevReadyKeys: {},
    };

    function apiBase() {
        if (window.getTrademanthanApiBase) return window.getTrademanthanApiBase();
        if (location.hostname === 'localhost' || location.hostname === '127.0.0.1') {
            return 'http://localhost:8000';
        }
        return location.origin;
    }

    function token() {
        return localStorage.getItem('trademanthan_token') || '';
    }

    async function apiGet(path) {
        const res = await fetch(apiBase() + path, {
            headers: { Authorization: 'Bearer ' + token() },
        });
        const data = await res.json().catch(function () { return {}; });
        if (!res.ok) throw new Error(data.detail || data.error || res.statusText);
        return data;
    }

    async function apiPost(path, body) {
        const res = await fetch(apiBase() + path, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: 'Bearer ' + token(),
            },
            body: JSON.stringify(body || {}),
        });
        const data = await res.json().catch(function () { return {}; });
        if (!res.ok) throw new Error(data.detail || data.error || res.statusText);
        return data;
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function fmtNum(v, dec) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(dec == null ? 2 : dec);
    }

    function fmtPct(v) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
    }

    function fmtTs(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (!Number.isFinite(d.getTime())) return String(iso);
            return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        } catch (e) {
            return String(iso);
        }
    }

    function statusClass(st) {
        const u = String(st || '').toUpperCase();
        if (u === 'READY') return 'vmf-status-ready';
        if (u === 'TRIGGERED') return 'vmf-status-triggered';
        if (u === 'EXPIRED') return 'vmf-status-expired';
        return 'vmf-status-waiting';
    }

    function fireDesktopNotification(title, body) {
        try {
            if (!('Notification' in window)) return;
            if (Notification.permission === 'granted') {
                new Notification(title, { body: body || '' });
            } else if (Notification.permission !== 'denied') {
                Notification.requestPermission().then(function (perm) {
                    if (perm === 'granted') new Notification(title, { body: body || '' });
                });
            }
        } catch (e) { /* ignore */ }
    }

    function notifyReady(row) {
        const sym = String(row.symbol || row.future_symbol || '—');
        const dir = String(row.direction || '').toUpperCase();
        const title = 'Volume Mismatch Futures\n' + sym + '\n' + dir + ' READY';
        const body =
            'Entry: ' + fmtNum(row.preferred_entry || row.entry_price) +
            '\nSL: ' + fmtNum(row.stop_loss) +
            '\nT1: ' + fmtNum(row.target1) +
            '\nT2: ' + fmtNum(row.target2);
        fireDesktopNotification(title.replace(/\n/g, ' — '), body);
        if (typeof window.notifyTelegramUserMessage === 'function') {
            const tmsg =
                '📊 Volume Mismatch Futures\n' + sym + '\n' + dir + ' READY\n' +
                'Entry: ' + fmtNum(row.preferred_entry || row.entry_price) +
                '\nSL: ' + fmtNum(row.stop_loss) +
                '\nT1: ' + fmtNum(row.target1) +
                '\nT2: ' + fmtNum(row.target2);
            window.notifyTelegramUserMessage(tmsg).catch(function () {});
        }
    }

    function detectReadyTransitions(rows) {
        rows.forEach(function (r) {
            const id = String(r.id);
            const st = String(r.entry_status || '').toUpperCase();
            const key = id + '|' + st;
            if (st === 'READY' && !state.prevReadyKeys[key]) {
                notifyReady(r);
            }
            if (st === 'READY' || st === 'TRIGGERED') {
                state.prevReadyKeys[key] = true;
            }
        });
    }

    function applyFilters(rows) {
        let out = rows.slice();
        const f = state.filter;
        if (f === 'LONG') out = out.filter(function (r) { return String(r.direction).toUpperCase() === 'LONG'; });
        else if (f === 'SHORT') out = out.filter(function (r) { return String(r.direction).toUpperCase() === 'SHORT'; });
        else if (f === 'READY') out = out.filter(function (r) { return String(r.entry_status).toUpperCase() === 'READY'; });
        else if (f === 'TRIGGERED') out = out.filter(function (r) { return String(r.entry_status).toUpperCase() === 'TRIGGERED'; });
        else if (f === 'SCORE60') out = out.filter(function (r) { return Number(r.score) > 60; });
        else if (f === 'SCORE80') out = out.filter(function (r) { return Number(r.score) > 80; });

        const q = state.search.trim().toUpperCase();
        if (q) {
            out = out.filter(function (r) {
                const sym = String(r.symbol || '').toUpperCase();
                const fs = String(r.future_symbol || '').toUpperCase();
                return sym.indexOf(q) >= 0 || fs.indexOf(q) >= 0;
            });
        }
        out.sort(function (a, b) {
            const sa = Number(a.score) || 0;
            const sb = Number(b.score) || 0;
            if (sb !== sa) return sb - sa;
            const ga = Math.abs(Number(a.gap_percent) || 0);
            const gb = Math.abs(Number(b.gap_percent) || 0);
            if (gb !== ga) return gb - ga;
            return (Number(b.first_15m_volume) || 0) - (Number(a.first_15m_volume) || 0);
        });
        return out;
    }

    let chartEngineLoadPromise = null;
    const chartPayloadRegistry = {};
    let chartPayloadSeq = 0;

    function ensureChartEngine() {
        if (global.SecurityChartEngine) return Promise.resolve(global.SecurityChartEngine);
        if (chartEngineLoadPromise) return chartEngineLoadPromise;
        chartEngineLoadPromise = new Promise(function (resolve, reject) {
            const s = document.createElement('script');
            s.src = 'security-chart/security-chart-engine.js?v=10';
            s.async = true;
            s.onload = function () { resolve(global.SecurityChartEngine); };
            s.onerror = function () { reject(new Error('Chart module failed to load')); };
            document.head.appendChild(s);
        });
        return chartEngineLoadPromise;
    }

    function registerChartPayload(payload) {
        const id = 'vmf' + ++chartPayloadSeq;
        chartPayloadRegistry[id] = payload || {};
        return id;
    }

    function buildScreenerFromRow(r) {
        return {
            direction: r.direction,
            gapPercent: r.gap_percent,
            netVolume: r.net_volume,
            relativeVolume: r.relative_volume,
            score: r.score,
            entryPrice: r.preferred_entry || r.entry_price,
            stopLoss: r.stop_loss,
            target1: r.target1,
            target2: r.target2,
            entryStatus: r.entry_status,
            lifecycle: r.entry_status,
        };
    }

    function openChart(row) {
        const stock = String(row.symbol || '').trim();
        const ik = String(row.instrument_key || row.instrument_token || '').trim();
        const label = String(row.future_symbol || stock);
        const screenerData = buildScreenerFromRow(row);
        ensureChartEngine()
            .then(function (eng) {
                return eng.openSecurityChart({
                    symbol: stock,
                    instrumentType: 'FUT',
                    instrumentKey: ik,
                    displaySymbol: label,
                    exchange: 'NSE',
                    timeframe: '15m',
                    direction: String(row.direction || ''),
                    screenerData: screenerData,
                    metadata: { algo: 'volume_mismatch' },
                });
            })
            .catch(function (err) {
                if (global.console && global.console.warn) global.console.warn('Chart:', err);
            });
    }

    function renderTable(rows) {
        const tbody = document.getElementById('vmfTbody');
        if (!tbody) return;
        const filtered = applyFilters(rows);
        if (!filtered.length) {
            tbody.innerHTML = '<tr><td colspan="18" class="vmf-empty">No signals match filters.</td></tr>';
            return;
        }
        tbody.innerHTML = filtered.map(function (r) {
            const dir = String(r.direction || '').toUpperCase();
            const dirCls = dir === 'SHORT' ? 'vmf-dir-short' : 'vmf-dir-long';
            const st = String(r.entry_status || 'WAITING').toUpperCase();
            const canEnter = st === 'READY';
            const sym = escapeHtml(r.symbol || '—');
            const pid = registerChartPayload({ screenerData: buildScreenerFromRow(r) });
            return (
                '<tr data-id="' + escapeHtml(r.id) + '">' +
                '<td><button type="button" class="vmf-security-link" data-chart-pid="' + escapeHtml(pid) + '" data-sym="' + sym + '">' + sym + '</button></td>' +
                '<td class="' + dirCls + '">' + escapeHtml(dir) + '</td>' +
                '<td class="num">' + escapeHtml(fmtPct(r.gap_percent)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.first_15m_volume, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.relative_volume, 2)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.net_volume, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.score, 1)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.bb_lower)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.bb_upper)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.preferred_entry || r.entry_price)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.stop_loss)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.target1)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.target2)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.current_price)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.vwap)) + '</td>' +
                '<td class="' + statusClass(st) + '">' + escapeHtml(st) + '</td>' +
                '<td>' + escapeHtml(fmtTs(r.updated_at)) + '</td>' +
                '<td><button type="button" class="vmf-enter-btn" data-enter-id="' + escapeHtml(r.id) + '" ' +
                (canEnter ? '' : 'disabled') + '>ENTER</button></td>' +
                '</tr>'
            );
        }).join('');
    }

    function updateMeta(data) {
        const el = document.getElementById('vmfMeta');
        if (!el) return;
        const parts = [];
        if (data.trade_date) parts.push('Session: ' + data.trade_date);
        if (data.signal_count != null) parts.push('Signals: ' + data.signal_count + ' (L:' + (data.long_count || 0) + ' S:' + (data.short_count || 0) + ')');
        if (data.universe_count != null) parts.push('Universe: ' + data.universe_count);
        if (data.last_updated) parts.push('Updated: ' + fmtTs(data.last_updated));
        el.textContent = parts.join(' · ') || '—';
    }

    async function loadSignals() {
        const data = await apiGet('/volume-mismatch-futures/signals');
        state.rows = data.rows || [];
        detectReadyTransitions(state.rows);
        updateMeta(data);
        renderTable(state.rows);
    }

    async function onEnter(signalId) {
        try {
            await apiPost('/volume-mismatch-futures/enter', { signal_id: Number(signalId) });
            await loadSignals();
        } catch (e) {
            alert(e.message || 'Enter failed');
        }
    }

    function bindEvents() {
        document.getElementById('vmfRefreshBtn')?.addEventListener('click', function () {
            loadSignals().catch(function (e) {
                document.getElementById('vmfBanner').textContent = 'Refresh failed: ' + (e.message || e);
            });
        });

        document.getElementById('vmfSearch')?.addEventListener('input', function (ev) {
            state.search = ev.target.value || '';
            renderTable(state.rows);
        });

        document.querySelectorAll('.vmf-filter-btn').forEach(function (btn) {
            btn.addEventListener('click', function () {
                document.querySelectorAll('.vmf-filter-btn').forEach(function (b) { b.classList.remove('active'); });
                btn.classList.add('active');
                state.filter = btn.getAttribute('data-filter') || 'ALL';
                renderTable(state.rows);
            });
        });

        const tbody = document.getElementById('vmfTbody');
        if (tbody) {
            tbody.addEventListener('click', function (ev) {
                const enterBtn = ev.target.closest('[data-enter-id]');
                if (enterBtn && !enterBtn.disabled) {
                    onEnter(enterBtn.getAttribute('data-enter-id'));
                    return;
                }
                const symBtn = ev.target.closest('.vmf-security-link');
                if (symBtn) {
                    const tr = symBtn.closest('tr');
                    const id = tr && tr.getAttribute('data-id');
                    const row = state.rows.find(function (r) { return String(r.id) === String(id); });
                    if (row) openChart(row);
                }
            });
        }
    }

    let pollTimer = null;

    function startPoll() {
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = setInterval(function () {
            loadSignals().catch(function () {});
        }, POLL_MS);
    }

    document.addEventListener('DOMContentLoaded', function () {
        bindEvents();
        loadSignals()
            .then(startPoll)
            .catch(function (e) {
                const tbody = document.getElementById('vmfTbody');
                if (tbody) tbody.innerHTML = '<tr><td colspan="18" class="vmf-empty">Failed to load: ' + escapeHtml(e.message || e) + '</td></tr>';
            });
    });
})(window);
