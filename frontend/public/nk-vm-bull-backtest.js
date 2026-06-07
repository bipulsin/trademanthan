/**
 * Public NK VM Bull backtest viewer.
 * No login required — /nk-vm-bull-backtest.html
 */
(function () {
    'use strict';

    const API_PATHS = ['/nk-vm-bull-backtest/data', '/api/nk-vm-bull-backtest/data'];
    const COL_COUNT = 11;

    const state = {
        rows: [],
        meta: {},
        sort: { key: 'signal_time', dir: 'asc' },
    };

    function apiBase() {
        if (location.hostname === 'localhost' || location.hostname === '127.0.0.1') {
            return 'http://localhost:8000';
        }
        return location.origin;
    }

    async function loadData() {
        let lastErr = null;
        for (const path of API_PATHS) {
            try {
                const res = await fetch(apiBase() + path, { cache: 'no-store' });
                if (!res.ok) {
                    const j = await res.json().catch(function () { return {}; });
                    throw new Error(j.detail || res.statusText);
                }
                return await res.json();
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load backtest');
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function fmtNum(v, d) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d == null ? 2 : d);
    }

    function fmtPnl(v) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        const cls = n > 0 ? 'nvb-pos' : (n < 0 ? 'nvb-neg' : '');
        const txt = (n >= 0 ? '+' : '') + n.toFixed(2);
        return cls ? '<span class="' + cls + '">' + txt + '</span>' : txt;
    }

    function tradeDateFromSignal(signalTime) {
        if (!signalTime) return '';
        return String(signalTime).slice(0, 10);
    }

    function applyFilters(rows) {
        const from = document.getElementById('nvbFrom')?.value || '';
        const to = document.getElementById('nvbTo')?.value || '';
        const symQ = (document.getElementById('nvbSymbol')?.value || '').trim().toUpperCase();
        let out = rows.slice();
        if (from) {
            out = out.filter(function (r) {
                return tradeDateFromSignal(r.signal_time) >= from || String(r.trade_date) >= from;
            });
        }
        if (to) {
            out = out.filter(function (r) {
                return tradeDateFromSignal(r.signal_time) <= to || String(r.trade_date) <= to;
            });
        }
        if (symQ) {
            out = out.filter(function (r) {
                return String(r.symbol || '').toUpperCase().indexOf(symQ) >= 0;
            });
        }
        const sk = state.sort.key;
        const sd = state.sort.dir === 'asc' ? 1 : -1;
        out.sort(function (a, b) {
            const va = a[sk];
            const vb = b[sk];
            if (sk === 'signal_time') return sd * String(va).localeCompare(String(vb));
            const na = Number(va);
            const nb = Number(vb);
            if (Number.isFinite(na) && Number.isFinite(nb)) return sd * (na - nb);
            return sd * String(va || '').localeCompare(String(vb || ''));
        });
        return out;
    }

    function renderTable(rows) {
        const tbody = document.getElementById('nvbTbody');
        const countEl = document.getElementById('nvbCount');
        const filtered = applyFilters(rows);
        if (countEl) countEl.textContent = filtered.length + ' rows';
        if (!tbody) return;
        if (!filtered.length) {
            tbody.innerHTML = '<tr><td colspan="' + COL_COUNT + '" class="nvb-empty">No rows match filters.</td></tr>';
            return;
        }
        tbody.innerHTML = filtered.map(function (r) {
            const err = r.error ? ' title="' + escapeHtml(r.error) + '"' : '';
            return (
                '<tr' + err + '>' +
                '<td>' + escapeHtml(r.signal_time || '—') + '</td>' +
                '<td><strong>' + escapeHtml(r.symbol || '—') + '</strong></td>' +
                '<td>' + escapeHtml(r.future_symbol || '—') + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.lot_size, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.entry_price)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.exit_1230_price)) + '</td>' +
                '<td class="num">' + fmtPnl(r.pnl_1230) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.exit_1515_price)) + '</td>' +
                '<td class="num">' + fmtPnl(r.pnl_1515) + '</td>' +
                '<td>' + escapeHtml(r.pnl_5000_time || '—') + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.pnl_5000_ltp)) + '</td>' +
                '</tr>'
            );
        }).join('');
    }

    function fillSummary(doc) {
        const s = doc.summary || {};
        const rows = doc.rows || [];
        document.getElementById('nvbStatTrades').textContent = s.total_trades != null ? s.total_trades : '—';
        document.getElementById('nvbStatErrors').textContent = s.errors != null ? s.errors : '—';

        let sum1230 = 0;
        let sum1515 = 0;
        let hits5k = 0;
        rows.forEach(function (r) {
            if (Number.isFinite(Number(r.pnl_1230))) sum1230 += Number(r.pnl_1230);
            if (Number.isFinite(Number(r.pnl_1515))) sum1515 += Number(r.pnl_1515);
            if (r.pnl_5000_time) hits5k += 1;
        });
        document.getElementById('nvbStatPnl1230').innerHTML = fmtPnl(sum1230);
        document.getElementById('nvbStatPnl1515').innerHTML = fmtPnl(sum1515);
        document.getElementById('nvbStatPnl5k').textContent = hits5k;

        const dates = rows.map(function (r) { return tradeDateFromSignal(r.signal_time) || r.trade_date; }).filter(Boolean).sort();
        const fromEl = document.getElementById('nvbFrom');
        const toEl = document.getElementById('nvbTo');
        if (fromEl && dates.length) fromEl.value = dates[0];
        if (toEl && dates.length) toEl.value = dates[dates.length - 1];

        const foot = document.getElementById('nvbFooter');
        if (foot) {
            const partialNote = doc.partial ? ' · Run in progress (partial results)' : '';
            foot.textContent = 'Generated: ' + (doc.generated_at || '—') + partialNote;
        }
        const err = document.getElementById('nvbErr');
        if (err && doc.partial) {
            err.hidden = false;
            err.className = 'nvb-callout nvb-callout-info';
            err.textContent = 'Backtest still running — refresh later for full results.';
        }
    }

    function setupTheme() {
        const stored = localStorage.getItem('tradentical_theme') || 'dark';
        document.body.setAttribute('data-theme', stored);
        const btn = document.getElementById('nvbThemeBtn');
        if (!btn) return;
        function syncIcon() {
            const t = document.body.getAttribute('data-theme');
            btn.innerHTML = t === 'light'
                ? '<i class="fas fa-sun"></i>'
                : '<i class="fas fa-moon"></i>';
        }
        syncIcon();
        btn.addEventListener('click', function () {
            const next = document.body.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
            document.body.setAttribute('data-theme', next);
            localStorage.setItem('tradentical_theme', next);
            syncIcon();
        });
    }

    function bindFilters() {
        ['nvbFrom', 'nvbTo', 'nvbSymbol'].forEach(function (id) {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', function () { renderTable(state.rows); });
        });
        document.querySelectorAll('#nvbTable th[data-sort]').forEach(function (th) {
            th.addEventListener('click', function () {
                const key = th.getAttribute('data-sort');
                if (state.sort.key === key) {
                    state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sort.key = key;
                    state.sort.dir = 'asc';
                }
                renderTable(state.rows);
            });
        });
    }

    document.addEventListener('DOMContentLoaded', function () {
        setupTheme();
        bindFilters();
        loadData()
            .then(function (doc) {
                state.meta = doc;
                state.rows = doc.rows || [];
                fillSummary(doc);
                renderTable(state.rows);
                const err = document.getElementById('nvbErr');
                if (err) err.hidden = true;
            })
            .catch(function (e) {
                const err = document.getElementById('nvbErr');
                if (err) {
                    err.hidden = false;
                    err.textContent = e.message || String(e);
                }
                const tbody = document.getElementById('nvbTbody');
                if (tbody) {
                    tbody.innerHTML = '<tr><td colspan="' + COL_COUNT + '" class="nvb-empty">Backtest data unavailable.</td></tr>';
                }
            });
    });
})();
