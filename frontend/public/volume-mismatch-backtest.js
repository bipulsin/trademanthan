/**
 * Public Gap + Bollinger Band Futures backtest viewer.
 * No login required — /volumemismatch-backtest.html
 */
(function () {
    'use strict';

    const API_PATHS = ['/volume-mismatch-backtest/data', '/api/volume-mismatch-backtest/data'];
    const COL_COUNT = 18;

    const state = {
        rows: [],
        meta: {},
        sort: { key: 'trade_date', dir: 'desc' },
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

    function fmtPct(v) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return (n >= 0 ? '+' : '') + n.toFixed(2) + '%';
    }

    function applyFilters(rows) {
        const from = document.getElementById('vmbFrom')?.value || '';
        const to = document.getElementById('vmbTo')?.value || '';
        const dir = (document.getElementById('vmbDir')?.value || '').toUpperCase();
        const symQ = (document.getElementById('vmbSymbol')?.value || '').trim().toUpperCase();
        let out = rows.slice();
        if (from) out = out.filter(function (r) { return String(r.trade_date) >= from; });
        if (to) out = out.filter(function (r) { return String(r.trade_date) <= to; });
        if (dir) out = out.filter(function (r) { return String(r.direction).toUpperCase() === dir; });
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
            if (sk === 'trade_date') return sd * String(va).localeCompare(String(vb));
            const na = Number(va);
            const nb = Number(vb);
            if (Number.isFinite(na) && Number.isFinite(nb)) return sd * (na - nb);
            return sd * String(va || '').localeCompare(String(vb || ''));
        });
        return out;
    }

    function renderTable(rows) {
        const tbody = document.getElementById('vmbTbody');
        const countEl = document.getElementById('vmbCount');
        const filtered = applyFilters(rows);
        if (countEl) countEl.textContent = filtered.length + ' rows';
        if (!tbody) return;
        if (!filtered.length) {
            tbody.innerHTML = '<tr><td colspan="' + COL_COUNT + '" class="vmb-empty">No rows match filters.</td></tr>';
            return;
        }
        tbody.innerHTML = filtered.map(function (r) {
            const dir = String(r.direction || '').toUpperCase();
            const pillCls = dir === 'SHORT' ? 'vmb-pill-short' : 'vmb-pill-long';
            return (
                '<tr>' +
                '<td>' + escapeHtml(r.trade_date || '—') + '</td>' +
                '<td><strong>' + escapeHtml(r.symbol || '—') + '</strong></td>' +
                '<td><span class="vmb-pill ' + pillCls + '">' + escapeHtml(dir) + '</span></td>' +
                '<td class="num">' + escapeHtml(fmtPct(r.gap_percent)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.previous_close)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.first_15m_open)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.first_15m_high)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.first_15m_low)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.first_15m_close)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.first_15m_volume, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.net_volume, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.relative_volume, 2)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.score, 1)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.volume_bought, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.volume_sold, 0)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.bb_upper)) + '</td>' +
                '<td class="num">' + escapeHtml(fmtNum(r.bb_lower)) + '</td>' +
                '<td>' + escapeHtml(r.future_symbol || '—') + '</td>' +
                '</tr>'
            );
        }).join('');
    }

    function fillSummary(doc) {
        const s = doc.summary || {};
        document.getElementById('vmbStatSignals').textContent = s.total_signals != null ? s.total_signals : '—';
        document.getElementById('vmbStatLong').textContent = s.long_count != null ? s.long_count : '—';
        document.getElementById('vmbStatShort').textContent = s.short_count != null ? s.short_count : '—';
        document.getElementById('vmbStatSymbols').textContent = s.unique_symbols != null ? s.unique_symbols : '—';
        document.getElementById('vmbStatDays').textContent = s.trading_days_scanned != null ? s.trading_days_scanned : '—';
        document.getElementById('vmbStatRange').textContent =
            (doc.from_date || '—') + ' → ' + (doc.to_date || '—');
        const fromEl = document.getElementById('vmbFrom');
        const toEl = document.getElementById('vmbTo');
        if (fromEl && doc.from_date) fromEl.value = doc.from_date;
        if (toEl && doc.to_date) toEl.value = doc.to_date;
        const foot = document.getElementById('vmbFooter');
        if (foot) {
            const partialNote = doc.partial ? ' · Run in progress (partial results)' : '';
            const criteria = doc.signal_criteria ? ' · ' + doc.signal_criteria : '';
            foot.textContent =
                'Generated: ' + (doc.generated_at || '—') +
                ' · Scan: first 15m after open (' + (doc.scan_time_ist || '09:30:30') + ' IST)' +
                criteria +
                partialNote;
        }
        const err = document.getElementById('vmbErr');
        if (err && doc.partial) {
            err.hidden = false;
            err.className = 'vmb-callout vmb-callout-info';
            err.textContent =
                'Backtest still running — showing results scanned so far. Refresh later for the full range.';
        }
    }

    function setupTheme() {
        const stored = localStorage.getItem('tradentical_theme') || 'dark';
        document.body.setAttribute('data-theme', stored);
        const btn = document.getElementById('vmbThemeBtn');
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
        ['vmbFrom', 'vmbTo', 'vmbDir', 'vmbSymbol'].forEach(function (id) {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', function () { renderTable(state.rows); });
            if (el && el.tagName === 'SELECT') el.addEventListener('change', function () { renderTable(state.rows); });
        });
        document.querySelectorAll('#vmbTable th[data-sort]').forEach(function (th) {
            th.addEventListener('click', function () {
                const key = th.getAttribute('data-sort');
                if (state.sort.key === key) {
                    state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sort.key = key;
                    state.sort.dir = 'desc';
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
                const err = document.getElementById('vmbErr');
                if (err) err.hidden = true;
            })
            .catch(function (e) {
                const err = document.getElementById('vmbErr');
                if (err) {
                    err.hidden = false;
                    err.textContent = e.message || String(e);
                }
                const tbody = document.getElementById('vmbTbody');
                if (tbody) {
                    tbody.innerHTML = '<tr><td colspan="' + COL_COUNT + '" class="vmb-empty">Backtest data unavailable.</td></tr>';
                }
            });
    });
})();
