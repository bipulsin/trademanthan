(function () {
    'use strict';

    const API_PATHS = ['/api/open-low-15m-backtest/data', '/open-low-15m-backtest/data'];

    function apiBase() {
        if (location.hostname === 'localhost' || location.hostname === '127.0.0.1') {
            return 'http://localhost:8000';
        }
        return location.origin;
    }

    let raw = null;
    let allRows = [];
    let rows = [];
    let sortKey = 'session_date';
    let sortAsc = false;

    const $ = (id) => document.getElementById(id);

    function fmt(n, d) {
        if (n == null || n === '' || Number.isNaN(Number(n))) return '—';
        return Number(n).toFixed(d == null ? 2 : d);
    }

    function showErr(msg) {
        const el = $('oltErr');
        el.hidden = !msg;
        el.textContent = msg || '';
    }

    function aggregateMetrics(list) {
        const rs = list.map((r) => Number(r.r_realized) || 0);
        const n = rs.length;
        if (!n) {
            return { total_trades: 0, win_rate: 0, avg_r: 0 };
        }
        const wins = rs.filter((x) => x > 0).length;
        return {
            total_trades: n,
            win_rate: 100 * wins / n,
            avg_r: rs.reduce((a, b) => a + b, 0) / n,
        };
    }

    function applyFilters() {
        if (!raw) return;
        const f = ($('oltFrom').value || '').trim();
        const t = ($('oltTo').value || '').trim();
        const tp = ($('oltTp').value || '').trim();
        const sl = ($('oltSl').value || '').trim();
        const sym = ($('oltSymbol').value || '').trim().toUpperCase();
        const topOnly = $('oltTopGainer').checked;

        rows = allRows.filter((r) => {
            const sd = String(r.session_date || '');
            if (f && sd < f) return false;
            if (t && sd > t) return false;
            if (tp && String(r.tp_variant || '') !== tp) return false;
            if (sl && String(r.sl_type || '') !== sl) return false;
            if (sym && !String(r.symbol || '').toUpperCase().includes(sym)) return false;
            if (topOnly && !r.is_top_gainer) return false;
            return true;
        });

        sortRows();
        renderAll();
    }

    function resetFilters() {
        if (!raw) return;
        if (raw.from_date) $('oltFrom').value = raw.from_date;
        if (raw.to_date) $('oltTo').value = raw.to_date;
        $('oltTp').value = '';
        $('oltSl').value = '';
        $('oltSymbol').value = '';
        $('oltTopGainer').checked = false;
        applyFilters();
    }

    function sortRows() {
        rows.sort((a, b) => {
            let va = a[sortKey];
            let vb = b[sortKey];
            if (sortKey === 'is_top_gainer') {
                va = va ? 1 : 0;
                vb = vb ? 1 : 0;
            }
            if (va == null) va = '';
            if (vb == null) vb = '';
            if (typeof va === 'number' && typeof vb === 'number') {
                return sortAsc ? va - vb : vb - va;
            }
            return sortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
        });
    }

    function renderSummary() {
        const rs = rows.map((r) => Number(r.r_realized) || 0);
        const wins = rs.filter((x) => x > 0).length;
        const n = rs.length;
        let eq = 0, peak = 0, dd = 0;
        rs.forEach((x) => { eq += x; peak = Math.max(peak, eq); dd = Math.max(dd, peak - eq); });
        $('oltStatTrades').textContent = n;
        $('oltStatWin').textContent = n ? fmt(100 * wins / n, 1) + '%' : '—';
        $('oltStatAvgR').textContent = n ? fmt(rs.reduce((a, b) => a + b, 0) / n, 3) : '—';
        $('oltStatDD').textContent = fmt(dd, 3);
        $('oltStatBestWorst').textContent = n ? fmt(Math.max(...rs), 2) + ' / ' + fmt(Math.min(...rs), 2) : '—';
        $('oltStatRun').textContent = raw.run_id || '—';

        const parts = [n + ' trades shown'];
        if (allRows.length !== n) parts.push(allRows.length + ' total');
        if (raw.partial) parts.push('partial run — refresh later');
        $('oltCount').textContent = parts.join(' · ');
    }

    function renderTpSlCards() {
        const tpEl = $('oltTpSummary');
        const slEl = $('oltSlSummary');
        tpEl.innerHTML = '';
        slEl.innerHTML = '';

        ['TP1', 'TP2', 'TP3', 'TP4'].forEach((k) => {
            const sub = rows.filter((r) => r.tp_variant === k);
            const m = aggregateMetrics(sub);
            const d = document.createElement('div');
            d.className = 'vmb-stat';
            d.innerHTML = '<span class="vmb-stat-label">' + k + '</span><span class="vmb-stat-val">' +
                fmt(m.win_rate, 1) + '% · avgR ' + fmt(m.avg_r, 2) + ' · n=' + m.total_trades + '</span>';
            tpEl.appendChild(d);
        });

        ['primary', 'alternative'].forEach((k) => {
            const sub = rows.filter((r) => r.sl_type === k);
            const m = aggregateMetrics(sub);
            const d = document.createElement('div');
            d.className = 'vmb-stat';
            d.innerHTML = '<span class="vmb-stat-label">SL ' + k + '</span><span class="vmb-stat-val">' +
                fmt(m.win_rate, 1) + '% · avgR ' + fmt(m.avg_r, 2) + ' · n=' + m.total_trades + '</span>';
            slEl.appendChild(d);
        });
    }

    function renderDaily() {
        const byDate = {};
        rows.forEach((r) => {
            const d = r.session_date;
            if (!byDate[d]) byDate[d] = [];
            byDate[d].push(Number(r.r_realized) || 0);
        });
        const body = $('oltDailyBody');
        const keys = Object.keys(byDate).sort();
        if (!keys.length) {
            body.innerHTML = '<tr><td colspan="4" class="vmb-empty">No data for current filters</td></tr>';
            return;
        }
        body.innerHTML = keys.map((d) => {
            const rs = byDate[d];
            const wins = rs.filter((x) => x > 0).length;
            const avg = rs.reduce((a, b) => a + b, 0) / rs.length;
            return '<tr><td>' + d + '</td><td class="num">' + rs.length + '</td><td class="num">' +
                fmt(100 * wins / rs.length, 1) + '</td><td class="num">' + fmt(avg, 3) + '</td></tr>';
        }).join('');
    }

    function renderTrades() {
        const tb = $('oltTbody');
        if (!rows.length) {
            tb.innerHTML = '<tr><td colspan="12" class="vmb-empty">No trades match filters</td></tr>';
            return;
        }
        tb.innerHTML = rows.map((r) =>
            '<tr><td>' + r.session_date + '</td><td>' + r.symbol + '</td><td>' + r.tp_variant +
            '</td><td>' + r.sl_type + '</td><td class="num">' + fmt(r.entry_price, 2) +
            '</td><td class="num">' + fmt(r.sl_price, 2) + '</td><td class="num">' + fmt(r.exit_price, 2) +
            '</td><td>' + (r.exit_reason || '') + '</td><td class="num">' + fmt(r.r_realized, 3) +
            '</td><td class="num">' + fmt(r.pnl_inr, 0) + '</td><td class="num">' + (r.holding_minutes || '') +
            '</td><td>' + (r.is_top_gainer ? '★' : '') + '</td></tr>'
        ).join('');
    }

    function drawLine(canvasId, values, color) {
        const c = $(canvasId);
        if (!c) return;
        const ctx = c.getContext('2d');
        const w = c.clientWidth || 400;
        const h = c.clientHeight || 180;
        c.width = w;
        c.height = h;
        ctx.clearRect(0, 0, w, h);
        if (!values.length) return;
        const min = Math.min(...values);
        const max = Math.max(...values);
        const pad = 20;
        const range = max - min || 1;
        ctx.strokeStyle = color || '#059669';
        ctx.lineWidth = 2;
        ctx.beginPath();
        values.forEach((v, i) => {
            const x = pad + (i / Math.max(1, values.length - 1)) * (w - 2 * pad);
            const y = h - pad - ((v - min) / range) * (h - 2 * pad);
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();
    }

    function renderCharts() {
        const sorted = rows.slice().sort((a, b) => String(a.session_date).localeCompare(String(b.session_date)));
        let cum = 0;
        const equity = sorted.map((r) => { cum += Number(r.r_realized) || 0; return cum; });
        drawLine('oltEquityChart', equity, '#059669');

        const byDate = {};
        sorted.forEach((r) => {
            const d = r.session_date;
            if (!byDate[d]) byDate[d] = { w: 0, n: 0 };
            byDate[d].n += 1;
            if (Number(r.r_realized) > 0) byDate[d].w += 1;
        });
        const dates = Object.keys(byDate).sort();
        const winRoll = dates.map((d, i) => {
            const window = dates.slice(Math.max(0, i - 4), i + 1);
            let w = 0, n = 0;
            window.forEach((dd) => { w += byDate[dd].w; n += byDate[dd].n; });
            return n ? (100 * w / n) : 0;
        });
        drawLine('oltWinChart', winRoll, '#7c3aed');
    }

    function renderAll() {
        renderSummary();
        renderTpSlCards();
        renderDaily();
        renderTrades();
        renderCharts();
        const scan = raw.trading_days_scanned && raw.trading_days_total
            ? ' · scanned ' + raw.trading_days_scanned + '/' + raw.trading_days_total + ' days'
            : '';
        $('oltFooter').textContent = (raw.generated_at ? 'Generated ' + raw.generated_at : '') +
            scan + (raw.artifact_path ? ' · ' + raw.artifact_path : '');
    }

    function bindSort() {
        document.querySelectorAll('#oltTable th[data-sort]').forEach((th) => {
            th.addEventListener('click', () => {
                const k = th.getAttribute('data-sort');
                if (sortKey === k) sortAsc = !sortAsc;
                else { sortKey = k; sortAsc = false; }
                sortRows();
                renderTrades();
            });
        });
    }

    function bindFilters() {
        $('oltApplyBtn').addEventListener('click', applyFilters);
        $('oltResetBtn').addEventListener('click', resetFilters);
        $('oltRefreshBtn').addEventListener('click', load);
        $('oltSymbol').addEventListener('keydown', (e) => {
            if (e.key === 'Enter') applyFilters();
        });
    }

    async function load() {
        showErr('');
        $('oltTbody').innerHTML = '<tr><td colspan="12" class="vmb-empty">Loading…</td></tr>';
        let lastErr = null;
        for (const path of API_PATHS) {
            try {
                const r = await fetch(apiBase() + path, { cache: 'no-store' });
                const ct = (r.headers.get('content-type') || '').toLowerCase();
                if (!r.ok || !ct.includes('json')) {
                    throw new Error(r.status === 503 ? 'Artifact not found' : 'Non-JSON response');
                }
                raw = await r.json();
                allRows = raw.rows || [];
                rows = allRows.slice();
                if (raw.from_date) $('oltFrom').value = raw.from_date;
                if (raw.to_date) $('oltTo').value = raw.to_date;
                applyFilters();
                return;
            } catch (e) {
                lastErr = e;
            }
        }
        showErr('Failed to load backtest: ' + (lastErr && lastErr.message ? lastErr.message : 'Artifact not found'));
        $('oltTbody').innerHTML = '<tr><td colspan="12" class="vmb-empty">No artifact — run scripts/run_open_low_15m_backtest.py</td></tr>';
    }

    bindSort();
    bindFilters();
    load();
})();
