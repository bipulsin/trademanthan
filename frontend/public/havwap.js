(function () {
    'use strict';

    const API_PATHS = ['/api/ha-vwap/backtest', 'ha_vwap_data.json'];

    function apiBase() {
        if (location.hostname === 'localhost' || location.hostname === '127.0.0.1') {
            return 'http://localhost:8000';
        }
        return location.origin;
    }

    const $ = (id) => document.getElementById(id);

    function fmt(n, d) {
        if (n == null || n === '' || Number.isNaN(Number(n))) return '—';
        return Number(n).toLocaleString('en-IN', { maximumFractionDigits: d == null ? 2 : d });
    }

    function pnlClass(n) {
        const x = Number(n) || 0;
        return x > 0 ? 'hv-pos' : x < 0 ? 'hv-neg' : '';
    }

    async function load() {
        const err = $('hvErr');
        let doc = null;
        for (const p of API_PATHS) {
            try {
                const url = p.startsWith('http') || p.endsWith('.json') ? (p.startsWith('/') || p.startsWith('http') ? (p.startsWith('ha') ? p : apiBase() + p) : p) : apiBase() + p;
                const resolved = p.endsWith('.json') ? p : apiBase() + p;
                const res = await fetch(resolved, { cache: 'no-store' });
                if (!res.ok) continue;
                doc = await res.json();
                if (doc) break;
            } catch (e) {
                continue;
            }
        }
        if (!doc) {
            err.hidden = false;
            err.textContent = 'No backtest artifact yet. Run python3 scripts/run_ha_vwap_backtest.py';
            $('hvBody').innerHTML = '<tr><td colspan="13" class="vmb-empty">No trades</td></tr>';
            $('hvMonthBody').innerHTML = '<tr><td colspan="5" class="vmb-empty">In progress</td></tr>';
            return;
        }
        const s = doc.summary || {};
        $('hvTrades').textContent = s.trades == null ? '—' : String(s.trades);
        $('hvWin').textContent = s.win_pct == null ? '—' : Number(s.win_pct).toFixed(1) + '%';
        const pnlEl = $('hvPnl');
        pnlEl.textContent = '₹' + fmt(s.pnl, 0);
        pnlEl.className = 'vmb-stat-val ' + pnlClass(s.pnl);
        const st = doc.months_status || {};
        $('hvStatus').textContent = Object.keys(st).length ? JSON.stringify(st).slice(0, 80) : (doc.detail || 'ok');
        const months = s.by_month || {};
        const mrows = Object.keys(months).sort().reverse().map((m) => {
            const b = months[m];
            const run = st['futures_' + m] || st['cash_' + m] || st[m] || '';
            return '<tr><td>' + m + '</td><td class="num">' + (b.trades || 0) + '</td><td class="num">' +
                (b.win_pct != null ? Number(b.win_pct).toFixed(1) + '%' : '—') + '</td><td class="num ' + pnlClass(b.pnl) + '">₹' +
                fmt(b.pnl, 0) + '</td><td>' + run + '</td></tr>';
        });
        $('hvMonthBody').innerHTML = mrows.length ? mrows.join('') : '<tr><td colspan="5" class="vmb-empty">No months yet</td></tr>';
        const trades = doc.trades || [];
        const tbody = trades.slice().sort((a, b) => String(b.date).localeCompare(String(a.date)) || String(b.entry_time).localeCompare(String(a.entry_time)))
            .map((t) => {
                return '<tr><td>' + (t.date || '') + '</td><td>' + (t.symbol || '') + '</td><td>' + (t.instrument || '') +
                    '</td><td>' + (t.entry_time || '') + '</td><td class="num">' + fmt(t.entry, 2) + '</td><td class="num">' +
                    fmt(t.tp, 2) + '</td><td>' + (t.exit_time || '') + '</td><td class="num">' + fmt(t.exit, 2) +
                    '</td><td>' + (t.reason || '') + '</td><td class="num">' + fmt(t.volume, 0) +
                    '</td><td class="num">' + fmt(t.qty, 0) + '</td><td class="num ' +
                    pnlClass(t.pnl) + '">' + fmt(t.pnl, 0) + '</td><td class="num">' + fmt(t.R, 2) + '</td></tr>';
            });
        $('hvBody').innerHTML = tbody.length ? tbody.join('') : '<tr><td colspan="13" class="vmb-empty">No trades</td></tr>';
        $('hvFooter').textContent = (s.entry_fill || '') + ' · ' + (s.bars || '');
        if (doc.detail) {
            err.hidden = false;
            err.textContent = doc.detail;
        }
    }

    load();
})();
