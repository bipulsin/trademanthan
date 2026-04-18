/**
 * Dashboard: Smart Futures Carry-Forward Watch.
 *
 * Pulls /smart-futures/watchlist and renders signals that fired AFTER the 14:00 IST cutoff
 * but still passed the reclaim-score (>= 55) and VWAP entry gates. These are "review
 * tomorrow pre-market" items. Today's fresh watchlist rows are highlighted separately.
 */
(function () {
    'use strict';

    const API_BASE = window.API_BASE_URL || window.API_BASE || '';
    const HOST_ID = 'sfWatchlistHost';
    const MSG_ID = 'sfWatchlistMsg';
    const UPDATED_ID = 'sfWatchlistUpdated';

    function authHeaders() {
        const headers = { 'Content-Type': 'application/json' };
        try {
            const token =
                (window.sessionStorage && window.sessionStorage.getItem('accessToken')) ||
                (window.localStorage && window.localStorage.getItem('accessToken')) ||
                null;
            if (token) headers['Authorization'] = 'Bearer ' + token;
        } catch (e) { /* ignore */ }
        return headers;
    }

    function escapeHtml(s) {
        return String(s == null ? '' : s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function fmtHhmm(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (!Number.isFinite(d.getTime())) return '—';
            return new Intl.DateTimeFormat('en-GB', {
                timeZone: 'Asia/Kolkata',
                hour: '2-digit',
                minute: '2-digit',
                hour12: false,
            }).format(d);
        } catch (e) { return '—'; }
    }

    function fmtDate(ymd) {
        if (!ymd) return '—';
        try {
            const d = new Date(ymd.length === 10 ? ymd + 'T00:00:00+05:30' : ymd);
            if (!Number.isFinite(d.getTime())) return escapeHtml(ymd);
            return new Intl.DateTimeFormat('en-IN', {
                timeZone: 'Asia/Kolkata',
                day: '2-digit',
                month: 'short',
                year: 'numeric',
            }).format(d);
        } catch (e) { return escapeHtml(ymd); }
    }

    function fmtNum(v, d) {
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d == null ? 2 : d);
    }

    function render(data) {
        const host = document.getElementById(HOST_ID);
        const msg = document.getElementById(MSG_ID);
        const updated = document.getElementById(UPDATED_ID);
        if (!host) return;
        if (updated) updated.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        const rows = (data && Array.isArray(data.rows)) ? data.rows : [];
        const today = String((data && data.today_session_date) || '');
        if (msg) msg.textContent = '';
        if (!rows.length) {
            host.innerHTML = '<p class="sf-wl-empty">No carry-forward watchlist items in the last 3 sessions.</p>';
            return;
        }
        const thead =
            '<thead><tr>' +
            '<th>Trigger Date</th><th>Time</th><th>Symbol</th><th>Side</th>' +
            '<th title="Reclaim probability at trigger (0–100)">Score</th>' +
            '<th>Price</th><th>VWAP</th>' +
            '</tr></thead>';
        const body = rows.map(function (r) {
            const trig = String(r.trigger_date || '');
            const isToday = trig === today;
            const sideU = String(r.side || '').trim().toUpperCase();
            const sidePill = sideU === 'LONG'
                ? '<span class="sf-side-pill sf-side-long">LONG</span>'
                : (sideU === 'SHORT'
                    ? '<span class="sf-side-pill sf-side-short">SHORT</span>'
                    : escapeHtml(sideU));
            const rowCls = isToday ? ' class="sf-wl-row-today"' : '';
            return (
                '<tr' + rowCls + '>' +
                '<td>' + escapeHtml(fmtDate(trig)) + '</td>' +
                '<td>' + escapeHtml(fmtHhmm(r.trigger_at)) + '</td>' +
                '<td>' + escapeHtml(String(r.fut_symbol || r.symbol || '—')) + '</td>' +
                '<td>' + sidePill + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.trigger_score, 0)) + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.trigger_price, 2)) + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.vwap_at_trigger, 2)) + '</td>' +
                '</tr>'
            );
        }).join('');
        host.innerHTML =
            '<div class="sf-wl-wrap"><table class="sf-wl-table">' +
            thead +
            '<tbody>' + body + '</tbody>' +
            '</table></div>';
    }

    async function load() {
        const msg = document.getElementById(MSG_ID);
        if (msg) msg.textContent = 'Loading…';
        const paths = ['/api/smart-futures/watchlist?days=3', '/smart-futures/watchlist?days=3'];
        let data = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, {
                    method: 'GET',
                    headers: authHeaders(),
                    cache: 'no-store',
                });
                if (!res.ok) continue;
                data = await res.json();
                if (data) break;
            } catch (e) { /* try next */ }
        }
        if (!data) {
            if (msg) msg.textContent = 'Watchlist unavailable.';
            return;
        }
        render(data);
    }

    document.addEventListener('DOMContentLoaded', function () {
        const refresh = document.getElementById('sfWatchlistRefresh');
        if (refresh) refresh.addEventListener('click', function () { load(); });
        load();
    });
})();
