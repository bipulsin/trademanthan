/**
 * Dashboard: Smart Futures Carry-Forward Watch.
 *
 * Pulls /smart-futures/watchlist and renders signals that fired AFTER the 14:00 IST cutoff
 * but still passed the reclaim-score (>= 55) and VWAP entry gates. These are "review
 * tomorrow pre-market" items. Today's fresh watchlist rows are highlighted separately.
 *
 * After 14:30 IST, auto-refresh: every 2 min until at least one row for today's session
 * exists, then every 15 min. Before 14:30, load once on open (plus manual refresh).
 */
(function () {
    'use strict';

    const API_BASE =
        (typeof window.API_BASE_URL === 'string' && window.API_BASE_URL) ||
        (typeof window.API_BASE === 'string' && window.API_BASE) ||
        (function () {
            const h = window.location.hostname;
            if (h === 'localhost' || h === '127.0.0.1') return 'http://localhost:8000';
            return window.location.origin;
        })();
    const HOST_ID = 'sfWatchlistHost';
    const MSG_ID = 'sfWatchlistMsg';
    const UPDATED_ID = 'sfWatchlistUpdated';
    const POLL_FAST_MS = 2 * 60 * 1000;
    const POLL_SLOW_MS = 15 * 60 * 1000;
    const PRE_1430_MS = 5 * 60 * 1000;

    var sfCarryFulfilled = false; // at least one watchlist row for today_session_date
    var sfPollTimer = null;
    var sfWlSessionYmd = null; // last seen today_session_date from API (reset carry flag on new day)

    function getToken() {
        try {
            return (
                (window.localStorage && window.localStorage.getItem('trademanthan_token')) ||
                (window.sessionStorage && window.sessionStorage.getItem('trademanthan_token')) ||
                (window.localStorage && window.localStorage.getItem('accessToken')) ||
                (window.sessionStorage && window.sessionStorage.getItem('accessToken')) ||
                null
            );
        } catch (e) {
            return null;
        }
    }

    function authHeaders() {
        const headers = {
            'Content-Type': 'application/json',
            Accept: 'application/json',
        };
        const token = getToken();
        if (token) headers['Authorization'] = 'Bearer ' + token;
        return headers;
    }

    /** Minutes from midnight in Asia/Kolkata (0–1440). */
    function istMinutesFromMidnight() {
        const parts = new Intl.DateTimeFormat('en-GB', {
            timeZone: 'Asia/Kolkata',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false,
        }).formatToParts(new Date());
        var h = 0;
        var m = 0;
        for (var i = 0; i < parts.length; i++) {
            if (parts[i].type === 'hour') h = parseInt(parts[i].value, 10) || 0;
            if (parts[i].type === 'minute') m = parseInt(parts[i].value, 10) || 0;
        }
        return h * 60 + m;
    }

    function isAfter1430Ist() {
        return istMinutesFromMidnight() >= 14 * 60 + 30;
    }

    function normYmd(s) {
        if (s == null) return '';
        var t = String(s).trim();
        if (t.length >= 10) return t.slice(0, 10);
        return t;
    }

    function updateCarryFulfilledFlag(data) {
        var today = normYmd((data && data.today_session_date) || '');
        if (today && sfWlSessionYmd && today !== sfWlSessionYmd) {
            sfCarryFulfilled = false;
        }
        if (today) {
            sfWlSessionYmd = today;
        }
        var rows = (data && Array.isArray(data.rows)) ? data.rows : [];
        if (!today) {
            sfCarryFulfilled = false;
            return;
        }
        for (var i = 0; i < rows.length; i++) {
            if (normYmd(rows[i] && rows[i].trigger_date) === today) {
                sfCarryFulfilled = true;
                return;
            }
        }
        sfCarryFulfilled = false;
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

    function renderStatusHint() {
        const msg = document.getElementById(MSG_ID);
        if (!msg) return;
        if (isAfter1430Ist()) {
            if (sfCarryFulfilled) {
                msg.textContent = 'Auto-refresh every 15 min (today’s list is present).';
            } else {
                msg.textContent =
                    'After 14:30 IST: refreshing every 2 min until today’s carry-forward rows appear, then every 15 min.';
            }
        } else {
            msg.textContent =
                'List loads now; from 14:30 IST the dashboard auto-refreshes for today’s late-session carry-forwards.';
        }
    }

    function render(data) {
        const host = document.getElementById(HOST_ID);
        const msg = document.getElementById(MSG_ID);
        const updated = document.getElementById(UPDATED_ID);
        if (!host) return;
        updateCarryFulfilledFlag(data);
        if (updated) {
            updated.textContent = 'Updated ' + new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        }
        const rows = (data && Array.isArray(data.rows)) ? data.rows : [];
        const today = String((data && data.today_session_date) || '');
        renderStatusHint();
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

    function clearSfPoll() {
        if (sfPollTimer) {
            clearTimeout(sfPollTimer);
            sfPollTimer = null;
        }
    }

    function nextPollDelayMs() {
        if (!isAfter1430Ist()) {
            return PRE_1430_MS;
        }
        return sfCarryFulfilled ? POLL_SLOW_MS : POLL_FAST_MS;
    }

    function scheduleCarryForwardPoll() {
        clearSfPoll();
        sfPollTimer = setTimeout(function () {
            load({ scheduled: true });
        }, nextPollDelayMs());
    }

    async function load(opts) {
        opts = opts || {};
        const msg = document.getElementById(MSG_ID);
        if (!opts.scheduled && msg) msg.textContent = 'Loading…';
        const paths = ['/api/smart-futures/watchlist?days=3', '/smart-futures/watchlist?days=3'];
        let data = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, {
                    method: 'GET',
                    headers: authHeaders(),
                    cache: 'no-store',
                });
                if (res.status === 401) {
                    if (msg) msg.textContent = 'Session expired — refresh the page to sign in again.';
                    scheduleCarryForwardPoll();
                    return;
                }
                if (!res.ok) continue;
                const ct = (res.headers.get('content-type') || '').toLowerCase();
                if (!ct.includes('application/json')) continue;
                data = await res.json();
                if (data) break;
            } catch (e) {
                /* try next */
            }
        }
        if (!data) {
            if (msg) msg.textContent = 'Watchlist unavailable (check /api proxy or try again).';
            scheduleCarryForwardPoll();
            return;
        }
        render(data);
        scheduleCarryForwardPoll();
    }

    document.addEventListener('DOMContentLoaded', function () {
        const refresh = document.getElementById('sfWatchlistRefresh');
        if (refresh) {
            refresh.addEventListener('click', function () {
                load({ scheduled: false });
            });
        }
        load({ scheduled: false });
    });
})();
