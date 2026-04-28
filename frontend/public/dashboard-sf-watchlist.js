/**
 * Dashboard: Smart Futures Carry-Forward Watch.
 *
 * Pulls /smart-futures/watchlist and renders signals that fired AFTER the 14:00 IST cutoff
 * but still passed the reclaim-score (>= 55) and VWAP entry gates. These are "review
 * tomorrow pre-market" items. Today's fresh watchlist rows are highlighted separately.
 *
 * Display window (IST): from 14:30 on day D through 14:30 on D+1, show only rows with
 * trigger_date = D. Before 14:30, show the previous NSE session only (e.g. 24 Apr 09:00
 * → 23 Apr's list). No date column in the table.
 * Auto-refresh every 300 seconds so LTP/PnL stay updated.
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
    const POLL_LTP_MS = 300 * 1000;

    var sfCarryFulfilled = false; // after 14:30: at least one row for today's IST date
    var sfPollTimer = null;
    var sfWlIstYmd = null; // last IST calendar day seen (reset carry flag when IST day changes)

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

    function getIstYmdNow() {
        return new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Kolkata',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
        }).format(new Date());
    }

    /**
     * Most recent NSE cash-session calendar day strictly before `istYmd` (skips Sat/Sun in IST).
     */
    function previousNseSessionYmd(istYmd) {
        var t = new Date(istYmd.slice(0, 10) + 'T12:00:00+05:30');
        for (var i = 0; i < 10; i++) {
            t = new Date(t.getTime() - 24 * 60 * 60 * 1000);
            var wd = new Intl.DateTimeFormat('en', {
                timeZone: 'Asia/Kolkata',
                weekday: 'short',
            }).format(t);
            if (wd !== 'Sat' && wd !== 'Sun') {
                return new Intl.DateTimeFormat('en-CA', {
                    timeZone: 'Asia/Kolkata',
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit',
                }).format(t);
            }
        }
        return normYmd(istYmd);
    }

    /**
     * trigger_date to show: today after 14:30 IST, else previous session (e.g. before 14:30 on 24th → 23rd).
     */
    function getDisplayTriggerYmd() {
        var istYmd = getIstYmdNow();
        if (isAfter1430Ist()) {
            return istYmd;
        }
        return previousNseSessionYmd(istYmd);
    }

    function filterRowsForWindow(rows) {
        var y = getDisplayTriggerYmd();
        var out = [];
        if (!Array.isArray(rows) || !y) return out;
        for (var i = 0; i < rows.length; i++) {
            if (normYmd(rows[i] && rows[i].trigger_date) === y) {
                out.push(rows[i]);
            }
        }
        return out;
    }

    function updateCarryFulfilledFlag(data) {
        var istDay = getIstYmdNow();
        if (sfWlIstYmd && istDay !== sfWlIstYmd) {
            sfCarryFulfilled = false;
        }
        sfWlIstYmd = istDay;
        var rows = (data && Array.isArray(data.rows)) ? data.rows : [];
        if (!isAfter1430Ist()) {
            sfCarryFulfilled = true;
            return;
        }
        for (var j = 0; j < rows.length; j++) {
            if (normYmd(rows[j] && rows[j].trigger_date) === istDay) {
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

    function fmtNum(v, d) {
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d == null ? 2 : d);
    }

    function renderStatusHint() {
        const msg = document.getElementById(MSG_ID);
        if (!msg) return;
        if (isAfter1430Ist()) {
            msg.textContent =
                'Showing today’s carry-forwards (14:30 IST → next 14:30). LTP/PnL refresh every 300 seconds.';
        } else {
            msg.textContent =
                'Before 14:30 IST, showing the prior session’s list; LTP/PnL refresh every 300 seconds.';
        }
    }

    function render(data) {
        const host = document.getElementById(HOST_ID);
        const updated = document.getElementById(UPDATED_ID);
        if (!host) return;
        updateCarryFulfilledFlag(data);
        if (updated) {
            updated.textContent = 'Updated ' + new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        }
        const allRows = (data && Array.isArray(data.rows)) ? data.rows : [];
        const vis = filterRowsForWindow(allRows);
        renderStatusHint();
        if (!allRows.length) {
            host.innerHTML =
                '<p class="sf-wl-empty">No carry-forward watchlist data in the last few sessions.</p>';
            return;
        }
        if (!vis.length) {
            host.innerHTML =
                '<p class="sf-wl-empty">No items for the current review window (see note above). After 14:30 IST, new rows appear for today.</p>';
            return;
        }
        const thead =
            '<thead><tr>' +
            '<th>Time</th><th>Symbol</th><th>Side</th>' +
            '<th title="Reclaim probability at trigger (0–100)">Score</th>' +
            '<th>Price</th><th>VWAP</th><th>LTP</th><th>PnL</th>' +
            '<th title="Serial current-month future from arbitrage_master (underlying = first segment of Symbol)">' +
            'Cur.mth FUT</th>' +
            '<th title="arbitrage_master.currmth_future_ltp" class="num">Cur.mth LTP</th>' +
            '</tr></thead>';
        const body = vis.map(function (r) {
            const sideU = String(r.side || '').trim().toUpperCase();
            const sidePill = sideU === 'LONG'
                ? '<span class="sf-side-pill sf-side-long">LONG</span>'
                : (sideU === 'SHORT'
                    ? '<span class="sf-side-pill sf-side-short">SHORT</span>'
                    : escapeHtml(sideU));
            var pnl = Number(r.pnl_points);
            var pnlTxt = Number.isFinite(pnl)
                ? ((pnl > 0 ? '+' : '') + pnl.toFixed(2))
                : '—';
            var pnlStyle = Number.isFinite(pnl)
                ? (pnl > 0 ? ' style="color:#15803d;font-weight:600;"' : (pnl < 0 ? ' style="color:#dc2626;font-weight:600;"' : ''))
                : '';
            var mSym = String(r.master_currmth_future_symbol || '').trim();
            var mKey = String(r.master_currmth_future_inst_key || '').trim();
            var masterTit = (mSym || mKey) ? escapeHtml(mSym + (mKey ? ' · ' + mKey : '')) : '';
            var masterFutCell =
                '<td style="font-size:0.82rem;line-height:1.25;max-width:12rem"' +
                (masterTit ? (' title="' + masterTit + '"') : '') +
                '>' +
                (mSym ? escapeHtml(mSym) : '—') +
                '</td>';
            var ml = Number(r.master_currmth_future_ltp);
            var masterLtpTxt = Number.isFinite(ml) ? ml.toFixed(2) : '—';
            return (
                '<tr>' +
                '<td>' + escapeHtml(fmtHhmm(r.trigger_at)) + '</td>' +
                '<td>' + escapeHtml(String(r.fut_symbol || r.symbol || '—')) + '</td>' +
                '<td>' + sidePill + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.trigger_score, 0)) + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.trigger_price, 2)) + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.vwap_at_trigger, 2)) + '</td>' +
                '<td>' + escapeHtml(fmtNum(r.ltp, 2)) + '</td>' +
                '<td' + pnlStyle + '>' + escapeHtml(pnlTxt) + '</td>' +
                masterFutCell +
                '<td class="num">' + escapeHtml(masterLtpTxt) + '</td>' +
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
        return POLL_LTP_MS;
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
