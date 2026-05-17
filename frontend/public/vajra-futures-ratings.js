/**
 * TWCTO Vajra — futures trade qualification table (Daily + Smart Futures pages).
 */
(function (global) {
    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;

    function authHeaders() {
        const t = global.localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            'Content-Type': 'application/json',
            Accept: 'application/json',
        };
    }

    function fmtUpdated(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', {
                    timeZone: 'Asia/Kolkata',
                    dateStyle: 'short',
                    timeStyle: 'short',
                });
            }
        } catch (e) {}
        return String(iso);
    }

    function tradeTypeClass(tt) {
        const s = String(tt || '');
        if (s.indexOf('LONG') === 0) return 'vajra-tt-long';
        if (s.indexOf('SHORT') === 0) return 'vajra-tt-short';
        if (s === 'REJECT') return 'vajra-tt-reject';
        return 'vajra-tt-watch';
    }

    function revRiskClass(r) {
        const u = String(r || '').toUpperCase();
        if (u === 'HIGH') return 'vajra-rev-high';
        if (u === 'MEDIUM') return 'vajra-rev-med';
        return 'vajra-rev-low';
    }

    function passClass(cell) {
        return String(cell || '').indexOf('PASS') >= 0 ? 'vajra-pass' : 'vajra-fail';
    }

    function renderTable(rows) {
        if (!rows || !rows.length) {
            return '<p class="vajra-meta">No Vajra ratings yet for this session. The engine runs every 15 minutes (9:30–15:00 IST).</p>';
        }
        let html =
            '<div class="vajra-table-wrap"><table class="vajra-table"><thead><tr>' +
            '<th>Security</th><th>Trade Type</th><th class="num">Confidence</th>' +
            '<th>Structure</th><th>Momentum</th><th>Trend</th><th>Volume</th>' +
            '<th>OBV</th><th>Market Phase</th><th>Reversal Risk</th>' +
            '</tr></thead><tbody>';
        rows.forEach(function (r) {
            const conf = r.confidence != null ? Number(r.confidence).toFixed(1) : '—';
            html +=
                '<tr><td>' +
                escapeHtml(r.security || r.stock || '—') +
                '</td><td class="' +
                tradeTypeClass(r.trade_type) +
                '">' +
                escapeHtml(r.trade_type || '—') +
                '</td><td class="num">' +
                conf +
                '</td><td class="' +
                passClass(r.structure) +
                '">' +
                escapeHtml(r.structure || '—') +
                '</td><td class="' +
                passClass(r.momentum) +
                '">' +
                escapeHtml(r.momentum || '—') +
                '</td><td class="' +
                passClass(r.trend) +
                '">' +
                escapeHtml(r.trend || '—') +
                '</td><td class="' +
                passClass(r.volume) +
                '">' +
                escapeHtml(r.volume || '—') +
                '</td><td>' +
                escapeHtml(r.obv || '—') +
                '</td><td>' +
                escapeHtml(r.market_phase || '—') +
                '</td><td class="' +
                revRiskClass(r.reversal_risk) +
                '">' +
                escapeHtml(r.reversal_risk || '—') +
                '</td></tr>';
        });
        html += '</tbody></table></div>';
        return html;
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    async function fetchRatings() {
        const paths = [API_BASE + '/api/vajra-futures/ratings', API_BASE + '/vajra-futures/ratings'];
        let lastErr = null;
        for (let i = 0; i < paths.length; i++) {
            try {
                const res = await fetch(paths[i], { headers: authHeaders(), cache: 'no-store' });
                const data = await res.json();
                if (!res.ok) {
                    lastErr = data && data.message ? data.message : res.statusText;
                    continue;
                }
                return data;
            } catch (e) {
                lastErr = e.message || String(e);
            }
        }
        throw new Error(lastErr || 'Failed to load Vajra ratings');
    }

    /**
     * @param {object} opts
     * @param {string} opts.tableElId — container for table HTML
     * @param {string} [opts.metaElId] — session / updated line
     * @param {string} [opts.msgElId] — status / error line
     * @param {number} [opts.pollMs] — auto-refresh interval (0 = none)
     */
    function init(opts) {
        const tableEl = document.getElementById(opts.tableElId);
        const metaEl = opts.metaElId ? document.getElementById(opts.metaElId) : null;
        const msgEl = opts.msgElId ? document.getElementById(opts.msgElId) : null;

        async function load() {
            if (msgEl) msgEl.textContent = 'Loading Vajra ratings…';
            try {
                const data = await fetchRatings();
                const rows = (data && data.rows) || [];
                if (tableEl) tableEl.innerHTML = renderTable(rows);
                if (metaEl) {
                    metaEl.textContent =
                        'Session: ' +
                        (data.session_date || '—') +
                        ' · Updated: ' +
                        fmtUpdated(data.computed_at || (rows[0] && rows[0].computed_at)) +
                        ' · ' +
                        rows.length +
                        ' symbols';
                }
                if (msgEl) msgEl.textContent = '';
            } catch (e) {
                if (tableEl) tableEl.innerHTML = '';
                if (msgEl) msgEl.textContent = 'Vajra: ' + (e.message || String(e));
            }
        }

        load();
        const poll = opts.pollMs != null ? Number(opts.pollMs) : 120000;
        if (poll > 0) {
            setInterval(load, poll);
        }
        return { refresh: load };
    }

    global.VajraFuturesRatings = { init: init, fetchRatings: fetchRatings };
})(window);
