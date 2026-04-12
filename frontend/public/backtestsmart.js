/**
 * Smart Futures backtest viewer — all rows from backtest_smart_future (no session filter).
 * Open directly: /backtestsmart.html (not linked from the main menu).
 */
(function () {
    const API_BASE =
        window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : window.location.origin;

    function authHeaders() {
        const t = localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            'Content-Type': 'application/json',
        };
    }

    async function fetchRows() {
        const paths = ['/api/smart-futures-backtest/rows', '/smart-futures-backtest/rows'];
        let lastErr = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p + '?limit=8000', { headers: authHeaders(), cache: 'no-store' });
                if (res.ok) return await res.json();
                lastErr = new Error((await res.text()) || res.statusText);
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load backtest rows');
    }

    function fmtNum(v, d) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d);
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function escapeAttr(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;');
    }

    function fmtSymbolCell(r) {
        const sym = r && r.fut_symbol != null && r.fut_symbol !== '' ? String(r.fut_symbol) : '—';
        const ratio = r && r.atr5_14_ratio != null && r.atr5_14_ratio !== '' ? Number(r.atr5_14_ratio) : NaN;
        const hot = Number.isFinite(ratio) && ratio >= 1.1;
        const tip = Number.isFinite(ratio)
            ? 'ATR(5)/ATR(14) = ' + ratio.toFixed(3) + ' (session 5‑minute bars). Fire when ≥ 1.1.'
            : '';
        const titleAttr = tip ? ' title="' + escapeAttr(tip) + '"' : '';
        const label = sym === '—' ? sym : escapeHtml(sym);
        const fire = hot ? '<span class="sf-atr-fire" aria-hidden="true">🔥</span> ' : '';
        if (tip) {
            return '<span class="sf-symbol-wrap"' + titleAttr + '>' + fire + label + '</span>';
        }
        return fire ? fire + label : label;
    }

    function fmtSideCell(side) {
        const s = String(side || '').trim().toUpperCase();
        if (s === 'LONG') return '<span class="sf-side-pill sf-side-long">LONG</span>';
        if (s === 'SHORT') return '<span class="sf-side-pill sf-side-short">SHORT</span>';
        return side ? escapeHtml(String(side)) : '—';
    }

    function fmtGroupLabel(isoKey) {
        if (!isoKey || isoKey === '—') return 'Simulated as-of';
        try {
            const d = new Date(isoKey.length >= 16 ? isoKey + ':00' : isoKey);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'medium', timeStyle: 'short' });
            }
        } catch (e) {
            /* ignore */
        }
        return isoKey;
    }

    function render(data) {
        const host = document.getElementById('btGroups');
        const msg = document.getElementById('btMsg');
        const updated = document.getElementById('btUpdated');
        if (!host) return;
        if (data.error) {
            if (msg) msg.textContent = data.error;
        } else if (msg) {
            msg.textContent = '';
        }
        const groups = data.groups && data.groups.length ? data.groups : [];
        if (!groups.length) {
            host.innerHTML =
                '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="12" style="padding:14px;">No backtest rows yet.</td></tr></tbody></table></div>';
            if (updated) updated.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
            return;
        }
        const thead =
            '<thead><tr>' +
            '<th>Session date</th><th>Scan (IST)</th><th>Symbol</th><th>Side</th><th>Final CMS</th>' +
            '<th>Sector</th><th>Sentiment</th><th>Entry</th><th>SL</th><th>Target</th><th>VIX</th><th>Sentiment note</th>' +
            '</tr></thead>';
        let html = '';
        groups.forEach(function (g) {
            html += '<div class="sf-group-title">' + escapeHtml(fmtGroupLabel(g.simulated_asof)) + '</div>';
            html += '<div class="sf-table-wrap"><table class="sf-table">' + thead + '<tbody>';
            (g.rows || []).forEach(function (r) {
                html +=
                    '<tr>' +
                    '<td>' +
                    escapeHtml(String(r.session_date || '—')) +
                    '</td>' +
                    '<td>' +
                    escapeHtml(String(r.scan_time_label || '—')) +
                    '</td>' +
                    '<td>' +
                    fmtSymbolCell(r) +
                    '</td>' +
                    '<td>' +
                    fmtSideCell(r.side) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.final_cms, 2) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.sector_score, 2) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.combined_sentiment, 3) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.entry_price, 2) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.sl_price, 2) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.target_price, 2) +
                    '</td>' +
                    '<td>' +
                    fmtNum(r.vix_at_scan, 2) +
                    '</td>' +
                    '<td style="max-width:220px;font-size:0.8rem;">' +
                    escapeHtml(String(r.sentiment_source || '—').slice(0, 200)) +
                    '</td>' +
                    '</tr>';
            });
            html += '</tbody></table></div>';
        });
        host.innerHTML = html;
        if (updated) updated.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    }

    async function load(silent) {
        const msg = document.getElementById('btMsg');
        try {
            const data = await fetchRows();
            render(data);
        } catch (e) {
            if (!silent && msg) msg.textContent = String(e.message || e);
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const ref = document.getElementById('btRefresh');
        if (ref) ref.addEventListener('click', function () { load(false); });
        load(false);
    });
})();
