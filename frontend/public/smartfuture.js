/**
 * Smart Futures — Today's Trend from smart_futures_daily (grouped by entry time).
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

    async function fetchDailyJson() {
        const paths = ['/api/smart-futures/daily', '/smart-futures/daily'];
        let lastErr = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { headers: authHeaders(), cache: 'no-store' });
                if (res.ok) return await res.json();
                lastErr = new Error((await res.text()) || res.statusText);
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load daily picks');
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

    /** Plain-language tooltip for LONG/SHORT (uses row.cms when API sends it). */
    function sideTooltip(r) {
        const s = String(r.side || '').trim().toUpperCase();
        const fc = Number(r.final_cms);
        const ss = Number(r.sector_score);
        const cs = Number(r.combined_sentiment);
        const core = r.cms != null && r.cms !== '' ? Number(r.cms) : NaN;
        const lines = [];
        lines.push(
            'LONG if Final CMS > 0 at scan time, SHORT if < 0. ' +
                'Final CMS = core CMS × (1 + 0.5×sector score) × (1 + sentiment). ' +
                'Core CMS is from this futures contract’s session (5‑minute bars: VWAP vs price, volume surge, Renko/Heikin‑Ashi, divergences) plus recent daily OBV—not the same as a spot equity “uptrend” on another timeframe.'
        );
        const bits = [];
        if (Number.isFinite(core)) bits.push('core CMS ' + core.toFixed(2));
        if (Number.isFinite(ss)) bits.push('sector ' + ss.toFixed(2));
        if (Number.isFinite(cs)) bits.push('sentiment ' + cs.toFixed(3));
        if (Number.isFinite(fc)) bits.push('final CMS ' + fc.toFixed(2));
        if (bits.length) lines.push('This row: ' + bits.join(' · ') + '.');
        if (s === 'LONG' || s === 'SHORT') {
            lines.push('Side shown: ' + s + ' (follows sign of final CMS).');
        }
        return lines.join(' ');
    }

    function fmtSideCell(r) {
        const side = r && r.side != null ? r.side : '';
        const s = String(side || '').trim().toUpperCase();
        const tip = escapeAttr(sideTooltip(r || {}));
        const titleAttr = tip ? ' title="' + tip + '"' : '';
        if (s === 'LONG') {
            return '<span class="sf-side-pill sf-side-long"' + titleAttr + '>LONG</span>';
        }
        if (s === 'SHORT') {
            return '<span class="sf-side-pill sf-side-short"' + titleAttr + '>SHORT</span>';
        }
        return side ? escapeHtml(String(side)) : '—';
    }

    function fmtTrendCell(r) {
        if (String(r.trend_continuation || '').trim() !== 'Yes') return '';
        return (
            '<span class="sf-trend-yes" title="Yes">' +
            '<i class="fas fa-check" aria-hidden="true"></i>' +
            '<span class="sr-only">Yes</span>' +
            '</span>'
        );
    }

    /** ATR(5)/ATR(14) on session 5m bars; 🔥 next to symbol when ratio ≥ 1.1. */
    function fmtSymbolCell(r) {
        const sym = r && r.fut_symbol != null && r.fut_symbol !== '' ? String(r.fut_symbol) : '—';
        const ratio = r && r.atr5_14_ratio != null && r.atr5_14_ratio !== '' ? Number(r.atr5_14_ratio) : NaN;
        const hot = Number.isFinite(ratio) && ratio >= 1.1;
        const tip = Number.isFinite(ratio)
            ? 'ATR(5)/ATR(14) = ' + ratio.toFixed(3) + ' (session 5‑minute bars). Fire when ≥ 1.1.'
            : '';
        const titleAttr = tip ? ' title="' + escapeAttr(tip) + '"' : '';
        let inner = sym === '—' ? sym : escapeHtml(sym);
        if (hot) {
            inner += ' <span class="sf-atr-fire" aria-hidden="true">🔥</span>';
        }
        if (tip) {
            inner = '<span class="sf-symbol-wrap"' + titleAttr + '>' + inner + '</span>';
        }
        return inner;
    }

    function fmtEntryGroupLabel(bucket) {
        if (!bucket || bucket === '—') return 'Entry time';
        try {
            const d = new Date(bucket.length >= 16 ? bucket + ':00' : bucket);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'medium', timeStyle: 'short' });
            }
        } catch (e) { /* ignore */ }
        return bucket;
    }

    function renderGroups(data) {
        const host = document.getElementById('sfTrendGroups');
        const sessionEl = document.getElementById('sfTrendSession');
        const msg = document.getElementById('sfTrendMsg');
        if (!host) return;

        if (sessionEl) sessionEl.textContent = data.session_date || '—';

        if (data.error) {
            msg.textContent = data.error;
        } else {
            msg.textContent = '';
        }

        const groups = data.groups && data.groups.length ? data.groups : [];
        if (!groups.length) {
            host.innerHTML =
                '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="10" style="padding:14px;">No Record</td></tr></tbody></table></div>';
            return;
        }

        const thead =
            '<thead><tr>' +
            '<th>Symbol</th><th>Side</th><th>Final CMS</th><th>Sector Score</th><th>Sentiment Score</th>' +
            '<th>Entry</th><th>SL</th><th>Target</th><th>In Trend</th><th>Order</th>' +
            '</tr></thead>';

        let html = '';
        groups.forEach(function (g) {
            const label = fmtEntryGroupLabel(g.entry_at);
            html += '<div class="sf-group-title">' + label + '</div>';
            html += '<div class="sf-table-wrap"><table class="sf-table">' + thead + '<tbody>';
            (g.rows || []).forEach(function (r) {
                const bought = String(r.order_status || '').toLowerCase() === 'bought';
                const btnLabel = bought ? 'Bought' : 'Order';
                html +=
                    '<tr data-row-id="' +
                    r.id +
                    '">' +
                    '<td>' +
                    fmtSymbolCell(r) +
                    '</td>' +
                    '<td>' +
                    fmtSideCell(r) +
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
                    fmtTrendCell(r) +
                    '</td>' +
                    '<td><button type="button" class="sf-btn-order" data-order-id="' +
                    r.id +
                    '"' +
                    (bought ? ' disabled' : '') +
                    '>' +
                    btnLabel +
                    '</button></td>' +
                    '</tr>';
            });
            html += '</tbody></table></div>';
        });
        host.innerHTML = html;

        host.querySelectorAll('.sf-btn-order[data-order-id]').forEach(function (btn) {
            btn.addEventListener('click', onOrderClick);
        });
    }

    async function onOrderClick(ev) {
        const btn = ev.currentTarget;
        const id = btn.getAttribute('data-order-id');
        if (!id || btn.disabled) return;
        if (!window.confirm('Mark this row as bought at current LTP?')) return;
        btn.disabled = true;
        const paths = ['/api/smart-futures/daily/' + id + '/order', '/smart-futures/daily/' + id + '/order'];
        let ok = false;
        let errText = '';
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { method: 'POST', headers: authHeaders() });
                if (res.ok) {
                    ok = true;
                    break;
                }
                errText = (await res.text()) || res.statusText;
            } catch (e) {
                errText = String(e.message || e);
            }
        }
        if (!ok) {
            alert(errText || 'Order failed');
            btn.disabled = false;
            return;
        }
        btn.textContent = 'Bought';
        await loadTrend(true);
    }

    async function loadTrend(silent) {
        const updated = document.getElementById('sfTrendUpdated');
        const msg = document.getElementById('sfTrendMsg');
        try {
            const data = await fetchDailyJson();
            renderGroups(data);
            if (updated) updated.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        } catch (e) {
            if (!silent && msg) msg.textContent = String(e.message || e);
            const host = document.getElementById('sfTrendGroups');
            if (host)
                host.innerHTML =
                    '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="10" style="padding:14px;">No Record</td></tr></tbody></table></div>';
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const ref = document.getElementById('sfTrendRefresh');
        if (ref) ref.addEventListener('click', function () { loadTrend(false); });
        loadTrend(false);
    });
})();
