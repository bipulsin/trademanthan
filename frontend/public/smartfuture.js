/**
 * Smart Futures — Today's Trend + Open Positions from smart_futures_daily.
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

    function isTokenExpiredResponse(res, payloadText, payloadJson) {
        if (res && Number(res.status) === 401) return true;
        const detail =
            (payloadJson && (payloadJson.detail || payloadJson.message)) ||
            payloadText ||
            '';
        return /token\s+expired/i.test(String(detail));
    }

    function redirectToLoginExpired() {
        try {
            localStorage.removeItem('trademanthan_token');
            sessionStorage.setItem('auth_redirect_reason', 'Your session expired. Please sign in again.');
        } catch (e) { /* ignore */ }
        window.location.replace('index.html');
    }

    async function fetchDailyJson() {
        const paths = ['/api/smart-futures/daily', '/smart-futures/daily'];
        let lastErr = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { headers: authHeaders(), cache: 'no-store' });
                const raw = await res.text();
                const ct = (res.headers.get('content-type') || '').toLowerCase();
                const looksJson =
                    ct.includes('application/json') || /^\s*[\[{]/.test(raw.slice(0, 20));
                if (res.ok) {
                    if (!looksJson) {
                        lastErr = new Error(
                            'Server returned non-JSON (often HTML). Sign in or ask ops to proxy /smart-futures/ to the API.'
                        );
                        continue;
                    }
                    try {
                        return JSON.parse(raw);
                    } catch (parseErr) {
                        lastErr = new Error('Invalid JSON from ' + p + ': ' + (parseErr.message || parseErr));
                        continue;
                    }
                }
                if (isTokenExpiredResponse(res, raw, null)) {
                    redirectToLoginExpired();
                    throw new Error('Session expired');
                }
                lastErr = new Error(raw.slice(0, 200) || res.statusText || String(res.status));
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load daily picks');
    }

    function flattenRows(data) {
        if (data.rows && data.rows.length) return data.rows;
        const out = [];
        (data.groups || []).forEach(function (g) {
            (g.rows || []).forEach(function (r) {
                out.push(r);
            });
        });
        return out;
    }

    function fmtNum(v, d) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d);
    }

    function fmtSellTime(iso) {
        if (!iso) return '';
        try {
            const d = new Date(iso);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'short', timeStyle: 'medium' });
            }
        } catch (e) { /* ignore */ }
        return String(iso);
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

    function sideTooltip(r) {
        const s = String(r.side || '').trim().toUpperCase();
        const fc = Number(r.final_cms);
        const ss = Number(r.sector_score);
        const cs = Number(r.combined_sentiment);
        const core = r.cms != null && r.cms !== '' ? Number(r.cms) : NaN;
        const lines = [];
        lines.push(
            'CMS v2: regime filter (ATR5>ATR14, ADX>20 rising), normalized score, then Final CMS = score × (ATR5/ATR14) × (ADX/25). ' +
                'Entry needs Final CMS beyond threshold, price vs session VWAP, sector alignment, and sentiment band. ' +
                'Divergences are not used for entry; they feed exit hints after you mark Bought.'
        );
        const bits = [];
        if (Number.isFinite(core)) bits.push('core CMS ' + core.toFixed(2));
        if (Number.isFinite(ss)) bits.push('sector ' + ss.toFixed(2));
        if (Number.isFinite(cs)) bits.push('sentiment ' + cs.toFixed(3));
        if (Number.isFinite(fc)) bits.push('final CMS ' + fc.toFixed(2));
        if (bits.length) lines.push('This row: ' + bits.join(' · ') + '.');
        if (s === 'LONG' || s === 'SHORT') {
            lines.push('Side: ' + s + ' (meets entry rules at scan time).');
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

    /** First section: status text for bought/sold — no Sell here. */
    function fmtTrendActionCell(r) {
        const ost = String(r.order_status || '').trim().toLowerCase();
        const rawStatus = String(r.order_status || '').trim();
        const displayStatus = rawStatus ? escapeHtml(rawStatus) : '—';
        const exitS = Boolean(r.exit_suggested);
        const reason = escapeAttr(String(r.exit_reason || ''));

        if (ost === 'sold') {
            const sp = r.sell_price != null && r.sell_price !== '' ? ' @' + fmtNum(r.sell_price, 2) : '';
            const st = r.sell_time ? ' · ' + escapeHtml(fmtSellTime(r.sell_time)) : '';
            return '<span class="sf-sold">Sold' + sp + st + '</span>';
        }
        if (ost === 'bought') {
            let hint = '';
            if (exitS) {
                hint =
                    '<span class="sf-exit-hint" title="' +
                    (reason || 'Exit signal (algo)') +
                    '">Exit</span> ';
            }
            return hint + '<span class="sf-order-status">' + displayStatus + '</span>';
        }
        return (
            '<button type="button" class="sf-btn-order" data-order-id="' +
            r.id +
            '">Order</button>'
        );
    }

    /** Open Positions: Sell only when exit_suggested; blink when enabled. */
    function fmtOpenActionCell(r) {
        const id = r.id;
        const exitOk = Boolean(r.exit_suggested);
        const reason = escapeAttr(String(r.exit_reason || ''));
        const dis = exitOk ? '' : ' disabled';
        const blink = exitOk ? ' sf-btn-sell--blink' : '';
        const title = exitOk
            ? 'Square off at LTP — exit signal is active'
            : 'Disabled until the algo signals exit (see Today\'s Trend Exit hint)';
        return (
            '<button type="button" class="sf-btn-sell' +
            blink +
            '" data-sell-id="' +
            id +
            '" data-open-sell="1"' +
            dis +
            ' title="' +
            title +
            (reason && exitOk ? ' — ' + reason : '') +
            '">Sell</button>'
        );
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
        const fire =
            hot ? '<span class="sf-atr-fire" aria-hidden="true">🔥</span> ' : '';
        if (tip) {
            return '<span class="sf-symbol-wrap"' + titleAttr + '>' + fire + label + '</span>';
        }
        return fire ? fire + label : label;
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

    function trendTableRowHtml(r) {
        return (
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
            '<td>' +
            fmtTrendActionCell(r) +
            '</td>' +
            '</tr>'
        );
    }

    function openTableRowHtml(r) {
        return (
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
            '<td>' +
            fmtOpenActionCell(r) +
            '</td>' +
            '</tr>'
        );
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
            const sd = escapeHtml(String(data.session_date || '—'));
            const hint =
                'No picks for session ' +
                sd +
                '. On market days the scanner runs from 9:15 IST; if markets are closed or no symbol passes CMS filters, this list stays empty.';
            host.innerHTML =
                '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="10" style="padding:14px;">No Record</td></tr></tbody></table></div>' +
                '<p class="sf-meta" style="margin-top:10px;max-width:42rem;">' +
                hint +
                '</p>';
            return;
        }

        const thead =
            '<thead><tr>' +
            '<th>Symbol</th><th>Side</th><th>Final CMS</th><th>Sector Score</th><th>Sentiment Score</th>' +
            '<th>Entry</th><th>SL</th><th>Target</th><th>In Trend</th><th>Status</th>' +
            '</tr></thead>';

        let html = '';
        groups.forEach(function (g) {
            const label = fmtEntryGroupLabel(g.entry_at);
            html += '<div class="sf-group-title">' + label + '</div>';
            html += '<div class="sf-table-wrap"><table class="sf-table">' + thead + '<tbody>';
            (g.rows || []).forEach(function (r) {
                html += trendTableRowHtml(r);
            });
            html += '</tbody></table></div>';
        });
        host.innerHTML = html;

        host.onclick = function (ev) {
            const ob = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-order') : null;
            if (ob) onOrderClick(ev);
        };
    }

    function renderOpenPositions(data) {
        const host = document.getElementById('sfOpenPositions');
        const msg = document.getElementById('sfOpenMsg');
        if (!host) return;

        const all = flattenRows(data);
        const bought = all.filter(function (r) {
            return String(r.order_status || '').trim().toLowerCase() === 'bought';
        });
        bought.sort(function (a, b) {
            const ea = String(a.entry_at || '');
            const eb = String(b.entry_at || '');
            return eb.localeCompare(ea);
        });

        if (msg) msg.textContent = '';

        if (!bought.length) {
            host.innerHTML =
                '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="10" style="padding:14px;">No open positions</td></tr></tbody></table></div>';
            return;
        }

        const thead =
            '<thead><tr>' +
            '<th>Symbol</th><th>Side</th><th>Final CMS</th><th>Sector Score</th><th>Sentiment Score</th>' +
            '<th>Entry</th><th>SL</th><th>Target</th><th>In Trend</th><th>Action</th>' +
            '</tr></thead>';
        let body = '';
        bought.forEach(function (r) {
            body += openTableRowHtml(r);
        });
        host.innerHTML =
            '<div class="sf-table-wrap"><table class="sf-table">' + thead + '<tbody>' + body + '</tbody></table></div>';

        host.onclick = function (ev) {
            const sb = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-sell[data-open-sell]') : null;
            if (sb) onSellClick(ev);
        };
    }

    async function onOrderClick(ev) {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-order') : null;
        if (!btn) return;
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
                const raw = (await res.text()) || res.statusText;
                let data = null;
                try {
                    data = JSON.parse(raw);
                } catch (e) {
                    data = null;
                }
                if (isTokenExpiredResponse(res, raw, data)) {
                    redirectToLoginExpired();
                    return;
                }
                errText = (data && data.detail) || raw || res.statusText;
            } catch (e) {
                errText = String(e.message || e);
            }
        }
        if (!ok) {
            alert(errText || 'Order failed');
            btn.disabled = false;
            return;
        }
        await loadTrend(true);
    }

    async function onSellClick(ev) {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-sell[data-open-sell]') : null;
        if (!btn) return;
        if (btn.disabled) return;
        const id = btn.getAttribute('data-sell-id');
        if (!id) return;
        if (!window.confirm('Mark this position as sold at current LTP?')) return;
        btn.disabled = true;
        const paths = ['/api/smart-futures/daily/' + id + '/sell', '/smart-futures/daily/' + id + '/sell'];
        let ok = false;
        let errText = '';
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { method: 'POST', headers: authHeaders() });
                const raw = await res.text();
                let data = null;
                try {
                    data = JSON.parse(raw);
                } catch (e) {
                    data = null;
                }
                if (res.ok && data && data.success) {
                    ok = true;
                    break;
                }
                if (isTokenExpiredResponse(res, raw, data)) {
                    redirectToLoginExpired();
                    return;
                }
                errText = (data && data.detail) || raw || res.statusText;
            } catch (e) {
                errText = String(e.message || e);
            }
        }
        if (!ok) {
            alert(errText || 'Sell failed');
            btn.disabled = false;
            return;
        }
        await loadTrend(true);
    }

    async function loadTrend(silent) {
        const updated = document.getElementById('sfTrendUpdated');
        const msg = document.getElementById('sfTrendMsg');
        try {
            const data = await fetchDailyJson();
            renderGroups(data);
            renderOpenPositions(data);
            if (updated) updated.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        } catch (e) {
            if (!silent && msg) msg.textContent = String(e.message || e);
            const host = document.getElementById('sfTrendGroups');
            if (host) {
                host.innerHTML =
                    '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="10" style="padding:14px;">No Record</td></tr></tbody></table></div>';
            }
            const openHost = document.getElementById('sfOpenPositions');
            if (openHost) openHost.innerHTML = '';
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const ref = document.getElementById('sfTrendRefresh');
        if (ref) ref.addEventListener('click', function () { loadTrend(false); });
        loadTrend(false);
    });
})();
