/**
 * Smart Futures backtest viewer — all rows from backtest_smart_future (no session filter).
 * Open directly: /backtestsmart.html (not linked from the main menu).
 */
(function () {
    const MIN_BACKTEST = '2026-02-01';

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

    /** Same rule as left-menu.js: DB isAdmin "Yes" → API isAdmin / is_admin. */
    function isUserAdmin(user) {
        if (!user || typeof user !== 'object') return false;
        const raw = user.isAdmin != null ? user.isAdmin : user.is_admin;
        if (raw == null || raw === '') return false;
        return String(raw).trim().toLowerCase() === 'yes';
    }

    async function refreshUserFromMe() {
        const paths = [API_BASE + '/api/auth/me', API_BASE + '/auth/me'];
        for (let i = 0; i < paths.length; i++) {
            try {
                const res = await fetch(paths[i], { headers: authHeaders(), cache: 'no-store' });
                if (res.ok) {
                    const me = await res.json();
                    try {
                        const prev = JSON.parse(localStorage.getItem('trademanthan_user') || '{}');
                        localStorage.setItem('trademanthan_user', JSON.stringify(Object.assign({}, prev, me)));
                    } catch (e) {
                        localStorage.setItem('trademanthan_user', JSON.stringify(me));
                    }
                    return me;
                }
            } catch (e) {
                /* try next path */
            }
        }
        return null;
    }

    function readStoredUser() {
        try {
            return JSON.parse(localStorage.getItem('trademanthan_user') || '{}');
        } catch (e) {
            return {};
        }
    }

    function showAdminRunPanelIfEligible() {
        const panel = document.getElementById('btAdminRunPanel');
        if (!panel) return;
        const user = readStoredUser();
        if (!isUserAdmin(user)) {
            panel.style.display = 'none';
            panel.setAttribute('aria-hidden', 'true');
            return;
        }
        panel.style.display = 'block';
        panel.setAttribute('aria-hidden', 'false');
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

    function parseTimesInput(raw) {
        return String(raw || '')
            .split(',')
            .map(function (s) {
                return s.trim();
            })
            .filter(Boolean);
    }

    async function postRunBacktest() {
        const fromEl = document.getElementById('btRunFrom');
        const toEl = document.getElementById('btRunTo');
        const timesEl = document.getElementById('btRunTimes');
        const btn = document.getElementById('btRunBtn');
        const statusEl = document.getElementById('btRunStatus');
        if (!fromEl || !toEl || !timesEl || !btn) return;
        const from_date = (fromEl.value || '').trim();
        const to_date = (toEl.value || '').trim();
        const times = parseTimesInput(timesEl.value);
        if (!from_date || !to_date) {
            if (statusEl) statusEl.textContent = 'Choose from and to dates.';
            return;
        }
        if (from_date > to_date) {
            if (statusEl) statusEl.textContent = 'From date must be on or before to date.';
            return;
        }
        if (from_date < MIN_BACKTEST || to_date < MIN_BACKTEST) {
            if (statusEl) {
                statusEl.textContent =
                    'Backtest dates must be on or after ' + MIN_BACKTEST +
                    ' (Feb–Mar 2026 uses April 2026 futures from instruments; Apr+ uses arbitrage_master currmth).';
            }
            return;
        }
        if (!times.length) {
            if (statusEl) statusEl.textContent = 'Enter at least one scan time (IST), e.g. 09:30,10:30.';
            return;
        }
        for (let i = 0; i < times.length; i++) {
            if (!/^\d{1,2}:\d{2}$/.test(times[i])) {
                if (statusEl) statusEl.textContent = 'Invalid time format: use HH:MM like 09:30.';
                return;
            }
        }
        const body = JSON.stringify({ from_date: from_date, to_date: to_date, times: times });
        const paths = ['/api/smart-futures-backtest/run', '/smart-futures-backtest/run'];
        btn.disabled = true;
        if (statusEl) statusEl.textContent = 'Running… this may take several minutes. Do not close the tab.';
        let lastErr = null;
        let ok = false;
        let payload = null;
        for (let p = 0; p < paths.length; p++) {
            try {
                const res = await fetch(API_BASE + paths[p], {
                    method: 'POST',
                    headers: authHeaders(),
                    body: body,
                });
                const text = await res.text();
                try {
                    payload = text ? JSON.parse(text) : {};
                } catch (e) {
                    payload = { detail: text || res.statusText };
                }
                if (res.status === 403) {
                    if (statusEl) statusEl.textContent = 'Administrator only (403).';
                    lastErr = '403';
                    break;
                }
                if (res.ok) {
                    ok = true;
                    break;
                }
                lastErr = (payload && payload.detail) || text || res.statusText;
            } catch (e) {
                lastErr = String(e.message || e);
            }
        }
        btn.disabled = false;
        if (ok && payload) {
            const okSlots = payload.ok_slots != null ? payload.ok_slots : '—';
            const total = payload.total_slots != null ? payload.total_slots : '—';
            if (statusEl) statusEl.textContent = 'Done. Slots completed: ' + okSlots + ' / ' + total + '.';
            await load(true);
        } else {
            if (statusEl) statusEl.textContent = lastErr ? String(lastErr) : 'Run failed.';
        }
    }

    document.addEventListener('DOMContentLoaded', async function () {
        const ref = document.getElementById('btRefresh');
        if (ref) ref.addEventListener('click', function () { load(false); });
        await refreshUserFromMe();
        showAdminRunPanelIfEligible();
        const runBtn = document.getElementById('btRunBtn');
        if (runBtn) runBtn.addEventListener('click', function () { postRunBacktest(); });
        load(false);
    });
})();
