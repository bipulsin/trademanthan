/**
 * Smart Futures dashboard — polls /api/smart-futures/dashboard every 60s.
 */
(function () {
    // Same-origin /api on the site you are on (avoids cross-origin "Failed to fetch" to another domain).
    // Requires nginx proxy_pass to preserve /api — see scripts/nginx-tradentical.conf.
    const API_BASE_URL =
        window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : window.location.origin;

    // Dashboard can wait on DB pool / cold workers; 45s was aborting too often in production.
    const FETCH_TIMEOUT_MS = 120000;

    const REFRESH_MS = 60000;

    function getToken() {
        return localStorage.getItem('trademanthan_token') || '';
    }

    async function apiGet(path) {
        const token = getToken();
        const headers = { Authorization: `Bearer ${token}` };
        const ctrl = new AbortController();
        const tid = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
        const opts = { headers, cache: 'no-store', signal: ctrl.signal };
        try {
            return await fetch(`${API_BASE_URL}${path}`, opts);
        } finally {
            clearTimeout(tid);
        }
    }

    async function apiPost(path, body) {
        const token = getToken();
        const headers = {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
        };
        const ctrl = new AbortController();
        const tid = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
        try {
            return await fetch(`${API_BASE_URL}${path}`, {
                method: 'POST',
                headers,
                body: JSON.stringify(body || {}),
                signal: ctrl.signal,
            });
        } finally {
            clearTimeout(tid);
        }
    }

    function el(id) {
        return document.getElementById(id);
    }

    function setStatus(msg) {
        const s = el('sfStatus');
        if (s) s.textContent = msg;
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    /** Clears "Loading…" in both candidate tables when the dashboard request fails. */
    function showCandidateTablesError(message) {
        const msg = escapeHtml(message);
        const row = `<tr><td colspan="7" style="padding:12px;">${msg}</td></tr>`;
        const top = el('sfCandBody');
        const recent = el('sfCandBodyRecent');
        if (top) top.innerHTML = row;
        if (recent) recent.innerHTML = row;
    }

    function renderCandidates(tbodyId, rows, live, emptyMsg) {
        const tbody = el(tbodyId);
        if (!tbody) return;
        if (!rows || !rows.length) {
            tbody.innerHTML =
                '<tr><td colspan="7" style="padding:12px;">' +
                (emptyMsg ||
                    'No rows with score &#8805; 4 for this session yet. Scheduler runs every 5 minutes during market hours.') +
                '</td></tr>';
            return;
        }
        tbody.innerHTML = rows
            .map((r) => {
                const orderDisabled = !live || !r.entry_signal;
                const dir = r.direction || '—';
                const brick = r.last_brick_color || '—';
                const sym = (r.symbol || '').replace(/</g, '&lt;');
                return `<tr>
                    <td style="padding:8px;font-weight:600;">${sym}</td>
                    <td style="padding:8px;">${r.score ?? 0}</td>
                    <td style="padding:8px;">${r.ltp != null ? Number(r.ltp).toFixed(2) : '—'}</td>
                    <td style="padding:8px;">${brick}</td>
                    <td style="padding:8px;">${dir}</td>
                    <td style="padding:8px;">
                        <button type="button" class="sf-btn-order" data-ik="${(r.instrument_key || '').replace(/"/g, '')}" data-dir="${dir}" data-sym="${sym}"
                          ${orderDisabled ? 'disabled' : ''}>Order</button>
                    </td>
                    <td style="padding:8px;">—</td>
                </tr>`;
            })
            .join('');

        tbody.querySelectorAll('.sf-btn-order').forEach((btn) => {
            btn.addEventListener('click', async () => {
                if (!live) {
                    alert('Live trading is off. Ask admin to enable Live in Admin → Smart Futures.');
                    return;
                }
                const ik = btn.getAttribute('data-ik');
                const dr = btn.getAttribute('data-dir');
                const sym = btn.getAttribute('data-sym');
                if (!ik || (dr !== 'LONG' && dr !== 'SHORT')) return;
                if (!confirm(`Place ${dr} order for ${sym}?`)) return;
                setStatus('Placing order…');
                const res = await apiPost('/api/smart-futures/order', {
                    instrument_key: ik,
                    direction: dr,
                    symbol: sym,
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) {
                    alert(data.detail || data.error || 'Order failed');
                    setStatus('Order failed');
                    return;
                }
                setStatus('Order placed');
                loadDashboard();
            });
        });
    }

    function renderPositions(positions, live) {
        const tbody = el('sfPosBody');
        if (!tbody) return;
        if (!positions || !positions.length) {
            tbody.innerHTML = '<tr><td colspan="5" style="padding:12px;">No open positions.</td></tr>';
            return;
        }
        tbody.innerHTML = positions
            .map((p) => {
                const ex = !!p.exit_ready;
                const exitDis = !live || !ex;
                return `<tr>
                    <td style="padding:8px;">${(p.symbol || '').replace(/</g, '&lt;')}</td>
                    <td style="padding:8px;">${p.direction || '—'}</td>
                    <td style="padding:8px;">${p.lots_open ?? 0}</td>
                    <td style="padding:8px;">${ex ? 'Yes' : 'No'}</td>
                    <td style="padding:8px;">
                        <button type="button" class="sf-btn-exit" data-pid="${p.id}" ${exitDis ? 'disabled' : ''}>Exit</button>
                    </td>
                </tr>`;
            })
            .join('');

        tbody.querySelectorAll('.sf-btn-exit').forEach((btn) => {
            btn.addEventListener('click', async () => {
                if (!live) {
                    alert('Live trading is off.');
                    return;
                }
                const pid = btn.getAttribute('data-pid');
                if (!confirm('Square off this position?')) return;
                setStatus('Exiting…');
                const res = await apiPost('/api/smart-futures/exit', { position_id: parseInt(pid, 10) });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) {
                    alert(data.detail || data.error || 'Exit failed');
                    setStatus('Exit failed');
                    return;
                }
                setStatus('Exit sent');
                loadDashboard();
            });
        });
    }

    async function loadDashboard() {
        setStatus('Loading…');
        try {
            const res = await apiGet('/api/smart-futures/dashboard');
            const raw = await res.text();
            let data;
            try {
                data = raw ? JSON.parse(raw) : {};
            } catch (parseErr) {
                const snippet = raw.slice(0, 200).replace(/\s+/g, ' ');
                showCandidateTablesError(
                    'Invalid response (not JSON). ' + (snippet ? snippet : 'Empty body.')
                );
                setStatus('Bad response (' + res.status + ')');
                return;
            }
            if (!res.ok) {
                const detail =
                    data && typeof data.detail === 'string'
                        ? data.detail
                        : data && data.detail && typeof data.detail === 'object'
                          ? JSON.stringify(data.detail)
                          : data && typeof data.message === 'string'
                            ? data.message
                            : '';
                const errLine =
                    'Failed to load dashboard (HTTP ' +
                    res.status +
                    '). ' +
                    (detail ? detail : raw.slice(0, 160).replace(/\s+/g, ' '));
                showCandidateTablesError(errLine);
                setStatus('Failed (' + res.status + ')');
                return;
            }
            const cfg = data.config || {};
            const live = !!cfg.live_enabled;
            const liveEl = el('sfLiveFlag');
            if (liveEl) liveEl.textContent = live ? 'Yes' : 'No';
            const psEl = el('sfPosSize');
            if (psEl) psEl.textContent = String(cfg.position_size ?? 1);
            const atrEl = el('sfAtrMeta');
            if (atrEl) {
                const p = cfg.brick_atr_period != null ? cfg.brick_atr_period : 10;
                const o = cfg.brick_atr_override;
                atrEl.textContent =
                    o != null && o !== '' && Number(o) > 0
                        ? `Fixed brick: ${Number(o).toFixed(4)}`
                        : `ATR(${p}) on 1h — auto`;
            }
            const cand = data.candidates || [];
            const recent = data.candidates_recent || [];
            renderCandidates(
                'sfCandBody',
                cand,
                live,
                'No top candidates with score &#8805; 4 for this session yet.'
            );
            renderCandidates(
                'sfCandBodyRecent',
                recent,
                live,
                'No recently updated rows with score &#8805; 4 (or same as top list).'
            );
            renderPositions(data.positions || [], live);
            const up = el('sfUpdated');
            if (up) up.textContent = new Date().toLocaleString('en-IN');
            setStatus('');
        } catch (e) {
            console.error(e);
            const net =
                e && e.name === 'AbortError'
                    ? 'Request timed out — check network or try refreshing.'
                    : e && e.message
                      ? 'Network error: ' + e.message
                      : 'Could not reach the API.';
            showCandidateTablesError(net);
            if (e && e.name === 'AbortError') {
                setStatus('Timed out');
            } else {
                setStatus('Error');
            }
        }
    }

    window.addEventListener('DOMContentLoaded', () => {
        loadDashboard();
        setInterval(loadDashboard, REFRESH_MS);
    });
})();
