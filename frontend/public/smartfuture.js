/**
 * Smart Futures dashboard — polls /api/smart-futures/dashboard every 60s.
 */
(function () {
    // Mirror domains (tradewithcto / tradentical) often serve static files only; API lives on trademanthan.in.
    const API_BASE_URL = (() => {
        const h = window.location.hostname;
        if (h === 'localhost' || h === '127.0.0.1') return 'http://localhost:8000';
        if (
            h === 'www.tradewithcto.com' ||
            h === 'tradewithcto.com' ||
            h.endsWith('.tradewithcto.com') ||
            h === 'www.tradentical.com' ||
            h === 'tradentical.com' ||
            h.endsWith('.tradentical.com')
        ) {
            return 'https://trademanthan.in';
        }
        return window.location.origin;
    })();

    const FETCH_TIMEOUT_MS = 45000;

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
            let res = await fetch(`${API_BASE_URL}${path}`, opts);
            if (!res.ok && path.startsWith('/api/')) {
                res = await fetch(`${API_BASE_URL}${path.replace(/^\/api/, '')}`, opts);
            }
            return res;
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

    function renderCandidates(rows, live) {
        const tbody = el('sfCandBody');
        if (!tbody) return;
        if (!rows || !rows.length) {
            tbody.innerHTML =
                '<tr><td colspan="7" style="padding:12px;">No candidates yet. Scheduler runs every 5 minutes during market hours.</td></tr>';
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
            if (!res.ok) {
                setStatus('Failed to load (' + res.status + ')');
                return;
            }
            const data = await res.json();
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
            renderCandidates(cand, live);
            renderPositions(data.positions || [], live);
            const up = el('sfUpdated');
            if (up) up.textContent = new Date().toLocaleString('en-IN');
            setStatus('');
        } catch (e) {
            console.error(e);
            if (e && e.name === 'AbortError') {
                setStatus('Timed out — check network or try refreshing.');
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
