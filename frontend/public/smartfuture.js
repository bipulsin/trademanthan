/**
 * Smart Futures: (1) DB top-3 only, (2) config + positions (DB), (3) broker LTP patch.
 */
(function () {
    const API_BASE_URL =
        window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : window.location.origin;

    /** Per-request cap; broker quote batch can be slow on congested networks. */
    const FETCH_TIMEOUT_MS = 60000;

    /** Set from /api/smart-futures/config after load (not shown in UI). */
    let uiLiveEnabled = false;

    let topLoadChain = Promise.resolve();
    let posLoadChain = Promise.resolve();

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

    function showCandidateTableError(message) {
        const top = el('sfCandBody');
        if (top) top.innerHTML = `<tr><td colspan="7" style="padding:12px;">${escapeHtml(message)}</td></tr>`;
    }

    function showCandidateTableLoading() {
        const top = el('sfCandBody');
        if (top) top.innerHTML = '<tr><td colspan="7" style="padding:12px;">Loading…</td></tr>';
    }

    function showPositionsError(message) {
        const tbody = el('sfPosBody');
        if (tbody) {
            tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;">${escapeHtml(message)}</td></tr>`;
        }
    }

    function showPositionsLoading() {
        const tbody = el('sfPosBody');
        if (tbody) {
            tbody.innerHTML = '<tr><td colspan="5" style="padding:12px;">Loading…</td></tr>';
        }
    }

    function normIk(s) {
        return String(s || '')
            .replace(/\s/g, '')
            .replace(/:/g, '|')
            .toUpperCase();
    }

    function patchLtpCells(ltps) {
        if (!ltps || typeof ltps !== 'object') return;
        const map = {};
        Object.keys(ltps).forEach((k) => {
            map[normIk(k)] = ltps[k];
        });
        document.querySelectorAll('td.sf-ltp').forEach((td) => {
            const ik = td.getAttribute('data-ik');
            if (!ik) return;
            let v = ltps[ik];
            if (v == null || Number.isNaN(Number(v))) {
                v = map[normIk(ik)];
            }
            if (v != null && !Number.isNaN(Number(v))) {
                td.textContent = Number(v).toFixed(2);
            }
        });
    }

    function applyLiveToOrderButtons(live) {
        document.querySelectorAll('.sf-btn-order').forEach((btn) => {
            const entry = btn.getAttribute('data-entry') === '1';
            btn.disabled = !live || !entry;
        });
    }

    function renderCandidates(tbodyId, rows, emptyMsg) {
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
                const orderDisabled = !uiLiveEnabled || !r.entry_signal;
                const dir = r.direction || '—';
                const brick = r.last_brick_color || '—';
                const sym = (r.symbol || '').replace(/</g, '&lt;');
                const ik = (r.instrument_key || '').replace(/"/g, '');
                const ltpDisp = r.ltp != null ? Number(r.ltp).toFixed(2) : '—';
                return `<tr>
                    <td style="padding:8px;font-weight:600;">${sym}</td>
                    <td style="padding:8px;">${r.score ?? 0}</td>
                    <td class="sf-ltp" data-ik="${escapeHtml(ik)}" style="padding:8px;">${ltpDisp}</td>
                    <td style="padding:8px;">${brick}</td>
                    <td style="padding:8px;">${dir}</td>
                    <td style="padding:8px;">
                        <button type="button" class="sf-btn-order" data-ik="${ik.replace(/"/g, '')}" data-dir="${dir}" data-sym="${sym}" data-entry="${r.entry_signal ? '1' : '0'}"
                          ${orderDisabled ? 'disabled' : ''}>Order</button>
                    </td>
                    <td style="padding:8px;">—</td>
                </tr>`;
            })
            .join('');

        tbody.querySelectorAll('.sf-btn-order').forEach((btn) => {
            btn.addEventListener('click', async () => {
                if (!uiLiveEnabled) {
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
                if (!uiLiveEnabled) {
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

    function httpErrDetail(data, raw) {
        if (data && typeof data.detail === 'string') return data.detail;
        if (data && data.detail && typeof data.detail === 'object') return JSON.stringify(data.detail);
        if (data && typeof data.message === 'string') return data.message;
        return raw.slice(0, 160).replace(/\s+/g, ' ');
    }

    function touchUpdated(spanId) {
        const up = el(spanId);
        if (up) up.textContent = new Date().toLocaleString('en-IN');
    }

    async function loadSectionTop() {
        const btn = el('sfRefreshTop');
        if (btn) btn.disabled = true;
        setStatus('Loading top…');
        showCandidateTableLoading();

        try {
            const [resTop, rCfg] = await Promise.all([
                apiGet('/api/smart-futures/dashboard/top'),
                apiGet('/api/smart-futures/config'),
            ]);
            const rawTop = await resTop.text();
            let dataTop;
            try {
                dataTop = rawTop ? JSON.parse(rawTop) : {};
            } catch (parseErr) {
                const snippet = rawTop.slice(0, 200).replace(/\s+/g, ' ');
                showCandidateTableError(
                    'Invalid response (not JSON). ' + (snippet ? snippet : 'Empty body.')
                );
                setStatus('Bad response (top)');
                return;
            }
            if (!resTop.ok) {
                const errLine =
                    'Failed to load top list (HTTP ' + resTop.status + '). ' + httpErrDetail(dataTop, rawTop);
                showCandidateTableError(errLine);
                setStatus('Failed (top)');
                return;
            }

            let cfg = {};
            const rawCfg = await rCfg.text();
            try {
                cfg = rawCfg ? JSON.parse(rawCfg) : {};
            } catch (e) {
                cfg = {};
            }
            uiLiveEnabled = !!cfg.live_enabled;

            renderCandidates(
                'sfCandBody',
                dataTop.candidates || [],
                'No top candidates with score &#8805; 4 for this session yet.'
            );
            applyLiveToOrderButtons(uiLiveEnabled);
            touchUpdated('sfUpdatedTop');

            setStatus('Refreshing LTP…');
            try {
                const rLtp = await apiGet('/api/smart-futures/dashboard/live-quotes');
                const rawLtp = await rLtp.text();
                let j = {};
                try {
                    j = rawLtp ? JSON.parse(rawLtp) : {};
                } catch (e) {
                    j = {};
                }
                if (rLtp.ok && j.ltps) {
                    patchLtpCells(j.ltps);
                }
            } catch (e) {
                /* keep DB LTP */
            }
            setStatus('');
        } catch (e) {
            console.error(e);
            const net =
                e && e.name === 'AbortError'
                    ? 'Request timed out — check network or try again.'
                    : e && e.message
                      ? 'Network error: ' + e.message
                      : 'Could not reach the API.';
            showCandidateTableError(net);
            setStatus(e && e.name === 'AbortError' ? 'Timed out' : 'Error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async function loadSectionPositions() {
        const btn = el('sfRefreshPositions');
        if (btn) btn.disabled = true;
        setStatus('Loading positions…');
        showPositionsLoading();

        try {
            const [rCfg, rPos] = await Promise.all([
                apiGet('/api/smart-futures/config'),
                apiGet('/api/smart-futures/dashboard/positions'),
            ]);
            const rawCfg = await rCfg.text();
            const rawPos = await rPos.text();
            let cfg = {};
            let dataPos = {};
            try {
                cfg = rawCfg ? JSON.parse(rawCfg) : {};
            } catch (e) {
                cfg = {};
            }
            try {
                dataPos = rawPos ? JSON.parse(rawPos) : {};
            } catch (e) {
                dataPos = {};
            }

            uiLiveEnabled = !!cfg.live_enabled;

            if (!rPos.ok) {
                showPositionsError(
                    'Failed to load positions (HTTP ' + rPos.status + '). ' + httpErrDetail(dataPos, rawPos)
                );
                setStatus('Failed (positions)');
                return;
            }
            renderPositions(dataPos.positions || [], uiLiveEnabled);
            applyLiveToOrderButtons(uiLiveEnabled);
            touchUpdated('sfUpdatedPos');
            setStatus('');
        } catch (e) {
            console.error(e);
            const net =
                e && e.name === 'AbortError'
                    ? 'Request timed out — check network or try again.'
                    : e && e.message
                      ? 'Network error: ' + e.message
                      : 'Could not reach the API.';
            showPositionsError(net);
            setStatus(e && e.name === 'AbortError' ? 'Timed out' : 'Error');
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    function loadDashboard() {
        return Promise.all([
            (topLoadChain = topLoadChain.catch(() => {}).then(() => loadSectionTop())),
            (posLoadChain = posLoadChain.catch(() => {}).then(() => loadSectionPositions())),
        ]);
    }

    window.addEventListener('DOMContentLoaded', () => {
        const bTop = el('sfRefreshTop');
        const bPos = el('sfRefreshPositions');
        if (bTop) {
            bTop.addEventListener('click', () => {
                topLoadChain = topLoadChain.catch(() => {}).then(() => loadSectionTop());
            });
        }
        if (bPos) {
            bPos.addEventListener('click', () => {
                posLoadChain = posLoadChain.catch(() => {}).then(() => loadSectionPositions());
            });
        }
        loadDashboard();
    });
})();
