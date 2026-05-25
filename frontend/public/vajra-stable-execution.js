/**
 * Vajra Stable Execution Mode — UI controls (overlay on dynamic scanner).
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

    async function api(path, opts) {
        opts = opts || {};
        const paths = [API_BASE + '/api/vajra-futures' + path, API_BASE + '/vajra-futures' + path];
        let lastErr = null;
        for (let i = 0; i < paths.length; i++) {
            try {
                const res = await fetch(paths[i], {
                    method: opts.method || 'GET',
                    headers: authHeaders(),
                    body: opts.body ? JSON.stringify(opts.body) : undefined,
                });
                const data = await res.json().catch(function () {
                    return {};
                });
                if (!res.ok) throw new Error(data.message || res.statusText);
                return data;
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Stable execution API failed');
    }

    function $(id) {
        return document.getElementById(id);
    }

    let _stable = {
        stable_mode_enabled: true,
        focus_mode_enabled: false,
        sticky_persist_minutes: 30,
        freeze_window_open: false,
        watchlist_frozen: false,
        frozen_focus_stocks: [],
        sticky_top3: [],
        suggested_rotations: [],
        attention_banner: null,
    };

    function syncFromPayload(data) {
        const se = (data && data.stable_execution) || {};
        _stable = Object.assign(_stable, se);
        if (se.workflow_notice && !_stable.attention_banner) {
            _stable.attention_banner = se.workflow_notice;
        }
        global._vajraStableExecution = _stable;
        updateControlChrome();
    }

    function updateControlChrome() {
        const bar = $('vajraStableBar');
        if (!bar) return;
        bar.classList.toggle('vajra-stable-bar--on', !!_stable.stable_mode_enabled);
        bar.classList.toggle('vajra-stable-bar--focus', !!_stable.focus_mode_enabled);
        const toggle = $('vajraStableModeToggle');
        const focus = $('vajraStableFocusToggle');
        const persist = $('vajraStickyPersist');
        const freezeBtn = $('vajraFreezeFocusBtn');
        if (toggle) toggle.checked = !!_stable.stable_mode_enabled;
        if (focus) focus.checked = !!_stable.focus_mode_enabled;
        if (persist) persist.value = String(_stable.sticky_persist_minutes || 30);
        if (freezeBtn) {
            freezeBtn.disabled = !_stable.freeze_window_open && !_stable.watchlist_frozen;
            freezeBtn.textContent = _stable.watchlist_frozen
                ? 'Watchlist frozen'
                : 'Freeze Top 3 focus';
        }
        const badge = $('vajraStableBadge');
        if (badge) {
            if (_stable.stable_mode_enabled) {
                badge.hidden = false;
                badge.textContent = _stable.watchlist_frozen
                    ? 'Sticky Top 3 · Watchlist frozen'
                    : 'Sticky Top 3 active';
            } else {
                badge.hidden = true;
            }
        }
    }

    async function savePrefs(partial) {
        await api('/stable-execution/state', {
            method: 'PUT',
            body: partial,
        });
        Object.assign(_stable, partial);
        global._vajraStableExecution = _stable;
        updateControlChrome();
        if (global.VajraFuturesRatings && global.VajraFuturesRatings.refresh) {
            await global.VajraFuturesRatings.refresh(true);
        }
    }

    async function freezeFromCurrentTop3() {
        const sticky = (_stable.sticky_top3 || []).map(function (r) {
            return String(r.stock || r.security || '').trim().toUpperCase();
        }).filter(Boolean);
        if (sticky.length < 1) {
            alert('Load ratings first — need Sticky Top 3 before freezing focus.');
            return;
        }
        await api('/stable-execution/freeze-focus', {
            method: 'POST',
            body: { stocks: sticky.slice(0, 3) },
        });
        if (global.VajraFuturesRatings && global.VajraFuturesRatings.refresh) {
            await global.VajraFuturesRatings.refresh(true);
        }
    }

    async function loadState() {
        try {
            const st = await api('/stable-execution/state');
            _stable.stable_mode_enabled = st.stable_mode_enabled !== false;
            _stable.focus_mode_enabled = !!st.focus_mode_enabled;
            _stable.sticky_persist_minutes = st.sticky_persist_minutes || 30;
            _stable.freeze_window_open = !!st.freeze_window_open;
            _stable.watchlist_frozen = !!(st.frozen_focus_stocks && st.frozen_focus_stocks.length);
            _stable.frozen_focus_stocks = st.frozen_focus_stocks || [];
            global._vajraStableExecution = _stable;
            updateControlChrome();
        } catch (e) {
            console.warn('Vajra stable state load', e);
        }
    }

    function ensureBar(prefix) {
        const card = document.getElementById(prefix + 'VajraCard');
        if (!card || document.getElementById('vajraStableBar')) return;
        const bar = document.createElement('div');
        bar.id = 'vajraStableBar';
        bar.className = 'vajra-stable-bar';
        bar.innerHTML =
            '<div class="vajra-stable-bar-row">' +
            '<label class="vajra-stable-toggle"><input type="checkbox" id="vajraStableModeToggle" checked> Execution Stability Mode</label>' +
            '<span id="vajraStableBadge" class="vajra-stable-badge">Sticky Top 3 active</span>' +
            '</div>' +
            '<div class="vajra-stable-bar-row vajra-stable-bar-row--sub">' +
            '<label class="vajra-stable-toggle"><input type="checkbox" id="vajraStableFocusToggle"> Focus Mode (Top 3 only)</label>' +
            '<label class="vajra-stable-persist">Sticky window' +
            '<select id="vajraStickyPersist"><option value="15">15 min</option><option value="30" selected>30 min</option><option value="60">60 min</option></select></label>' +
            '<button type="button" class="df-more-link" id="vajraFreezeFocusBtn">Freeze Top 3 focus</button>' +
            '</div>';
        const meta = document.getElementById(prefix + 'VajraMeta');
        if (meta && meta.parentNode) {
            meta.parentNode.insertBefore(bar, meta);
        } else {
            card.insertBefore(bar, card.firstChild);
        }
        $('vajraStableModeToggle').addEventListener('change', function (ev) {
            savePrefs({ stable_mode_enabled: ev.target.checked });
        });
        $('vajraStableFocusToggle').addEventListener('change', function (ev) {
            savePrefs({ focus_mode_enabled: ev.target.checked });
        });
        $('vajraStickyPersist').addEventListener('change', function (ev) {
            savePrefs({ sticky_persist_minutes: parseInt(ev.target.value, 10) || 30 });
        });
        $('vajraFreezeFocusBtn').addEventListener('click', freezeFromCurrentTop3);
    }

    function init(opts) {
        opts = opts || {};
        ensureBar(opts.prefix || 'vf');
        loadState();
    }

    global.VajraStableExecution = {
        init: init,
        syncFromPayload: syncFromPayload,
        getState: function () {
            return _stable;
        },
    };
})(window);
