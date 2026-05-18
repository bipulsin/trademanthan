/**
 * Vajra discretionary trade workflow — validation modal + running cockpit.
 */
(function (global) {
    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;

    const STRUCTURE_CHECKS = [
        ['vwap_reclaimed', 'VWAP reclaimed'],
        ['ema_reclaimed', 'EMA reclaimed'],
        ['hilega_milega', 'Hilega-Milega forming'],
        ['pullback_shallow', 'Pullback shallow'],
        ['no_vertical_exhaustion', 'No vertical exhaustion'],
        ['candle_spread_healthy', 'Candle spread healthy'],
        ['not_into_major_level', 'Not entering into major resistance/support'],
        ['reclaim_candle_strong', 'Reclaim candle closed strong'],
    ];
    const MARKET_CHECKS = [
        ['market_structure_supportive', 'Market structure supportive'],
        ['sector_not_conflicting', 'Sector not conflicting'],
        ['volume_acceptable', 'Volume acceptable'],
        ['not_extended_vwap', 'Not extended from VWAP'],
    ];
    const PSYCH_CHECKS = [
        ['not_fomo', 'Not FOMO entry'],
        ['risk_accepted', 'Risk accepted beforehand'],
        ['not_revenge', 'Entry not revenge trade'],
        ['comfortable_exit', 'Comfortable exiting if invalidated'],
        ['structure_valid_pullback', 'Structure still valid after small pullback'],
    ];
    const EXIT_REASONS = [
        'Target achieved',
        'Structure weakening',
        'EMA failure',
        'Time-based exit',
        'Emotional exit',
        'Manual discretionary exit',
        'Other',
    ];

    let _platform = 'daily_futures';
    let _runningElId = 'dfVajraActiveTrades';
    let _closedElId = 'dfVajraClosedTrades';
    let _discoveryRow = null;
    let _preview = null;
    let _entryDraft = {};

    function authHeaders() {
        const t = global.localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            'Content-Type': 'application/json',
            Accept: 'application/json',
        };
    }

    function api(path, opts) {
        const urls = [API_BASE + '/api/vajra-futures' + path, API_BASE + '/vajra-futures' + path];
        return (async function tryOne(i) {
            if (i >= urls.length) throw new Error('API failed');
            const res = await fetch(urls[i], Object.assign({ headers: authHeaders() }, opts || {}));
            const data = await res.json();
            if (!res.ok) throw new Error(data.message || res.statusText);
            return data;
        })(0);
    }

    function esc(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function nowLocalInput() {
        const d = new Date();
        const p = function (n) {
            return String(n).padStart(2, '0');
        };
        return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + 'T' + p(d.getHours()) + ':' + p(d.getMinutes());
    }

    function dirFromRow(row) {
        const tt = String(row.trade_type || '');
        if (tt.indexOf('SHORT') >= 0) return 'SHORT';
        return 'LONG';
    }

    function healthClass(score) {
        const n = Number(score);
        if (n >= 60) return 'vajra-health-strong';
        if (n >= 40) return 'vajra-health-warn';
        return 'vajra-health-risk';
    }

    function ensureModal() {
        let m = document.getElementById('vajraWfModal');
        if (m) return m;
        m = document.createElement('div');
        m.id = 'vajraWfModal';
        m.className = 'vajra-wf-modal';
        m.setAttribute('aria-hidden', 'true');
        m.innerHTML =
            '<div class="vajra-wf-backdrop" data-vajra-wf-close="1"></div>' +
            '<div class="vajra-wf-panel">' +
            '<h2 class="vajra-wf-title">Trade Validation &amp; Entry</h2>' +
            '<div id="vajraWfBody"></div>' +
            '</div>';
        document.body.appendChild(m);
        m.querySelector('[data-vajra-wf-close]').addEventListener('click', closeModal);
        return m;
    }

    function closeModal() {
        const m = document.getElementById('vajraWfModal');
        if (m) {
            m.classList.remove('vajra-wf-modal--open');
            m.setAttribute('aria-hidden', 'true');
        }
    }

    function renderCheckGrid(items, prefix, autoKeys) {
        let h = '<div class="vajra-wf-grid-2">';
        items.forEach(function (pair) {
            const key = prefix + pair[0];
            const auto = autoKeys && autoKeys[pair[0]];
            const checked = auto ? ' checked' : '';
            const dis = auto ? ' disabled' : '';
            h +=
                '<label><input type="checkbox" data-chk="' +
                esc(key) +
                '"' +
                checked +
                dis +
                '> ' +
                esc(pair[1]) +
                (auto ? ' (auto)' : '') +
                '</label>';
        });
        h += '</div>';
        return h;
    }

    function renderStepA() {
        const sym = _discoveryRow.security || _discoveryRow.stock || '—';
        const dir = dirFromRow(_discoveryRow);
        return (
            '<div class="vajra-wf-step vajra-wf-step--active" data-step="a">' +
            '<div class="vajra-wf-field"><label>Symbol</label><input readonly value="' +
            esc(sym) +
            '"></div>' +
            '<div class="vajra-wf-field"><label>Direction</label><select id="vajraWfDir"><option value="LONG"' +
            (dir === 'LONG' ? ' selected' : '') +
            '>LONG</option><option value="SHORT"' +
            (dir === 'SHORT' ? ' selected' : '') +
            '>SHORT</option></select></div>' +
            '<div class="vajra-wf-field"><label>Entry Price</label><input type="number" step="0.05" id="vajraWfEntryPrice" value=""></div>' +
            '<div class="vajra-wf-field"><label>Lots</label><input type="number" min="1" id="vajraWfLots" value="1"></div>' +
            '<div class="vajra-wf-field"><label>Entry Time</label><input type="datetime-local" id="vajraWfEntryTime" value="' +
            nowLocalInput() +
            '"></div>' +
            '<div class="vajra-wf-actions"><button type="button" class="vajra-wf-btn vajra-wf-btn-ghost" data-vajra-wf-close="1">Cancel</button>' +
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-primary" id="vajraWfNextA">Next</button></div></div>'
        ).replace(/<div /g, '<div ').replace(/<\/motion>/g, '</div>');
    }

    function renderStepB() {
        const auto = (_preview && _preview.checklist) || {};
        const psych = {};
        let warns = '';
        ((_preview && _preview.warnings) || []).forEach(function (w) {
            warns += '<div class="vajra-wf-warn">' + esc(w) + '</div>';
        });
        warns = warns.replace(/<\/motion>/g, '</div>');
        const metrics = (_preview && _preview.metrics) || {};
        let met =
            '<div class="vajra-wf-metrics">' +
            ['tps_score', 'ecs_score', 'extension_risk_score', 'pullback_quality_score'].map(function (k) {
                const lbl = k.replace(/_/g, ' ');
                return (
                    '<div class="vajra-wf-metric"><span>' +
                    esc(lbl) +
                    '</span><strong>' +
                    esc(metrics[k] != null ? metrics[k] : _discoveryRow[k] || '—') +
                    '</strong></div>'
                );
            }).join('') +
            '</div>';
        met +=
            '<p class="vajra-meta">Trend: ' +
            esc(metrics.trend_strength || '—') +
            ' · Phase: ' +
            esc(metrics.market_phase || _discoveryRow.market_phase || '—') +
            '</p>';

        const structItems = STRUCTURE_CHECKS.slice();
        structItems[2] = ['hilega_milega', 'Hilega-Milega forming'];

        return (
            '<div class="vajra-wf-step vajra-wf-step--active" data-step="b">' +
            '<h3>Structure (5m)</h3>' +
            renderCheckGrid(structItems, '', auto) +
            '<h3>Market context</h3>' +
            renderCheckGrid(MARKET_CHECKS, '', auto) +
            '<h3>Psychology</h3>' +
            renderCheckGrid(PSYCH_CHECKS, '', psych) +
            '<h3>Auto-calculated metrics</h3>' +
            met +
            (warns ? '<h3>Pre-entry warnings</h3>' + warns : '') +
            '<div class="vajra-wf-actions">' +
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-ghost" id="vajraWfBackB">Back</button>' +
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-primary" id="vajraWfActivate">ACTIVATE TRADE</button>' +
            '</div></div>'
        ).replace(/<div /g, '<div ').replace(/<\/motion>/g, '</div>').replace(/<\/motion>/g, '</div>');
    }

    function collectChecklist() {
        const out = {};
        document.querySelectorAll('#vajraWfBody input[data-chk]').forEach(function (inp) {
            out[inp.getAttribute('data-chk')] = inp.checked;
        });
        return out;
    }

    function allPsychChecked() {
        let ok = true;
        PSYCH_CHECKS.forEach(function (p) {
            const el = document.querySelector('#vajraWfBody input[data-chk="' + p[0] + '"]');
            if (el && !el.checked) ok = false;
        });
        return ok;
    }

    async function openEntry(row) {
        _discoveryRow = row;
        _preview = null;
        const m = ensureModal();
        const body = document.getElementById('vajraWfBody');
        body.innerHTML = renderStepA();
        m.classList.add('vajra-wf-modal--open');
        m.setAttribute('aria-hidden', 'false');
        body.querySelector('[data-vajra-wf-close]') &&
            body.querySelector('[data-vajra-wf-close]').addEventListener('click', closeModal);
        document.getElementById('vajraWfNextA').addEventListener('click', async function () {
            const dir = document.getElementById('vajraWfDir').value;
            _entryDraft = {
                direction: dir,
                entry_price: document.getElementById('vajraWfEntryPrice').value,
                lots: document.getElementById('vajraWfLots').value,
                entry_time: document.getElementById('vajraWfEntryTime').value,
            };
            try {
                _preview = await api('/trades/validate-preview', {
                    method: 'POST',
                    body: JSON.stringify({
                        stock: row.stock || row.security,
                        direction: dir,
                        instrument_key: row.instrument_key || '',
                        discovery_row: row,
                    }),
                });
            } catch (e) {
                alert('Validation preview failed: ' + e.message);
                return;
            }
            body.innerHTML = renderStepB();
            document.getElementById('vajraWfBackB').addEventListener('click', function () {
                body.innerHTML = renderStepA();
                wireStepA();
            });
            document.getElementById('vajraWfActivate').addEventListener('click', activateTrade);
        });
    }

    function wireStepA() {
        document.getElementById('vajraWfNextA').addEventListener('click', function () {
            openEntry(_discoveryRow);
        });
    }

    async function activateTrade() {
        if (!allPsychChecked()) {
            alert('Please confirm all psychology checklist items.');
            return;
        }
        const entryPrice = parseFloat(_entryDraft.entry_price);
        if (!Number.isFinite(entryPrice)) {
            alert('Entry price is required.');
            return;
        }
        const payload = {
            platform: _platform,
            stock: _discoveryRow.stock || _discoveryRow.security,
            future_symbol: _discoveryRow.security || _discoveryRow.stock,
            instrument_key: _discoveryRow.instrument_key || '',
            direction: _entryDraft.direction || dirFromRow(_discoveryRow),
            entry_price: entryPrice,
            lots: parseInt(_entryDraft.lots, 10) || 1,
            entry_time: _entryDraft.entry_time,
            discovery_row: _discoveryRow,
            checklist: collectChecklist(),
            metrics: (_preview && _preview.metrics) || {},
            warnings: (_preview && _preview.warnings) || [],
        };
        try {
            await api('/trades', { method: 'POST', body: JSON.stringify(payload) });
            closeModal();
            refreshCockpit();
        } catch (e) {
            alert('Activate failed: ' + e.message);
        }
    }

    function renderActiveTrades(rows) {
        const el = document.getElementById(_runningElId);
        if (!el) return;
        if (!rows.length) {
            el.innerHTML = '';
            return;
        }
        let h = '<div class="vajra-active-block"><h3>Vajra managed (discretionary)</h3>';
        rows.forEach(function (t) {
            const pnl = t.unrealized_pnl != null ? Number(t.unrealized_pnl).toFixed(2) : '—';
            const health = t.trade_health != null ? Number(t.trade_health).toFixed(0) : '—';
            let alerts = '';
            (t.alerts || []).slice(-4).forEach(function (a) {
                const cls =
                    a.level === 'positive' ? 'vajra-alert-pos' : a.level === 'warning' ? 'vajra-alert-warn' : 'vajra-alert-risk';
                alerts += '<div class="' + cls + '">' + esc(a.message || '') + '</div>';
            });
            h +=
                '<details class="vajra-active-card" open><summary>' +
                esc(t.future_symbol || t.stock) +
                ' · ' +
                esc(t.direction) +
                ' · Health ' +
                health +
                ' · P&amp;L ' +
                esc(pnl) +
                '</summary><div class="vajra-active-body">' +
                '<div class="vajra-active-grid">' +
                '<span>Lifecycle: <strong>' +
                esc(t.lifecycle_state || '—') +
                '</strong></span>' +
                '<span>TPS: ' +
                esc(t.discovery_snapshot && t.discovery_snapshot.tps_score) +
                '</span>' +
                '<span>ECS: ' +
                esc(t.discovery_snapshot && t.discovery_snapshot.ecs_score) +
                '</span>' +
                '<span>EMA: ' +
                esc(t.ema_status || '—') +
                '</span>' +
                '<span>VWAP: ' +
                esc(t.vwap_status || '—') +
                '</span>' +
                '<span>Structure: ' +
                esc(t.structure_status || '—') +
                '</span>' +
                '<span>Momentum: ' +
                esc(t.momentum_status || '—') +
                '</span>' +
                '</div>' +
                alerts +
                '<div class="vajra-wf-actions"><button type="button" class="vajra-wf-btn vajra-wf-btn-danger" data-close-trade="' +
                t.id +
                '">CLOSE TRADE</button></div></div></details>';
        });
        h += '</div>';
        el.innerHTML = h;
        el.querySelectorAll('[data-close-trade]').forEach(function (btn) {
            btn.addEventListener('click', function () {
                openCloseModal(parseInt(btn.getAttribute('data-close-trade'), 10));
            });
        });
    }

    function renderClosedTrades(rows) {
        const el = document.getElementById(_closedElId);
        if (!el || !rows.length) return;
        let h = '<div class="vajra-active-block"><h3>Vajra journal (closed)</h3><table class="vajra-active-table"><thead><tr>' +
            '<th>Symbol</th><th>Dir</th><th>Entry</th><th>Exit</th><th>P&amp;L</th><th>Lifecycle</th></tr></thead><tbody>';
        rows.forEach(function (t) {
            h +=
                '<tr><td>' +
                esc(t.stock) +
                '</td><td>' +
                esc(t.direction) +
                '</td><td>' +
                esc(t.entry_price) +
                '</td><td>' +
                esc(t.exit_price) +
                '</td><td>' +
                esc(t.realized_pnl) +
                '</td><td>' +
                esc(t.lifecycle_state) +
                '</td></tr>';
        });
        h += '</tbody></table></div>';
        el.innerHTML = h.replace(/<div /g, '<div ').replace(/<\/motion>/g, '</div>').replace(/<div /g, '<div ').replace(/<\/motion>/g, '</div>');
    }

    function openCloseModal(tradeId) {
        let reasons = EXIT_REASONS.map(function (r) {
            return '<label><input type="checkbox" data-exit-reason value="' + esc(r) + '"> ' + esc(r) + '</label>';
        }).join('');
        const html =
            '<div class="vajra-wf-step vajra-wf-step--active">' +
            '<h3>Close trade</h3>' +
            '<div class="vajra-wf-field"><label>Exit price</label><input type="number" step="0.05" id="vajraWfExitPrice"></div>' +
            '<div class="vajra-wf-field"><label>Reason</label><div style="display:flex;flex-direction:column;gap:4px">' +
            reasons +
            '</div></div>' +
            '<div class="vajra-wf-actions"><button type="button" class="vajra-wf-btn vajra-wf-btn-ghost" data-vajra-wf-close="1">Cancel</button>' +
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-danger" id="vajraWfConfirmClose">Confirm close</button></div></div>';
        const m = ensureModal();
        document.getElementById('vajraWfBody').innerHTML = html
            .replace(/<div /g, '<div ')
            .replace(/<\/motion>/g, '</div>');
        m.classList.add('vajra-wf-modal--open');
        document.getElementById('vajraWfConfirmClose').addEventListener('click', async function () {
            const selected = [];
            document.querySelectorAll('[data-exit-reason]:checked').forEach(function (c) {
                selected.push(c.value);
            });
            const xp = parseFloat(document.getElementById('vajraWfExitPrice').value);
            if (!selected.length) {
                alert('Select at least one exit reason.');
                return;
            }
            try {
                await api('/trades/' + tradeId + '/close', {
                    method: 'POST',
                    body: JSON.stringify({
                        exit_price: xp,
                        exit_time: new Date().toISOString(),
                        exit_reasons: selected,
                    }),
                });
                closeModal();
                refreshCockpit();
            } catch (e) {
                alert(e.message);
            }
        });
    }

    async function refreshCockpit() {
        try {
            const active = await api('/trades?status=active&platform=' + encodeURIComponent(_platform));
            renderActiveTrades(active.rows || []);
            const closed = await api('/trades?status=closed&platform=' + encodeURIComponent(_platform));
            renderClosedTrades(closed.rows || []);
        } catch (e) {
            console.warn('Vajra cockpit refresh', e);
        }
    }

    function init(opts) {
        _platform = opts.platform || 'daily_futures';
        _runningElId = opts.runningElId || 'dfVajraActiveTrades';
        _closedElId = opts.closedElId || 'dfVajraClosedTrades';
        refreshCockpit();
        setInterval(refreshCockpit, 300000);
    }

    global.VajraTradeWorkflow = { init: init, openEntry: openEntry, refresh: refreshCockpit };
})(window);
