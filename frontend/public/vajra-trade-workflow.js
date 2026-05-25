/**
 * Vajra discretionary trade workflow — validation modal + running cockpit.
 */
(function (global) {
    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;

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
    let _compactSections = false;
    let _emptyOpenHtml = '';
    let _emptyClosedHtml = '';
    let _listAllPlatforms = false;
    let _discoveryRow = null;
    let _preview = null;
    let _entryDraft = {};
    /** Uppercase stock symbols with status=active (hide from Vajra Screen). */
    let _activeStockKeys = new Set();

    function tradeStockKey(t) {
        return String((t && t.stock) || '').trim().toUpperCase();
    }

    function syncActiveStockKeys(rows) {
        const next = new Set();
        (rows || []).forEach(function (t) {
            const k = tradeStockKey(t);
            if (k) next.add(k);
        });
        _activeStockKeys = next;
        try {
            global.dispatchEvent(
                new CustomEvent('vajra:active-positions-changed', {
                    detail: { stocks: Array.from(_activeStockKeys) },
                })
            );
        } catch (e) {
            /* ignore */
        }
    }

    function getActiveTradeStocks() {
        return new Set(_activeStockKeys);
    }

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

    function statusIcon(status) {
        if (status === 'pass') {
            return '<span class="vajra-wf-st vajra-wf-st-pass" aria-label="Pass">✅</span>';
        }
        if (status === 'warn') {
            return '<span class="vajra-wf-st vajra-wf-st-warn" aria-label="Warning">⚠️</span>';
        }
        return '<span class="vajra-wf-st vajra-wf-st-fail" aria-label="Fail">❌</span>';
    }

    function renderEvalTable(items) {
        if (!items || !items.length) {
            return '<p class="vajra-meta">Auto-validation unavailable.</p>';
        }
        let h =
            '<table class="vajra-wf-check-table"><thead><tr>' +
            '<th>Condition</th><th>Status</th><th>Confidence</th><th>Metric</th>' +
            '</tr></thead><tbody>';
        items.forEach(function (it) {
            const tip = esc(it.tooltip || '');
            const st = esc(it.status || 'fail');
            h +=
                '<tr class="vajra-wf-check-row vajra-wf-check-' +
                st +
                '">' +
                '<td class="vajra-wf-check-label" title="' +
                tip +
                '">' +
                esc(it.label || it.key) +
                '</td>' +
                '<td class="vajra-wf-check-icon">' +
                statusIcon(it.status) +
                '</td>' +
                '<td class="vajra-wf-conf">' +
                esc(it.confidence != null ? it.confidence : '—') +
                '%</td>' +
                '<td class="vajra-wf-met" title="' +
                tip +
                '">' +
                esc(it.metric || '—') +
                '</td></tr>';
        });
        h += '</tbody></table>';
        return h;
    }

    /** Structure + Market automated checks; activation blocked if &gt;70% are not pass. */
    function structureMarketEvalSummary(evalAll) {
        const items = (evalAll || []).filter(function (it) {
            return it.section === 'structure' || it.section === 'market';
        });
        if (!items.length) {
            return { total: 0, passCount: 0, notPassCount: 0, notPassPct: 0, canActivate: true };
        }
        const passCount = items.filter(function (it) {
            return it.status === 'pass';
        }).length;
        const notPassCount = items.length - passCount;
        const notPassPct = (notPassCount / items.length) * 100;
        return {
            total: items.length,
            passCount: passCount,
            notPassCount: notPassCount,
            notPassPct: notPassPct,
            canActivate: notPassPct <= 70,
        };
    }

    function renderPsychGrid() {
        let h = '<div class="vajra-wf-grid-2">';
        PSYCH_CHECKS.forEach(function (pair) {
            h +=
                '<label><input type="checkbox" data-chk="' +
                esc(pair[0]) +
                '"> ' +
                esc(pair[1]) +
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
        let warns = '';
        ((_preview && _preview.warnings) || []).forEach(function (w) {
            warns += '<div class="vajra-wf-warn">' + esc(w) + '</div>';
        });
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
            (metrics.vwap_distance_pct != null
                ? ' · VWAP dist ' + esc(metrics.vwap_distance_pct) + '%'
                : '') +
            (metrics.pullback_depth_pct != null ? ' · Pullback ' + esc(metrics.pullback_depth_pct) + '%' : '') +
            '</p>';

        const evalAll = (_preview && _preview.checklist_eval) || [];
        const structure = evalAll.filter(function (it) {
            return it.section === 'structure';
        });
        const market = evalAll.filter(function (it) {
            return it.section === 'market';
        });
        const smSummary = structureMarketEvalSummary(evalAll);
        const canActivate = smSummary.canActivate;
        const extLevel = (_preview && _preview.extension_risk_level) || metrics.extension_risk_level || '—';
        const passN = metrics.validation_pass_count;
        const warnN = metrics.validation_warn_count;
        const failN = metrics.validation_fail_count;
        const summary =
            '<p class="vajra-wf-eval-summary">Automated checks: ' +
            '<span class="vajra-wf-sum-pass">' +
            esc(passN != null ? passN : '—') +
            ' pass</span> · ' +
            '<span class="vajra-wf-sum-warn">' +
            esc(warnN != null ? warnN : '—') +
            ' warn</span> · ' +
            '<span class="vajra-wf-sum-fail">' +
            esc(failN != null ? failN : '—') +
            ' fail</span> · Extension risk <strong>' +
            esc(extLevel) +
            '</strong></p>' +
            '<p class="vajra-meta vajra-wf-eval-note">Structure &amp; Market: ' +
            esc(smSummary.passCount) +
            '/' +
            esc(smSummary.total) +
            ' pass (' +
            esc(Math.round(smSummary.notPassPct)) +
            '% not pass). Activation requires ≤70% not pass.</p>' +
            (canActivate
                ? ''
                : '<p class="vajra-wf-block-msg" role="alert">More than 70% of Structure &amp; Market checks did not pass. Activate Trade is disabled — use Cancel or Back to exit.</p>');

        const activateBtn =
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-primary" id="vajraWfActivate"' +
            (canActivate ? '' : ' disabled aria-disabled="true"') +
            '>ACTIVATE TRADE</button>';

        return (
            '<div class="vajra-wf-step vajra-wf-step--active" data-step="b">' +
            summary +
            '<h3>Structure (5m) — automated</h3>' +
            renderEvalTable(structure) +
            '<h3>Market context — automated</h3>' +
            renderEvalTable(market) +
            '<h3>Psychology — confirm manually</h3>' +
            renderPsychGrid() +
            '<h3>Discovery metrics</h3>' +
            met +
            (warns ? '<h3>Pre-entry warnings</h3>' + warns : '') +
            '<div class="vajra-wf-actions">' +
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-ghost" id="vajraWfBackB">Back</button>' +
            '<button type="button" class="vajra-wf-btn vajra-wf-btn-ghost" data-vajra-wf-close="1">Cancel</button>' +
            activateBtn +
            '</div></div>'
        );
    }

    function wireStepB(body) {
        body.querySelectorAll('[data-vajra-wf-close]').forEach(function (el) {
            el.addEventListener('click', closeModal);
        });
        document.getElementById('vajraWfBackB').addEventListener('click', function () {
            body.innerHTML = renderStepA();
            wireStepA(body);
        });
        const activateBtn = document.getElementById('vajraWfActivate');
        if (activateBtn && !activateBtn.disabled) {
            activateBtn.addEventListener('click', activateTrade);
        }
    }

    async function onStepANext() {
        const body = document.getElementById('vajraWfBody');
        const row = _discoveryRow;
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
        wireStepB(body);
    }

    function wireStepA(body) {
        body.querySelectorAll('[data-vajra-wf-close]').forEach(function (el) {
            el.addEventListener('click', closeModal);
        });
        document.getElementById('vajraWfNextA').addEventListener('click', onStepANext);
    }

    function collectChecklist() {
        const out = {};
        const auto = (_preview && _preview.checklist) || {};
        Object.keys(auto).forEach(function (k) {
            out[k] = !!auto[k];
        });
        const evalSnap = (_preview && _preview.checklist_eval) || [];
        evalSnap.forEach(function (it) {
            if (it && it.key) {
                out[it.key + '_status'] = it.status;
                out[it.key + '_confidence'] = it.confidence;
                out[it.key + '_metric'] = it.metric;
            }
        });
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
        wireStepA(body);
    }

    async function activateTrade() {
        const smSummary = structureMarketEvalSummary((_preview && _preview.checklist_eval) || []);
        if (!smSummary.canActivate) {
            alert(
                'More than 70% of Structure & Market checks did not pass (' +
                    smSummary.notPassCount +
                    '/' +
                    smSummary.total +
                    '). Activation is not allowed.'
            );
            return;
        }
        if (!allPsychChecked()) {
            alert('Please confirm all psychology checklist items.');
            return;
        }
        const entryPrice = parseFloat(_entryDraft.entry_price);
        if (!Number.isFinite(entryPrice)) {
            alert('Entry price is required.');
            return;
        }
        const ik = _discoveryRow.instrument_key || '';
        let futLotSize = null;
        if (ik) {
            try {
                const lotRes = await api(
                    '/contract-lot-size?instrument_key=' + encodeURIComponent(ik)
                );
                if (lotRes && lotRes.lot_size > 0) futLotSize = parseInt(lotRes.lot_size, 10);
            } catch (e) {
                /* display-only; activate still proceeds */
            }
        }
        const payload = {
            platform: _platform,
            stock: _discoveryRow.stock || _discoveryRow.security,
            future_symbol: _discoveryRow.security || _discoveryRow.stock,
            instrument_key: ik,
            direction: _entryDraft.direction || dirFromRow(_discoveryRow),
            entry_price: entryPrice,
            lots: parseInt(_entryDraft.lots, 10) || 1,
            entry_time: _entryDraft.entry_time,
            discovery_row: _discoveryRow,
            checklist: collectChecklist(),
            metrics: Object.assign({}, (_preview && _preview.metrics) || {}, {
                checklist_eval: (_preview && _preview.checklist_eval) || [],
                extension_risk_level: (_preview && _preview.extension_risk_level) || null,
                fut_lot_size: futLotSize,
            }),
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
        syncActiveStockKeys(rows);
        const el = document.getElementById(_runningElId);
        if (!el) return;
        if (global.VajraOpenPositionCard && typeof global.VajraOpenPositionCard.mount === 'function') {
            global.VajraOpenPositionCard.mount(el, rows, {
                emptyHtml: _emptyOpenHtml || '',
                onClose: openCloseModal,
            });
            return;
        }
        if (!rows.length) {
            el.innerHTML = _emptyOpenHtml || '';
            return;
        }
        el.innerHTML = '<p class="vajra-meta">Open position UI module failed to load.</p>';
    }

    function renderClosedSymbolCell(t) {
        const ratings = global.VajraFuturesRatings;
        const label = t.future_symbol || t.stock || '—';
        if (ratings && typeof ratings.renderSecurityChartLink === 'function') {
            const buildTrade =
                typeof ratings.buildScreenerFromTrade === 'function'
                    ? ratings.buildScreenerFromTrade
                    : null;
            return ratings.renderSecurityChartLink({
                stock: t.stock || label,
                instrumentKey: t.instrument_key || '',
                label: label,
                direction: t.direction,
                screenerData: buildTrade ? buildTrade(t) : null,
            });
        }
        return esc(label);
    }

    function bindWorkflowChartClicks() {
        const ratings = global.VajraFuturesRatings;
        if (!ratings || typeof ratings.bindSecurityChartClicks !== 'function') return;
        const runEl = document.getElementById(_runningElId);
        const closedEl = document.getElementById(_closedElId);
        if (runEl) ratings.bindSecurityChartClicks(runEl);
        if (closedEl) ratings.bindSecurityChartClicks(closedEl);
    }

    function fmtRupeePnl(v) {
        const n = parseFloat(v);
        if (!Number.isFinite(n)) return '—';
        const sign = n < 0 ? '-' : '';
        return sign + '₹' + Math.abs(Math.round(n)).toLocaleString('en-IN', { maximumFractionDigits: 0 });
    }

    function fmtIstDateTime(iso) {
        if (!iso) return '—';
        const d = new Date(iso);
        if (!Number.isFinite(d.getTime())) return '—';
        return (
            d.toLocaleString('en-IN', {
                timeZone: 'Asia/Kolkata',
                day: '2-digit',
                month: 'short',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                hour12: false,
            }) + ' IST'
        );
    }

    function renderClosedTrades(rows) {
        const el = document.getElementById(_closedElId);
        if (!el) return;
        if (!rows.length) {
            el.innerHTML = _emptyClosedHtml || '';
            return;
        }
        let h = _compactSections
            ? '<div class="vajra-active-block"><table class="vajra-active-table"><thead><tr>'
            : '<div class="vajra-active-block"><h3>Vajra journal (closed)</h3><table class="vajra-active-table"><thead><tr>';
        h +=
            '<th>Symbol</th><th>Dir</th><th>Entry</th><th>Exit</th>' +
            '<th>Entry date &amp; time</th><th>Exit date &amp; time</th>' +
            '<th>P&amp;L</th><th>Lifecycle</th></tr></thead><tbody>';
        rows.forEach(function (t) {
            h +=
                '<tr><td>' +
                renderClosedSymbolCell(t) +
                '</td><td>' +
                esc(t.direction) +
                '</td><td>' +
                esc(t.entry_price) +
                '</td><td>' +
                esc(t.exit_price) +
                '</td><td class="vajra-dt-cell">' +
                esc(fmtIstDateTime(t.entry_time)) +
                '</td><td class="vajra-dt-cell">' +
                esc(fmtIstDateTime(t.exit_time || t.closed_at)) +
                '</td><td>' +
                fmtRupeePnl(t.realized_pnl) +
                '</td><td>' +
                esc(t.lifecycle_state) +
                '</td></tr>';
        });
        h += '</tbody></table></div>';
        el.innerHTML = h;
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

    function tradesQuery(status) {
        let q = '/trades?status=' + encodeURIComponent(status);
        if (!_listAllPlatforms) q += '&platform=' + encodeURIComponent(_platform);
        return q;
    }

    async function refreshCockpit() {
        try {
            const active = await api(tradesQuery('active'));
            renderActiveTrades(active.rows || []);
            const closed = await api(tradesQuery('closed'));
            renderClosedTrades(closed.rows || []);
        } catch (e) {
            console.warn('Vajra cockpit refresh', e);
        }
    }

    function init(opts) {
        opts = opts || {};
        _platform = opts.platform || 'daily_futures';
        _runningElId = opts.runningElId || 'dfVajraActiveTrades';
        _closedElId = opts.closedElId || 'dfVajraClosedTrades';
        _compactSections = opts.compactSections === true;
        _emptyOpenHtml = opts.emptyOpenHtml || '';
        _emptyClosedHtml = opts.emptyClosedHtml || '';
        _listAllPlatforms = opts.listAllPlatforms === true;
        bindWorkflowChartClicks();
        const tick = refreshCockpit();
        setInterval(refreshCockpit, 300000);
        return tick;
    }

    global.VajraTradeWorkflow = {
        init: init,
        openEntry: openEntry,
        refresh: refreshCockpit,
        getActiveTradeStocks: getActiveTradeStocks,
    };
})(window);
