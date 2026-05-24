/**
 * TWCTO Vajra — futures transition discovery + expansion confirmation.
 * Default: 30m TPS discovery → 5m execution validation on shortlist.
 */
(function (global) {
    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;

    const TOP_N = 8;
    /** Screen table: max ARMED rows when back-filling Top 8 (after EXECUTABLE). */
    const TOP8_ARMED_MAX = 6;
    const DEFAULT_SCAN_TF = '30m';
    const DEFAULT_HTF = '1hr';
    const HTF_OPTIONS = ['1hr', '1d', '1w'];
    const TF_MINUTES = { '5m': 5, '15m': 15, '30m': 30, '1hr': 60, '1d': 1440, '1w': 10080 };

    function validHtfForScan(scanTf) {
        const sm = TF_MINUTES[scanTf] || 0;
        return HTF_OPTIONS.filter(function (h) {
            return TF_MINUTES[h] > sm;
        });
    }

    function syncHtfSelect(scanSel, htfSel) {
        if (!scanSel || !htfSel) return;
        const allowed = validHtfForScan(scanSel.value);
        const prev = htfSel.value;
        htfSel.innerHTML = allowed
            .map(function (h) {
                return '<option value="' + h + '">' + h + '</option>';
            })
            .join('');
        if (allowed.indexOf(prev) >= 0) htfSel.value = prev;
        else if (allowed.indexOf(DEFAULT_HTF) >= 0) htfSel.value = DEFAULT_HTF;
        else if (allowed.length) htfSel.value = allowed[allowed.length - 1];
    }

    const QUAL_EXECUTABLE = 'EXECUTABLE';
    const QUAL_ARMED = 'ARMED';
    const QUAL_DISCOVERY = 'DISCOVERY';
    const QUAL_WATCHLIST = 'WATCHLIST';
    const QUAL_REJECT = 'REJECT';

    const TOP_COLUMNS = [
        { key: 'execution_bias', label: 'Direction' },
        { key: 'qualification', label: 'Qualification' },
        { key: 'confidence', label: 'Confidence', num: true },
        { key: 'setup_quality_score', label: 'Setup Quality', num: true },
        { key: 'market_context', label: 'Market Context' },
        { key: 'pullback_quality_score', label: 'Pullback', num: true },
        { key: 'extension_risk_score', label: 'Extension Risk', num: true },
    ];

    const ADVANCED_COLUMNS = [
        { key: 'tps_score', label: 'TPS' },
        { key: 'setup_potential_score', label: 'Setup Potential' },
        { key: 'ecs_score', label: 'ECS' },
        { key: 'vwap_reclaim_status', label: 'VWAP' },
        { key: 'obv', label: 'OBV' },
        { key: 'htf_alignment_score', label: 'HTF' },
    ];

    const VAJRA_ENTER_SEEN_KEY = 'vajra_enter_telegram_seen';

    function authHeaders() {
        const t = global.localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            'Content-Type': 'application/json',
            Accept: 'application/json',
        };
    }

    function fmtUpdated(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', {
                    timeZone: 'Asia/Kolkata',
                    dateStyle: 'short',
                    timeStyle: 'short',
                });
            }
        } catch (e) {}
        return String(iso);
    }

    /** Normalize API timestamps for reliable change detection. */
    function tsEpoch(iso) {
        if (!iso) return null;
        const t = new Date(iso).getTime();
        return Number.isFinite(t) ? t : null;
    }

    function cacheBustQ() {
        return '&_=' + Date.now();
    }

    let chartEngineLoadPromise = null;

    function ensureChartEngine() {
        if (global.SecurityChartEngine) return Promise.resolve(global.SecurityChartEngine);
        if (chartEngineLoadPromise) return chartEngineLoadPromise;
        chartEngineLoadPromise = new Promise(function (resolve, reject) {
            const s = document.createElement('script');
            s.src = 'security-chart/security-chart-engine.js?v=9';
            s.async = true;
            s.onload = function () {
                resolve(global.SecurityChartEngine);
            };
            s.onerror = function () {
                reject(new Error('Chart module failed to load'));
            };
            document.head.appendChild(s);
        });
        return chartEngineLoadPromise;
    }

    const chartPayloadRegistry = {};
    let chartPayloadSeq = 0;

    function registerChartPayload(payload) {
        const id = 'vcp' + ++chartPayloadSeq;
        chartPayloadRegistry[id] = payload || {};
        return id;
    }

    function getChartPayload(id) {
        return id ? chartPayloadRegistry[id] || null : null;
    }

    function buildScreenerFromRow(r) {
        if (!r) return {};
        const tt = String(r.trade_type || '').toUpperCase();
        const direction = tt.indexOf('SHORT') >= 0 || tt.indexOf('BEAR') >= 0 ? 'SHORT' : 'LONG';
        return {
            direction: direction,
            tps: r.tps_score,
            ecs: r.ecs_score,
            momentum: r.transition_state || r.momentum,
            emaState: r.ema_reclaim_status,
            vwapState: r.vwap_reclaim_status,
            structure: r.structure,
            trend: r.trend,
            volume: r.volume,
            lifecycle: r.pipeline_stage || r.entry_state || r.qualification_stage,
            armed: r.enter_enabled,
            setupQuality: r.trade_quality_score,
            institutionalBias: r.reversal_risk,
            pullbackQuality: r.pullback_quality_score,
            market_phase: r.market_phase,
            extension_risk_score: r.extension_risk_score,
            evs_score: r.evs_score,
            conviction_score: r.conviction_score,
            qualification: r.qualification_stage || r.qualification_state,
        };
    }

    function buildScreenerFromTrade(t) {
        if (!t) return {};
        const disc = t.discovery_snapshot || {};
        const met = t.metrics_at_entry || {};
        const entry = parseFloat(t.entry_price);
        const live = parseFloat(t.current_price);
        let pnlPct = null;
        if (Number.isFinite(entry) && entry > 0 && Number.isFinite(live)) {
            const bull = String(t.direction || '').toUpperCase().indexOf('L') === 0;
            pnlPct = bull ? ((live - entry) / entry) * 100 : ((entry - live) / entry) * 100;
        }
        const alerts = t.alerts || [];
        const insight =
            alerts.length && alerts[alerts.length - 1].message
                ? alerts[alerts.length - 1].message
                : '';
        return {
            direction: t.direction,
            livePnlPct: pnlPct,
            tradeHealth: t.trade_health,
            lifecycle: t.lifecycle_state,
            tps: disc.tps_score != null ? disc.tps_score : met.tps_score,
            ecs: disc.ecs_score != null ? disc.ecs_score : met.ecs_score,
            emaState: t.ema_status,
            vwapState: t.vwap_status,
            structure: t.structure_status,
            momentum: t.momentum_status,
            pullbackQuality: disc.pullback_quality_score,
            extension_risk_score: disc.extension_risk_score,
            market_phase: disc.market_phase,
            institutionalBias: disc.reversal_risk || disc.htf_bias,
            insight: insight,
        };
    }

    function renderSecurityChartLink(opts) {
        opts = opts || {};
        const stock = String(opts.stock || opts.symbol || '').trim();
        const ik = String(opts.instrumentKey || opts.instrument_key || '').trim();
        const label = String(opts.label || opts.displaySymbol || stock || '—').trim();
        const qual = String(opts.qual || opts.qualification || opts.qualification_stage || '').trim();
        const extra = String(opts.className || '').trim();
        const direction = String(opts.direction || '').trim();
        const screenerData = opts.screenerData || null;
        let payloadAttr = '';
        if (screenerData && typeof screenerData === 'object' && Object.keys(screenerData).length) {
            const pid = registerChartPayload({
                screenerData: screenerData,
                direction: direction || screenerData.direction,
            });
            payloadAttr = ' data-chart-payload-id="' + escapeHtml(pid) + '"';
        }
        return (
            '<button type="button" class="vajra-security-link' +
            (extra ? ' ' + extra : '') +
            '" title="Open chart + intelligence" ' +
            'data-chart-symbol="' +
            escapeHtml(stock) +
            '" data-chart-instrument-key="' +
            escapeHtml(ik) +
            '" data-chart-label="' +
            escapeHtml(label) +
            '" data-chart-qual="' +
            escapeHtml(qual) +
            '" data-chart-direction="' +
            escapeHtml(direction) +
            '"' +
            payloadAttr +
            '>' +
            escapeHtml(label) +
            '</button>'
        );
    }

    function renderSecurityCell(r) {
        const label = cellValue(r, { key: 'security' });
        const stock = String(r.stock || r.security || '').trim();
        const ik = String(r.instrument_key || '').trim();
        const qual = String(r.qualification_stage || r.qualification_state || '');
        return renderSecurityChartLink({
            stock: stock,
            instrumentKey: ik,
            label: label,
            qual: qual,
            screenerData: buildScreenerFromRow(r),
        });
    }

    function openChartFromButton(btn) {
        if (!btn) return;
        const symbol = btn.getAttribute('data-chart-symbol') || '';
        const instrumentKey = btn.getAttribute('data-chart-instrument-key') || '';
        const displaySymbol = btn.getAttribute('data-chart-label') || symbol;
        const qual = btn.getAttribute('data-chart-qual') || '';
        const stored = getChartPayload(btn.getAttribute('data-chart-payload-id'));
        const screenerData =
            (stored && stored.screenerData) || {};
        const direction =
            (stored && stored.direction) ||
            btn.getAttribute('data-chart-direction') ||
            screenerData.direction ||
            '';
        ensureChartEngine()
            .then(function (eng) {
                return eng.openSecurityChart({
                    symbol: symbol,
                    instrumentType: 'FUT',
                    instrumentKey: instrumentKey,
                    displaySymbol: displaySymbol,
                    exchange: 'NSE',
                    timeframe: '5m',
                    direction: direction,
                    screenerData: screenerData,
                    metadata: { qualification: qual },
                });
            })
            .catch(function (err) {
                if (global.console && global.console.warn) {
                    global.console.warn('Security chart:', err);
                }
            });
    }

    function bindSecurityChartClicks(rootEl) {
        if (!rootEl || rootEl._vajraChartBound) return;
        rootEl._vajraChartBound = true;
        rootEl.addEventListener('click', function (ev) {
            const btn = ev.target.closest('.vajra-security-link');
            if (!btn) return;
            ev.preventDefault();
            ev.stopPropagation();
            openChartFromButton(btn);
        });
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function fmtNum(v) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        return Number.isFinite(n) ? n.toFixed(1) : '—';
    }

    function qualificationOf(row) {
        return String((row && (row.qualification || row.entry_state)) || QUAL_DISCOVERY)
            .trim()
            .toUpperCase();
    }

    function cellValue(r, col) {
        if (col.key === 'security') return r.security || r.stock || '—';
        if (col.key === 'execution_bias') {
            const d = String(r.execution_bias || r.direction || 'LONG').toUpperCase();
            return d === 'NEUTRAL' ? 'LONG' : d;
        }
        if (col.key === 'qualification') return qualificationOf(r);
        if (col.key === 'market_context') {
            return String(r.market_context || r.market_phase || '—');
        }
        if (col.key === 'setup_quality_score') {
            const v = r.setup_quality_score != null ? r.setup_quality_score : r.trade_quality_score;
            return fmtNum(v != null ? v : r.confidence);
        }
        if (col.key === 'setup_potential_score') {
            const v = r.setup_potential_score != null ? r.setup_potential_score : r.ees_score;
            return fmtNum(v);
        }
        if (
            col.num ||
            col.key === 'confidence' ||
            col.key === 'tps_score' ||
            col.key === 'ecs_score'
        ) {
            return fmtNum(r[col.key] != null ? r[col.key] : r.confidence);
        }
        return r[col.key] != null && r[col.key] !== '' ? String(r[col.key]) : '—';
    }

    function confidenceTier(conf) {
        const n = Number(conf);
        if (!Number.isFinite(n)) return 'weak';
        if (n >= 70) return 'executable';
        if (n >= 40) return 'developing';
        return 'weak';
    }

    function renderConfidenceMeter(row) {
        const conf = Number(row.confidence);
        const n = Number.isFinite(conf) ? Math.max(0, Math.min(100, conf)) : 0;
        const tier = confidenceTier(n);
        return (
            '<div class="vajra-conf-wrap" title="Confidence ' +
            n.toFixed(0) +
            ' — 0–40 weak · 40–70 developing · 70+ executable">' +
            '<div class="vajra-conf-bar vajra-conf-bar--' +
            tier +
            '"><span class="vajra-conf-fill" style="width:' +
            n +
            '%"></span></div>' +
            '<span class="vajra-conf-num">' +
            n.toFixed(0) +
            '</span></div>'
        );
    }

    function renderEnterCell(row, idx, rowspan2) {
        const qual = qualificationOf(row);
        const action = String(row.enter_action || '').toUpperCase();
        const enabled = row.enter_enabled === true && qual === QUAL_EXECUTABLE;
        const title = escapeHtml(row.enter_reason || action || qual);
        const sym = escapeHtml(row.stock || row.security || '');
        const rs = rowspan2 ? ' rowspan="2"' : '';
        if (qual === QUAL_REJECT || !action) {
            return '<td class="vajra-td-enter' + rs + '"><span class="vajra-action-none">—</span></td>';
        }
        if (enabled) {
            return (
                '<td class="vajra-td-enter"' +
                rs +
                '><button type="button" class="vajra-enter-btn vajra-enter-btn-enter" data-vajra-enter="1" data-vajra-stock="' +
                sym +
                '" title="' +
                title +
                '">ENTER</button></td>'
            );
        }
        let btnClass = 'vajra-enter-btn';
        if (action === 'ARMED' || qual === QUAL_ARMED || qual === QUAL_WATCHLIST) {
            btnClass += ' vajra-enter-btn-armed';
        } else if (action === 'MONITOR' || qual === QUAL_DISCOVERY) {
            btnClass += ' vajra-enter-btn-discovery';
        } else {
            btnClass += ' vajra-enter-btn-watch';
        }
        const label = action === 'MONITOR' ? 'MONITOR' : action || qual;
        return (
            '<td class="vajra-td-enter"' +
            rs +
            '><button type="button" class="' +
            btnClass +
            '" disabled title="' +
            title +
            '">' +
            escapeHtml(label) +
            '</button></td>'
        );
    }

    function isPassText(val) {
        const s = String(val || '').toUpperCase();
        return s.indexOf('PASS') >= 0;
    }

    function chipDisplayValue(col, row) {
        const raw = cellValue(row, col);
        if (col.key === 'structure' || col.key === 'momentum' || col.key === 'trend' || col.key === 'volume') {
            return isPassText(raw) ? 'PASS' : 'FAIL';
        }
        if (col.key === 'trade_type') {
            return String(row.trade_type || raw || '—')
                .replace(/\s+/g, ' ')
                .replace('LONG  [A+]', 'LONG (A+)')
                .trim();
        }
        if (col.key === 'transition_state') {
            return String(row.transition_state || raw || '—').trim();
        }
        if (col.key === 'obv') {
            const o = String(row.obv || raw || '').toUpperCase();
            if (o.indexOf('ABOVE') >= 0 && o.indexOf('RISING') >= 0) return 'Above MA';
            if (o.indexOf('BELOW') >= 0 && o.indexOf('FALLING') >= 0) return 'Below MA';
            if (o.indexOf('FLAT') >= 0) return 'Flat';
            return String(row.obv || raw || '—').trim();
        }
        return raw;
    }

    function qualTone(qual) {
        if (qual === QUAL_EXECUTABLE) return 'vajra-qual-exec';
        if (qual === QUAL_ARMED || qual === QUAL_WATCHLIST) return 'vajra-qual-armed';
        if (qual === QUAL_DISCOVERY) return 'vajra-qual-discovery';
        if (qual === QUAL_REJECT) return 'vajra-qual-reject';
        return 'vajra-qual-watch';
    }

    function chipToneClass(col, row) {
        const raw = cellValue(row, col);
        if (col.key === 'execution_bias') {
            let d = String(row.execution_bias || row.direction || 'LONG').toUpperCase();
            if (d === 'NEUTRAL') d = 'LONG';
            const conf = String(row.directional_confidence || '').toLowerCase();
            let extra = '';
            if (conf.indexOf('weak') >= 0) extra = ' vajra-dir-weak';
            else if (conf.indexOf('strong') >= 0) extra = ' vajra-dir-strong';
            else if (conf.indexOf('moderate') >= 0) extra = ' vajra-dir-moderate';
            if (d === 'LONG') return 'df-dir-long' + extra;
            if (d === 'SHORT') return 'df-dir-short' + extra;
            return 'df-dir-long' + extra;
        }
        if (col.key === 'qualification') {
            return qualTone(qualificationOf(row));
        }
        if (col.key === 'market_context') {
            const ctx = String(row.market_context || row.market_phase || '').toUpperCase();
            if (ctx.indexOf('EARLY BULL') >= 0) return 'vajra-ctx-early-bull';
            if (ctx.indexOf('EARLY BEAR') >= 0) return 'vajra-ctx-early-bear';
            if (ctx.indexOf('BULL EXPANSION') >= 0) return 'vajra-ctx-bull-exp';
            if (ctx.indexOf('BEAR EXPANSION') >= 0) return 'vajra-ctx-bear-exp';
            if (ctx.indexOf('TREND CONTINUATION') >= 0) return 'vajra-ctx-continuation';
            if (ctx.indexOf('ROTATIONAL') >= 0) return 'vajra-ctx-rotational';
            if (ctx.indexOf('WEAKENING') >= 0) return 'vajra-ctx-weakening';
            if (ctx.indexOf('COMPRESSION') >= 0) return 'vajra-ctx-compression';
            return 'df-dir-neutral';
        }
        if (col.key === 'extension_risk_score') {
            const n = Number(row.extension_risk_score);
            if (n >= 65) return 'df-dir-short';
            if (n >= 40) return 'vajra-rev-med';
            return 'df-dir-long';
        }
        if (
            col.key === 'pullback_quality_score' ||
            col.key === 'tps_score' ||
            col.key === 'setup_potential_score' ||
            col.key === 'setup_quality_score'
        ) {
            const n = Number(row[col.key] || row.setup_quality_score || row.ees_score);
            if (n >= 60) return 'df-dir-long';
            if (n >= 45) return 'vajra-rev-med';
            return 'df-dir-neutral';
        }
        if (col.key === 'reversal_risk') {
            const u = String(row.reversal_risk || raw || '').toUpperCase();
            if (u === 'HIGH') return 'df-dir-short';
            if (u === 'MEDIUM') return 'vajra-rev-med';
            return 'df-dir-long';
        }
        if (col.key === 'structure' || col.key === 'momentum' || col.key === 'trend' || col.key === 'volume') {
            return isPassText(raw) ? 'df-dir-long' : 'df-dir-short';
        }
        if (col.key === 'vwap_reclaim_status' || col.key === 'ema_reclaim_status') {
            const u = String(row[col.key] || raw || '').toUpperCase();
            if (u.indexOf('RECLAIM') >= 0) return 'df-dir-long';
            if (u.indexOf('BELOW') >= 0 || u.indexOf('ABOVE') >= 0) return 'df-dir-neutral';
            return 'df-dir-neutral';
        }
        return 'df-dir-neutral';
    }

    function transitionTwoLines(raw) {
        const s = String(raw || '—').trim();
        if (!s || s === '—') return { line1: '—', line2: '' };
        const words = s.split(/\s+/);
        if (words.length >= 2) {
            const mid = Math.ceil(words.length / 2);
            return { line1: words.slice(0, mid).join(' '), line2: words.slice(mid).join(' ') };
        }
        if (s.length > 9) {
            const mid = Math.ceil(s.length / 2);
            return { line1: s.slice(0, mid), line2: s.slice(mid) };
        }
        return { line1: s, line2: '' };
    }

    function renderQualificationCell(row) {
        const qual = qualificationOf(row);
        const tags = row.qualification_tags || [];
        let tagsHtml = '';
        if (tags.length) {
            tagsHtml =
                '<div class="vajra-qual-tags">' +
                tags
                    .map(function (t) {
                        return '<span class="vajra-qual-tag">' + escapeHtml(t) + '</span>';
                    })
                    .join('') +
                '</div>';
        }
        return (
            '<div class="vajra-qual-cell">' +
            '<span class="df-dir-pill ' +
            qualTone(qual) +
            '">' +
            escapeHtml(qual) +
            '</span>' +
            tagsHtml +
            '</div>'
        );
    }

    function renderChip(col, row) {
        if (col.key === 'qualification') {
            return renderQualificationCell(row);
        }
        if (col.key === 'confidence') {
            return renderConfidenceMeter(row);
        }
        const tone = chipToneClass(col, row);
        if (col.key === 'transition_state') {
            const lines = transitionTwoLines(row.transition_state);
            const full = String(row.transition_state || '—');
            return (
                '<span class="df-dir-pill vajra-transition-pill ' +
                tone +
                '" title="' +
                escapeHtml(col.label + ': ' + full) +
                '">' +
                escapeHtml(lines.line1) +
                (lines.line2 ? '<br>' + escapeHtml(lines.line2) : '') +
                '</span>'
            );
        }
        const text = chipDisplayValue(col, row);
        let tip = col.label + ': ' + text;
        if (col.key === 'execution_bias' && row.directional_confidence) {
            tip = String(row.directional_confidence);
        }
        return (
            '<span class="df-dir-pill ' +
            tone +
            '" title="' +
            escapeHtml(tip) +
            '">' +
            escapeHtml(text) +
            '</span>'
        );
    }

    function rowQualClass(row) {
        const q = qualificationOf(row);
        if (q === QUAL_REJECT) return ' vajra-row-reject';
        if (q === QUAL_ARMED || q === QUAL_WATCHLIST) return ' vajra-row-armed';
        if (q === QUAL_DISCOVERY) return ' vajra-row-discovery';
        if (q === QUAL_EXECUTABLE) return ' vajra-row-exec';
        return '';
    }

    function renderAdvancedRow(row) {
        let cells = '';
        ADVANCED_COLUMNS.forEach(function (col) {
            let tip = col.label + ': ' + cellValue(row, col);
            if (col.key === 'setup_potential_score') {
                tip =
                    'Setup Potential — attractiveness of emerging setup, not execution approval. ' +
                    cellValue(row, col);
            }
            cells +=
                '<span class="vajra-adv-item" title="' +
                escapeHtml(tip) +
                '"><em>' +
                escapeHtml(col.label) +
                '</em> ' +
                escapeHtml(cellValue(row, col)) +
                '</span>';
        });
        return '<tr class="vajra-adv-row"><td colspan="99">' + cells + '</td></tr>';
    }

    function renderTableBodyRows(rows, columns, showEnter) {
        const cols = columns || TOP_COLUMNS;
        let tbody = '';
        rows.forEach(function (r, idx) {
            tbody += '<tr class="vajra-screener-row' + rowQualClass(r) + '">';
            tbody += '<td class="vajra-td-security">' + renderSecurityCell(r) + '</td>';
            cols.forEach(function (col) {
                let tdClass = col.num ? 'vajra-td-chip num' : 'vajra-td-chip';
                if (col.key === 'qualification') tdClass += ' vajra-td-qual';
                if (col.key === 'confidence') tdClass += ' vajra-td-conf';
                tbody += '<td class="' + tdClass + '">' + renderChip(col, r) + '</td>';
            });
            if (showEnter) tbody += renderEnterCell(r, idx);
            tbody += '</tr>';
            tbody += renderAdvancedRow(r);
        });
        return tbody;
    }

    function renderTableHead(columns, showEnter) {
        const cols = columns || TOP_COLUMNS;
        let head = '<thead><tr><th scope="col">Symbol</th>';
        cols.forEach(function (col) {
            const thClass = col.num ? 'num' : '';
            head +=
                '<th scope="col" class="' +
                thClass +
                '">' +
                escapeHtml(col.label) +
                '</th>';
        });
        if (showEnter) head += '<th scope="col" class="vajra-th-enter">Action</th>';
        head += '</tr></thead>';
        return head;
    }

    function isEnterRow(r) {
        return r.enter_enabled === true || String(r.enter_action || '').toUpperCase() === 'ENTER';
    }

    /** @deprecated use sortForDisplay — kept for cached old script compatibility */
    function sortByTpsDesc(rows) {
        return sortForDisplay(rows);
    }

    function scoreNum(row, key) {
        const n = Number(row[key]);
        return Number.isFinite(n) ? n : 0;
    }

    function qualityScore(row) {
        const tq = scoreNum(row, 'trade_quality_score');
        if (tq > 0) return tq;
        return scoreNum(row, 'confidence');
    }

    function entryStateSortRank(entryState) {
        const s = String(entryState || '')
            .trim()
            .toUpperCase();
        if (!s) return 0;
        if (s === 'EXECUTABLE') return 4;
        if (s === 'ARMED' || s.indexOf('WATCHLIST') >= 0 || s.indexOf('PULLBACK') >= 0) return 3;
        if (s === 'DISCOVERY' || s.indexOf('MONITOR') >= 0) return 2;
        if (s.indexOf('REJECT') >= 0 || s.indexOf('AVOID') >= 0) return 1;
        return 0;
    }

    /** Modal default: EXECUTABLE → ARMED → DISCOVERY → REJECT (higher sorts first when desc). */
    function modalQualificationSortRank(row) {
        const q = qualificationOf(row);
        if (q === QUAL_EXECUTABLE) return 5;
        if (q === QUAL_ARMED) return 4;
        if (q === QUAL_WATCHLIST) return 3;
        if (q === QUAL_DISCOVERY) return 2;
        if (q === QUAL_REJECT) return 1;
        return 0;
    }

    function armedRankValue(row) {
        const armed = Number(row && row.armed_rank_score);
        if (Number.isFinite(armed) && armed > 0) return armed;
        const sq = Number(row && row.setup_quality_score);
        const conf = Number(
            row && row.confidence_score != null ? row.confidence_score : row && row.confidence
        );
        const inst = Number(row && row.institutional_participation_score);
        return (
            (Number.isFinite(sq) ? sq : 0) * 0.65 +
            (Number.isFinite(conf) ? conf : 0) * 0.25 +
            (Number.isFinite(inst) ? inst : 0) * 0.1
        );
    }

    /** Same ordering as backend rank_armed / Top-8 ARMED section (higher rank first). */
    function compareArmedRank(a, b) {
        const ar = armedRankValue(b) - armedRankValue(a);
        if (ar !== 0) return ar;
        const sq =
            (Number(b.setup_quality_score) || 0) - (Number(a.setup_quality_score) || 0);
        if (sq !== 0) return sq;
        const ig =
            (Number(b.ignition_quality_score) || 0) -
            (Number(a.ignition_quality_score) || 0);
        if (ig !== 0) return ig;
        const ip =
            (Number(b.institutional_participation_score) || 0) -
            (Number(a.institutional_participation_score) || 0);
        if (ip !== 0) return ip;
        return String(a.security || a.stock || '').localeCompare(
            String(b.security || b.stock || ''),
            undefined,
            { numeric: true }
        );
    }

    function confidenceVal(row) {
        const c = row.confidence_score != null ? row.confidence_score : row.confidence;
        const n = Number(c);
        return Number.isFinite(n) ? n : 0;
    }

    /** Align with backend rank_executable (highest setup quality first). */
    function compareExecutableRank(a, b) {
        const sq =
            (Number(b.setup_quality_score) || 0) - (Number(a.setup_quality_score) || 0);
        if (sq !== 0) return sq;
        const conf = confidenceVal(b) - confidenceVal(a);
        if (conf !== 0) return conf;
        const vol = (Number(b.volume_score) || 0) - (Number(a.volume_score) || 0);
        if (vol !== 0) return vol;
        const ex = (Number(b.execution_score) || 0) - (Number(a.execution_score) || 0);
        if (ex !== 0) return ex;
        return String(a.security || a.stock || '').localeCompare(
            String(b.security || b.stock || ''),
            undefined,
            { numeric: true }
        );
    }

    /** Align with backend rank_discovery. */
    function compareDiscoveryRank(a, b) {
        const ip =
            (Number(b.institutional_participation_score) || 0) -
            (Number(a.institutional_participation_score) || 0);
        if (ip !== 0) return ip;
        const ds = (Number(b.discovery_score) || 0) - (Number(a.discovery_score) || 0);
        if (ds !== 0) return ds;
        const tps = (Number(b.tps_score) || 0) - (Number(a.tps_score) || 0);
        if (tps !== 0) return tps;
        const evs =
            (Number(b.evs_score) || Number(b.expansion_velocity_score) || 0) -
            (Number(a.evs_score) || Number(a.expansion_velocity_score) || 0);
        if (evs !== 0) return evs;
        return String(a.security || a.stock || '').localeCompare(
            String(b.security || b.stock || ''),
            undefined,
            { numeric: true }
        );
    }

    function poolByQual(rows, qual) {
        return (rows || []).filter(function (r) {
            if (isRejectRow(r)) return false;
            const q = qualificationOf(r);
            if (qual === QUAL_EXECUTABLE) return q === QUAL_EXECUTABLE;
            if (qual === QUAL_ARMED) {
                return q === QUAL_ARMED || q === QUAL_WATCHLIST;
            }
            if (qual === QUAL_DISCOVERY) return q === QUAL_DISCOVERY;
            return false;
        });
    }

    /**
     * Screen — Futures Rating: Top 8 only.
     * 8+ EXECUTABLE → top 8 EXECUTABLE only.
     * Else EXECUTABLE + up to 6 ARMED + DISCOVERY fill; never REJECT.
     */
    function composeTop8ScreenRows(data) {
        const sections = (data && data.top_sections) || {};
        const picks = (data && data.top_picks) || [];
        const execSource = sections.EXECUTABLE && sections.EXECUTABLE.length
            ? sections.EXECUTABLE
            : picks;
        const execPool = poolByQual(execSource, QUAL_EXECUTABLE).slice().sort(compareExecutableRank);
        const armedPool = poolByQual(sections.ARMED, QUAL_ARMED).slice().sort(compareArmedRank);
        const discPool = poolByQual(sections.DISCOVERY, QUAL_DISCOVERY)
            .slice()
            .sort(compareDiscoveryRank);

        if (execPool.length >= TOP_N) {
            const execEight = execPool.slice(0, TOP_N);
            return {
                EXECUTABLE: execEight,
                ARMED: [],
                DISCOVERY: [],
                top8: execEight,
            };
        }

        const execRows = execPool.slice();
        let slots = TOP_N - execRows.length;
        const armedTake = Math.min(TOP8_ARMED_MAX, slots, armedPool.length);
        const armedRows = armedPool.slice(0, armedTake);
        slots -= armedRows.length;
        const discRows = discPool.slice(0, slots);

        return {
            EXECUTABLE: execRows,
            ARMED: armedRows,
            DISCOVERY: discRows,
            top8: execRows.concat(armedRows).concat(discRows),
        };
    }

    function sortModalByQualification(rows) {
        return rows.slice().sort(function (a, b) {
            const tier = modalQualificationSortRank(b) - modalQualificationSortRank(a);
            if (tier !== 0) return tier;
            if (qualificationOf(a) === QUAL_ARMED && qualificationOf(b) === QUAL_ARMED) {
                const armedCmp = compareArmedRank(a, b);
                if (armedCmp !== 0) return armedCmp;
            }
            return String(a.security || a.stock || '').localeCompare(
                String(b.security || b.stock || ''),
                undefined,
                { numeric: true }
            );
        });
    }

    function isRejectRow(row) {
        const st = String((row && row.entry_state) || '').toUpperCase();
        return st === 'REJECT' || st.indexOf('REJECT') >= 0;
    }

    function rowStockKey(row) {
        return String((row && (row.stock || row.security)) || '')
            .trim()
            .toUpperCase();
    }

    function getActiveTradeStocks() {
        if (global.VajraTradeWorkflow && typeof global.VajraTradeWorkflow.getActiveTradeStocks === 'function') {
            return global.VajraTradeWorkflow.getActiveTradeStocks();
        }
        return new Set();
    }

    function filterRowsByActivePositions(rows, activeSet) {
        if (!activeSet || !activeSet.size) return rows || [];
        return (rows || []).filter(function (r) {
            const k = rowStockKey(r);
            return !k || !activeSet.has(k);
        });
    }

    function filterRatingsPayload(data, activeSet) {
        if (!data || !activeSet || !activeSet.size) return data;
        const sections = data.top_sections || {};
        const filteredSections = {
            EXECUTABLE: filterRowsByActivePositions(sections.EXECUTABLE, activeSet),
            ARMED: filterRowsByActivePositions(sections.ARMED, activeSet),
            DISCOVERY: filterRowsByActivePositions(sections.DISCOVERY, activeSet),
        };
        const topPicks = filterRowsByActivePositions(data.top_picks, activeSet);
        const allFiltered = filterRowsByActivePositions(data.rows, activeSet);
        return Object.assign({}, data, {
            rows: allFiltered,
            top_picks: topPicks,
            top_sections: filteredSections,
            remainder: filterRowsByActivePositions(data.remainder, activeSet),
        });
    }

    function rowsForTopTable(rows) {
        const pool = (rows || []).filter(function (r) {
            return !isRejectRow(r);
        });
        return sortForDisplay(pool.length ? pool : rows || []);
    }

    function sortForDisplay(rows) {
        return rows.slice().sort(function (a, b) {
            const stateDiff = entryStateSortRank(b.entry_state) - entryStateSortRank(a.entry_state);
            if (stateDiff !== 0) return stateDiff;
            const scoreDiff = qualityScore(b) - qualityScore(a);
            if (scoreDiff !== 0) return scoreDiff;
            return String(a.security || a.stock || '').localeCompare(
                String(b.security || b.stock || ''),
                undefined,
                { numeric: true }
            );
        });
    }


    function renderSectionHeader(title, cssClass) {
        const colSpan = TOP_COLUMNS.length + 2;
        return (
            '<tr class="vajra-section-head ' +
            (cssClass || '') +
            '"><td colspan="' +
            colSpan +
            '">' +
            escapeHtml(title) +
            '</td></tr>'
        );
    }

    function renderTopTableFromPayload(data) {
        const composed = composeTop8ScreenRows(data || {});
        const execRows = composed.EXECUTABLE || [];
        const armedRows = composed.ARMED || [];
        const discRows = composed.DISCOVERY || [];
        const banner = data && data.banner;
        const hasAny = composed.top8 && composed.top8.length;

        if (!hasAny) {
            return (
                '<p class="vajra-meta">No Vajra ratings for this session yet. ' +
                'The engine runs every 5 minutes (9:30–15:00 IST). ' +
                'If this persists after market open, use Refresh or wait for the next scan.</p>'
            );
        }

        let bannerHtml = '';
        if (banner && banner.message) {
            bannerHtml =
                '<p class="vajra-banner vajra-banner-' +
                escapeHtml(banner.type || 'info') +
                '">' +
                escapeHtml(banner.message) +
                '</p>';
        }

        let tbody = '';
        if (execRows.length) {
            tbody += renderSectionHeader('Executable Now', 'vajra-section-exec');
            tbody += renderTableBodyRows(execRows, TOP_COLUMNS, true);
        }
        if (armedRows.length) {
            tbody += renderSectionHeader('Armed — One Trigger Away', 'vajra-section-armed');
            tbody += renderTableBodyRows(armedRows, TOP_COLUMNS, true);
        }
        if (discRows.length) {
            tbody += renderSectionHeader('Discovery — Institutional Attention', 'vajra-section-discovery');
            tbody += renderTableBodyRows(discRows, TOP_COLUMNS, true);
        }

        return (
            bannerHtml +
            '<p class="vajra-meta vajra-pipeline-note">Execution screener — EXECUTABLE / ARMED / DISCOVERY sections. No watchlist padding.</p>' +
            '<div class="vajra-table-wrap"><table class="vajra-table vajra-top-table">' +
            renderTableHead(TOP_COLUMNS, true) +
            '<tbody>' +
            tbody +
            '</tbody></table></div>' +
            '<p class="vajra-score-footnote">Advanced row: TPS, Setup Potential, ECS, VWAP, OBV, HTF. Blocker shown in Action tooltip.</p>'
        );
    }

    function renderTopTable(rows, data) {
        if (data && (data.top_sections || data.sections || data.top_picks)) {
            return renderTopTableFromPayload(data);
        }
        if (!rows || !rows.length) {
            return renderTopTableFromPayload({ top_picks: [] });
        }
        return renderTopTableFromPayload({ top_picks: rows.slice(0, TOP_N), top_sections: {} });
    }

    function sortRows(rows, sortKey, sortDir) {
        const dir = sortDir === 'asc' ? 1 : -1;
        const col = TOP_COLUMNS.find(function (c) {
            return c.key === sortKey;
        });
        return rows.slice().sort(function (a, b) {
            if (sortKey === 'security') {
                const av = String(a.security || a.stock || '');
                const bv = String(b.security || b.stock || '');
                return av.localeCompare(bv, undefined, { numeric: true }) * dir;
            }
            if (
                sortKey === 'tps_score' ||
                sortKey === 'ees_score' ||
                sortKey === 'ecs_score' ||
                sortKey === 'confidence'
            ) {
                const av = Number(a[sortKey] != null ? a[sortKey] : a.confidence);
                const bv = Number(b[sortKey] != null ? b[sortKey] : b.confidence);
                return ((Number.isFinite(av) ? av : -1) - (Number.isFinite(bv) ? bv : -1)) * dir;
            }
            if (sortKey === 'qualification') {
                const av = modalQualificationSortRank(a);
                const bv = modalQualificationSortRank(b);
                const tier = (av - bv) * dir;
                if (tier !== 0) return tier;
                if (
                    qualificationOf(a) === QUAL_ARMED &&
                    qualificationOf(b) === QUAL_ARMED
                ) {
                    const armedCmp = compareArmedRank(a, b) * (sortDir === 'asc' ? -1 : 1);
                    if (armedCmp !== 0) return armedCmp;
                }
                return (
                    String(a.security || a.stock || '').localeCompare(
                        String(b.security || b.stock || ''),
                        undefined,
                        { numeric: true }
                    ) * dir
                );
            }
            if (sortKey === 'setup_quality_score' || sortKey === 'armed_rank_score') {
                const av = armedRankValue(a);
                const bv = armedRankValue(b);
                return (av - bv) * dir;
            }
            const av = chipDisplayValue(col || { key: sortKey }, a);
            const bv = chipDisplayValue(col || { key: sortKey }, b);
            return String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: 'base' }) * dir;
        });
    }

    function sortIndicator(sortKey, sortDir, colKey) {
        if (sortKey !== colKey) return '<span class="vajra-sort-ind" aria-hidden="true">↕</span>';
        return sortDir === 'asc'
            ? '<span class="vajra-sort-ind vajra-sort-ind--active" aria-hidden="true">▲</span>'
            : '<span class="vajra-sort-ind vajra-sort-ind--active" aria-hidden="true">▼</span>';
    }

    function renderModalTable(rows, sortKey, sortDir) {
        const colCount = 1 + TOP_COLUMNS.length;
        let thead =
            '<thead><tr>' +
            '<th scope="col" class="vajra-sort-th" data-sort-key="security" role="columnheader" aria-sort="' +
            (sortKey === 'security' ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none') +
            '" tabindex="0">Security' +
            sortIndicator(sortKey, sortDir, 'security') +
            '</th>';
        TOP_COLUMNS.forEach(function (col) {
            const thNum = col.num ? ' num' : '';
            thead +=
                '<th scope="col" class="vajra-sort-th' +
                thNum +
                '" data-sort-key="' +
                escapeHtml(col.key) +
                '" role="columnheader" aria-sort="' +
                (sortKey === col.key ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none') +
                '" tabindex="0">' +
                escapeHtml(col.label) +
                sortIndicator(sortKey, sortDir, col.key) +
                '</th>';
        });
        thead += '</tr></thead>';
        let tbody = '';
        if (!rows.length) {
            tbody =
                '<tr><td colspan="' +
                colCount +
                '" class="vajra-meta">No additional ratings.</td></tr>';
        } else {
            tbody = renderTableBodyRows(rows, TOP_COLUMNS, false);
        }
        return (
            '<p class="vajra-meta vajra-pipeline-note">Full universe — ARMED → DISCOVERY → REJECT (ARMED by setup rank)</p>' +
            '<div class="vajra-table-wrap"><table class="vajra-table vajra-top-table vajra-modal-table">' +
            thead +
            '<tbody>' +
            tbody +
            '</tbody></table></div>' +
            '<p class="vajra-score-footnote">' +
            '<strong>TPS</strong> = Transition Potential Score (30m discovery) · ' +
            '<strong>EES</strong> = Executable Entry Score (5m timing) · ' +
            '<strong>ECS</strong> = Expansion Confirmation Score' +
            '</p>'
        );
    }

    function ensureModal(prefix) {
        const modalId = prefix + 'VajraMoreModal';
        let modal = document.getElementById(modalId);
        if (modal) return modal;
        modal = document.createElement('div');
        modal.id = modalId;
        modal.className = 'vajra-modal';
        modal.setAttribute('aria-hidden', 'true');
        modal.setAttribute('role', 'dialog');
        modal.setAttribute('aria-modal', 'true');
        modal.setAttribute('aria-labelledby', prefix + 'VajraMoreTitle');
        modal.innerHTML =
            '<div class="vajra-modal-backdrop" data-vajra-close="1"></div>' +
            '<div class="vajra-modal-panel">' +
            '<h3 id="' + prefix + 'VajraMoreTitle" class="vajra-modal-title">Vajra transition ratings</h3>' +
            '<p class="vajra-meta vajra-modal-sub" id="' + prefix + 'VajraMoreSub"></p>' +
            '<div id="' + prefix + 'VajraMoreTable" class="vajra-modal-body"></div>' +
            '<div class="vajra-modal-actions">' +
            '<button type="button" class="vajra-modal-close-btn" data-vajra-close="1">Close</button>' +
            '</div></div>';
        document.body.appendChild(modal);
        return modal;
    }

    const RATINGS_FETCH_TIMEOUT_MS = 90000;

    async function fetchJsonWithTimeout(url, options, timeoutMs) {
        const ms = timeoutMs != null ? timeoutMs : RATINGS_FETCH_TIMEOUT_MS;
        const ctrl = new AbortController();
        const timer = global.setTimeout(function () {
            ctrl.abort();
        }, ms);
        try {
            const res = await fetch(url, Object.assign({}, options || {}, { signal: ctrl.signal }));
            const data = await res.json();
            return { res: res, data: data };
        } catch (e) {
            if (e && e.name === 'AbortError') {
                throw new Error('Request timed out — try Refresh in a moment');
            }
            throw e;
        } finally {
            global.clearTimeout(timer);
        }
    }

    async function fetchRatings(scanTf, htf) {
        const q =
            '?mode=transition&scan_tf=' +
            encodeURIComponent(scanTf || DEFAULT_SCAN_TF) +
            '&htf=' +
            encodeURIComponent(htf || DEFAULT_HTF) +
            cacheBustQ();
        const paths = [API_BASE + '/api/vajra-futures/ratings' + q, API_BASE + '/vajra-futures/ratings' + q];
        let lastErr = null;
        for (let i = 0; i < paths.length; i++) {
            try {
                const out = await fetchJsonWithTimeout(
                    paths[i],
                    { headers: authHeaders(), cache: 'no-store' },
                    RATINGS_FETCH_TIMEOUT_MS
                );
                const res = out.res;
                const data = out.data;
                if (!res.ok) {
                    lastErr = data && data.message ? data.message : res.statusText;
                    continue;
                }
                return data;
            } catch (e) {
                lastErr = e.message || String(e);
            }
        }
        throw new Error(lastErr || 'Failed to load Vajra ratings');
    }

    async function fetchRatingsStatus() {
        const paths = [
            API_BASE + '/api/vajra-futures/ratings-status' + cacheBustQ(),
            API_BASE + '/vajra-futures/ratings-status' + cacheBustQ(),
        ];
        let lastErr = null;
        for (let i = 0; i < paths.length; i++) {
            try {
                const res = await fetch(paths[i], { headers: authHeaders(), cache: 'no-store' });
                const data = await res.json();
                if (!res.ok) {
                    lastErr = data && data.message ? data.message : res.statusText;
                    continue;
                }
                return data;
            } catch (e) {
                lastErr = e.message || String(e);
            }
        }
        throw new Error(lastErr || 'Failed to load Vajra ratings status');
    }

    function isVajraTelegramEnabled() {
        try {
            const raw = global.localStorage.getItem('trademanthan_settings');
            if (!raw) return false;
            const settings = JSON.parse(raw);
            const tg = settings && settings.notifications && settings.notifications.telegram;
            if (!tg || !tg.enabled) return false;
            const types = tg.types || {};
            return types.vajraEnter === true;
        } catch (e) {
            return false;
        }
    }

    function loadEnterSeenForSession(sessionDate) {
        const day = String(sessionDate || 'unknown');
        try {
            const raw = global.sessionStorage.getItem(VAJRA_ENTER_SEEN_KEY);
            const all = raw ? JSON.parse(raw) : {};
            return all[day] || {};
        } catch (e) {
            return {};
        }
    }

    function saveEnterSeenForSession(sessionDate, seen) {
        const day = String(sessionDate || 'unknown');
        try {
            const raw = global.sessionStorage.getItem(VAJRA_ENTER_SEEN_KEY);
            const all = raw ? JSON.parse(raw) : {};
            all[day] = seen;
            global.sessionStorage.setItem(VAJRA_ENTER_SEEN_KEY, JSON.stringify(all));
        } catch (e) {
            /* ignore */
        }
    }

    /** One Telegram message when ENTER becomes enabled for symbols (first time per session). */
    function processEnterTelegramAlerts(rows, sessionDate) {
        if (!isVajraTelegramEnabled()) return;
        if (typeof global.notifyTelegramUserMessage !== 'function') return;
        const seen = loadEnterSeenForSession(sessionDate);
        const newly = [];
        (rows || []).forEach(function (r) {
            if (!isEnterRow(r)) return;
            const sym = String(r.security || r.stock || '').trim();
            if (!sym || seen[sym]) return;
            seen[sym] = true;
            newly.push(sym);
        });
        if (!newly.length) return;
        saveEnterSeenForSession(sessionDate, seen);
        const msg =
            'Vajra ENTER ready (' +
            newly.length +
            '):\n' +
            newly
                .map(function (s, i) {
                    return i + 1 + '. ' + s;
                })
                .join('\n');
        global.notifyTelegramUserMessage(msg).catch(function () {});
    }

    function init(opts) {
        const prefix = opts.prefix || 'df';
        const listEl = document.getElementById(opts.listElId || prefix + 'VajraTable');
        const moreBtn = document.getElementById(opts.moreBtnId || prefix + 'VajraMoreBtn');
        const refreshBtn = document.getElementById(opts.refreshBtnId || prefix + 'VajraRefreshBtn');
        const metaEl = opts.metaElId ? document.getElementById(opts.metaElId) : null;
        const msgEl = opts.msgElId ? document.getElementById(opts.msgElId) : null;
        const scanTfEl = document.getElementById(prefix + 'VajraScanTf');
        const htfEl = document.getElementById(prefix + 'VajraHtf');
        if (scanTfEl) {
            const scanLbl = scanTfEl.closest('.vajra-tf-label');
            if (scanLbl) scanLbl.style.display = 'none';
        }
        if (htfEl) {
            const htfLbl = htfEl.closest('.vajra-tf-label');
            if (htfLbl) htfLbl.style.display = 'none';
        }
        const modal = ensureModal(prefix);
        const modalTableEl = document.getElementById(prefix + 'VajraMoreTable');
        const modalSubEl = document.getElementById(prefix + 'VajraMoreSub');

        let allRows = [];
        let _lastRawData = null;
        let modalRows = [];
        let sortKey = 'tps_score';
        let sortDir = 'desc';
        const seenAlertKeys = {};
        let lastComputedEpoch = null;
        let lastFullLoadMs = 0;
        let loadInFlight = false;
        const STALE_DATA_SEC = 420;
        const FORCE_RELOAD_MS = 300000;

        function renderLoadedData(data) {
            const activeSet = getActiveTradeStocks();
            const filtered = filterRatingsPayload(data, activeSet);
            allRows = filtered.rows || [];
            const composed = composeTop8ScreenRows(filtered);
            const top8Keys = {};
            (composed.top8 || []).forEach(function (r) {
                const k = rowStockKey(r);
                if (k) top8Keys[k] = true;
            });
            global._vajraRemainder = (filtered.remainder || allRows).filter(function (r) {
                const k = rowStockKey(r);
                return !k || !top8Keys[k];
            });
            if (listEl) {
                listEl._vajraTopRows = composed.top8 || [];
                listEl.innerHTML = renderTopTable(null, filtered);
                bindSecurityChartClicks(listEl);
                listEl.querySelectorAll('[data-vajra-enter]').forEach(function (btn) {
                    btn.addEventListener('click', function (ev) {
                        const sym = ev.currentTarget.getAttribute('data-vajra-stock');
                        const row = (listEl._vajraTopRows || []).find(function (r) {
                            return String(r.stock || r.security) === sym;
                        });
                        if (row && global.VajraTradeWorkflow && global.VajraTradeWorkflow.openEntry) {
                            global.VajraTradeWorkflow.openEntry(row);
                        }
                    });
                });
            }
            if (moreBtn) {
                const rest = (filtered.remainder || allRows).length;
                moreBtn.hidden = rest <= 0;
                moreBtn.textContent = rest > 0 ? 'more… (' + rest + ')' : 'more…';
            }
            if (metaEl) {
                let meta =
                    'Session: ' +
                    (filtered.session_date || data.session_date || '—') +
                    ' · Updated: ' +
                    fmtUpdated(
                        filtered.computed_at || data.computed_at || (allRows[0] && allRows[0].computed_at)
                    ) +
                    ' · ' +
                    allRows.length +
                    ' symbols · 30m TPS · EES/Entry every ' +
                    (filtered.ees_refresh_minutes || data.ees_refresh_minutes || 5) +
                    'm · HTF ' +
                    (filtered.htf_bias_tf || data.htf_bias_tf || '1hr') +
                    (filtered.alert_count != null || data.alert_count != null
                        ? ' · Alerts: ' + (filtered.alert_count != null ? filtered.alert_count : data.alert_count)
                        : '');
                if (
                    (filtered.data_age_sec != null && filtered.data_age_sec > STALE_DATA_SEC) ||
                    (data.data_age_sec != null && data.data_age_sec > STALE_DATA_SEC)
                ) {
                    const age =
                        filtered.data_age_sec != null ? filtered.data_age_sec : data.data_age_sec;
                    meta += ' · ⚠ data ' + Math.round(age / 60) + 'm old';
                }
                if (filtered.stale_reason || data.stale_reason || filtered.source === 'db_stale' || data.source === 'db_stale') {
                    meta +=
                        ' · Showing last saved scan' +
                        (filtered.stale_reason || data.stale_reason
                            ? ' (' + (filtered.stale_reason || data.stale_reason) + ')'
                            : '') +
                        ' — 5m job will refresh';
                }
                if (filtered.source === 'empty' || data.source === 'empty') {
                    meta += ' · No saved scan yet — wait for next 5m cycle or click Refresh';
                }
                if (activeSet.size) {
                    meta += ' · ' + activeSet.size + ' in open position (hidden from screen)';
                }
                metaEl.textContent = meta;
            }
            processEnterTelegramAlerts(allRows, filtered.session_date || data.session_date);
            if (modal.classList.contains('vajra-modal--open')) {
                modalRows = global._vajraRemainder || allRows.slice(TOP_N);
                renderModal();
            }
        }

        function refilterForActivePositions() {
            if (!_lastRawData) return;
            renderLoadedData(_lastRawData);
        }

        function openModal() {
            modalRows = sortModalByQualification(
                (window._vajraRemainder || allRows).slice()
            );
            sortKey = 'qualification';
            sortDir = 'desc';
            renderModal();
            if (modalSubEl) {
                modalSubEl.textContent =
                    modalRows.length +
                    ' symbols · ARMED → DISCOVERY → REJECT (ARMED ranked like Top 8). Click headers to sort.';
            }
            modal.setAttribute('aria-hidden', 'false');
            modal.classList.add('vajra-modal--open');
        }

        function closeModal() {
            modal.setAttribute('aria-hidden', 'true');
            modal.classList.remove('vajra-modal--open');
        }

        function renderModal() {
            if (!modalTableEl) return;
            modalTableEl.innerHTML = renderModalTable(sortRows(modalRows, sortKey, sortDir), sortKey, sortDir);
            bindSecurityChartClicks(modalTableEl);
            modalTableEl.querySelectorAll('.vajra-sort-th').forEach(function (th) {
                th.addEventListener('click', function () {
                    const key = th.getAttribute('data-sort-key');
                    if (!key) return;
                    if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                    else {
                        sortKey = key;
                        sortDir =
                            key === 'tps_score' || key === 'ecs_score' || key === 'trade_type' ? 'desc' : 'asc';
                    }
                    renderModal();
                });
                th.addEventListener('keydown', function (ev) {
                    if (ev.key === 'Enter' || ev.key === ' ') {
                        ev.preventDefault();
                        th.click();
                    }
                });
            });
        }

        if (moreBtn) {
            moreBtn.addEventListener('click', function () {
                if (!moreBtn.hidden) openModal();
            });
        }
        if (refreshBtn) {
            refreshBtn.addEventListener('click', function () {
                load(true);
            });
        }

        modal.querySelectorAll('[data-vajra-close]').forEach(function (el) {
            el.addEventListener('click', closeModal);
        });
        document.addEventListener('keydown', function (ev) {
            if (ev.key === 'Escape' && modal.classList.contains('vajra-modal--open')) closeModal();
        });

        async function load(force) {
            if (loadInFlight && !force) return;
            loadInFlight = true;
            if (msgEl) msgEl.textContent = 'Loading transition scan (30m + 5m)…';
            try {
                const data = await fetchRatings(DEFAULT_SCAN_TF, DEFAULT_HTF);
                _lastRawData = data;
                const computedIso =
                    (data && data.computed_at) ||
                    (data && data.rows && data.rows[0] && data.rows[0].computed_at) ||
                    null;
                const ep = tsEpoch(computedIso);
                if (ep != null) lastComputedEpoch = ep;
                lastFullLoadMs = Date.now();
                renderLoadedData(data);
                if (msgEl) msgEl.textContent = '';
            } catch (e) {
                if (listEl) listEl.innerHTML = '';
                if (moreBtn) moreBtn.hidden = true;
                if (msgEl) msgEl.textContent = 'Vajra: ' + (e.message || String(e));
            } finally {
                loadInFlight = false;
            }
        }

        async function checkForScheduledUpdate() {
            if (loadInFlight) return;
            const nowMs = Date.now();
            if (lastFullLoadMs && nowMs - lastFullLoadMs >= FORCE_RELOAD_MS) {
                await load();
                return;
            }
            try {
                const st = await fetchRatingsStatus();
                const ep = tsEpoch(st && st.computed_at);
                if (ep == null) return;
                const ageSec = st && st.data_age_sec != null ? Number(st.data_age_sec) : null;
                if (lastComputedEpoch == null) {
                    lastComputedEpoch = ep;
                    return;
                }
                if (ep !== lastComputedEpoch || (ageSec != null && ageSec > STALE_DATA_SEC)) {
                    await load();
                }
            } catch (e) {
                /* status poll is best-effort; force reload still runs on interval */
            }
        }

        document.addEventListener('vajra:active-positions-changed', refilterForActivePositions);

        load();
        const watchMs = opts.watchMs != null ? Number(opts.watchMs) : 20000;
        if (watchMs > 0) setInterval(checkForScheduledUpdate, watchMs);
        setInterval(load, FORCE_RELOAD_MS);
        document.addEventListener('visibilitychange', function () {
            if (document.visibilityState === 'visible') checkForScheduledUpdate();
        });
        const poll = opts.pollMs != null ? Number(opts.pollMs) : 0;
        if (poll > 0) setInterval(load, poll);
        return { refresh: load, openModal: openModal, closeModal: closeModal };
    }

    global.VajraFuturesRatings = {
        init: init,
        fetchRatings: fetchRatings,
        fetchRatingsStatus: fetchRatingsStatus,
        TOP_N: TOP_N,
        renderSecurityChartLink: renderSecurityChartLink,
        bindSecurityChartClicks: bindSecurityChartClicks,
        buildScreenerFromRow: buildScreenerFromRow,
        buildScreenerFromTrade: buildScreenerFromTrade,
    };
})(window);
