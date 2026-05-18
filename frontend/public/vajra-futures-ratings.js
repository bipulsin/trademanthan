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

    const TRADE_TYPE_ORDER = {
        'EARLY LONG TRANSITION': 0,
        'EARLY SHORT TRANSITION': 1,
        'LONG  [A+]': 2,
        LONG: 3,
        'SHORT [A+]': 4,
        SHORT: 5,
        'LONG WATCH': 6,
        'SHORT WATCH': 7,
        REJECT: 8,
    };

    const TOP_COLUMNS = [
        { key: 'trade_type', label: 'Status', chip: true },
        { key: 'tps_score', label: 'TPS', chip: true, num: true },
        { key: 'ecs_score', label: 'ECS', chip: true, num: true },
        { key: 'transition_state', label: 'Transition', chip: true },
        { key: 'vwap_reclaim_status', label: 'VWAP', chip: true },
        { key: 'pullback_quality_score', label: 'Pullback', chip: true, num: true },
        { key: 'extension_risk_score', label: 'Extension', chip: true, num: true },
    ];

    const MODAL_COLUMNS = [
        { key: 'security', label: 'Security', chip: false },
        { key: 'trade_type', label: 'Status', chip: true },
        { key: 'tps_score', label: 'TPS', chip: true, num: true },
        { key: 'ecs_score', label: 'ECS', chip: true, num: true },
        { key: 'transition_state', label: 'Transition State', chip: true },
        { key: 'vwap_reclaim_status', label: 'VWAP Reclaim', chip: true },
        { key: 'ema_reclaim_status', label: 'EMA Reclaim', chip: true },
        { key: 'rsi_transition_status', label: 'RSI Transition', chip: true },
        { key: 'pullback_quality_score', label: 'Pullback Q', chip: true, num: true },
        { key: 'extension_risk_score', label: 'Extension Risk', chip: true, num: true },
        { key: 'execution_step', label: '5m Step', chip: true },
        { key: 'structure', label: 'Structure', chip: true },
        { key: 'momentum', label: 'Momentum', chip: true },
        { key: 'trend', label: 'Trend', chip: true },
        { key: 'volume', label: 'Volume', chip: true },
        { key: 'obv', label: 'OBV', chip: true },
        { key: 'market_phase', label: 'Phase', chip: true },
        { key: 'reversal_risk', label: 'Rev Risk', chip: true },
    ];

    const CHIP_COLUMNS = TOP_COLUMNS.filter(function (c) {
        return c.chip;
    });

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

    function cellValue(r, col) {
        if (col.key === 'security') return r.security || r.stock || '—';
        if (col.num || col.key === 'confidence' || col.key === 'tps_score' || col.key === 'ecs_score') {
            return fmtNum(r[col.key] != null ? r[col.key] : r.confidence);
        }
        return r[col.key] != null && r[col.key] !== '' ? String(r[col.key]) : '—';
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
            const t = String(row.transition_state || raw || '—');
            return t.length > 28 ? t.slice(0, 26) + '…' : t;
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

    function chipToneClass(col, row) {
        const raw = cellValue(row, col);
        if (col.key === 'extension_risk_score') {
            const n = Number(row.extension_risk_score);
            if (n >= 65) return 'df-dir-short';
            if (n >= 40) return 'vajra-rev-med';
            return 'df-dir-long';
        }
        if (col.key === 'pullback_quality_score' || col.key === 'tps_score') {
            const n = Number(row[col.key]);
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
        if (col.key === 'trade_type') {
            const s = String(row.trade_type || '');
            if (s.indexOf('EARLY SHORT') === 0) return 'df-dir-short vajra-early-pill';
            if (s.indexOf('EARLY LONG') === 0) return 'df-dir-long vajra-early-pill';
            if (s.indexOf('SHORT') === 0) return 'df-dir-short';
            if (s.indexOf('LONG') === 0) return 'df-dir-long';
            return 'df-dir-neutral';
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

    function renderChip(col, row) {
        const tone = chipToneClass(col, row);
        const text = chipDisplayValue(col, row);
        return (
            '<span class="df-dir-pill ' +
            tone +
            '" title="' +
            escapeHtml(col.label + ': ' + (row[col.key] || '')) +
            '">' +
            escapeHtml(text) +
            '</span>'
        );
    }

    function renderTableBodyRows(rows, columns, showEnter) {
        const cols = columns || CHIP_COLUMNS;
        let tbody = '';
        rows.forEach(function (r, idx) {
            tbody += '<tr>';
            tbody += '<td class="vajra-td-security">' + escapeHtml(cellValue(r, { key: 'security' })) + '</td>';
            cols.forEach(function (col) {
                const tdClass = col.num ? 'vajra-td-chip num' : 'vajra-td-chip';
                tbody += '<td class="' + tdClass + '">' + renderChip(col, r) + '</td>';
            });
            if (showEnter) {
                tbody +=
                    '<td class="vajra-td-enter"><button type="button" class="vajra-enter-btn" data-vajra-enter="1" data-vajra-idx="' +
                    idx +
                    '">ENTER</button></td>';
            }
            tbody += '</tr>';
        });
        return tbody;
    }

    function renderTableHead(columns, showEnter) {
        const cols = columns || CHIP_COLUMNS;
        let head = '<thead><tr><th scope="col">Security</th>';
        cols.forEach(function (col) {
            const thClass = col.num ? 'num' : '';
            head += '<th scope="col" class="' + thClass + '">' + escapeHtml(col.label) + '</th>';
        });
        if (showEnter) head += '<th scope="col" class="vajra-th-enter">Action</th>';
        head += '</tr></thead>';
        return head;
    }

    function prioritizeRows(rows) {
        return rows.slice().sort(function (a, b) {
            const av = TRADE_TYPE_ORDER[String(a.trade_type || '')] ?? 99;
            const bv = TRADE_TYPE_ORDER[String(b.trade_type || '')] ?? 99;
            if (av !== bv) return av - bv;
            return (Number(b.tps_score) || 0) - (Number(a.tps_score) || 0);
        });
    }

    function renderTopTable(rows) {
        if (!rows || !rows.length) {
            return (
                '<p class="vajra-meta">No Vajra ratings for this session yet. ' +
                'The engine runs every 5 minutes (9:30–15:00 IST). ' +
                'If this persists after market open, use Refresh or wait for the next scan.</p>'
            );
        }
        const top = rows;
        return (
            '<p class="vajra-meta vajra-pipeline-note">30m discovery · 5m validation on shortlist</p>' +
            '<div class="vajra-table-wrap"><table class="vajra-table vajra-top-table">' +
            renderTableHead(TOP_COLUMNS, true) +
            '<tbody>' +
            renderTableBodyRows(top, TOP_COLUMNS, true) +
            '</tbody></table></div>'
        );
    }

    function sortRows(rows, sortKey, sortDir) {
        const dir = sortDir === 'asc' ? 1 : -1;
        const col = MODAL_COLUMNS.find(function (c) {
            return c.key === sortKey;
        });
        return rows.slice().sort(function (a, b) {
            if (sortKey === 'security') {
                const av = String(a.security || a.stock || '');
                const bv = String(b.security || b.stock || '');
                return av.localeCompare(bv, undefined, { numeric: true }) * dir;
            }
            if (sortKey === 'tps_score' || sortKey === 'ecs_score' || sortKey === 'confidence') {
                const av = Number(a[sortKey] != null ? a[sortKey] : a.confidence);
                const bv = Number(b[sortKey] != null ? b[sortKey] : b.confidence);
                return ((Number.isFinite(av) ? av : -1) - (Number.isFinite(bv) ? bv : -1)) * dir;
            }
            if (sortKey === 'trade_type') {
                const av = TRADE_TYPE_ORDER[String(a.trade_type || '')] ?? 99;
                const bv = TRADE_TYPE_ORDER[String(b.trade_type || '')] ?? 99;
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
        let thead = '<thead><tr>';
        MODAL_COLUMNS.forEach(function (col) {
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
                MODAL_COLUMNS.length +
                '" class="vajra-meta">No additional ratings.</td></tr>';
        } else {
            let body = '';
            rows.forEach(function (r) {
                body += '<tr><td class="vajra-td-security">' + escapeHtml(cellValue(r, { key: 'security' })) + '</td>';
                MODAL_COLUMNS.filter(function (c) {
                    return c.chip;
                }).forEach(function (col) {
                    const tdClass = col.num ? 'vajra-td-chip num' : 'vajra-td-chip';
                    body += '<td class="' + tdClass + '">' + renderChip(col, r) + '</td>';
                });
                body += '</tr>';
            });
            tbody = body;
        }
        return (
            '<div class="vajra-table-wrap"><table class="vajra-table vajra-modal-table">' +
            thead +
            '<tbody>' +
            tbody +
            '</tbody></table></div>'
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

    async function fetchRatings(scanTf, htf) {
        const q =
            '?mode=transition&scan_tf=' +
            encodeURIComponent(scanTf || DEFAULT_SCAN_TF) +
            '&htf=' +
            encodeURIComponent(htf || DEFAULT_HTF);
        const paths = [API_BASE + '/api/vajra-futures/ratings' + q, API_BASE + '/vajra-futures/ratings' + q];
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
        throw new Error(lastErr || 'Failed to load Vajra ratings');
    }

    function processAlerts(alerts, seenKeys) {
        if (!alerts || !alerts.length) return;
        alerts.forEach(function (r) {
            const key = (r.stock || r.security || '') + '|' + (r.trade_type || '');
            if (seenKeys[key]) return;
            seenKeys[key] = true;
            const tt = String(r.trade_type || '');
            if (tt.indexOf('EARLY') !== 0) return;
            const msg =
                'Vajra ' +
                tt +
                ': ' +
                (r.security || r.stock) +
                ' · TPS ' +
                fmtNum(r.tps_score) +
                ' · ' +
                (r.transition_state || '');
            if (typeof global.notifyTelegramUserMessage === 'function') {
                global.notifyTelegramUserMessage(msg).catch(function () {});
            }
        });
    }

    function init(opts) {
        const prefix = opts.prefix || 'df';
        const listEl = document.getElementById(opts.listElId || prefix + 'VajraTable');
        const moreBtn = document.getElementById(opts.moreBtnId || prefix + 'VajraMoreBtn');
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
        let modalRows = [];
        let sortKey = 'tps_score';
        let sortDir = 'desc';
        const seenAlertKeys = {};

        function openModal() {
            modalRows = allRows.slice(TOP_N);
            sortKey = 'tps_score';
            sortDir = 'desc';
            renderModal();
            if (modalSubEl) {
                modalSubEl.textContent =
                    modalRows.length +
                    ' symbols · TPS discovery (30m) + 5m validation on shortlist. Click headers to sort.';
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

        modal.querySelectorAll('[data-vajra-close]').forEach(function (el) {
            el.addEventListener('click', closeModal);
        });
        document.addEventListener('keydown', function (ev) {
            if (ev.key === 'Escape' && modal.classList.contains('vajra-modal--open')) closeModal();
        });

        async function load() {
            if (msgEl) msgEl.textContent = 'Loading transition scan (30m + 5m)…';
            try {
                const data = await fetchRatings(DEFAULT_SCAN_TF, DEFAULT_HTF);
                allRows = (data && data.rows) || [];
                if (listEl) {
                    const topRows = prioritizeRows(allRows).slice(0, TOP_N);
                    listEl._vajraTopRows = topRows;
                    listEl.innerHTML = renderTopTable(topRows);
                    listEl.querySelectorAll('[data-vajra-enter]').forEach(function (btn) {
                        btn.addEventListener('click', function (ev) {
                            const idx = parseInt(ev.currentTarget.getAttribute('data-vajra-idx'), 10);
                            const row = (listEl._vajraTopRows || [])[idx];
                            if (row && global.VajraTradeWorkflow && global.VajraTradeWorkflow.openEntry) {
                                global.VajraTradeWorkflow.openEntry(row);
                            }
                        });
                    });
                }
                if (moreBtn) {
                    const rest = Math.max(0, allRows.length - TOP_N);
                    moreBtn.hidden = rest <= 0;
                    moreBtn.textContent = rest > 0 ? 'more… (' + rest + ')' : 'more…';
                }
                if (metaEl) {
                    metaEl.textContent =
                        'Session: ' +
                        (data.session_date || '—') +
                        ' · Updated: ' +
                        fmtUpdated(data.computed_at || (allRows[0] && allRows[0].computed_at)) +
                        ' · ' +
                        allRows.length +
                        ' symbols · 30m TPS · 5m exec · HTF ' +
                        (data.htf_bias_tf || '1hr') +
                        (data.alert_count != null ? ' · Alerts: ' + data.alert_count : '');
                }
                if (msgEl) msgEl.textContent = '';
                processAlerts(data.alerts || allRows.filter(function (r) {
                    return r.alertable;
                }), seenAlertKeys);
                if (modal.classList.contains('vajra-modal--open')) {
                    modalRows = allRows.slice(TOP_N);
                    renderModal();
                }
            } catch (e) {
                if (listEl) listEl.innerHTML = '';
                if (moreBtn) moreBtn.hidden = true;
                if (msgEl) msgEl.textContent = 'Vajra: ' + (e.message || String(e));
            }
        }

        load();
        const poll = opts.pollMs != null ? Number(opts.pollMs) : 300000;
        if (poll > 0) setInterval(load, poll);
        return { refresh: load, openModal: openModal, closeModal: closeModal };
    }

    global.VajraFuturesRatings = { init: init, fetchRatings: fetchRatings, TOP_N: TOP_N };
})(window);
