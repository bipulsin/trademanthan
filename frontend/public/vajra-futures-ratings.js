/**
 * TWCTO Vajra — futures trade qualification (Daily + Smart Futures).
 * Top 5 as security + chips; "more…" modal for remainder with sortable columns.
 */
(function (global) {
    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;

    const TOP_N = 5;

    const TRADE_TYPE_ORDER = {
        'LONG  [A+]': 0,
        LONG: 1,
        'SHORT [A+]': 2,
        SHORT: 3,
        'LONG WATCH': 4,
        'SHORT WATCH': 5,
        REJECT: 6,
    };

    const MODAL_COLUMNS = [
        { key: 'security', label: 'Security', chip: false },
        { key: 'trade_type', label: 'Trade Type', chip: true, chipClass: tradeTypeClass },
        { key: 'confidence', label: 'Confidence', chip: true, numeric: true },
        { key: 'structure', label: 'Structure', chip: true, chipClass: passClass },
        { key: 'momentum', label: 'Momentum', chip: true, chipClass: passClass },
        { key: 'trend', label: 'Trend', chip: true, chipClass: passClass },
        { key: 'volume', label: 'Volume', chip: true, chipClass: passClass },
        { key: 'obv', label: 'OBV', chip: true },
        { key: 'market_phase', label: 'Market Phase', chip: true },
        { key: 'reversal_risk', label: 'Reversal Risk', chip: true, chipClass: revRiskClass },
    ];

    const CHIP_COLUMNS = MODAL_COLUMNS.filter(function (c) {
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

    function tradeTypeClass(tt) {
        const s = String(tt || '');
        if (s.indexOf('LONG') === 0) return 'vajra-tt-long';
        if (s.indexOf('SHORT') === 0) return 'vajra-tt-short';
        if (s === 'REJECT') return 'vajra-tt-reject';
        return 'vajra-tt-watch';
    }

    function revRiskClass(r) {
        const u = String(r || '').toUpperCase();
        if (u === 'HIGH') return 'vajra-rev-high';
        if (u === 'MEDIUM') return 'vajra-rev-med';
        return 'vajra-rev-low';
    }

    function passClass(cell) {
        return String(cell || '').indexOf('PASS') >= 0 ? 'vajra-pass' : 'vajra-fail';
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function cellValue(r, col) {
        if (col.key === 'security') return r.security || r.stock || '—';
        if (col.key === 'confidence') {
            return r.confidence != null && r.confidence !== '' ? Number(r.confidence).toFixed(1) : '—';
        }
        return r[col.key] != null && r[col.key] !== '' ? String(r[col.key]) : '—';
    }

    function chipExtraClass(col, value, row) {
        if (col.chipClass) return col.chipClass(col.key === 'trade_type' ? row.trade_type : value);
        if (col.key === 'trade_type') return tradeTypeClass(row.trade_type);
        return '';
    }

    function renderChip(col, row) {
        const val = cellValue(row, col);
        const extra = chipExtraClass(col, val, row);
        return (
            '<span class="vajra-chip' +
            (extra ? ' ' + escapeHtml(extra) : '') +
            '" title="' +
            escapeHtml(col.label) +
            '">' +
            '<span class="vajra-chip-lbl">' +
            escapeHtml(col.label) +
            '</span>' +
            '<span class="vajra-chip-val">' +
            escapeHtml(val) +
            '</span></span>'
        );
    }

    function renderRowCard(row) {
        let chips = '';
        CHIP_COLUMNS.forEach(function (col) {
            chips += renderChip(col, row);
        });
        return (
            '<article class="vajra-row-card">' +
            '<div class="vajra-row-security">' +
            escapeHtml(row.security || row.stock || '—') +
            '</div>' +
            '<div class="vajra-chips">' +
            chips +
            '</div>' +
            '</article>'
        );
    }

    function renderTopList(rows) {
        if (!rows || !rows.length) {
            return '<p class="vajra-meta">No Vajra ratings yet for this session. The engine runs every 15 minutes (9:30–15:00 IST).</p>';
        }
        const top = rows.slice(0, TOP_N);
        let html = '<div class="vajra-top-list">';
        top.forEach(function (r) {
            html += renderRowCard(r);
        });
        html += '</div>';
        return html;
    }

    function sortRows(rows, sortKey, sortDir) {
        const dir = sortDir === 'asc' ? 1 : -1;
        const col = MODAL_COLUMNS.find(function (c) {
            return c.key === sortKey;
        });
        return rows.slice().sort(function (a, b) {
            let av;
            let bv;
            if (sortKey === 'security') {
                av = String(a.security || a.stock || '');
                bv = String(b.security || b.stock || '');
            } else if (sortKey === 'confidence') {
                av = Number(a.confidence);
                bv = Number(b.confidence);
                if (!Number.isFinite(av)) av = -1;
                if (!Number.isFinite(bv)) bv = -1;
                return (av - bv) * dir;
            } else if (sortKey === 'trade_type') {
                av = TRADE_TYPE_ORDER[String(a.trade_type || '')] ?? 99;
                bv = TRADE_TYPE_ORDER[String(b.trade_type || '')] ?? 99;
                return (av - bv) * dir;
            } else {
                av = cellValue(a, col || { key: sortKey });
                bv = cellValue(b, col || { key: sortKey });
            }
            const cmp = String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: 'base' });
            return cmp * dir;
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
            thead +=
                '<th scope="col" class="vajra-sort-th" data-sort-key="' +
                escapeHtml(col.key) +
                '" role="columnheader" aria-sort="' +
                (sortKey === col.key ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none') +
                '" tabindex="0">' +
                escapeHtml(col.label) +
                sortIndicator(sortKey, sortDir, col.key) +
                '</th>';
        });
        thead += '</tr></thead><tbody>';
        let tbody = '';
        if (!rows.length) {
            tbody = '<tr><td colspan="' + MODAL_COLUMNS.length + '" class="vajra-meta">No additional ratings.</td></tr>';
        } else {
            rows.forEach(function (r) {
                tbody += '<tr>';
                MODAL_COLUMNS.forEach(function (col) {
                    if (col.key === 'security') {
                        tbody += '<td class="vajra-td-security">' + escapeHtml(cellValue(r, col)) + '</td>';
                    } else {
                        tbody += '<td class="vajra-td-chip">' + renderChip(col, r) + '</td>';
                    }
                });
                tbody += '</tr>';
            });
        }
        return (
            '<div class="vajra-modal-table-wrap"><table class="vajra-table vajra-modal-table">' +
            thead +
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
            '<h3 id="' + prefix + 'VajraMoreTitle" class="vajra-modal-title">Vajra ratings</h3>' +
            '<p class="vajra-meta vajra-modal-sub" id="' + prefix + 'VajraMoreSub"></p>' +
            '<div id="' + prefix + 'VajraMoreTable" class="vajra-modal-body"></div>' +
            '<div class="vajra-modal-actions">' +
            '<button type="button" class="vajra-modal-close-btn" data-vajra-close="1">Close</button>' +
            '</div></div>';
        document.body.appendChild(modal);
        return modal;
    }

    async function fetchRatings() {
        const paths = [API_BASE + '/api/vajra-futures/ratings', API_BASE + '/vajra-futures/ratings'];
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

    /**
     * @param {object} opts
     * @param {string} opts.prefix — 'df' or 'sf' for unique modal/button ids
     * @param {string} opts.listElId — container for top-5 list
     * @param {string} [opts.moreBtnId]
     * @param {string} [opts.metaElId]
     * @param {string} [opts.msgElId]
     * @param {number} [opts.pollMs]
     */
    function init(opts) {
        const prefix = opts.prefix || 'df';
        const listEl = document.getElementById(opts.listElId || prefix + 'VajraTable');
        const moreBtn = document.getElementById(opts.moreBtnId || prefix + 'VajraMoreBtn');
        const metaEl = opts.metaElId ? document.getElementById(opts.metaElId) : null;
        const msgEl = opts.msgElId ? document.getElementById(opts.msgElId) : null;
        const modal = ensureModal(prefix);
        const modalTableEl = document.getElementById(prefix + 'VajraMoreTable');
        const modalSubEl = document.getElementById(prefix + 'VajraMoreSub');

        let allRows = [];
        let modalRows = [];
        let sortKey = 'confidence';
        let sortDir = 'desc';

        function openModal() {
            modalRows = allRows.slice(TOP_N);
            sortKey = 'confidence';
            sortDir = 'desc';
            renderModal();
            if (modalSubEl) {
                modalSubEl.textContent =
                    modalRows.length +
                    ' additional symbol' +
                    (modalRows.length === 1 ? '' : 's') +
                    ' (excluding top ' +
                    TOP_N +
                    '). Click a column header to sort.';
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
            const sorted = sortRows(modalRows, sortKey, sortDir);
            modalTableEl.innerHTML = renderModalTable(sorted, sortKey, sortDir);
            modalTableEl.querySelectorAll('.vajra-sort-th').forEach(function (th) {
                th.addEventListener('click', function () {
                    const key = th.getAttribute('data-sort-key');
                    if (!key) return;
                    if (sortKey === key) {
                        sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                    } else {
                        sortKey = key;
                        sortDir = key === 'confidence' || key === 'trade_type' ? 'desc' : 'asc';
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
            if (msgEl) msgEl.textContent = 'Loading Vajra ratings…';
            try {
                const data = await fetchRatings();
                allRows = (data && data.rows) || [];
                if (listEl) listEl.innerHTML = renderTopList(allRows);
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
                        ' symbols';
                }
                if (msgEl) msgEl.textContent = '';
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
        const poll = opts.pollMs != null ? Number(opts.pollMs) : 120000;
        if (poll > 0) setInterval(load, poll);
        return { refresh: load, openModal: openModal, closeModal: closeModal };
    }

    global.VajraFuturesRatings = { init: init, fetchRatings: fetchRatings, TOP_N: TOP_N };
})(window);
