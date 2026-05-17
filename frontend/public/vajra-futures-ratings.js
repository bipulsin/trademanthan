/**
 * TWCTO Vajra — futures trade qualification (Daily + Smart Futures).
 * Top 5 table with column headers once; value-only chips; modal for the rest.
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
        { key: 'trade_type', label: 'Trade Type', chip: true },
        { key: 'confidence', label: 'Confidence', chip: true },
        { key: 'structure', label: 'Structure', chip: true },
        { key: 'momentum', label: 'Momentum', chip: true },
        { key: 'trend', label: 'Trend', chip: true },
        { key: 'volume', label: 'Volume', chip: true },
        { key: 'obv', label: 'OBV', chip: true },
        { key: 'market_phase', label: 'Market Phase', chip: true },
        { key: 'reversal_risk', label: 'Reversal Risk', chip: true },
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
        if (col.key === 'obv') {
            const o = String(row.obv || raw || '').toUpperCase();
            if (o.indexOf('ABOVE') >= 0 && o.indexOf('RISING') >= 0) return 'Above MA';
            if (o.indexOf('BELOW') >= 0 && o.indexOf('FALLING') >= 0) return 'Below MA-Falling';
            if (o.indexOf('ABOVE') >= 0) return 'Above MA';
            if (o.indexOf('BELOW') >= 0) return 'Below MA-Falling';
            if (o.indexOf('FLAT') >= 0) return 'Flat';
            return String(row.obv || raw || '—').trim();
        }
        return raw;
    }

    function chipToneClass(col, row) {
        const raw = cellValue(row, col);
        if (col.key === 'reversal_risk') {
            const u = String(row.reversal_risk || raw || '').toUpperCase();
            if (u === 'HIGH') return 'df-dir-short';
            if (u === 'MEDIUM') return 'vajra-rev-med';
            return 'df-dir-long';
        }
        if (col.key === 'trade_type') {
            const s = String(row.trade_type || '');
            if (s.indexOf('SHORT') === 0) return 'df-dir-short';
            if (s.indexOf('LONG') === 0) return 'df-dir-long';
            return 'df-dir-neutral';
        }
        if (col.key === 'structure' || col.key === 'momentum' || col.key === 'trend' || col.key === 'volume') {
            return isPassText(raw) ? 'df-dir-long' : 'df-dir-short';
        }
        if (col.key === 'obv') {
            const o = String(row.obv || raw || '').toUpperCase();
            if (o.indexOf('BELOW') >= 0 || o.indexOf('FALLING') >= 0) return 'df-dir-short';
            if (o.indexOf('ABOVE') >= 0 || o.indexOf('RISING') >= 0) return 'df-dir-long';
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
            escapeHtml(col.label) +
            '">' +
            escapeHtml(text) +
            '</span>'
        );
    }

    function renderTableBodyRows(rows) {
        let tbody = '';
        rows.forEach(function (r) {
            tbody += '<tr>';
            tbody += '<td class="vajra-td-security">' + escapeHtml(cellValue(r, { key: 'security' })) + '</td>';
            CHIP_COLUMNS.forEach(function (col) {
                const tdClass = col.key === 'confidence' ? 'vajra-td-chip num' : 'vajra-td-chip';
                tbody += '<td class="' + tdClass + '">' + renderChip(col, r) + '</td>';
            });
            tbody += '</tr>';
        });
        return tbody;
    }

    function renderTableHead() {
        let head = '<thead><tr><th scope="col">Security</th>';
        CHIP_COLUMNS.forEach(function (col) {
            const thClass = col.key === 'confidence' ? 'num' : '';
            head += '<th scope="col" class="' + thClass + '">' + escapeHtml(col.label) + '</th>';
        });
        head += '</tr></thead>';
        return head;
    }

    function renderTopTable(rows) {
        if (!rows || !rows.length) {
            return '<p class="vajra-meta">No Vajra ratings yet for this session. The engine runs every 15 minutes (9:30–15:00 IST).</p>';
        }
        const top = rows.slice(0, TOP_N);
        return (
            '<div class="vajra-table-wrap"><table class="vajra-table vajra-top-table">' +
            renderTableHead() +
            '<tbody>' +
            renderTableBodyRows(top) +
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
            if (sortKey === 'confidence') {
                const av = Number(a.confidence);
                const bv = Number(b.confidence);
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
            const thNum = col.key === 'confidence' ? ' num' : '';
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
            tbody = renderTableBodyRows(rows).replace(/<\/?tbody>/g, '');
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
            modalTableEl.innerHTML = renderModalTable(sortRows(modalRows, sortKey, sortDir), sortKey, sortDir);
            modalTableEl.querySelectorAll('.vajra-sort-th').forEach(function (th) {
                th.addEventListener('click', function () {
                    const key = th.getAttribute('data-sort-key');
                    if (!key) return;
                    if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
                    else {
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
                if (listEl) listEl.innerHTML = renderTopTable(allRows);
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
