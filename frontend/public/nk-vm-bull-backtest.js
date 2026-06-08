/**
 * Public NK VM Bull backtest viewer.
 * No login required — /nk-vm-bull-backtest.html
 */
(function () {
    'use strict';

    const API_PATHS = ['/nk-vm-bull-backtest/data', '/api/nk-vm-bull-backtest/data'];
    const COL_COUNT = 16;

    const state = {
        rows: [],
        meta: {},
        sort: { key: 'signal_time', dir: 'asc' },
        unchecked: new Set(),
    };

    function apiBase() {
        if (location.hostname === 'localhost' || location.hostname === '127.0.0.1') {
            return 'http://localhost:8000';
        }
        return location.origin;
    }

    async function loadData() {
        let lastErr = null;
        for (const path of API_PATHS) {
            try {
                const res = await fetch(apiBase() + path, { cache: 'no-store' });
                if (!res.ok) {
                    const j = await res.json().catch(function () { return {}; });
                    throw new Error(j.detail || res.statusText);
                }
                return await res.json();
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load backtest');
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function fmtNum(v, d) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d == null ? 2 : d);
    }

    function fmtPnl(v) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        const cls = n > 0 ? 'nvb-pos' : (n < 0 ? 'nvb-neg' : '');
        const txt = (n >= 0 ? '+' : '') + n.toFixed(2);
        return cls ? '<span class="' + cls + '">' + txt + '</span>' : txt;
    }

    function fmtPnlPlain(v) {
        if (v == null || v === '') return null;
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
    }

    function tradeDateFromSignal(signalTime) {
        if (!signalTime) return '';
        return String(signalTime).slice(0, 10);
    }

    function rowKey(r) {
        return String(r.signal_time || '') + '|' + String(r.symbol || '') + '|' + String(r.future_symbol || '');
    }

    function isRowChecked(key) {
        return !state.unchecked.has(key);
    }

    function fmtDateHeader(isoDate) {
        if (!isoDate) return 'Unknown date';
        const parts = String(isoDate).split('-');
        if (parts.length !== 3) return isoDate;
        const d = new Date(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2]), 12, 0, 0);
        return d.toLocaleDateString('en-IN', {
            weekday: 'long',
            day: '2-digit',
            month: 'short',
            year: 'numeric',
        });
    }

    function applyFilters(rows) {
        const from = document.getElementById('nvbFrom')?.value || '';
        const to = document.getElementById('nvbTo')?.value || '';
        const symQ = (document.getElementById('nvbSymbol')?.value || '').trim().toUpperCase();
        let out = rows.slice();
        if (from) {
            out = out.filter(function (r) {
                return tradeDateFromSignal(r.signal_time) >= from || String(r.trade_date) >= from;
            });
        }
        if (to) {
            out = out.filter(function (r) {
                return tradeDateFromSignal(r.signal_time) <= to || String(r.trade_date) <= to;
            });
        }
        if (symQ) {
            out = out.filter(function (r) {
                return String(r.symbol || '').toUpperCase().indexOf(symQ) >= 0;
            });
        }
        return out;
    }

    function sortRows(rows) {
        const sk = state.sort.key;
        const sd = state.sort.dir === 'asc' ? 1 : -1;
        return rows.slice().sort(function (a, b) {
            const va = a[sk];
            const vb = b[sk];
            if (sk === 'signal_time') return sd * String(va).localeCompare(String(vb));
            const na = Number(va);
            const nb = Number(vb);
            if (Number.isFinite(na) && Number.isFinite(nb)) return sd * (na - nb);
            return sd * String(va || '').localeCompare(String(vb || ''));
        });
    }

    function groupByDate(rows) {
        const map = {};
        rows.forEach(function (r) {
            const d = tradeDateFromSignal(r.signal_time) || String(r.trade_date || '');
            if (!map[d]) map[d] = [];
            map[d].push(r);
        });
        const dates = Object.keys(map).sort();
        if (state.sort.key === 'signal_time') {
            dates.forEach(function (d) {
                map[d].sort(function (a, b) {
                    return String(a.signal_time).localeCompare(String(b.signal_time));
                });
            });
        } else {
            dates.forEach(function (d) {
                map[d] = sortRows(map[d]);
            });
        }
        return dates.map(function (d) { return { date: d, rows: map[d] }; });
    }

    function fmtSlHit(v) {
        if (v === true || v === 'true' || v === 1) return 'Y';
        if (v === false || v === 'false' || v === 0) return 'N';
        return '—';
    }

    function sumCheckedRows(rows) {
        let sum1230 = 0;
        let sum1515 = 0;
        let slHits = 0;
        let reachedProfit = 0;
        let checked = 0;
        rows.forEach(function (r) {
            const key = rowKey(r);
            if (!isRowChecked(key)) return;
            checked += 1;
            const p1230 = fmtPnlPlain(r.pnl_1230);
            const p1515 = fmtPnlPlain(r.pnl_1515);
            if (p1230 != null) sum1230 += p1230;
            if (p1515 != null) sum1515 += p1515;
            if (r.sl_hit) slHits += 1;
            if (r.reached_profit) reachedProfit += 1;
        });
        return {
            sum1230: sum1230,
            sum1515: sum1515,
            slHits: slHits,
            reachedProfit: reachedProfit,
            checked: checked,
            total: rows.length,
        };
    }

    function updateSummary(filteredRows) {
        const totals = sumCheckedRows(filteredRows);
        const tradesEl = document.getElementById('nvbStatTrades');
        if (tradesEl) {
            tradesEl.textContent = totals.checked + ' / ' + totals.total;
        }
        const p1230El = document.getElementById('nvbStatPnl1230');
        const p1515El = document.getElementById('nvbStatPnl1515');
        const slEl = document.getElementById('nvbStatSlHit');
        const hitsEl = document.getElementById('nvbStatPnl5k');
        if (p1230El) p1230El.innerHTML = fmtPnl(totals.sum1230);
        if (p1515El) p1515El.innerHTML = fmtPnl(totals.sum1515);
        if (slEl) slEl.textContent = totals.slHits;
        if (hitsEl) hitsEl.textContent = totals.reachedProfit;
    }

    function dateGroupAllChecked(groupRows) {
        return groupRows.every(function (r) { return isRowChecked(rowKey(r)); });
    }

    function dateGroupSomeChecked(groupRows) {
        return groupRows.some(function (r) { return isRowChecked(rowKey(r)); });
    }

    function renderTable(rows) {
        const tbody = document.getElementById('nvbTbody');
        const countEl = document.getElementById('nvbCount');
        const filtered = applyFilters(rows);
        const totals = sumCheckedRows(filtered);
        if (countEl) {
            countEl.textContent = totals.checked + ' / ' + filtered.length + ' rows selected';
        }
        updateSummary(filtered);
        if (!tbody) return;
        if (!filtered.length) {
            tbody.innerHTML = '<tr><td colspan="' + COL_COUNT + '" class="nvb-empty">No rows match filters.</td></tr>';
            return;
        }

        const groups = groupByDate(filtered);
        const html = [];

        groups.forEach(function (group) {
            const allChecked = dateGroupAllChecked(group.rows);
            const someChecked = dateGroupSomeChecked(group.rows);
            const dateIndeterminate = someChecked && !allChecked;

            html.push(
                '<tr class="nvb-date-header">' +
                '<td colspan="' + COL_COUNT + '">' +
                '<label class="nvb-date-label">' +
                '<input type="checkbox" class="nvb-date-select-all"' +
                ' data-date="' + escapeHtml(group.date) + '"' +
                (allChecked ? ' checked' : '') +
                (dateIndeterminate ? ' data-indeterminate="1"' : '') +
                '> ' +
                '<span class="nvb-date-title">' + escapeHtml(fmtDateHeader(group.date)) + '</span>' +
                '</label>' +
                '<span class="nvb-date-meta">' + group.rows.length + ' trade' + (group.rows.length === 1 ? '' : 's') + '</span>' +
                '</td>' +
                '</tr>'
            );

            group.rows.forEach(function (r) {
                const key = rowKey(r);
                const checked = isRowChecked(key);
                const err = r.error ? ' title="' + escapeHtml(r.error) + '"' : '';
                html.push(
                    '<tr class="nvb-trade-row' + (checked ? '' : ' nvb-row-excluded') + '"' + err +
                    ' data-row-key="' + escapeHtml(key) + '">' +
                    '<td class="nvb-chk">' +
                    '<input type="checkbox" class="nvb-row-chk" data-row-key="' + escapeHtml(key) + '"' +
                    (checked ? ' checked' : '') + ' aria-label="Include trade in totals">' +
                    '</td>' +
                    '<td>' + escapeHtml(r.signal_time || '—') + '</td>' +
                    '<td><strong>' + escapeHtml(r.symbol || '—') + '</strong></td>' +
                    '<td>' + escapeHtml(r.future_symbol || '—') + '</td>' +
                    '<td class="num">' + escapeHtml(fmtNum(r.lot_size, 0)) + '</td>' +
                    '<td class="num">' + escapeHtml(fmtNum(r.entry_price)) + '</td>' +
                    '<td class="num">' + escapeHtml(fmtNum(r.stop_loss_price)) + '</td>' +
                    '<td>' + escapeHtml(fmtSlHit(r.sl_hit)) + '</td>' +
                    '<td>' + escapeHtml(r.sl_hit_time || '—') + '</td>' +
                    '<td class="num">' + fmtPnl(r.pnl_at_sl) + '</td>' +
                    '<td class="num">' + escapeHtml(fmtNum(r.exit_1230_price)) + '</td>' +
                    '<td class="num">' + fmtPnl(r.pnl_1230) + '</td>' +
                    '<td class="num">' + escapeHtml(fmtNum(r.exit_1515_price)) + '</td>' +
                    '<td class="num">' + fmtPnl(r.pnl_1515) + '</td>' +
                    '<td>' + escapeHtml(r.pnl_5000_time || '—') + '</td>' +
                    '<td class="num">' + escapeHtml(fmtNum(r.pnl_5000_ltp)) + '</td>' +
                    '</tr>'
                );
            });

            const sub = sumCheckedRows(group.rows);
            html.push(
                '<tr class="nvb-subtotal" data-date="' + escapeHtml(group.date) + '">' +
                '<td class="nvb-chk"></td>' +
                '<td colspan="10"><strong>Subtotal</strong>' +
                (sub.checked < sub.total
                    ? ' <span class="nvb-subtotal-meta">(' + sub.checked + ' of ' + sub.total + ' included)</span>'
                    : ' <span class="nvb-subtotal-meta">(' + sub.checked + ' trade' + (sub.checked === 1 ? '' : 's') + ')</span>') +
                '</td>' +
                '<td class="num"><strong>' + fmtPnl(sub.sum1230) + '</strong></td>' +
                '<td class="num">—</td>' +
                '<td class="num"><strong>' + fmtPnl(sub.sum1515) + '</strong></td>' +
                '<td class="num"><strong>' + (sub.slHits ? sub.slHits + ' SL' : '—') + '</strong></td>' +
                '<td class="num"><strong>' + (sub.reachedProfit ? sub.reachedProfit + ' Rs5k' : '—') + '</strong></td>' +
                '</tr>'
            );
        });

        tbody.innerHTML = html.join('');

        tbody.querySelectorAll('.nvb-date-select-all[data-indeterminate="1"]').forEach(function (cb) {
            cb.indeterminate = true;
        });
    }

    function setRowChecked(key, checked) {
        if (checked) {
            state.unchecked.delete(key);
        } else {
            state.unchecked.add(key);
        }
    }

    function setDateGroupChecked(date, groupRows, checked) {
        groupRows.forEach(function (r) {
            setRowChecked(rowKey(r), checked);
        });
    }

    function getGroupRowsForDate(date, filtered) {
        return filtered.filter(function (r) {
            return (tradeDateFromSignal(r.signal_time) || String(r.trade_date || '')) === date;
        });
    }

    function bindTableEvents() {
        const tbody = document.getElementById('nvbTbody');
        if (!tbody || tbody.dataset.bound) return;
        tbody.dataset.bound = '1';

        tbody.addEventListener('change', function (e) {
            const target = e.target;
            if (!target || target.type !== 'checkbox') return;

            const filtered = applyFilters(state.rows);

            if (target.classList.contains('nvb-row-chk')) {
                const key = target.getAttribute('data-row-key');
                if (!key) return;
                setRowChecked(key, target.checked);
                renderTable(state.rows);
                return;
            }

            if (target.classList.contains('nvb-date-select-all')) {
                const date = target.getAttribute('data-date');
                if (!date) return;
                const groupRows = getGroupRowsForDate(date, filtered);
                setDateGroupChecked(date, groupRows, target.checked);
                renderTable(state.rows);
            }
        });
    }

    function fillSummary(doc) {
        const s = doc.summary || {};
        document.getElementById('nvbStatErrors').textContent = s.errors != null ? s.errors : '—';

        const dates = (doc.rows || []).map(function (r) {
            return tradeDateFromSignal(r.signal_time) || r.trade_date;
        }).filter(Boolean).sort();
        const fromEl = document.getElementById('nvbFrom');
        const toEl = document.getElementById('nvbTo');
        if (fromEl && dates.length) fromEl.value = dates[0];
        if (toEl && dates.length) toEl.value = dates[dates.length - 1];

        const foot = document.getElementById('nvbFooter');
        if (foot) {
            const partialNote = doc.partial ? ' · Run in progress (partial results)' : '';
            foot.textContent = 'Generated: ' + (doc.generated_at || '—') + partialNote;
        }
        const err = document.getElementById('nvbErr');
        if (err && doc.partial) {
            err.hidden = false;
            err.className = 'nvb-callout nvb-callout-info';
            err.textContent = 'Backtest still running — refresh later for full results.';
        }
    }

    function setupTheme() {
        const stored = localStorage.getItem('tradentical_theme') || 'dark';
        document.body.setAttribute('data-theme', stored);
        const btn = document.getElementById('nvbThemeBtn');
        if (!btn) return;
        function syncIcon() {
            const t = document.body.getAttribute('data-theme');
            btn.innerHTML = t === 'light'
                ? '<i class="fas fa-sun"></i>'
                : '<i class="fas fa-moon"></i>';
        }
        syncIcon();
        btn.addEventListener('click', function () {
            const next = document.body.getAttribute('data-theme') === 'light' ? 'dark' : 'light';
            document.body.setAttribute('data-theme', next);
            localStorage.setItem('tradentical_theme', next);
            syncIcon();
        });
    }

    function bindFilters() {
        ['nvbFrom', 'nvbTo', 'nvbSymbol'].forEach(function (id) {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', function () { renderTable(state.rows); });
        });
        document.querySelectorAll('#nvbTable th[data-sort]').forEach(function (th) {
            th.addEventListener('click', function () {
                const key = th.getAttribute('data-sort');
                if (state.sort.key === key) {
                    state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
                } else {
                    state.sort.key = key;
                    state.sort.dir = 'asc';
                }
                renderTable(state.rows);
            });
        });
    }

    document.addEventListener('DOMContentLoaded', function () {
        setupTheme();
        bindFilters();
        bindTableEvents();
        loadData()
            .then(function (doc) {
                state.meta = doc;
                state.rows = doc.rows || [];
                state.unchecked = new Set();
                fillSummary(doc);
                renderTable(state.rows);
                const err = document.getElementById('nvbErr');
                if (err) err.hidden = true;
            })
            .catch(function (e) {
                const err = document.getElementById('nvbErr');
                if (err) {
                    err.hidden = false;
                    err.textContent = e.message || String(e);
                }
                const tbody = document.getElementById('nvbTbody');
                if (tbody) {
                    tbody.innerHTML = '<tr><td colspan="' + COL_COUNT + '" class="nvb-empty">Backtest data unavailable.</td></tr>';
                }
            });
    });
})();
