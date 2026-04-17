/**
 * Smart Futures — Today's Trend + Open Positions from smart_futures_daily.
 */
(function () {
    const API_BASE =
        window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : window.location.origin;

    function authHeaders() {
        const t = localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            'Content-Type': 'application/json',
        };
    }

    function isTokenExpiredResponse(res, payloadText, payloadJson) {
        if (res && Number(res.status) === 401) return true;
        const detail =
            (payloadJson && (payloadJson.detail || payloadJson.message)) ||
            payloadText ||
            '';
        return /token\s+expired/i.test(String(detail));
    }

    function redirectToLoginExpired() {
        try {
            localStorage.removeItem('trademanthan_token');
            sessionStorage.setItem('auth_redirect_reason', 'Your session expired. Please sign in again.');
        } catch (e) { /* ignore */ }
        window.location.replace('index.html');
    }

    async function fetchSfConfigJson() {
        const paths = ['/api/smart-futures/config', '/smart-futures/config'];
        let lastErr = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { headers: authHeaders(), cache: 'no-store' });
                const raw = await res.text();
                const ct = (res.headers.get('content-type') || '').toLowerCase();
                const looksJson =
                    ct.includes('application/json') || /^\s*[\[{]/.test(raw.slice(0, 20));
                if (res.ok && looksJson) {
                    return JSON.parse(raw);
                }
                if (isTokenExpiredResponse(res, raw, null)) {
                    redirectToLoginExpired();
                    throw new Error('Session expired');
                }
                lastErr = new Error(raw.slice(0, 200) || res.statusText);
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load Smart Futures config');
    }

    async function fetchDailyJson() {
        const paths = ['/api/smart-futures/daily', '/smart-futures/daily'];
        let lastErr = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { headers: authHeaders(), cache: 'no-store' });
                const raw = await res.text();
                const ct = (res.headers.get('content-type') || '').toLowerCase();
                const looksJson =
                    ct.includes('application/json') || /^\s*[\[{]/.test(raw.slice(0, 20));
                if (res.ok) {
                    if (!looksJson) {
                        lastErr = new Error(
                            'Server returned non-JSON (often HTML). Sign in or ask ops to proxy /smart-futures/ to the API.'
                        );
                        continue;
                    }
                    try {
                        return JSON.parse(raw);
                    } catch (parseErr) {
                        lastErr = new Error('Invalid JSON from ' + p + ': ' + (parseErr.message || parseErr));
                        continue;
                    }
                }
                if (isTokenExpiredResponse(res, raw, null)) {
                    redirectToLoginExpired();
                    throw new Error('Session expired');
                }
                lastErr = new Error(raw.slice(0, 200) || res.statusText || String(res.status));
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error('Failed to load daily picks');
    }

    function flattenRows(data) {
        if (data.rows && data.rows.length) return data.rows;
        const out = [];
        (data.groups || []).forEach(function (g) {
            (g.rows || []).forEach(function (r) {
                out.push(r);
            });
        });
        return out;
    }

    function fmtNum(v, d) {
        if (v == null || v === '') return '—';
        const n = Number(v);
        if (!Number.isFinite(n)) return '—';
        return n.toFixed(d);
    }

    function fmtSellTime(iso) {
        if (!iso) return '';
        try {
            const d = new Date(iso);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'short', timeStyle: 'medium' });
            }
        } catch (e) { /* ignore */ }
        return String(iso);
    }

    /** Signal / row time when the pick was created (used as buy-time proxy before a dedicated column exists). */
    function fmtBuyTime(iso) {
        return fmtSellTime(iso);
    }

    function winLossLabel(pnl) {
        if (pnl == null || pnl === '') return '—';
        const n = Number(pnl);
        if (!Number.isFinite(n)) return '—';
        if (n > 0) return '<span class="sf-wl-win">Win</span>';
        if (n < 0) return '<span class="sf-wl-loss">Loss</span>';
        return '<span class="sf-wl-flat">Flat</span>';
    }

    function fmtPnlCell(pnl) {
        if (pnl == null || pnl === '') return '—';
        const n = Number(pnl);
        if (!Number.isFinite(n)) return '—';
        const cls = n > 0 ? 'sf-pnl-pos' : n < 0 ? 'sf-pnl-neg' : 'sf-pnl-flat';
        return '<span class="' + cls + '">₹' + n.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + '</span>';
    }

    function escapeHtml(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function escapeAttr(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/</g, '&lt;');
    }

    function sideTooltip(r) {
        const s = String(r.side || '').trim().toUpperCase();
        const fc = Number(r.final_cms);
        const ss = Number(r.sector_score);
        const cs = Number(r.combined_sentiment);
        const core = r.cms != null && r.cms !== '' ? Number(r.cms) : NaN;
        const lines = [];
        lines.push(
            'CMS v2: regime filter (ATR5>ATR14, ADX>20 rising), normalized score, then Final CMS = score × (ATR5/ATR14) × (ADX/25). ' +
                'Entry needs Final CMS beyond threshold, price vs session VWAP, sector alignment, and sentiment band. ' +
                'Divergences are not used for entry; they feed exit hints after you mark Bought.'
        );
        const bits = [];
        if (Number.isFinite(core)) bits.push('core CMS ' + core.toFixed(2));
        if (Number.isFinite(ss)) bits.push('sector ' + ss.toFixed(2));
        if (Number.isFinite(cs)) bits.push('sentiment ' + cs.toFixed(3));
        if (Number.isFinite(fc)) bits.push('final CMS ' + fc.toFixed(2));
        if (bits.length) lines.push('This row: ' + bits.join(' · ') + '.');
        if (s === 'LONG' || s === 'SHORT') {
            lines.push('Side: ' + s + ' (meets entry rules at scan time).');
        }
        return lines.join(' ');
    }

    function fmtTierBadge(r) {
        const t = String((r && r.signal_tier) || '').trim().toUpperCase();
        if (t === 'TIER1') {
            return '<span class="sf-badge sf-b-t1" title="Tier 1">T1</span>';
        }
        if (t === 'TIER2') {
            return '<span class="sf-badge sf-b-t2" title="Tier 2">T2</span>';
        }
        return '—';
    }

    function fmtOiTag(r) {
        const m = {
            LONG_BUILDUP: 'LB',
            SHORT_BUILDUP: 'SB',
            LONG_UNWINDING: 'LU',
            SHORT_COVERING: 'SC',
            NEUTRAL: 'N',
        };
        const s = String((r && r.oi_signal) || '');
        const short = m[s] || (s ? s.slice(0, 6) : '');
        return short ? '<span class="sf-oi-tag" title="' + escapeAttr(s) + '">' + escapeHtml(short) + '</span>' : '—';
    }

    function fmtStopStageChip(r) {
        const st = String((r && r.stop_stage) || '').toUpperCase();
        if (st === 'TRAILING') return '<span class="sf-stop-chip sf-stop-trail">Trail</span>';
        if (st === 'BREAKEVEN') return '<span class="sf-stop-chip sf-stop-be">BE</span>';
        if (st === 'INITIAL') return '<span class="sf-stop-chip sf-stop-ini">Init</span>';
        return r && r.stop_stage ? escapeHtml(String(r.stop_stage)) : '—';
    }

    function fmtFilterDot(r) {
        const tf = r && r.time_filter_passed;
        const rf = r && r.regime_filter_passed;
        if (tf === true && rf === true) {
            return '<span class="sf-filter-dot sf-filter-ok" title="Both filters passed">Y</span>';
        }
        if (tf === true && rf === false) {
            return '<span class="sf-filter-dot sf-filter-bad" title="Only time filter passed">T</span>';
        }
        if (tf === false && rf === true) {
            return '<span class="sf-filter-dot sf-filter-bad" title="Only regime filter passed">R</span>';
        }
        if (tf === false && rf === false) {
            return '<span class="sf-filter-dot sf-filter-bad" title="Neither filter passed">X</span>';
        }
        return '—';
    }

    function fmtSideCell(r) {
        const side = r && r.side != null ? r.side : '';
        const s = String(side || '').trim().toUpperCase();
        const tip = escapeAttr(sideTooltip(r || {}));
        const titleAttr = tip ? ' title="' + tip + '"' : '';
        if (s === 'LONG') {
            return '<span class="sf-side-pill sf-side-long"' + titleAttr + '>LONG</span>';
        }
        if (s === 'SHORT') {
            return '<span class="sf-side-pill sf-side-short"' + titleAttr + '>SHORT</span>';
        }
        return side ? escapeHtml(String(side)) : '—';
    }

    function fmtTrendCell(r) {
        if (String(r.trend_continuation || '').trim() !== 'Yes') return '';
        return (
            '<span class="sf-trend-yes" title="Yes">' +
            '<i class="fas fa-check" aria-hidden="true"></i>' +
            '<span class="sr-only">Yes</span>' +
            '</span>'
        );
    }

    /** First section: status text for bought/sold — no Sell here. */
    function fmtTrendActionCell(r) {
        const ost = String(r.order_status || '').trim().toLowerCase();
        const rawStatus = String(r.order_status || '').trim();
        const displayStatus = rawStatus ? escapeHtml(rawStatus) : '—';
        const exitS = Boolean(r.exit_suggested);
        const reason = escapeAttr(String(r.exit_reason || ''));

        if (ost === 'sold') {
            const sp = r.sell_price != null && r.sell_price !== '' ? ' @' + fmtNum(r.sell_price, 2) : '';
            const st = r.sell_time ? ' · ' + escapeHtml(fmtSellTime(r.sell_time)) : '';
            return '<span class="sf-sold">Sold' + sp + st + '</span>';
        }
        if (ost === 'bought') {
            let hint = '';
            if (exitS) {
                hint =
                    '<span class="sf-exit-hint" title="' +
                    (reason || 'Exit signal (algo)') +
                    '">Exit</span> ';
            }
            return hint + '<span class="sf-order-status">' + displayStatus + '</span>';
        }
        return (
            '<button type="button" class="sf-btn-order" data-order-id="' +
            r.id +
            '">Order</button>'
        );
    }

    /** Open Positions: Sell only when exit_suggested; blink when enabled. */
    function fmtOpenActionCell(r) {
        const id = r.id;
        const exitOk = Boolean(r.exit_suggested);
        const reason = escapeAttr(String(r.exit_reason || ''));
        const dis = exitOk ? '' : ' disabled';
        const blink = exitOk ? ' sf-btn-sell--blink' : '';
        const title = exitOk
            ? 'Square off at LTP — exit signal is active'
            : 'Disabled until the algo signals exit (see Today\'s Trend Exit hint)';
        return (
            '<button type="button" class="sf-btn-sell' +
            blink +
            '" data-sell-id="' +
            id +
            '" data-open-sell="1"' +
            dis +
            ' title="' +
            title +
            (reason && exitOk ? ' — ' + reason : '') +
            '">Sell</button>'
        );
    }

    function fmtSymbolCell(r) {
        const sym = r && r.fut_symbol != null && r.fut_symbol !== '' ? String(r.fut_symbol) : '—';
        const ratio = r && r.atr5_14_ratio != null && r.atr5_14_ratio !== '' ? Number(r.atr5_14_ratio) : NaN;
        const hot = Number.isFinite(ratio) && ratio >= 1.1;
        const tip = Number.isFinite(ratio)
            ? 'ATR(5)/ATR(14) = ' + ratio.toFixed(3) + ' (session 5‑minute bars). Fire when ≥ 1.1.'
            : '';
        const titleAttr = tip ? ' title="' + escapeAttr(tip) + '"' : '';
        const label = sym === '—' ? sym : escapeHtml(sym);
        const fire =
            hot ? '<span class="sf-atr-fire" aria-hidden="true">🔥</span> ' : '';
        if (tip) {
            return '<span class="sf-symbol-wrap"' + titleAttr + '>' + fire + label + '</span>';
        }
        return fire ? fire + label : label;
    }

    function fmtEntryGroupLabel(bucket) {
        if (!bucket || bucket === '—') return 'Entry time';
        try {
            const d = new Date(bucket.length >= 16 ? bucket + ':00' : bucket);
            if (!Number.isNaN(d.getTime())) {
                return d.toLocaleString('en-IN', { timeZone: 'Asia/Kolkata', dateStyle: 'medium', timeStyle: 'short' });
            }
        } catch (e) { /* ignore */ }
        return bucket;
    }

    function pad2(n) {
        return String(n).padStart(2, '0');
    }

    function todayIstYmd() {
        const now = new Date();
        const parts = new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Kolkata',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
        }).formatToParts(now);
        const y = parts.find(function (p) { return p.type === 'year'; });
        const m = parts.find(function (p) { return p.type === 'month'; });
        const d = parts.find(function (p) { return p.type === 'day'; });
        return (y ? y.value : '') + '-' + (m ? m.value : '') + '-' + (d ? d.value : '');
    }

    function nowIstMinutes() {
        const now = new Date();
        const hh = Number(
            new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kolkata', hour: '2-digit', hour12: false }).format(now)
        );
        const mm = Number(new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kolkata', minute: '2-digit' }).format(now));
        if (!Number.isFinite(hh) || !Number.isFinite(mm)) return 24 * 60;
        return hh * 60 + mm;
    }

    function scheduledSlotsForSession(sessionDate) {
        const out = [];
        for (let h = 9, m = 30; ; ) {
            out.push(pad2(h) + ':' + pad2(m));
            if (h === 15 && m === 0) break;
            m += 15;
            if (m >= 60) {
                h += 1;
                m -= 60;
            }
        }
        const isTodaySession = String(sessionDate || '') === todayIstYmd();
        if (!isTodaySession) return out;
        const nowMin = nowIstMinutes();
        return out.filter(function (slot) {
            const p = slot.split(':');
            const mins = Number(p[0]) * 60 + Number(p[1]);
            return mins <= nowMin;
        });
    }

    function normalizeRunSlot(v) {
        const s = String(v || '').trim();
        const m = s.match(/(\d{1,2}):(\d{2})/);
        if (!m) return '';
        return pad2(Number(m[1])) + ':' + pad2(Number(m[2]));
    }

    function deriveRunSlot(r) {
        const fromScan = normalizeRunSlot(r && r.scan_trigger);
        if (fromScan) return fromScan;
        const ea = String((r && r.entry_at) || '');
        const m = ea.match(/T(\d{2}):(\d{2})/);
        if (!m) return '';
        return m[1] + ':' + m[2];
    }

    function trendTableRowHtml(r) {
        const runSlot = escapeHtml(String((r && r.__run_slot) || deriveRunSlot(r) || ''));
        return (
            '<tr data-row-id="' +
            r.id +
            '">' +
            '<td>' +
            runSlot +
            '</td>' +
            '<td>' +
            fmtSymbolCell(r) +
            '</td>' +
            '<td>' +
            fmtSideCell(r) +
            '</td>' +
            '<td>' +
            fmtTierBadge(r) +
            '</td>' +
            '<td>' +
            fmtOiTag(r) +
            '</td>' +
            '<td>' +
            fmtFilterDot(r) +
            '</td>' +
            '<td>' +
            fmtNum(r.final_cms, 2) +
            '</td>' +
            '<td>' +
            fmtNum(r.sector_score, 2) +
            '</td>' +
            '<td>' +
            fmtNum(r.combined_sentiment, 3) +
            '</td>' +
            '<td>' +
            fmtNum(r.entry_price, 2) +
            '</td>' +
            '<td>' +
            fmtNum(r.sl_price, 2) +
            '</td>' +
            '<td>' +
            fmtTrendActionCell(r) +
            '</td>' +
            '</tr>'
        );
    }

    function openTableRowHtml(r) {
        const tier1 = r.breakeven_activated ? 'Activated' : 'Not Activated';
        const tier2 = r.profit_locking_activated ? 'Activated' : 'Not Activated';
        const tier3 = r.trailing_stop_activated ? 'Activated' : 'Not Activated';
        const activeStop = r.current_active_stop_loss_level != null ? fmtNum(r.current_active_stop_loss_level, 2) : '—';
        const trailStop = r.current_trailing_stop_level != null ? fmtNum(r.current_trailing_stop_level, 2) : '—';
        const exitReason = r.exit_reason ? escapeHtml(String(r.exit_reason)) : '—';
        return (
            '<tr data-row-id="' +
            r.id +
            '">' +
            '<td>' +
            fmtSymbolCell(r) +
            '</td>' +
            '<td>' +
            fmtSideCell(r) +
            '</td>' +
            '<td>' +
            fmtTierBadge(r) +
            '</td>' +
            '<td>' +
            fmtOiTag(r) +
            '</td>' +
            '<td>' +
            fmtStopStageChip(r) +
            '</td>' +
            '<td>' +
            (r.calculated_lots != null && r.calculated_lots !== '' ? escapeHtml(String(r.calculated_lots)) : '—') +
            '</td>' +
            '<td>' +
            fmtNum(r.final_cms, 2) +
            '</td>' +
            '<td>' +
            fmtNum(r.entry_price, 2) +
            '</td>' +
            '<td>' +
            fmtNum(r.sl_price, 2) +
            '</td>' +
            '<td>' +
            fmtNum(r.target_price, 2) +
            '</td>' +
            '<td>' +
            tier1 +
            '</td>' +
            '<td>' +
            tier2 +
            '</td>' +
            '<td>' +
            tier3 +
            '</td>' +
            '<td>' +
            activeStop +
            '</td>' +
            '<td>' +
            trailStop +
            '</td>' +
            '<td>' +
            exitReason +
            '</td>' +
            '<td>' +
            fmtTrendCell(r) +
            '</td>' +
            '<td>' +
            fmtOpenActionCell(r) +
            '</td>' +
            '</tr>'
        );
    }

    function renderGroups(data) {
        const host = document.getElementById('sfTrendGroups');
        const sessionEl = document.getElementById('sfTrendSession');
        const msg = document.getElementById('sfTrendMsg');
        if (!host) return;

        if (sessionEl) sessionEl.textContent = data.session_date || '—';

        if (data.error) {
            msg.textContent = data.error;
        } else {
            msg.textContent = '';
        }

        const groups = data.groups && data.groups.length ? data.groups : [];
        const flat = flattenRows(data);
        const flatTrend = flat.filter(function (r) {
            const o = String(r.order_status || '').trim().toLowerCase();
            return o !== 'bought' && o !== 'sold';
        });

        const thead =
            '<thead><tr>' +
            '<th>Run Slot</th><th>Symbol</th><th>Side</th><th>Tier</th><th>OI</th><th>Filters</th><th>CMS</th><th>Sector</th><th>Sentiment</th>' +
            '<th>Entry</th><th>SL</th><th>Status</th>' +
            '</tr></thead>';

        const slots = scheduledSlotsForSession(data.session_date);
        const bySlot = {};
        flatTrend.forEach(function (r) {
            const slot = deriveRunSlot(r);
            if (!slot) return;
            if (!bySlot[slot]) bySlot[slot] = [];
            bySlot[slot].push(r);
        });

        const rows = [];
        slots.forEach(function (slot) {
            const picked = bySlot[slot] || [];
            if (picked.length) {
                picked.forEach(function (r) {
                    const row = Object.assign({}, r, { __run_slot: slot });
                    rows.push(row);
                });
            }
        });

        // Always render qualifying rows even when their slot is outside the
        // scheduled slot list (e.g. manual/off-cycle scans).
        const extra = flatTrend.filter(function (r) {
            const slot = deriveRunSlot(r);
            return slot && slots.indexOf(slot) === -1;
        });
        if (extra.length) {
            extra
                .slice()
                .sort(function (a, b) {
                    const ea = String((a && a.entry_at) || '');
                    const eb = String((b && b.entry_at) || '');
                    return eb.localeCompare(ea);
                })
                .forEach(function (r) {
                    const row = Object.assign({}, r, { __run_slot: deriveRunSlot(r) || '—' });
                    rows.push(row);
                });
        }

        let html = '<div class="sf-table-wrap"><table class="sf-table">' + thead + '<tbody>';
        rows.forEach(function (r) {
            html += trendTableRowHtml(r);
        });
        html += '</tbody></table></div>';
        if (extra.length) {
            html +=
                '<p class="sf-meta" style="margin-top:10px;">Including ' +
                extra.length +
                ' off-slot/manual row(s).</p>';
        }
        host.innerHTML = html;

        host.onclick = function (ev) {
            const ob = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-order') : null;
            if (ob) onOrderClick(ev);
        };
    }

    function closedTableRowHtml(r) {
        const sym =
            r && r.fut_symbol != null && r.fut_symbol !== '' ? escapeHtml(String(r.fut_symbol)) : '—';
        const buyT = fmtBuyTime(r.entry_at);
        const sellT = fmtSellTime(r.sell_time);
        return (
            '<tr data-row-id="' +
            r.id +
            '">' +
            '<td>' +
            sym +
            '</td>' +
            '<td>' +
            escapeHtml(buyT || '—') +
            '</td>' +
            '<td>' +
            fmtNum(r.buy_price, 2) +
            '</td>' +
            '<td>' +
            escapeHtml(sellT || '—') +
            '</td>' +
            '<td>' +
            fmtNum(r.sell_price, 2) +
            '</td>' +
            '<td>' +
            fmtPnlCell(r.realized_pnl) +
            '</td>' +
            '<td>' +
            winLossLabel(r.realized_pnl) +
            '</td>' +
            '</tr>'
        );
    }

    function renderClosedPositions(data) {
        const host = document.getElementById('sfClosedPositions');
        const msg = document.getElementById('sfClosedMsg');
        if (!host) return;

        const all = flattenRows(data);
        const sold = all.filter(function (r) {
            return String(r.order_status || '').trim().toLowerCase() === 'sold';
        });
        sold.sort(function (a, b) {
            const sa = String(a.sell_time || '');
            const sb = String(b.sell_time || '');
            return sb.localeCompare(sa);
        });

        if (msg) msg.textContent = '';

        if (!sold.length) {
            host.innerHTML =
                '<div class="sf-table-wrap"><table class="sf-table sf-closed-table"><tbody><tr><td colspan="7" style="padding:14px;">No closed positions this session</td></tr></tbody></table></div>';
            return;
        }

        let sumPnl = 0;
        let nPnl = 0;
        let wins = 0;
        sold.forEach(function (r) {
            const p = r.realized_pnl;
            if (p == null || p === '') return;
            const n = Number(p);
            if (!Number.isFinite(n)) return;
            nPnl += 1;
            sumPnl += n;
            if (n > 0) wins += 1;
        });
        const winRatioPct = nPnl > 0 ? (wins / nPnl) * 100 : null;
        const sumStr =
            nPnl > 0
                ? '<span class="' +
                  (sumPnl >= 0 ? 'sf-pnl-pos' : 'sf-pnl-neg') +
                  '">₹' +
                  sumPnl.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) +
                  '</span>'
                : '—';
        const ratioStr =
            winRatioPct != null
                ? wins +
                  ' / ' +
                  nPnl +
                  ' (' +
                  winRatioPct.toFixed(1) +
                  '%)'
                : '—';

        const thead =
            '<thead><tr>' +
            '<th>Symbol</th><th>Buy time</th><th>Buy price</th><th>Sell time</th><th>Sell price</th><th>PnL</th><th>Win / Loss</th>' +
            '</tr></thead>';
        let body = '';
        sold.forEach(function (r) {
            body += closedTableRowHtml(r);
        });
        const foot =
            '<tfoot><tr class="sf-closed-footer-row">' +
            '<td colspan="5"><strong>Session totals</strong></td>' +
            '<td><strong>' +
            sumStr +
            '</strong><div class="sf-meta" style="margin-top:4px;">Cumulative PnL (rows with PnL)</div></td>' +
            '<td><strong>' +
            escapeHtml(ratioStr) +
            '</strong><div class="sf-meta" style="margin-top:4px;">Win ratio</div></td>' +
            '</tr></tfoot>';
        host.innerHTML =
            '<div class="sf-table-wrap"><table class="sf-table sf-closed-table">' +
            thead +
            '<tbody>' +
            body +
            '</tbody>' +
            foot +
            '</table></div>';
    }

    function renderOpenPositions(data) {
        const host = document.getElementById('sfOpenPositions');
        const msg = document.getElementById('sfOpenMsg');
        if (!host) return;

        const all = flattenRows(data);
        const bought = all.filter(function (r) {
            return String(r.order_status || '').trim().toLowerCase() === 'bought';
        });
        bought.sort(function (a, b) {
            const ea = String(a.entry_at || '');
            const eb = String(b.entry_at || '');
            return eb.localeCompare(ea);
        });

        if (msg) msg.textContent = '';

        if (!bought.length) {
            host.innerHTML =
                '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="21" style="padding:14px;">No open positions</td></tr></tbody></table></div>';
            return;
        }

        const thead =
            '<thead><tr>' +
            '<th>Symbol</th><th>Side</th><th>Tier</th><th>OI</th><th>Stop</th><th>Lots</th><th>CMS</th>' +
            '<th>Entry</th><th>SL</th><th>Target</th><th>Tier 1</th><th>Tier 2</th><th>Tier 3</th><th>Active SL</th><th>Trail SL</th><th>Exit Reason</th><th>In Trend</th><th>Action</th>' +
            '</tr></thead>';
        let body = '';
        bought.forEach(function (r) {
            body += openTableRowHtml(r);
        });
        host.innerHTML =
            '<div class="sf-table-wrap"><table class="sf-table">' + thead + '<tbody>' + body + '</tbody></table></div>';

        host.onclick = function (ev) {
            const sb = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-sell[data-open-sell]') : null;
            if (sb) onSellClick(ev);
        };
    }

    async function onOrderClick(ev) {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-order') : null;
        if (!btn) return;
        const id = btn.getAttribute('data-order-id');
        if (!id || btn.disabled) return;
        if (!window.confirm('Mark this row as bought at current LTP?')) return;
        btn.disabled = true;
        const paths = ['/api/smart-futures/daily/' + id + '/order', '/smart-futures/daily/' + id + '/order'];
        let ok = false;
        let errText = '';
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { method: 'POST', headers: authHeaders() });
                if (res.ok) {
                    ok = true;
                    break;
                }
                const raw = (await res.text()) || res.statusText;
                let data = null;
                try {
                    data = JSON.parse(raw);
                } catch (e) {
                    data = null;
                }
                if (isTokenExpiredResponse(res, raw, data)) {
                    redirectToLoginExpired();
                    return;
                }
                errText = (data && data.detail) || raw || res.statusText;
            } catch (e) {
                errText = String(e.message || e);
            }
        }
        if (!ok) {
            alert(errText || 'Order failed');
            btn.disabled = false;
            return;
        }
        await loadTrend(true);
    }

    async function onSellClick(ev) {
        const btn = ev.target && ev.target.closest ? ev.target.closest('.sf-btn-sell[data-open-sell]') : null;
        if (!btn) return;
        if (btn.disabled) return;
        const id = btn.getAttribute('data-sell-id');
        if (!id) return;
        if (!window.confirm('Mark this position as sold at current LTP?')) return;
        btn.disabled = true;
        const paths = ['/api/smart-futures/daily/' + id + '/sell', '/smart-futures/daily/' + id + '/sell'];
        let ok = false;
        let errText = '';
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { method: 'POST', headers: authHeaders() });
                const raw = await res.text();
                let data = null;
                try {
                    data = JSON.parse(raw);
                } catch (e) {
                    data = null;
                }
                if (res.ok && data && data.success) {
                    ok = true;
                    break;
                }
                if (isTokenExpiredResponse(res, raw, data)) {
                    redirectToLoginExpired();
                    return;
                }
                errText = (data && data.detail) || raw || res.statusText;
            } catch (e) {
                errText = String(e.message || e);
            }
        }
        if (!ok) {
            alert(errText || 'Sell failed');
            btn.disabled = false;
            return;
        }
        await loadTrend(true);
    }

    function applyPickSelectionNote(cfg) {
        const note = document.getElementById('sfPickSelectionNote');
        if (!note) return;
        if (!cfg || cfg.pick_selection_top_n == null) {
            note.textContent = '';
            return;
        }
        const tn = Number(cfg.pick_selection_top_n);
        const lc =
            cfg.pick_selection_long_cap != null ? Number(cfg.pick_selection_long_cap) : Math.floor(tn / 3);
        const sc =
            cfg.pick_selection_short_cap != null ? Number(cfg.pick_selection_short_cap) : Math.floor(tn / 2);
        const maxPublish =
            cfg.max_publish_per_scan != null ? Number(cfg.max_publish_per_scan) : 5;
        const minL =
            cfg.min_long_buildup_selection != null ? Number(cfg.min_long_buildup_selection) : 3;
        note.textContent =
            'Picker mix (each scan): at least ' +
            minL +
            ' LONG_BUILDUP when available, up to ' +
            lc +
            ' LONG + ' +
            sc +
            ' SHORT by CMS (top_n=' +
            tn +
            '). Saves up to ' +
            maxPublish +
            ' qualifying symbols per scan (after order-stage filters).';
    }

    function showPickerRunBanner(data) {
        const el = document.getElementById('sfPickerRunBanner');
        if (!el) return;
        if (!data || data.success === false) {
            el.style.display = 'none';
            return;
        }
        const p = data.picker || {};
        const skipped = p.skipped || data.skipped;
        if (skipped) {
            el.innerHTML =
                '<strong>Picker:</strong> skipped (' + escapeHtml(String(skipped)) + ').';
            el.style.display = 'block';
            return;
        }
        const picks = p.picks != null ? p.picks : data.picks;
        const merged = (p.merged_pick_symbols || data.merged_pick_symbols || []).join(', ') || '—';
        const longs = (p.picked_long || data.picked_long || []).join(', ') || '—';
        const shorts = (p.picked_short || data.picked_short || []).join(', ') || '—';
        const details = p.merged_pick_details || data.merged_pick_details || [];
        const newFo = details
            .filter(function (d) {
                return String((d && d.history_status) || '').toUpperCase() === 'NEW_FO_LISTING';
            })
            .map(function (d) {
                const days = Number(d && d.history_days_used);
                const suffix = Number.isFinite(days) ? ' (' + String(days) + 'd)' : '';
                return String((d && d.stock) || '') + suffix;
            })
            .filter(Boolean)
            .join(', ') || '—';
        const hr = data.warmup && data.warmup.heatmap_refresh;
        const warmOk = hr && hr.success !== false && hr.success !== undefined;
        el.innerHTML =
            '<strong>Manual scan complete.</strong> New rows saved: <strong>' +
            escapeHtml(String(picks != null ? picks : '0')) +
            '</strong>. ' +
            (warmOk ? 'OI heatmap was refreshed first (live OI + WebSocket). ' : '') +
            '<br><span class="sf-meta">Long:</span> ' +
            escapeHtml(longs) +
            ' &nbsp;|&nbsp; <span class="sf-meta">Short:</span> ' +
            escapeHtml(shorts) +
            '<br><span class="sf-meta">Merged filter set:</span> ' +
            escapeHtml(merged) +
            '<br><span class="sf-meta">Status: NEW_FO_LISTING</span> ' +
            escapeHtml(newFo);
        el.style.display = 'block';
    }

    async function runPickerScan() {
        const btn = document.getElementById('sfRunPicker');
        const msg = document.getElementById('sfTrendMsg');
        if (btn) btn.disabled = true;
        if (msg) msg.textContent = 'Running CMS picker (OI heatmap warmup can take 1–3 minutes)…';
        try {
            const url = API_BASE + '/scan/smart-futures/run-picker?warmup_oi=true';
            const res = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                cache: 'no-store',
            });
            const raw = await res.text();
            let data = null;
            try {
                data = JSON.parse(raw);
            } catch (e) {
                data = null;
            }
            if (!res.ok) {
                const err = (data && (data.message || data.detail)) || raw || res.statusText;
                if (msg) msg.textContent = err;
                return;
            }
            showPickerRunBanner(data);
            if (msg) msg.textContent = '';
            await loadTrend(true);
        } catch (e) {
            if (msg) msg.textContent = String(e.message || e);
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    async function loadTrend(silent) {
        const updated = document.getElementById('sfTrendUpdated');
        const msg = document.getElementById('sfTrendMsg');
        try {
            let cfg = null;
            try {
                cfg = await fetchSfConfigJson();
            } catch (ce) {
                cfg = null;
            }
            applyPickSelectionNote(cfg);
            const data = await fetchDailyJson();
            renderGroups(data);
            renderOpenPositions(data);
            renderClosedPositions(data);
            if (updated) updated.textContent = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
        } catch (e) {
            if (!silent && msg) msg.textContent = String(e.message || e);
            const host = document.getElementById('sfTrendGroups');
            if (host) {
                host.innerHTML =
                    '<div class="sf-table-wrap"><table class="sf-table"><tbody><tr><td colspan="9" style="padding:14px;">No Record</td></tr></tbody></table></div>';
            }
            const openHost = document.getElementById('sfOpenPositions');
            if (openHost) openHost.innerHTML = '';
            const closedHost = document.getElementById('sfClosedPositions');
            if (closedHost) closedHost.innerHTML = '';
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const ref = document.getElementById('sfTrendRefresh');
        if (ref) ref.addEventListener('click', function () { loadTrend(false); });
        const runPicker = document.getElementById('sfRunPicker');
        if (runPicker) runPicker.addEventListener('click', function () { runPickerScan(); });
        loadTrend(false);
        window.setInterval(function () { loadTrend(true); }, 15 * 60 * 1000);
    });
})();
