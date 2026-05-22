/**
 * Vajra Open Position — institutional trade management cards (presentation only).
 * No backend / API changes. Derives display-only risk levels when SL/T1/T2 not stored.
 */
(function (global) {
    'use strict';

    const LIFECYCLE_STAGES = ['DISCOVERY', 'ENTRY', 'EXPANSION', 'HOLD', 'EXIT'];

    function esc(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function num(v, digits) {
        const n = parseFloat(v);
        if (!Number.isFinite(n)) return null;
        return digits != null ? n.toFixed(digits) : n;
    }

    function fmtInr(v) {
        const n = num(v, 2);
        return n != null ? '₹' + n : '—';
    }

    function dirBull(direction) {
        return String(direction || '').toUpperCase().indexOf('L') === 0;
    }

    function healthBand(score) {
        const n = parseFloat(score);
        if (!Number.isFinite(n)) return { label: '—', cls: 'vop-health-neutral', state: 'CAUTION' };
        if (n >= 80) return { label: 'STRONG', cls: 'vop-health-strong', state: 'STRONG' };
        if (n >= 60) return { label: 'HEALTHY', cls: 'vop-health-healthy', state: 'HEALTHY' };
        if (n >= 40) return { label: 'CAUTION', cls: 'vop-health-caution', state: 'CAUTION' };
        return { label: 'WEAKENING', cls: 'vop-health-weak', state: 'WEAKENING' };
    }

    function mapLifecycleStage(lifecycleState) {
        const s = String(lifecycleState || '').toLowerCase();
        if (s.indexOf('early') >= 0) return 'ENTRY';
        if (s.indexOf('expansion') >= 0) return 'EXPANSION';
        if (s.indexOf('exhaust') >= 0 || s.indexOf('breakdown') >= 0 || s.indexOf('failed') >= 0) {
            return 'EXIT';
        }
        if (s.indexOf('stable') >= 0 || s.indexOf('rotation') >= 0 || s.indexOf('consolid') >= 0) {
            return 'HOLD';
        }
        return 'HOLD';
    }

    /** Display-only SL / targets from entry + extension risk (no backend writes). */
    function deriveRiskLevels(trade) {
        const entry = parseFloat(trade.entry_price);
        if (!Number.isFinite(entry) || entry <= 0) {
            return { sl: null, t1: null, t2: null, riskAmt: null, rr: null };
        }
        const bull = dirBull(trade.direction);
        const disc = trade.discovery_snapshot || {};
        const met = trade.metrics_at_entry || {};
        const ext = parseFloat(disc.extension_risk_score || met.extension_risk_score || 50);
        const riskPct = 0.007 + Math.min(1, Math.max(0, ext / 100)) * 0.018;
        const riskDist = entry * riskPct;
        const sl = bull ? entry - riskDist : entry + riskDist;
        const t1 = bull ? entry + riskDist * 1.55 : entry - riskDist * 1.55;
        const t2 = bull ? entry + riskDist * 2.35 : entry - riskDist * 2.35;
        const rr = riskDist > 0 ? (Math.abs(t1 - entry) / riskDist).toFixed(1) : null;
        return {
            sl: sl,
            t1: t1,
            t2: t2,
            riskAmt: riskDist,
            rr: rr,
        };
    }

    function renderSymbolChartLink(trade, identity) {
        const ratings = global.VajraFuturesRatings;
        if (!ratings || typeof ratings.renderSecurityChartLink !== 'function') {
            return esc(identity.title);
        }
        return ratings.renderSecurityChartLink({
            stock: trade.stock || identity.title,
            instrumentKey: trade.instrument_key || '',
            label: identity.title,
            className: 'vop-symbol-link',
        });
    }

    function parseIdentity(trade) {
        const fs = String(trade.future_symbol || '').trim();
        const stock = String(trade.stock || '').trim();
        let title = stock || fs;
        let subtitle = 'FUTURES';
        if (fs && stock && fs.toUpperCase().indexOf(stock.toUpperCase()) === 0) {
            title = stock;
            const rest = fs.slice(stock.length).trim();
            subtitle = rest ? rest.replace(/\s+FUT\s*/i, ' ').trim() + ' FUTURES' : 'FUTURES';
        } else if (fs) {
            const parts = fs.split(/\s+/);
            title = parts[0] || fs;
            subtitle = parts.slice(1).join(' ') || 'FUTURES';
        }
        return { title: title, subtitle: subtitle };
    }

    const API_BASE =
        global.location.hostname === 'localhost' || global.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : global.location.origin;
    const _lotCache = {};
    const _lotPending = {};

    function authHeaders() {
        const t = global.localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            Accept: 'application/json',
        };
    }

    function contractLotSize(trade) {
        const disc = trade.discovery_snapshot || {};
        const met = trade.metrics_at_entry || {};
        const ik = String(trade.instrument_key || '').trim();
        const ls = parseInt(
            trade.fut_lot_size ||
                trade.lot_size ||
                met.fut_lot_size ||
                met.lot_size ||
                disc.fut_lot_size ||
                disc.lot_size ||
                (_lotCache[ik] || 0),
            10
        );
        return ls > 0 ? ls : 0;
    }

    function pnlPct(trade, live) {
        const entry = parseFloat(trade.entry_price);
        const px = parseFloat(live);
        if (!Number.isFinite(entry) || entry <= 0 || !Number.isFinite(px)) return null;
        const bull = dirBull(trade.direction);
        const raw = bull ? ((px - entry) / entry) * 100 : ((entry - px) / entry) * 100;
        return raw;
    }

    /** Rupee P&L for 1 exchange lot × trade lots: (live − entry) × lot_size × lots. */
    function pnlRupees(trade, live) {
        const entry = parseFloat(trade.entry_price);
        const px = parseFloat(live);
        const lots = parseInt(trade.lots, 10) || 1;
        const unit = contractLotSize(trade);
        if (!Number.isFinite(entry) || !Number.isFinite(px) || unit <= 0) return null;
        const bull = dirBull(trade.direction);
        const diff = bull ? px - entry : entry - px;
        return diff * lots * unit;
    }

    function fmtPnlAmt(v) {
        if (v == null || !Number.isFinite(v)) return '—';
        const n = Math.round(v);
        const sign = n < 0 ? '-' : '';
        return sign + '₹' + Math.abs(n).toLocaleString('en-IN', { maximumFractionDigits: 0 });
    }

    function fetchContractLotSize(instrumentKey) {
        const ik = String(instrumentKey || '').trim();
        if (!ik || _lotCache[ik] || _lotPending[ik]) return Promise.resolve(_lotCache[ik] || 0);
        _lotPending[ik] = true;
        const urls = [
            API_BASE + '/api/vajra-futures/contract-lot-size?instrument_key=' + encodeURIComponent(ik),
            API_BASE + '/vajra-futures/contract-lot-size?instrument_key=' + encodeURIComponent(ik),
        ];
        return (async function tryOne(i) {
            if (i >= urls.length) {
                delete _lotPending[ik];
                return 0;
            }
            try {
                const res = await fetch(urls[i], { headers: authHeaders() });
                const data = await res.json();
                if (res.ok && data && data.lot_size > 0) {
                    _lotCache[ik] = parseInt(data.lot_size, 10);
                }
            } catch (e) {
                return tryOne(i + 1);
            }
            delete _lotPending[ik];
            return _lotCache[ik] || 0;
        })(0);
    }

    function prefetchLotSizes(rows, container) {
        const need = {};
        (rows || []).forEach(function (t) {
            const ik = String(t.instrument_key || '').trim();
            if (!ik || contractLotSize(t) > 0) return;
            need[ik] = true;
        });
        Object.keys(need).forEach(function (ik) {
            fetchContractLotSize(ik).then(function () {
                if (!container) return;
                container.querySelectorAll('.vop-card').forEach(function (card) {
                    const tid = card.getAttribute('data-trade-id');
                    const trade = (rows || []).find(function (r) {
                        return String(r.id) === String(tid);
                    });
                    if (trade && String(trade.instrument_key || '').trim() === ik) {
                        patchCard(card, trade);
                    }
                });
            });
        });
    }

    function progressModel(trade, levels, live) {
        const sl = levels.sl;
        const t2 = levels.t2;
        const entry = parseFloat(trade.entry_price);
        const px = parseFloat(live != null ? live : trade.current_price);
        if (!Number.isFinite(sl) || !Number.isFinite(t2) || !Number.isFinite(entry) || !Number.isFinite(px)) {
            return { entryPct: 35, currentPct: 35, distTargetPct: null };
        }
        const span = t2 - sl;
        if (Math.abs(span) < 1e-9) {
            return { entryPct: 35, currentPct: 35, distTargetPct: null };
        }
        const entryPct = Math.max(4, Math.min(96, ((entry - sl) / span) * 100));
        const currentPct = Math.max(4, Math.min(96, ((px - sl) / span) * 100));
        const bull = dirBull(trade.direction);
        const distTarget =
            bull && Number.isFinite(t2)
                ? ((t2 - px) / Math.max(1e-9, t2 - entry)) * 100
                : ((px - t2) / Math.max(1e-9, entry - t2)) * 100;
        return {
            entryPct: entryPct,
            currentPct: currentPct,
            distTargetPct: Number.isFinite(distTarget) ? Math.max(0, distTarget) : null,
        };
    }

    function buildSignals(trade) {
        const out = [];
        const bull = dirBull(trade.direction);
        const ema = String(trade.ema_status || '');
        const vwap = String(trade.vwap_status || '');
        const mom = String(trade.momentum_status || '');
        const struct = String(trade.structure_status || '');

        if (ema) {
            const ok =
                (bull && ema.indexOf('Above') >= 0) || (!bull && ema.indexOf('Below') >= 0);
            out.push({
                type: ok ? 'pos' : 'warn',
                text: ok ? (bull ? 'EMA reclaimed' : 'EMA breakdown held') : 'EMA under pressure',
            });
        }
        if (vwap) {
            const ok =
                (bull && vwap.indexOf('Above') >= 0) || (!bull && vwap.indexOf('Below') >= 0);
            out.push({ type: ok ? 'pos' : 'warn', text: ok ? 'Above VWAP' : 'Below VWAP' });
        }
        if (struct === 'Supportive') {
            out.push({ type: 'pos', text: 'Structure supportive' });
        } else if (struct === 'Weakening') {
            out.push({ type: 'warn', text: 'Structure weakening' });
        }
        if (mom === 'Strengthening') {
            out.push({ type: 'pos', text: 'Momentum strengthening' });
        } else if (mom === 'Weakening') {
            out.push({ type: 'warn', text: 'Momentum slowing' });
        }
        (trade.alerts || []).slice(-3).forEach(function (a) {
            const lvl = a.level === 'positive' ? 'pos' : a.level === 'warning' ? 'warn' : 'risk';
            if (a.message) out.push({ type: lvl, text: a.message });
        });
        const seen = {};
        return out.filter(function (s) {
            const k = s.type + '|' + s.text;
            if (seen[k]) return false;
            seen[k] = true;
            return true;
        }).slice(0, 5);
    }

    function buildIntelligenceLine(trade) {
        const bull = dirBull(trade.direction);
        const live = trade.current_price;
        const entry = parseFloat(trade.entry_price);
        const alerts = trade.alerts || [];
        const risk = alerts.filter(function (a) {
            return a.level === 'risk' || a.level === 'warning';
        });
        if (risk.length) {
            return risk[risk.length - 1].message;
        }
        if (trade.momentum_status === 'Weakening') {
            return 'Momentum slowing after expansion leg. Consider protecting partial profits.';
        }
        if (trade.structure_status === 'Weakening') {
            return 'Structure acceptance is fading. Tighten risk if key levels fail to hold.';
        }
        if (trade.vwap_status && String(trade.vwap_status).indexOf('Above') >= 0 && bull) {
            const lvl = num(entry, 2);
            return (
                'Buyers continue defending VWAP. Trade remains healthy unless ₹' +
                (lvl != null ? lvl : '—') +
                ' breaks.'
            );
        }
        if (trade.ema_status && String(trade.ema_status).indexOf('Above') >= 0 && bull) {
            return 'Price holds above short-term EMA. Momentum remains constructive while structure holds.';
        }
        return 'Trade under active monitoring. Structure and momentum updates reflect the latest 5-minute read.';
    }

    function isDangerState(trade, band) {
        const lc = String(trade.lifecycle_state || '').toLowerCase();
        if (lc.indexOf('failed') >= 0 || lc.indexOf('breakdown') >= 0) return true;
        if (band.state === 'WEAKENING') return true;
        const h = parseFloat(trade.trade_health);
        return Number.isFinite(h) && h < 35;
    }

    function renderLifecycleBar(activeStage) {
        let h = '<div class="vop-lifecycle" role="list">';
        LIFECYCLE_STAGES.forEach(function (stage, i) {
            const on = stage === activeStage;
            h +=
                '<span class="vop-lc-step' +
                (on ? ' vop-lc-step--active' : '') +
                '" role="listitem">' +
                esc(stage) +
                '</span>';
            if (i < LIFECYCLE_STAGES.length - 1) {
                h += '<span class="vop-lc-arrow" aria-hidden="true">→</span>';
            }
        });
        h += '</div>';
        return h;
    }

    function signalIcon(type) {
        if (type === 'pos') return '✅';
        if (type === 'warn') return '⚠️';
        return '❌';
    }

    function renderSignals(signals) {
        if (!signals.length) {
            return '<p class="vop-signal-empty">Awaiting structure updates…</p>';
        }
        return (
            '<ul class="vop-signals">' +
            signals
                .map(function (s) {
                    const icon = signalIcon(s.type);
                    const label =
                        s.type === 'pos' ? 'Positive' : s.type === 'warn' ? 'Caution' : 'At risk';
                    return (
                        '<li class="vop-signal vop-signal--' +
                        esc(s.type) +
                        '"><span class="vop-signal-icon" aria-hidden="true" title="' +
                        esc(label) +
                        '">' +
                        icon +
                        '</span><span class="vop-signal-text">' +
                        esc(s.text) +
                        '</span></li>'
                    );
                })
                .join('') +
            '</ul>'
        );
    }

    function renderCard(trade) {
        const id = trade.id;
        const bull = dirBull(trade.direction);
        const identity = parseIdentity(trade);
        const live = trade.current_price != null ? trade.current_price : trade.entry_price;
        const pct = pnlPct(trade, live);
        const pctStr =
            pct != null ? (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%' : '—';
        const pctCls =
            pct == null ? '' : pct >= 0 ? ' vop-pnl--pos' : ' vop-pnl--neg';
        const pnlRs = pnlRupees(trade, live);
        const pnlAmtStr = fmtPnlAmt(pnlRs);
        const pnlAmtCls =
            pnlRs == null ? '' : pnlRs >= 0 ? ' vop-pnl-amt--pos' : ' vop-pnl-amt--neg';
        const health = parseFloat(trade.trade_health);
        const band = healthBand(health);
        const disc = trade.discovery_snapshot || {};
        const tps = disc.tps_score != null ? disc.tps_score : '—';
        const ecs = disc.ecs_score != null ? disc.ecs_score : '—';
        const levels = deriveRiskLevels(trade);
        const prog = progressModel(trade, levels, live);
        const stage = mapLifecycleStage(trade.lifecycle_state);
        const signals = buildSignals(trade);
        const intel = buildIntelligenceLine(trade);
        const danger = isDangerState(trade, band);
        const dirCls = bull ? 'vop-dir--long' : 'vop-dir--short';
        const rrStr = levels.rr != null ? '1 : ' + levels.rr : '—';
        const distT =
            prog.distTargetPct != null
                ? Math.round(Math.max(0, prog.distTargetPct)) + '% to T1'
                : '—';

        return (
            '<article class="vop-card' +
            (danger ? ' vop-card--danger' : '') +
            '" data-trade-id="' +
            esc(id) +
            '" data-trade-key="' +
            esc(String(trade.stock || trade.future_symbol || id).toUpperCase()) +
            '">' +
            '<div class="vop-top">' +
            '<div class="vop-zone vop-zone--identity">' +
            '<div class="vop-symbol">' +
            renderSymbolChartLink(trade, identity) +
            '</div>' +
            '<div class="vop-sub">' +
            esc(identity.subtitle) +
            '</div>' +
            '<span class="vop-dir ' +
            dirCls +
            '">' +
            esc(bull ? 'LONG' : 'SHORT') +
            '</span>' +
            '<div class="vop-prices">' +
            '<div><span class="vop-lbl">Entry</span> <span class="vop-val" data-vop-entry>' +
            fmtInr(trade.entry_price) +
            '</span></div>' +
            '<div><span class="vop-lbl">Live</span> <span class="vop-val vop-val--live" data-vop-live>' +
            fmtInr(live) +
            '</span></div>' +
            '<div class="vop-pnl' +
            pctCls +
            '" data-vop-pnl>' +
            esc(pctStr) +
            '</div>' +
            '</div>' +
            '<div class="vop-pnl-amt' +
            pnlAmtCls +
            '" data-vop-pnl-amt>' +
            esc(pnlAmtStr) +
            '</div>' +
            '</div>' +
            '<div class="vop-zone vop-zone--risk">' +
            '<div class="vop-zone-title">Risk / Reward</div>' +
            '<div class="vop-rr-grid">' +
            '<div><span class="vop-lbl">SL</span><span class="vop-val" data-vop-sl>' +
            fmtInr(levels.sl) +
            '</span></div>' +
            '<div><span class="vop-lbl">T1</span><span class="vop-val" data-vop-t1>' +
            fmtInr(levels.t1) +
            '</span></div>' +
            '<div><span class="vop-lbl">T2</span><span class="vop-val" data-vop-t2>' +
            fmtInr(levels.t2) +
            '</span></div>' +
            '<div><span class="vop-lbl">R:R</span><span class="vop-val" data-vop-rr>' +
            esc(rrStr) +
            '</span></div>' +
            '<div><span class="vop-lbl">Risk</span><span class="vop-val" data-vop-risk>' +
            (levels.riskAmt != null ? fmtInr(levels.riskAmt) : '—') +
            '</span></div>' +
            '<div><span class="vop-lbl">To target</span><span class="vop-val" data-vop-dist>' +
            esc(distT) +
            '</span></div>' +
            '</div>' +
            '<div class="vop-progress-wrap" aria-label="Trade progress from stop to target">' +
            '<div class="vop-progress-labels"><span>SL</span><span>Entry</span><span data-vop-cur-label>Now</span><span>Target</span></div>' +
            '<div class="vop-progress-track">' +
            '<div class="vop-progress-entry" data-vop-entry-mark style="left:' +
            prog.entryPct +
            '%"></div>' +
            '<div class="vop-progress-fill" data-vop-progress style="width:' +
            prog.currentPct +
            '%"></div>' +
            '<div class="vop-progress-marker" data-vop-cur-mark style="left:' +
            prog.currentPct +
            '%"></div>' +
            '</div>' +
            '</div>' +
            '</div>' +
            '<div class="vop-zone vop-zone--health">' +
            '<div class="vop-zone-title">Trade Health</div>' +
            '<div class="vop-health-head" title="TPS = Trade Probability Score (' +
            esc(tps) +
            ') · ECS = Entry Confidence Score (' +
            esc(ecs) +
            ')" data-vop-health>' +
            '<div class="vop-health-score ' +
            band.cls +
            '">' +
            '<span class="vop-health-num">' +
            esc(num(health, 0) != null ? num(health, 0) : '—') +
            '</span>' +
            '<span class="vop-health-max">/ 100</span>' +
            '</div>' +
            '<span class="vop-health-state ' +
            band.cls +
            '" data-vop-health-state>' +
            esc(band.state) +
            '</span>' +
            '</div>' +
            '<div data-vop-signals-wrap>' +
            renderSignals(signals) +
            '</div>' +
            '</div>' +
            '</div>' +
            renderLifecycleBar(stage) +
            '<p class="vop-intel" data-vop-intel>' +
            esc(intel) +
            '</p>' +
            '<div class="vop-actions">' +
            '<div class="vop-actions-primary">' +
            '<button type="button" class="vop-btn vop-btn--ghost" disabled title="Advisory tracking only — no broker link">Trail SL</button>' +
            '<button type="button" class="vop-btn vop-btn--ghost" disabled title="Advisory tracking only — no broker link">Lock Profit</button>' +
            '</div>' +
            '<div class="vop-actions-secondary">' +
            '<button type="button" class="vop-btn vop-btn--soft" disabled title="Advisory tracking only">Partial Exit</button>' +
            '</div>' +
            '<button type="button" class="vop-btn vop-btn--close' +
            (danger ? ' vop-btn--close-emphasis' : '') +
            '" data-close-trade="' +
            esc(id) +
            '">Close Trade</button>' +
            '</div>' +
            '</article>'
        );
    }

    function snapshotKey(trade) {
        return JSON.stringify({
            id: trade.id,
            cp: trade.current_price,
            ep: trade.entry_price,
            ls: contractLotSize(trade),
            th: trade.trade_health,
            lc: trade.lifecycle_state,
            es: trade.ema_status,
            vs: trade.vwap_status,
            ms: trade.momentum_status,
            ss: trade.structure_status,
            al: (trade.alerts || []).length,
        });
    }

    function patchCard(card, trade) {
        const live = trade.current_price != null ? trade.current_price : trade.entry_price;
        const pct = pnlPct(trade, live);
        const pctStr =
            pct != null ? (pct >= 0 ? '+' : '') + pct.toFixed(2) + '%' : '—';
        const health = parseFloat(trade.trade_health);
        const band = healthBand(health);
        const levels = deriveRiskLevels(trade);
        const prog = progressModel(trade, levels, live);
        const stage = mapLifecycleStage(trade.lifecycle_state);
        const danger = isDangerState(trade, band);

        const set = function (sel, text, cls) {
            const el = card.querySelector(sel);
            if (!el) return;
            el.textContent = text;
            if (cls) {
                el.className = cls;
            }
        };

        set('[data-vop-live]', fmtInr(live));
        const pnlEl = card.querySelector('[data-vop-pnl]');
        if (pnlEl) {
            pnlEl.textContent = pctStr;
            pnlEl.className =
                'vop-pnl' + (pct == null ? '' : pct >= 0 ? ' vop-pnl--pos' : ' vop-pnl--neg');
        }
        const pnlRs = pnlRupees(trade, live);
        const pnlAmtEl = card.querySelector('[data-vop-pnl-amt]');
        if (pnlAmtEl) {
            pnlAmtEl.textContent = fmtPnlAmt(pnlRs);
            pnlAmtEl.className =
                'vop-pnl-amt' +
                (pnlRs == null ? '' : pnlRs >= 0 ? ' vop-pnl-amt--pos' : ' vop-pnl-amt--neg');
        }
        const hWrap = card.querySelector('[data-vop-health]');
        if (hWrap) {
            const scoreEl = hWrap.querySelector('.vop-health-score');
            if (scoreEl) {
                scoreEl.className = 'vop-health-score ' + band.cls;
                const numEl = scoreEl.querySelector('.vop-health-num');
                if (numEl) {
                    numEl.textContent = num(health, 0) != null ? num(health, 0) : '—';
                }
            }
            const stateEl = hWrap.querySelector('[data-vop-health-state]');
            if (stateEl) {
                stateEl.textContent = band.state;
                stateEl.className = 'vop-health-state ' + band.cls;
            }
        }
        const fill = card.querySelector('[data-vop-progress]');
        if (fill) fill.style.width = prog.currentPct + '%';
        const curMark = card.querySelector('[data-vop-cur-mark]');
        if (curMark) curMark.style.left = prog.currentPct + '%';
        set('[data-vop-dist]', prog.distTargetPct != null ? Math.round(prog.distTargetPct) + '% to T1' : '—');
        set('[data-vop-intel]', buildIntelligenceLine(trade));

        const signalsWrap = card.querySelector('[data-vop-signals-wrap]');
        if (signalsWrap) {
            signalsWrap.innerHTML = renderSignals(buildSignals(trade));
        }

        card.querySelectorAll('.vop-lc-step').forEach(function (el) {
            const on = el.textContent.trim() === stage;
            el.classList.toggle('vop-lc-step--active', on);
        });
        card.classList.toggle('vop-card--danger', danger);
        const closeBtn = card.querySelector('[data-close-trade]');
        if (closeBtn) {
            closeBtn.classList.toggle('vop-btn--close-emphasis', danger);
        }
    }

    let _snapshots = {};
    let _onClose = null;

    function bindCloseHandlers(root) {
        if (!root) return;
        root.querySelectorAll('[data-close-trade]').forEach(function (btn) {
            btn.onclick = function () {
                const tid = parseInt(btn.getAttribute('data-close-trade'), 10);
                if (_onClose && Number.isFinite(tid)) _onClose(tid);
            };
        });
    }

    function mount(container, rows, options) {
        options = options || {};
        _onClose = options.onClose || null;
        if (!container) return;

        if (!rows || !rows.length) {
            container.innerHTML = options.emptyHtml || '';
            _snapshots = {};
            return;
        }

        const ids = rows.map(function (t) {
            return String(t.id);
        });
        const existing = container.querySelectorAll('.vop-card');
        const existingIds = Array.prototype.map.call(existing, function (c) {
            return c.getAttribute('data-trade-id');
        });

        const sameSet =
            existingIds.length === ids.length &&
            ids.every(function (id, i) {
                return existingIds[i] === String(id);
            });

        if (sameSet && existing.length) {
            rows.forEach(function (trade) {
                const card = container.querySelector('[data-trade-id="' + trade.id + '"]');
                if (!card) return;
                const key = snapshotKey(trade);
                if (_snapshots[trade.id] === key) return;
                _snapshots[trade.id] = key;
                patchCard(card, trade);
            });
            bindCloseHandlers(container);
            prefetchLotSizes(rows, container);
            bindChartClicks(container);
            return;
        }

        _snapshots = {};
        let html = '<div class="vop-stack">';
        rows.forEach(function (t) {
            _snapshots[t.id] = snapshotKey(t);
            html += renderCard(t);
        });
        html += '</div>';
        container.innerHTML = html;
        bindCloseHandlers(container);
        prefetchLotSizes(rows, container);
    }

    global.VajraOpenPositionCard = {
        mount: mount,
        renderCard: renderCard,
    };
})(window);
