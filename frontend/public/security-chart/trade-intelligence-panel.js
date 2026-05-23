/**
 * Generic Trade Intelligence Panel — presentation-only, decoupled from chart lifecycle.
 */
(function (global) {
    'use strict';

    /** Full display names for score abbreviations (Vajra help / pipeline docs). */
    const METRIC_LABELS = {
        tps: 'Transition Potential Score',
        ecs: 'Expansion Confirmation Score',
        evs: 'Expansion Velocity Score',
        evs_score: 'Expansion Velocity Score',
    };

    const SKIP_KEYS = {
        insight: 1,
        insightBanner: 1,
        sections: 1,
        symbol: 1,
        direction: 1,
        livePnlPct: 1,
        pnlPct: 1,
        livePrice: 1,
        metadata: 1,
    };

    const SECTION_RULES = [
        {
            title: 'TRADE HEALTH',
            keys: [
                'tradeHealth',
                'trade_health',
                'health',
                'tps',
                'ecs',
                'evs_score',
                'evs',
                'setupQuality',
                'setup_quality',
                'trade_quality_score',
                'ees_score',
                'conviction_score',
            ],
        },
        {
            title: 'MARKET STRUCTURE',
            keys: [
                'ema',
                'emaState',
                'ema_state',
                'ema_status',
                'ema_reclaim_status',
                'vwap',
                'vwapState',
                'vwap_state',
                'vwap_status',
                'vwap_reclaim_status',
                'structure',
                'structure_status',
                'momentum',
                'momentum_status',
                'transition_state',
                'trend',
            ],
        },
        {
            title: 'ORDERFLOW',
            keys: [
                'buyersActive',
                'buyers_active',
                'pullback',
                'pullbackQuality',
                'pullback_quality',
                'pullback_quality_score',
                'volume',
                'volumeBehavior',
                'volume_behavior',
                'volume_pass',
                'obv',
                'obv_label',
            ],
        },
        {
            title: 'LIFECYCLE',
            keys: [
                'lifecycle',
                'lifecycle_state',
                'currentStage',
                'current_stage',
                'pipeline_stage',
                'entry_state',
                'institutionalBias',
                'institutional_bias',
                'reversal_risk',
                'armed',
                'enter_enabled',
                'rr',
                'risk_reward',
                'market_phase',
                'trade_type',
            ],
        },
    ];

    function esc(s) {
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function labelize(key) {
        const raw = String(key || '').trim();
        const mapped = METRIC_LABELS[raw.toLowerCase()];
        if (mapped) return mapped;
        return raw
            .replace(/_/g, ' ')
            .replace(/([a-z])([A-Z])/g, '$1 $2')
            .replace(/\b\w/g, function (c) {
                return c.toUpperCase();
            })
            .trim();
    }

    function isEmpty(v) {
        if (v == null || v === '') return true;
        if (typeof v === 'boolean') return false;
        if (typeof v === 'number' && Number.isFinite(v)) return false;
        return String(v).trim() === '';
    }

    function formatValue(v) {
        if (typeof v === 'boolean') return v ? 'Yes' : 'No';
        if (typeof v === 'number' && Number.isFinite(v)) {
            return Number.isInteger(v) ? String(v) : v.toFixed(1);
        }
        return String(v).trim();
    }

    function semanticTone(key, raw) {
        const k = String(key || '').toLowerCase();
        const s = String(raw == null ? '' : raw).toLowerCase();
        if (typeof raw === 'boolean') return raw ? 'pos' : 'neg';
        if (s.indexOf('fail') >= 0 || s.indexOf('weak') >= 0 || s.indexOf('below') >= 0) {
            return 'neg';
        }
        if (s.indexOf('warn') >= 0 || s.indexOf('slow') >= 0 || s.indexOf('caution') >= 0) {
            return 'warn';
        }
        if (
            s.indexOf('pass') >= 0 ||
            s.indexOf('strong') >= 0 ||
            s.indexOf('above') >= 0 ||
            s.indexOf('support') >= 0 ||
            s.indexOf('strengthen') >= 0 ||
            s.indexOf('bull') >= 0 ||
            s === 'yes' ||
            s === 'a+' ||
            s.indexOf('expansion') >= 0
        ) {
            return 'pos';
        }
        if (k.indexOf('lifecycle') >= 0 || k.indexOf('stage') >= 0 || k.indexOf('pipeline') >= 0) {
            return 'lifecycle';
        }
        if (k.indexOf('setup') >= 0 || k.indexOf('quality') >= 0) {
            if (s.indexOf('a') === 0) return 'premium';
        }
        if (k.indexOf('tps') >= 0 || k.indexOf('ecs') >= 0 || k.indexOf('health') >= 0) {
            const n = parseFloat(raw);
            if (Number.isFinite(n)) {
                if (n >= 80) return 'pos';
                if (n >= 55) return 'neutral';
                return 'warn';
            }
        }
        return 'neutral';
    }

    function metricHtml(label, value, key) {
        const text = formatValue(value);
        const tone = semanticTone(key || label, value);
        const isLifecycle = tone === 'lifecycle';
        const valCls =
            'tip-metric__val' +
            (isLifecycle ? ' tip-metric__val--lifecycle' : ' tip-metric__val--' + tone);
        const inner = isLifecycle
            ? '<span class="tip-lifecycle-badge">' + esc(text) + '</span>'
            : esc(text);
        return (
            '<div class="tip-metric">' +
            '<span class="tip-metric__lbl">' +
            esc(label) +
            '</span>' +
            '<span class="' +
            valCls +
            '">' +
            inner +
            '</span></div>'
        );
    }

    function sectionsFromExplicit(data) {
        if (!data || !Array.isArray(data.sections)) return null;
        const out = [];
        data.sections.forEach(function (sec) {
            if (!sec || !sec.metrics || !sec.metrics.length) return;
            const metrics = [];
            sec.metrics.forEach(function (m) {
                if (!m || isEmpty(m.value)) return;
                metrics.push({
                    label: m.label || labelize(m.key || ''),
                    value: m.value,
                    key: m.key || m.label,
                });
            });
            if (metrics.length) out.push({ title: sec.title || 'METRICS', metrics: metrics });
        });
        return out.length ? out : null;
    }

    function sectionsFromFlat(data) {
        const used = {};
        const sections = [];
        SECTION_RULES.forEach(function (rule) {
            const metrics = [];
            rule.keys.forEach(function (key) {
                if (Object.prototype.hasOwnProperty.call(data, key) && !isEmpty(data[key])) {
                    metrics.push({ label: labelize(key), value: data[key], key: key });
                    used[key] = true;
                }
            });
            if (metrics.length) sections.push({ title: rule.title, metrics: metrics });
        });
        const misc = [];
        Object.keys(data).forEach(function (key) {
            if (SKIP_KEYS[key] || used[key] || isEmpty(data[key])) return;
            if (typeof data[key] === 'object') return;
            misc.push({ label: labelize(key), value: data[key], key: key });
        });
        if (misc.length) sections.push({ title: 'CONTEXT', metrics: misc });
        return sections;
    }

    function buildSections(data) {
        if (!data || typeof data !== 'object') return [];
        const explicit = sectionsFromExplicit(data);
        if (explicit) return explicit;
        return sectionsFromFlat(data);
    }

    function panelCacheKey(data, insight) {
        try {
            return JSON.stringify({ d: data, i: insight });
        } catch (e) {
            return String(Date.now());
        }
    }

    function render(host, payload) {
        if (!host) return;
        payload = payload || {};
        const data = payload.screenerData || payload.data || payload;
        const insight =
            payload.insight ||
            payload.insightBanner ||
            (data && (data.insight || data.insightBanner)) ||
            '';
        const sections = buildSections(data);
        const key = panelCacheKey(data, insight);
        if (host._tipCacheKey === key) return;
        host._tipCacheKey = key;

        if (!sections.length && !insight) {
            host.innerHTML = '';
            host.classList.add('tip-panel--empty');
            host.style.display = 'none';
            return;
        }
        host.classList.remove('tip-panel--empty');
        host.style.display = '';

        let html = '<div class="tip-panel">';
        if (insight) {
            html +=
                '<div class="tip-insight" role="note">' + esc(String(insight)) + '</div>';
        }
        sections.forEach(function (sec, idx) {
            html += '<section class="tip-section">';
            html += '<h4 class="tip-section__title">' + esc(sec.title) + '</h4>';
            html += '<div class="tip-section__grid">';
            sec.metrics.forEach(function (m) {
                html += metricHtml(m.label, m.value, m.key);
            });
            html += '</div></section>';
            if (idx < sections.length - 1) {
                html += '<div class="tip-section__rule" aria-hidden="true"></div>';
            }
        });
        html += '</div>';
        host.innerHTML = html;
    }

    function clear(host) {
        if (!host) return;
        host._tipCacheKey = null;
        host.innerHTML = '';
        host.classList.add('tip-panel--empty');
        host.style.display = 'none';
    }

    global.TradeIntelligencePanel = {
        render: render,
        clear: clear,
        buildSections: buildSections,
        semanticTone: semanticTone,
    };
})(typeof window !== 'undefined' ? window : globalThis);
