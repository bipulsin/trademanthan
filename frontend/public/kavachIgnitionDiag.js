(function () {
    const API_BASE =
        window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
            ? 'http://localhost:8000'
            : window.location.origin;

    const API = API_BASE + '/api/kavach-ignition-diagnostics';

    let lastBacktestPayload = null;
    let lastLogPayload = null;
    let pollTimer = null;

    function authHeaders() {
        const t = localStorage.getItem('trademanthan_token') || '';
        return {
            Authorization: 'Bearer ' + t,
            'Content-Type': 'application/json',
        };
    }

    function pct(v) {
        if (v == null || v === '') return '—';
        return (Number(v) * 100).toFixed(1) + '%';
    }

    function setStatus(el, msg) {
        if (el) el.textContent = msg;
    }

    async function copyText(text) {
        try {
            await navigator.clipboard.writeText(text);
            return true;
        } catch (e) {
            const ta = document.createElement('textarea');
            ta.value = text;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            document.body.removeChild(ta);
            return true;
        }
    }

    function downloadText(filename, text) {
        const blob = new Blob([text], { type: 'text/plain;charset=utf-8' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        a.click();
        URL.revokeObjectURL(a.href);
    }

    function liftPp(v) {
        if (v == null || v === '') return '—';
        const n = Number(v) * 100;
        return (n >= 0 ? '+' : '') + n.toFixed(1) + 'pp';
    }

    function liftRatio(v) {
        if (v == null || v === '') return '—';
        return Number(v).toFixed(2) + '×';
    }

    function renderBacktestTable(result) {
        const wrap = document.getElementById('btTableWrap');
        if (!wrap || !result || !result.components) return;
        const comps = result.components;
        const baseline = result.baseline || {};
        const blRate = pct(baseline.favorable_rate_3bar);
        const blDetail =
            (baseline.favorable_moves != null ? baseline.favorable_moves : '—') +
            ' / ' +
            (baseline.bar_samples != null ? baseline.bar_samples : '—') +
            ' bars';
        const labels = {
            order_flow_imbalance: 'Order-flow imbalance',
            oi_triangulation: 'OI-price-volume triangulation',
            pullback_depth: 'Pullback-depth contraction',
            absorption: 'Absorption',
            vwap_slope: 'VWAP slope',
            composite_full: 'Composite (reweighted)',
        };
        const side = ((result.parameters || {}).side || '').toUpperCase();
        let rows =
            '<tr class="kid-baseline-row"><td><strong>Baseline (unconditional)</strong></td><td>' +
            blRate +
            '</td><td>—</td><td>—</td><td>—</td><td>' +
            blDetail +
            '</td></tr>';
        Object.keys(labels).forEach(function (key) {
            const c = comps[key] || {};
            let prec = '—';
            let liftPpVal = '—';
            let liftX = '—';
            let cred = '—';
            let detail = '';
            if (c.status === 'not_applicable') {
                prec = 'N/A';
                detail = c.note || 'WS only';
            } else {
                prec = pct(c.precision_3bar);
                liftPpVal = liftPp(c.lift_pp);
                liftX = liftRatio(c.lift_ratio);
                cred = c.credibility || '—';
                detail = (c.hits || 0) + ' / ' + (c.signals || 0) + ' hits';
            }
            rows +=
                '<tr><td>' +
                labels[key] +
                '</td><td>' +
                prec +
                '</td><td>' +
                liftPpVal +
                '</td><td>' +
                liftX +
                '</td><td>' +
                cred +
                '</td><td>' +
                detail +
                '</td></tr>';
        });
        wrap.innerHTML =
            '<table class="kid-table"><thead><tr><th>Component</th><th>3-bar precision</th><th>Lift (pp)</th><th>Lift (×)</th><th>Credibility</th><th>Detail</th></tr></thead><tbody>' +
            rows +
            '</tbody></table>';
        wrap.hidden = false;
    }

    function showBacktestResult(payload) {
        lastBacktestPayload = payload;
        const pre = document.getElementById('btOutput');
        const tools = document.getElementById('btTools');
        const result = payload.result || payload;
        const text = result.plain_text || JSON.stringify(result, null, 2);
        if (pre) {
            pre.textContent = text;
            pre.hidden = false;
        }
        if (tools) tools.hidden = false;
        renderBacktestTable(result);
    }

    async function pollBacktest(jobId) {
        const statusEl = document.getElementById('btStatus');
        const btn = document.getElementById('btnRunBacktest');
        try {
            const res = await fetch(API + '/backtest/status/' + encodeURIComponent(jobId), {
                headers: authHeaders(),
                cache: 'no-store',
            });
            if (!res.ok) {
                const t = await res.text();
                throw new Error(t || res.statusText);
            }
            const data = await res.json();
            if (data.status === 'running') {
                setStatus(statusEl, 'Running… (poll every 3s, expect ~7–15 min for 20 symbols)');
                return;
            }
            clearInterval(pollTimer);
            pollTimer = null;
            if (btn) btn.disabled = false;
            if (data.status === 'error') {
                setStatus(statusEl, 'Error: ' + (data.error || 'unknown'));
                showBacktestResult({ plain_text: data.error || JSON.stringify(data, null, 2) });
                return;
            }
            setStatus(statusEl, 'Done at ' + (data.finished_at || ''));
            showBacktestResult(data);
        } catch (err) {
            clearInterval(pollTimer);
            pollTimer = null;
            if (btn) btn.disabled = false;
            setStatus(statusEl, 'Poll failed: ' + err.message);
        }
    }

    async function runBacktest() {
        const btn = document.getElementById('btnRunBacktest');
        const statusEl = document.getElementById('btStatus');
        const days = parseInt(document.getElementById('btDays').value, 10) || 10;
        const symbols = parseInt(document.getElementById('btSymbols').value, 10) || 20;
        const side = document.getElementById('btSide').value || 'BULL';
        if (btn) btn.disabled = true;
        setStatus(statusEl, 'Starting backtest job…');
        document.getElementById('btTableWrap').hidden = true;
        document.getElementById('btOutput').hidden = true;
        document.getElementById('btTools').hidden = true;

        try {
            const res = await fetch(API + '/backtest/start', {
                method: 'POST',
                headers: authHeaders(),
                body: JSON.stringify({ days: days, symbols: symbols, side: side }),
            });
            if (!res.ok) {
                const t = await res.text();
                throw new Error(t || res.statusText);
            }
            const data = await res.json();
            setStatus(statusEl, 'Job ' + data.job_id.slice(0, 8) + '… running');
            if (pollTimer) clearInterval(pollTimer);
            pollTimer = setInterval(function () {
                pollBacktest(data.job_id);
            }, 3000);
            pollBacktest(data.job_id);
        } catch (err) {
            if (btn) btn.disabled = false;
            setStatus(statusEl, 'Failed: ' + err.message);
        }
    }

    function renderLogSummary(data) {
        const wrap = document.getElementById('logTableWrap');
        if (!wrap) return;
        const feed = data.feed || {};
        const of = data.orderflow_table || {};
        const alive = feed.thread_alive ? 'alive' : 'dead';
        const aliveLabel = feed.thread_alive ? 'YES' : 'NO';
        wrap.innerHTML =
            '<table class="kid-table">' +
            '<tr><th>Feed thread_alive</th><td><span class="kid-feed-pill ' +
            alive +
            '">' +
            aliveLabel +
            '</span></td></tr>' +
            '<tr><th>cached_instruments</th><td>' +
            (feed.cached_instruments != null ? feed.cached_instruments : '—') +
            '</td></tr>' +
            '<tr><th>orderflow row_count</th><td>' +
            (of.row_count != null ? of.row_count : 0) +
            '</td></tr>' +
            '<tr><th>orderflow updated_at min</th><td>' +
            (of.updated_at_min || '—') +
            '</td></tr>' +
            '<tr><th>orderflow updated_at max</th><td>' +
            (of.updated_at_max || '—') +
            '</td></tr>' +
            '</table>' +
            (data.ignition_empty_message
                ? '<p class="kid-hint">' + data.ignition_empty_message + '</p>'
                : '') +
            (of.empty_message ? '<p class="kid-hint">' + of.empty_message + '</p>' : '');

        const rows = data.ignition_log || [];
        if (rows.length) {
            let tr = '';
            rows.forEach(function (r) {
                tr +=
                    '<tr><td>' +
                    (r.computed_at || '—') +
                    '</td><td>' +
                    (r.symbol || '') +
                    '</td><td>' +
                    (r.side || '') +
                    '</td><td>' +
                    (r.ignition_score != null ? r.ignition_score : '—') +
                    '</td><td>' +
                    (r.ignition_building ? 'yes' : 'no') +
                    '</td></tr>';
            });
            wrap.innerHTML +=
                '<table class="kid-table" style="margin-top:12px"><thead><tr><th>computed_at</th><th>symbol</th><th>side</th><th>score</th><th>building</th></tr></thead><tbody>' +
                tr +
                '</tbody></table>';
        }
        wrap.hidden = false;
    }

    async function loadLiveLog() {
        const btn = document.getElementById('btnLoadLog');
        const statusEl = document.getElementById('logStatus');
        const limit = parseInt(document.getElementById('logLimit').value, 10) || 50;
        if (btn) btn.disabled = true;
        setStatus(statusEl, 'Loading…');
        try {
            const res = await fetch(API + '/live-log?limit=' + limit, {
                headers: authHeaders(),
                cache: 'no-store',
            });
            if (!res.ok) {
                const t = await res.text();
                throw new Error(t || res.statusText);
            }
            const data = await res.json();
            lastLogPayload = data;
            const pre = document.getElementById('logOutput');
            if (pre) {
                pre.textContent = data.plain_text || JSON.stringify(data, null, 2);
                pre.hidden = false;
            }
            document.getElementById('logTools').hidden = false;
            renderLogSummary(data);
            setStatus(statusEl, 'Fetched ' + (data.fetched_at || ''));
        } catch (err) {
            setStatus(statusEl, 'Failed: ' + err.message);
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    function init() {
        document.getElementById('btnRunBacktest').addEventListener('click', runBacktest);
        document.getElementById('btnLoadLog').addEventListener('click', loadLiveLog);

        document.getElementById('btCopy').addEventListener('click', function () {
            const r = lastBacktestPayload && (lastBacktestPayload.result || lastBacktestPayload);
            const text = (r && r.plain_text) || document.getElementById('btOutput').textContent;
            copyText(text).then(function () {
                setStatus(document.getElementById('btStatus'), 'Copied to clipboard');
            });
        });
        document.getElementById('btDownload').addEventListener('click', function () {
            const r = lastBacktestPayload && (lastBacktestPayload.result || lastBacktestPayload);
            const text = (r && r.plain_text) || document.getElementById('btOutput').textContent;
            downloadText('ignition-backtest.txt', text);
        });
        document.getElementById('logCopy').addEventListener('click', function () {
            const text = (lastLogPayload && lastLogPayload.plain_text) || document.getElementById('logOutput').textContent;
            copyText(text).then(function () {
                setStatus(document.getElementById('logStatus'), 'Copied to clipboard');
            });
        });
        document.getElementById('logDownload').addEventListener('click', function () {
            const text = (lastLogPayload && lastLogPayload.plain_text) || document.getElementById('logOutput').textContent;
            downloadText('ignition-live-log.txt', text);
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
