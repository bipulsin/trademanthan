/**
 * TWCTO Vajra — in-app help guide (callout next to section title).
 */
(function (global) {
    function guideHtml() {
        return (
            '<h2 id="vajraHelpTitle">TWCTO Vajra — Futures Rating</h2>' +
            '<p class="vajra-help-lead">A discretionary trade operating system for current-month F&amp;O futures. ' +
            'It helps you find <strong>early transitions</strong> before trends mature — then validate, paper-track, and manage trades with structure — not auto-buy/sell.</p>' +
            '<div class="vajra-help-callout"><strong>Important:</strong> Vajra does not place broker orders. ' +
            'You decide every entry and exit. The app scores, checks, and monitors — you execute.</div>' +
            '<h3>How the system is organized</h3>' +
            '<ol style="padding-left:1.2rem;margin:0 0 12px;font-size:0.84rem;line-height:1.5">' +
            '<li><strong>Discovery</strong> — scan ~200 stocks on 30-minute bars (TPS ranking).</li>' +
            '<li><strong>Shortlist validation</strong> — top names get an extra 5-minute structure check.</li>' +
            '<li><strong>ENTER workflow</strong> — your manual checklist before activating a trade.</li>' +
            '<li><strong>Running cockpit</strong> — lifecycle, health, and alerts while the trade is open.</li>' +
            '<li><strong>Journal</strong> — closed trades with reasons and checklist snapshot.</li>' +
            '</ol>' +
            '<h3>Two scores (do not confuse them)</h3>' +
            '<table class="vajra-help-table"><thead><tr><th>Score</th><th>Meaning</th><th>Use</th></tr></thead><tbody>' +
            '<tr><td><strong>TPS</strong><br>Transition Potential</td>' +
            '<td>Early transition quality: momentum improving, VWAP/EMA reclaim, compression phases, shallow pullback, low extension.</td>' +
            '<td><strong>Discovery only.</strong> List is sorted by TPS (highest first). Does not require mature trend or LONG A+.</td></tr>' +
            '<tr><td><strong>ECS</strong><br>Expansion Confirmation</td>' +
            '<td>Classic Vajra confirmation: structure, breakout, OBV, volume, trend passes.</td>' +
            '<td>Mature trend / continuation context. Useful after transition or for management.</td></tr>' +
            '</tbody></table>' +
            '<h3>Discovery table (top 8)</h3>' +
            '<p>Refreshes every 5 minutes in market hours. Columns:</p>' +
            '<ul>' +
            '<li><strong>Status</strong> — e.g. EARLY LONG/SHORT TRANSITION, LONG, LONG (A+).</li>' +
            '<li><strong>TPS / ECS</strong> — discovery vs confirmation strength (0–100).</li>' +
            '<li><strong>Transition</strong> — what is forming (VWAP reclaim, momentum shift, etc.).</li>' +
            '<li><strong>VWAP / Pullback / Extension</strong> — reclaim state, pullback quality, late-entry risk.</li>' +
            '<li><strong>ENTER</strong> — opens Trade Validation &amp; Entry (no broker order).</li>' +
            '</ul>' +
            '<h3>Timeframes</h3>' +
            '<ul>' +
            '<li><strong>30m</strong> — primary discovery (TPS + ECS baseline).</li>' +
            '<li><strong>5m</strong> — execution validation on shortlisted symbols only.</li>' +
            '<li><strong>1hr</strong> — optional higher-timeframe directional bias.</li>' +
            '</ul>' +
            '<h3>Trade Validation &amp; Entry</h3>' +
            '<h4>Section A — Basic entry</h4>' +
            '<p>Symbol (read-only), direction, entry price, lots (default 1), entry time. Click <strong>Next</strong> for the checklist.</p>' +
            '<h4>Section B — Structure checklist (5m)</h4>' +
            '<p>Two-column checklist. Items marked <em>(auto)</em> are pre-filled from live 5m candles and index/sector data — always verify yourself.</p>' +
            '<table class="vajra-help-table"><thead><tr><th>Group</th><th>Item</th><th>What it means</th></tr></thead><tbody>' +
            '<tr><td rowspan="8">Structure</td><td>VWAP reclaimed</td><td>Price is on the correct side of session VWAP after a pullback.</td></tr>' +
            '<tr><td>EMA reclaimed</td><td>Price reclaimed EMA(5) — early trend resumption.</td></tr>' +
            '<tr><td>Hilega-Milega forming</td><td>Pullback + recovery pattern building (manual).</td></tr>' +
            '<tr><td>Pullback shallow</td><td>(auto) Retracement &lt; 40% of prior impulse range.</td></tr>' +
            '<tr><td>No vertical exhaustion</td><td>(auto) Not 3+ strong trend candles with large upper wicks.</td></tr>' +
            '<tr><td>Candle spread healthy</td><td>(auto) Body larger than wick vs recent average.</td></tr>' +
            '<tr><td>Not into major level</td><td>(auto) Not sitting just under resistance / above support (&gt; 0.7R away).</td></tr>' +
            '<tr><td>Reclaim candle strong</td><td>(auto) Close strong vs VWAP/EMA and candle range.</td></tr>' +
            '<tr><td rowspan="4">Market</td><td>Market structure supportive</td><td>(auto) NIFTY &amp; Bank NIFTY direction align with your trade.</td></tr>' +
            '<tr><td>Sector not conflicting</td><td>(auto) Stock sector not against your direction (sector movers).</td></tr>' +
            '<tr><td>Volume acceptable</td><td>(auto) Current volume &gt; 1.2× 20-bar average.</td></tr>' +
            '<tr><td>Not extended from VWAP</td><td>(auto) Distance from VWAP &lt; ~1.5% (not chasing).</td></tr>' +
            '<tr><td rowspan="5">Psychology</td><td>Not FOMO / Risk accepted / Not revenge / Comfortable exit / Structure after pullback</td>' +
            '<td><strong>You must tick manually</strong> — discipline gate before ACTIVATE.</td></tr>' +
            '</tbody></table>' +
            '<h4>Section C — Auto metrics (read-only)</h4>' +
            '<p>TPS, ECS, extension risk, pullback quality, trend strength, momentum state, market phase, HTF bias — snapshot at validation time.</p>' +
            '<h4>Section D — Pre-entry warnings</h4>' +
            '<p>Highlighted if extension is high, price is far from VWAP, move is vertical, level is nearby, EMA reclaim is weak, or pullback is deep.</p>' +
            '<h4>ACTIVATE TRADE</h4>' +
            '<p>Saves the trade under <strong>Vajra managed</strong> in Running order / Open Positions. Starts 5-minute monitoring. No broker execution.</p>' +
            '<h3>Running trade cockpit</h3>' +
            '<ul>' +
            '<li><strong>Lifecycle</strong> — Early Transition → Expansion → Consolidation / Rotation → Exhaustion → Breakdown Risk → Failed Structure.</li>' +
            '<li><strong>Trade health (0–100)</strong> — Strong (80+), Healthy (60–79), Weakening (40–59), High Risk (20–39), Failure Risk (&lt;20).</li>' +
            '<li><strong>Alerts</strong> — structural interpretation only (not buy/sell).</li>' +
            '<li><strong>CLOSE TRADE</strong> — exit price + reasons → journal.</li>' +
            '</ul>' +
            '<h3>Quick workflow</h3>' +
            '<p><strong>Scan TPS list → ENTER → psychology ticks → ACTIVATE → manage → CLOSE → review journal.</strong></p>'
        );
    }

    function ensureModal() {
        let m = document.getElementById('vajraHelpModal');
        if (m) return m;
        m = document.createElement('div');
        m.id = 'vajraHelpModal';
        m.className = 'vajra-help-modal';
        m.setAttribute('aria-hidden', 'true');
        m.setAttribute('role', 'dialog');
        m.setAttribute('aria-labelledby', 'vajraHelpTitle');
        m.innerHTML = (
            '<div class="vajra-help-backdrop" data-vajra-help-close="1"></div>'
            + '<div class="vajra-help-panel" role="document">'
            + '<div id="vajraHelpBody"></div>'
            + '<motion class="vajra-help-close-row">'
            + '<button type="button" class="vajra-help-close-btn" data-vajra-help-close="1">Close guide</button>'
            + '</div></div>'
        ).replace('<motion class="vajra-help-close-row">', '<div class="vajra-help-close-row">')
        document.body.appendChild(m)
        m.querySelectorAll('[data-vajra-help-close]').forEach(function (el) {
            el.addEventListener('click', close);
        });
        document.addEventListener('keydown', function (ev) {
            if (ev.key === 'Escape' && m.classList.contains('vajra-help-modal--open')) close();
        });
        return m;
    }

    function open() {
        let m = document.getElementById('vajraHelpModal');
        if (!m) m = ensureModal();
        const body = document.getElementById('vajraHelpBody');
        if (body) body.innerHTML = guideHtml();
        m.classList.add('vajra-help-modal--open');
        m.setAttribute('aria-hidden', 'false');
    }

    function close() {
        const m = document.getElementById('vajraHelpModal');
        if (m) {
            m.classList.remove('vajra-help-modal--open');
            m.setAttribute('aria-hidden', 'true');
        }
    }

    function init(opts) {
        const prefix = (opts && opts.prefix) || 'df';
        const btn = document.getElementById(prefix + 'VajraHelpBtn');
        if (!btn) return;
        btn.addEventListener('click', open);
    }

    global.VajraHelpGuide = { init: init, open: open, close: close };
})(window);
