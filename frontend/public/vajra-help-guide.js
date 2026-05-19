/**
 * TWCTO Vajra — in-app help guide (callout next to section title).
 */
(function (global) {
    function guideHtml() {
        return (
            '<h2 id="vajraHelpTitle">TWCTO Vajra — Futures Rating</h2>' +
            '<p class="vajra-help-lead">A discretionary trade operating system for <strong>current-month F&amp;O futures</strong>. ' +
            'Vajra finds <strong>early transitions</strong> on 30-minute bars, times entries on 5-minute bars, then helps you validate, paper-track, and journal trades — it does <strong>not</strong> place broker orders.</p>' +
            '<div class="vajra-help-callout"><strong>Important:</strong> You execute every entry and exit in your terminal. ' +
            'Vajra scores, checks, and monitors — you decide.</div>' +
            '<h3>Where to find Vajra</h3>' +
            '<p>Open <strong>Vajra Futures</strong> from the left menu. Three sections: <strong>Screen</strong> (ratings), <strong>Open Position</strong>, <strong>Closed Trades</strong>.</p>' +
            '<h3>How the engine runs</h3>' +
            '<ol style="padding-left:1.2rem;margin:0 0 12px;font-size:0.84rem;line-height:1.5">' +
            '<li><strong>Discovery (30m)</strong> — ~200 current-month futures: TPS + ECS + transition labels every 5 minutes (9:30–15:00 IST, trading days).</li>' +
            '<li><strong>Shortlist validation (5m)</strong> — highest TPS names get an extra 5m execution-structure pass.</li>' +
            '<li><strong>EES refresh (5m)</strong> — Executable Entry Score and Entry State recomputed on 5m candles each run.</li>' +
            '<li><strong>Your workflow</strong> — ENTER → checklist → ACTIVATE → manage → CLOSE → journal.</li>' +
            '</ol>' +
            '<p>The screen <strong>auto-refreshes shortly after each scheduled run</strong> (watch the <em>Updated</em> line in the meta row).</p>' +
            '<h3>Three scores (do not confuse them)</h3>' +
            '<table class="vajra-help-table"><thead><tr><th>Score</th><th>Timeframe</th><th>Meaning</th></tr></thead><tbody>' +
            '<tr><td><strong>TPS</strong><br>Transition Potential</td><td>30m</td>' +
            '<td>Early transition quality: momentum shift, VWAP/EMA reclaim, compression, shallow pullback, low extension. Discovery ranking — does not require LONG A+.</td></tr>' +
            '<tr><td><strong>EES</strong><br>Executable Entry</td><td>5m</td>' +
            '<td>Can I enter right now with acceptable risk? Timing, extension, reclaim quality, distance to levels. Refreshes every 5 minutes.</td></tr>' +
            '<tr><td><strong>ECS</strong><br>Expansion Confirmation</td><td>30m + 1hr</td>' +
            '<td>Classic Vajra confirmation: structure, breakout, OBV, volume, trend. Mature / continuation context.</td></tr>' +
            '</tbody></table>' +
            '<p>Sorted by <strong>Entry State</strong> (EXECUTABLE &rarr; PULLBACK &rarr; WATCHLIST &rarr; AVOID), then <strong>TPS + EES</strong> within each band.</p>' +
            '<h3>Discovery table (top 8)</h3>' +
            '<ul>' +
            '<li><strong>Symbol</strong> — current-month future.</li>' +
            '<li><strong>Status → Entry State band</strong> — Status, TPS, EES, Entry State; second line = Transition detail.</li>' +
            '<li><strong>ECS, VWAP, Pullback, Extension</strong> — confirmation and risk.</li>' +
            '<li><strong>Action</strong> — ENTER workflow (see below).</li>' +
            '</ul>' +
            '<h4>Entry State (from EES)</h4>' +
            '<table class="vajra-help-table"><thead><tr><th>EES</th><th>State</th><th>Meaning</th></tr></thead><tbody>' +
            '<tr><td>&ge; 75</td><td>EXECUTABLE</td><td>Good 5m timing when TPS supports entry.</td></tr>' +
            '<tr><td>60–74</td><td>PULLBACK</td><td>Prefer shallow pullback first.</td></tr>' +
            '<tr><td>45–59</td><td>WATCHLIST</td><td>Monitor; do not chase.</td></tr>' +
            '<tr><td>&lt; 45</td><td>AVOID</td><td>Extended — avoid chasing.</td></tr>' +
            '</tbody></table>' +
            '<h4>Action buttons</h4>' +
            '<ul>' +
            '<li><strong>ENTER</strong> — TPS &ge; 52 and EES &ge; 65; opens validation modal.</li>' +
            '<li><strong>WAIT PULLBACK / WATCH / EXTENDED</strong> — disabled; hover for reason.</li>' +
            '</ul>' +
            '<h3>Timeframes (transition mode)</h3>' +
            '<ul>' +
            '<li><strong>30m</strong> — TPS + ECS discovery.</li>' +
            '<li><strong>5m</strong> — EES + shortlist execution validation.</li>' +
            '<li><strong>1hr</strong> — HTF bias for ECS.</li>' +
            '</ul>' +
            '<p>Scan TF / HTF dropdowns on this page are not used by the live transition pipeline.</p>' +
            '<h3>Telegram (optional)</h3>' +
            '<p>Settings &rarr; Telegram ON &rarr; <strong>Vajra ENTER alerts (Futures)</strong>. One message per symbol per session when ENTER first becomes available.</p>' +
            '<h3>Trade Validation &amp; Entry</h3>' +
            '<h4>Step A</h4>' +
            '<p>Symbol, direction, entry price, lots, entry time. <strong>Next</strong> loads checklist. <strong>Cancel</strong> closes the modal.</p>' +
            '<h4>Step B — Checklist</h4>' +
            '<p>Structure and Market checks are automated from 5m + index/sector data (verify yourself). Psychology is <strong>manual only</strong>.</p>' +
            '<table class="vajra-help-table"><thead><tr><th>Group</th><th>Item</th><th>What it means</th></tr></thead><tbody>' +
            '<tr><td rowspan="8">Structure</td><td>VWAP reclaimed</td><td>Correct side of session VWAP after pullback.</td></tr>' +
            '<tr><td>EMA reclaimed</td><td>Price reclaimed EMA(5).</td></tr>' +
            '<tr><td>Hilega-Milega forming</td><td>Pullback + recovery building (manual).</td></tr>' +
            '<tr><td>Pullback shallow</td><td>(auto) Retracement &lt; 40% of impulse.</td></tr>' +
            '<tr><td>No vertical exhaustion</td><td>(auto) Not 3+ extended trend candles.</td></tr>' +
            '<tr><td>Candle spread healthy</td><td>(auto) Body vs wick vs recent average.</td></tr>' +
            '<tr><td>Not into major level</td><td>(auto) Not hugging resistance/support.</td></tr>' +
            '<tr><td>Reclaim candle strong</td><td>(auto) Strong close vs VWAP/EMA.</td></tr>' +
            '<tr><td rowspan="4">Market</td><td>Market structure supportive</td><td>(auto) NIFTY &amp; Bank NIFTY align.</td></tr>' +
            '<tr><td>Sector not conflicting</td><td>(auto) Sector not against your direction.</td></tr>' +
            '<tr><td>Volume acceptable</td><td>(auto) Volume &gt; 1.2&times; 20-bar average.</td></tr>' +
            '<tr><td>Not extended from VWAP</td><td>(auto) Not chasing (&lt; ~1.5% from VWAP).</td></tr>' +
            '<tr><td rowspan="5">Psychology</td><td>Not FOMO / Risk accepted / Not revenge / Comfortable exit / Structure after pullback</td>' +
            '<td><strong>Tick all manually</strong> before ACTIVATE.</td></tr>' +
            '</tbody></table>' +
            '<p><strong>ACTIVATE TRADE</strong> is disabled if more than <strong>70%</strong> of Structure + Market checks are not PASS (warn/fail count as not pass).</p>' +
            '<p>Read-only metrics and pre-entry warnings appear on this step.</p>' +
            '<h4>ACTIVATE TRADE</h4>' +
            '<p>Saves to <strong>Vajra Futures &rarr; Open Position</strong>. 5-minute monitoring while open. No broker execution.</p>' +
            '<h3>Open Position</h3>' +
            '<ul>' +
            '<li><strong>Lifecycle</strong> — Early Transition &rarr; Expansion &rarr; Consolidation / Rotation &rarr; Exhaustion &rarr; Breakdown Risk &rarr; Failed Structure.</li>' +
            '<li><strong>Health (0–100)</strong> — Strong 80+, Healthy 60–79, Weakening 40–59, High Risk 20–39, Failure Risk &lt;20.</li>' +
            '<li><strong>Alerts</strong> — interpretation only, not buy/sell.</li>' +
            '<li><strong>CLOSE TRADE</strong> — exit + reasons &rarr; Closed Trades.</li>' +
            '</ul>' +
            '<h3>Recommended workflow</h3>' +
            '<p><strong>Watch Updated &rarr; scan TPS+EES+ECS &rarr; ENTER &rarr; psychology &rarr; ACTIVATE &rarr; manage Open Position &rarr; CLOSE &rarr; review Closed Trades.</strong></p>'
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
