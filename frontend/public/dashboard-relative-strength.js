/**
 * Dashboard: Relative Strength Scanner.
 * Reads the latest scan snapshot from /api/dashboard/relative-strength and renders
 * Top 5 Bullish / Bearish current-month futures (RS vs NIFTY + Kavach + Trade Score).
 * The dashboard never recalculates — the 5-min backend scheduler owns all maths.
 */
(function () {
    const API = "/api/dashboard/relative-strength";
    const POLL_MS = 5 * 60 * 1000;
    const RADAR_POLL_MS = 5 * 60 * 1000;
    const MATURITY_TOOLTIP =
        "FRESH = first day on this list, highest continuation odds. " +
        "CONTINUING = been here before but no exhaustion signal. " +
        "EXTENDED = yesterday's move was unusually large for this stock — higher chance of profit-taking today. " +
        "STRETCHED = 4+ consecutive days on the list — treat with reduced conviction regardless of setup quality.";
    const MATURITY_ORDER = { FRESH: 0, CONTINUING: 1, EXTENDED: 2, STRETCHED: 3 };
    let timer = null;
    let countdownTimer = null;
    let nextRefreshAt = 0;
    let chartEnginePromise = null;
    let sortMode = "score";
    let lastData = null;
    let rsCfg = { show_ema10_passive: true, alert_sound_enabled: false };
    let seenTriggered = new Set();

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function escapeAttr(s) {
        return escapeHtml(s).replace(/"/g, "&quot;");
    }

    function istMinutesNow() {
        var parts = new Intl.DateTimeFormat("en-GB", {
            timeZone: "Asia/Kolkata", hour12: false, hour: "2-digit", minute: "2-digit",
        }).formatToParts(new Date());
        var h = 0, m = 0;
        parts.forEach(function (p) {
            if (p.type === "hour") h = parseInt(p.value, 10);
            if (p.type === "minute") m = parseInt(p.value, 10);
        });
        return h * 60 + m;
    }

    function playTriggerSound() {
        try {
            var ctx = new (window.AudioContext || window.webkitAudioContext)();
            var o = ctx.createOscillator();
            var g = ctx.createGain();
            o.connect(g);
            g.connect(ctx.destination);
            o.frequency.value = 880;
            g.gain.value = 0.08;
            o.start();
            o.stop(ctx.currentTime + 0.25);
        } catch (_) {}
    }

    function checkTriggeredAlerts(setups) {
        if (!setups || !setups.length) return;
        var start = Number(rsCfg.alert_window_start_min || 9 * 60 + 25);
        var end = Number(rsCfg.alert_window_end_min || 14 * 60 + 30);
        var mins = istMinutesNow();
        if (mins < start || mins > end) return;
        setups.forEach(function (s) {
            var st = (s.state || "").toUpperCase();
            if (st !== "TRIGGERED" && st !== "TRIGGERED_CHOP") return;
            var key = s.symbol + ":" + st;
            if (seenTriggered.has(key)) return;
            seenTriggered.add(key);
            if (typeof Notification !== "undefined") {
                if (Notification.permission === "granted") {
                    new Notification("RS Setup TRIGGERED", {
                        body: s.symbol + " · " + s.side + " · SL " + fmtNum(s.sl_pct, 2) + "%",
                    });
                } else if (Notification.permission !== "denied") {
                    Notification.requestPermission();
                }
            }
            if (rsCfg.alert_sound_enabled !== false) playTriggerSound();
        });
    }

    async function loadRsCfg() {
        try {
            var res = await fetch("/api/dashboard/relative-strength/config", { credentials: "same-origin" });
            rsCfg = await res.json();
        } catch (_) {}
    }

    function fmtNum(n, dp) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        return Number(n).toLocaleString(undefined, {
            minimumFractionDigits: dp,
            maximumFractionDigits: dp,
        });
    }

    function fmtSignedPct(n) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        const x = Number(n);
        return (x > 0 ? "+" : "") + x.toFixed(2) + "%";
    }

    function tradeScoreClass(score) {
        const s = Number(score);
        if (s >= 90) return "rs-score--darkgreen";
        if (s >= 80) return "rs-score--green";
        if (s >= 70) return "rs-score--lightgreen";
        if (s >= 60) return "rs-score--yellow";
        return "rs-score--grey";
    }

    function dirBadge(isBull, bullLabel, bearLabel) {
        if (isBull === null || isBull === undefined) return "—";
        const cls = isBull ? "rs-badge--up" : "rs-badge--down";
        return `<span class="rs-badge ${cls}">${isBull ? bullLabel : bearLabel}</span>`;
    }

    function kavachBadge(state) {
        const up = /^(BUY|READY|WATCH)$/.test(state || "");
        const cls = up ? "rs-badge--up" : "rs-badge--down";
        return `<span class="rs-badge ${cls}">${escapeHtml(state || "—")}</span>`;
    }

    function maturityBadge(tag, days) {
        const t = (tag || "FRESH").toUpperCase();
        let cls = "rs-maturity--fresh";
        let text = "FRESH";
        if (t === "CONTINUING") {
            cls = "rs-maturity--continuing";
            text = "DAY " + (days || 2);
        } else if (t === "EXTENDED") {
            cls = "rs-maturity--extended";
            text = "EXTENDED";
        } else if (t === "STRETCHED") {
            cls = "rs-maturity--stretched";
            text = "STRETCHED · " + (days || 4) + "D";
        }
        return `<span class="rs-maturity-badge ${cls}">${escapeHtml(text)}</span>`;
    }

    function rowClass(tag) {
        const t = (tag || "").toUpperCase();
        if (t === "EXTENDED") return "rs-scanner-row rs-scanner-row--extended";
        if (t === "STRETCHED") return "rs-scanner-row rs-scanner-row--stretched";
        return "rs-scanner-row";
    }

    function sortRows(rows) {
        if (!rows || !rows.length) return rows || [];
        const list = rows.slice();
        if (sortMode === "maturity") {
            list.sort(function (a, b) {
                const oa = MATURITY_ORDER[(a.maturity_tag || "FRESH").toUpperCase()] != null
                    ? MATURITY_ORDER[(a.maturity_tag || "FRESH").toUpperCase()] : 9;
                const ob = MATURITY_ORDER[(b.maturity_tag || "FRESH").toUpperCase()] != null
                    ? MATURITY_ORDER[(b.maturity_tag || "FRESH").toUpperCase()] : 9;
                if (oa !== ob) return oa - ob;
                return Number(b.trade_score || 0) - Number(a.trade_score || 0);
            });
        }
        return list;
    }

    function convictionChips(r) {
        const chips = [];
        if (r.has_anchor) chips.push('<span class="rs-chip rs-chip--anchor">ANCHOR</span>');
        if (r.accum_active) chips.push('<span class="rs-chip rs-chip--accum">ACCUM</span>');
        if (r.chop_flag || (r.whip_penalty || 0) >= 60) chips.push('<span class="rs-chip rs-chip--chop">CHOP</span>');
        return chips.join(" ");
    }

    function setupBadge(r) {
        const st = (r.setup_state || "NEUTRAL").toUpperCase();
        if (st === "NEUTRAL" || st === "EXPIRED") return "";
        let cls = "rs-setup--neutral";
        if (st === "CONVERGING") cls = "rs-setup--conv";
        else if (st === "TRIGGERED") cls = "rs-setup--trig";
        else if (st === "TRIGGERED_CHOP" || st === "LATE") cls = "rs-setup--chop";
        let sl = "";
        if (r.sl_rupees != null && r.sl_pct != null) {
            sl = ' <span class="rs-sl-cost">SL ₹' + fmtNum(r.sl_rupees, 2) + " · " + fmtNum(r.sl_pct, 2) + "%</span>";
        }
        return '<span class="rs-setup ' + cls + '">' + escapeHtml(st.replace("_", "·")) + "</span>" + sl;
    }

    function rowHtml(r) {
        const rsCls = Number(r.rs_percent) >= 0 ? "rs-pos" : "rs-neg";
        const vwapCls = r.above_vwap ? "rs-pos" : "rs-neg";
        const matTag = (r.maturity_tag || "FRESH").toUpperCase();
        const days = r.consecutive_days_on_list || 1;
        const conv = r.conviction_score != null ? fmtNum(r.conviction_score, 0) : "—";
        return (
            `<tr class="${rowClass(matTag)}" tabindex="0" role="button"` +
            ` data-symbol="${escapeAttr(r.symbol)}"` +
            ` data-instrument-key="${escapeAttr(r.instrument_key)}"` +
            ` data-label="${escapeAttr(r.future_symbol || r.symbol)}"` +
            ` title="Open chart for ${escapeAttr(r.symbol)}">` +
            `<td class="rs-rank">${escapeHtml(r.rank)}</td>` +
            `<td class="rs-sym">${escapeHtml(r.symbol)} ${convictionChips(r)}</td>` +
            `<td class="num rs-conv">${conv}</td>` +
            `<td class="num ${rsCls}">${fmtSignedPct(r.rs_percent)}</td>` +
            `<td class="num"><span class="rs-score ${tradeScoreClass(r.trade_score)}">${escapeHtml(r.trade_score)}</span></td>` +
            `<td class="rs-setup-col">${setupBadge(r)}</td>` +
            (rsCfg.show_ema10_passive !== false
                ? `<td class="num rs-ema10">${r.ema10_10m != null ? fmtNum(r.ema10_10m, 2) : "—"}</td>`
                : "") +
            `<td class="num">${fmtNum(r.volume_ratio, 2)}</td>` +
            `<td class="num ${vwapCls}">${fmtNum(r.vwap, 2)}</td>` +
            `<td>${dirBadge(r.supertrend_bullish, "Bull", "Bear")}</td>` +
            `<td>${dirBadge(r.macd_bullish, "Bull", "Bear")}</td>` +
            `<td class="num">${fmtNum(r.adx, 1)}</td>` +
            `<td>${kavachBadge(r.kavach_state)}</td>` +
            `<td class="rs-maturity-col">${maturityBadge(matTag, days)}</td>` +
            `</tr>`
        );
    }

    function benchRowHtml(r) {
        return (
            `<tr class="rs-scanner-row rs-scanner-row--bench">` +
            `<td class="rs-sym">${escapeHtml(r.symbol)}</td>` +
            `<td class="num">${fmtNum(r.conviction_score, 0)}</td>` +
            `<td class="num">${fmtNum(r.persistence_credit, 0)}</td>` +
            `<td>${setupBadge(r)}</td>` +
            `</tr>`
        );
    }

    function maturityHeaderHtml() {
        return (
            'Maturity <span class="rs-maturity-info" title="' + escapeAttr(MATURITY_TOOLTIP) + '" aria-label="Maturity legend">ⓘ</span>'
        );
    }

    function renderTable(host, rows) {
        if (!host) return;
        const sorted = sortRows(rows);
        if (!sorted || !sorted.length) {
            host.innerHTML = '<p class="rs-scanner-empty">No data yet — scanner runs every 5 min during market hours.</p>';
            return;
        }
        const columns = ["Rank", "Symbol", "Conv", "RS %", "Score", "Setup"];
        if (rsCfg.show_ema10_passive !== false) columns.push("EMA10");
        columns.push("Vol×", "VWAP", "ST", "MACD", "ADX", "Kavach", "Maturity");
        host.innerHTML =
            '<table class="rs-scanner-table"><thead><tr>' +
            columns.map(function (c) {
                if (c === "Maturity") {
                    return '<th class="rs-maturity-col">' + maturityHeaderHtml() + "</th>";
                }
                return "<th>" + c + "</th>";
            }).join("") +
            "</tr></thead><tbody>" +
            sorted.map(rowHtml).join("") +
            "</tbody></table>";
    }

    function setUpdated(iso) {
        const el = document.getElementById("rsScannerUpdated");
        if (!el) return;
        if (!iso) {
            el.textContent = "Updated: —";
            return;
        }
        try {
            const d = new Date(iso);
            el.textContent =
                "Updated: " +
                d.toLocaleString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
        } catch (_) {
            el.textContent = "Updated: —";
        }
    }

    function tickCountdown() {
        const el = document.getElementById("rsScannerCountdown");
        if (!el) return;
        const ms = Math.max(0, nextRefreshAt - Date.now());
        const m = Math.floor(ms / 60000);
        const s = Math.floor((ms % 60000) / 1000);
        el.textContent = "Next: " + m + ":" + String(s).padStart(2, "0");
    }

    function ensureChartEngine() {
        if (chartEnginePromise) return chartEnginePromise;
        chartEnginePromise = new Promise((resolve, reject) => {
            if (window.SecurityChartEngine) return resolve(window.SecurityChartEngine);
            const s = document.createElement("script");
            s.src = "security-chart/security-chart-engine.js";
            s.onload = () => resolve(window.SecurityChartEngine);
            s.onerror = () => reject(new Error("chart engine load failed"));
            document.body.appendChild(s);
        });
        return chartEnginePromise;
    }

    function openChart(row) {
        const symbol = row.getAttribute("data-symbol") || "";
        const instrumentKey = row.getAttribute("data-instrument-key") || "";
        const label = row.getAttribute("data-label") || symbol;
        ensureChartEngine()
            .then((eng) => {
                if (!eng || typeof eng.openSecurityChart !== "function") return;
                eng.openSecurityChart({
                    symbol: symbol,
                    instrumentType: "FUT",
                    instrumentKey: instrumentKey,
                    displaySymbol: label,
                    exchange: "NSE",
                    timeframe: "15m",
                    metadata: { algo: "relative_strength" },
                });
            })
            .catch((e) => window.console && console.warn("RS scanner chart:", e));
    }

    function bindRowClicks(host) {
        if (!host || host._rsBound) return;
        host._rsBound = true;
        host.addEventListener("click", (e) => {
            const row = e.target.closest(".rs-scanner-row");
            if (row) openChart(row);
        });
        host.addEventListener("keydown", (e) => {
            if (e.key !== "Enter" && e.key !== " ") return;
            const row = e.target.closest(".rs-scanner-row");
            if (row) {
                e.preventDefault();
                openChart(row);
            }
        });
    }

    function renderBenchTable(host, rows) {
        if (!host) return;
        if (!rows || !rows.length) {
            host.innerHTML = "";
            return;
        }
        host.innerHTML =
            '<table class="rs-scanner-table rs-scanner-table--bench"><thead><tr>' +
            "<th>Symbol</th><th>Conv</th><th>Persist</th><th>Setup</th></tr></thead><tbody>" +
            rows.map(benchRowHtml).join("") +
            "</tbody></table>";
    }

    function renderLiveSetups(setups) {
        const empty = document.getElementById("rsLiveSetupsEmpty");
        const chips = document.getElementById("rsLiveSetupsChips");
        if (!chips) return;
        if (!setups || !setups.length) {
            if (empty) empty.hidden = false;
            chips.innerHTML = "";
            return;
        }
        if (empty) empty.hidden = true;
        chips.innerHTML = setups.map(function (s) {
            return (
                '<span class="rs-setup-chip rs-setup-chip--' + escapeAttr((s.state || "").toLowerCase()) + '">' +
                escapeHtml(s.symbol) + " · " + escapeHtml(s.side) + " · " + escapeHtml(s.state) +
                (s.sl_pct != null ? " · SL " + fmtNum(s.sl_pct, 2) + "%" : "") +
                "</span>"
            );
        }).join("");
    }

    function renderPromoEvents(events) {
        const el = document.getElementById("rsPromoEvents");
        if (!el) return;
        if (!events || !events.length) {
            el.hidden = true;
            el.innerHTML = "";
            return;
        }
        el.hidden = false;
        el.innerHTML = events.map(function (e) {
            const t = e.time ? new Date(e.time).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" }) : "—";
            if (e.type === "promote") {
                return '<div class="rs-promo-banner">' + t + " — " + escapeHtml(e.symbol) +
                    " promoted to " + escapeHtml(e.side) + " Core (replaced " + escapeHtml(e.replaced || "—") + ")</div>";
            }
            return '<div class="rs-promo-banner rs-promo-banner--eject">' + t + " — " + escapeHtml(e.symbol) + " ejected</div>";
        }).join("");
    }

    function renderPending(pending, side, hostId) {
        const el = document.getElementById(hostId);
        if (!el || !pending) return;
        el.textContent = pending.challenger + " challenging " + pending.displaced +
            " — promotes next scan if sustained (" + pending.cycles_won + "/" + pending.cycles_required + ")";
    }

    function normalizeData(data) {
        if (!data) return { bullish: [], bearish: [] };
        if (data.bullish_core || data.bearish_core) {
            return {
                bullish: data.bullish_core || data.bullish || [],
                bearish: data.bearish_core || data.bearish || [],
                bullish_bench: data.bullish_bench || [],
                bearish_bench: data.bearish_bench || [],
                live_setups: data.live_setups || [],
                fast_watch: data.fast_watch || [],
                promotion_events: data.promotion_events || [],
                bullish_pending: data.bullish_pending,
                bearish_pending: data.bearish_pending,
                last_updated: data.last_updated || data.last_board_cycle,
            };
        }
        return data;
    }

    function fwMomentumHtml(m) {
        if (m === "rising") return '<span class="rs-fw-momentum rs-fw-momentum--rising">↑ rising</span>';
        if (m === "fading") return '<span class="rs-fw-momentum rs-fw-momentum--fading">↓ fading</span>';
        return '<span class="rs-fw-momentum rs-fw-momentum--flat">→ flat</span>';
    }

    function normalizeFastWatch(fw) {
        if (!fw) return { featured: { long: [], short: [] }, all: [], total_count: 0 };
        if (Array.isArray(fw)) {
            const longs = fw.filter((x) => (x.direction || "LONG") !== "SHORT");
            const shorts = fw.filter((x) => (x.direction || "LONG") === "SHORT");
            return { featured: { long: longs, short: shorts }, all: fw, total_count: fw.length };
        }
        return {
            featured: fw.featured || { long: [], short: [] },
            all: fw.all || [],
            total_count: fw.total_count != null ? fw.total_count : (fw.all || []).length,
        };
    }

    function fmtFwElapsed(fw) {
        const t = fw.first_flip_at ? new Date(fw.first_flip_at).toLocaleTimeString(undefined, {
            hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Asia/Kolkata",
        }) : "—";
        const mins = fw.minutes_since_flip != null ? fw.minutes_since_flip : 0;
        return (fw.direction || "LONG") + " · first flip " + t + " · " + mins + " min ago";
    }

    function fastWatchCardHtml(fw) {
        const side = fw.direction === "SHORT" ? "short" : "long";
        const grade = fw.confidence_grade || fw.live_grade ? " · " + escapeHtml(fw.confidence_grade || fw.live_grade) : "";
        const score = fw.trade_score != null ? " · Score " + escapeHtml(fw.trade_score) : "";
        const kav = escapeHtml(fw.kavach_state || fw.live_kavach || "?");
        return '<div class="rs-fast-watch-card rs-fast-watch-card--' + side + '">' +
            "<strong>" + escapeHtml(fw.symbol) + "</strong>" + fwMomentumHtml(fw.momentum) +
            " · " + kav + grade + score +
            ' <span class="rs-fw-meta">· ' + escapeHtml(fmtFwElapsed(fw)) + "</span></div>";
    }

    let fastWatchExpanded = false;

    function fillFastWatchStack(stackEl, items) {
        if (!stackEl) return;
        stackEl.innerHTML = (items || []).map(fastWatchCardHtml).join("");
    }

    function renderFastWatch(fwPayload) {
        const wrap = document.getElementById("rsFastWatch");
        const bullStack = document.getElementById("rsFastWatchBull");
        const bearStack = document.getElementById("rsFastWatchBear");
        const expandBtn = document.getElementById("rsFastWatchExpand");
        const allWrap = document.getElementById("rsFastWatchAll");
        const allBull = document.getElementById("rsFastWatchAllBull");
        const allBear = document.getElementById("rsFastWatchAllBear");
        if (!wrap || !bullStack || !bearStack) return;
        const fw = normalizeFastWatch(fwPayload);
        const longs = fw.featured.long || [];
        const shorts = fw.featured.short || [];
        const featured = longs.concat(shorts);
        if (!fw.total_count) {
            wrap.hidden = true;
            fillFastWatchStack(bullStack, []);
            fillFastWatchStack(bearStack, []);
            if (expandBtn) expandBtn.hidden = true;
            if (allWrap) allWrap.hidden = true;
            fillFastWatchStack(allBull, []);
            fillFastWatchStack(allBear, []);
            return;
        }
        wrap.hidden = false;
        fillFastWatchStack(bullStack, longs);
        fillFastWatchStack(bearStack, shorts);
        if (expandBtn) {
            const extra = fw.total_count - featured.length;
            if (extra > 0) {
                expandBtn.hidden = false;
                expandBtn.textContent = (fastWatchExpanded ? "Hide" : "Show") +
                    " all flips (" + fw.total_count + ")";
            } else {
                expandBtn.hidden = true;
                fastWatchExpanded = false;
            }
        }
        if (allWrap) {
            if (fastWatchExpanded && fw.all.length) {
                allWrap.hidden = false;
                const allLongs = fw.all.filter(function (x) { return (x.direction || "LONG") !== "SHORT"; });
                const allShorts = fw.all.filter(function (x) { return (x.direction || "LONG") === "SHORT"; });
                fillFastWatchStack(allBull, allLongs);
                fillFastWatchStack(allBear, allShorts);
            } else {
                allWrap.hidden = true;
                fillFastWatchStack(allBull, []);
                fillFastWatchStack(allBear, []);
            }
        }
    }

    function render(data) {
        const d = normalizeData(data);
        lastData = d;
        renderTable(document.getElementById("rsScannerBullish"), d.bullish);
        renderTable(document.getElementById("rsScannerBearish"), d.bearish);
        renderBenchTable(document.getElementById("rsScannerBullBench"), d.bullish_bench);
        renderBenchTable(document.getElementById("rsScannerBearBench"), d.bearish_bench);
        const bc = document.getElementById("rsBullCount");
        const brc = document.getElementById("rsBearCount");
        if (bc) bc.textContent = (d.bullish ? d.bullish.length : 0) + "/5";
        if (brc) brc.textContent = (d.bearish ? d.bearish.length : 0) + "/5";
        const bbw = document.getElementById("rsBullBenchWrap");
        const brw = document.getElementById("rsBearBenchWrap");
        if (bbw) bbw.hidden = !(d.bullish_bench && d.bullish_bench.length);
        if (brw) brw.hidden = !(d.bearish_bench && d.bearish_bench.length);
        const bbc = document.getElementById("rsBullBenchCount");
        const brbc = document.getElementById("rsBearBenchCount");
        if (bbc) bbc.textContent = String((d.bullish_bench || []).length);
        if (brbc) brbc.textContent = String((d.bearish_bench || []).length);
        renderLiveSetups(d.live_setups);
        renderFastWatch(d.fast_watch);
        checkTriggeredAlerts(d.live_setups);
        renderPromoEvents(d.promotion_events);
        renderPending(d.bullish_pending, "BULL", "rsPendingChallenges");
        setUpdated(d.last_updated);
        nextRefreshAt = Date.now() + POLL_MS;
        tickCountdown();
    }

    async function fetchScanner() {
        const host = document.getElementById("rsScannerBullish");
        if (!host) return;
        try {
            const res = await fetch(API, { cache: "no-store", credentials: "same-origin" });
            const raw = await res.text();
            const ct = (res.headers.get("content-type") || "").toLowerCase();
            const looksJson = ct.includes("application/json") || /^\s*[\[{]/.test(raw.slice(0, 40));
            if (!looksJson) {
                render({ bullish: [], bearish: [] });
                return;
            }
            render(JSON.parse(raw));
        } catch (e) {
            window.console && console.warn("dashboard-relative-strength:", e);
            render({ bullish: [], bearish: [] });
        }
    }

    async function fetchLiveSetups() {
        try {
            const res = await fetch(API + "/live-setups", { cache: "no-store", credentials: "same-origin" });
            const data = await res.json();
            renderLiveSetups(data.live_setups || []);
            checkTriggeredAlerts(data.live_setups || []);
        } catch (_) {}
    }

    function startPolling() {
        if (timer) clearInterval(timer);
        timer = setInterval(function () {
            fetchScanner();
            fetchLiveSetups();
        }, POLL_MS);
        if (countdownTimer) clearInterval(countdownTimer);
        countdownTimer = setInterval(tickCountdown, 1000);
    }

    function init() {
        if (!document.getElementById("rsScannerBullish")) return;
        bindRowClicks(document.getElementById("rsScannerBullish"));
        bindRowClicks(document.getElementById("rsScannerBearish"));
        const sortSel = document.getElementById("rsScannerSort");
        if (sortSel) {
            sortSel.addEventListener("change", function () {
                sortMode = sortSel.value === "maturity" ? "maturity" : "score";
                if (lastData) render(lastData);
            });
        }
        loadRsCfg().then(function () {
            fetchScanner();
            startPolling();
        });
        const btn = document.getElementById("rsScannerRefresh");
        if (btn) {
            btn.addEventListener("click", () => {
                btn.disabled = true;
                fetchScanner().finally(() => {
                    btn.disabled = false;
                });
            });
        }
        const fwExpand = document.getElementById("rsFastWatchExpand");
        if (fwExpand) {
            fwExpand.addEventListener("click", () => {
                fastWatchExpanded = !fastWatchExpanded;
                if (lastData) render(lastData);
            });
        }
        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === "visible") fetchScanner();
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
