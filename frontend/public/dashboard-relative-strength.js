/**
 * Dashboard: Relative Strength Scanner.
 * Reads the latest scan snapshot from /api/dashboard/relative-strength and renders
 * Top 5 Bullish / Bearish current-month futures (RS vs NIFTY + Kavach + Trade Score).
 * The dashboard never recalculates — the 5-min backend scheduler owns all maths.
 */
(function () {
    const API = "/api/dashboard/relative-strength";
    const POLL_MS = 5 * 60 * 1000;
    const COLUMNS = ["Rank", "Symbol", "Price", "RS %", "Score", "Vol×", "VWAP", "ST", "MACD", "ADX", "Kavach"];
    let timer = null;
    let countdownTimer = null;
    let nextRefreshAt = 0;
    let chartEnginePromise = null;

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function escapeAttr(s) {
        return escapeHtml(s).replace(/"/g, "&quot;");
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
        const cls = isBull ? "rs-badge--up" : "rs-badge--down";
        return `<span class="rs-badge ${cls}">${isBull ? bullLabel : bearLabel}</span>`;
    }

    function kavachBadge(state) {
        const up = /^(BUY|READY|WATCH)$/.test(state || "");
        const cls = up ? "rs-badge--up" : "rs-badge--down";
        return `<span class="rs-badge ${cls}">${escapeHtml(state || "—")}</span>`;
    }

    function rowHtml(r) {
        const rsCls = Number(r.rs_percent) >= 0 ? "rs-pos" : "rs-neg";
        const vwapCls = r.above_vwap ? "rs-pos" : "rs-neg";
        return (
            `<tr class="rs-scanner-row" tabindex="0" role="button"` +
            ` data-symbol="${escapeAttr(r.symbol)}"` +
            ` data-instrument-key="${escapeAttr(r.instrument_key)}"` +
            ` data-label="${escapeAttr(r.future_symbol || r.symbol)}"` +
            ` title="Open chart for ${escapeAttr(r.symbol)}">` +
            `<td class="rs-rank">${escapeHtml(r.rank)}</td>` +
            `<td class="rs-sym">${escapeHtml(r.symbol)}</td>` +
            `<td class="num">${fmtNum(r.price, 2)}</td>` +
            `<td class="num ${rsCls}">${fmtSignedPct(r.rs_percent)}</td>` +
            `<td class="num"><span class="rs-score ${tradeScoreClass(r.trade_score)}">${escapeHtml(r.trade_score)}</span></td>` +
            `<td class="num">${fmtNum(r.volume_ratio, 2)}</td>` +
            `<td class="num ${vwapCls}">${fmtNum(r.vwap, 2)}</td>` +
            `<td>${dirBadge(r.supertrend_bullish, "Bull", "Bear")}</td>` +
            `<td>${dirBadge(r.macd_bullish, "Bull", "Bear")}</td>` +
            `<td class="num">${fmtNum(r.adx, 1)}</td>` +
            `<td>${kavachBadge(r.kavach_state)}</td>` +
            `</tr>`
        );
    }

    function renderTable(host, rows) {
        if (!host) return;
        if (!rows || !rows.length) {
            host.innerHTML = '<p class="rs-scanner-empty">No data yet — scanner runs every 5 min during market hours.</p>';
            return;
        }
        host.innerHTML =
            '<table class="rs-scanner-table"><thead><tr>' +
            COLUMNS.map((c) => `<th>${c}</th>`).join("") +
            "</tr></thead><tbody>" +
            rows.map(rowHtml).join("") +
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

    function render(data) {
        renderTable(document.getElementById("rsScannerBullish"), data && data.bullish);
        renderTable(document.getElementById("rsScannerBearish"), data && data.bearish);
        setUpdated(data && data.last_updated);
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

    function startPolling() {
        if (timer) clearInterval(timer);
        timer = setInterval(fetchScanner, POLL_MS);
        if (countdownTimer) clearInterval(countdownTimer);
        countdownTimer = setInterval(tickCountdown, 1000);
    }

    function init() {
        if (!document.getElementById("rsScannerBullish")) return;
        bindRowClicks(document.getElementById("rsScannerBullish"));
        bindRowClicks(document.getElementById("rsScannerBearish"));
        fetchScanner();
        startPolling();
        const btn = document.getElementById("rsScannerRefresh");
        if (btn) {
            btn.addEventListener("click", () => {
                btn.disabled = true;
                fetchScanner().finally(() => {
                    btn.disabled = false;
                });
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
