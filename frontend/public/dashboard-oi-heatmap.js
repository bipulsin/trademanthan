/**
 * Dashboard: Live OI heatmap — API returns ~200 NSE stock futures; UI shows top N only.
 */
(function () {
    const API_PATH = "/scan/dashboard/oi-heatmap";
    const FETCH_MS = 35000;
    const POLL_MS = 60 * 1000;
    /** Rows returned by API are sorted; only this many are rendered in the table. */
    const DISPLAY_TOP_N = 10;
    let timer = null;
    let firstLoad = true;

    function apiUrl() {
        const base = window.location.origin || "";
        return base + API_PATH;
    }

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function fmtNum(n, d) {
        if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
        return Number(n).toFixed(d);
    }

    function fmtInt(n) {
        if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
        return String(Math.round(Number(n)));
    }

    function fmtTime(iso) {
        if (!iso) return "—";
        try {
            const x = new Date(iso);
            if (Number.isNaN(x.getTime())) return escapeHtml(String(iso));
            return x.toLocaleString("en-IN", {
                timeZone: "Asia/Kolkata",
                dateStyle: "short",
                timeStyle: "medium",
            });
        } catch (e) {
            return "—";
        }
    }

    function signalLabel(sig) {
        const s = String(sig || "").toUpperCase();
        const map = {
            LONG_BUILDUP: "Long buildup",
            SHORT_BUILDUP: "Short buildup",
            LONG_UNWIND: "Long unwind",
            LONG_UNWINDING: "Long unwind",
            SHORT_COVER: "Short cover",
            SHORT_COVERING: "Short cover",
            NEUTRAL: "Neutral",
        };
        return map[s] || sig || "—";
    }

    function heatStyle(sig) {
        const s = String(sig || "").toUpperCase();
        let r = 156;
        let g = 163;
        let b = 175;
        if (s === "LONG_BUILDUP") {
            r = 34;
            g = 197;
            b = 94;
        } else if (s === "SHORT_BUILDUP") {
            r = 239;
            g = 68;
            b = 68;
        } else if (s === "LONG_UNWIND" || s === "LONG_UNWINDING") {
            r = 251;
            g = 146;
            b = 60;
        } else if (s === "SHORT_COVER" || s === "SHORT_COVERING") {
            r = 74;
            g = 222;
            b = 128;
        }
        return "background: rgba(" + r + "," + g + "," + b + ",0.12);";
    }

    function renderTable(rows, totalInUniverse) {
        if (!rows || rows.length === 0) {
            return '<p class="oi-heatmap-empty">No heatmap data yet (scheduler or instruments file).</p>';
        }
        const head =
            "<thead><tr>" +
            "<th>#</th><th>Symbol</th><th>LTP</th><th>Chg%</th><th>OI</th><th>OI Chg</th>" +
            "<th>OI Signal</th><th>Volume</th><th>Score</th>" +
            "</tr></thead>";
        const body = rows
            .map(function (r) {
                const sig = r.oi_signal || "";
                const hs = heatStyle(sig);
                const sigClass = /^[A-Z_]+$/.test(sig) ? sig.replace(/[^A-Z_]/g, "_") : "NEUTRAL";
                const sym = r.underlying_symbol || r.trading_symbol || "";
                return (
                    "<tr>" +
                    "<td>" +
                    escapeHtml(String(r.rank != null ? r.rank : "")) +
                    "</td>" +
                    "<td><strong>" +
                    escapeHtml(String(sym)) +
                    "</strong></td>" +
                    "<td>" +
                    fmtNum(r.ltp, 2) +
                    "</td>" +
                    "<td>" +
                    fmtNum(r.chg_pct, 2) +
                    "</td>" +
                    "<td>" +
                    fmtInt(r.oi) +
                    "</td>" +
                    "<td>" +
                    fmtInt(r.oi_chg) +
                    "</td>" +
                    '<td style="' +
                    hs +
                    '"><span class="oi-heatmap-signal oi-heatmap-signal--' +
                    sigClass +
                    '">' +
                    escapeHtml(signalLabel(sig)) +
                    "</span></td>" +
                    "<td>" +
                    fmtInt(r.volume) +
                    "</td>" +
                    "<td>" +
                    fmtNum(r.score, 4) +
                    "</td>" +
                    "</tr>"
                );
            })
            .join("");
        return (
            '<div class="oi-heatmap-table-wrap"><table class="oi-heatmap-table">' +
            head +
            "<tbody>" +
            body +
            "</tbody></table></div>" +
            '<p class="oi-heatmap-legend">' +
            (totalInUniverse != null && totalInUniverse > rows.length
                ? "Displaying top " +
                  rows.length +
                  " of " +
                  totalInUniverse +
                  ". "
                : "") +
            "Sorted by |OI change| (Upstox NSE_FO stock futures, near-month per underlying). Colors: buildup / unwind vs price.</p>"
        );
    }

    async function load() {
        const host = document.getElementById("oiHeatmapHost");
        const msg = document.getElementById("oiHeatmapMsg");
        const updated = document.getElementById("oiHeatmapUpdated");
        if (!host) {
            if (msg) msg.textContent = "Error: heatmap container missing (reload the page).";
            return;
        }

        if (msg) {
            msg.textContent = "Loading…";
            msg.style.display = "block";
        }

        try {
            const qs = new URLSearchParams();
            qs.set("_", String(Date.now()));
            if (firstLoad) {
                qs.set("reload_db", "1");
                firstLoad = false;
            }
            const ctrl = new AbortController();
            const to = setTimeout(function () {
                ctrl.abort();
            }, FETCH_MS);
            let res;
            try {
                res = await fetch(apiUrl() + "?" + qs.toString(), {
                    cache: "no-store",
                    credentials: "same-origin",
                    signal: ctrl.signal,
                });
            } finally {
                clearTimeout(to);
            }
            let data;
            try {
                const text = await res.text();
                data = text ? JSON.parse(text) : {};
            } catch (parseErr) {
                throw new Error(
                    "Invalid response from server (not JSON). Status " + res.status + "."
                );
            }
            if (!res.ok || data.success === false) {
                throw new Error((data && data.message) || data.error || res.statusText || "Failed");
            }
            const allRows = data.rows || [];
            const displayRows = allRows.slice(0, DISPLAY_TOP_N);
            const inner = renderTable(displayRows, allRows.length);
            host.innerHTML = inner;
            if (msg) {
                const origin = data.data_origin || "";
                let originNote = "";
                if (allRows.length > 0) {
                    if (data.snapshot_note) {
                        originNote = " · " + data.snapshot_note;
                    } else if (origin === "snapshot") {
                        originNote =
                            " · Last saved snapshot (will update when live Upstox refresh runs)";
                    } else if (origin === "live") {
                        originNote = " · Live Upstox";
                    }
                }
                const err = data.error ? " · last error: " + data.error : "";
                var summary = "";
                if (allRows.length > 0) {
                    if (allRows.length > DISPLAY_TOP_N) {
                        summary =
                            "Top " +
                            DISPLAY_TOP_N +
                            " of " +
                            allRows.length +
                            " symbols";
                    } else {
                        summary = allRows.length + " symbol" + (allRows.length === 1 ? "" : "s");
                    }
                }
                msg.textContent =
                    allRows.length > 0
                        ? summary + originNote + err
                        : (data.message || "No rows.") + err;
                msg.style.display = "block";
            }
            if (updated && data.updated_at) {
                updated.textContent = "Updated " + fmtTime(data.updated_at) + " IST";
            } else if (updated) {
                updated.textContent = "—";
            }
        } catch (e) {
            var _abort =
                e &&
                (e.name === "AbortError" ||
                    String(e.message || "")
                        .toLowerCase()
                        .indexOf("abort") >= 0);
            var errMsg = _abort
                ? "Request timed out after " +
                  Math.round(FETCH_MS / 1000) +
                  "s. Click refresh or check your connection."
                : e.message || String(e);
            host.innerHTML =
                '<p class="oi-heatmap-error">' + escapeHtml(errMsg) + "</p>";
            if (msg) {
                msg.textContent = "";
                msg.style.display = "none";
            }
            if (updated) updated.textContent = "—";
        }
    }

    function startPoll() {
        if (timer) clearInterval(timer);
        timer = setInterval(load, POLL_MS);
    }

    document.addEventListener("DOMContentLoaded", function () {
        const btn = document.getElementById("oiHeatmapRefresh");
        if (btn)
            btn.addEventListener("click", function () {
                load();
            });
        load();
        startPoll();
    });
})();
