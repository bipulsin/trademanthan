/**
 * Dashboard: OI buildup heatmap for top 10 F&O underlyings (server-cached NSE data).
 */
(function () {
    const API = "/scan/dashboard-oi-heatmap";
    const POLL_MS = 180 * 1000;
    let timer = null;

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
            LONG_UNWINDING: "Long unwinding",
            SHORT_COVERING: "Short covering",
            NEUTRAL: "Neutral",
            ERROR: "—",
        };
        return map[s] || sig || "—";
    }

    function heatStyle(heat01, signal) {
        const h = Math.max(0, Math.min(1, Number(heat01) || 0));
        const sig = String(signal || "").toUpperCase();
        let r = 100;
        let g = 116;
        let b = 139;
        if (sig === "LONG_BUILDUP") {
            r = 34;
            g = 197;
            b = 94;
        } else if (sig === "SHORT_BUILDUP") {
            r = 239;
            g = 68;
            b = 68;
        } else if (sig === "LONG_UNWINDING") {
            r = 251;
            g = 146;
            b = 60;
        } else if (sig === "SHORT_COVERING") {
            r = 59;
            g = 130;
            b = 246;
        }
        const a = 0.08 + h * 0.42;
        return "background: rgba(" + r + "," + g + "," + b + "," + a.toFixed(3) + ");";
    }

    function renderTable(rows) {
        if (!rows || rows.length === 0) {
            return '<p class="oi-heatmap-empty">No symbols available for this view.</p>';
        }
        const head =
            "<thead><tr>" +
            "<th>#</th><th>Symbol</th><th>Price Δ%</th><th>OI</th><th>Δ OI</th><th>Δ OI %</th>" +
            "<th>OI vs price</th>" +
            "</tr></thead>";
        const body = rows
            .map(function (r) {
                const sig = r.signal || "";
                const hs = heatStyle(r.heat01, sig);
                const err = r.error ? ' title="' + escapeHtml(r.error) + '"' : "";
                const sigClass = /^[A-Z_]+$/.test(sig) ? sig : "NEUTRAL";
                return (
                    "<tr>" +
                    "<td>" +
                    escapeHtml(String(r.rank != null ? r.rank : "")) +
                    "</td>" +
                    "<td><strong>" +
                    escapeHtml(String(r.symbol || "")) +
                    "</strong></td>" +
                    "<td>" +
                    fmtNum(r.price_change_pct, 2) +
                    "</td>" +
                    "<td>" +
                    fmtInt(r.oi) +
                    "</td>" +
                    "<td>" +
                    fmtInt(r.change_in_oi) +
                    "</td>" +
                    '<td class="oi-heatmap-oi-pct"' +
                    err +
                    ' style="' +
                    hs +
                    '">' +
                    fmtNum(r.oi_change_pct, 2) +
                    "</td>" +
                    '<td><span class="oi-heatmap-signal oi-heatmap-signal--' +
                    sigClass +
                    '">' +
                    escapeHtml(signalLabel(sig)) +
                    "</span></td>" +
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
            '<p class="oi-heatmap-legend">OI vs price uses intraday price change and change in open interest (near-month futures, NSE). Darker Δ OI % shading = larger move vs prior OI.</p>'
        );
    }

    async function load() {
        const host = document.getElementById("oiHeatmapHost");
        const msg = document.getElementById("oiHeatmapMsg");
        const updated = document.getElementById("oiHeatmapUpdated");
        if (!host) return;

        if (msg) {
            msg.textContent = "Loading…";
            msg.style.display = "block";
        }

        try {
            const res = await fetch(API, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error((data && data.message) || res.statusText || "Failed");
            }
            const rows = data.rows || [];
            let src = data.symbols_source || "—";
            if (src.startsWith("premarket_session_")) {
                src = "Saved session " + src.replace("premarket_session_", "");
            } else if (src === "premarket_today") {
                src = "Today’s pre-market rank";
            } else if (src === "premarket_today_partial") {
                src = "Today’s pre-market rank (partial)";
            }
            const inner = renderTable(rows);
            host.innerHTML = inner;
            if (msg) {
                const cacheNote = data.cached ? " · cached " + (data.cache_age_sec != null ? data.cache_age_sec + "s" : "") : "";
                msg.textContent =
                    rows.length > 0
                        ? "Universe: " + src + " · " + rows.length + " names" + cacheNote
                        : (data.message || "No rows.") + cacheNote;
                msg.style.display = "block";
            }
            if (updated && data.updated_at) {
                updated.textContent = "Updated " + fmtTime(data.updated_at) + " IST";
            } else if (updated) {
                updated.textContent = "—";
            }
        } catch (e) {
            host.innerHTML =
                '<p class="oi-heatmap-error">' + escapeHtml(e.message || String(e)) + "</p>";
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
