/**
 * Dashboard: Live OI heatmap — API returns ~200 NSE stock futures; UI shows top N only.
 */
(function () {
    const API_PATH = "/scan/dashboard/oi-heatmap";
    const FETCH_MS = 35000;
    const POLL_MS = 60 * 1000;
    /** Rows returned by API are sorted; only this many are rendered in the table. */
    const DISPLAY_TOP_N = 10;
    /** Show "(live)" only if scan is same IST calendar day as now and ≤30 minutes old. */
    const LIVE_FRESH_WINDOW_MS = 30 * 60 * 1000;
    let timer = null;
    let firstLoad = true;
    let fullRowsCache = [];
    let modalSortKey = "symbol";
    let modalSortDir = "asc";

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

    function sameCalendarDayIST(a, b) {
        var fmt = new Intl.DateTimeFormat("en-CA", {
            timeZone: "Asia/Kolkata",
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
        });
        return fmt.format(a) === fmt.format(b);
    }

    /** True when scan time is on today's IST date and not older than 30 minutes (and not in the future). */
    function isOiHeatmapConsideredLive(updatedIso) {
        if (!updatedIso) return false;
        var scan = new Date(updatedIso);
        if (Number.isNaN(scan.getTime())) return false;
        var now = new Date();
        if (scan > now) return false;
        if (!sameCalendarDayIST(scan, now)) return false;
        return now - scan <= LIVE_FRESH_WINDOW_MS;
    }

    function updateOiHeatmapHeader(data) {
        var modeEl = document.getElementById("oiHeatmapHeaderMode");
        var scanEl = document.getElementById("oiHeatmapHeaderScan");
        var sepEl = document.getElementById("oiHeatmapScanSep");
        if (!modeEl || !scanEl) return;
        var rows = (data && data.rows) || [];
        var iso = data && data.updated_at;
        if (!iso) {
            modeEl.textContent = "(snapshot)";
            modeEl.classList.add("oi-heatmap-mode--snapshot");
            scanEl.textContent = "";
            if (sepEl) sepEl.style.display = "none";
            return;
        }
        scanEl.textContent = fmtTime(iso) + " IST";
        if (sepEl) sepEl.style.display = "inline";
        var live = rows.length > 0 && isOiHeatmapConsideredLive(iso);
        modeEl.textContent = live ? "(live)" : "(snapshot)";
        if (live) {
            modeEl.classList.remove("oi-heatmap-mode--snapshot");
        } else {
            modeEl.classList.add("oi-heatmap-mode--snapshot");
        }
    }

    function resetOiHeatmapHeader() {
        var modeEl = document.getElementById("oiHeatmapHeaderMode");
        var scanEl = document.getElementById("oiHeatmapHeaderScan");
        var sepEl = document.getElementById("oiHeatmapScanSep");
        if (modeEl) {
            modeEl.textContent = "(snapshot)";
            modeEl.classList.add("oi-heatmap-mode--snapshot");
        }
        if (scanEl) scanEl.textContent = "—";
        if (sepEl) sepEl.style.display = "none";
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

    function signalTooltip(sig) {
        const s = String(sig || "").toUpperCase();
        if (s === "LONG_BUILDUP") return "FII/DII Buying";
        if (s === "SHORT_BUILDUP") return "FII / DII Selling";
        if (s === "SHORT_COVER" || s === "SHORT_COVERING")
            return "No new buying - Rally temporary";
        if (s === "LONG_UNWIND" || s === "LONG_UNWINDING")
            return "No new Selling. Decline temporary";
        if (s === "NEUTRAL") return "No clear direction";
        return "";
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

    function renderRowsTable(rows) {
        if (!rows || rows.length === 0) {
            return '<p class="oi-heatmap-empty">No heatmap data yet (scheduler or instruments file).</p>';
        }
        const head =
            "<thead><tr>" +
            "<th>#</th><th>Symbol</th><th>LTP</th><th>Chg%</th><th>OI</th><th>OI Chg</th>" +
            "<th>OI Signal</th><th>Prev OI Signal</th><th>Volume</th><th>Score</th>" +
            "</tr></thead>";
        const body = rows
            .map(function (r) {
                const sig = r.oi_signal || "";
                const sigTip = signalTooltip(sig);
                const hs = heatStyle(sig);
                const sigClass = /^[A-Z_]+$/.test(sig) ? sig.replace(/[^A-Z_]/g, "_") : "NEUTRAL";
                const prevSig = r.prev_oi_signal || "";
                const prevSigTip = signalTooltip(prevSig);
                const prevSigClass = /^[A-Z_]+$/.test(prevSig)
                    ? prevSig.replace(/[^A-Z_]/g, "_")
                    : "NEUTRAL";
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
                    '" title="' +
                    escapeHtml(sigTip) +
                    '">' +
                    escapeHtml(signalLabel(sig)) +
                    "</span></td>" +
                    "<td><span class=\"oi-heatmap-signal oi-heatmap-signal--" +
                    prevSigClass +
                    "\" title=\"" +
                    escapeHtml(prevSigTip) +
                    "\">" +
                    escapeHtml(signalLabel(prevSig || "—")) +
                    "</span></td>" +
                    "<td>" +
                    fmtInt(r.volume) +
                    "</td>" +
                    "<td>" +
                    fmtNum(r.score, 2) +
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
            "</tbody></table></div>"
        );
    }

    function renderTable(rows) {
        return renderRowsTable(rows);
    }

    function sortedModalRows() {
        var out = (fullRowsCache || []).slice();
        var dir = modalSortDir === "desc" ? -1 : 1;
        out.sort(function (a, b) {
            if (modalSortKey === "score") {
                var av = Number(a && a.score) || 0;
                var bv = Number(b && b.score) || 0;
                return (av - bv) * dir;
            }
            if (modalSortKey === "oi_signal") {
                var as = String(signalLabel(a && a.oi_signal) || "").toUpperCase();
                var bs = String(signalLabel(b && b.oi_signal) || "").toUpperCase();
                if (as < bs) return -1 * dir;
                if (as > bs) return 1 * dir;
                return 0;
            }
            var asym = String((a && (a.underlying_symbol || a.trading_symbol)) || "").toUpperCase();
            var bsym = String((b && (b.underlying_symbol || b.trading_symbol)) || "").toUpperCase();
            if (asym < bsym) return -1 * dir;
            if (asym > bsym) return 1 * dir;
            return 0;
        });
        return out;
    }

    function renderModalTable() {
        var body = document.getElementById("oiHeatmapModalBody");
        if (!body) return;
        body.innerHTML = renderRowsTable(sortedModalRows());
    }

    function updateSortDirButton() {
        var btn = document.getElementById("oiHeatmapSortDir");
        if (!btn) return;
        var isAsc = modalSortDir !== "desc";
        btn.textContent = isAsc ? "↑" : "↓";
        btn.title = isAsc ? "Ascending" : "Descending";
    }

    function openModal() {
        var modal = document.getElementById("oiHeatmapModal");
        if (!modal) return;
        renderModalTable();
        modal.hidden = false;
        document.body.style.overflow = "hidden";
    }

    function closeModal() {
        var modal = document.getElementById("oiHeatmapModal");
        if (!modal) return;
        modal.hidden = true;
        document.body.style.overflow = "";
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
            fullRowsCache = allRows.slice();
            const displayRows = allRows.slice(0, DISPLAY_TOP_N);
            const inner = renderTable(displayRows);
            host.innerHTML = inner;
            updateOiHeatmapHeader(data);
            if (msg) {
                const err = data.error ? String(data.error) : "";
                if (allRows.length > 0) {
                    if (err) {
                        msg.textContent = "Error: " + err;
                        msg.style.display = "block";
                    } else {
                        msg.textContent = "";
                        msg.style.display = "none";
                    }
                } else {
                    msg.textContent = (data.message || "No rows.") + (err ? " — " + err : "");
                    msg.style.display = "block";
                }
            }
            if (updated) {
                updated.textContent = "";
                updated.style.display = "none";
            }
            var moreBtn = document.getElementById("oiHeatmapMoreBtn");
            if (moreBtn) {
                moreBtn.style.display = allRows.length > 0 ? "inline-block" : "none";
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
            resetOiHeatmapHeader();
            var upd = document.getElementById("oiHeatmapUpdated");
            if (upd) {
                upd.textContent = "";
                upd.style.display = "none";
            }
            fullRowsCache = [];
            var moreBtn2 = document.getElementById("oiHeatmapMoreBtn");
            if (moreBtn2) moreBtn2.style.display = "none";
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
        const moreBtn = document.getElementById("oiHeatmapMoreBtn");
        if (moreBtn)
            moreBtn.addEventListener("click", function () {
                openModal();
            });
        const closeBtn = document.getElementById("oiHeatmapModalClose");
        if (closeBtn)
            closeBtn.addEventListener("click", function () {
                closeModal();
            });
        const modal = document.getElementById("oiHeatmapModal");
        if (modal)
            modal.addEventListener("click", function (e) {
                if (e.target === modal) closeModal();
            });
        const sortKey = document.getElementById("oiHeatmapSortKey");
        if (sortKey)
            sortKey.addEventListener("change", function () {
                modalSortKey = String(sortKey.value || "symbol");
                renderModalTable();
            });
        const sortDirBtn = document.getElementById("oiHeatmapSortDir");
        if (sortDirBtn)
            sortDirBtn.addEventListener("click", function () {
                modalSortDir = modalSortDir === "asc" ? "desc" : "asc";
                updateSortDirButton();
                renderModalTable();
            });
        document.addEventListener("keydown", function (e) {
            if (e.key === "Escape") closeModal();
        });
        updateSortDirButton();
        load();
        startPoll();
    });
})();
