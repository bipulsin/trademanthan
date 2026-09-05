/**
 * Dashboard: OI Buildup Heatmap — four signal tabs (arbitrage_master currmth FUT).
 */
(function () {
    const API_PATH = "/scan/dashboard/oi-heatmap";
    const FETCH_MS = 35000;
    const POLL_MS = 60 * 1000;
    const LIVE_FRESH_WINDOW_MS = 30 * 60 * 1000;
    const TAB_KEYS = ["LONG_BUILDUP", "SHORT_COVERING", "SHORT_BUILDUP", "LONG_UNWINDING"];
    /** Main screen: Top-10 per tab. Rank: global rank (|oi_chg| desc), then score desc, then |oi_chg|, |oi_chg_pct|, |chg_pct|. Modal keeps the full snapshot. */
    const TAB_TOP_N = 10;
    let timer = null;
    let firstLoad = true;
    let fullRowsCache = [];
    let bySignalCache = {};
    let activeTab = "LONG_BUILDUP";
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

    function bucketKey(sig) {
        const s = String(sig || "").toUpperCase();
        if (s === "LONG_UNWIND" || s === "LONG_UNWINDING") return "LONG_UNWINDING";
        if (s === "SHORT_COVER" || s === "SHORT_COVERING") return "SHORT_COVERING";
        if (s === "LONG_BUILDUP" || s === "SHORT_BUILDUP") return s;
        return "";
    }

    function rowsForTab(tab) {
        const key = String(tab || activeTab);
        const fromApi = bySignalCache && bySignalCache[key];
        if (fromApi && fromApi.length) return fromApi;
        return (fullRowsCache || []).filter(function (r) {
            return bucketKey(r && r.oi_signal) === key;
        });
    }

    function sortRowsForTab(rows) {
        return (rows || []).slice().sort(function (a, b) {
            var ra = a && a.rank != null && a.rank !== "" ? Number(a.rank) : 1e9;
            var rb = b && b.rank != null && b.rank !== "" ? Number(b.rank) : 1e9;
            if (ra !== rb) return ra - rb;
            var sa = Number(a && a.score) || 0;
            var sb = Number(b && b.score) || 0;
            if (sa !== sb) return sb - sa;
            var oa = Math.abs(Number(a && a.oi_chg) || 0);
            var ob = Math.abs(Number(b && b.oi_chg) || 0);
            if (oa !== ob) return ob - oa;
            var pa = Math.abs(Number(a && a.oi_chg_pct) || 0);
            var pb = Math.abs(Number(b && b.oi_chg_pct) || 0);
            if (pa !== pb) return pb - pa;
            var ca = Math.abs(Number(a && a.chg_pct) || 0);
            var cb = Math.abs(Number(b && b.chg_pct) || 0);
            return cb - ca;
        });
    }

    function mainTabRows(tab) {
        return sortRowsForTab(rowsForTab(tab)).slice(0, TAB_TOP_N);
    }

    function updateTabCounts() {
        TAB_KEYS.forEach(function (k) {
            const btn = document.querySelector('.oi-heatmap-tab[data-oi-tab="' + k + '"]');
            if (!btn) return;
            const n = rowsForTab(k).length;
            const labels = {
                LONG_BUILDUP: "Long Buildup",
                SHORT_COVERING: "Short Covering",
                SHORT_BUILDUP: "Short Buildup",
                LONG_UNWINDING: "Long Unwinding",
            };
            btn.textContent = labels[k] + " (" + n + ")";
            btn.classList.toggle("is-active", k === activeTab);
            btn.setAttribute("aria-selected", k === activeTab ? "true" : "false");
        });
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
            return '<p class="oi-heatmap-empty">No names in this OI bucket for the latest snapshot.</p>';
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
                const sym = r.trading_symbol || r.underlying_symbol || "";
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
            bySignalCache = data.by_signal || {};
            const inner = renderTable(mainTabRows(activeTab));
            host.innerHTML = inner;
            updateOiHeatmapHeader(data);
            updateTabCounts();
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
        document.querySelectorAll(".oi-heatmap-tab").forEach(function (tabBtn) {
            tabBtn.addEventListener("click", function () {
                activeTab = String(tabBtn.getAttribute("data-oi-tab") || "LONG_BUILDUP");
                const host = document.getElementById("oiHeatmapHost");
                if (host) host.innerHTML = renderTable(mainTabRows(activeTab));
                updateTabCounts();
            });
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
