(function () {
    const API = "/api/breakfast-strategy/data";
    const HIST_API = "/api/breakfast-strategy/history";
    const HIST_TRADES_API = "/api/breakfast-strategy/history";
    const LIVE_API = "/api/breakfast-strategy/live";
    const LIVE_SIGNALS_API = "/api/breakfast-strategy/live/signals";
    const PREVCLOSE_API = "/api/breakfast-strategy/prevclose-backtest";
    const TRAP_CE_API = "/api/breakfast-strategy/trap-ce";
    const TRAP_CE_LIVE_API = "/api/breakfast-strategy/trap-ce-live";
    var livePollTimer = null;
    var liveSignalsCache = {};
    var liveSessionDate = "";
    var liveLastData = null;

    function fetchWithTimeout(url, opts, ms) {
        var ctrl = new AbortController();
        var timer = setTimeout(function () { ctrl.abort(); }, ms || 25000);
        return fetch(url, Object.assign({}, opts || {}, { signal: ctrl.signal }))
            .finally(function () { clearTimeout(timer); });
    }

    function $(id) { return document.getElementById(id); }

    function fmt(n, d) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        return Number(n).toLocaleString(undefined, { minimumFractionDigits: d || 0, maximumFractionDigits: d || 2 });
    }

    function pnlClass(v) {
        var x = Number(v);
        if (Number.isNaN(x)) return "";
        return x >= 0 ? "bf-pnl-pos" : "bf-pnl-neg";
    }

    function setStatus(msg, err, elId) {
        var el = $(elId || "bfStatus");
        if (!el) return;
        if (!msg) { el.hidden = true; return; }
        el.hidden = false;
        el.textContent = msg;
        el.style.borderColor = err ? "#f87171" : "#34d399";
        el.style.color = err ? "#fecaca" : "#bbf7d0";
    }

    async function parseJsonResponse(res) {
        var text = await res.text();
        try { return JSON.parse(text); }
        catch (_e) {
            if (text && text.trim().charAt(0) === "<") {
                throw new Error("Server timed out or returned an error page.");
            }
            throw new Error("Invalid server response");
        }
    }

    function renderSummary(s, boxId) {
        var box = $(boxId || "bfSummary");
        if (!box || !s) return;
        var cards = [
            ["Trades", s.total_trades],
            ["Win rate", (s.win_rate_pct != null ? s.win_rate_pct + "%" : "—")],
            ["Total P&L", "₹" + fmt(s.total_pnl_inr, 0)],
            ["Avg P&L", "₹" + fmt(s.avg_pnl_inr, 0)],
            ["Avg R", fmt(s.avg_r, 2)],
            ["Long", (s.long_trades || 0) + " · ₹" + fmt(s.long_pnl_inr, 0)],
            ["Short", (s.short_trades || 0) + " · ₹" + fmt(s.short_pnl_inr, 0)],
        ];
        box.innerHTML = cards.map(function (c) {
            return '<div class="vmb-stat"><span class="vmb-stat-label">' + c[0] +
                '</span><span class="vmb-stat-val">' + c[1] + "</span></div>";
        }).join("");
    }

    function renderSectorTable(rows, tableId) {
        var tb = $(tableId || "bfSectorTable") && $(tableId || "bfSectorTable").querySelector("tbody");
        if (!tb) return;
        if (!rows || !rows.length) {
            tb.innerHTML = "<tr><td colspan='4'>No sector data</td></tr>";
            return;
        }
        tb.innerHTML = rows.map(function (r) {
            return "<tr><td>" + (r.sector || "") + "</td><td>" + (r.trades || 0) +
                "</td><td>" + (r.win_rate_pct != null ? r.win_rate_pct + "%" : "—") +
                "</td><td class='" + pnlClass(r.pnl_inr) + "'>₹" + fmt(r.pnl_inr, 0) + "</td></tr>";
        }).join("");
    }

    function renderTrades(rows, tableId) {
        var tb = $(tableId || "bfTradesTable") && $(tableId || "bfTradesTable").querySelector("tbody");
        if (!tb) return;
        if (!rows || !rows.length) {
            tb.innerHTML = "<tr><td colspan='15'>No trades</td></tr>";
            return;
        }
        var compact = tableId === "bfHistTradesTable";
        tb.innerHTML = rows.map(function (t) {
            var sd = String(t.session_date || "").slice(0, 10);
            var sym = t.symbol || "";
            if (!sym && t.underlying_symbol) {
                sym = t.underlying_symbol + (t.instrument_label ? " " + t.instrument_label : "");
            }
            if (compact) {
                return "<tr><td>" + sd + "</td><td>" + sym + "</td><td>" + (t.direction || "") +
                    "</td><td>" + (t.sector || "") + "</td><td>" + (t.exit_trigger_type || "") +
                    "</td><td class='" + pnlClass(t.pnl_inr) + "'>₹" + fmt(t.pnl_inr, 0) + "</td></tr>";
            }
            var anchor = t.anchor_price != null ? t.anchor_price : t.setup_close_5m;
            return "<tr><td>" + sd + "</td><td title='" + (t.price_source || "") + "'>" + sym + "</td>" +
                "<td>" + (t.direction || "") + "</td><td>" + (t.sector || "") + "</td>" +
                "<td>" + fmt(t.nifty_bias_pct, 2) + "%</td><td>" + fmt(t.stock_move_pct_at_entry, 2) + "%</td>" +
                "<td>" + fmt(anchor, 2) + "</td><td>" + fmt(t.tp_price, 2) + "</td><td>" + fmt(t.sl_price, 2) + "</td>" +
                "<td>" + fmt(t.pre_exit_extreme, 2) + "</td><td>" + fmt(t.entry_price, 2) + "</td>" +
                "<td>" + fmt(t.exit_price, 2) + "</td><td>" + (t.exit_trigger_type || "") + "</td>" +
                "<td>" + (t.lot_size != null ? t.lot_size : "") + "</td>" +
                "<td class='" + pnlClass(t.pnl_inr) + "'>₹" + fmt(t.pnl_inr, 0) + "</td></tr>";
        }).join("");
    }

    function renderPrimaryData(data) {
        renderSummary(data.summary || {});
        renderSectorTable((data.summary && data.summary.by_sector) || []);
        renderTrades(data.trades || []);
        var cap = $("bfPnlCapToggle");
        if (cap && data.pnl_cap_enabled != null) cap.checked = !!data.pnl_cap_enabled;
        var caveat = $("bfCaveat");
        if (caveat) {
            if (data.comparability_caveat) {
                caveat.hidden = false;
                caveat.textContent = data.comparability_caveat;
            } else { caveat.hidden = true; }
        }
        setStatus(
            "Loaded " + (data.trades || []).length + " trades · " +
            (data.date_from || "?") + " → " + (data.date_to || "?") +
            (data.pnl_cap_enabled ? " · ₹5K cap ON" : " · ₹5K cap OFF"),
            false
        );
    }

    async function loadPrimary(pnlCapEnabled) {
        var cap = $("bfPnlCapToggle");
        if (pnlCapEnabled == null && cap) pnlCapEnabled = cap.checked;
        var url = API + "?pnl_cap_enabled=" + (pnlCapEnabled ? "true" : "false");
        setStatus("Loading primary…", false);
        if (cap) cap.disabled = true;
        try {
            var res = await fetch(url, { cache: "no-store", credentials: "same-origin" });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            renderPrimaryData(data);
        } catch (e) {
            setStatus(String(e), true);
            renderSummary({}, "bfSummary");
            renderSectorTable([]);
            renderTrades([]);
        } finally {
            if (cap) cap.disabled = false;
        }
    }

    var histPollTimer = null;

    function coverageLabel(m) {
        var c = m.coverage || {};
        if (c.error) return "err";
        var uni = c.universe_symbols;
        if (c.symbols_active_month_coverage != null && uni) {
            return c.symbols_active_month_coverage + "/" + uni + " active";
        }
        if (c.symbols_full_month_coverage != null && uni) {
            return c.symbols_full_month_coverage + "/" + uni;
        }
        if (c.min_spot_ready_per_day != null && c.max_spot_ready_per_day != null && uni) {
            return c.min_spot_ready_per_day + "–" + c.max_spot_ready_per_day + "/day";
        }
        return c.note ? "—" : "—";
    }

    function periodDisplay(m) {
        var pl = m.period_label || "";
        if (pl === "2026-07-08") return "Jul–Aug 2026";
        if (/^\d{4}-\d{2}$/.test(pl)) {
            var parts = pl.split("-");
            var months = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
            return months[parseInt(parts[1], 10)] + " " + parts[0];
        }
        return pl;
    }

    function renderHistory(doc) {
        var caveat = $("bfHistCaveat");
        if (caveat) caveat.textContent = doc.comparability_caveat || "";

        renderSummary(doc.spot_proxy_rollup || {}, "bfHistRollup");

        var tb = $("bfHistMonthTable") && $("bfHistMonthTable").querySelector("tbody");
        var months = doc.months || [];
        if (!tb) return;
        if (!months.length) {
            tb.innerHTML = "<tr><td colspan='9'>No history yet — run is in progress or not started.</td></tr>";
            return;
        }
        tb.innerHTML = months.map(function (m) {
            var s = m.summary || {};
            var st = m.status || "";
            var stCls = st === "failed" ? "bf-status-failed" : (st === "running" ? "bf-status-running" : "");
            var stLabel = st;
            if (st === "failed" && m.error) {
                stLabel = m.error.indexOf("data issue") >= 0 ? "discarded" : st;
            }
            var stTitle = m.error ? (" title=\"" + String(m.error).replace(/"/g, "&quot;") + "\"") : "";
            return "<tr data-period='" + (m.period_label || "") + "'>" +
                "<td>" + periodDisplay(m) + "</td>" +
                "<td>" + (m.price_source || "") + "</td>" +
                "<td class='" + stCls + "'" + stTitle + ">" + stLabel + "</td>" +
                "<td>" + ((m.coverage && m.coverage.session_days) || "—") + "</td>" +
                "<td>" + (s.total_trades != null ? s.total_trades : (m.trade_count || 0)) + "</td>" +
                "<td>" + (s.win_rate_pct != null ? s.win_rate_pct + "%" : "—") + "</td>" +
                "<td class='" + pnlClass(s.total_pnl_inr) + "'>₹" + fmt(s.total_pnl_inr, 0) + "</td>" +
                "<td>" + fmt(s.avg_r, 2) + "</td>" +
                "<td>" + coverageLabel(m) + "</td></tr>";
        }).join("");

        Array.prototype.forEach.call(tb.querySelectorAll("tr[data-period]"), function (tr) {
            tr.addEventListener("click", function () {
                Array.prototype.forEach.call(tb.querySelectorAll("tr"), function (r) {
                    r.classList.remove("bf-hist-selected");
                });
                tr.classList.add("bf-hist-selected");
                loadHistoryTrades(tr.getAttribute("data-period"));
            });
        });

        var running = months.some(function (m) { return m.status === "running"; });
        var complete = months.filter(function (m) { return m.status === "complete"; }).length;
        setStatus(
            "History: " + complete + " complete · " + months.length + " rows · updated " +
            (doc.updated_at || "?").slice(0, 19),
            false,
            "bfHistStatus"
        );
        if (running && !histPollTimer) {
            histPollTimer = setInterval(loadHistory, 45000);
        } else if (!running && histPollTimer) {
            clearInterval(histPollTimer);
            histPollTimer = null;
        }
    }

    async function loadHistory() {
        setStatus("Loading history…", false, "bfHistStatus");
        try {
            var res = await fetch(HIST_API, { cache: "no-store", credentials: "same-origin" });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            renderHistory(data);
        } catch (e) {
            setStatus(String(e), true, "bfHistStatus");
        }
    }

    async function loadHistoryTrades(periodLabel) {
        if (!periodLabel) return;
        var title = $("bfHistTradesTitle");
        if (title) title.textContent = "Trade sheet — " + periodLabel;
        try {
            var res = await fetch(HIST_TRADES_API + "/" + encodeURIComponent(periodLabel) + "/trades", {
                cache: "no-store", credentials: "same-origin"
            });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || "Load failed");
            renderTrades(data.trades || [], "bfHistTradesTable");
        } catch (e) {
            renderTrades([], "bfHistTradesTable");
        }
    }

    function stopLivePoll() {
        if (livePollTimer) {
            clearInterval(livePollTimer);
            livePollTimer = null;
        }
    }

    function renderLiveBanner(data) {
        var el = $("bfLiveBanner");
        if (!el) return;
        var banner = data.banner || "";
        if (!banner) { el.hidden = true; return; }
        el.hidden = false;
        el.textContent = banner;
        el.className = "vmb-callout bf-live-banner";
        if (data.lock_failed) el.classList.add("bf-banner-alert");
        else if (data.state === "mismatch" || data.state === "stale") el.classList.add("bf-banner-alert");
        else if (data.off_cycle) el.classList.add("bf-banner-offcycle");
        else if (data.phase === "locked" || data.phase === "frozen" || data.state === "locked") el.classList.add("bf-banner-locked");
        else el.classList.add("bf-banner-forming");
    }

    function liveEmptySubtext(data) {
        data = data || {};
        if (data.loading) return "Loading…";
        var phase = data.phase || "";
        var state = data.state || "";
        if (phase === "frozen" || phase === "locked" || state === "off_session" || state === "locked") {
            return "—";
        }
        if (phase === "forming" || phase === "opening" || phase === "bar_closing") {
            return "Forming 9:15–9:20 bar…";
        }
        return "Waiting for 9:15…";
    }

    function niftyIsFilled(n) {
        return !!(n && n.direction);
    }

    function sectorIsFilled(s) {
        return !!(s && (s.sector_key || s.sector_label) && s.direction);
    }

    function stockIsFilled(st) {
        return !!(st && st.symbol);
    }

    function mergeStocksPreserve(prevStocks, nextStocks) {
        prevStocks = prevStocks || [];
        nextStocks = nextStocks || [];
        var max = Math.max(prevStocks.length, nextStocks.length);
        var out = [];
        for (var i = 0; i < max; i++) {
            var n = nextStocks[i];
            var p = prevStocks[i];
            if (stockIsFilled(n)) out.push(n);
            else if (stockIsFilled(p)) out.push(p);
            else out.push(n || p || {});
        }
        return out;
    }

    function mergeLivePreserve(prev, next) {
        if (!prev) return next;
        if (!next) return prev;
        var out = Object.assign({}, prev, next);
        if (!niftyIsFilled(next.nifty) && niftyIsFilled(prev.nifty)) {
            out.nifty = prev.nifty;
        }
        var prevS = prev.sectors || [];
        var nextS = next.sectors || [];
        var max = Math.max(prevS.length, nextS.length);
        if (max === 0) {
            out.sectors = nextS;
            return out;
        }
        var merged = [];
        for (var i = 0; i < max; i++) {
            var n = nextS[i];
            var p = prevS[i];
            if (sectorIsFilled(n)) {
                merged.push(Object.assign({}, n, {
                    stocks: mergeStocksPreserve((p && p.stocks) || [], n.stocks || [])
                }));
            } else if (sectorIsFilled(p)) {
                merged.push(p);
            } else if (n) {
                merged.push(n);
            } else if (p) {
                merged.push(p);
            }
        }
        out.sectors = merged;
        return out;
    }

    function renderLiveNifty(n, data) {
        var box = $("bfLiveNifty");
        if (!box) return;
        if (!niftyIsFilled(n)) {
            if (box.classList.contains("bf-long") || box.classList.contains("bf-short")) return;
            box.className = "bf-live-box bf-nifty-box bf-empty bf-skeleton";
            box.innerHTML = "<h3>NIFTY50</h3><div class='bf-live-pct'>—</div><div class='bf-live-sub'>" +
                liveEmptySubtext(data) + "</div>";
            return;
        }
        var longSide = n.direction === "LONG";
        box.className = "bf-live-box bf-nifty-box " + (longSide ? "bf-long" : "bf-short");
        box.innerHTML = "<h3>NIFTY50</h3>" +
            "<div class='bf-live-pct'>" + (n.bias_pct != null ? (n.bias_pct >= 0 ? "+" : "") + fmt(n.bias_pct, 2) + "%" : "—") + "</div>" +
            "<div class='bf-live-row'>Bias: <strong>" + (n.bias || "—") + "</strong>" +
            "<span class='bf-live-tag " + (longSide ? "long" : "short") + "'>" + (n.direction || "") + "</span></div>" +
            "<div class='bf-live-sub'>O " + fmt(n.open, 2) + " · C " + fmt(n.close, 2) +
            (n.ltp != null ? " · LTP " + fmt(n.ltp, 2) : "") + "</div>";
    }

    function renderStockBox(st, rank, sig) {
        var longSide = st.direction === "LONG";
        var move = st.move_pct_at_entry;
        var risk = st.risk_inr != null ? st.risk_inr : st.risk_inr_1lot;
        var rankInSector = st.rank_in_sector != null ? st.rank_in_sector : (st.stock_rank || rank);
        var sym = st.symbol || "";
        var dir = st.direction || "";
        var logged = sig && sig.trade_taken;
        return "<article class='bf-stock-box " + (longSide ? "bf-long" : "bf-short") +
            (logged ? " bf-has-log" : "") + "' data-symbol='" + sym + "' data-direction='" + dir + "'>" +
            "<button type='button' class='bf-stock-log-btn' data-bf-log-open title='Record trade'>Trade</button>" +
            "<div class='bf-stock-num'>" + rankInSector + "</div>" +
            "<div class='bf-stock-row1'>" +
            "<span class='bf-stock-name'>" + (st.display_symbol || st.symbol) + "</span>" +
            "</div>" +
            "<div class='bf-stock-row2'>LTP " + fmt(st.ltp, 2) + " · Risk ₹" + fmt(risk, 0) +
            (logged ? " · <span class='bf-logged-tag'>Logged</span>" : "") + "</div>" +
            "<div class='bf-stock-row3'>Vol " + fmt(st.volume, 0) +
            " · <span class='bf-stock-move'>" + (move != null ? (move >= 0 ? "+" : "") + fmt(move, 2) + "%" : "—") + "</span>" +
            (st.wick ? " · Wick " + st.wick : "") + "</div>" +
            "</article>";
    }

    function signalKey(sym, dir) {
        return String(sym || "").toUpperCase() + "|" + String(dir || "").toUpperCase();
    }

    function lookupSignal(sym, dir) {
        return liveSignalsCache[signalKey(sym, dir)] || null;
    }

    function ensureSectorColumns(count) {
        var wrap = $("bfLiveSectors");
        if (!wrap) return null;
        var hint = wrap.querySelector(".bf-live-empty-hint");
        if (hint) hint.remove();
        var n = Math.max(2, count || 0);
        for (var i = 0; i < n; i++) {
            if ($("bfLiveSectorCol" + i)) continue;
            var col = document.createElement("div");
            col.className = "bf-sector-column";
            col.id = "bfLiveSectorCol" + i;
            wrap.appendChild(col);
        }
        return wrap;
    }

    function renderLiveSector(i, sector, data) {
        ensureSectorColumns(i + 1);
        var col = $("bfLiveSectorCol" + i);
        if (!col) return;
        if (!sectorIsFilled(sector)) {
            if (col.querySelector(".bf-sector-box:not(.bf-empty)")) return;
            var num = i + 1;
            col.innerHTML =
                "<section class='bf-live-box bf-sector-box bf-empty bf-skeleton'><span class='bf-sector-num'>" + num +
                "</span><h3>Sector " + num + "</h3><div class='bf-live-pct'>—</div></section>" +
                "<div class='bf-sector-stocks'>" +
                "<div class='bf-stock-box bf-empty bf-skeleton'>—</div>" +
                "<div class='bf-stock-box bf-empty bf-skeleton'>—</div>" +
                "<div class='bf-stock-box bf-empty bf-skeleton'>—</div>" +
                "</div>";
            return;
        }
        var longSide = sector.direction === "LONG";
        var secNum = sector.sector_rank || (i + 1);
        var stocks = (sector.stocks || []).slice();
        while (stocks.length < 3) stocks.push({});
        var stockHtml = stocks.slice(0, 3).map(function (st, si) {
            if (!stockIsFilled(st)) {
                return "<article class='bf-stock-box bf-empty'><div class='bf-stock-num'>" + (si + 1) + "</div>—</article>";
            }
            return renderStockBox(st, si + 1, lookupSignal(st.symbol, st.direction));
        }).join("");
        col.innerHTML = "<section class='bf-live-box bf-sector-box " + (longSide ? "bf-long" : "bf-short") + "'>" +
            "<span class='bf-sector-num'>" + secNum + "</span>" +
            "<h3>" + (sector.sector_label || sector.sector_key) +
            "<span class='bf-live-tag " + (longSide ? "long" : "short") + "'>" + sector.direction + "</span></h3>" +
            "<div class='bf-live-pct'>" + (sector.move_pct >= 0 ? "+" : "") + fmt(sector.move_pct, 2) + "%</div>" +
            "</section>" +
            "<div class='bf-sector-stocks'>" + stockHtml + "</div>";
        bindLiveLogButtons(col);
    }

    function renderLiveSectors(sectors, data) {
        sectors = sectors || [];
        var n = Math.max(sectors.length, 2);
        ensureSectorColumns(n);
        for (var i = 0; i < n; i++) {
            renderLiveSector(i, sectors[i], data);
        }
    }

    function showLiveSkeletons() {
        var loading = { loading: true };
        renderLiveNifty({}, loading);
        renderLiveSector(0, null, loading);
        renderLiveSector(1, null, loading);
    }

    function renderLive(data) {
        renderLiveBanner(data);
        var meta = $("bfLiveMeta");
        if (meta) {
            var cross = data.cross_check_status ? (" · " + data.cross_check_status) : "";
            meta.textContent = "Session " + (data.session_date || "") + " · " + (data.phase || "") +
                " · " + (data.universe_instruments || 0) + " instruments · updated " +
                (data.server_time || "").slice(11, 19) + cross;
        }
        renderLiveNifty(data.nifty || {}, data);
        renderLiveSectors(data.sectors || [], data);
    }

    function istTimeFromIso(ts) {
        if (!ts) return "";
        var s = String(ts);
        var m = s.match(/T(\d{2}:\d{2}(?::\d{2})?)/);
        return m ? m[1].slice(0, 8) : "";
    }

    function isoFromSessionTime(sessionDate, timeVal) {
        if (!sessionDate || !timeVal) return null;
        var t = String(timeVal);
        if (t.length === 5) t += ":00";
        return sessionDate + "T" + t + "+05:30";
    }

    function openLogModal(sym, dir) {
        var modal = $("bfLogModal");
        if (!modal) return;
        var sig = lookupSignal(sym, dir) || {};
        $("bfLogSymbol").value = sym;
        $("bfLogDirection").value = dir;
        $("bfLogSessionDate").value = liveSessionDate;
        $("bfLogEntryPrice").value = sig.manual_entry_price != null ? sig.manual_entry_price : "";
        $("bfLogEntryTime").value = istTimeFromIso(sig.manual_entry_time);
        $("bfLogExitPrice").value = sig.manual_exit_price != null ? sig.manual_exit_price : "";
        $("bfLogExitTime").value = istTimeFromIso(sig.manual_exit_time);
        $("bfLogNote").value = sig.manual_note || "";
        var title = $("bfLogModalTitle");
        if (title) title.textContent = "Log trade — " + sym + " " + dir;
        var toast = $("bfLogToast");
        if (toast) toast.hidden = true;
        modal.hidden = false;
    }

    function closeLogModal() {
        var modal = $("bfLogModal");
        if (modal) modal.hidden = true;
    }

    function bindLiveLogButtons(root) {
        var wrap = root || $("bfLiveSectors");
        if (!wrap) return;
        Array.prototype.forEach.call(wrap.querySelectorAll("[data-bf-log-open]"), function (btn) {
            btn.addEventListener("click", function (e) {
                e.stopPropagation();
                var box = btn.closest(".bf-stock-box");
                if (!box) return;
                openLogModal(box.getAttribute("data-symbol"), box.getAttribute("data-direction"));
            });
        });
        Array.prototype.forEach.call(wrap.querySelectorAll(".bf-stock-box[data-symbol]"), function (box) {
            box.addEventListener("click", function (e) {
                if (e.target.closest("[data-bf-log-open]")) return;
                openLogModal(box.getAttribute("data-symbol"), box.getAttribute("data-direction"));
            });
        });
    }

    function applyLiveSignals(data) {
        if (!data) return;
        (data.signals || []).forEach(function (s) {
            liveSignalsCache[signalKey(s.symbol, s.direction)] = s;
        });
    }

    function fetchLiveSignals(sessionDate) {
        var url = LIVE_SIGNALS_API;
        if (sessionDate) url += "?session_date=" + encodeURIComponent(sessionDate);
        return fetch(url, { cache: "no-store", credentials: "same-origin" })
            .then(function (res) {
                return parseJsonResponse(res).then(function (data) {
                    return { ok: res.ok, data: data };
                });
            })
            .catch(function () { return null; });
    }

    async function loadLiveSignals(sessionDate) {
        if (!sessionDate) return;
        try {
            var sig = await fetchLiveSignals(sessionDate);
            if (!sig || !sig.ok) return;
            applyLiveSignals(sig.data);
        } catch (_e) { /* optional pre-fill */ }
    }

    async function saveLogModal(e) {
        if (e) e.preventDefault();
        var sym = $("bfLogSymbol").value;
        var dir = $("bfLogDirection").value;
        var sd = $("bfLogSessionDate").value;
        var body = { direction: dir };
        var ep = $("bfLogEntryPrice").value;
        var et = $("bfLogEntryTime").value;
        var xp = $("bfLogExitPrice").value;
        var xt = $("bfLogExitTime").value;
        var note = $("bfLogNote").value;
        if (ep) body.manual_entry_price = Number(ep);
        if (et) body.manual_entry_time = isoFromSessionTime(sd, et);
        if (xp) body.manual_exit_price = Number(xp);
        if (xt) body.manual_exit_time = isoFromSessionTime(sd, xt);
        if (note) body.manual_note = note;
        var btn = $("bfLogSaveBtn");
        if (btn) btn.disabled = true;
        try {
            var url = LIVE_SIGNALS_API + "/" + encodeURIComponent(sd) + "/" + encodeURIComponent(sym);
            var res = await fetch(url, {
                method: "PATCH",
                credentials: "same-origin",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body)
            });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Save failed");
            if (data.signal) liveSignalsCache[signalKey(sym, dir)] = data.signal;
            var toast = $("bfLogToast");
            if (toast) {
                toast.hidden = false;
                toast.textContent = "Saved — you can update again anytime today.";
            }
            await loadLive();
        } catch (err) {
            var toastErr = $("bfLogToast");
            if (toastErr) {
                toastErr.hidden = false;
                toastErr.style.color = "#fecaca";
                toastErr.textContent = String(err);
            }
        } finally {
            if (btn) btn.disabled = false;
        }
    }

    function initLogModal() {
        var form = $("bfLogForm");
        if (form) form.addEventListener("submit", saveLogModal);
        document.querySelectorAll("[data-bf-log-close]").forEach(function (el) {
            el.addEventListener("click", closeLogModal);
        });
    }

    async function loadLive() {
        setStatus("Loading live…", false, "bfLiveStatus");
        if (!liveLastData) showLiveSkeletons();
        var signalsP = fetchLiveSignals(liveSessionDate);
        try {
            var res = await fetchWithTimeout(LIVE_API, { cache: "no-store", credentials: "same-origin" }, 45000);
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            liveSessionDate = data.session_date || liveSessionDate || "";
            liveLastData = mergeLivePreserve(liveLastData, data);
            renderLive(liveLastData);
            setStatus("", false, "bfLiveStatus");
            signalsP.then(function (sig) {
                if (!sig || !sig.ok) return;
                applyLiveSignals(sig.data);
                if (liveLastData) renderLiveSectors(liveLastData.sectors || [], liveLastData);
            });
            stopLivePoll();
            if (data.refresh_allowed && (data.poll_interval_sec || 0) > 0) {
                livePollTimer = setInterval(loadLive, (data.poll_interval_sec || 5) * 1000);
            }
        } catch (e) {
            var msg = String(e);
            if (msg.indexOf("abort") >= 0 || msg.indexOf("AbortError") >= 0) {
                msg = "Live refresh timed out — retrying…";
            }
            if (liveLastData) {
                renderLive(liveLastData);
                setStatus(msg + " (showing last update)", true, "bfLiveStatus");
            } else {
                setStatus(msg, true, "bfLiveStatus");
            }
            stopLivePoll();
            livePollTimer = setInterval(loadLive, 5000);
        }
    }

    async function loadPrevclose() {
        setStatus("Loading prev-close backtest…", false, "bfPcStatus");
        try {
            var res = await fetch(PREVCLOSE_API, { cache: "no-store", credentials: "same-origin" });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            var caveat = $("bfPcCaveat");
            if (caveat) {
                caveat.textContent = data.comparability_caveat || caveat.textContent;
            }
            renderSummary(data.summary || {}, "bfPcSummary");
            renderSectorTable((data.summary && data.summary.by_sector) || [], "bfPcSectorTable");
            renderTrades(data.trades || [], "bfPcTradesTable");
            setStatus(
                "Loaded " + (data.trades || []).length + " trades · " +
                (data.date_from || "?") + " → " + (data.date_to || "?") +
                " · 2 sectors × 2 stocks",
                false,
                "bfPcStatus"
            );
        } catch (e) {
            setStatus(String(e), true, "bfPcStatus");
            renderSummary({}, "bfPcSummary");
            renderSectorTable([], "bfPcSectorTable");
            renderTrades([], "bfPcTradesTable");
        }
    }

    function renderTrapSummary(s) {
        var box = $("bfTrapSummary");
        if (!box) return;
        s = s || {};
        var win = s.win_pct != null ? fmt(s.win_pct, 1) + "%" : "—";
        var stockWin = s.stock_win_pct != null ? fmt(s.stock_win_pct, 1) + "%" : "—";
        var cards = [
            ["FUT trades", s.trade_count],
            ["Stock trades", s.stock_trade_count],
            ["Skipped", s.skip_count],
            ["FUT win %", win],
            ["Stock win %", stockWin],
            ["FUT avg R", fmt(s.avg_r, 2)],
            ["FUT P&L", "₹" + fmt(s.sum_pnl_inr, 0)],
            ["Stock P&L", "₹" + fmt(s.stock_sum_pnl_inr, 0)],
            ["CSV rows", s.csv_rows],
        ];
        box.innerHTML = cards.map(function (c) {
            return '<div class="vmb-stat"><span class="vmb-stat-label">' + c[0] +
                '</span><span class="vmb-stat-val">' + c[1] + "</span></div>";
        }).join("");
    }

    function mixText(obj) {
        if (!obj || typeof obj !== "object") return "—";
        var keys = Object.keys(obj);
        if (!keys.length) return "—";
        return keys.map(function (k) { return k + " " + obj[k]; }).join(" · ");
    }

    function trapBucket(r) {
        var b = String(r && r.bucket || "").toLowerCase();
        if (b === "stock" || b === "eq") return "stock";
        var k = String(r && r.instrument_kind || "").toLowerCase();
        if (k === "eq" || k === "stock") return "stock";
        return "fut";
    }

    function trapRiskTxt(t) {
        return t.risk_inr != null ? "₹" + fmt(t.risk_inr, 0) : "—";
    }

    function trapFutRowHtml(t) {
        return "<tr><td>" + String(t.session_date || "").slice(0, 10) + "</td>" +
            "<td>" + (t.symbol || "") + "</td>" +
            "<td>" + (t.trigger_time || "") + "</td>" +
            "<td>" + fmt(t.entry, 2) + "</td>" +
            "<td>" + fmt(t.sl_initial, 2) + "</td>" +
            "<td>" + fmt(t.exit, 2) + "</td>" +
            "<td>" + (t.exit_reason || "") + "</td>" +
            "<td>" + fmt(t.r_realized, 2) + "</td>" +
            "<td>" + trapRiskTxt(t) + "</td>" +
            "<td class='" + pnlClass(t.pnl_inr) + "'>₹" + fmt(t.pnl_inr, 0) + "</td></tr>";
    }

    function trapStockRowHtml(t) {
        return "<tr><td>" + String(t.session_date || "").slice(0, 10) + "</td>" +
            "<td>" + (t.symbol || "") + "</td>" +
            "<td>" + (t.trigger_time || "") + "</td>" +
            "<td>" + fmt(t.entry, 2) + "</td>" +
            "<td>" + fmt(t.sl_initial, 2) + "</td>" +
            "<td>" + trapRiskTxt(t) + "</td>" +
            "<td>" + fmt(t.exit, 2) + "</td>" +
            "<td>" + (t.exit_reason || "") + "</td>" +
            "<td>" + fmt(t.r_realized, 2) + "</td>" +
            "<td class='" + pnlClass(t.pnl_inr) + "'>₹" + fmt(t.pnl_inr, 0) + "</td></tr>";
    }

    function fillTrapTable(tableId, rows, emptyMsg, cols, rowFn) {
        var tb = $(tableId) && $(tableId).querySelector("tbody");
        if (!tb) return;
        if (!rows.length) {
            tb.innerHTML = "<tr><td colspan='" + cols + "'>" + emptyMsg + "</td></tr>";
            return;
        }
        tb.innerHTML = rows.map(rowFn).join("");
    }

    function renderTrapRows(data) {
        var rows = data.rows || [];
        var futTaken = rows.filter(function (r) { return r.taken && trapBucket(r) !== "stock"; });
        var stockTaken = rows.filter(function (r) { return r.taken && trapBucket(r) === "stock"; });
        var skipped = rows.filter(function (r) {
            return !r.taken && String(r.skip_reason || "").indexOf("risk cap") < 0;
        });
        fillTrapTable("bfTrapTradesTable", futTaken, "No FUT trades taken", 10, trapFutRowHtml);
        fillTrapTable("bfTrapStockTable", stockTaken, "No stock trades", 10, trapStockRowHtml);
        var sb = $("bfTrapSkipTable") && $("bfTrapSkipTable").querySelector("tbody");
        if (sb) {
            if (!skipped.length) {
                sb.innerHTML = "<tr><td colspan='5'>No skips</td></tr>";
            } else {
                sb.innerHTML = skipped.map(function (t) {
                    var riskTxt = t.risk_inr != null ? "₹" + fmt(t.risk_inr, 0) : "—";
                    return "<tr><td>" + String(t.session_date || "").slice(0, 10) + "</td>" +
                        "<td>" + (t.symbol || "") + "</td>" +
                        "<td>" + (t.trigger_time || "") + "</td>" +
                        "<td>" + (t.skip_reason || "") + "</td>" +
                        "<td>" + riskTxt + "</td></tr>";
                }).join("");
            }
        }
        var notes = $("bfTrapNotes");
        if (notes) {
            var s = data.summary || {};
            notes.textContent = "FUT exits: " + mixText(s.exit_reasons) +
                ". Stock exits: " + mixText(s.stock_exit_reasons) +
                ". Skip mix: " + mixText(s.skip_reasons) +
                ". Risk Amount = (entry − SL) × 1 lot qty. Stock qty = 1 share.";
        }
    }

    function escapeHtml(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function fillTrapLiveDateSelect(days, selected) {
        var sel = $("bfTrapLiveDate");
        if (!sel) return;
        var opts = (days || []).slice();
        if (selected && opts.indexOf(selected) < 0) opts.unshift(selected);
        sel.innerHTML = opts.map(function (d) {
            return "<option value=\"" + escapeHtml(d) + "\"" + (d === selected ? " selected" : "") + ">" + escapeHtml(d) + "</option>";
        }).join("");
    }

    function renderTrapLiveRows(rows) {
        var tb = $("bfTrapLiveTable") && $("bfTrapLiveTable").querySelector("tbody");
        if (!tb) return;
        if (!rows || !rows.length) {
            tb.innerHTML = "<tr><td colspan='8'>No webhook rows for this day</td></tr>";
            return;
        }
        tb.innerHTML = rows.map(function (r) {
            var raw = "";
            try { raw = JSON.stringify(r.raw_payload, null, 2); } catch (_e) { raw = String(r.raw_payload); }
            var payload = r.raw_payload && typeof r.raw_payload === "object" ? r.raw_payload : {};
            var received = r.received_at || r.received_at_hms || "";
            var triggered = r.triggered_at_raw || r.triggered_at || payload.triggered_at || "";
            return "<tr>" +
                "<td>" + escapeHtml(received) + "</td>" +
                "<td>" + escapeHtml(triggered || "—") + "</td>" +
                "<td>" + escapeHtml(r.symbol || "—") + "</td>" +
                "<td>" + (r.trigger_price != null ? escapeHtml(r.trigger_price) : "—") + "</td>" +
                "<td>" + escapeHtml(r.scan_name || "—") + "</td>" +
                "<td>" + escapeHtml(r.parse_status || "—") + "</td>" +
                "<td>" + escapeHtml(r.source_ip || "—") + "</td>" +
                "<td><details class=\"bf-raw-details\"><summary>View Raw</summary><pre class=\"bf-raw-pre\">" +
                escapeHtml(raw) + "</pre></details></td>" +
                "</tr>";
        }).join("");
    }

    async function loadTrapCeLive(dateStr) {
        setStatus("Loading Trap-CE-Live…", false, "bfTrapLiveStatus");
        var sel = $("bfTrapLiveDate");
        var d = dateStr || (sel && sel.value) || "";
        var url = TRAP_CE_LIVE_API + (d ? ("?date=" + encodeURIComponent(d)) : "");
        try {
            var res = await fetch(url, { cache: "no-store", credentials: "same-origin" });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            fillTrapLiveDateSelect(data.days || [], data.date);
            renderTrapLiveRows(data.rows || []);
            setStatus("Loaded " + (data.count || 0) + " row(s)", false, "bfTrapLiveStatus");
        } catch (e) {
            setStatus(String(e), true, "bfTrapLiveStatus");
            renderTrapLiveRows([]);
        }
    }

    async function loadTrapCe() {
        setStatus("Loading Trap-CE backtest…", false, "bfTrapStatus");
        try {
            var res = await fetch(TRAP_CE_API, { cache: "no-store", credentials: "same-origin" });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            renderTrapSummary(data.summary || {});
            renderTrapRows(data);
            var s = data.summary || {};
            setStatus(
                "Loaded " + (s.trade_count || 0) + " trades · " + (s.skip_count || 0) + " skipped",
                false,
                "bfTrapStatus"
            );
        } catch (e) {
            setStatus(String(e), true, "bfTrapStatus");
            renderTrapSummary({});
            renderTrapRows({ rows: [], summary: {} });
        }
    }

    function switchTab(tab) {
        var primary = tab === "primary";
        var history = tab === "history";
        var live = tab === "live";
        var prevclose = tab === "prevclose";
        var trapce = tab === "trapce";
        var trapcelive = tab === "trapcelive";
        $("bfPanelPrimary").hidden = !primary;
        $("bfPanelHistory").hidden = !history;
        $("bfPanelLive").hidden = !live;
        if ($("bfPanelPrevclose")) $("bfPanelPrevclose").hidden = !prevclose;
        if ($("bfPanelTrapCe")) $("bfPanelTrapCe").hidden = !trapce;
        if ($("bfPanelTrapCeLive")) $("bfPanelTrapCeLive").hidden = !trapcelive;
        $("bfTabPrimary").classList.toggle("active", primary);
        $("bfTabHistory").classList.toggle("active", history);
        $("bfTabLive").classList.toggle("active", live);
        if ($("bfTabPrevclose")) $("bfTabPrevclose").classList.toggle("active", prevclose);
        if ($("bfTabTrapCe")) $("bfTabTrapCe").classList.toggle("active", trapce);
        if ($("bfTabTrapCeLive")) $("bfTabTrapCeLive").classList.toggle("active", trapcelive);
        document.querySelectorAll(".bf-primary-only").forEach(function (el) {
            el.style.display = primary ? "" : "none";
        });
        stopLivePoll();
        if (primary) loadPrimary(null);
        else if (history) loadHistory();
        else if (prevclose) loadPrevclose();
        else if (trapce) loadTrapCe();
        else if (trapcelive) loadTrapCeLive();
        else if (live) loadLive();
    }

    function initTabs() {
        $("bfTabPrimary").addEventListener("click", function () { switchTab("primary"); });
        $("bfTabHistory").addEventListener("click", function () { switchTab("history"); });
        $("bfTabLive").addEventListener("click", function () { switchTab("live"); });
        if ($("bfTabPrevclose")) {
            $("bfTabPrevclose").addEventListener("click", function () { switchTab("prevclose"); });
        }
        if ($("bfTabTrapCe")) {
            $("bfTabTrapCe").addEventListener("click", function () { switchTab("trapce"); });
        }
        if ($("bfTabTrapCeLive")) {
            $("bfTabTrapCeLive").addEventListener("click", function () { switchTab("trapcelive"); });
        }
        var dateSel = $("bfTrapLiveDate");
        if (dateSel) {
            dateSel.addEventListener("change", function () { loadTrapCeLive(dateSel.value); });
        }
    }

    function initPnlCapToggle() {
        var cap = $("bfPnlCapToggle");
        if (!cap) return;
        cap.addEventListener("change", function () { loadPrimary(cap.checked); });
    }

    document.addEventListener("DOMContentLoaded", function () {
        initTabs();
        initPnlCapToggle();
        initLogModal();
        var reload = $("bfReloadBtn");
        if (reload) reload.addEventListener("click", function () {
            if (!$("bfPanelHistory").hidden) loadHistory();
            else if (!$("bfPanelLive").hidden) loadLive();
            else if ($("bfPanelPrevclose") && !$("bfPanelPrevclose").hidden) loadPrevclose();
            else if ($("bfPanelTrapCe") && !$("bfPanelTrapCe").hidden) loadTrapCe();
            else if ($("bfPanelTrapCeLive") && !$("bfPanelTrapCeLive").hidden) loadTrapCeLive();
            else loadPrimary(null);
        });
        switchTab("live");
    });
})();
