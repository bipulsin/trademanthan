(function () {
    const API = "/api/breakfast-strategy/data";
    const HIST_API = "/api/breakfast-strategy/history";
    const HIST_TRADES_API = "/api/breakfast-strategy/history";
    const LIVE_API = "/api/breakfast-strategy/live";
    const LIVE_SIGNALS_API = "/api/breakfast-strategy/live/signals";
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

    function renderSectorTable(rows) {
        var tb = $("bfSectorTable") && $("bfSectorTable").querySelector("tbody");
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

    function renderLiveNifty(n, data) {
        var box = $("bfLiveNifty");
        if (!box) return;
        if (!n || !n.direction) {
            box.className = "bf-live-box bf-nifty-box bf-empty";
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
            "<button type='button' class='bf-stock-log-btn' data-bf-log-open title='Log manual trade'>Log</button>" +
            "<div class='bf-stock-num'>" + rankInSector + "</div>" +
            "<div class='bf-stock-row1'>" +
            "<span class='bf-stock-name'>" + (st.display_symbol || st.symbol) + "</span>" +
            "<span class='bf-stock-move'>" + (move != null ? (move >= 0 ? "+" : "") + fmt(move, 2) + "%" : "—") + "</span>" +
            "</div>" +
            "<div class='bf-stock-row2'>LTP " + fmt(st.ltp, 2) + " · Risk ₹" + fmt(risk, 0) +
            (logged ? " · <span class='bf-logged-tag'>Logged</span>" : "") + "</div>" +
            "<div class='bf-stock-row3'>Vol " + fmt(st.volume, 0) + " · Rank #" + rankInSector + "</div>" +
            "</article>";
    }

    function signalKey(sym, dir) {
        return String(sym || "").toUpperCase() + "|" + String(dir || "").toUpperCase();
    }

    function lookupSignal(sym, dir) {
        return liveSignalsCache[signalKey(sym, dir)] || null;
    }

    function renderLiveSectors(sectors, data) {
        var wrap = $("bfLiveSectors");
        if (!wrap) return;
        if (!sectors || !sectors.length) {
            var hint = liveEmptySubtext(data);
            wrap.innerHTML = "<div class='bf-live-empty-hint'>" + hint + "</div>" +
                "<div class='bf-sector-column'>" +
                "<section class='bf-live-box bf-sector-box bf-empty'><span class='bf-sector-num'>1</span><h3>Sector 1</h3><div class='bf-live-pct'>—</div></section>" +
                "<div class='bf-sector-stocks'><div class='bf-stock-box bf-empty'>—</div><div class='bf-stock-box bf-empty'>—</div><div class='bf-stock-box bf-empty'>—</div></div>" +
                "</div>" +
                "<div class='bf-sector-column'>" +
                "<section class='bf-live-box bf-sector-box bf-empty'><span class='bf-sector-num'>2</span><h3>Sector 2</h3><div class='bf-live-pct'>—</div></section>" +
                "<div class='bf-sector-stocks'><div class='bf-stock-box bf-empty'>—</div><div class='bf-stock-box bf-empty'>—</div><div class='bf-stock-box bf-empty'>—</div></div>" +
                "</div>";
            return;
        }
        wrap.innerHTML = sectors.map(function (s) {
            var longSide = s.direction === "LONG";
            var secNum = s.sector_rank || 1;
            var stocks = s.stocks || [];
            while (stocks.length < 3) stocks.push({});
            var stockHtml = stocks.slice(0, 3).map(function (st, i) {
                if (!st || !st.symbol) {
                    return "<article class='bf-stock-box bf-empty'><div class='bf-stock-num'>" + (i + 1) + "</div>—</article>";
                }
                return renderStockBox(st, i + 1, lookupSignal(st.symbol, st.direction));
            }).join("");
            return "<div class='bf-sector-column'>" +
                "<section class='bf-live-box bf-sector-box " + (longSide ? "bf-long" : "bf-short") + "'>" +
                "<span class='bf-sector-num'>" + secNum + "</span>" +
                "<h3>" + (s.sector_label || s.sector_key) +
                "<span class='bf-live-tag " + (longSide ? "long" : "short") + "'>" + s.direction + "</span></h3>" +
                "<div class='bf-live-pct'>" + (s.move_pct >= 0 ? "+" : "") + fmt(s.move_pct, 2) + "%</div>" +
                "<div class='bf-live-sub'>Vol " + fmt(s.volume, 0) + " · Rank #" + s.sector_rank + "</div>" +
                "</section>" +
                "<div class='bf-sector-stocks'>" + stockHtml + "</div>" +
                "</div>";
        }).join("");
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
        bindLiveLogButtons();
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

    function bindLiveLogButtons() {
        var wrap = $("bfLiveSectors");
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

    async function loadLiveSignals(sessionDate) {
        if (!sessionDate) return;
        try {
            var res = await fetch(LIVE_SIGNALS_API + "?session_date=" + encodeURIComponent(sessionDate), {
                cache: "no-store", credentials: "same-origin"
            });
            var data = await parseJsonResponse(res);
            if (!res.ok) return;
            liveSignalsCache = {};
            (data.signals || []).forEach(function (s) {
                liveSignalsCache[signalKey(s.symbol, s.direction)] = s;
            });
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
        try {
            var res = await fetchWithTimeout(LIVE_API, { cache: "no-store", credentials: "same-origin" }, 45000);
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            liveSessionDate = data.session_date || "";
            await loadLiveSignals(liveSessionDate);
            liveLastData = data;
            renderLive(data);
            setStatus("", false, "bfLiveStatus");
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

    function switchTab(tab) {
        var primary = tab === "primary";
        var history = tab === "history";
        var live = tab === "live";
        $("bfPanelPrimary").hidden = !primary;
        $("bfPanelHistory").hidden = !history;
        $("bfPanelLive").hidden = !live;
        $("bfTabPrimary").classList.toggle("active", primary);
        $("bfTabHistory").classList.toggle("active", history);
        $("bfTabLive").classList.toggle("active", live);
        document.querySelectorAll(".bf-primary-only").forEach(function (el) {
            el.style.display = primary ? "" : "none";
        });
        stopLivePoll();
        if (primary) loadPrimary(null);
        else if (history) loadHistory();
        else if (live) loadLive();
    }

    function initTabs() {
        $("bfTabPrimary").addEventListener("click", function () { switchTab("primary"); });
        $("bfTabHistory").addEventListener("click", function () { switchTab("history"); });
        $("bfTabLive").addEventListener("click", function () { switchTab("live"); });
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
            else loadPrimary(null);
        });
        switchTab("live");
    });
})();
