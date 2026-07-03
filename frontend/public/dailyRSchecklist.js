/**
 * Daily RS Trade Checklist — left-menu layout, bull/bear columns, modal checklist.
 * System fields auto-fill from RS scanner; user only edits news, ADX 9:35 override, notes, counter-RS.
 */
(function () {
    "use strict";

    var API = "/api/dashboard/daily-checklist";
    var state = null;
    var saveTimers = {};
    var cardEls = {};
    var modalSymbol = null;
    var lastAdxRecheckAlertKey = null;

    var AUTO_FIELDS = [
        "entry_time", "kavach_score_entry", "confidence", "trading_state",
        "ema_vs_vwap", "supertrend", "macd", "adx_entry", "volume", "di_alignment"
    ];
    var AUTO_LABELS = {
        entry_time: "Entry Time",
        kavach_score_entry: "Kavach Score @ Entry",
        confidence: "Confidence Grade",
        trading_state: "Trading State",
        ema_vs_vwap: "EMA5 vs VWAP",
        supertrend: "Supertrend",
        macd: "MACD",
        adx_entry: "ADX @ Entry",
        di_alignment: "DI+ vs DI-",
        volume: "Volume"
    };
    var FLAG = {
        entry_time: "time_ok", kavach_score_entry: "score_ok", confidence: "confidence_ok",
        trading_state: "state_ok", ema_vs_vwap: "ema_ok", supertrend: "st_ok",
        macd: "macd_ok", adx_entry: "adx_ok", volume: "volume_ok"
    };
    var SECTION_ORDER = { GO: 0, WATCH: 1, OUT: 2, NONE: 1 };

    function $(id) { return document.getElementById(id); }
    function el(tag, cls, txt) {
        var e = document.createElement(tag);
        if (cls) e.className = cls;
        if (txt != null) e.textContent = txt;
        return e;
    }
    function lsKey() { return "dc_state_" + (state ? state.session_date : "today"); }

    function api(path, opts) {
        return fetch(API + path, opts).then(function (r) { return r.json(); });
    }

    function toast(msg) {
        var t = $("dcToast");
        t.textContent = msg;
        t.classList.add("show");
        setTimeout(function () { t.classList.remove("show"); }, 1800);
    }

    function nowIST() {
        var parts = new Intl.DateTimeFormat("en-GB", {
            timeZone: "Asia/Kolkata", hour12: false,
            hour: "2-digit", minute: "2-digit", second: "2-digit"
        }).formatToParts(new Date());
        var o = {};
        parts.forEach(function (p) { if (p.type !== "literal") o[p.type] = p.value; });
        var h = parseInt(o.hour, 10), m = parseInt(o.minute, 10), s = parseInt(o.second, 10);
        return { minutes: h * 60 + m, secs: h * 3600 + m * 60 + s,
                 str: o.hour + ":" + o.minute + ":" + o.second };
    }

    // ADX recheck alert windows: show banner only in the 10 minutes before each target (IST).
    var ADX_RECHECK_TARGETS = [10 * 60, 10 * 60 + 30]; // 10:00, 10:30
    var ADX_RECHECK_LEAD_MIN = 10;
    var ADX_RECHECK_FLASH_MIN = 2; // flash in the last 2 minutes before target

    function fmtIstAmPm(totalMinutes) {
        var h24 = Math.floor(totalMinutes / 60);
        var mm = totalMinutes % 60;
        var h12 = h24 % 12;
        if (h12 === 0) h12 = 12;
        return h12 + ":" + ("0" + mm).slice(-2) + " " + (h24 < 12 ? "AM" : "PM");
    }

    function adxRecheckAlert(nowMinutes) {
        for (var i = 0; i < ADX_RECHECK_TARGETS.length; i++) {
            var target = ADX_RECHECK_TARGETS[i];
            var start = target - ADX_RECHECK_LEAD_MIN;
            if (nowMinutes >= start && nowMinutes < target) {
                var minsLeft = target - nowMinutes;
                var label = fmtIstAmPm(target);
                return {
                    show: true,
                    flash: minsLeft <= ADX_RECHECK_FLASH_MIN,
                    text: minsLeft <= ADX_RECHECK_FLASH_MIN
                        ? "⏰ Now is " + label + " — recheck ADX for this stock"
                        : "⏰ Recheck ADX at " + label
                };
            }
        }
        return { show: false };
    }

    function fmtDate(iso) {
        if (!iso) return "—";
        var d = new Date(iso + "T00:00:00");
        return ("0" + d.getDate()).slice(-2) + "-" +
            ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][d.getMonth()] +
            "-" + d.getFullYear();
    }

    function decisionClass(stock) {
        if (!stock.decision || stock.decision.indexOf("⬜") === 0) return "NONE";
        return stock.section || "WATCH";
    }

    function scoreClass(score) {
        var s = Number(score);
        if (s >= 90) return "dc-score--green";
        if (s >= 70) return "dc-score--amber";
        return "dc-score--red";
    }

    function maturityBadgeHtml(tag, days) {
        var t = (tag || "FRESH").toUpperCase();
        var cls = "dc-maturity--fresh";
        var text = "FRESH";
        if (t === "CLIMACTIC") {
            cls = "dc-maturity--climactic";
            text = "CLIMACTIC";
        } else if (t === "CONTINUING") {
            cls = "dc-maturity--continuing";
            text = "DAY " + (days || 2);
        } else if (t === "EXTENDED") {
            cls = "dc-maturity--extended";
            text = "EXTENDED";
        } else if (t === "STRETCHED") {
            cls = "dc-maturity--stretched";
            text = "STRETCHED · " + (days || 4) + "D";
        }
        return '<span class="dc-maturity-badge ' + cls + '">' + text + "</span>";
    }

    function fmtDataAsOf(iso) {
        if (!iso) return "—";
        var d = new Date(iso);
        if (isNaN(d.getTime())) return "—";
        return ("0" + d.getHours()).slice(-2) + ":" +
            ("0" + d.getMinutes()).slice(-2) + ":" +
            ("0" + d.getSeconds()).slice(-2) + " IST";
    }

    function dataAgeMinutes(iso) {
        if (!iso) return 999;
        var d = new Date(iso);
        if (isNaN(d.getTime())) return 999;
        return (Date.now() - d.getTime()) / 60000;
    }

    function currentStock(symbol) {
        if (!state) return null;
        var pools = [state.today, state.carryover, state.preview, state.stocks];
        for (var p = 0; p < pools.length; p++) {
            var list = pools[p];
            if (!list) continue;
            for (var i = 0; i < list.length; i++) {
                if (list[i].symbol === symbol) return list[i];
            }
        }
        return null;
    }

    function isActionableStock(stock) {
        return stock && !stock.is_carryover && !stock.is_preview;
    }

    function hintFor(field, stock) {
        if (field === "adx_935") {
            var st = stock.adx_935_status;
            if (st === "immediate") return { text: "✓ Immediate list", cls: "dc-item-hint--ok" };
            if (st === "recheck") return { text: "⚠ Recheck 10AM & 10:30AM", cls: "dc-item-hint--warn" };
            if (st === "watch") return { text: "✗ Watch only", cls: "dc-item-hint--bad" };
            return null;
        }
        var flag = FLAG[field];
        if (!flag) return null;
        var v = stock[flag];
        if (v === true) return { text: "✓ PASS", cls: "dc-item-hint--ok" };
        if (v === false) {
            if (field === "entry_time") return { text: "✗ HARD FAIL (outside 9:45–14:30)", cls: "dc-item-hint--bad" };
            return { text: "✗ FAIL", cls: "dc-item-hint--bad" };
        }
        return null;
    }

    function autoValClass(field, stock) {
        var flag = FLAG[field];
        if (!flag) return "neutral";
        var v = stock[flag];
        if (v === true) return "pass";
        if (v === false) return "fail";
        return "neutral";
    }

    // ---- cards (bull / bear columns) ----
    function ensureCard(symbol) {
        if (cardEls[symbol]) return cardEls[symbol];
        var node = $("dcCardTpl").content.firstElementChild.cloneNode(true);
        node.dataset.symbol = symbol;
        node.addEventListener("click", function () { openModal(symbol); });
        cardEls[symbol] = node;
        return node;
    }

    function patchCard(card, stock, opts) {
        opts = opts || {};
        var dcls = decisionClass(stock);
        card.className = "dc-card";
        if (opts.preview || stock.is_preview) card.classList.add("dc-card--preview");
        if (dcls === "GO") card.classList.add("dc-card--go");
        if (dcls === "OUT") card.classList.add("dc-card--out");
        if (stock.carryover_warning) card.classList.add("dc-card--carryover");
        else card.classList.remove("dc-card--carryover");
        card.querySelector(".dc-symbol").textContent = stock.symbol;
        var sb = card.querySelector(".dc-sector-badge");
        if (sb) {
            sb.textContent = stock.sector_badge || "";
            sb.style.display = stock.sector_badge ? "" : "none";
        }
        var rsv = stock.rs_pct;
        var rs = card.querySelector(".dc-rs");
        rs.textContent = rsv == null ? "" : "RS " + (rsv > 0 ? "+" : "") + Number(rsv).toFixed(2) + "%";
        rs.className = "dc-rs " + (Number(rsv) >= 0 ? "dc-rs--pos" : "dc-rs--neg");
        var score = card.querySelector(".dc-score");
        if (stock.dashboard_score != null) {
            score.textContent = stock.dashboard_score;
            score.className = "dc-score " + scoreClass(stock.dashboard_score);
            score.style.display = "";
        } else { score.style.display = "none"; }
        var conf = card.querySelector(".dc-conf");
        conf.textContent = stock.confidence || stock.dashboard_kavach || "";
        var mat = card.querySelector(".dc-maturity");
        if (mat) {
            mat.innerHTML = maturityBadgeHtml(stock.maturity_tag, stock.consecutive_days_on_list);
        }
        var dec = card.querySelector(".dc-decision");
        dec.textContent = stock.decision || "⬜ Not assessed";
        dec.className = "dc-decision dc-decision--" + dcls;
        var setupEl = card.querySelector(".dc-setup");
        if (setupEl) {
            var st = (stock.setup_state || "NEUTRAL").toUpperCase();
            if (st === "NEUTRAL" || st === "EXPIRED") {
                setupEl.textContent = "";
                setupEl.className = "dc-setup";
            } else {
                setupEl.textContent = st.replace("_", "·");
                setupEl.className = "dc-setup dc-setup--" + st.toLowerCase().replace("_", "-");
                if (stock.sl_pct != null) setupEl.textContent += " · SL " + Number(stock.sl_pct).toFixed(2) + "%";
            }
        }
        var lockEl = card.querySelector(".dc-grade-lock");
        if (lockEl) lockEl.hidden = !stock.grade_gate_locked;
    }

    function sortStocks(list) {
        return list.slice().sort(function (a, b) {
            var oa = SECTION_ORDER[decisionClass(a)] != null ? SECTION_ORDER[decisionClass(a)] : 1;
            var ob = SECTION_ORDER[decisionClass(b)] != null ? SECTION_ORDER[decisionClass(b)] : 1;
            if (oa !== ob) return oa - ob;
            return (b.rs_pct || 0) - (a.rs_pct || 0);
        });
    }

    function renderLiveSetups() {
        var wrap = $("dcLiveSetups");
        var chips = $("dcLiveSetupsChips");
        if (!wrap || !chips) return;
        var setups = (state && state.live_setups) || [];
        if (!setups.length) {
            wrap.hidden = true;
            chips.innerHTML = "";
            return;
        }
        wrap.hidden = false;
        chips.innerHTML = setups.map(function (s) {
            var cls = "dc-live-chip dc-live-chip--" + String(s.state || "").toLowerCase();
            return '<span class="' + cls + '">' + s.symbol + " · " + s.side + " · " + s.state +
                (s.sl_pct != null ? " · SL " + Number(s.sl_pct).toFixed(2) + "%" : "") + "</span>";
        }).join("");
    }

    function render() {
        if (!state) return;
        $("dcDate").textContent = fmtDate(state.session_date);
        var nifty = "";
        if (state.nifty50 != null) nifty += "NIFTY <b>" + state.nifty50 + "</b>";
        if (state.banknifty != null) nifty += (nifty ? " · " : "") + "BANKNIFTY <b>" + state.banknifty + "</b>";
        $("dcNifty").innerHTML = nifty;
        var c = state.counts || { go: 0, watch: 0, out: 0 };
        $("dcPillGo").textContent = "🟢 " + c.go + " GO";
        $("dcPillWatch").textContent = "🟡 " + c.watch + " WATCH";
        $("dcPillOut").textContent = "🔴 " + c.out + " OUT";

        var sel = $("dcNiftyDir");
        if (document.activeElement !== sel) sel.value = state.nifty_open_direction || "";
        $("dcGapWarn").classList.toggle("show", (state.nifty_open_direction || "") === "Gap reversed");

        var rot = state.rotation_day || {};
        var rotEl = $("dcRotationBanner");
        if (rot.rotation_day_type === "CONTINUATION") {
            rotEl.hidden = false;
            rotEl.className = "dc-rotation-banner dc-rotation--continuation";
            rotEl.textContent = "CONTINUATION day — " + (rot.bull_overlap || 0) + " bull / " +
                (rot.bear_overlap || 0) + " bear overlap with yesterday. Dual-scan rules apply.";
        } else if (rot.rotation_day_type === "ROTATION") {
            rotEl.hidden = false;
            rotEl.className = "dc-rotation-banner dc-rotation--rotation";
            rotEl.textContent = "ROTATION day — fresh scan is primary. Yesterday carryover names may mean-revert.";
        } else if (rot.rotation_day_type === "MIXED") {
            rotEl.hidden = false;
            rotEl.className = "dc-rotation-banner dc-rotation--mixed";
            rotEl.textContent = "MIXED day — overlap names (" + (rot.bull_overlap || 0) + " bull / " +
                (rot.bear_overlap || 0) + " bear) are highest conviction.";
        } else {
            rotEl.hidden = true;
        }

        var asofEl = $("dcDataAsOf");
        var asofIso = state.data_refreshed_at || null;
        asofEl.textContent = "Data as of " + fmtDataAsOf(asofIso);
        asofEl.classList.toggle("dc-data-asof--stale", dataAgeMinutes(asofIso) > 6);

        var stocks = state.stocks || state.today || state.preview || [];
        var carry = state.carryover || [];
        var locked = !!state.locked;
        var preview = !locked && (state.preview || []).length > 0;

        $("dcPendingLock").hidden = locked;
        if (!locked) {
            var start = 9 * 60 + 25;
            var t = nowIST();
            if (t.minutes < start) {
                var rem = (start * 60) - t.secs;
                $("dcLockCountdown").textContent =
                    ("0" + Math.floor(rem / 60)).slice(-2) + ":" + ("0" + (rem % 60)).slice(-2);
            } else {
                $("dcLockCountdown").textContent = "pending next scan";
            }
        }

        var lockedTitle = $("dcLockedTitle");
        if (locked) {
            lockedTitle.innerHTML = '<i class="fas fa-lock"></i> Today\'s Kavach List';
            var atEl = $("dcLockedAt");
            atEl.textContent = state.locked_at
                ? "Locked at " + fmtDataAsOf(state.locked_at)
                : "";
        } else if (preview) {
            lockedTitle.innerHTML = '<i class="fas fa-eye"></i> Preview — unconfirmed until 09:25 lock';
            $("dcLockedAt").textContent = "";
        } else {
            lockedTitle.innerHTML = '<i class="fas fa-clock"></i> Today\'s Kavach List';
            $("dcLockedAt").textContent = "";
        }

        var empty = stocks.length === 0 && carry.length === 0;
        $("dcEmpty").hidden = !empty;
        $("dcColumns").hidden = empty;

        var bull = sortStocks(stocks.filter(function (s) { return s.direction === "LONG"; }));
        var bear = sortStocks(stocks.filter(function (s) { return s.direction === "SHORT"; }));
        var bullGrid = $("dcBullGrid");
        var bearGrid = $("dcBearGrid");
        bull.forEach(function (stock) {
            var card = ensureCard(stock.symbol);
            patchCard(card, stock, { preview: preview });
            if (card.parentNode !== bullGrid) bullGrid.appendChild(card);
        });
        bear.forEach(function (stock) {
            var card = ensureCard(stock.symbol);
            patchCard(card, stock, { preview: preview });
            if (card.parentNode !== bearGrid) bearGrid.appendChild(card);
        });
        [bullGrid, bearGrid].forEach(function (grid) {
            var syms = {};
            stocks.forEach(function (s) { syms[s.symbol] = true; });
            Array.prototype.slice.call(grid.children).forEach(function (ch) {
                if (!syms[ch.dataset.symbol]) grid.removeChild(ch);
            });
        });

        var coSec = $("dcCarryoverSection");
        var coGrid = $("dcCarryoverGrid");
        if (carry.length > 0) {
            coSec.hidden = false;
            $("dcCarryoverCount").textContent = String(carry.length);
            carry.forEach(function (stock) {
                var row = coGrid.querySelector('[data-symbol="' + stock.symbol + '"]');
                if (!row) {
                    row = $("dcCarryTpl").content.firstElementChild.cloneNode(true);
                    row.dataset.symbol = stock.symbol;
                    coGrid.appendChild(row);
                }
                row.querySelector(".dc-carry-sym").textContent = stock.symbol + " · " + stock.direction;
                var rsv = stock.rs_pct;
                row.querySelector(".dc-carry-rs").textContent = rsv == null ? "—" :
                    "RS " + (rsv > 0 ? "+" : "") + Number(rsv).toFixed(2) + "%";
                row.querySelector(".dc-carry-conf").textContent = stock.confidence || "—";
                var mat = row.querySelector(".dc-carry-maturity");
                if (mat) mat.innerHTML = maturityBadgeHtml(stock.maturity_tag, stock.consecutive_days_on_list);
            });
            var carrySyms = {};
            carry.forEach(function (s) { carrySyms[s.symbol] = true; });
            Array.prototype.slice.call(coGrid.children).forEach(function (ch) {
                if (!carrySyms[ch.dataset.symbol]) coGrid.removeChild(ch);
            });
        } else {
            coSec.hidden = true;
            coGrid.innerHTML = "";
        }

        renderLiveSetups();

        if (modalSymbol) renderModal(currentStock(modalSymbol));
    }

    function applyState(s) {
        if (!s) return;
        if (s.error) { toast("Error: " + s.error); return; }
        state = s;
        try { localStorage.setItem(lsKey(), JSON.stringify(s)); } catch (e) {}
        render();
    }

    // ---- modal ----
    function openModal(symbol) {
        var stock = currentStock(symbol);
        if (stock && stock.is_carryover) return;
        modalSymbol = symbol;
        $("dcModal").hidden = false;
        $("dcModal").setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
        if (stock && stock.is_preview) {
            renderModal(stock);
            return;
        }
        toast("Refreshing from RS…");
        api("/sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ symbol: symbol })
        }).then(function (s) {
            applyState(s);
            renderModal(currentStock(symbol));
        }).catch(function (e) {
            renderModal(currentStock(symbol));
        });
    }

    function closeModal() {
        modalSymbol = null;
        lastAdxRecheckAlertKey = null;
        $("dcModal").hidden = true;
        $("dcModal").setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
    }

    function renderModal(stock) {
        if (!stock) return;
        $("dcModalTitle").textContent = stock.symbol + " · " + stock.direction;
        var sub = [];
        if (stock.rs_pct != null) sub.push("RS " + (stock.rs_pct > 0 ? "+" : "") + Number(stock.rs_pct).toFixed(2) + "%");
        if (stock.dashboard_score != null) sub.push("Score " + stock.dashboard_score);
        if (stock.vol_multiplier != null) sub.push("Vol " + Number(stock.vol_multiplier).toFixed(2) + "×");
        $("dcModalSub").textContent = sub.join(" · ");

        var body = $("dcModalBody");
        body.innerHTML = "";

        var grid = el("div", "dc-modal-grid");

        // recheck banner — only in 10-min windows before 10:00 / 10:30 IST
        var rc = el("div", "dc-recheck dc-modal-span2");
        if (stock.adx_935_status === "recheck") {
            var alert = adxRecheckAlert(nowIST().minutes);
            if (alert.show) {
                rc.classList.add("show");
                rc.textContent = alert.text;
                if (alert.flash) rc.classList.add("flash");
            }
        }
        grid.appendChild(rc);

        var preTitle = el("div", "dc-group-title dc-modal-span2", "Pre-market");
        grid.appendChild(preTitle);
        grid.appendChild(buildNewsItem(stock));
        grid.appendChild(buildAdx935Item(stock));
        grid.appendChild(buildMaturityItem(stock));

        if (stock.quality_display) {
            var qrow = el("div", "dc-quality-row dc-modal-span2");
            qrow.innerHTML = "<strong>Quality</strong> " + stock.quality_display;
            grid.appendChild(qrow);
        }
        if (stock.live_rs_direction) {
            var live = el("div", "dc-live-rs dc-modal-span2");
            live.textContent = "Live RS direction: " + stock.live_rs_direction +
                (stock.live_rs_updated_at ? " (as of " + fmtDataAsOf(stock.live_rs_updated_at) + ")" : "");
            grid.appendChild(live);
        }
        if (stock.carryover_warning) {
            grid.appendChild(el("div", "dc-carryover-chip dc-modal-span2", "⚠ CARRYOVER — not on today's 09:25 fresh scan"));
        }
        var setupSt = (stock.setup_state || "NEUTRAL").toUpperCase();
        if (setupSt !== "NEUTRAL" && setupSt !== "EXPIRED") {
            var setupRow = el("div", "dc-setup-row dc-modal-span2");
            setupRow.textContent = "Setup radar: " + setupSt.replace("_", "·");
            if (stock.sl_pct != null) setupRow.textContent += " · SL " + Number(stock.sl_pct).toFixed(2) + "%";
            grid.appendChild(setupRow);
        }
        if (stock.grade_gate_locked) {
            grid.appendChild(el("div", "dc-grade-lock-banner dc-modal-span2",
                "🔒 Setup live but grade gate failed — wait for A-grade or GO section"));
        }

        var gateTitle = el("div", "dc-group-title dc-modal-span2", "Entry gate (auto from RS scanner)");
        grid.appendChild(gateTitle);

        AUTO_FIELDS.forEach(function (field) {
            grid.appendChild(buildAutoItem(field, stock));
        });

        // Counter-RS — full width
        var cr = el("label", "dc-counter dc-modal-span2");
        var cb = el("input"); cb.type = "checkbox";
        cb.checked = !!stock.counter_rs;
        cb.addEventListener("change", function () { onChange(stock.symbol, "counter_rs", cb.checked); });
        cr.appendChild(cb);
        cr.appendChild(el("span", null, "Counter-RS direction? (A-grade mandatory)"));
        grid.appendChild(cr);

        // Progress + decision — full width
        var gs = Number(stock.gate_score || 0);
        var pw = el("div", "dc-progress-wrap dc-modal-span2");
        pw.appendChild(el("div", "dc-progress-label", gs + " / 9 entry conditions met"));
        var pbar = el("div", "dc-progress");
        var pfill = el("div", "dc-progress-fill");
        pfill.style.width = Math.round((gs / 9) * 100) + "%";
        pbar.appendChild(pfill);
        pw.appendChild(pbar);
        grid.appendChild(pw);

        var dec = el("div", "dc-modal-decision dc-decision dc-decision--" + decisionClass(stock) + " dc-modal-span2");
        dec.textContent = stock.decision || "⬜ Not assessed";
        grid.appendChild(dec);

        if (stock.eligibility_note) {
            grid.appendChild(el("div", "dc-eligibility-note dc-modal-span2", stock.eligibility_note));
        }

        var notes = el("textarea", "dc-notes dc-modal-span2");
        notes.placeholder = "Trade notes…";
        notes.value = stock.notes || "";
        notes.addEventListener("input", function () { onChange(stock.symbol, "notes", notes.value); });
        grid.appendChild(notes);

        if (stock.updated_at) {
            var d = new Date(stock.updated_at);
            grid.appendChild(el("div", "dc-saved dc-modal-span2",
                "Last saved: " + ("0" + d.getHours()).slice(-2) + ":" +
                ("0" + d.getMinutes()).slice(-2) + ":" + ("0" + d.getSeconds()).slice(-2)));
        }

        body.appendChild(grid);
    }

    function buildAutoItem(field, stock) {
        var it = el("div", "dc-item");
        var lab = el("div", "dc-item-label");
        lab.appendChild(el("span", null, AUTO_LABELS[field] || field));
        lab.appendChild(el("span", "dc-sys-badge", "System"));
        var hint = el("span", "dc-item-hint");
        var h = hintFor(field, stock);
        if (h) { hint.textContent = h.text; hint.className = "dc-item-hint " + h.cls; }
        lab.appendChild(hint);
        it.appendChild(lab);
        var val = el("div", "dc-auto-val " + autoValClass(field, stock));
        val.textContent = stock[field] == null ? "—" : String(stock[field]);
        it.appendChild(val);
        return it;
    }

    function buildNewsItem(stock) {
        var it = el("div", "dc-item");
        it.appendChild(el("div", "dc-item-label", "News Clean?"));
        var row = el("div", "dc-toggle-row");
        [["CLEAN", "true"], ["ADVERSE NEWS", "false"]].forEach(function (pair) {
            var b = el("button", "dc-toggle", pair[0]);
            b.type = "button";
            if (stock.news_clean === (pair[1] === "true")) {
                b.classList.add(pair[1] === "true" ? "sel-pass" : "sel-fail");
            }
            b.addEventListener("click", function () {
                var cur = currentStock(stock.symbol);
                var isSel = cur && cur.news_clean === (pair[1] === "true");
                onChange(stock.symbol, "news_clean", isSel ? "" : pair[1]);
            });
            row.appendChild(b);
        });
        it.appendChild(row);
        return it;
    }

    function buildAdx935Item(stock) {
        var it = el("div", "dc-item");
        var lab = el("div", "dc-item-label");
        lab.appendChild(el("span", null, "ADX at 9:35 AM"));
        lab.appendChild(el("span", "dc-sys-badge", "Override"));
        var hint = el("span", "dc-item-hint");
        var h = hintFor("adx_935", stock);
        if (h) { hint.textContent = h.text; hint.className = "dc-item-hint " + h.cls; }
        lab.appendChild(hint);
        it.appendChild(lab);
        var inp = el("input", "dc-num"); inp.type = "number"; inp.step = "0.01"; inp.inputMode = "decimal";
        inp.placeholder = "TradingView 9:35 close";
        inp.value = stock.adx_935 == null ? "" : stock.adx_935;
        inp.addEventListener("input", function () { onChange(stock.symbol, "adx_935", inp.value); });
        it.appendChild(inp);
        return it;
    }

    function buildMaturityItem(stock) {
        var it = el("div", "dc-item");
        var lab = el("div", "dc-item-label");
        lab.appendChild(el("span", null, "Maturity"));
        lab.appendChild(el("span", "dc-sys-badge", "System"));
        it.appendChild(lab);
        var val = el("div", "dc-auto-val neutral");
        val.innerHTML = maturityBadgeHtml(stock.maturity_tag, stock.consecutive_days_on_list);
        it.appendChild(val);
        return it;
    }

    // ---- updates ----
    function onChange(symbol, field, value) {
        var stock = currentStock(symbol);
        if (stock) {
            if (field === "counter_rs") stock[field] = !!value;
            else if (field === "news_clean") stock[field] = value === "" ? null : (value === "true" || value === true);
            else stock[field] = value === "" ? null : value;
        }
        var key = (symbol || "_page") + "|" + field;
        clearTimeout(saveTimers[key]);
        saveTimers[key] = setTimeout(function () {
            api("/update", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ symbol: symbol, field: field, value: value })
            }).then(applyState).catch(function () { toast("Save failed"); });
        }, 500);
    }

    function pull() {
        toast("Refreshing Kavach data for locked watchlist…");
        api("/refresh", { method: "POST" }).then(function (s) {
            applyState(s);
            if (s.refresh_status === "no_lock") {
                toast(s.refresh_message || "Morning snapshot not yet taken");
            } else if (s.refresh_status === "ok") {
                toast("Watchlist updated");
            }
        }).catch(function () { toast("Refresh failed"); });
    }

    function resetDay() {
        if (!confirm("Reset today's checklist and morning snapshot lock? All saved values will be cleared.")) return;
        api("/reset", { method: "POST" })
            .then(function (s) {
                cardEls = {};
                $("dcBullGrid").innerHTML = "";
                $("dcBearGrid").innerHTML = "";
                closeModal();
                applyState(s);
                toast("Day reset");
            });
    }

    function tickClock() {
        var t = nowIST();
        $("dcClock").textContent = t.str;
        var w = $("dcWindow");
        var start = 9 * 60 + 45, end = 14 * 60 + 30;
        if (t.minutes < start) {
            var rem = (start * 60) - t.secs;
            w.textContent = "Entry opens in " + ("0" + Math.floor(rem / 60)).slice(-2) + ":" + ("0" + (rem % 60)).slice(-2);
            w.className = "dc-window pre";
        } else if (t.minutes <= end) {
            w.textContent = "Entry window open";
            w.className = "dc-window open";
        } else {
            w.textContent = "Entry window closed";
            w.className = "dc-window closed";
        }
        if (modalSymbol) {
            var stock = currentStock(modalSymbol);
            if (stock && stock.adx_935_status === "recheck") {
                var alert = adxRecheckAlert(t.minutes);
                var key = alert.show ? alert.text + (alert.flash ? "|flash" : "") : "hidden";
                if (key !== lastAdxRecheckAlertKey) {
                    lastAdxRecheckAlertKey = key;
                    renderModal(stock);
                }
            } else {
                lastAdxRecheckAlertKey = null;
            }
        }
    }

    function boot() {
        try {
            var cached = localStorage.getItem("dc_state_" + new Date().toISOString().slice(0, 10));
            if (cached) { state = JSON.parse(cached); render(); }
        } catch (e) {}

        $("dcNiftyDir").addEventListener("change", function () {
            onChange("", "nifty_open_direction", this.value);
        });
        $("dcPull").addEventListener("click", pull);
        $("dcPullEmpty").addEventListener("click", pull);
        $("dcReset").addEventListener("click", resetDay);
        $("dcPrint").addEventListener("click", function () { window.print(); });
        $("dcSave").addEventListener("click", function () {
            try { localStorage.setItem(lsKey(), JSON.stringify(state)); } catch (e) {}
            toast("Session saved");
        });
        $("dcModalClose").addEventListener("click", closeModal);
        $("dcModalBackdrop").addEventListener("click", closeModal);
        $("dcCarryoverToggle").addEventListener("click", function () {
            var body = $("dcCarryoverBody");
            var open = body.hidden;
            body.hidden = !open;
            this.setAttribute("aria-expanded", open ? "true" : "false");
            this.querySelector(".dc-carryover-chevron").classList.toggle("dc-carryover-chevron--open", open);
        });

        api("/data").then(function (s) {
            if (s.locked && (!s.stocks || s.stocks.length === 0)) {
                return api("/refresh", { method: "POST" });
            }
            if (!s.locked && atOrAfter925() && (!s.preview || s.preview.length === 0)) {
                return api("/refresh", { method: "POST" });
            }
            return s;
        }).then(applyState).catch(function () { $("dcEmpty").querySelector("p").textContent = "Could not load checklist."; });

        function atOrAfter925() {
            var t = nowIST();
            return t.minutes >= 9 * 60 + 25;
        }

        tickClock();
        setInterval(tickClock, 1000);
        setInterval(function () {
            api("/data").then(applyState).catch(function () {});
        }, 60000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else { boot(); }
})();
