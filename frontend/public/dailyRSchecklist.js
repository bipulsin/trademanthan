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
    var lastGoAlertKey = null;
    var goAlertEnabled = false;

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
    var TRADE_STATE_ORDER = {
        "READY": 0,
        "READY(RECHECK)": 1,
        "WAIT FOR PULLBACK": 2,
        "SCANNING": 2,
        "EXPIRED": 3,
        "BLOCKED": 4
    };
    var GRADE_ORDER = { "A+": 0, "A": 1, "B": 2, "C": 3, "D": 4 };

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

    function fmtGoTime(iso) {
        if (!iso) return "";
        try {
            var d = new Date(iso);
            return new Intl.DateTimeFormat("en-GB", {
                timeZone: "Asia/Kolkata", hour: "2-digit", minute: "2-digit", hour12: false
            }).format(d);
        } catch (e) { return ""; }
    }

    function fmtFwElapsed(fw) {
        var t = fmtGoTime(fw.first_flip_at);
        var mins = fw.minutes_since_flip != null ? fw.minutes_since_flip : 0;
        if (!t) return "";
        return "first flip " + t + " · " + mins + " min ago";
    }

    function fwMomentumLabel(m) {
        if (m === "rising") return "↑ rising";
        if (m === "fading") return "↓ fading";
        return "→ flat";
    }

    function normalizeFastWatch(fw) {
        if (!fw) return { featured: { long: [], short: [] }, all: [], total_count: 0 };
        if (Array.isArray(fw)) {
            var longs = fw.filter(function (x) { return (x.direction || "LONG") !== "SHORT"; });
            var shorts = fw.filter(function (x) { return (x.direction || "LONG") === "SHORT"; });
            return { featured: { long: longs, short: shorts }, all: fw, total_count: fw.length };
        }
        return {
            featured: fw.featured || { long: [], short: [] },
            all: fw.all || [],
            total_count: fw.total_count != null ? fw.total_count : (fw.all || []).length,
        };
    }

    function buildFastWatchCard(fw) {
        var card = el("div", "dc-fast-watch-card dc-fast-watch-card--" +
            (fw.direction === "SHORT" ? "short" : "long"));
        var title = el("strong");
        title.textContent = fw.symbol || "?";
        card.appendChild(title);
        if (fw.is_reversal) {
            var rev = el("span", "dc-fw-reversal");
            rev.textContent = "REVERSAL";
            card.appendChild(rev);
        }
        var mom = el("span", "dc-fw-momentum dc-fw-momentum--" + (fw.momentum || "flat"));
        mom.textContent = fwMomentumLabel(fw.momentum);
        card.appendChild(mom);
        card.appendChild(document.createTextNode(
            " · " + (fw.kavach_state || fw.live_kavach || "?") +
            (fw.confidence_grade || fw.live_grade ? " · " + (fw.confidence_grade || fw.live_grade) : "") +
            (fw.trade_score != null ? " · Score " + fw.trade_score : "") +
            " · " + (fw.direction === "SHORT" ? "SHORT" : "LONG") + " · " + fmtFwElapsed(fw)
        ));
        return card;
    }

    var fastWatchExpanded = false;

    function stickyCountdownSec(untilIso) {
        if (!untilIso) return 0;
        try {
            var end = new Date(untilIso).getTime();
            return Math.max(0, Math.floor((end - Date.now()) / 1000));
        } catch (e) { return 0; }
    }

    function playGoAlert() {
        if (!goAlertEnabled) return;
        try {
            var ctx = new (window.AudioContext || window.webkitAudioContext)();
            [880, 1100].forEach(function (freq, i) {
                var o = ctx.createOscillator();
                var g = ctx.createGain();
                o.frequency.value = freq;
                g.gain.value = 0.08;
                o.connect(g);
                g.connect(ctx.destination);
                o.start(ctx.currentTime + i * 0.15);
                o.stop(ctx.currentTime + i * 0.15 + 0.12);
            });
        } catch (e) { /* muted */ }
    }

    function checkGoAlerts(stocks) {
        if (!goAlertEnabled || !stocks) return;
        stocks.forEach(function (s) {
            if (s.section !== "GO" || !s.go_enter_first_at) return;
            var key = s.symbol + "|" + s.go_enter_first_at;
            if (lastGoAlertKey === key) return;
            lastGoAlertKey = key;
            playGoAlert();
        });
    }

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
        node.addEventListener("click", function (ev) {
            if (ev.target.closest && ev.target.closest(".dc-take-trade")) return;
            openModal(symbol);
        });
        node.addEventListener("keydown", function (ev) {
            if (ev.key === "Enter" || ev.key === " ") {
                if (ev.target.closest && ev.target.closest(".dc-take-trade")) return;
                openModal(symbol);
            }
        });
        var takeBtn = node.querySelector(".dc-take-trade");
        if (takeBtn) {
            takeBtn.addEventListener("click", function (ev) {
                ev.preventDefault();
                ev.stopPropagation();
                takeTrade(symbol);
            });
        }
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
        if (stock.trade_taken) card.classList.add("dc-card--taken");
        if (stock.carryover_warning) card.classList.add("dc-card--carryover");
        else card.classList.remove("dc-card--carryover");
        if ((stock.decision || "").indexOf("CHART REVERSED") >= 0) card.classList.add("dc-card--reversed");
        else card.classList.remove("dc-card--reversed");
        card.querySelector(".dc-symbol").textContent = stock.symbol;
        var persEl = card.querySelector(".dc-persist");
        if (persEl) {
            var frac = stock.persistence_top5_frac;
            if (frac == null || frac === "") {
                persEl.textContent = "";
                persEl.style.display = "none";
            } else {
                persEl.textContent = "P" + Math.round(Number(frac) * 100) + "%";
                persEl.style.display = "";
                persEl.title = "Top-5 persistence since lock: " + Math.round(Number(frac) * 100) +
                    "%" + (stock.persistence_clean_bars != null ? (" · " + stock.persistence_clean_bars + " clean VWAP bars") : "");
            }
        }
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
        var ignEl = card.querySelector(".dc-ignition");
        if (ignEl) {
            if (stock.ignition_building) {
                ignEl.hidden = false;
                ignEl.textContent = "Ignition Building" +
                    (stock.ignition_score != null ? " · " + Math.round(stock.ignition_score) : "");
            } else {
                ignEl.hidden = true;
            }
        }
        patchTradeRow(card, stock);
        var takeBtn = card.querySelector(".dc-take-trade");
        var takenLbl = card.querySelector(".dc-trade-taken-label");
        if (takeBtn) {
            var isBull = (stock.direction || "LONG") !== "SHORT";
            takeBtn.className = "dc-take-trade " + (isBull ? "dc-take-trade--long" : "dc-take-trade--short");
            if (stock.trade_taken) {
                takeBtn.disabled = true;
                takeBtn.title = "Position already open in Open Trades panel";
            } else if (stock.stopped_out_today || stock.trade_exited || stock.trade_state === "BLOCKED") {
                takeBtn.disabled = true;
                takeBtn.title = stock.trade_state_reason || "Blocked — no re-entry today";
            } else if (stock.trade_take_enabled === false || stock.trade_state === "SCANNING") {
                takeBtn.disabled = true;
                takeBtn.title = stock.trade_state_reason || "Take Trade from 09:45 IST";
            } else if (!isReadyState(stock.trade_state)) {
                takeBtn.disabled = true;
                takeBtn.title = stock.trade_state_reason || "Not READY";
            } else {
                takeBtn.disabled = false;
                takeBtn.title = "Mark trade taken";
            }
        }
        if (takenLbl) {
            if (stock.trade_taken) {
                takenLbl.hidden = false;
                takenLbl.textContent = stock.trade_taken_label || "Trade taken · see Open Trades";
            } else if (stock.trade_exited) {
                takenLbl.hidden = false;
                takenLbl.textContent = stock.trade_exited_label || "Exited";
            } else {
                takenLbl.hidden = true;
                takenLbl.textContent = "";
            }
        }
        var gt = card.querySelector(".dc-go-timing");
        var meta = card.querySelector(".dc-card-meta");
        if (gt) {
            if (dcls === "GO" && stock.go_enter_first_at) {
                var parts = ["GO @ " + fmtGoTime(stock.go_enter_first_at)];
                if (stock.go_sticky_active && stock.go_sticky_until) {
                    var rem = stickyCountdownSec(stock.go_sticky_until);
                    parts.push("sticky " + ("0" + Math.floor(rem / 60)).slice(-2) + ":" + ("0" + (rem % 60)).slice(-2));
                }
                if (stock.indicator_stale) parts.push("⚠ stale");
                gt.textContent = parts.join(" · ");
                gt.hidden = false;
                if (meta) meta.hidden = false;
            } else {
                gt.hidden = true;
                gt.textContent = "";
                if (meta) meta.hidden = true;
            }
        }
    }

    function fmtInr(n) {
        if (n == null || n === "") return "—";
        var v = Math.round(Number(n));
        return "₹" + v.toLocaleString("en-IN");
    }

    function fmtPx(n) {
        if (n == null || n === "") return "—";
        return Number(n).toFixed(2);
    }

    function fmtPromotedAt(iso) {
        if (!iso) return "";
        var d = new Date(iso);
        if (isNaN(d.getTime())) return "";
        return ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2);
    }

    function tradeStateClass(st) {
        if (st === "READY") return "dc-tstate--ready";
        if (st === "READY(RECHECK)") return "dc-tstate--recheck";
        if (st === "WAIT FOR PULLBACK") return "dc-tstate--wait";
        if (st === "SCANNING") return "dc-tstate--scanning";
        if (st === "EXPIRED") return "dc-tstate--expired";
        if (st === "BLOCKED") return "dc-tstate--blocked";
        if (st === "CHART REVERSED") return "dc-tstate--reversed";
        return "";
    }

    function patchTradeRow(card, stock) {
        var row = card.querySelector(".dc-trade-row");
        if (!row) return;
        var st = stock.trade_state;
        if (!st) {
            row.hidden = true;
            card.classList.remove("dc-card--expired");
            return;
        }
        row.hidden = false;
        card.classList.toggle("dc-card--expired", st === "EXPIRED");

        var stEl = row.querySelector(".dc-trade-state");
        if (stEl) {
            var label = st;
            if (st === "READY(RECHECK)" && stock.trade_adx != null) {
                label = "READY(RECHECK) · ADX " + stock.trade_adx;
            }
            if (st === "BLOCKED" && stock.trade_state_reason) {
                label = stock.trade_state_reason;
            }
            stEl.textContent = label;
            stEl.className = "dc-trade-state " + tradeStateClass(st);
            stEl.title = stock.trade_state_reason || st;
        }

        var en = row.querySelector(".dc-trade-entry");
        if (en) {
            en.textContent = (st === "EXPIRED" || st === "BLOCKED")
                ? "Entry —"
                : ("Entry " + fmtPx(stock.trade_entry));
        }
        var sl = row.querySelector(".dc-trade-sl");
        if (sl) {
            var riskTxt = stock.trade_risk_inr != null ? fmtInr(stock.trade_risk_inr) : "—";
            sl.innerHTML = "SL " + fmtPx(stock.trade_sl) +
                ' · <span class="dc-trade-risk' + (stock.trade_risk_over ? " dc-trade-risk--over" : "") + '">' +
                riskTxt + "</span>";
        }
        var rr = row.querySelector(".dc-trade-rr");
        if (rr) {
            if (stock.trade_rr_label) {
                rr.textContent = stock.trade_rr_label + (stock.trade_rr_low ? " R:R low" : "");
                rr.className = "dc-trade-rr" + (stock.trade_rr_low ? " dc-trade-rr--low" : "");
            } else {
                rr.textContent = "R:R —";
                rr.className = "dc-trade-rr";
            }
        }
        var obs = row.querySelector(".dc-trade-obs");
        if (obs) {
            var bits = [];
            if (stock.promoted_at) bits.push("↗ " + fmtPromotedAt(stock.promoted_at));
            if (stock.lock_cycles > 1) bits.push("cycles " + stock.lock_cycles);
            obs.textContent = bits.join(" · ");
            obs.style.display = bits.length ? "" : "none";
        }
        var gates = row.querySelector(".dc-trade-gates");
        if (gates) {
            gates.innerHTML = renderGateBadgesHtml(stock.gate_badges || []);
        }
        var pos = row.querySelector(".dc-trade-pos");
        if (pos) {
            var p = stock.position;
            if (p && p.trail_state) {
                pos.hidden = false;
                var pnl = p.open_pnl_inr != null ? fmtInr(p.open_pnl_inr) : "—";
                var posTxt = p.trail_state + " · P&L " + pnl;
                if (p.trail_sl != null) posTxt += " · trail " + fmtPx(p.trail_sl);
                if (p.profit_locked && p.alt_exit_ema5 != null) {
                    posTxt += " · alt EMA5 " + fmtPx(p.alt_exit_ema5);
                }
                pos.textContent = posTxt;
                var pcls = "dc-trade-pos";
                if (p.trail_state === "BOOK-NOW") pcls += " dc-trade-pos--book";
                else if (p.profit_locked) pcls += " dc-trade-pos--locked";
                else pcls += " dc-trade-pos--hold";
                pos.className = pcls;
                pos.title = p.trail_reason || p.trail_state;
            } else {
                pos.hidden = true;
                pos.textContent = "";
            }
        }
    }

    function gateBadgeClass(t) {
        var cls = "dc-gate-badge";
        t = String(t || "");
        if (t.indexOf("WHIPSAW") >= 0) cls += " dc-gate-badge--whip";
        else if (t.indexOf("DIR CONFLICT") >= 0) cls += " dc-gate-badge--dirconflict";
        else if (t.indexOf("COUNTER-REGIME") >= 0) cls += " dc-gate-badge--counter";
        else if (t.indexOf("REGIME") >= 0) cls += " dc-gate-badge--regime";
        else if (t.indexOf("CHURN") >= 0) cls += " dc-gate-badge--churn";
        else if (t.indexOf("DIRECTION") >= 0 || t.indexOf("RE-ENTRY") >= 0) cls += " dc-gate-badge--flip";
        else if (t.indexOf("1st") >= 0) cls += " dc-gate-badge--pb1";
        else if (t.indexOf("2nd") >= 0) cls += " dc-gate-badge--pb2";
        else if (t.indexOf("pullback") >= 0) cls += " dc-gate-badge--pb3";
        else if (t.indexOf("CHOP") >= 0) cls += " dc-gate-badge--chop";
        else if (t.indexOf("CAP WAIVED") >= 0) cls += " dc-gate-badge--waiver";
        return cls;
    }

    function renderGateBadgesHtml(badges) {
        return (badges || []).map(function (b) {
            var t = String(b);
            return '<span class="' + gateBadgeClass(t) + '">' + t + "</span>";
        }).join("");
    }

    function gradeRank(stock) {
        var g = String(stock.confidence || stock.dashboard_kavach || "").toUpperCase().replace("*", "");
        if (g.indexOf("A+") === 0) return 0;
        if (g.indexOf("A") === 0) return 1;
        if (g.indexOf("B") === 0) return 2;
        if (g.indexOf("C") === 0) return 3;
        if (g.indexOf("D") === 0) return 4;
        return 9;
    }

    function sortStocks(list) {
        return list.slice().sort(function (a, b) {
            var ta = TRADE_STATE_ORDER[a.trade_state];
            var tb = TRADE_STATE_ORDER[b.trade_state];
            if (ta != null || tb != null) {
                ta = ta != null ? ta : 9;
                tb = tb != null ? tb : 9;
                if (ta !== tb) return ta - tb;
                var ga = gradeRank(a);
                var gb = gradeRank(b);
                if (ga !== gb) return ga - gb;
                return (a.rs_pct == null ? 99 : -Number(a.rs_pct)) - (b.rs_pct == null ? 99 : -Number(b.rs_pct));
            }
            var oa = SECTION_ORDER[decisionClass(a)] != null ? SECTION_ORDER[decisionClass(a)] : 1;
            var ob = SECTION_ORDER[decisionClass(b)] != null ? SECTION_ORDER[decisionClass(b)] : 1;
            if (oa !== ob) return oa - ob;
            return (b.rs_pct || 0) - (a.rs_pct || 0);
        });
    }

    function renderTradeObs() {
        var warn = $("dcTradeChurnWarn");
        var strip = $("dcRemovalsStrip");
        var chips = $("dcRemovalsChips");
        var remCount = $("dcRemovalsCount");
        var obs = (state && state.trade_state_obs) || {};
        var regimeEl = $("dcMktRegime");
        var exitEl = $("dcExitRule");
        var sessEl = $("dcSessionWindow");
        if (regimeEl) {
            var reg = obs.market_regime || "—";
            var label = obs.market_regime_label || "";
            if (label && label.toUpperCase().indexOf(String(reg).toUpperCase()) === 0) {
                regimeEl.textContent = label;
            } else if (label) {
                regimeEl.textContent = reg + " · " + label;
            } else {
                regimeEl.textContent = reg;
            }
            regimeEl.className = "dc-mkt-regime dc-mkt-regime--" + String(reg).toLowerCase();
            regimeEl.title = (obs.chop_reasons || []).join("; ") || label || "";
        }
        if (exitEl) {
            exitEl.textContent = obs.exit_rule_reminder ||
                "Exit rule: 10m close beyond EMA10 reverse — not VWAP break";
        }
        if (sessEl) {
            sessEl.textContent = obs.session_window_text || "Entry 09:45–14:30 · Square-off 15:15";
        }
        var rotChip = $("dcRotationChip");
        if (rotChip) {
            var rc = obs.rotation_chip;
            rotChip.hidden = !(rc && rc.active);
            if (rc && rc.active) {
                rotChip.textContent = rc.label || "ROTATION DAY";
                rotChip.title = rc.subtitle || "";
            }
        }
        var imbChip = $("dcImbalanceChip");
        if (imbChip) {
            var imb = obs.direction_imbalance;
            imbChip.hidden = !(imb && imb.active);
            if (imb && imb.active) imbChip.textContent = imb.label || "";
        }
        var compChip = $("dcCompromisedChip");
        if (compChip) {
            var comp = obs.compromised_lock;
            compChip.hidden = !(comp && comp.active);
            if (comp && comp.active) {
                compChip.textContent = "⚠ Manual lock recovery";
                compChip.title = comp.label || "";
            }
        }
        if (warn) {
            warn.hidden = !obs.churn_warning;
            if (obs.churn_warning && obs.churn_symbols && obs.churn_symbols.length) {
                warn.textContent = "Lock churn elevated (" + obs.churn_count +
                    " symbols with cycles > 1: " + obs.churn_symbols.join(", ") +
                    "). Check promotion / R1·R2 behaviour.";
            }
        }
        if (!strip || !chips) return;
        var rem = obs.recent_removals || [];
        if (remCount) remCount.textContent = String(rem.length);
        if (!rem.length) {
            strip.hidden = true;
            chips.innerHTML = "";
            return;
        }
        chips.innerHTML = rem.map(function (r) {
            var t = "";
            if (r.at) {
                var d = new Date(r.at);
                if (!isNaN(d.getTime())) t = ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2);
            }
            return '<span class="dc-removal-chip dc-removal-chip--' +
                String(r.rule_tag || "").toLowerCase() + '">' +
                r.symbol + " · " + (r.rule_tag || "—") + (t ? " @" + t : "") + "</span>";
        }).join("");
        // keep collapsed unless expanded
        var tog = $("dcRemovalsToggle");
        if (tog && tog.getAttribute("aria-expanded") === "true") strip.hidden = false;
        else strip.hidden = true;
    }

    function isReadyState(st) {
        return st === "READY" || st === "READY(RECHECK)";
    }

    function isZone3Card(st) {
        return isReadyState(st) || st === "EXPIRED";
    }

    function nextTenMinBoundaryFromSecs(secs) {
        // Kavach 10m closes: minutes ending in 5
        var m = Math.floor(secs / 60) % (24 * 60);
        var minute = m % 60;
        var hour = Math.floor(m / 60);
        var targets = [5, 15, 25, 35, 45, 55];
        var i, t;
        for (i = 0; i < targets.length; i++) {
            t = targets[i];
            if (minute < t) return { hour: hour, minute: t, dayMin: hour * 60 + t };
        }
        hour = (hour + 1) % 24;
        return { hour: hour, minute: 5, dayMin: hour * 60 + 5 + (hour === 0 ? 24 * 60 : 0) };
    }

    function secsToNextTenMin() {
        var n = nowIST();
        var b = nextTenMinBoundaryFromSecs(n.secs);
        var targetSecs = b.dayMin * 60;
        if (targetSecs <= n.secs) targetSecs += 24 * 3600;
        return Math.max(0, targetSecs - n.secs);
    }

    function readyWindowKey(sym) {
        return "dc_ready_win_" + ((state && state.session_date) || "") + "_" + sym;
    }

    function getReadyWindowMeta(sym, stock) {
        var key = readyWindowKey(sym);
        var meta = null;
        try { meta = JSON.parse(sessionStorage.getItem(key) || "null"); } catch (e) { meta = null; }
        var st = stock.trade_state;
        if (!isReadyState(st)) {
            try { sessionStorage.removeItem(key); } catch (e) {}
            return null;
        }
        var nowSec = Math.floor(Date.now() / 1000);
        if (!meta || meta.state !== st) {
            meta = { state: st, startedAt: nowSec, attempt: (meta && meta.attempt) || 1, missed: false, startBoundary: secsToNextTenMin() };
            try { sessionStorage.setItem(key, JSON.stringify(meta)); } catch (e) {}
        }
        var remaining = secsToNextTenMin();
        // Crossed into a new 10m slot since start → missed until early in next slot
        if (!meta.missed && meta.startBoundary != null && remaining > meta.startBoundary + 30) {
            meta.missed = true;
            meta.attempt = (meta.attempt || 1) + 1;
            try { sessionStorage.setItem(key, JSON.stringify(meta)); } catch (e) {}
        }
        if (meta.missed && remaining > 9 * 60) {
            meta.missed = false;
            meta.startedAt = nowSec;
            meta.startBoundary = remaining;
            try { sessionStorage.setItem(key, JSON.stringify(meta)); } catch (e) {}
        }
        return { remaining: remaining, missed: !!meta.missed, attempt: meta.attempt || 1, active: !meta.missed };
    }

    function oneWordReason(stock) {
        var r = String(stock.trade_state_reason || "").toLowerCase();
        var d = String(stock.decision || "").toUpperCase();
        if (d.indexOf("CHART REVERSED") >= 0) return "chart reversed";
        if (r.indexOf("whip") >= 0) return "whipsawed";
        if (r.indexOf("extend") >= 0) return "extended";
        if (r.indexOf("risk") >= 0) return "risk high";
        if (r.indexOf("sl") >= 0 || stock.stopped_out_today) return "SL earlier today";
        if (r.indexOf("direction conflict") >= 0 || r.indexOf("dir conflict") >= 0) return "dir conflict";
        if (r.indexOf("unstable") >= 0 || stock.direction_unstable) return "direction unstable";
        if (r.indexOf("manual") >= 0 || stock.zone_downgrade === "compromised_lock") return "caution";
        if (stock.trade_state === "WAIT FOR PULLBACK") return "wait pullback";
        if (stock.trade_state === "SCANNING") return "scanning";
        if (stock.gate_badges && stock.gate_badges.indexOf("DIR CONFLICT") >= 0) return "dir conflict";
        if (stock.trade_state === "BLOCKED") return "blocked";
        if (stock.trade_state === "EXPIRED") return "expired";
        return (stock.trade_state_reason || "").split(/[·—-]/)[0].trim().slice(0, 24) || "";
    }

    function patchReadyCard(card, stock) {
        var sym = stock.symbol;
        card.dataset.symbol = sym;
        card.querySelector(".dc-ready-symbol").textContent = sym;
        var dir = (stock.direction || "LONG").toUpperCase();
        var dirEl = card.querySelector(".dc-ready-dir");
        dirEl.textContent = dir === "SHORT" ? "SHORT" : "LONG";
        dirEl.className = "dc-ready-dir dc-ready-dir--" + (dir === "SHORT" ? "short" : "long");
        var entry = stock.trade_entry;
        var sl = stock.trade_sl;
        card.querySelector(".dc-ready-entry").textContent =
            entry != null ? Number(entry).toFixed(2) : "—";
        card.querySelector(".dc-ready-sl").textContent = sl != null ? "SL " + Number(sl).toFixed(2) : "SL —";
        var risk = stock.trade_risk_inr;
        var riskEl = card.querySelector(".dc-ready-risk");
        riskEl.textContent = risk != null ? "Risk ₹" + Math.abs(Number(risk)).toLocaleString("en-IN") : "Risk —";
        riskEl.classList.toggle("dc-ready-risk--over", !!stock.trade_risk_cap_flag);
        card.classList.toggle("dc-ready-card--risk-over", !!stock.trade_risk_cap_flag);
        card.querySelector(".dc-ready-rr").textContent = stock.trade_rr_label || "";

        var expEl = card.querySelector(".dc-ready-expiry");
        var expPx = stock.trade_expiry_price;
        var atrN = stock.trade_expiry_atr != null ? Number(stock.trade_expiry_atr) : 1.5;
        if (expEl) {
            expEl.textContent = expPx != null
                ? ("Invalidation (not SL): price beyond ₹" +
                    Number(expPx).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) +
                    " · " + atrN + " ATR from EMA5 entry")
                : "";
        }
        var waivedEl = card.querySelector(".dc-ready-waiver");
        if (waivedEl) {
            if (stock.trade_risk_cap_waived && stock.trade_risk_cap_waiver_label) {
                waivedEl.hidden = false;
                waivedEl.textContent = stock.trade_risk_cap_waiver_label;
            } else {
                waivedEl.hidden = true;
                waivedEl.textContent = "";
            }
        }
        var flagsEl = card.querySelector(".dc-ready-flags");
        if (flagsEl) {
            var show = rflags.length ? rflags.slice() : [];
            (stock.gate_badges || []).forEach(function (b) {
                var t = String(b);
                if (
                    t.indexOf("REGIME") >= 0
                    || t.indexOf("COUNTER") >= 0
                    || t.indexOf("CHURN") === 0
                    || t.indexOf("DIR CONFLICT") >= 0
                ) {
                    if (show.indexOf(t) < 0) show.push(t);
                }
            });
            flagsEl.innerHTML = renderGateBadgesHtml(show);
            flagsEl.hidden = !show.length;
        }
        var expired = stock.trade_state === "EXPIRED" || !!stock.trade_expiry_crossed;
        card.classList.toggle("dc-ready-card--expired", expired);
        var expLabel = card.querySelector(".dc-ready-expired-label");
        if (expLabel) expLabel.hidden = !expired;

        var grade = stock.confidence || stock.dashboard_kavach || "—";
        var rs = stock.rs_pct != null ? ((stock.rs_pct >= 0 ? "+" : "") + Number(stock.rs_pct).toFixed(2) + "%") : "";
        var pb = stock.pullback_label || "";
        card.querySelector(".dc-ready-meta").textContent = [grade, rs, pb].filter(Boolean).join(" · ");
        var win = getReadyWindowMeta(sym, stock);
        var timer = card.querySelector(".dc-ready-timer");
        var missedEl = card.querySelector(".dc-ready-missed");
        var recheck = card.querySelector(".dc-ready-recheck");
        var takeBtn = card.querySelector(".dc-ready-take");
        if (expired) {
            card.classList.remove("dc-ready-card--missed");
            if (missedEl) missedEl.hidden = true;
            takeBtn.disabled = true;
            timer.textContent = "";
            if (recheck) recheck.hidden = true;
        } else if (win && win.missed) {
            card.classList.add("dc-ready-card--missed");
            missedEl.hidden = false;
            var b = nextTenMinBoundaryFromSecs(nowIST().secs);
            missedEl.textContent = "MISSED WINDOW · re-evaluating at " +
                ("0" + b.hour).slice(-2) + ":" + ("0" + b.minute).slice(-2);
            takeBtn.disabled = true;
            timer.textContent = "";
        } else {
            card.classList.remove("dc-ready-card--missed");
            missedEl.hidden = true;
            takeBtn.disabled = !(
                stock.trade_take_enabled !== false
                && !stock.trade_taken
                && !stock.stopped_out_today
                && !stock.trade_exited
            );
            var rem = win ? win.remaining : secsToNextTenMin();
            var mm = Math.floor(rem / 60);
            var ss = rem % 60;
            timer.textContent = "Enter within " + mm + ":" + ("0" + ss).slice(-2);
        }
        if (!expired && win && win.attempt > 1 && !(win && win.missed)) {
            recheck.hidden = false;
            recheck.textContent = "Recheck confirmed · attempt " + win.attempt;
        } else if (recheck) {
            recheck.hidden = true;
        }
        takeBtn.onclick = function (e) {
            e.stopPropagation();
            if (takeBtn.disabled) return;
            takeTrade(sym);
        };
        card.onclick = function () { openModal(sym); };
    }

    function patchWatchRow(row, stock) {
        row.dataset.symbol = stock.symbol;
        var symEl = row.querySelector(".dc-watch-sym");
        symEl.textContent = stock.symbol;
        symEl.classList.toggle("dc-watch-sym--expired", stock.trade_state === "EXPIRED");
        var dir = (stock.direction || "LONG").toUpperCase();
        var dirEl = row.querySelector(".dc-watch-dir");
        dirEl.textContent = dir === "SHORT" ? "SHORT" : "LONG";
        dirEl.className = "dc-watch-dir dc-watch-dir--" + (dir === "SHORT" ? "short" : "long");
        var st = stock.trade_state || stock.section || "—";
        var stEl = row.querySelector(".dc-watch-state");
        stEl.textContent = st;
        stEl.className = "dc-watch-state " + tradeStateClass(st);
        if (String(stock.decision || "").indexOf("CHART REVERSED") >= 0) {
            stEl.className = "dc-watch-state dc-tstate--reversed";
            stEl.textContent = "CHART REVERSED";
        }
        row.querySelector(".dc-watch-reason").textContent = oneWordReason(stock);
        var wflags = row.querySelector(".dc-watch-flags");
        if (wflags) {
            var rf = (stock.regime_context && stock.regime_context.flags) || [];
            wflags.innerHTML = renderGateBadgesHtml(rf);
            wflags.hidden = !rf.length;
        }
        var grade = stock.confidence || stock.dashboard_kavach || "";
        var rs = stock.rs_pct != null ? ((stock.rs_pct >= 0 ? "+" : "") + Number(stock.rs_pct).toFixed(2) + "%") : "";
        row.querySelector(".dc-watch-meta").textContent = [rs, grade].filter(Boolean).join(" · ");
        row.onclick = function () { openModal(stock.symbol); };
    }

    function renderZones(stocks, preview) {
        var ready = sortStocks(stocks.filter(function (s) { return isZone3Card(s.trade_state); }));
        var watching = sortStocks(stocks.filter(function (s) { return !isZone3Card(s.trade_state); }));
        var z3 = $("dcZone3Grid");
        var z3empty = $("dcZone3Empty");
        var z4 = $("dcZone4List");
        if (!z3 || !z4) return;

        var readySyms = {};
        ready.forEach(function (stock) {
            readySyms[stock.symbol] = true;
            var card = z3.querySelector('.dc-ready-card[data-symbol="' + stock.symbol + '"]');
            if (!card) {
                card = $("dcReadyTpl").content.firstElementChild.cloneNode(true);
                z3.appendChild(card);
            }
            patchReadyCard(card, stock);
        });
        Array.prototype.slice.call(z3.querySelectorAll(".dc-ready-card")).forEach(function (ch) {
            if (!readySyms[ch.dataset.symbol]) z3.removeChild(ch);
        });
        if (z3empty) z3empty.hidden = ready.length > 0;

        var watchSyms = {};
        watching.forEach(function (stock) {
            watchSyms[stock.symbol] = true;
            var row = z4.querySelector('.dc-watch-row[data-symbol="' + stock.symbol + '"]');
            if (!row) {
                row = $("dcWatchTpl").content.firstElementChild.cloneNode(true);
                z4.appendChild(row);
            }
            patchWatchRow(row, stock);
        });
        Array.prototype.slice.call(z4.querySelectorAll(".dc-watch-row")).forEach(function (ch) {
            if (!watchSyms[ch.dataset.symbol]) z4.removeChild(ch);
        });
        // Detail lives in the modal (openModal → currentStock); no duplicate fat-card columns.
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
        var fiiSel = $("dcFiiDii");
        if (fiiSel && document.activeElement !== fiiSel) fiiSel.value = state.fii_dii_flow || "";
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
        renderZones(stocks, preview);
        // Keep pill counts from section decisions
        void bull; void bear;

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
        renderTradeObs();
        renderOpenTrades();
        renderGoBoard();
        renderFastWatch();
        checkGoAlerts(stocks);

        if (modalSymbol) renderModal(currentStock(modalSymbol));
    }

    function fillFastWatchStack(stackEl, items) {
        if (!stackEl) return;
        stackEl.innerHTML = "";
        (items || []).forEach(function (item) { stackEl.appendChild(buildFastWatchCard(item)); });
    }

    function renderGoBoard() {
        var wrap = $("dcGoBoard");
        var stack = $("dcGoBoardStack");
        var empty = $("dcGoBoardEmpty");
        var winEl = $("dcGoBoardWindow");
        if (!wrap || !stack) return;
        var cfg = (state && state.checklist_config) || {};
        var gb = (state && state.go_board) || {};
        var items = gb.symbols || [];
        if (!cfg.go_board_ui_enabled || !items.length) {
            wrap.hidden = true;
            stack.innerHTML = "";
            if (empty) empty.hidden = true;
            return;
        }
        wrap.hidden = false;
        if (winEl) winEl.textContent = gb.window ? ("Window " + gb.window) : "";
        stack.innerHTML = "";
        if (empty) empty.hidden = true;
        items.forEach(function (item) {
            var card = el("div", "dc-go-board-card dc-go-board-card--" + (item.side === "SHORT" ? "short" : "long"));
            card.innerHTML = "<strong>" + (item.symbol || "?") + "</strong>" +
                (item.is_reversal ? " <span class=\"dc-fw-reversal\">REVERSAL</span>" : "") +
                " · " + (item.kavach_state || "?") +
                " · Stop " + (item.stop_pct != null ? item.stop_pct + "%" : "—") +
                " · ₹" + (item.stop_inr_1lot != null ? item.stop_inr_1lot : "—") + " / lot" +
                (item.confidence_grade ? " · " + item.confidence_grade : "");
            stack.appendChild(card);
        });
    }

    function renderFastWatch() {
        var wrap = $("dcFastWatch");
        var bullStack = $("dcFastWatchBull");
        var bearStack = $("dcFastWatchBear");
        var expandBtn = $("dcFastWatchExpand");
        var allWrap = $("dcFastWatchAll");
        var allBull = $("dcFastWatchAllBull");
        var allBear = $("dcFastWatchAllBear");
        if (!wrap || !bullStack || !bearStack) return;
        var cfg = (state && state.checklist_config) || {};
        var fw = normalizeFastWatch(state && state.fast_watch);
        var longs = fw.featured.long || [];
        var shorts = fw.featured.short || [];
        var featured = longs.concat(shorts);
        if (!cfg.fast_watch_ui_enabled || !fw.total_count) {
            wrap.hidden = true;
            fillFastWatchStack(bullStack, []);
            fillFastWatchStack(bearStack, []);
            if (expandBtn) expandBtn.hidden = true;
            if (allWrap) { allWrap.hidden = true; }
            fillFastWatchStack(allBull, []);
            fillFastWatchStack(allBear, []);
            return;
        }
        wrap.hidden = false;
        fillFastWatchStack(bullStack, longs);
        fillFastWatchStack(bearStack, shorts);
        if (expandBtn) {
            var extra = fw.total_count - featured.length;
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
            if (fastWatchExpanded && fw.all && fw.all.length) {
                allWrap.hidden = false;
                var allLongs = fw.all.filter(function (x) { return (x.direction || "LONG") !== "SHORT"; });
                var allShorts = fw.all.filter(function (x) { return (x.direction || "LONG") === "SHORT"; });
                fillFastWatchStack(allBull, allLongs);
                fillFastWatchStack(allBear, allShorts);
            } else {
                allWrap.hidden = true;
                fillFastWatchStack(allBull, []);
                fillFastWatchStack(allBear, []);
            }
        }
    }

    var EXIT_REASONS = [
        "EMA10 reverse close (rule)",
        "EMA5 reverse close (profit protection)",
        "Risk cap exceeded",
        "Lock removed via R1",
        "Lock removed via R2",
        "Discretionary early exit",
        "15:15 square-off",
        "Session loss cap hit"
    ];
    var defaultDocTitle = document.title;
    var pendingAlarmTradeId = null;
    var exitAudio = null;

    function alarmPlayedKey(trade) {
        return "dc_alarm_" + trade.id + "_" + (trade.alarm_fired_at || "");
    }

    function playExitAlarm(trade) {
        if (!trade || !trade.alarm_fired_at) return;
        try {
            if (sessionStorage.getItem(alarmPlayedKey(trade))) return;
        } catch (e) { /* ignore */ }
        if (!exitAudio) {
            exitAudio = new Audio("audio/attention.mp3");
            exitAudio.volume = 1;
        }
        var p = exitAudio.play();
        if (p && p.then) {
            p.then(function () {
                try { sessionStorage.setItem(alarmPlayedKey(trade), "1"); } catch (e) {}
                pendingAlarmTradeId = null;
                var ban = $("dcExitAckBanner");
                if (ban) ban.hidden = true;
            }).catch(function () {
                pendingAlarmTradeId = trade.id;
                var ban = $("dcExitAckBanner");
                var txt = $("dcExitAckText");
                if (ban) ban.hidden = false;
                if (txt) txt.textContent = "Audio blocked — click to play alarm for " + trade.symbol;
            });
        } else {
            try { sessionStorage.setItem(alarmPlayedKey(trade), "1"); } catch (e) {}
        }
    }

    function updateExitTabTitle(panel) {
        var exits = (panel && panel.exit_now_symbols) || [];
        var plans = (panel && panel.plan_exit_symbols) || [];
        if (exits.length) {
            document.title = "🚨 EXIT · " + exits.join(", ");
        } else if (plans.length) {
            document.title = "⚠ PLAN EXIT · " + plans.join(", ");
        } else {
            document.title = defaultDocTitle;
        }
    }

    function takeTrade(symbol) {
        var stock = currentStock(symbol);
        if (!stock) return;
        var dir = (stock.direction || "LONG").toUpperCase() === "SHORT" ? "SHORT" : "LONG";
        toast("Taking trade " + symbol + "…");
        api("/open-trades/take", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                symbol: symbol,
                direction: dir,
                session_date: state && state.session_date,
                context: {
                    confidence: stock.confidence || stock.dashboard_kavach,
                    rs_pct: stock.rs_pct,
                    trade_score: stock.dashboard_score,
                    trade_state: stock.trade_state,
                    decision: stock.decision,
                    decision_label: stock.decision,
                    gate_badges: stock.gate_badges || [],
                    zone: isReadyState(stock.trade_state) ? "Zone 3 READY" : "Zone 4",
                    market_regime: (state.trade_state_obs || {}).market_regime,
                    regime_context: stock.regime_context || null,
                    removals_last_hour: (stock.regime_context || {}).removals_last_hour
                        != null ? (stock.regime_context || {}).removals_last_hour
                        : (state.trade_state_obs || {}).removals_last_hour,
                    counter_regime: !!(stock.regime_context || {}).counter_regime
                }
            })
        }).then(function (res) {
            if (!res.ok) { toast(res.error || "Take trade failed"); return; }
            if (res.take_warning) {
                toast("⚠ " + res.take_warning);
                showTakeWarningBanner(symbol, res.take_warning);
            } else {
                toast(symbol + " → Open Trades");
            }
            return api("/data");
        }).then(function (s) { if (s) applyState(s); }).catch(function () { toast("Take trade failed"); });
    }

    function showTakeWarningBanner(symbol, msg) {
        var elBan = $("dcTakeWarnBanner");
        if (!elBan) {
            elBan = document.createElement("div");
            elBan.id = "dcTakeWarnBanner";
            elBan.className = "dc-warn-banner show";
            var host = $("dcOpenTrades") || document.body;
            host.insertBefore(elBan, host.firstChild);
        }
        elBan.hidden = false;
        elBan.classList.add("show");
        elBan.textContent = symbol + ": " + msg;
    }

    function fmtHm(iso) {
        if (!iso) return "—";
        var d = new Date(iso);
        if (isNaN(d.getTime())) return String(iso).slice(11, 16) || "—";
        return ("0" + d.getHours()).slice(-2) + ":" + ("0" + d.getMinutes()).slice(-2);
    }

    function renderOpenTrades() {
        var stack = $("dcOpenTradesStack");
        var empty = $("dcOpenTradesEmpty");
        if (!stack || !empty) return;
        var panel = (state && state.open_trades_panel) || {};
        var trades = panel.open_trades || [];
        updateExitTabTitle(panel);
        empty.hidden = trades.length > 0;
        stack.innerHTML = "";
        trades.forEach(function (t) {
            stack.appendChild(buildOpenTradeCard(t));
            if (t.state === "EXIT_NOW" || t.state === "PLAN_EXIT") playExitAlarm(t);
        });
    }

    function buildOpenTradeCard(t) {
        var cardCls = "dc-ot-card";
        if (t.state === "EXIT_NOW") cardCls += " dc-ot-card--exit";
        else if (t.state === "PLAN_EXIT") cardCls += " dc-ot-card--plan-exit";
        var card = el("div", cardCls);
        card.dataset.tradeId = t.id;

        var row1 = el("div", "dc-ot-row dc-ot-row--head");
        row1.appendChild(el("span", "dc-ot-sym", t.symbol));
        var dir = el("span", "dc-ot-dir dc-ot-dir--" + String(t.direction || "").toLowerCase(), t.direction || "—");
        row1.appendChild(dir);

        var etInp = document.createElement("input");
        etInp.className = "dc-ot-edit";
        etInp.type = "text";
        etInp.value = fmtHm(t.entry_time);
        etInp.title = "Entry time HH:MM";
        etInp.addEventListener("change", function () { editOpenField(t.id, "entry_time", etInp.value); });
        row1.appendChild(etInp);

        var pxInp = document.createElement("input");
        pxInp.className = "dc-ot-edit";
        pxInp.type = "number";
        pxInp.step = "0.01";
        pxInp.value = t.entry_price != null ? Number(t.entry_price).toFixed(2) : "";
        pxInp.title = "Entry price";
        pxInp.addEventListener("change", function () { editOpenField(t.id, "entry_price", pxInp.value); });
        row1.appendChild(pxInp);

        var qtyInp = document.createElement("input");
        qtyInp.className = "dc-ot-edit dc-ot-edit--qty";
        qtyInp.type = "number";
        qtyInp.value = t.entry_qty || "";
        qtyInp.title = "Quantity (lots × size)";
        qtyInp.addEventListener("change", function () { editOpenField(t.id, "entry_qty", qtyInp.value); });
        row1.appendChild(qtyInp);

        var dirSel = document.createElement("select");
        dirSel.className = "dc-ot-edit";
        ["LONG", "SHORT"].forEach(function (d) {
            var o = document.createElement("option");
            o.value = d; o.textContent = d;
            if (d === t.direction) o.selected = true;
            dirSel.appendChild(o);
        });
        dirSel.addEventListener("change", function () { editOpenField(t.id, "direction", dirSel.value); });
        row1.appendChild(dirSel);

        var stBadge = el("span", "dc-ot-state dc-ot-state--" + String(t.state || "").toLowerCase().replace(/_/g, "-"),
            (t.state || "").replace(/_/g, " "));
        row1.appendChild(stBadge);

        if (t.provenance || (t.state_context_snapshot && t.state_context_snapshot.provenance)) {
            row1.appendChild(el("span", "dc-ot-prov", "📌 Provenance captured"));
        }

        if (t.alarm_fired_at) {
            row1.appendChild(el("span", "dc-ot-alarm", "🔔 Alarm @" + fmtHm(t.alarm_fired_at)));
        }

        var exitBtn = el("button", "dc-btn dc-btn--danger dc-ot-exit-btn", "EXIT");
        exitBtn.type = "button";
        exitBtn.addEventListener("click", function () { beginExit(t); });
        row1.appendChild(exitBtn);
        card.appendChild(row1);

        var row2 = el("div", "dc-ot-row dc-ot-row--math");
        row2.appendChild(el("span", null, "LTP " + fmtPx(t.live_price)));
        row2.appendChild(el("span", null, "SL " + fmtPx(t.display_sl)));
        var dSl = el("span", null, "ΔSL " + fmtPx(t.distance_sl_pts) + " / " + fmtInr(t.distance_sl_inr));
        if (t.trade_risk_cap_flag) dSl.className = "dc-ot-risk--over";
        row2.appendChild(dSl);
        var pnlCls = (t.unrealized_pnl_inr || 0) >= 0 ? "dc-ot-pnl--pos" : "dc-ot-pnl--neg";
        row2.appendChild(el("span", pnlCls, "P&L " + fmtPx(t.unrealized_pnl_pts) + " / " + fmtInr(t.unrealized_pnl_inr)));
        row2.appendChild(el("span", null, "R:R " + (t.achieved_rr != null ? t.achieved_rr + ":1" : "—")));
        row2.appendChild(el("span", null, "Peak " + (t.highest_rr_reached != null ? t.highest_rr_reached + ":1" : "—")));
        card.appendChild(row2);
        if (t.trade_risk_cap_flag) card.classList.add("dc-ot-card--risk-over");

        var row3 = el("div", "dc-ot-row dc-ot-row--hint");
        row3.appendChild(el("span", "dc-ot-held", t.held_minutes != null ? ("held " + t.held_minutes + " min") : ""));
        row3.appendChild(el("span", "dc-ot-hint", t.action_hint || ""));
        card.appendChild(row3);

        var lrc = t.lock_removal_context;
        if ((t.state === "EXIT_NOW" || t.state === "PLAN_EXIT") && lrc && lrc.label) {
            var ctxRow = el("div", "dc-ot-row dc-ot-row--rank-ctx");
            var isR1 = lrc.rule === "R1";
            var isPlan = t.state === "PLAN_EXIT" || lrc.plan_exit;
            var cls = isPlan
                ? "dc-ot-rank-ctx dc-ot-rank-ctx--plan"
                : (isR1
                    ? "dc-ot-rank-ctx dc-ot-rank-ctx--r1"
                    : ((lrc.rule === "R2" && !lrc.price_closed_beyond_ema10)
                        ? "dc-ot-rank-ctx dc-ot-rank-ctx--r2"
                        : "dc-ot-rank-ctx dc-ot-rank-ctx--r1"));
            ctxRow.appendChild(el("span", cls, lrc.label));
            var metaParts = [];
            if (isR1) {
                if (lrc.vwap_close_hm) metaParts.push("VWAP@" + lrc.vwap_close_hm);
                if (lrc.ema10_distance_pts != null) metaParts.push("ΔEMA10 " + lrc.ema10_distance_pts);
                if (lrc.pnl_at_flag_inr != null) metaParts.push("P&L at flag " + fmtInr(lrc.pnl_at_flag_inr));
                if (isPlan || !lrc.price_closed_beyond_ema10) metaParts.push("EMA10 not yet crossed");
            } else {
                metaParts.push("ranks " + (lrc.rank_trail || "—"));
                metaParts.push(lrc.direction || "");
                if (lrc.entry_rank != null) metaParts.push("entry #" + lrc.entry_rank);
                if (lrc.removal_rank != null) metaParts.push("remove #" + lrc.removal_rank);
                metaParts.push(lrc.price_closed_beyond_ema10
                    ? "confirmed close beyond EMA10"
                    : "confirmed close NOT beyond EMA10");
            }
            ctxRow.appendChild(el("span", "dc-ot-rank-meta", metaParts.filter(Boolean).join(" · ")));
            card.appendChild(ctxRow);
        }

        var exitForm = el("div", "dc-ot-exit-form");
        exitForm.hidden = true;
        exitForm.innerHTML = "";
        card.appendChild(exitForm);
        card._exitForm = exitForm;
        return card;
    }

    function editOpenField(tradeId, field, value) {
        api("/open-trades/" + tradeId + "/edit", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ field: field, value: value })
        }).then(function (res) {
            if (!res.ok) toast(res.error || "Edit failed");
            return api("/data");
        }).then(function (s) { if (s) applyState(s); });
    }

    function beginExit(t) {
        var card = document.querySelector('.dc-ot-card[data-trade-id="' + t.id + '"]');
        if (!card || !card._exitForm) return;
        var form = card._exitForm;
        form.hidden = false;
        form.innerHTML = "";
        var px = document.createElement("input");
        px.type = "number"; px.step = "0.01"; px.className = "dc-ot-edit";
        px.value = t.live_price != null ? Number(t.live_price).toFixed(2) : "";
        px.placeholder = "Exit price";
        var reason = document.createElement("select");
        reason.className = "dc-ot-edit";
        EXIT_REASONS.forEach(function (r) {
            var o = document.createElement("option");
            o.value = r; o.textContent = r;
            if (t.exit_trigger_reason && r.indexOf((t.exit_trigger_reason || "").split(" ")[0]) === 0) o.selected = true;
            if (t.state === "EXIT_NOW" && t.exit_trigger_reason) {
                if (t.exit_trigger_reason.indexOf("EMA10") >= 0 && r.indexOf("EMA10") >= 0) o.selected = true;
                if (t.exit_trigger_reason.indexOf("EMA5") >= 0 && r.indexOf("EMA5") >= 0) o.selected = true;
                if (t.exit_trigger_reason.indexOf("Risk") >= 0 && r.indexOf("Risk") >= 0) o.selected = true;
                if (t.exit_trigger_reason.indexOf("Lock removed via R1") >= 0 && r === "Lock removed via R1") o.selected = true;
                if (t.exit_trigger_reason.indexOf("Lock removed via R2") >= 0 && r === "Lock removed via R2") o.selected = true;
            }
            reason.appendChild(o);
        });
        var note = document.createElement("input");
        note.type = "text"; note.className = "dc-ot-edit dc-ot-edit--note";
        note.placeholder = "Optional note";
        var conf = el("button", "dc-btn dc-btn--danger", "Confirm EXIT");
        conf.type = "button";
        conf.addEventListener("click", function () {
            api("/open-trades/" + t.id + "/exit", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    exit_price: Number(px.value),
                    exit_reason: reason.value,
                    exit_note: note.value || null
                })
            }).then(function (res) {
                if (!res.ok) { toast(res.error || "Exit failed"); return; }
                toast(t.symbol + " exited · " + fmtInr(res.trade && res.trade.realized_pnl_inr));
                return api("/data");
            }).then(function (s) { if (s) applyState(s); });
        });
        var cancel = el("button", "dc-btn", "Back");
        cancel.type = "button";
        cancel.addEventListener("click", function () { form.hidden = true; });
        form.appendChild(el("span", "dc-ot-exit-label", "Confirm exit"));
        form.appendChild(px);
        form.appendChild(reason);
        form.appendChild(note);
        form.appendChild(conf);
        form.appendChild(cancel);
    }

    function applyState(s) {
        if (!s) return;
        if (s.error) { toast("Error: " + s.error); return; }
        state = s;
        if (s.checklist_config && $("dcGoAlertSound") && localStorage.getItem("dc_go_alert_sound") == null) {
            goAlertEnabled = !!s.checklist_config.go_alert_sound_enabled;
            $("dcGoAlertSound").checked = goAlertEnabled;
        }
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
                if ($("dcZone3Grid")) $("dcZone3Grid").innerHTML = "";
                if ($("dcZone4List")) $("dcZone4List").innerHTML = "";
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
        // Refresh Zone 3 entry countdowns each second
        if (state && state.stocks && $("dcZone3Grid")) {
            var ready = (state.stocks || []).filter(function (s) { return isReadyState(s.trade_state); });
            ready.forEach(function (stock) {
                var card = $("dcZone3Grid").querySelector('.dc-ready-card[data-symbol="' + stock.symbol + '"]');
                if (card) patchReadyCard(card, stock);
            });
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
        var fiiEl = $("dcFiiDii");
        if (fiiEl) {
            fiiEl.addEventListener("change", function () {
                onChange("", "fii_dii_flow", this.value);
            });
        }
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
        var remTog = $("dcRemovalsToggle");
        if (remTog) {
            remTog.addEventListener("click", function () {
                var strip = $("dcRemovalsStrip");
                if (!strip) return;
                var open = strip.hidden;
                strip.hidden = !open;
                this.setAttribute("aria-expanded", open ? "true" : "false");
                var icon = this.querySelector("i");
                if (icon) icon.classList.toggle("dc-carryover-chevron--open", open);
            });
        }
        var fwExpand = $("dcFastWatchExpand");
        if (fwExpand) {
            fwExpand.addEventListener("click", function () {
                fastWatchExpanded = !fastWatchExpanded;
                renderFastWatch();
            });
        }
        var goAlertEl = $("dcGoAlertSound");
        if (goAlertEl) {
            try {
                goAlertEnabled = localStorage.getItem("dc_go_alert_sound") === "1";
            } catch (e) { goAlertEnabled = false; }
            goAlertEl.checked = goAlertEnabled;
            goAlertEl.addEventListener("change", function () {
                goAlertEnabled = !!this.checked;
                try {
                    localStorage.setItem("dc_go_alert_sound", goAlertEnabled ? "1" : "0");
                } catch (e) { /* ignore */ }
            });
        }
        var ackBtn = $("dcExitAckBtn");
        if (ackBtn) {
            ackBtn.addEventListener("click", function () {
                if (!exitAudio) exitAudio = new Audio("audio/attention.mp3");
                exitAudio.volume = 1;
                exitAudio.play().then(function () {
                    var ban = $("dcExitAckBanner");
                    if (ban) ban.hidden = true;
                    pendingAlarmTradeId = null;
                }).catch(function () {});
            });
        }

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
        // Live LTP / PnL for open trades (state machine still candle-close gated server-side)
        setInterval(function () {
            if (!state || !state.open_trades_panel || !(state.open_trades_panel.open_trades || []).length) return;
            api("/open-trades").then(function (p) {
                if (!p || p.error) return;
                state.open_trades_panel = p;
                renderOpenTrades();
            }).catch(function () {});
        }, 20000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else { boot(); }
})();
