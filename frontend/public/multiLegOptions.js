/* Multi-Leg Options journal */
(function () {
  "use strict";

  var MIN_LEGS = { STRADDLE: 2, IRON_FLY: 4, IRON_CONDOR: 4 };
  var instruments = { indices: [], equities: [], commodities: [] };
  var activeTrades = [];
  var orphanLegs = [];
  var assignableTrades = [];
  var reportRows = [];
  var reportSort = { key: "entry_date", dir: -1 };
  var tab = "active";
  var expandedTrades = {};
  var mode = "new";
  var editingId = null;
  var expiryTouched = false;
  var pollTimer = null;

  function $(id) { return document.getElementById(id); }

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function authHeaders() {
    var t = "";
    try { t = localStorage.getItem("trademanthan_token") || ""; } catch (e) { t = ""; }
    var h = { "Content-Type": "application/json", Accept: "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  async function api(path, opts) {
    var res = await fetch(apiBase() + "/api/multi-leg-options" + path, Object.assign({ headers: authHeaders() }, opts || {}));
    var data = await res.json().catch(function () { return {}; });
    if (!res.ok) {
      var detail = data.detail || data.message || res.statusText;
      throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
    }
    return data;
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function inr(n) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
    return new Intl.NumberFormat("en-IN", {
      style: "currency",
      currency: "INR",
      maximumFractionDigits: 2,
    }).format(Number(n));
  }

  function pnlClass(n) {
    if (n == null || Number.isNaN(Number(n))) return "";
    return Number(n) >= 0 ? "mlo-pos" : "mlo-neg";
  }

  function todayISO() {
    try {
      var parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      }).format(new Date());
      if (/^\d{4}-\d{2}-\d{2}$/.test(parts)) return parts;
    } catch (e) { /* local date below */ }
    var d = new Date();
    var m = String(d.getMonth() + 1).padStart(2, "0");
    var day = String(d.getDate()).padStart(2, "0");
    return d.getFullYear() + "-" + m + "-" + day;
  }

  function pad2(n) { return String(n).padStart(2, "0"); }

  var DEFAULT_CLOCK = "03:10 PM";

  function parseClock(text) {
    var match = String(text || "").trim().match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
    if (!match) return null;
    var hour = Number(match[1]);
    var minute = Number(match[2]);
    if (hour < 1 || hour > 12 || minute > 59) return null;
    var ap = match[3].toUpperCase();
    var hour24 = ap === "AM" ? (hour === 12 ? 0 : hour) : (hour === 12 ? 12 : hour + 12);
    return { hour: hour24, minute: minute };
  }

  function formatClock(hour24, minute) {
    var ap = hour24 >= 12 ? "PM" : "AM";
    var hour = hour24 % 12;
    if (hour === 0) hour = 12;
    return pad2(hour) + ":" + pad2(minute) + " " + ap;
  }

  function normalizeClock(text) {
    var parsed = parseClock(text);
    if (!parsed) return "";
    return formatClock(parsed.hour, parsed.minute);
  }

  function composeClock(dateStr, clockText) {
    var shown = normalizeClock(clockText);
    if (!dateStr || !shown) return "";
    return String(dateStr) + "T" + shown;
  }

  function parseISODate(s) {
    var p = String(s || "").slice(0, 10).split("-");
    if (p.length < 3) return NaN;
    return Date.UTC(+p[0], +p[1] - 1, +p[2]);
  }

  function dayDiff(fromIso, toIso) {
    var a = parseISODate(fromIso);
    var b = parseISODate(toIso);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    return Math.round((b - a) / 86400000);
  }

  function typeLabel(t) {
    return { STRADDLE: "Straddle", IRON_FLY: "Iron Fly", IRON_CONDOR: "Iron Condor", UNCLASSIFIED: "Unclassified" }[t] || t;
  }

  function lastWeekday(year, month, weekday) {
    var last = new Date(Date.UTC(year, month, 0));
    while (last.getUTCDay() !== weekday) last.setUTCDate(last.getUTCDate() - 1);
    var m = String(last.getUTCMonth() + 1).padStart(2, "0");
    var d = String(last.getUTCDate()).padStart(2, "0");
    return last.getUTCFullYear() + "-" + m + "-" + d;
  }

  function nextMonth(year, monthIndex) {
    return monthIndex === 11 ? [year + 1, 0] : [year, monthIndex + 1];
  }

  // Client preview. Server /expiry is the source of truth (NSE holiday shift + MCX master).
  // JS weekday: Sun=0 … Tue=2, Thu=4. Step one month at a time until DTE from IST today is 40+.
  var MIN_DTE = 40;

  function clientExpiry(entryIso, instrument) {
    var p = String(entryIso || "").split("-");
    if (p.length < 3) return "";
    var y = +p[0];
    var m = +p[1] - 1;
    var mcx = (instruments.commodities || []).indexOf(String(instrument || "").toUpperCase()) >= 0;
    var weekday = mcx ? 4 : 2;
    var exp = lastWeekday(y, m + 1, weekday);
    if (entryIso > exp) {
      var nm = nextMonth(y, m);
      exp = lastWeekday(nm[0], nm[1] + 1, weekday);
    }
    var today = todayISO();
    for (var i = 0; i < 24 && dayDiff(today, exp) < MIN_DTE; i++) {
      var parts = exp.split("-");
      var nxt = nextMonth(+parts[0], +parts[1] - 1);
      exp = lastWeekday(nxt[0], nxt[1] + 1, weekday);
    }
    return exp;
  }

  function setBanner(msg) {
    var el = $("mloBanner");
    if (!msg) { el.hidden = true; el.textContent = ""; return; }
    el.hidden = false;
    el.textContent = msg;
  }

  function setSyncBanner(msg, isError) {
    var el = $("mloSyncBanner");
    if (!msg) { el.hidden = true; el.textContent = ""; el.classList.remove("is-error"); return; }
    el.hidden = false;
    el.classList.toggle("is-error", !!isError);
    el.textContent = msg;
  }

  function countPhrase(n, singular, plural) {
    var value = Number(n) || 0;
    return value + " " + (value === 1 ? singular : plural);
  }

  function syncSummary(data) {
    var parts = [
      countPhrase(data.new_legs, "new leg", "new legs"),
      countPhrase(data.new_trades, "new trade created", "new trades created"),
      countPhrase(data.orphans, "orphan flagged", "orphans flagged"),
      countPhrase(data.duplicates_skipped, "duplicate skipped", "duplicates skipped"),
    ];
    if (data.skipped_no_lot) parts.push(countPhrase(data.skipped_no_lot, "skipped (no lot)", "skipped (no lot)"));
    if (data.skipped_unmapped) parts.push(countPhrase(data.skipped_unmapped, "skipped (unmapped)", "skipped (unmapped)"));
    if (data.master_empty) parts.push("instrument master had no index option contracts");
    return parts.join(", ") + ".";
  }

  function minForType() {
    if ($("mloType").value === "UNCLASSIFIED") return 1;
    return MIN_LEGS[$("mloType").value] || 2;
  }

  function legCount() {
    return $("mloLegs").querySelectorAll(".mlo-leg").length;
  }

  function refreshSaveGate() {
    var under = legCount() < minForType();
    $("mloSave").disabled = under;
    $("mloSave").title = under ? "Need at least " + minForType() + " legs" : "";
    var closeBtn = $("mloCloseTrade");
    if (!closeBtn.hidden) {
      closeBtn.disabled = !allLegsExited();
    }
  }

  function refreshDte() {
    var exp = $("mloExpiry").value;
    var days = dayDiff(todayISO(), exp);
    $("mloDte").textContent = days == null ? "DTE —" : "DTE " + days;
  }

  function clockParts(iso) {
    if (!iso) return null;
    var d = new Date(iso);
    if (!Number.isNaN(d.getTime())) {
      var bag = {};
      new Intl.DateTimeFormat("en-GB", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
      }).formatToParts(d).forEach(function (part) { bag[part.type] = part.value; });
      var hour = Number(bag.hour);
      if (hour === 24) hour = 0;
      return {
        date: bag.year + "-" + bag.month + "-" + bag.day,
        hour: hour,
        minute: Number(bag.minute),
      };
    }
    var match = String(iso).match(/(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2})/);
    if (!match) return null;
    return { date: match[1], hour: Number(match[2]), minute: Number(match[3]) };
  }

  function dateField(name, value, placeholder) {
    var shown = value ? ' value="' + esc(value) + '"' : "";
    var typ = value ? "date" : "text";
    return '<input data-f="' + name + '" data-ph="' + esc(placeholder) + '" type="' + typ +
      '" placeholder="' + esc(placeholder) + '" aria-label="' + esc(placeholder) + '" required' + shown + ">";
  }

  function bindDateField(inp) {
    function show() {
      inp.type = inp.value ? "date" : "text";
    }
    inp.addEventListener("focus", function () { inp.type = "date"; });
    inp.addEventListener("blur", show);
  }

  function bindClockField(inp) {
    inp.addEventListener("blur", function () {
      var shown = normalizeClock(inp.value);
      if (shown) inp.value = shown;
    });
  }

  function legCard(leg, exitMode) {
    var wrap = document.createElement("div");
    wrap.className = "mlo-leg";
    if (leg && leg.id) wrap.dataset.id = leg.id;
    var entryClock = clockParts(leg && leg.entry_time);
    var entryText = entryClock ? formatClock(entryClock.hour, entryClock.minute) : DEFAULT_CLOCK;
    var exit = "";
    if (exitMode) {
      var exitClock = clockParts(leg && leg.exit_time);
      var exitDate = exitClock ? exitClock.date : todayISO();
      var exitText = exitClock ? formatClock(exitClock.hour, exitClock.minute) : DEFAULT_CLOCK;
      exit = '<input data-f="exit_price" type="number" min="0" step="0.01" placeholder="Exit price" aria-label="Exit price" value="' + esc(leg && leg.exit_price != null ? leg.exit_price : "") + '">' +
        dateField("exit_date", exitDate, "Exit date") +
        '<input data-f="exit_clock" type="text" placeholder="Time" aria-label="Exit time" value="' + esc(exitText) + '" required>';
    }
    wrap.innerHTML =
      '<select data-f="side" aria-label="Side" required><option value="" hidden>Side</option><option>BUY</option><option>SELL</option></select>' +
      '<select data-f="option_type" aria-label="Type" required><option value="" hidden>Type</option><option>CE</option><option>PE</option></select>' +
      '<input data-f="strike_price" type="number" min="0" step="0.01" placeholder="Strike" aria-label="Strike" required>' +
      dateField("leg_expiry_date", "", "Expiry") +
      '<input data-f="entry_price" type="number" min="0" step="0.01" placeholder="Entry price" aria-label="Entry price" required>' +
      '<input data-f="entry_clock" type="text" placeholder="Time" aria-label="Time" value="' + esc(entryText) + '" required>' +
      exit +
      '<button type="button" class="mlo-icon-btn mlo-remove" aria-label="Remove leg"><i class="fas fa-trash" aria-hidden="true"></i></button>';
    wrap.querySelector('[data-f="side"]').value = (leg && leg.side) || "SELL";
    wrap.querySelector('[data-f="option_type"]').value = (leg && leg.option_type) || "CE";
    wrap.querySelector('[data-f="strike_price"]').value = leg && leg.strike_price != null ? leg.strike_price : "";
    var expiryInp = wrap.querySelector('[data-f="leg_expiry_date"]');
    expiryInp.value = (leg && leg.leg_expiry_date) || $("mloExpiry").value || "";
    if (expiryInp.value) expiryInp.type = "date";
    wrap.querySelector('[data-f="entry_price"]').value = leg && leg.entry_price != null ? leg.entry_price : "";
    wrap.querySelectorAll("input[data-ph]").forEach(bindDateField);
    wrap.querySelectorAll('[data-f="entry_clock"], [data-f="exit_clock"]').forEach(bindClockField);
    if (leg && leg.leg_expiry_date) expiryInp.dataset.touched = "1";
    expiryInp.addEventListener("input", function () { expiryInp.dataset.touched = "1"; });
    wrap.querySelector(".mlo-remove").addEventListener("click", function () {
      wrap.remove();
      refreshSaveGate();
    });
    wrap.querySelectorAll("input, select").forEach(function (inp) {
      inp.addEventListener("input", refreshSaveGate);
      inp.addEventListener("change", refreshSaveGate);
    });
    return wrap;
  }

  function toLocal(iso) {
    var parts = clockParts(iso);
    if (!parts) return "";
    return parts.date + " " + formatClock(parts.hour, parts.minute);
  }

  function addLeg(leg) {
    $("mloLegs").appendChild(legCard(leg, mode === "exit"));
    refreshSaveGate();
  }

  function allLegsExited() {
    var rows = $("mloLegs").querySelectorAll(".mlo-leg");
    if (!rows.length) return false;
    for (var i = 0; i < rows.length; i++) {
      var px = rows[i].querySelector('[data-f="exit_price"]');
      var day = rows[i].querySelector('[data-f="exit_date"]');
      var clock = rows[i].querySelector('[data-f="exit_clock"]');
      if (!px || !day || !clock) return false;
      if (px.value === "" || !day.value || !parseClock(clock.value)) return false;
    }
    return true;
  }

  function readLegs() {
    return Array.prototype.map.call($("mloLegs").querySelectorAll(".mlo-leg"), function (row) {
      function val(name) {
        var el = row.querySelector('[data-f="' + name + '"]');
        return el ? el.value : "";
      }
      var leg = {
        side: val("side"),
        option_type: val("option_type"),
        strike_price: Number(val("strike_price")),
        leg_expiry_date: val("leg_expiry_date"),
        entry_price: Number(val("entry_price")),
        entry_time: composeClock($("mloEntryDate").value, val("entry_clock")),
      };
      if (row.dataset.id) leg.id = row.dataset.id;
      var exitPx = val("exit_price");
      var exitDay = val("exit_date");
      if (exitPx !== "") {
        leg.exit_price = Number(exitPx);
        leg.exit_time = composeClock(exitDay, val("exit_clock")) || null;
      } else {
        leg.exit_price = null;
        leg.exit_time = null;
      }
      return leg;
    });
  }

  function legClockError() {
    var rows = $("mloLegs").querySelectorAll(".mlo-leg");
    for (var i = 0; i < rows.length; i++) {
      var entry = rows[i].querySelector('[data-f="entry_clock"]');
      if (!entry || !parseClock(entry.value)) return "Time must look like 03:10 PM";
      var exitPx = rows[i].querySelector('[data-f="exit_price"]');
      var exitClock = rows[i].querySelector('[data-f="exit_clock"]');
      if (exitPx && exitPx.value !== "" && exitClock && !parseClock(exitClock.value)) {
        return "Exit time must look like 03:10 PM";
      }
    }
    return "";
  }

  function payload() {
    return {
      trade_type: $("mloType").value,
      instrument: $("mloInstrument").value.trim().toUpperCase(),
      spot_price_entry: Number($("mloSpot").value),
      entry_date: $("mloEntryDate").value,
      expiry_date: $("mloExpiry").value,
      legs: readLegs(),
    };
  }

  function showFormError(msg) {
    var el = $("mloFormErr");
    el.hidden = !msg;
    el.textContent = msg || "";
  }

  async function loadExpiry() {
    if (mode !== "new" || expiryTouched) return;
    var inst = $("mloInstrument").value.trim();
    var entry = $("mloEntryDate").value;
    if (!inst || !entry) return;
    $("mloExpiry").value = clientExpiry(entry, inst);
    try {
      var data = await api("/expiry?instrument=" + encodeURIComponent(inst) + "&entry_date=" + encodeURIComponent(entry));
      if (!expiryTouched && data.expiry_date) $("mloExpiry").value = data.expiry_date;
    } catch (e) { /* keep client preview */ }
    syncLegExpiries();
    refreshDte();
  }

  function syncLegExpiries() {
    var exp = $("mloExpiry").value;
    $("mloLegs").querySelectorAll('[data-f="leg_expiry_date"]').forEach(function (inp) {
      if (!inp.dataset.touched) {
        inp.value = exp;
        inp.type = exp ? "date" : "text";
      }
    });
  }

  function ensureTypeOptions(trade) {
    var sel = $("mloType");
    var opt = sel.querySelector('option[value="UNCLASSIFIED"]');
    if (trade && trade.trade_type === "UNCLASSIFIED") {
      if (!opt) {
        opt = document.createElement("option");
        opt.value = "UNCLASSIFIED";
        opt.textContent = "Unclassified";
        sel.appendChild(opt);
      }
    } else if (opt) {
      opt.remove();
    }
  }

  function openModal(nextMode, trade) {
    mode = nextMode;
    editingId = trade ? trade.id : null;
    expiryTouched = !!trade;
    ensureTypeOptions(trade);
    $("mloModalTitle").textContent = nextMode === "new" ? "New Trade" : nextMode === "exit" ? "Exit Trade" : "Edit Trade";
    $("mloCloseTrade").hidden = nextMode !== "exit";
    var tradeNo = $("mloTradeNo");
    if (nextMode === "new") {
      tradeNo.hidden = true;
    } else {
      tradeNo.hidden = false;
      $("mloTradeNoVal").textContent = trade && trade.trade_no != null ? String(trade.trade_no) : "—";
    }
    $("mloType").value = (trade && trade.trade_type) || "STRADDLE";
    $("mloInstrument").value = (trade && trade.instrument) || "";
    $("mloSpot").value = trade && trade.spot_price_entry != null ? trade.spot_price_entry : "";
    $("mloEntryDate").value = (trade && trade.entry_date) || todayISO();
    $("mloExpiry").value = (trade && trade.expiry_date) || "";
    $("mloLegs").innerHTML = "";
    showFormError("");
    var legs = (trade && trade.legs) || [];
    if (!legs.length) {
      var n = minForType();
      for (var i = 0; i < n; i++) addLeg(null);
    } else {
      legs.forEach(function (leg) { addLeg(leg); });
    }
    if (!trade) loadExpiry();
    refreshDte();
    refreshSaveGate();
    $("mloModal").hidden = false;
  }

  function closeModal() {
    $("mloModal").hidden = true;
  }

  function assignFieldFocused() {
    var el = document.activeElement;
    return !!(el && el.closest && el.closest(".mlo-orphan-assign"));
  }

  function tradeChoiceLabel(t) {
    var num = t.trade_no != null ? String(t.trade_no) : "—";
    return num + " · " + t.instrument + " · " + t.expiry_date;
  }

  function rowField(label, value, extraClass) {
    return '<span class="mlo-row-field"><span class="mlo-row-k">' + label + "</span>" +
      '<span class="' + (extraClass || "") + '">' + esc(value) + "</span></span>";
  }

  function iconButton(act, label, icon, expanded) {
    return '<button type="button" class="mlo-icon-btn" data-act="' + act + '" aria-label="' + esc(label) + '"' +
      (act === "toggle" ? ' aria-expanded="' + (expanded ? "true" : "false") + '"' : "") + ">" +
      '<i class="fas ' + icon + '" aria-hidden="true"></i></button>';
  }

  function renderOrphans(orphans) {
    if (!orphans || !orphans.length) return "";
    var cards = orphans.map(function (leg) {
      return '<article class="mlo-card mlo-orphan so-card" data-leg="' + esc(leg.id) + '">' +
        '<div class="mlo-leg-ro">' +
          '<span data-label="Underlying">' + esc(leg.instrument || "—") + "</span>" +
          '<span data-label="Expiry">' + esc(leg.expiry_date) + "</span>" +
          '<span data-label="Strike">' + esc(leg.strike_price) + "</span>" +
          '<span data-label="Side">' + esc(leg.side) + "</span>" +
          '<span data-label="Type">' + esc(leg.option_type) + "</span>" +
          '<span data-label="Entry">' + esc(leg.entry_price) + "</span>" +
          '<span data-label="Entry time">' + esc(toLocal(leg.entry_time) || leg.entry_time || "—") + "</span>" +
          '<span class="mlo-badge">Orphan</span>' +
        "</div>" +
        '<div class="mlo-combo mlo-orphan-assign">' +
          '<input type="text" placeholder="Assign to trade — search number, instrument, or expiry" autocomplete="off" data-leg="' + esc(leg.id) + '" />' +
          '<div class="mlo-combo-list" hidden></div>' +
        "</div></article>";
    }).join("");
    return '<section class="mlo-orphans"><h2 class="mlo-orphans-title">Orphan legs</h2>' +
      '<p class="mlo-meta">These fills are not on a trade. Assign one to count it in that trade’s P&amp;L.</p>' +
      cards + "</section>";
  }

  function renderActive(trades, orphans) {
    activeTrades = trades || [];
    if (orphans) orphanLegs = orphans;
    if (assignFieldFocused()) return;
    var host = $("mloActive");
    var empty = '<p class="mlo-empty">No active trades. Use + New Trade to record a straddle, iron fly, or iron condor.</p>';
    var cards = activeTrades.length ? activeTrades.map(function (t) {
      var open = !!expandedTrades[t.id];
      var legs = (t.legs || []).map(function (leg) {
        var mark = leg.exited ? leg.exit_price : leg.ltp;
        var markLabel = leg.exited ? "Exit" : "LTP";
        return '<div class="mlo-leg-ro">' +
          '<span data-label="Side">' + esc(leg.side) + "</span>" +
          '<span data-label="Type">' + esc(leg.option_type) + "</span>" +
          '<span data-label="Strike">' + esc(leg.strike_price) + "</span>" +
          '<span data-label="Entry">' + esc(leg.entry_price) + "</span>" +
          '<span data-label="' + markLabel + '">' + esc(mark == null ? "—" : mark) + "</span>" +
          '<span data-label="Delta">' + (leg.delta == null ? "—" : esc(Number(leg.delta).toFixed(4))) + "</span>" +
          '<span data-label="P&L" class="' + pnlClass(leg.leg_pnl) + '">' + esc(inr(leg.leg_pnl)) + "</span>" +
          "</div>";
      }).join("");
      var head = '<div class="mlo-leg-ro mlo-leg-head"><span>Side</span><span>CE/PE</span><span>Strike</span><span>Entry</span><span>LTP</span><span>Delta</span><span>Leg P&L</span></div>';
      return '<article class="mlo-trade so-card" data-id="' + esc(t.id) + '">' +
        '<div class="mlo-row-summary">' +
          rowField("Trade No", t.trade_no == null ? "—" : t.trade_no) +
          rowField("Instrument and Type", t.instrument + " · " + typeLabel(t.trade_type)) +
          rowField("Expiry date", t.expiry_date || "—") +
          rowField("DTE", t.dte == null ? "—" : t.dte) +
          rowField("P&L", inr(t.total_pnl), pnlClass(t.total_pnl)) +
          '<span class="mlo-row-actions">' +
            iconButton("edit", "Edit", "fa-pen") +
            iconButton("delete", "Delete", "fa-trash") +
            iconButton("exit", "Exit Trade", "fa-right-from-bracket") +
            iconButton("toggle", open ? "Collapse" : "Expand", open ? "fa-chevron-up" : "fa-chevron-down", open) +
          "</span></div>" +
        '<div class="mlo-row-body"' + (open ? "" : " hidden") + ">" + head + legs + "</div></article>";
    }).join("") : empty;
    host.innerHTML = renderOrphans(orphanLegs) + cards;
  }

  function cmp(a, b, key) {
    var av = a[key];
    var bv = b[key];
    if (key === "total_pnl") {
      av = av == null ? -Infinity : Number(av);
      bv = bv == null ? -Infinity : Number(bv);
    } else {
      av = av == null ? "" : String(av);
      bv = bv == null ? "" : String(bv);
    }
    if (av < bv) return -1;
    if (av > bv) return 1;
    return 0;
  }

  function renderReport() {
    var rows = reportRows.slice().sort(function (a, b) {
      return cmp(a, b, reportSort.key) * reportSort.dir;
    });
    var body = $("mloReportBody");
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="5" class="mlo-empty">No trades for these filters.</td></tr>';
      return;
    }
    body.innerHTML = rows.map(function (t, idx) {
      var legs = (t.legs || []).map(function (leg) {
        var mark = leg.exited ? leg.exit_price : leg.ltp;
        return "<tr><td>" + esc(leg.side) + "</td><td>" + esc(leg.option_type) + "</td><td>" + esc(leg.strike_price) +
          "</td><td>" + esc(leg.entry_price) + "</td><td>" + esc(mark == null ? "—" : mark) +
          '</td><td class="' + pnlClass(leg.leg_pnl) + '">' + esc(inr(leg.leg_pnl)) + "</td></tr>";
      }).join("");
      return '<tr class="mlo-report-row" data-idx="' + idx + '">' +
        '<td><button type="button" class="mlo-btn mlo-expand" data-idx="' + idx + '">' + esc(t.instrument) + "</button> " +
        '<span class="mlo-meta">' + esc(typeLabel(t.trade_type)) + "</span></td>" +
        "<td>" + esc(t.expiry_date) + "</td><td>" + esc(t.entry_date) + "</td><td>" + esc(t.exit_date || "") + "</td>" +
        '<td class="' + pnlClass(t.total_pnl) + '">' + esc(inr(t.total_pnl)) + "</td></tr>" +
        '<tr class="mlo-legs-detail" data-detail="' + idx + '" hidden><td colspan="5"><table class="mlo-table"><thead><tr>' +
        "<th>Side</th><th>CE/PE</th><th>Strike</th><th>Entry</th><th>Exit or LTP</th><th>Leg P&L</th></tr></thead><tbody>" +
        legs + "</tbody></table></td></tr>";
    }).join("");
  }

  async function loadActive(refreshQuotes) {
    try {
      var data = refreshQuotes ? await api("/quotes") : await api("/trades/active");
      setBanner(data.quote_error ? "Live quotes unavailable. Showing last saved LTP. " + data.quote_error : "");
      assignableTrades = data.assignable_trades || [];
      renderActive(data.trades || [], data.orphans || []);
    } catch (e) {
      setBanner(e.message || "Could not load active trades");
    }
  }

  async function loadReport() {
    var q = [];
    function add(name, id) {
      var v = $(id).value.trim();
      if (v) q.push(name + "=" + encodeURIComponent(v));
    }
    add("instrument", "mloFInstrument");
    add("trade_type", "mloFType");
    add("status", "mloFStatus");
    add("date_from", "mloFFrom");
    add("date_to", "mloFTo");
    try {
      var data = await api("/trades/report" + (q.length ? "?" + q.join("&") : ""));
      reportRows = data.trades || [];
      renderReport();
    } catch (e) {
      setBanner(e.message || "Could not load report");
    }
  }

  function showTab(name) {
    tab = name;
    document.querySelectorAll(".bf-tab").forEach(function (btn) {
      btn.classList.toggle("active", btn.dataset.tab === name);
    });
    $("mloActive").hidden = name !== "active";
    $("mloReport").hidden = name !== "report";
    if (name === "report") loadReport();
    else loadActive(true);
  }

  function fillCombo(query) {
    var list = $("mloInstrumentList");
    var q = String(query || "").trim().toUpperCase();
    function group(title, names) {
      var hits = names.filter(function (n) { return !q || n.indexOf(q) >= 0; }).slice(0, 40);
      if (!hits.length) return "";
      return '<div class="mlo-combo-group">' + esc(title) + "</div>" + hits.map(function (n) {
        return '<button type="button" data-sym="' + esc(n) + '">' + esc(n) + "</button>";
      }).join("");
    }
    var html = group("Index", instruments.indices || []) +
      group("Equity", instruments.equities || []) +
      group("MCX", instruments.commodities || []);
    list.innerHTML = html || '<div class="mlo-combo-group">No match</div>';
    list.hidden = false;
  }

  async function loadInstruments() {
    try {
      var data = await api("/instruments");
      instruments.indices = data.indices || [];
      instruments.equities = data.equities || [];
      instruments.commodities = data.commodities || [];
    } catch (e) {
      instruments.indices = ["NIFTY", "BANKNIFTY", "SENSEX"];
    }
  }

  function tradeById(id) {
    return activeTrades.filter(function (t) { return t.id === id; })[0] || null;
  }

  $("mloNewTrade").addEventListener("click", function () { openModal("new", null); });
  $("mloSyncUpstox").addEventListener("click", openSyncModal);
  $("mloCancel").addEventListener("click", closeModal);
  $("mloModalClose").addEventListener("click", closeModal);
  $("mloAddLeg").addEventListener("click", function () { addLeg(null); });
  $("mloType").addEventListener("change", function () {
    if (mode === "new" && legCount() < minForType()) {
      while (legCount() < minForType()) addLeg(null);
    }
    refreshSaveGate();
  });
  $("mloEntryDate").addEventListener("change", function () {
    if (mode !== "new") return;
    expiryTouched = false;
    loadExpiry();
  });
  $("mloExpiry").addEventListener("input", function () { expiryTouched = true; refreshDte(); syncLegExpiries(); });
  $("mloInstrument").addEventListener("focus", function () { fillCombo(this.value); });
  $("mloInstrument").addEventListener("input", function () {
    fillCombo(this.value);
    if (mode !== "new") return;
    expiryTouched = false;
    loadExpiry();
  });
  $("mloInstrumentList").addEventListener("click", function (ev) {
    var btn = ev.target.closest("button[data-sym]");
    if (!btn) return;
    $("mloInstrument").value = btn.getAttribute("data-sym");
    $("mloInstrumentList").hidden = true;
    if (mode !== "new") return;
    expiryTouched = false;
    loadExpiry();
  });
  document.addEventListener("click", function (ev) {
    if (ev.target.closest(".mlo-combo")) return;
    $("mloInstrumentList").hidden = true;
    document.querySelectorAll(".mlo-orphan-assign .mlo-combo-list").forEach(function (el) { el.hidden = true; });
  });

  $("mloForm").addEventListener("submit", async function (ev) {
    ev.preventDefault();
    showFormError("");
    if (legCount() < minForType()) {
      showFormError(typeLabel($("mloType").value) + " needs at least " + minForType() + " legs");
      return;
    }
    var clockErr = legClockError();
    if (clockErr) {
      showFormError(clockErr);
      return;
    }
    var body = payload();
    try {
      if (mode === "new") await api("/trades", { method: "POST", body: JSON.stringify(body) });
      else await api("/trades/" + editingId, { method: "PUT", body: JSON.stringify(body) });
      closeModal();
      await loadActive(true);
    } catch (e) {
      showFormError(e.message || "Save failed");
    }
  });

  $("mloCloseTrade").addEventListener("click", async function () {
    showFormError("");
    if (!allLegsExited()) {
      showFormError("Close requires every leg to have an exit price and exit time");
      return;
    }
    try {
      await api("/trades/" + editingId + "/close", { method: "POST", body: JSON.stringify({ legs: readLegs() }) });
      closeModal();
      await loadActive(true);
    } catch (e) {
      showFormError(e.message || "Close failed");
    }
  });

  function showAssignList(input) {
    var list = input.parentElement.querySelector(".mlo-combo-list");
    if (!list) return;
    var q = input.value.trim().toUpperCase();
    var hits = assignableTrades.filter(function (t) {
      if (!q) return true;
      var hay = ((t.trade_no == null ? "" : t.trade_no) + " " + t.instrument + " " + t.expiry_date + " " + t.trade_type).toUpperCase();
      return hay.indexOf(q) >= 0;
    }).slice(0, 30);
    list.innerHTML = hits.length ? hits.map(function (t) {
      return '<button type="button" data-trade="' + esc(t.id) + '" data-leg="' + esc(input.getAttribute("data-leg")) + '">' +
        esc(tradeChoiceLabel(t)) + "</button>";
    }).join("") : '<div class="mlo-combo-group">No matching trade</div>';
    list.hidden = false;
  }

  async function assignOrphan(legId, tradeId) {
    try {
      await api("/orphans/" + encodeURIComponent(legId) + "/assign", {
        method: "POST",
        body: JSON.stringify({ trade_id: tradeId }),
      });
      await loadActive(true);
    } catch (e) {
      setBanner(e.message || "Could not assign the leg");
    }
  }

  function openSyncModal() {
    $("mloSyncDate").value = todayISO();
    $("mloSyncErr").hidden = true;
    $("mloSyncErr").textContent = "";
    $("mloSyncModal").hidden = false;
  }

  function closeSyncModal() {
    $("mloSyncModal").hidden = true;
  }

  async function syncFromUpstox(tradeDate) {
    var btn = $("mloSyncUpstox");
    var label = btn.querySelector(".text");
    btn.disabled = true;
    if (label) label.textContent = "Syncing…";
    try {
      var data = await api("/sync/upstox", {
        method: "POST",
        body: JSON.stringify({ trade_date: tradeDate }),
      });
      var dated = data.trade_date ? data.trade_date + ": " : "";
      setSyncBanner(dated + syncSummary(data), false);
      closeSyncModal();
      await loadActive(true);
    } catch (e) {
      $("mloSyncErr").hidden = false;
      $("mloSyncErr").textContent = e.message || "Sync failed";
      setSyncBanner(e.message || "Sync failed", true);
    } finally {
      btn.disabled = false;
      if (label) label.textContent = "Sync frm Broker";
    }
  }

  $("mloSyncCancel").addEventListener("click", closeSyncModal);
  $("mloSyncClose").addEventListener("click", closeSyncModal);
  $("mloSyncForm").addEventListener("submit", function (ev) {
    ev.preventDefault();
    var day = $("mloSyncDate").value;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) {
      $("mloSyncErr").hidden = false;
      $("mloSyncErr").textContent = "Choose a trade date";
      return;
    }
    syncFromUpstox(day);
  });

  $("mloActive").addEventListener("input", function (ev) {
    var input = ev.target.closest(".mlo-orphan-assign input");
    if (input) showAssignList(input);
  });
  $("mloActive").addEventListener("focusin", function (ev) {
    var input = ev.target.closest && ev.target.closest(".mlo-orphan-assign input");
    if (input) showAssignList(input);
  });

  $("mloActive").addEventListener("click", async function (ev) {
    var pick = ev.target.closest("button[data-trade]");
    if (pick) {
      var legId = pick.getAttribute("data-leg");
      var tradeId = pick.getAttribute("data-trade");
      if (legId && tradeId) await assignOrphan(legId, tradeId);
      return;
    }
    var btn = ev.target.closest("button[data-act]");
    if (!btn) return;
    var card = btn.closest(".mlo-trade");
    if (!card) return;
    var trade = tradeById(card.getAttribute("data-id"));
    if (!trade) return;
    var act = btn.getAttribute("data-act");
    if (act === "toggle") {
      var body = card.querySelector(".mlo-row-body");
      var willOpen = !!(body && body.hidden);
      if (body) body.hidden = !willOpen;
      if (willOpen) expandedTrades[trade.id] = true;
      else delete expandedTrades[trade.id];
      var icon = btn.querySelector("i");
      if (icon) icon.className = willOpen ? "fas fa-chevron-up" : "fas fa-chevron-down";
      btn.setAttribute("aria-expanded", willOpen ? "true" : "false");
      btn.setAttribute("aria-label", willOpen ? "Collapse" : "Expand");
      return;
    }
    if (act === "edit") openModal("edit", trade);
    if (act === "exit") openModal("exit", trade);
    if (act === "delete") {
      if (!window.confirm("Delete this " + trade.instrument + " trade?")) return;
      try {
        await api("/trades/" + trade.id, { method: "DELETE" });
        await loadActive(true);
      } catch (e) {
        setBanner(e.message || "Delete failed");
      }
    }
  });

  document.querySelectorAll(".bf-tab").forEach(function (btn) {
    btn.addEventListener("click", function () { showTab(btn.dataset.tab); });
  });
  $("mloApplyFilters").addEventListener("click", loadReport);
  $("mloReportTable").querySelector("thead").addEventListener("click", function (ev) {
    var th = ev.target.closest("th[data-sort]");
    if (!th) return;
    var key = th.getAttribute("data-sort");
    if (reportSort.key === key) reportSort.dir *= -1;
    else { reportSort.key = key; reportSort.dir = 1; }
    renderReport();
  });
  $("mloReportBody").addEventListener("click", function (ev) {
    var btn = ev.target.closest(".mlo-expand");
    if (!btn) return;
    var detail = $("mloReportBody").querySelector('[data-detail="' + btn.getAttribute("data-idx") + '"]');
    if (detail) detail.hidden = !detail.hidden;
  });

  loadInstruments().then(function () { return loadActive(true); });
  pollTimer = setInterval(function () {
    if (tab === "active" && $("mloModal").hidden) loadActive(true);
  }, 8000);
})();
