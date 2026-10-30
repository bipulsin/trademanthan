/* MomentX desk — polls /api/momentx/ready; sound + Take Trade confirm */
(function () {
  "use strict";

  var SOUND_KEY = "momentx_sound_enabled";
  var VOL_FILTER_KEY = "momentx_volume_filter";
  var TRIGGERED_KEY = "momentx_triggered_symbols";
  var POLL_DEFAULT_MS = 5000;
  var audio = null;
  var soundOn = false;
  var volFilterOn = true;
  var pollTimer = null;
  var knownStocks = {};
  var cardsByStock = {};
  var pendingTrade = null;
  var audioUnlocked = false;
  var prefsSyncing = false;

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function authHeaders() {
    var t = "";
    try { t = localStorage.getItem("trademanthan_token") || ""; } catch (e) { t = ""; }
    var h = { Accept: "application/json", "Content-Type": "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  function $(id) { return document.getElementById(id); }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function fmtNum(n, digits) {
    if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
    return Number(n).toLocaleString("en-IN", {
      minimumFractionDigits: digits == null ? 2 : digits,
      maximumFractionDigits: digits == null ? 2 : digits,
    });
  }

  function fmtHm(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return "—";
      return d.toLocaleString("en-GB", {
        timeZone: "Asia/Kolkata",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
    } catch (e) {
      return "—";
    }
  }

  function fmtTime(iso) {
    return fmtHm(iso);
  }

  function cardStatus(card) {
    var s = String((card && card.status) || "").toLowerCase();
    if (s === "locked") return "locked";
    if (s === "filters_off") return "filters_off";
    if (s === "active") return "active";
    if (card && card.active === false) return "filters_off";
    return "active";
  }

  function loadSoundPref() {
    try { return localStorage.getItem(SOUND_KEY) === "1"; } catch (e) { return false; }
  }

  function saveSoundPref(on) {
    try { localStorage.setItem(SOUND_KEY, on ? "1" : "0"); } catch (e) { /* private */ }
  }

  function loadVolFilterPref() {
    try {
      var v = localStorage.getItem(VOL_FILTER_KEY);
      if (v === null || v === undefined || v === "") return true;
      return v === "1";
    } catch (e) {
      return true;
    }
  }

  function saveVolFilterPref(on) {
    try { localStorage.setItem(VOL_FILTER_KEY, on ? "1" : "0"); } catch (e) { /* private */ }
  }

  function syncVolFilterUi() {
    var el = $("mxVolFilter");
    if (el) el.checked = !!volFilterOn;
  }

  async function pushVolFilterPref(on, reload) {
    prefsSyncing = true;
    try {
      var res = await fetch(apiBase() + "/api/momentx/prefs", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ volume_filter_enabled: !!on }),
      });
      var data = await res.json().catch(function () { return {}; });
      if (res.ok && data && typeof data.volume_filter_enabled === "boolean") {
        volFilterOn = !!data.volume_filter_enabled;
        syncVolFilterUi();
      }
      if (reload !== false) await loadReady();
    } catch (e) {
      /* keep local preference; next poll may re-sync */
    } finally {
      prefsSyncing = false;
    }
  }

  function loadTriggeredMap() {
    try {
      return JSON.parse(localStorage.getItem(TRIGGERED_KEY) || "{}") || {};
    } catch (e) {
      return {};
    }
  }

  function saveTriggeredMap(map) {
    try { localStorage.setItem(TRIGGERED_KEY, JSON.stringify(map || {})); } catch (e) { /* */ }
  }

  function ensureAudio() {
    if (!audio) {
      audio = new Audio("audio/trade_now.mp3");
      audio.preload = "auto";
    }
    return audio;
  }

  function unlockAudio() {
    var a = ensureAudio();
    a.muted = true;
    var p = a.play();
    if (p && typeof p.then === "function") {
      p.then(function () {
        a.pause();
        a.currentTime = 0;
        a.muted = false;
        audioUnlocked = true;
      }).catch(function () { /* still locked */ });
    } else {
      a.muted = false;
      audioUnlocked = true;
    }
  }

  function playTradeSound(symbol) {
    if (!soundOn) return;
    var map = loadTriggeredMap();
    var key = String(symbol || "").toUpperCase();
    if (map[key]) return;
    map[key] = Date.now();
    saveTriggeredMap(map);
    try {
      var a = ensureAudio();
      a.currentTime = 0;
      var played = a.play();
      if (played && typeof played.catch === "function") played.catch(function () {});
    } catch (e) { /* autoplay */ }
  }

  function clearTriggerMemory(symbol) {
    var map = loadTriggeredMap();
    var key = String(symbol || "").toUpperCase();
    if (map[key]) {
      delete map[key];
      saveTriggeredMap(map);
    }
  }

  function setSoundUi() {
    var btn = $("mxSoundBtn");
    if (!btn) return;
    btn.setAttribute("aria-pressed", soundOn ? "true" : "false");
    btn.innerHTML = soundOn
      ? '<i class="fas fa-volume-up" aria-hidden="true"></i> Sound on'
      : '<i class="fas fa-volume-mute" aria-hidden="true"></i> Sound off';
  }

  function cardSide(card) {
    var s = String((card && (card.side || (card.stage1 && card.stage1.side))) || "bullish").toLowerCase();
    return s === "bearish" ? "bearish" : "bullish";
  }

  function stage1Html(s1) {
    if (!s1) return "";
    var volOk = s1.volume_ok;
    if (volOk == null && s1.conditions) volOk = !!s1.conditions.volume_ok;
    var volNote = volOk === true ? " ✓" : volOk === false ? " ✗" : "";
    return (
      '<details class="mx-details"><summary>Stage 1 values</summary>' +
      '<div class="mx-details-grid">' +
      "<div>Price " + esc(fmtNum(s1.price)) + "</div>" +
      "<div>SMA200 " + esc(fmtNum(s1.sma200)) + "</div>" +
      "<div>%chg " + esc(fmtNum(s1.pct_change)) + "</div>" +
      "<div>RSI " + esc(fmtNum(s1.rsi)) + "</div>" +
      "<div>W%R " + esc(fmtNum(s1.williams_r)) + "</div>" +
      "<div>ADX " + esc(fmtNum(s1.adx)) + "</div>" +
      "<div>Vol " + esc(fmtNum(s1.today_volume, 0)) + esc(volNote) + "</div>" +
      "<div>VolSMA5 " + esc(fmtNum(s1.volume_sma5, 0)) + "</div>" +
      "</div></details>"
    );
  }

  function emaStackHtml(card) {
    var ordered = card.ema_ordered || [];
    if (!ordered.length) {
      ordered = [
        { label: "EMA(5)", value: card.ema5 },
        { label: "EMA(15)", value: card.ema15 },
        { label: "EMA(50)", value: card.ema50 },
      ]
        .filter(function (x) { return x.value != null; })
        .sort(function (a, b) { return Number(b.value) - Number(a.value); });
    }
    if (!ordered.length) {
      return '<div class="mx-ema-stack"><div class="mx-ema-row">EMA warming…</div></div>';
    }
    return (
      '<div class="mx-ema-stack">' +
      ordered.map(function (row) {
        var lab = row.label || "";
        var fast = /EMA\(5\)/.test(lab);
        return (
          '<div class="mx-ema-row' + (fast ? " is-fast" : "") + '">' +
          "<span>" + esc(lab) + "</span>" +
          "<span>" + esc(fmtNum(row.value)) + "</span></div>"
        );
      }).join("") +
      "</div>"
    );
  }

  function cardHtml(card) {
    var stock = card.stock || "—";
    var status = cardStatus(card);
    var side = cardSide(card);
    var canTrade = status === "active" && !!card.trigger;
    var dir = card.ltp_dir || "flat";
    var cls = "mx-card is-" + side;
    if (status === "filters_off") cls += " is-inactive";
    if (status === "locked") cls += " is-locked";
    if (canTrade) cls += " is-trigger";
    var sidePill =
      '<span class="mx-side-pill is-' + side + '">' +
      (side === "bearish" ? "Bearish" : "Bullish") +
      "</span>";
    var pill =
      status === "active"
        ? '<span class="mx-status-pill is-active">Active</span>'
        : status === "locked"
          ? '<span class="mx-status-pill is-locked">Locked</span>'
          : '<span class="mx-status-pill is-inactive">Filters Off</span>';
    var lot =
      card.lot_size != null && card.lot_size !== ""
        ? '<span class="mx-lot">Lot ' + esc(fmtNum(card.lot_size, 0)) + "</span>"
        : '<span class="mx-lot">Lot —</span>';
    return (
      '<article class="' + cls + '" data-stock="' + esc(stock) + '" data-status="' + esc(status) + '" data-side="' + esc(side) + '">' +
      '<div class="mx-card-top">' +
        "<div><button type=\"button\" class=\"mx-sym-link mx-symbol\" data-act=\"chart\">" +
          esc(card.future_symbol || stock) + "</button>" +
        '<div class="mx-lot-row">' + lot + sidePill + pill + "</div>" +
        "</div>" +
        '<div class="mx-ltp ' + esc(dir) + '">' + esc(fmtNum(card.ltp)) + "</div>" +
      "</div>" +
      emaStackHtml(card) +
      '<div class="mx-ema-updated">' +
        "EMA @ " + esc(fmtHm(card.ema_at)) +
        " · 1st Activ @ " + esc(fmtHm(card.first_active_at)) +
      "</div>" +
      stage1Html(card.stage1) +
      '<div class="mx-card-actions">' +
        '<button type="button" class="mx-btn mx-btn-primary mx-take" data-act="take"' +
        (canTrade ? "" : " disabled") + ">Take Trade</button>" +
      "</div></article>"
    );
  }

  function render(data) {
    var loading = $("mxLoading");
    var empty = $("mxEmpty");
    var err = $("mxError");
    var grid = $("mxGrid");
    if (loading) loading.hidden = true;

    if (data && data.last_error) {
      if (err) {
        err.hidden = false;
        err.textContent = data.last_error;
      }
    } else if (err) {
      err.hidden = true;
      err.textContent = "";
    }

    $("mxScanAt").textContent = "15m scan: " + fmtTime(data && data.scan_at);
    $("mxEmaAt").textContent = "5m EMA: " + fmtTime(data && data.ema_at);
    $("mxMarket").textContent = "Market: " + ((data && data.market_status) || "—");
    if (data && typeof data.volume_filter_enabled === "boolean" && !prefsSyncing) {
      volFilterOn = !!data.volume_filter_enabled;
      syncVolFilterUi();
    }
    var ready = (data && data.ready) || [];
    var activeN = ready.filter(function (c) { return cardStatus(c) === "active"; }).length;
    var dayN = ready.length;
    $("mxCount").textContent =
      dayN > 0 ? ("Active: " + activeN + " · Day: " + dayN) : "Ready: 0";
    if (data && data.scan_running && !ready.length) {
      if (empty) {
        empty.hidden = false;
        empty.textContent = "Scanning universe (≈3 min first run)… Ready stocks will appear when Stage 1 finishes.";
      }
      if (grid) grid.hidden = true;
      return;
    }
    if (empty && !ready.length) {
      empty.textContent = "No stocks on the day board yet. The next 15m scan may add some.";
    }

    var nextKnown = {};
    ready.forEach(function (c) {
      var stock = String(c.stock || "").toUpperCase();
      nextKnown[stock] = true;
      if (cardStatus(c) === "active" && c.trigger) {
        playTradeSound(stock);
      } else {
        clearTriggerMemory(stock);
      }
    });

    if (!ready.length) {
      empty.hidden = false;
      grid.hidden = true;
      knownStocks = {};
      return;
    }
    empty.hidden = true;
    grid.hidden = false;
    // Full re-render keeps active cards on top (API sort) without removing day cards.
    grid.innerHTML = ready.map(cardHtml).join("");
    knownStocks = nextKnown;
    cardsByStock = {};
    ready.forEach(function (c) {
      cardsByStock[String(c.stock || "").toUpperCase()] = c;
    });
  }

  function toLocalInputNow() {
    try {
      var d = new Date();
      var parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      }).formatToParts(d);
      var map = {};
      parts.forEach(function (p) { map[p.type] = p.value; });
      return map.year + "-" + map.month + "-" + map.day + "T" + map.hour + ":" + map.minute;
    } catch (e) {
      return "";
    }
  }

  function fromLocalInput(v) {
    if (!v) return null;
    return String(v) + ":00+05:30";
  }

  async function loadReady() {
    try {
      var res = await fetch(apiBase() + "/api/momentx/ready", { headers: authHeaders() });
      var data = await res.json().catch(function () { return {}; });
      if (!res.ok) throw new Error((data && data.detail) || res.statusText || "Load failed");
      render(data);
      var poll = Number((data && data.poll_sec) || 5) * 1000;
      if (!Number.isFinite(poll) || poll < 2000) poll = POLL_DEFAULT_MS;
      schedulePoll(poll);
    } catch (e) {
      $("mxLoading").hidden = true;
      $("mxError").hidden = false;
      $("mxError").textContent = (e && e.message) || "Could not load MomentX";
      schedulePoll(POLL_DEFAULT_MS);
    }
  }

  function schedulePoll(ms) {
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = setTimeout(loadReady, ms);
  }

  function openTradeModal(card) {
    pendingTrade = card;
    var side = cardSide(card);
    var body =
      "Side: " + (side === "bearish" ? "Bearish" : "Bullish") + "\n" +
      "Symbol: " + (card.future_symbol || card.stock) + "\n" +
      "Stock: " + (card.stock || "—") + "\n" +
      "Lot: " + (card.lot_size != null ? card.lot_size : "—") + "\n" +
      "LTP: " + fmtNum(card.ltp) + "\n" +
      "EMA(5): " + fmtNum(card.ema5) + "\n" +
      "EMA(15): " + fmtNum(card.ema15) + "\n" +
      "EMA(50): " + fmtNum(card.ema50);
    $("mxTradeBody").textContent = body;
    var ltp = card.ltp != null ? Number(card.ltp) : "";
    $("mxTradeEntryPrice").value = ltp;
    $("mxTradeExitPrice").value = ltp;
    $("mxTradeEntryTime").value = toLocalInputNow();
    $("mxTradeModal").hidden = false;
  }

  function closeTradeModal() {
    pendingTrade = null;
    $("mxTradeModal").hidden = true;
  }

  async function confirmTrade() {
    if (!pendingTrade) return;
    var entryPrice = $("mxTradeEntryPrice").value === "" ? null : Number($("mxTradeEntryPrice").value);
    var entryTime = fromLocalInput($("mxTradeEntryTime").value);
    var exitPrice = pendingTrade.ltp != null ? Number(pendingTrade.ltp) : null;
    if (entryPrice != null && (exitPrice == null || Number.isNaN(exitPrice))) {
      exitPrice = entryPrice;
    }
    $("mxTradeExitPrice").value = exitPrice != null ? exitPrice : "";
    var body = {
      stock: pendingTrade.stock,
      side: cardSide(pendingTrade),
      future_symbol: pendingTrade.future_symbol,
      future_instrument_key: pendingTrade.future_instrument_key,
      lot_size: pendingTrade.lot_size,
      ltp: pendingTrade.ltp,
      entry_price: entryPrice,
      entry_time: entryTime,
      exit_price: exitPrice,
      exit_time: entryTime,
      ema5: pendingTrade.ema5,
      ema15: pendingTrade.ema15,
      ema50: pendingTrade.ema50,
    };
    try {
      await fetch(apiBase() + "/api/momentx/take-trade", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
    } catch (e) { /* logged best-effort */ }
    closeTradeModal();
  }

  function bind() {
    soundOn = loadSoundPref();
    setSoundUi();
    volFilterOn = loadVolFilterPref();
    syncVolFilterUi();

    $("mxSoundBtn").addEventListener("click", function () {
      soundOn = !soundOn;
      saveSoundPref(soundOn);
      setSoundUi();
      if (soundOn) unlockAudio();
    });

    var volEl = $("mxVolFilter");
    if (volEl) {
      volEl.addEventListener("change", function () {
        volFilterOn = !!volEl.checked;
        saveVolFilterPref(volFilterOn);
        pushVolFilterPref(volFilterOn, true);
      });
      // Align server with local preference; DOMContentLoaded still loads the board once.
      pushVolFilterPref(volFilterOn, false);
    }

    $("mxRefreshBtn").addEventListener("click", function () {
      $("mxLoading").hidden = false;
      loadReady();
    });

    $("mxGrid").addEventListener("click", function (ev) {
      var chartBtn = ev.target.closest("[data-act='chart']");
      if (chartBtn) {
        var cardEl0 = chartBtn.closest(".mx-card");
        var stock0 = cardEl0 ? cardEl0.getAttribute("data-stock") : "";
        var card0 = cardsByStock[String(stock0 || "").toUpperCase()] || {};
        if (typeof openMomentxChart === "function") {
          openMomentxChart({
            instrumentKey: card0.future_instrument_key,
            displaySymbol: card0.future_symbol || stock0,
            stock: stock0,
          });
        }
        return;
      }
      var btn = ev.target.closest("[data-act='take']");
      if (!btn || btn.disabled) return;
      var cardEl = btn.closest(".mx-card");
      if (!cardEl) return;
      var stock = cardEl.getAttribute("data-stock");
      var live = cardsByStock[String(stock || "").toUpperCase()];
      if (live) {
        openTradeModal(live);
        return;
      }
      var label = cardEl.querySelector(".mx-symbol");
      var ltpEl = cardEl.querySelector(".mx-ltp");
      openTradeModal({
        stock: stock,
        future_symbol: label ? label.textContent : stock,
        ltp: ltpEl ? Number(String(ltpEl.textContent).replace(/,/g, "")) : null,
      });
    });

    $("mxTradeCancel").addEventListener("click", closeTradeModal);
    $("mxTradeConfirm").addEventListener("click", confirmTrade);
    $("mxTradeModal").addEventListener("click", function (ev) {
      if (ev.target === $("mxTradeModal")) closeTradeModal();
    });

    // First user gesture unlocks audio if sound already preferred on.
    if (soundOn) {
      document.addEventListener("click", function once() {
        unlockAudio();
        document.removeEventListener("click", once);
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    bind();
    loadReady();
  });
})();
