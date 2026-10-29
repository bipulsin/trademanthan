/* MomentX desk — polls /api/momentx/ready; sound + Take Trade confirm */
(function () {
  "use strict";

  var SOUND_KEY = "momentx_sound_enabled";
  var TRIGGERED_KEY = "momentx_triggered_symbols";
  var POLL_DEFAULT_MS = 5000;
  var audio = null;
  var soundOn = false;
  var pollTimer = null;
  var knownStocks = {};
  var pendingTrade = null;
  var audioUnlocked = false;

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

  function fmtTime(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 19);
      return d.toLocaleString("en-IN", {
        timeZone: "Asia/Kolkata",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
      });
    } catch (e) {
      return String(iso).slice(0, 19);
    }
  }

  function loadSoundPref() {
    try { return localStorage.getItem(SOUND_KEY) === "1"; } catch (e) { return false; }
  }

  function saveSoundPref(on) {
    try { localStorage.setItem(SOUND_KEY, on ? "1" : "0"); } catch (e) { /* private */ }
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

  function stage1Html(s1) {
    if (!s1) return "";
    return (
      '<details class="mx-details"><summary>Stage 1 values</summary>' +
      '<div class="mx-details-grid">' +
      "<div>Price " + esc(fmtNum(s1.price)) + "</div>" +
      "<div>SMA200 " + esc(fmtNum(s1.sma200)) + "</div>" +
      "<div>%chg " + esc(fmtNum(s1.pct_change)) + "</div>" +
      "<div>RSI " + esc(fmtNum(s1.rsi)) + "</div>" +
      "<div>W%R " + esc(fmtNum(s1.williams_r)) + "</div>" +
      "<div>ADX " + esc(fmtNum(s1.adx)) + "</div>" +
      "<div>Vol " + esc(fmtNum(s1.today_volume, 0)) + "</div>" +
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
    var active = card.active !== false;
    var trigger = !!card.trigger && active;
    var dir = card.ltp_dir || "flat";
    var cls = "mx-card";
    if (!active) cls += " is-inactive";
    if (trigger) cls += " is-trigger";
    var status = active
      ? '<span class="mx-status-pill is-active">Active</span>'
      : '<span class="mx-status-pill is-inactive">Filters off</span>';
    return (
      '<article class="' + cls + '" data-stock="' + esc(stock) + '" data-active="' + (active ? "1" : "0") + '">' +
      '<div class="mx-card-top">' +
        "<div><p class=\"mx-symbol\">" + esc(card.future_symbol || stock) + "</p>" +
        '<span class="mx-expiry">' + esc(stock) + " · curr mth FUT</span>" +
        status +
        "</div>" +
        '<div class="mx-ltp ' + esc(dir) + '">' + esc(fmtNum(card.ltp)) + "</div>" +
      "</div>" +
      emaStackHtml(card) +
      '<div class="mx-ema-updated">EMA @ ' + esc(fmtTime(card.ema_at)) + "</div>" +
      stage1Html(card.stage1) +
      '<div class="mx-card-actions">' +
        '<button type="button" class="mx-btn mx-btn-primary mx-take" data-act="take"' +
        (active ? "" : " disabled") + ">Take Trade</button>" +
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
    var ready = (data && data.ready) || [];
    var activeN = ready.filter(function (c) { return c.active !== false; }).length;
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
      var active = c.active !== false;
      if (active && c.trigger) {
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
    var body =
      "Symbol: " + (card.future_symbol || card.stock) + "\n" +
      "Stock: " + (card.stock || "—") + "\n" +
      "LTP: " + fmtNum(card.ltp) + "\n" +
      "EMA(5): " + fmtNum(card.ema5) + "\n" +
      "EMA(15): " + fmtNum(card.ema15) + "\n" +
      "EMA(50): " + fmtNum(card.ema50);
    $("mxTradeBody").textContent = body;
    $("mxTradeModal").hidden = false;
  }

  function closeTradeModal() {
    pendingTrade = null;
    $("mxTradeModal").hidden = true;
  }

  async function confirmTrade() {
    if (!pendingTrade) return;
    var body = {
      stock: pendingTrade.stock,
      future_symbol: pendingTrade.future_symbol,
      future_instrument_key: pendingTrade.future_instrument_key,
      ltp: pendingTrade.ltp,
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

    $("mxSoundBtn").addEventListener("click", function () {
      soundOn = !soundOn;
      saveSoundPref(soundOn);
      setSoundUi();
      if (soundOn) unlockAudio();
    });

    $("mxRefreshBtn").addEventListener("click", function () {
      $("mxLoading").hidden = false;
      loadReady();
    });

    $("mxGrid").addEventListener("click", function (ev) {
      var btn = ev.target.closest("[data-act='take']");
      if (!btn) return;
      var cardEl = btn.closest(".mx-card");
      if (!cardEl) return;
      var stock = cardEl.getAttribute("data-stock");
      // Re-read from current DOM payload via data attributes is limited; fetch from last render by stock text.
      var label = cardEl.querySelector(".mx-symbol");
      var ltpEl = cardEl.querySelector(".mx-ltp");
      var emas = cardEl.querySelectorAll(".mx-ema-row");
      var emaMap = {};
      emas.forEach(function (row) {
        var spans = row.querySelectorAll("span");
        if (spans.length >= 2) emaMap[spans[0].textContent.trim()] = Number(String(spans[1].textContent).replace(/,/g, ""));
      });
      openTradeModal({
        stock: stock,
        future_symbol: label ? label.textContent : stock,
        ltp: ltpEl ? Number(String(ltpEl.textContent).replace(/,/g, "")) : null,
        ema5: emaMap["EMA(5)"],
        ema15: emaMap["EMA(15)"],
        ema50: emaMap["EMA(50)"],
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
