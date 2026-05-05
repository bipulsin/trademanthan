(function () {
  var API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  function authHeaders() {
    var t = localStorage.getItem("trademanthan_token") || "";
    return {
      Authorization: "Bearer " + t,
      "Content-Type": "application/json",
      Accept: "application/json",
    };
  }

  function icHtmlOrAuthMessage(status) {
    if (status === 401 || status === 403) return "Session expired — please sign in again.";
    if (status === 504) return "Gateway timeout — try again.";
    if (status >= 500) return "Server error — try again shortly.";
    return (
      "Received a web page instead of API JSON (often: session or proxy). " +
      "Sign in again, or reconnect Upstox if quotes fail."
    );
  }

  async function fj(paths, opts) {
    var lastErr = "Request failed";
    var o = opts || {};
    for (var i = 0; i < paths.length; i++) {
      try {
        var r = await fetch(API_BASE + paths[i], o);
        var txt = await r.text();
        var trimmed = txt.replace(/^\uFEFF/, "").trim();
        var t0 = trimmed ? trimmed.charAt(0) : "";

        if (!r.ok) {
          if (!trimmed || t0 === "<") {
            lastErr = icHtmlOrAuthMessage(r.status);
          } else {
            try {
              var ej = JSON.parse(trimmed);
              lastErr =
                fmtFastApiDetail(ej.detail) || ej.message || paths[i] + " HTTP " + r.status;
            } catch (_parseErr) {
              lastErr = paths[i] + " HTTP " + r.status;
            }
          }
          continue;
        }

        /* 200 + HTML on a non-/api/ path: SPA fallback or mis-proxy — keep a prior JSON/API error if we have one. */
        if (t0 === "<") {
          if (paths[i].indexOf("/api/") !== 0) {
            if (!lastErr) lastErr = icHtmlOrAuthMessage(r.status);
            continue;
          }
          throw new Error(icHtmlOrAuthMessage(r.status));
        }
        try {
          if (!trimmed) return {};
          return JSON.parse(trimmed);
        } catch (_e4) {
          throw new Error("Server response was not valid JSON.");
        }
      } catch (e) {
        if (e && (e.name === "AbortError" || /aborted/i.test(String(e.message || "")))) {
          lastErr = "Request timed out — try again.";
        } else {
          lastErr = e.message || String(e);
        }
      }
    }
    throw new Error(lastErr || "fetch failed");
  }

  /**
   * FastAPI exposes Iron Condor at /api/iron-condor/* and /iron-condor/* (see nginx-tradentical.conf proxy).
   */
  function icApiPaths(rest) {
    var p = "/api/iron-condor/" + rest;
    if (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1") {
      return [p, "/iron-condor/" + rest];
    }
    return [p];
  }

  /** Batched LTP — no JWT (server Upstox token only). */
  function universeQuotesPublicPaths() {
    return icApiPaths("universe-board-quotes-public");
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  /** FastAPI may return detail as string or list of validation objects ({msg,...}). */
  function fmtFastApiDetail(detail) {
    if (detail == null || detail === "") return "";
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      var out = [];
      for (var di = 0; di < detail.length; di++) {
        var it = detail[di];
        if (it && typeof it === "object" && typeof it.msg === "string") out.push(it.msg);
        else if (typeof it === "string") out.push(it);
        else out.push(JSON.stringify(it));
      }
      return out.join("; ");
    }
    return String(detail);
  }

  var state = {
    symbol: "",
    nxt_earning_iso: "",
    detailed: null,
    checklist: null,
    pollTimer: null,
    universeStep1Rows: [],
    universeStep1PickLocked: false,
    universeStep1AuthMerged: false,
    universeStep1QuoteGen: 0,
    universeQuoteTimer: null,
    checklistFillPollTimer: null,
    checklistPollRounds: 0,
    mtmSpark: [],
    soundEpoch: 0,
    mtmChartJs: null,
    equityChartJs: null,
  };

  function fmtPxCell(v) {
    if (v == null || v === "") return "—";
    var n = parseFloat(v);
    if (!isFinite(n) || n <= 0) return "—";
    return n.toFixed(2);
  }

  function fmtPctCell(v) {
    if (v == null || v === "") return "—";
    var n = parseFloat(v);
    if (!isFinite(n)) return "—";
    return n.toFixed(2) + "%";
  }

  /** Step 1 Next: one eligible row (No Trade) must be selected via radio. */
  function setUniverseNextFromRadio() {
    var btn = document.getElementById("gotoChecklistBtn");
    if (!btn) return;
    if (state.universeStep1PickLocked) {
      btn.disabled = true;
      return;
    }
    var rad = document.querySelector('input[name="icUniversePick"]:checked');
    btn.disabled = !rad || !String(rad.value || "").trim();
  }

  function pickerShowQuoteBanner(msg) {
    var el = document.getElementById("pickerQuoteWarn");
    if (!el) return;
    var s = msg != null ? String(msg).trim() : "";
    if (!s) {
      el.setAttribute("hidden", "hidden");
      el.textContent = "";
      el.removeAttribute("aria-label");
      return;
    }
    el.removeAttribute("hidden");
    el.textContent = s;
    el.setAttribute("aria-label", s);
  }

  /** After quotes patch: show server error, or fallback if no row has LTP (common when Upstox disconnected). */
  function quoteStatusMessageAfterPatch(qj) {
    var err = qj && qj.quotes_error != null ? String(qj.quotes_error).trim() : "";
    if (err) return err;
    var vis = filterUniverseRowsForStep1EarningsWindow(state.universeStep1Rows || []);
    if (!vis.length) return "";
    for (var i = 0; i < vis.length; i++) {
      var lp = vis[i].ltp;
      if (lp == null || lp === "") continue;
      var n = typeof lp === "number" ? lp : parseFloat(String(lp));
      if (isFinite(n) && n > 0) return "";
    }
    return (
      "Live LTP not received from the broker (table shows prev. close from DB). " +
      "Reconnect Upstox from the app broker / Settings link, then refresh this page."
    );
  }

  function bodyEl() {
    return document.body;
  }

  function applySavedTheme() {
    if (!bodyEl().classList.contains("iron-c-page")) return;
    // Match left-menu theme (tradentical_theme); legacy ic_ui_theme as fallback
    var d = localStorage.getItem("tradentical_theme") || localStorage.getItem("ic_ui_theme") || "dark";
    if (d !== "light" && d !== "dark") d = "dark";
    bodyEl().setAttribute("data-theme", d);
  }

  function bindThemeSyncForCharts() {
    try {
      var obs = new MutationObserver(function () {
        renderMtmChart();
        loadEquityCurve();
      });
      obs.observe(document.body, { attributes: true, attributeFilter: ["data-theme"] });
    } catch (_e) {}
  }

  function chartPalette() {
    var dark = bodyEl().getAttribute("data-theme") === "dark";
    return {
      text: dark ? "#e2e8f0" : "#1e293b",
      grid: dark ? "rgba(148,163,184,0.2)" : "rgba(31,56,100,0.12)",
      linePrimary: "#1f3864",
      lineAccent: "#1976d2",
      fillPrimary: dark ? "rgba(31,56,100,0.35)" : "rgba(31,56,100,0.12)",
      fillAccent: dark ? "rgba(25,118,210,0.28)" : "rgba(25,118,210,0.14)",
    };
  }

  function destroyChartJs(key) {
    if (state[key]) {
      try {
        state[key].destroy();
      } catch (_e) {}
      state[key] = null;
    }
  }

  function renderMtmChart() {
    if (typeof Chart === "undefined") return;
    destroyChartJs("mtmChartJs");
    var cnv = document.getElementById("icMtmChart");
    if (!cnv) return;
    var raw = state.mtmSpark.slice();
    if (raw.length < 2) raw = raw.length === 1 ? [raw[0], raw[0]] : [0, 0];
    var pal = chartPalette();
    var lbl = raw.map(function (_v, i) {
      return String(i + 1);
    });
    state.mtmChartJs = new Chart(cnv.getContext("2d"), {
      type: "line",
      data: {
        labels: lbl,
        datasets: [
          {
            label: "Open MTM est",
            data: raw,
            borderColor: pal.lineAccent,
            backgroundColor: pal.fillAccent,
            fill: true,
            tension: 0.35,
            pointRadius: 0,
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            ticks: { color: pal.text, font: { size: 10 }, maxTicksLimit: 8 },
            grid: { color: pal.grid },
          },
          y: {
            ticks: { color: pal.text, font: { size: 10 } },
            grid: { color: pal.grid },
          },
        },
      },
    });
  }

  function showPane(n) {
    document.querySelectorAll("[data-pane]").forEach(function (el) {
      el.style.display = el.getAttribute("data-pane") === String(n) ? "block" : "none";
    });
    document.querySelectorAll(".ic-step-pill").forEach(function (b) {
      b.setAttribute("data-active", b.getAttribute("data-step") === String(n) ? "1" : "0");
    });
    if (n !== 2) {
      stopChecklistFillPolling();
    }
    if (n === 1) {
      refreshUniverseQuotesQuiet();
    }
  }

  function skelBars(n) {
    var h = "";
    for (var i = 0; i < n; i++)
      h += '<div class="ic-skel-row"><div class="ic-skel-bar"></div><div class="ic-skel-bar"></div><div class="ic-skel-bar"></div></div>';
    return h;
  }

  function playRedSound() {
    var now = Date.now();
    if (now - state.soundEpoch < 90000) return;
    state.soundEpoch = now;
    try {
      var ctx = new (window.AudioContext || window.webkitAudioContext)();
      var o = ctx.createOscillator();
      var g = ctx.createGain();
      o.connect(g);
      g.connect(ctx.destination);
      o.frequency.value = 880;
      o.type = "sine";
      g.gain.value = 0.045;
      o.start();
      setTimeout(function () {
        o.stop();
      }, 180);
    } catch (_e) {}
    try {
      if (Notification.permission === "granted") new Notification("Iron Condor: critical advisory");
      else if (Notification.permission !== "denied") Notification.requestPermission();
    } catch (_e2) {}
  }

  function renderAlertsBar(alerts) {
    var host = document.getElementById("alertStack");
    if (!host) return;
    if (!alerts || !alerts.length) {
      host.innerHTML = "";
      return;
    }
    host.innerHTML = alerts
      .filter(function (a) {
        return !a.acknowledged;
      })
      .slice(0, 15)
      .map(function (a) {
        var sev = a.severity || "default";
        if (!a.severity && /STOP|CRITICAL/i.test(String(a.rule_code || a.alert_type || ""))) sev = "RED";
        var cls = "ic-alert-bar ic-sev-" + (sev || "default");
        var id = a.id;
        return (
          "<div class=\"" +
          cls +
          "\">" +
          "<span>" +
          esc(a.message || "") +
          "</span><span class=\"ic-num\">" +
          esc(a.rule_code || a.alert_type || "") +
          "</span>" +
          (id ? "<button type=\"button\" class=\"ic-btn-global\" data-aid=\"" + id + "\">Ack</button>" : "")
        );
      })
      .join("");
    host.querySelectorAll("button[data-aid]").forEach(function (btn) {
      btn.onclick = function () {
        ackAlert(Number(btn.getAttribute("data-aid")));
      };
    });
    alerts.some(function (a) {
      if (!a.acknowledged && /^RED|CRITICAL_RED/i.test(String(a.severity || ""))) return true;
      return !a.acknowledged && /STOP|CRITICAL/i.test(String(a.rule_code || ""));
    }) && playRedSound();
  }

  async function ackAlert(id) {
    await fj(icApiPaths("alerts/" + id + "/acknowledge"), {
      method: "POST",
      headers: authHeaders(),
      body: "{}",
    }).catch(function () {});
    refreshWorkspaceQuiet();
  }

  function renderSessionTop(sess) {
    var host = document.getElementById("sessionBannerHost");
    host.innerHTML = "";
    var line =
      sess.market_poll_active
        ? "Session · IST quotation window · polling on."
        : String(sess.banner || "Market closed — polling paused.");

    document.getElementById("sessionLine").textContent = line;

    if (sess.banner && sess.market_poll_active) {
      host.innerHTML = "<div class=\"ic-feed-banner\" role=\"status\">" + esc(sess.banner) + "</div>";
    }

    if (sess.position_verify_prompt && document.getElementById("verifyModal").getAttribute("data-show") !== "1") {
      document.getElementById("verifyModal").setAttribute("data-show", "1");
    }
  }

  async function loadSessionLine() {
    var el = document.getElementById("sessionLine");
    if (!el) return;
    for (var attempt = 0; attempt < 3; attempt++) {
      try {
        if (attempt > 0) {
          await new Promise(function (resolve) {
            setTimeout(resolve, 350 + attempt * 200);
          });
        }
        var s = await fj(icApiPaths("session"), { headers: authHeaders(), cache: "no-store" });
        renderSessionTop(s);
        return;
      } catch (_e) {}
    }
    el.textContent = "Session line unavailable — refresh or sign in again.";
  }

  function setUniverseTablePlaceholder(msg) {
    var tb = document.getElementById("pickerBody");
    if (!tb) return;
    tb.innerHTML =
      "<tr><td colspan=\"8\" class=\"ic-muted\">" + esc(msg || "Loading…") + "</td></tr>";
  }

  function parseNumOrNull(v) {
    if (v == null || v === "") return null;
    if (typeof v === "number" && isFinite(v)) return v;
    var n = parseFloat(String(v).trim());
    return isFinite(n) ? n : null;
  }

  /** YYYY-MM-DD from API (leading part of ISO datetime is enough). */
  function universeRowYmd(v) {
    if (v == null || v === "") return null;
    var s = String(v).trim().slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
  }

  /** True when nxt_earning_date is on or between prev_mth_expiry and curr_mth_expiry (endpoints inclusive). */
  function universeRowEarningsInsideExpiryWindow(row) {
    var e = universeRowYmd(row.nxt_earning_date);
    var p = universeRowYmd(row.prev_mth_expiry);
    var c = universeRowYmd(row.curr_mth_expiry);
    if (!e || !p || !c) return false;
    var lo = p <= c ? p : c;
    var hi = p <= c ? c : p;
    return e >= lo && e <= hi;
  }

  function filterUniverseRowsForStep1EarningsWindow(rows) {
    if (!Array.isArray(rows)) return [];
    return rows.filter(function (r) {
      return !universeRowEarningsInsideExpiryWindow(r);
    });
  }

  /** Public master shape → step-1 row (no LTP / no active until merged from auth grid). */
  function rowsFromApprovedUnderlyingsPayload(pub) {
    if (!pub || typeof pub !== "object") return [];
    var s = pub.symbols;
    if (!Array.isArray(s)) return [];
    var out = [];
    for (var i = 0; i < s.length; i++) {
      var r = s[i];
      if (!r || typeof r !== "object") continue;
      var sym = String(r.symbol || "").trim().toUpperCase();
      if (!sym) continue;
      var pdc = parseNumOrNull(r.previous_day_close);
      var cm = parseNumOrNull(r.curr_month_open);
      var pmc = parseNumOrNull(r.prev_mth_close);
      out.push({
        symbol: sym,
        sector: String(r.sector || "").trim(),
        instrument_key: String(r.instrument_key || "").trim(),
        previous_day_close: pdc !== null && pdc > 0 ? pdc : null,
        previous_close_as_of: String(r.previous_close_as_of || "").trim(),
        curr_month_open: cm !== null && cm > 0 ? cm : null,
        prev_mth_close: pmc !== null && pmc > 0 ? pmc : null,
        prev_mth_expiry: String(r.prev_mth_expiry || "").trim(),
        curr_mth_expiry: String(r.curr_mth_expiry || "").trim(),
        nxt_earning_date: String(r.nxt_earning_date || "").trim(),
        ltp: null,
        change_pct_day: null,
        active_position: null,
      });
    }
    out.sort(function (a, b) {
      return String(a.symbol || "").localeCompare(String(b.symbol || ""));
    });
    return out;
  }

  function universeActiveKnown(row) {
    return row.active_position === true || row.active_position === false;
  }

  /** Δ month = (LTP − curr_month_open) / curr_month_open × 100. Bands use signed % (direction matters). */
  function deltaMonthPctFromRow(row) {
    var mo = parseNumOrNull(row.curr_month_open);
    var lp = parseNumOrNull(row.ltp);
    if (mo == null || mo <= 0 || lp == null || lp <= 0) return null;
    var pct = ((lp - mo) / mo) * 100;
    return isFinite(pct) ? pct : null;
  }

  function deltaMonthChipHtml(row) {
    var pct = deltaMonthPctFromRow(row);
    if (pct == null) return "<span class=\"ic-muted\">—</span>";
    var txt = (pct >= 0 ? "+" : "") + pct.toFixed(2) + "%";
    var base = "ic-chip-delta-month ic-num ic-mono";
    var tier = "";
    var blink = "";
    if (pct < -10 || pct > 10) {
      tier = " ic-dm-red";
      blink = " ic-dm-blink";
    } else if ((pct >= -10 && pct < -8) || (pct > 8 && pct <= 10)) {
      tier = " ic-dm-red";
    } else if ((pct >= -8 && pct < -6) || (pct > 6 && pct <= 8)) {
      tier = " ic-dm-yellow";
    } else if (pct >= -6 && pct <= 6) {
      tier = " ic-dm-white";
    } else {
      // Defensive: should not hit (gaps covered by branches above).
      tier = " ic-dm-white";
    }
    return (
      '<span class="' +
      base +
      tier +
      blink +
      "\" title=\"(LTP − curr month open) / curr month open\">" +
      esc(txt) +
      "</span>"
    );
  }

  function renderUniverseStep1Row(row) {
    var sym = String(row.symbol || "").trim().toUpperCase();
    var apKnown = universeActiveKnown(row);
    var ap = !!row.active_position;
    var warn = apKnown && ap ? '<span class="ic-chip-warn ic-chip-pass">Dup</span>' : "";
    var act = !apKnown ? "—" : ap ? "Yes" : "No Trade";
    var pdcHtml = fmtPxCell(row.previous_day_close);
    var rad =
      "<span class=\"ic-muted\">—</span>";
    if (apKnown && !ap && !state.universeStep1PickLocked) {
      rad =
        "<label class=\"ic-universe-radio\"><input type=\"radio\" name=\"icUniversePick\" value=\"" +
        esc(sym) +
        "\" aria-label=\"Select " +
        esc(sym) +
        " for checklist\" /></label>";
    }
    return (
      "<tr data-sym=\"" +
      esc(sym) +
      "\">" +
      "<td class=\"ic-col-symbol\"><strong class=\"ic-mono\">" +
      esc(sym) +
      "</strong>" +
      warn +
      "</td>" +
      "<td class=\"ic-col-sector\"><span class=\"ic-chip-pass ic-chip-sector\">" +
      esc(String(row.sector || "—")) +
      "</span></td>" +
      "<td class=\"ic-col-prevclose ic-num ic-mono\">" +
      pdcHtml +
      "</td>" +
      "<td class=\"ic-col-ltp ic-num ic-mono\">" +
      fmtPxCell(row.ltp) +
      "</td>" +
      "<td class=\"ic-col-delta ic-num\">" +
      fmtPctCell(row.change_pct_day) +
      "</td>" +
      "<td class=\"ic-col-deltam ic-num\">" +
      deltaMonthChipHtml(row) +
      "</td>" +
      "<td class=\"ic-col-active\">" +
      act +
      "</td>" +
      "<td class=\"ic-col-pick\">" +
      rad +
      "</td>" +
      "</tr>"
    );
  }

  function universePickedSymbolFromDom() {
    var r = document.querySelector('input[name="icUniversePick"]:checked');
    return r && r.value ? String(r.value).trim().toUpperCase() : "";
  }

  function restoreUniverseRadioPick(symU) {
    if (!symU) return;
    var el = document.querySelector(
      'input[name="icUniversePick"][value="' + symU.replace(/"/g, "") + '"]'
    );
    if (el) el.checked = true;
  }

  function applyUniverseQuotesPatch(rows, quotesBySymbol) {
    var q = quotesBySymbol && typeof quotesBySymbol === "object" ? quotesBySymbol : {};
    for (var i = 0; i < rows.length; i++) {
      var sym = String(rows[i].symbol || "").trim().toUpperCase();
      var pq = q[sym];
      if (pq && typeof pq === "object") {
        if (pq.ltp != null && pq.ltp !== "") {
          var lp = typeof pq.ltp === "number" ? pq.ltp : parseFloat(String(pq.ltp));
          if (isFinite(lp) && lp > 0) rows[i].ltp = lp;
        }
        if (pq.change_pct_day != null && pq.change_pct_day !== "") {
          var ch = typeof pq.change_pct_day === "number" ? pq.change_pct_day : parseFloat(String(pq.change_pct_day));
          if (isFinite(ch)) rows[i].change_pct_day = ch;
        }
      }
    }
  }

  function renderUniverseStep1Table(rows) {
    var tb = document.getElementById("pickerBody");
    if (!tb) return;
    var src = Array.isArray(rows) ? rows : [];
    if (!src.length) {
      tb.innerHTML = "<tr><td colspan=\"8\" class=\"ic-muted\">No universe rows configured.</td></tr>";
      setUniverseNextFromRadio();
      return;
    }
    var vis = filterUniverseRowsForStep1EarningsWindow(src);
    if (!vis.length) {
      tb.innerHTML =
        "<tr><td colspan=\"8\" class=\"ic-muted\">No symbols to show — next earnings falls between the previous and current monthly F&amp;O expiries for every underlying (see master sheet).</td></tr>";
      wireUniverseStep1Radios();
      setUniverseNextFromRadio();
      return;
    }
    var picked = universePickedSymbolFromDom();
    tb.innerHTML = vis.map(renderUniverseStep1Row).join("");
    restoreUniverseRadioPick(picked);
    wireUniverseStep1Radios();
    setUniverseNextFromRadio();
  }

  function wireUniverseStep1Radios() {
    document.querySelectorAll('input[name="icUniversePick"]').forEach(function (inp) {
      inp.onchange = function () {
        setUniverseNextFromRadio();
      };
    });
  }

  /** Retries + 65s abort so the grid never sits forever on “Loading live LTP…”. */
  async function fetchUniverseBoardQuotesJson(maxAttempts) {
    var n = maxAttempts == null ? 3 : maxAttempts;
    var lastErr;
    for (var a = 0; a < n; a++) {
      var ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
      var tid = null;
      if (ctrl) {
        tid = setTimeout(function () {
          try {
            ctrl.abort();
          } catch (_e) {}
        }, 65000);
      }
      try {
        if (a > 0) {
          await new Promise(function (resolve) {
            setTimeout(resolve, 350 + a * 450);
          });
        }
        var qj = await fj(universeQuotesPublicPaths(), {
          cache: "no-store",
          signal: ctrl ? ctrl.signal : undefined,
        });
        if (tid) clearTimeout(tid);
        return qj;
      } catch (e) {
        lastErr = e;
        if (tid) clearTimeout(tid);
      }
    }
    throw lastErr || new Error("Universe quotes failed");
  }

  function startUniverseQuotesFetch(gen) {
    fetchUniverseBoardQuotesJson(3)
      .then(function (qj) {
        if (gen !== state.universeStep1QuoteGen) return;
        applyUniverseQuotesPatch(state.universeStep1Rows, qj.quotes_by_symbol);
        pickerShowQuoteBanner(quoteStatusMessageAfterPatch(qj));
        renderUniverseStep1Table(state.universeStep1Rows);
      })
      .catch(function () {
        if (gen !== state.universeStep1QuoteGen) return;
        pickerShowQuoteBanner(
          "Live quotes unavailable — prev close from DB. Reconnect Upstox in Settings or refresh the page."
        );
        renderUniverseStep1Table(state.universeStep1Rows);
      });
  }

  /** Re-fetch LTP / day % without reloading the grid (same quote-gen guard as initial fetch). */
  function refreshUniverseQuotesQuiet() {
    if (!state.universeStep1Rows || !state.universeStep1Rows.length) return;
    var myGen = state.universeStep1QuoteGen;
    fetchUniverseBoardQuotesJson(2)
      .then(function (qj) {
        if (myGen !== state.universeStep1QuoteGen) return;
        applyUniverseQuotesPatch(state.universeStep1Rows, qj.quotes_by_symbol);
        pickerShowQuoteBanner(quoteStatusMessageAfterPatch(qj));
        renderUniverseStep1Table(state.universeStep1Rows);
      })
      .catch(function () {
        if (myGen !== state.universeStep1QuoteGen) return;
        pickerShowQuoteBanner("Live quotes unavailable — auto-retry every 30s.");
        renderUniverseStep1Table(state.universeStep1Rows);
      });
  }

  var UNIVERSE_QUOTE_POLL_MS = 30000;

  function startUniverseQuotePolling() {
    if (state.universeQuoteTimer) {
      clearInterval(state.universeQuoteTimer);
      state.universeQuoteTimer = null;
    }
    state.universeQuoteTimer = setInterval(function () {
      refreshUniverseQuotesQuiet();
    }, UNIVERSE_QUOTE_POLL_MS);
  }

  /**
   * Public list paints first (no JWT). Signed-in tier upgrades Active? / radios then broker LTP batch.
   */
  function loadUniverseStep1Grid() {
    var tb = document.getElementById("pickerBody");
    var gen = ++state.universeStep1QuoteGen;
    state.universeStep1AuthMerged = false;
    state.universeStep1PickLocked = true;
    pickerShowQuoteBanner("");
    state.symbol = "";
    if (tb) {
      tb.innerHTML = "<tr><td colspan=\"8\" class=\"ic-muted\">Loading approved list…</td></tr>";
    }

    fj(["/api/iron-condor/approved-underlyings"], { cache: "no-store" })
      .then(function (pub) {
        if (gen !== state.universeStep1QuoteGen) return;
        if (state.universeStep1AuthMerged) return;
        var rows = rowsFromApprovedUnderlyingsPayload(pub);
        if (!rows.length) {
          setUniverseTablePlaceholder("No universe rows configured.");
          setUniverseNextFromRadio();
          return;
        }
        state.universeStep1Rows = rows;
        renderUniverseStep1Table(rows);
        pickerShowQuoteBanner("Loading live LTP…");
        startUniverseQuotesFetch(gen);
      })
      .catch(function () {
        if (gen !== state.universeStep1QuoteGen) return;
        if (state.universeStep1AuthMerged) return;
        state.universeStep1Rows = [];
        setUniverseTablePlaceholder("Could not load approved list — retry shortly.");
        setUniverseNextFromRadio();
      });

    fj(icApiPaths("universe-board-base"), {
      headers: authHeaders(),
      cache: "no-store",
    })
      .then(function (baseResp) {
        if (gen !== state.universeStep1QuoteGen) return;
        var rows = Array.isArray(baseResp.symbols) ? baseResp.symbols.slice() : [];
        rows.sort(function (a, b) {
          return String(a.symbol || "").localeCompare(String(b.symbol || ""));
        });
        state.universeStep1AuthMerged = true;
        state.universeStep1PickLocked = false;
        state.universeStep1Rows = rows;
        renderUniverseStep1Table(rows);
        pickerShowQuoteBanner("Loading live LTP from Upstox…");
        startUniverseQuotesFetch(gen);
      })
      .catch(function () {
        if (gen !== state.universeStep1QuoteGen) return;
        state.universeStep1AuthMerged = false;
        state.universeStep1PickLocked = true;
        pickerShowQuoteBanner(
          "Sign in for Active? and row radios. LTP updates use the public quote feed."
        );
        if (state.universeStep1Rows && state.universeStep1Rows.length) {
          renderUniverseStep1Table(state.universeStep1Rows);
        }
      });
  }

  /** Run as soon as DOM is ready — step 1 table does not depend on workspace. */
  function bootIronCondorUiEarly() {
    applySavedTheme();
    bindThemeSyncForCharts();
    loadSessionLine();
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) {
        refreshUniverseQuotesQuiet();
        loadSessionLine();
      }
    });
    var wa = document.getElementById("warnAck");
    if (wa) {
      wa.onchange = function () {
        updateToStrikesBtnState();
      };
    }
    startUniverseQuotePolling();
    loadUniverseStep1Grid();
  }

  function resetUniversePicker() {
    stopChecklistFillPolling();
    state.symbol = "";
    state.nxt_earning_iso = "";
    state.checklist = null;
    state.detailed = null;
    pickerShowQuoteBanner("");
    document.querySelectorAll('input[name="icUniversePick"]').forEach(function (inp) {
      inp.checked = false;
    });
    loadUniverseStep1Grid();
  }

  function chipCls(st) {
    if (st === "PASS") return "ic-chip-pass";
    if (st === "FAIL") return "ic-chip-fail";
    if (st === "WARN") return "ic-chip-warn";
    return "ic-chip-info";
  }

  /** Placeholder rows; real data replaces each line as NDJSON events arrive (no section-level loading). */
  var CHECKLIST_PLACEHOLDER_CODES = [
    "INDIA_VIX",
    "ACTIVE_SAME_STOCK",
    "SECTOR_POSITION",
    "EARNINGS_25D",
    "GAP_MOVE",
    "SPOT_CHG",
    "IV_VOL",
  ];

  function renderChecklistPlaceholders() {
    var area = document.getElementById("checklistArea");
    var cells = CHECKLIST_PLACEHOLDER_CODES.map(function (code) {
      return (
        '<div class="ic-chk-cell" data-ic-chk="' +
        esc(code) +
        '"><span class="ic-chip-info">' +
        esc(code) +
        ' · …</span> · <span class="ic-muted">—</span></div>'
      );
    }).join("");
    area.innerHTML = '<div class="ic-chk-grid" role="list">' + cells + "</div>";
  }

  function applyChipRow(chip) {
    var code = String(chip.code || "");
    if (!code) return;
    var host = document.getElementById("checklistArea");
    if (!host) return;
    var grid = host.querySelector(".ic-chk-grid");
    var root = grid || host;
    var el = root.querySelector('[data-ic-chk="' + code.replace(/"/g, "") + '"]');
    if (!el) {
      el = document.createElement("div");
      el.className = "ic-chk-cell";
      el.setAttribute("data-ic-chk", code);
      if (grid) grid.appendChild(el);
      else host.appendChild(el);
    }
    el.innerHTML =
      '<span class="' +
      chipCls(chip.status) +
      '">' +
      esc(code) +
      " · " +
      esc(chip.status) +
      '</span> · ' +
      esc(chip.message);
  }

  /** Re-fetch full checklist stream (merge) every 20s while chips still look empty/unavailable. */
  var CHECKLIST_FILL_POLL_MS = 20000;
  var CHECKLIST_POLL_MAX_ROUNDS = 30;

  function stopChecklistFillPolling() {
    if (state.checklistFillPollTimer) {
      clearInterval(state.checklistFillPollTimer);
      state.checklistFillPollTimer = null;
    }
    state.checklistPollRounds = 0;
  }

  function checklistStreamLooksIncomplete(chips) {
    if (!chips || !chips.length) return true;
    var blob = (chips || [])
      .map(function (c) {
        return String((c && c.message) || "") + " " + String((c && c.status) || "");
      })
      .join(" ");
    if (/India VIX not available/i.test(blob)) return true;
    if (/unavailable|not available|timed out|interrupted|Could not resolve/i.test(blob)) return true;
    return false;
  }

  function startChecklistFillPolling() {
    stopChecklistFillPolling();
    state.checklistPollRounds = 0;
    state.checklistFillPollTimer = setInterval(function () {
      if (!state.symbol || !state.checklist) {
        stopChecklistFillPolling();
        return;
      }
      state.checklistPollRounds++;
      if (state.checklistPollRounds > CHECKLIST_POLL_MAX_ROUNDS) {
        stopChecklistFillPolling();
        return;
      }
      if (!checklistStreamLooksIncomplete(state.checklist.chips)) {
        stopChecklistFillPolling();
        return;
      }
      runChecklistStreamMerge().catch(function () {});
    }, CHECKLIST_FILL_POLL_MS);
  }

  async function runChecklistStreamMerge() {
    if (!state.symbol) return;
    var paths = icApiPaths("checklist-stream");
    var url = API_BASE + paths[0];
    var resp = await fetch(url, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ underlying: state.symbol }),
      cache: "no-store",
    });
    if (!resp.ok) return;
    if (!resp.body || !resp.body.getReader) return;
    var reader = resp.body.getReader();
    var dec = new TextDecoder();
    var buf = "";
    var chipsAcc = [];
    while (true) {
      var rd = await reader.read();
      if (rd.done) break;
      buf += dec.decode(rd.value, { stream: true });
      var lines = buf.split("\n");
      buf = lines.pop() || "";
      for (var li = 0; li < lines.length; li++) {
        var line = lines[li].trim();
        if (!line) continue;
        var ev = JSON.parse(line);
        if (ev.kind === "chip") {
          chipsAcc.push(ev.chip);
          applyChipRow(ev.chip);
        } else if (ev.kind === "done") {
          state.checklist = {
            success: true,
            chips: chipsAcc,
            may_proceed_blocked: ev.may_proceed_blocked,
            warnings_require_ack: ev.warnings_require_ack,
            vix_value: ev.vix_value,
            vix_error: ev.vix_error,
          };
          updateToStrikesBtnState();
        }
      }
    }
  }

  /** After checklist stream finishes: enable unless FAIL, or until WARN ack when required. */
  function updateToStrikesBtnState() {
    var btn = document.getElementById("toStrikesBtn");
    var ackEl = document.getElementById("warnAck");
    if (!btn) return;
    if (!state.checklist) {
      btn.disabled = true;
      return;
    }
    if (state.checklist.may_proceed_blocked) {
      btn.disabled = true;
      return;
    }
    if (state.checklist.warnings_require_ack && !(ackEl && ackEl.checked)) {
      btn.disabled = true;
      return;
    }
    btn.disabled = false;
  }

  async function runChecklistStream() {
    if (!state.symbol) return;
    stopChecklistFillPolling();
    document.getElementById("strikeOverrideBox").style.display = "none";
    document.getElementById("strikeOverrideToggle").checked = false;
    ["ovSc", "ovBc", "ovSp", "ovBp"].forEach(function (id) {
      var el = document.getElementById(id);
      el.disabled = true;
      el.value = "";
    });
    state.detailed = null;
    state.checklist = null;
    var wa = document.getElementById("warnAck");
    if (wa) wa.checked = false;
    document.getElementById("toStrikesBtn").disabled = true;

    var paths = icApiPaths("checklist-stream");
    var url = API_BASE + paths[0];
    var resp = await fetch(url, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ underlying: state.symbol }),
      cache: "no-store",
    });
    if (!resp.ok) {
      var errBody = await resp.text();
      var msg = "Checklist stream HTTP " + resp.status;
      try {
        var trimmed = errBody.replace(/^\uFEFF/, "").trim();
        if (trimmed && trimmed.charAt(0) === "{") {
          var ej = JSON.parse(trimmed);
          msg = fmtFastApiDetail(ej.detail) || ej.message || msg;
        }
      } catch (_e) {}
      throw new Error(msg);
    }
    if (!resp.body || !resp.body.getReader) {
      throw new Error("Streaming checklist not supported in this browser.");
    }
    var reader = resp.body.getReader();
    var dec = new TextDecoder();
    var buf = "";
    var chipsAcc = [];
    while (true) {
      var rd = await reader.read();
      if (rd.done) break;
      buf += dec.decode(rd.value, { stream: true });
      var lines = buf.split("\n");
      buf = lines.pop() || "";
      for (var li = 0; li < lines.length; li++) {
        var line = lines[li].trim();
        if (!line) continue;
        var ev = JSON.parse(line);
        if (ev.kind === "chip") {
          chipsAcc.push(ev.chip);
          applyChipRow(ev.chip);
        } else if (ev.kind === "done") {
          state.checklist = {
            success: true,
            chips: chipsAcc,
            may_proceed_blocked: ev.may_proceed_blocked,
            warnings_require_ack: ev.warnings_require_ack,
            vix_value: ev.vix_value,
            vix_error: ev.vix_error,
          };
          updateToStrikesBtnState();
        }
      }
    }
    startChecklistFillPolling();
  }

  function fmtLeg(l) {
    if (!l) return "—";
    var bd = l.bid != null ? Number(l.bid).toFixed(2) : "—";
    var ak = l.ask != null ? Number(l.ask).toFixed(2) : "—";
    var oi = l.oi != null ? Math.round(l.oi) : "—";
    return Number(l.ltp || 0).toFixed(2) + " (Bid/Ask " + bd + "/" + ak + "; OI " + oi + ")";
  }

  async function analyzeDetailed(overrideMap) {
    if (!state.symbol) throw new Error("No symbol");
    var payload = { underlying: state.symbol };
    if (overrideMap) payload.strike_overrides = overrideMap;
    var host = document.getElementById("strikeCardSkeletonHost");
    host.style.display = "block";
    host.innerHTML = skelBars(4);
    try {
      var j = await fj(icApiPaths("analyze-detailed"), {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(payload),
      });
      host.style.display = "none";
      state.detailed = j.analysis;

      document.getElementById("strikeOverrideBox").style.display = "block";
      syncOverrideInputs(false);

      var a = state.detailed;
      var econ = a.economics || {};
      var lq = a.legs_quote || {};
      var hk =
        econ.hedge_gate_color === "GREEN" ? "#2e7d32" : econ.hedge_gate_color === "YELLOW" ? "#e65100" : "#c00000";
      var warns = (a.strike_selection_warnings || []).map(function (w) {
        return "<li class=\"ic-muted\">" + esc(w) + "</li>";
      });
      var wl = warns.length ? "<ul style=\"margin:8px 0 0;padding-left:20px;color:#e65100;\">" + warns.join("") + "</ul>" : "";

      document.getElementById("strikeCard").innerHTML =
        "<p><strong class=\"ic-mono\">" +
        esc(a.underlying) +
        "</strong> · Spot <span class=\"ic-num ic-mono\">₹" +
        esc(a.live && a.live.spot_ltp) +
        "</span>" +
        (a.live && a.live.underlying_change_pct_today != null
          ? " · Day <span class=\"ic-num\">" + esc(a.live.underlying_change_pct_today) + "%</span>"
          : "") +
        " · Sector " +
        esc(a.sector) +
        "</p>" +
        "<p class=\"ic-num\">Monthly ATR(14): ₹<span class=\"ic-mono\">" +
        esc(a.monthly_atr_14) +
        "</span> · Strike gap: ₹<span class=\"ic-mono\">" +
        esc(a.strike_distance) +
        "</span></p>" +
        wl +
        "<hr style=\"border-color:var(--theme-border)\">" +
        "<p style=\"margin:12px 0 6px;font-weight:700;\">SHORT strangle</p>" +
        "<p><span class=\"ic-mono\">" +
        a.strikes.sell_call +
        " CE</span> @ <span class=\"ic-mono\">" +
        fmtLeg(lq.sell_call) +
        "</span></p>" +
        "<p><span class=\"ic-mono\">" +
        a.strikes.sell_put +
        " PE</span> @ <span class=\"ic-mono\">" +
        fmtLeg(lq.sell_put) +
        "</span></p>" +
        "<p style=\"margin:12px 0 6px;font-weight:700;\">HEDGE</p>" +
        "<p><span class=\"ic-mono\">" +
        a.strikes.buy_call +
        " CE</span> @ <span class=\"ic-mono\">" +
        fmtLeg(lq.buy_call) +
        "</span></p>" +
        "<p><span class=\"ic-mono\">" +
        a.strikes.buy_put +
        " PE</span> @ <span class=\"ic-mono\">" +
        fmtLeg(lq.buy_put) +
        "</span></p>" +
        "<p style=\"margin:14px 0 6px;font-weight:700;\">Economics · lot qty</p>" +
        "<p class=\"ic-num\"><span>Premium ₹pts</span> " +
        esc(econ.premium_collected_pts) +
        " · Hedge " +
        esc(econ.hedge_cost_pts) +
        " · Net " +
        esc(econ.net_credit_pts) +
        "</p>" +
        "<p class=\"ic-num\"><span>Hedge ratio</span> " +
        Number(a.hedge_ratio).toFixed(3) +
        " · <strong style=\"color:" +
        hk +
        "\">" +
        esc(a.hedge_gate) +
        "</strong></p>" +
        "<p class=\"ic-num\"><span>MPE ₹</span> " +
        esc(econ.max_profit_rupees_est) +
        " · Max loss ₹" +
        esc(econ.max_loss_rupees_est) +
        "</p>" +
        "<p class=\"ic-num\">Breakevens ₹<span class=\"ic-mono\">" +
        econ.breakeven_lower +
        " ↔ " +
        econ.breakeven_upper +
        "</span></p>";

      document.getElementById("fsc").value = a.premiums.sell_call || "";
      document.getElementById("fbc").value = a.premiums.buy_call || "";
      document.getElementById("fsp").value = a.premiums.sell_put || "";
      document.getElementById("fbp").value = a.premiums.buy_put || "";

      syncOverrideInputs(true);
    } catch (e) {
      host.style.display = "none";
      throw e;
    }
  }

  function syncOverrideInputs(fromAnalysis) {
    var a = state.detailed;
    if (!a || !fromAnalysis) return;
    if (!document.getElementById("strikeOverrideToggle").checked) {
      document.getElementById("ovSc").value = a.strikes.sell_call;
      document.getElementById("ovBc").value = a.strikes.buy_call;
      document.getElementById("ovSp").value = a.strikes.sell_put;
      document.getElementById("ovBp").value = a.strikes.buy_put;
    }
  }

  function strikeOverridePayload() {
    if (!document.getElementById("strikeOverrideToggle").checked) return null;
    return {
      sell_call: Number(document.getElementById("ovSc").value),
      buy_call: Number(document.getElementById("ovBc").value),
      sell_put: Number(document.getElementById("ovSp").value),
      buy_put: Number(document.getElementById("ovBp").value),
    };
  }

  async function confirmEntrySave() {
    var a = state.detailed;
    if (!a) return alert("Analyze strikes first.");
    if (!document.getElementById("upstoxPlacedCk").checked) return alert('Check "I placed four orders in Upstox".');
    await fj(icApiPaths("confirm-entry"), {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        analysis: a,
        placed_orders_confirmed: true,
        fills: {
          sell_call_fill: Number(document.getElementById("fsc").value),
          buy_call_fill: Number(document.getElementById("fbc").value),
          sell_put_fill: Number(document.getElementById("fsp").value),
          buy_put_fill: Number(document.getElementById("fbp").value),
        },
        lot_size: a.economics && a.economics.lot_size,
        num_lots: Number(document.getElementById("flots").value) || 1,
        declared_next_earnings_iso: state.nxt_earning_iso || undefined,
      }),
    });
    showPane(5);
    refreshWorkspaceQuiet();
    loadEquityCurve();
    alert("Workbook ACTIVE. Maintain legs only through Upstox.");
  }

  async function refreshWorkspaceQuiet() {
    var w = await fj(icApiPaths("workspace"), { headers: authHeaders(), cache: "no-store" });
    renderAlertsBar(w.alerts || []);

    try {
      var adjBox = document.getElementById("adjustmentRecommendation");
      adjBox.style.display = "none";
      adjBox.innerHTML = "";
      var hits = (w.alerts || []).filter(function (a) {
        return String(a.rule_code || "").indexOf("ADJUST") >= 0 && !a.acknowledged && a.position_id;
      });
      if (hits.length) {
        adjBox.style.display = "block";
        adjBox.innerHTML =
          "<strong>Adjustment window</strong><p class=\"ic-muted\">" +
          esc(hits[0].message || "") +
          '</p><p class="ic-muted">Only roll/strengthen the <em>profit</em> side; never add risk on the pain side without a deliberate plan. Log fills after resting orders.</p>';
      }
    } catch (_) {}

    var d = w.dashboard || {};

    state.mtmSpark.push(Number(d.open_mtm_sum_rupees || 0));
    if (state.mtmSpark.length > 28) state.mtmSpark.shift();

    var kpis = [
      { k: "Capital", v: Number(d.trading_capital || 0).toFixed(0) },
      { k: "Deployed", v: Number(d.deployed_capital_rupees || 0).toFixed(0) },
      { k: "Open MTM", v: Number(d.open_mtm_sum_rupees || 0).toFixed(0) },
      { k: "Mo realized", v: Number(d.realized_month_rupees || 0).toFixed(0) },
      { k: "YTD realized", v: Number(d.realized_year_rupees || 0).toFixed(0) },
      { k: "Avail ₹", v: d.capital_available_est != null ? Number(d.capital_available_est).toFixed(0) : "—" },
    ];
    document.getElementById("kpiDash").innerHTML = kpis
      .map(function (t) {
        return (
          "<div class=\"ic-kpi-tile\"><div class=\"lbl\">" +
          esc(t.k) +
          "</div><div class=\"val ic-num\">" +
          esc(t.v) +
          "</div></div>"
        );
      })
      .join("");

    renderMtmChart();

    var pos = (w.positions || []).filter(function (p) {
      return String(p.status).toUpperCase() !== "CLOSED";
    });

    document.getElementById("posEmptyHint").style.display = pos.length ? "none" : "block";
    document.getElementById("posEmptyHint").innerHTML = pos.length
      ? ""
      : "<div class=\"ic-panel-empty\">No active condors.<br/><button type=\"button\" id=\"ctaEmptyIc\" class=\"ic-btn-global ic-btn-primary\">Start new Iron Condor</button></div>";

    document.getElementById("posCards").innerHTML =
      pos.length > 0
        ? pos
            .map(function (p) {
              var pkRaw = String(p.card_peak_severity || "");
              var sevHex = {
                CRITICAL_RED: "#620000",
                RED: "#C00000",
                ORANGE: "#E65100",
                YELLOW: "#F9A825",
                GREEN: "#2E7D32",
                BLUE: "#1976D2",
              };
              var ac = sevHex[pkRaw.toUpperCase()] || "";
              var border =
                ac !== ""
                  ? "border-left:6px solid " + ac + ";border-top:1px solid var(--theme-border);border-right:1px solid var(--theme-border);border-bottom:1px solid var(--theme-border)"
                  : "border:1px solid var(--theme-border)";
              var pk = esc(pkRaw || "—");
              return (
                '<div class="ic-pos-card ic-num" style="' +
                esc(border) +
                '" data-expand="0">' +
                '<div class="ic-pos-head"><span class="ic-mono">' +
                esc(p.underlying) +
                '</span> <span class="ic-pos-sector">' +
                esc(p.sector || "") +
                '</span></div>' +
                '<div class="ic-muted" style="margin-top:8px;font-size:0.8rem;line-height:1.45"><div>Expiry · <span class="ic-mono">' +
                esc(p.expiry_date) +
                '</span></div><div>Unread alert tier · <strong>' +
                pk +
                '</strong></div><div>Playbook chip · ' +
                esc(p.position_health || "—") +
                '</div></div>' +
                '<div class="ic-pos-expand">' +
                '<div class="ic-muted ic-mono" style="margin-top:10px;line-height:1.65;font-size:0.78rem;">' +
                "SL ₹ " +
                esc(p.stop_sl_call_px) +
                " / " +
                esc(p.stop_sl_put_px) +
                "<br />Adj ₹ " +
                esc(p.adjust_call_px) +
                " / " +
                esc(p.adjust_put_px) +
                "<br />Profit target ₹" +
                esc(p.profit_target_rupees) +
                "</div></div></div>"
              );
            })
            .join("")
        : "";

    wirePosExpand();
    bindEmptyCta();

    var sel = document.getElementById("closePick");
    var adj = document.getElementById("adjPick");
    var opts =
      '<option value="">—</option>' +
      pos
        .map(function (p) {
          return '<option value="' + esc(String(p.id)) + '">' + esc(p.underlying + " #" + p.id) + "</option>";
        })
        .join("");
    sel.innerHTML = opts;
    adj.innerHTML = opts;

    return w;
  }

  function wirePosExpand() {
    document.querySelectorAll(".ic-pos-card").forEach(function (c) {
      c.onclick = function () {
        var ex = c.getAttribute("data-expand") === "1";
        c.setAttribute("data-expand", ex ? "0" : "1");
        c.classList.toggle("ic-expanded", !ex);
      };
    });
  }

  function bindEmptyCta() {
    var b = document.getElementById("ctaEmptyIc");
    if (b) b.onclick = function () {showPane(1);};
  }

  async function loadEquityCurve() {
    destroyChartJs("equityChartJs");
    try {
      if (typeof Chart === "undefined") return;
      var p = await fj(icApiPaths("equity-curve"), { headers: authHeaders(), cache: "no-store" });
      var raw = (p.points || []).slice();
      var cnv = document.getElementById("icEquityChart");
      if (!cnv || !cnv.getContext) return;
      if (!raw.length) {
        return;
      }
      var lbl = raw.map(function (row) {
        var m = row.month || "";
        return typeof m === "string" ? m.slice(0, 7) : String(m || "");
      });
      var ys = raw.map(function (row) {
        return Number(row.cumulative || 0);
      });
      var pal = chartPalette();
      state.equityChartJs = new Chart(cnv.getContext("2d"), {
        type: "line",
        data: {
          labels: lbl.length ? lbl : ys.map(function (_y, i) { return "" + i; }),
          datasets: [
            {
              label: "Cumulative realized ₹",
              data: ys,
              borderColor: pal.linePrimary,
              backgroundColor: pal.fillPrimary,
              fill: true,
              tension: 0.25,
              borderWidth: 2,
              pointRadius: 2,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { labels: { color: pal.text } },
            tooltip: { mode: "index", intersect: false },
          },
          scales: {
            x: {
              ticks: { color: pal.text, maxRotation: 45, font: { size: 10 } },
              grid: { color: pal.grid },
            },
            y: {
              ticks: { color: pal.text, font: { size: 10 } },
              grid: { color: pal.grid },
            },
          },
        },
      });
    } catch (_e) {
      destroyChartJs("equityChartJs");
    }
  }

  async function pollTick() {
    try {
      var s = await fj(icApiPaths("session"), { headers: authHeaders(), cache: "no-store" });
      renderSessionTop(s);
      refreshUniverseQuotesQuiet();
      if (!s.market_poll_active) return;
      await fj(icApiPaths("poll"), { method: "POST", headers: authHeaders(), body: "{}" });
      await loadSessionLine();
      await refreshWorkspaceQuiet();
      loadEquityCurve();
    } catch (_e) {}
  }

  function startPolling() {
    if (state.pollTimer) clearInterval(state.pollTimer);
    state.pollTimer = setInterval(pollTick, 5 * 60 * 1000);
    pollTick();
  }

  document.querySelectorAll(".ic-step-pill").forEach(function (pill) {
    pill.onclick = function () {
      showPane(Number(pill.getAttribute("data-step")));
    };
  });


  document.getElementById("strikeOverrideToggle").onchange = function () {
    var on = document.getElementById("strikeOverrideToggle").checked;
    ["ovSc", "ovBc", "ovSp", "ovBp"].forEach(function (id) {
      document.getElementById(id).disabled = !on;
    });
    syncOverrideInputs(!!state.detailed);
  };

  document.getElementById("recalcStrikeBtn").onclick = async function () {
    if (!state.symbol) return;
    try {
      await analyzeDetailed(strikeOverridePayload());
    } catch (e) {
      alert(e.message);
    }
  };

  document.getElementById("gotoChecklistBtn").onclick = async function () {
    var rad = document.querySelector('input[name="icUniversePick"]:checked');
    if (!rad || !rad.value) {
      alert("Select one underlying (eligible rows only — no active duplicate on that symbol).");
      return;
    }
    state.symbol = String(rad.value).trim().toUpperCase();
    state.nxt_earning_iso = "";
    var rows = state.universeStep1Rows || [];
    for (var ri = 0; ri < rows.length; ri++) {
      if (String(rows[ri].symbol || "")
        .trim()
        .toUpperCase() === state.symbol) {
        var ned = rows[ri].nxt_earning_date;
        state.nxt_earning_iso = ned ? String(ned).trim().slice(0, 10) : "";
        break;
      }
    }
    showPane(2);
    renderChecklistPlaceholders();
    try {
      await runChecklistStream();
    } catch (e) {
      document.getElementById("checklistArea").innerHTML = "Error " + esc(e.message);
    }
  };

  document.getElementById("toStrikesBtn").onclick = async function () {
    showPane(3);
    document.getElementById("strikeCard").textContent = "Computing…";
    try {
      await analyzeDetailed(null);
    } catch (e) {
      document.getElementById("strikeCard").textContent = "Error: " + e.message;
    }
  };

  document.getElementById("btnNewIc").onclick = function () {
    resetUniversePicker();
    showPane(1);
  };

  document.getElementById("back1").onclick = function () {
    showPane(1);
  };
  document.getElementById("back2").onclick = function () {
    showPane(2);
  };
  document.getElementById("toConfirmBtn").onclick = function () {
    try {
      if (document.getElementById("strikeOverrideToggle").checked) {
        var ems = strikeOverridePayload();
        if (
          ![ems.sell_call, ems.buy_call, ems.sell_put, ems.buy_put].every(function (x) {
            return Number.isFinite(x) && x > 0;
          })
        )
          return alert("Override strikes incomplete.");
      }
    } catch (_) {}
    showPane(4);
  };
  document.getElementById("back3").onclick = function () {
    showPane(3);
  };
  document.getElementById("confirmEntryBtn").onclick = async function () {
    try {
      await confirmEntrySave();
    } catch (e) {
      alert(e.message || String(e));
    }
  };

  document.getElementById("verifyOkBtn").onclick = async function () {
    await fj(icApiPaths("session/verify-positions-held"), {
      method: "POST",
      headers: authHeaders(),
      body: "{}",
    }).catch(function () {});
    document.getElementById("verifyModal").setAttribute("data-show", "0");
    loadSessionLine();
  };

  document.getElementById("verifyDismissBtn").onclick = function () {
    document.getElementById("verifyModal").setAttribute("data-show", "0");
  };

  document.getElementById("journalCloseBtn").onclick = async function () {
    var pid = Number(document.getElementById("closePick").value);
    if (!pid) return alert("Pick position.");
    if (!document.getElementById("jxUpstoxOut").checked)
      return alert("Confirm exits were done in Upstox before saving the journal.");

    await fj(icApiPaths("close-with-journal"), {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        position_id: pid,
        squaring_confirmed: true,
        exit_reason: document.getElementById("jxReason").value,
        emotion: document.getElementById("jxEmo").value,
        followed_rules: document.getElementById("jxFollow").checked,
        deviation_notes: document.getElementById("jxDev").value,
        lesson_learned: document.getElementById("jxLes").value,
        exit_fills: {
          sell_call_exit: Number(document.getElementById("xsc").value),
          buy_call_exit: Number(document.getElementById("xbc").value),
          sell_put_exit: Number(document.getElementById("xsp").value),
          buy_put_exit: Number(document.getElementById("xbp").value),
        },
      }),
    });
    alert("Closed + journal recorded.");
    refreshWorkspaceQuiet();
    loadEquityCurve();
  };

  document.getElementById("adjSubmitBtn").onclick = async function () {
    var pid = Number(document.getElementById("adjPick").value);
    if (!pid) return alert("Pick row.");
    await fj(icApiPaths("positions/" + pid + "/log-adjustment"), {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        strikes: {
          sell_call: Number(document.getElementById("aSc").value),
          buy_call: Number(document.getElementById("aBc").value),
          sell_put: Number(document.getElementById("aSp").value),
          buy_put: Number(document.getElementById("aBp").value),
        },
        fills: {
          sell_call_fill: Number(document.getElementById("afSc").value),
          buy_call_fill: Number(document.getElementById("afBc").value),
          sell_put_fill: Number(document.getElementById("afSp").value),
          buy_put_fill: Number(document.getElementById("afBp").value),
        },
        notes: document.getElementById("adjNotes").value || null,
      }),
    }).catch(function (e) {
      alert(e.message || "");
    });
    alert("Adjustment stored.");
    refreshWorkspaceQuiet();
    loadEquityCurve();
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootIronCondorUiEarly);
  } else {
    bootIronCondorUiEarly();
  }

  fj(icApiPaths("workspace"), { headers: authHeaders() })
    .catch(function () {})
    .finally(function () {
      refreshWorkspaceQuiet();
      loadEquityCurve();
      startPolling();
      showPane(1);
    });
})();
