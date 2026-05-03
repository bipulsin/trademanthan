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
        var trimmed = txt.trim();
        var t0 = trimmed ? trimmed.charAt(0) : "";

        if (!r.ok) {
          if (!trimmed || t0 === "<") {
            lastErr = icHtmlOrAuthMessage(r.status);
          } else {
            try {
              var ej = JSON.parse(txt);
              lastErr =
                typeof ej.detail === "string"
                  ? ej.detail
                  : ej.message || paths[i] + " HTTP " + r.status;
            } catch (_parseErr) {
              lastErr = paths[i] + " HTTP " + r.status;
            }
          }
          continue;
        }

        if (t0 === "<") {
          throw new Error(icHtmlOrAuthMessage(r.status));
        }
        try {
          if (!trimmed) return {};
          return JSON.parse(txt);
        } catch (_e4) {
          throw new Error("Server response was not valid JSON.");
        }
      } catch (e) {
        lastErr = e.message || String(e);
      }
    }
    throw new Error(lastErr || "fetch failed");
  }

  var IC_PICK_CACHE = "tm_ic_universe_quotes_v1";
  var IC_CACHE_MS = 50000;

  function readPickerCache() {
    try {
      var raw = sessionStorage.getItem(IC_PICK_CACHE);
      if (!raw) return null;
      var o = JSON.parse(raw);
      if (!o || !o.symbols || !Array.isArray(o.symbols)) return null;
      if (typeof o.exp !== "number" || Date.now() > o.exp) return null;
      return o;
    } catch (_e) {
      return null;
    }
  }

  function writePickerCache(symbols, quotesError) {
    try {
      sessionStorage.setItem(
        IC_PICK_CACHE,
        JSON.stringify({
          symbols: symbols,
          quotes_error: quotesError || null,
          exp: Date.now() + IC_CACHE_MS,
        })
      );
    } catch (_e) {}
  }

  function pickerShowQuoteBanner(msg) {
    var el = document.getElementById("pickerQuoteWarn");
    if (!el) return;
    if (!msg) {
      el.setAttribute("hidden", "hidden");
      el.textContent = "";
      return;
    }
    el.removeAttribute("hidden");
    el.textContent = msg;
  }

  function populatePickerDatalist() {
    var dl = document.getElementById("icPickerList");
    if (!dl) return;
    var html = "";
    state.pickerSymbols.forEach(function (r) {
      html +=
        "<option value=\"" +
        esc(r.symbol) +
        "\">" +
        esc((r.sector || "") + " · " + (r.ltp != null ? Number(r.ltp).toFixed(2) : "—")) +
        "</option>";
    });
    dl.innerHTML = html;
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  var state = {
    symbol: "",
    detailed: null,
    checklist: null,
    pollTimer: null,
    pickerSymbols: [],
    mtmSpark: [],
    soundEpoch: 0,
    mtmChartJs: null,
    equityChartJs: null,
  };

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
    await fj(["/api/iron-condor/alerts/" + id + "/acknowledge", "/iron-condor/alerts/" + id + "/acknowledge"], {
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
    try {
      var s = await fj(["/api/iron-condor/session", "/iron-condor/session"], { headers: authHeaders(), cache: "no-store" });
      renderSessionTop(s);
    } catch (_) {
      document.getElementById("sessionLine").textContent = "Session unavailable";
    }
  }

  function filterPicker() {
    var q = (document.getElementById("pickerSearch").value || "").trim().toUpperCase();
    var tb = document.getElementById("pickerBody");
    if (!tb) return;
    if (!state.pickerSymbols.length) {
      tb.innerHTML = "<tr><td colspan=\"6\" class=\"ic-muted\">No universe rows yet.</td></tr>";
      return;
    }
    tb.innerHTML =
      "<tr><td colspan=\"6\"><span class=\"ic-muted\">No matches.</span></td></tr>";
    var found = [];
    state.pickerSymbols.forEach(function (row) {
      if (q && row.symbol.indexOf(q) < 0) return;
      found.push(renderPickerRow(row));
    });
    if (found.length) tb.innerHTML = found.join("");
    wirePicker(tb);
  }

  function renderPickerRow(row) {
    var warn = row.active_position ? '<span class="ic-chip-warn ic-chip-pass">Dup</span>' : "";
    var act = row.active_position ? "Yes" : "—";
    return (
      "<tr data-sym=\"" +
      esc(row.symbol) +
      "\">" +
      "<td><strong class=\"ic-mono\">" +
      esc(row.symbol) +
      "</strong>" +
      warn +
      "</td>" +
      "<td><span class=\"ic-chip-pass ic-chip-sector\">" +
      esc(row.sector) +
      "</span></td>" +
      "<td class=\"ic-num ic-mono\">" +
      (row.ltp != null ? Number(row.ltp).toFixed(2) : "—") +
      "</td>" +
      "<td class=\"ic-num\">" +
      (row.change_pct_day != null ? Number(row.change_pct_day).toFixed(2) + "%" : "—") +
      "</td>" +
      "<td class=\"ic-num\">" +
      esc(act) +
      "</td>" +
      "<td><button type=\"button\" class=\"ic-btn-global ic-btn-primary pickRow\">Analyze</button></td>" +
      "</tr>"
    );
  }

  function wirePicker(tb) {
    tb.querySelectorAll("button.pickRow").forEach(function (b) {
      b.onclick = function () {
        var tr = b.closest("tr");
        state.symbol = tr.getAttribute("data-sym") || "";
        document.getElementById("gotoChecklistBtn").disabled = false;
        tr.parentElement.querySelectorAll("tr").forEach(function (r) {
          r.style.outline = "";
        });
        tr.style.outline = "3px solid #1f3864";
      };
    });
  }

  async function loadPicker() {
    var tbody = document.getElementById("pickerBody");
    var sk = document.getElementById("pickerSkeletonHost");
    var cached = readPickerCache();

    if (cached && cached.symbols.length) {
      state.pickerSymbols = cached.symbols;
      pickerShowQuoteBanner(cached.quotes_error || "");
      populatePickerDatalist();
      if (sk) sk.style.display = "none";
      filterPicker();
    } else {
      pickerShowQuoteBanner("");
      if (sk) {
        sk.style.display = "block";
        sk.innerHTML = skelBars(3);
      }
      if (tbody)
        tbody.innerHTML = "<tr><td colspan=\"6\" class=\"ic-muted\">Loading quotes…</td></tr>";
    }

    try {
      var u = await fj(["/api/iron-condor/universe-with-quotes", "/iron-condor/universe-with-quotes"], {
        headers: authHeaders(),
        cache: "no-store",
      });
      if (sk) sk.style.display = "none";
      state.pickerSymbols = u.symbols || [];
      writePickerCache(state.pickerSymbols, u.quotes_error);
      populatePickerDatalist();
      filterPicker();
      pickerShowQuoteBanner(u.quotes_error || "");
      if (tbody && !tbody.querySelectorAll("tr[data-sym]").length) {
        tbody.innerHTML = "<tr><td colspan=\"6\" class=\"ic-muted\">Universe unavailable.</td></tr>";
      }
    } catch (e) {
      if (sk) sk.style.display = "none";
      if (!(cached && cached.symbols.length)) {
        pickerShowQuoteBanner("");
        if (tbody) {
          tbody.innerHTML =
            "<tr><td colspan=\"6\" class=\"ic-muted\">" + esc(e.message || String(e)) + "</td></tr>";
        }
      }
    }
  }

  function chipCls(st) {
    if (st === "PASS") return "ic-chip-pass";
    if (st === "FAIL") return "ic-chip-fail";
    if (st === "WARN") return "ic-chip-warn";
    return "ic-chip-info";
  }

  async function runChecklist() {
    if (!state.symbol) return;
    document.getElementById("strikeOverrideBox").style.display = "none";
    document.getElementById("strikeOverrideToggle").checked = false;
    ["ovSc", "ovBc", "ovSp", "ovBp"].forEach(function (id) {
      var el = document.getElementById(id);
      el.disabled = true;
      el.value = "";
    });

    var ed = document.getElementById("icEarningsDate").value;
    var j = await fj(["/api/iron-condor/checklist", "/iron-condor/checklist"], {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({
        underlying: state.symbol,
        declared_next_earnings_iso: ed || undefined,
      }),
    }).catch(function (e) {
      throw e;
    });
    state.checklist = j;
    state.detailed = null;
    var chips = j.chips || [];
    document.getElementById("checklistArea").innerHTML =
      chips
        .map(function (c) {
          return (
            "<div style=\"margin:8px 0;line-height:1.45\"><span class=\"" +
            chipCls(c.status) +
            "\">" +
            esc(c.code) +
            " · " +
            esc(c.status) +
            "</span> · " +
            esc(c.message) +
            "</div>"
          );
        })
        .join("") || "<span class=\"ic-muted\">—</span>";

    document.getElementById("toStrikesBtn").disabled = !!j.may_proceed_blocked;
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
      var j = await fj(["/api/iron-condor/analyze-detailed", "/iron-condor/analyze-detailed"], {
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
    await fj(["/api/iron-condor/confirm-entry", "/iron-condor/confirm-entry"], {
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
        declared_next_earnings_iso: document.getElementById("icEarningsDate").value || undefined,
      }),
    });
    showPane(5);
    refreshWorkspaceQuiet();
    loadEquityCurve();
    alert("Workbook ACTIVE. Maintain legs only through Upstox.");
  }

  async function refreshWorkspaceQuiet() {
    var w = await fj(["/api/iron-condor/workspace", "/iron-condor/workspace"], { headers: authHeaders(), cache: "no-store" });
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
      var p = await fj(["/api/iron-condor/equity-curve", "/iron-condor/equity-curve"], { headers: authHeaders(), cache: "no-store" });
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
      var s = await fj(["/api/iron-condor/session", "/iron-condor/session"], { headers: authHeaders(), cache: "no-store" });
      renderSessionTop(s);
      if (!s.market_poll_active) return;
      await fj(["/api/iron-condor/poll", "/iron-condor/poll"], { method: "POST", headers: authHeaders(), body: "{}" });
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

  document.getElementById("pickerSearch").addEventListener("input", filterPicker);

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
    showPane(2);
    document.getElementById("checklistArea").innerHTML = skelBars(3);
    try {
      await runChecklist();
    } catch (e) {
      document.getElementById("checklistArea").innerHTML = "Error " + esc(e.message);
    }
  };

  document.getElementById("toStrikesBtn").onclick = async function () {
    if (state.checklist && state.checklist.warnings_require_ack && !document.getElementById("warnAck").checked) {
      alert("Acknowledge WARN items first.");
      return;
    }
    showPane(3);
    document.getElementById("strikeCard").textContent = "Computing…";
    try {
      await analyzeDetailed(null);
    } catch (e) {
      document.getElementById("strikeCard").textContent = "Error: " + e.message;
    }
  };

  document.getElementById("btnNewIc").onclick = function () {showPane(1);};

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
    await fj(["/api/iron-condor/session/verify-positions-held", "/iron-condor/session/verify-positions-held"], {
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

    await fj(["/api/iron-condor/close-with-journal", "/iron-condor/close-with-journal"], {
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
    await fj(["/api/iron-condor/positions/" + pid + "/log-adjustment", "/iron-condor/positions/" + pid + "/log-adjustment"], {
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

  fj(["/api/iron-condor/workspace", "/iron-condor/workspace"], { headers: authHeaders() })
    .catch(function () {})
    .finally(function () {
      applySavedTheme();
      bindThemeSyncForCharts();
      loadSessionLine();
      loadPicker();
      refreshWorkspaceQuiet();
      loadEquityCurve();
      startPolling();
      showPane(1);
    });
})();
