/* At-expiry payoff chart. Same formulas as backend/services/multi_leg_payoff.py.
 *
 *   CE intrinsic = max(0, S - strike)
 *   PE intrinsic = max(0, strike - S)
 *   Open leg: (intrinsic - entry) * lot_size * (+1 BUY, -1 SELL)
 *   Exited leg (exit price and exit time both set): flat (exit_price - entry) * lot * sign
 *   Net credit is positive when premium is received.
 */
(function (global) {
  "use strict";

  var PAD = 0.125;
  var SPOT_EDGE_PAD = 0.02;
  var SAMPLE_COUNT = 201;
  var SLOPE_EPS = 1e-6;
  var ZERO_PNL = 1e-4;

  function num(raw) {
    if (raw == null || String(raw).trim() === "") return null;
    var n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }

  function legExited(raw) {
    var price = num(raw && raw.exit_price);
    var stamp = raw ? raw.exit_time : null;
    if (price == null || stamp == null) return false;
    return String(stamp).trim() !== "";
  }

  function normalizeLegs(legs) {
    var out = [];
    (legs || []).forEach(function (raw) {
      if (!raw) return;
      var side = String(raw.side || "").trim().toUpperCase();
      var right = String(raw.option_type || "").trim().toUpperCase();
      var direction = side === "BUY" ? 1 : side === "SELL" ? -1 : 0;
      var strike = num(raw.strike_price != null ? raw.strike_price : raw.strike);
      var entry = num(raw.entry_price != null ? raw.entry_price : raw.entry);
      var lot = num(raw.lot_size != null ? raw.lot_size : raw.lot);
      if (!direction || (right !== "CE" && right !== "PE")) return;
      if (strike == null || strike <= 0 || entry == null || entry < 0 || lot == null || lot <= 0) return;
      out.push({
        side: side,
        option_type: right,
        strike_price: strike,
        entry_price: entry,
        lot_size: lot,
        direction: direction,
        exited: legExited(raw),
        exit_price: num(raw.exit_price),
      });
    });
    return out;
  }

  function intrinsic(optionType, spot, strike) {
    if (optionType === "CE") return Math.max(0, spot - strike);
    return Math.max(0, strike - spot);
  }

  function payoffAt(legs, spot) {
    var total = 0;
    legs.forEach(function (leg) {
      var mark = leg.exited
        ? (leg.exit_price != null ? leg.exit_price : leg.entry_price)
        : intrinsic(leg.option_type, spot, leg.strike_price);
      total += (mark - leg.entry_price) * leg.lot_size * leg.direction;
    });
    return total;
  }

  function netCredit(legs) {
    var total = 0;
    legs.forEach(function (leg) {
      total += leg.entry_price * leg.lot_size * (-leg.direction);
    });
    return total;
  }

  function tailSlope(legs, tail) {
    var slope = 0;
    legs.forEach(function (leg) {
      if (leg.exited) return;
      if (tail === "right" && leg.option_type === "CE") slope += leg.lot_size * leg.direction;
      else if (tail === "left" && leg.option_type === "PE") slope += -leg.lot_size * leg.direction;
    });
    return slope;
  }

  function spotBounds(strikes, currentSpot) {
    var lo = Math.min.apply(null, strikes) * (1 - PAD);
    var hi = Math.max.apply(null, strikes) * (1 + PAD);
    var spot = num(currentSpot);
    if (spot != null && spot > 0) {
      if (spot < lo) lo = spot * (1 - SPOT_EDGE_PAD);
      else if (spot > hi) hi = spot * (1 + SPOT_EDGE_PAD);
    }
    if (hi <= lo) hi = lo + 1;
    return { lo: lo, hi: hi };
  }

  function samples(lo, hi, strikes) {
    var pts = [];
    var steps = SAMPLE_COUNT - 1;
    for (var i = 0; i < SAMPLE_COUNT; i++) pts.push(lo + (hi - lo) * i / steps);
    strikes.forEach(function (strike) {
      if (strike > lo && strike < hi) pts.push(strike);
    });
    pts.sort(function (a, b) { return a - b; });
    var out = [];
    pts.forEach(function (spot) {
      if (!out.length || Math.abs(spot - out[out.length - 1]) > 1e-6) out.push(spot);
    });
    return out;
  }

  function breakevens(spots, pnls) {
    var found = [];
    for (var i = 0; i < spots.length - 1; i++) {
      var y0 = pnls[i];
      var y1 = pnls[i + 1];
      var z0 = Math.abs(y0) <= ZERO_PNL;
      var z1 = Math.abs(y1) <= ZERO_PNL;
      if (z0 && z1) continue;
      if (z0) { found.push(spots[i]); continue; }
      if (z1) { found.push(spots[i + 1]); continue; }
      if (y0 * y1 < 0) {
        var denom = Math.abs(y0) + Math.abs(y1);
        found.push(spots[i] + (spots[i + 1] - spots[i]) * (Math.abs(y0) / denom));
      }
    }
    found.sort(function (a, b) { return a - b; });
    var out = [];
    found.forEach(function (spot) {
      if (!out.length || Math.abs(spot - out[out.length - 1]) > 1e-3) out.push(spot);
    });
    return out;
  }

  function analyze(legs, currentSpot) {
    var clean = normalizeLegs(legs);
    if (!clean.length) {
      return {
        legs: [],
        spotMin: null,
        spotMax: null,
        spots: [],
        pnl: [],
        breakevens: [],
        maxProfit: null,
        maxProfitUnlimited: false,
        maxLoss: null,
        maxLossUnlimited: false,
        netCredit: 0,
      };
    }
    var strikes = clean.map(function (leg) { return leg.strike_price; });
    var bounds = spotBounds(strikes, currentSpot);
    var spots = samples(bounds.lo, bounds.hi, strikes);
    var pnls = spots.map(function (spot) { return payoffAt(clean, spot); });
    var left = tailSlope(clean, "left");
    var right = tailSlope(clean, "right");
    var profitUnlimited = right > SLOPE_EPS || left < -SLOPE_EPS;
    var lossUnlimited = right < -SLOPE_EPS || left > SLOPE_EPS;
    var knots = [bounds.lo, bounds.hi];
    strikes.forEach(function (strike) {
      if (strike >= bounds.lo && strike <= bounds.hi) knots.push(strike);
    });
    var extremes = knots.map(function (spot) { return payoffAt(clean, spot); });
    return {
      legs: clean,
      spotMin: bounds.lo,
      spotMax: bounds.hi,
      spots: spots,
      pnl: pnls,
      breakevens: breakevens(spots, pnls),
      maxProfit: profitUnlimited ? null : Math.max.apply(null, extremes),
      maxProfitUnlimited: profitUnlimited,
      maxLoss: lossUnlimited ? null : Math.min.apply(null, extremes),
      maxLossUnlimited: lossUnlimited,
      netCredit: netCredit(clean),
    };
  }

  function inr(n) {
    return new Intl.NumberFormat("en-IN", {
      style: "currency",
      currency: "INR",
      maximumFractionDigits: 2,
    }).format(Number(n));
  }

  function spotText(n) {
    return new Intl.NumberFormat("en-IN", { maximumFractionDigits: 2 }).format(Number(n));
  }

  function compact(n) {
    var abs = Math.abs(n);
    var sign = n < 0 ? "-" : "";
    if (abs >= 10000000) return sign + (abs / 10000000).toFixed(1).replace(/\.0$/, "") + "Cr";
    if (abs >= 100000) return sign + (abs / 100000).toFixed(1).replace(/\.0$/, "") + "L";
    if (abs >= 1000) return sign + (abs / 1000).toFixed(abs >= 10000 ? 0 : 1).replace(/\.0$/, "") + "K";
    return sign + String(Math.round(abs));
  }

  function cssVar(name, fallback) {
    var value = "";
    try { value = getComputedStyle(document.body).getPropertyValue(name).trim(); } catch (e) { value = ""; }
    return value || fallback;
  }

  function el(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text != null) node.textContent = text;
    return node;
  }

  function stat(label, value, valueClass) {
    var wrap = el("div", "mlo-payoff-stat");
    wrap.appendChild(el("span", "mlo-payoff-lbl", label));
    wrap.appendChild(el("span", "mlo-payoff-val" + (valueClass ? " " + valueClass : ""), value));
    return wrap;
  }

  function pnlClass(n) {
    if (n == null || Number.isNaN(Number(n))) return "";
    return Number(n) >= 0 ? "mlo-pos" : "mlo-neg";
  }

  function beLabel(be, spot) {
    var text = spotText(be);
    if (!(spot > 0)) return text;
    var pct = ((be - spot) / spot) * 100;
    var sign = pct > 0 ? "+" : "";
    return text + " (" + sign + pct.toFixed(1) + "%)";
  }

  function ensureDom(host, result, model) {
    host.textContent = "";
    var stats = el("div", "mlo-payoff-strip");
    var mtm = model && model.mtm != null && Number.isFinite(Number(model.mtm)) ? Number(model.mtm) : null;
    stats.appendChild(stat("Live P&L", mtm == null ? "—" : inr(mtm), pnlClass(mtm)));
    stats.appendChild(stat(
      "Max Profit",
      result.maxProfitUnlimited ? "Unlimited" : result.maxProfit == null ? "—" : inr(result.maxProfit),
      result.maxProfitUnlimited || (result.maxProfit != null && result.maxProfit >= 0) ? "mlo-pos" : ""
    ));
    stats.appendChild(stat(
      "Max Loss",
      result.maxLossUnlimited ? "Unlimited" : result.maxLoss == null ? "—" : inr(result.maxLoss),
      result.maxLossUnlimited || (result.maxLoss != null && result.maxLoss < 0) ? "mlo-neg" : (result.maxLoss != null && result.maxLoss > 0 ? "mlo-pos" : "")
    ));
    var credit = result.legs.length ? result.netCredit : null;
    if (credit == null) {
      stats.appendChild(stat("Net Credit", "—", ""));
    } else if (credit < 0) {
      stats.appendChild(stat("Net Debit", inr(Math.abs(credit)), "mlo-neg"));
    } else {
      stats.appendChild(stat("Net Credit", inr(credit), "mlo-pos"));
    }
    var spot = num(model && model.spot);
    var beText = result.breakevens.length
      ? result.breakevens.map(function (be) { return beLabel(be, spot); }).join("  ·  ")
      : "—";
    stats.appendChild(stat("Breakevens", beText, ""));
    host.appendChild(stats);
    if (model && model.note) host.appendChild(el("p", "mlo-payoff-note", model.note));
    if (!result.spots.length) {
      host.appendChild(el(
        "p",
        "mlo-payoff-empty",
        "Payoff chart fills in once each leg has a strike, side, entry, and lot size."
      ));
      return;
    }
    var canvas = document.createElement("canvas");
    canvas.className = "mlo-payoff-canvas";
    canvas.setAttribute("aria-hidden", "true");
    host.appendChild(canvas);
  }

  function mapX(spot, result, padL, padR, width) {
    var span = result.spotMax - result.spotMin || 1;
    return padL + (spot - result.spotMin) / span * (width - padL - padR);
  }

  function draw(host) {
    var canvas = host.querySelector("canvas");
    var result = host._payoffResult;
    var model = host._payoffModel || {};
    if (!canvas || !result || !result.spots.length) return;
    var width = canvas.clientWidth;
    var height = canvas.clientHeight;
    if (width < 2 || height < 2) return;
    var dpr = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    var ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    var muted = cssVar("--muted", "#8fa398");
    var text = cssVar("--text", "#e7f0ea");
    var bg = cssVar("--bg1", "#121a17");
    var green = cssVar("--accent", "#3dba7a");
    var red = cssVar("--danger", "#e06b6b");
    var spotColor = cssVar("--warn", "#d4a017");
    var padL = 52;
    var padR = 12;
    var padT = 26;
    var padB = 28;
    var plotH = height - padT - padB;
    var minP = Math.min.apply(null, result.pnl.concat([0]));
    var maxP = Math.max.apply(null, result.pnl.concat([0]));
    var ySpan = maxP - minP || 1;
    minP -= ySpan * 0.08;
    maxP += ySpan * 0.08;
    ySpan = maxP - minP || 1;

    function mapY(pnl) {
      return padT + (maxP - pnl) / ySpan * plotH;
    }

    var yZero = mapY(0);
    ctx.fillStyle = "rgba(61, 186, 122, 0.34)";
    var lossFill = "rgba(224, 107, 107, 0.30)";
    for (var i = 0; i < result.spots.length - 1; i++) {
      var x0 = mapX(result.spots[i], result, padL, padR, width);
      var x1 = mapX(result.spots[i + 1], result, padL, padR, width);
      var y0 = mapY(result.pnl[i]);
      var y1 = mapY(result.pnl[i + 1]);
      var p0 = result.pnl[i];
      var p1 = result.pnl[i + 1];
      if (p0 === 0 && p1 === 0) continue;
      if ((p0 >= 0 && p1 >= 0) || (p0 < 0 && p1 < 0)) {
        ctx.beginPath();
        ctx.moveTo(x0, y0);
        ctx.lineTo(x1, y1);
        ctx.lineTo(x1, yZero);
        ctx.lineTo(x0, yZero);
        ctx.closePath();
        ctx.fillStyle = p0 >= 0 && p1 >= 0 ? "rgba(61, 186, 122, 0.34)" : lossFill;
        ctx.fill();
      } else {
        var t = Math.abs(p0) / (Math.abs(p0) + Math.abs(p1));
        var xc = x0 + (x1 - x0) * t;
        ctx.beginPath();
        ctx.moveTo(x0, y0);
        ctx.lineTo(xc, yZero);
        ctx.lineTo(x0, yZero);
        ctx.closePath();
        ctx.fillStyle = p0 >= 0 ? "rgba(61, 186, 122, 0.34)" : lossFill;
        ctx.fill();
        ctx.beginPath();
        ctx.moveTo(xc, yZero);
        ctx.lineTo(x1, y1);
        ctx.lineTo(x1, yZero);
        ctx.closePath();
        ctx.fillStyle = p1 >= 0 ? "rgba(61, 186, 122, 0.34)" : lossFill;
        ctx.fill();
      }
    }

    ctx.strokeStyle = muted;
    ctx.globalAlpha = 0.45;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padL, yZero);
    ctx.lineTo(width - padR, yZero);
    ctx.stroke();
    ctx.globalAlpha = 1;

    ctx.lineWidth = 2.25;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    for (var s = 0; s < result.spots.length - 1; s++) {
      var a = result.pnl[s];
      var b = result.pnl[s + 1];
      var xa = mapX(result.spots[s], result, padL, padR, width);
      var xb = mapX(result.spots[s + 1], result, padL, padR, width);
      var ya = mapY(a);
      var yb = mapY(b);
      if ((a >= 0 && b >= 0) || (a < 0 && b < 0) || a === 0 || b === 0) {
        ctx.beginPath();
        ctx.moveTo(xa, ya);
        ctx.lineTo(xb, yb);
        ctx.strokeStyle = a < 0 || b < 0 ? red : green;
        if (a === 0 && b === 0) ctx.strokeStyle = green;
        else if ((a < 0 && b === 0) || (b < 0 && a === 0)) ctx.strokeStyle = red;
        else if (a >= 0 && b >= 0) ctx.strokeStyle = green;
        ctx.stroke();
      } else {
        var frac = Math.abs(a) / (Math.abs(a) + Math.abs(b));
        var xz = xa + (xb - xa) * frac;
        ctx.beginPath();
        ctx.moveTo(xa, ya);
        ctx.lineTo(xz, yZero);
        ctx.strokeStyle = a >= 0 ? green : red;
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(xz, yZero);
        ctx.lineTo(xb, yb);
        ctx.strokeStyle = b >= 0 ? green : red;
        ctx.stroke();
      }
    }

    ctx.fillStyle = muted;
    ctx.font = '500 11px "IBM Plex Mono", ui-monospace, monospace';
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    var yTicks = 4;
    for (var y = 0; y < yTicks; y++) {
      var value = minP + (maxP - minP) * y / (yTicks - 1);
      var py = mapY(value);
      if (Math.abs(py - yZero) < 14 && Math.abs(value) > ySpan * 0.04) continue;
      ctx.fillText(value === 0 ? "0" : compact(value), padL - 8, py);
    }
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (var x = 0; x < 5; x++) {
      var sx = result.spotMin + (result.spotMax - result.spotMin) * x / 4;
      ctx.fillText(compact(sx), mapX(sx, result, padL, padR, width), height - padB + 8);
    }

    result.breakevens.forEach(function (be) {
      var bx = mapX(be, result, padL, padR, width);
      ctx.fillStyle = text;
      ctx.beginPath();
      ctx.arc(bx, yZero, 3.5, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = bg;
      ctx.beginPath();
      ctx.arc(bx, yZero, 1.6, 0, Math.PI * 2);
      ctx.fill();
      ctx.fillStyle = text;
      ctx.beginPath();
      ctx.moveTo(bx, height - padB);
      ctx.lineTo(bx - 4, height - padB + 7);
      ctx.lineTo(bx + 4, height - padB + 7);
      ctx.closePath();
      ctx.fill();
    });

    var spot = num(model.spot);
    if (spot != null && spot > 0) {
      var sxLine = mapX(spot, result, padL, padR, width);
      ctx.strokeStyle = spotColor;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(sxLine, padT);
      ctx.lineTo(sxLine, height - padB);
      ctx.stroke();
      var name = String(model.instrument || "").trim().toUpperCase();
      var label = (name ? name + " Spot: " : "Spot: ") + spotText(spot);
      ctx.font = '600 12px "IBM Plex Mono", ui-monospace, monospace';
      var labelW = ctx.measureText(label).width + 10;
      var labelX = Math.max(padL, Math.min(sxLine - labelW / 2, width - padR - labelW));
      ctx.fillStyle = bg;
      ctx.fillRect(labelX, 4, labelW, 16);
      ctx.fillStyle = spotColor;
      ctx.textAlign = "left";
      ctx.textBaseline = "top";
      ctx.fillText(label, labelX + 5, 6);
    }
  }

  function paint(host, retried) {
    var canvas = host.querySelector("canvas");
    if (!canvas) return;
    if (canvas.clientWidth < 2 || canvas.clientHeight < 2) {
      if (!retried) {
        requestAnimationFrame(function () { paint(host, true); });
      }
      return;
    }
    draw(host);
  }

  function render(host, model) {
    if (!host) return;
    var safe = model || {};
    var result = analyze(safe.legs, safe.spot);
    host.hidden = false;
    host._payoffModel = safe;
    host._payoffResult = result;
    ensureDom(host, result, safe);
    paint(host, false);
  }

  function renderPayoff(host, model) {
    var src = model || {};
    render(host, {
      legs: src.legs,
      spot: src.spot,
      instrument: src.instrument,
      mtm: src.livePnl != null ? src.livePnl : src.mtm,
      note: src.note || "",
    });
  }

  var resizeTimer = null;
  window.addEventListener("resize", function () {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function () {
      document.querySelectorAll(".mlo-payoff").forEach(function (host) {
        if (host._payoffResult && host.offsetParent) paint(host, false);
      });
    }, 150);
  });

  global.MultiLegPayoff = { analyze: analyze, render: render, renderPayoff: renderPayoff };
})(window);
