/**
 * Future Screener — client-side filter over materialized universe snapshot.
 * Filter rule: optional categories AND together; multi-select values OR within category.
 * Empty category ⇒ omitted from the predicate (not a hidden "match all" clause).
 */
(function () {
  const API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  const DEFAULT_GRADES = ["A+", "A"];
  let allRows = [];
  let distinctGrades = [];
  let distinctReadiness = [];
  let selectedGrades = new Set(DEFAULT_GRADES);
  let selectedReadiness = new Set();
  let sortKey = "trade_score";
  let sortAsc = false;

  function $(id) {
    return document.getElementById(id);
  }

  function authHeaders() {
    const t = localStorage.getItem("trademanthan_token") || "";
    return t ? { Authorization: "Bearer " + t } : {};
  }

  function parseOptionalNumber(el) {
    const raw = String(el.value || "").trim();
    if (!raw) return null;
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }

  function applyFilters(rows) {
    const scoreMin = parseOptionalNumber($("fsScoreMin"));
    const scoreMax = parseOptionalNumber($("fsScoreMax"));
    const gradeActive = selectedGrades.size > 0;
    const readyActive = selectedReadiness.size > 0;

    return rows.filter((r) => {
      if (gradeActive) {
        const g = r.confidence_grade || "";
        if (!selectedGrades.has(g)) return false;
      }
      if (readyActive) {
        const rd = r.readiness || "";
        if (!selectedReadiness.has(rd)) return false;
      }
      if (scoreMin != null) {
        const s = Number(r.trade_score);
        if (!Number.isFinite(s) || s < scoreMin) return false;
      }
      if (scoreMax != null) {
        const s = Number(r.trade_score);
        if (!Number.isFinite(s) || s > scoreMax) return false;
      }
      return true;
    });
  }

  function cmp(a, b, key, asc) {
    let va = a[key];
    let vb = b[key];
    if (key === "trade_score" || key === "adx" || key === "pct_from_open" || key === "pullback_number") {
      va = va == null || va === "" ? (asc ? Infinity : -Infinity) : Number(va);
      vb = vb == null || vb === "" ? (asc ? Infinity : -Infinity) : Number(vb);
      if (va < vb) return asc ? -1 : 1;
      if (va > vb) return asc ? 1 : -1;
      return String(a.symbol || "").localeCompare(String(b.symbol || ""));
    }
    va = va == null ? "" : String(va);
    vb = vb == null ? "" : String(vb);
    const c = va.localeCompare(vb, undefined, { numeric: true, sensitivity: "base" });
    return asc ? c : -c;
  }

  function fmtNum(v, digits) {
    if (v == null || v === "" || !Number.isFinite(Number(v))) return "—";
    return Number(v).toLocaleString("en-IN", {
      minimumFractionDigits: digits,
      maximumFractionDigits: digits,
    });
  }

  function readinessClass(v) {
    const s = String(v || "").toUpperCase();
    if (s === "READY TO LONG") return "fs-ready-long";
    if (s === "READY TO SHORT") return "fs-ready-short";
    if (s === "WATCHING") return "fs-watching";
    return "fs-not-ready";
  }

  function updateSortHeaders() {
    document.querySelectorAll("#fsTable th[data-sort]").forEach((th) => {
      th.classList.remove("is-sorted", "asc");
      if (th.getAttribute("data-sort") === sortKey) {
        th.classList.add("is-sorted");
        if (sortAsc) th.classList.add("asc");
      }
    });
  }

  function renderTable() {
    const filtered = applyFilters(allRows).slice().sort((a, b) => cmp(a, b, sortKey, sortAsc));
    const n = filtered.length;
    $("fsMatchCount").textContent = n === 1 ? "1 symbol matches" : n + " symbols match";

    $("fsLoading").hidden = true;
    $("fsError").hidden = true;

    if (!n) {
      $("fsEmpty").hidden = false;
      $("fsTableWrap").hidden = true;
      return;
    }
    $("fsEmpty").hidden = true;
    $("fsTableWrap").hidden = false;
    updateSortHeaders();

    const tbody = $("fsTbody");
    tbody.innerHTML = filtered
      .map((r) => {
        const pb = r.pullback_number;
        return (
          "<tr>" +
          "<td><strong>" +
          escapeHtml(r.symbol || "—") +
          "</strong></td>" +
          '<td class="fs-grade">' +
          escapeHtml(r.confidence_grade || "—") +
          "</td>" +
          '<td class="' +
          readinessClass(r.readiness) +
          '">' +
          escapeHtml(r.readiness || "—") +
          "</td>" +
          '<td class="num">' +
          fmtNum(r.trade_score, 1) +
          "</td>" +
          '<td class="num">' +
          fmtNum(r.adx, 1) +
          "</td>" +
          '<td class="num">' +
          fmtNum(r.pct_from_open, 2) +
          "</td>" +
          '<td class="num">' +
          (pb == null || pb === "" ? "—" : String(pb)) +
          "</td>" +
          "<td>" +
          escapeHtml(r.candle_ts || "—") +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function rebuildFilterUi() {
    const gradeBox = $("fsGradeOptions");
    gradeBox.innerHTML = "";
    distinctGrades.forEach((v) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "fs-chip" + (selectedGrades.has(v) ? " is-on" : "");
      btn.textContent = v;
      btn.setAttribute("aria-pressed", selectedGrades.has(v) ? "true" : "false");
      btn.addEventListener("click", () => {
        if (selectedGrades.has(v)) selectedGrades.delete(v);
        else selectedGrades.add(v);
        rebuildFilterUi();
        renderTable();
      });
      gradeBox.appendChild(btn);
    });

    const readyBox = $("fsReadinessOptions");
    readyBox.innerHTML = "";
    distinctReadiness.forEach((v) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "fs-chip" + (selectedReadiness.has(v) ? " is-on" : "");
      btn.textContent = v;
      btn.setAttribute("aria-pressed", selectedReadiness.has(v) ? "true" : "false");
      btn.addEventListener("click", () => {
        if (selectedReadiness.has(v)) selectedReadiness.delete(v);
        else selectedReadiness.add(v);
        rebuildFilterUi();
        renderTable();
      });
      readyBox.appendChild(btn);
    });
  }

  function clearFilters() {
    // Back to page-load default: Grade A+/A; readiness & score unset
    selectedGrades = new Set(DEFAULT_GRADES);
    selectedReadiness = new Set();
    $("fsScoreMin").value = "";
    $("fsScoreMax").value = "";
    rebuildFilterUi();
    renderTable();
  }

  async function load() {
    $("fsLoading").hidden = false;
    $("fsEmpty").hidden = true;
    $("fsError").hidden = true;
    $("fsTableWrap").hidden = true;
    try {
      const res = await fetch(API_BASE + "/api/dashboard/future-screener", {
        headers: authHeaders(),
        cache: "no-store",
      });
      const data = await res.json();
      if (!data.success) {
        throw new Error(data.error || "Failed to load screener");
      }
      allRows = Array.isArray(data.rows) ? data.rows : [];
      distinctGrades = Array.isArray(data.distinct_grades) && data.distinct_grades.length
        ? data.distinct_grades
        : ["A+", "A", "B", "C", "C*", "D"];
      distinctReadiness =
        Array.isArray(data.distinct_readiness) && data.distinct_readiness.length
          ? data.distinct_readiness
          : ["READY TO LONG", "READY TO SHORT", "WATCHING", "NOT READY"];

      // Keep only currently selected grades that still exist; seed defaults if empty selection
      selectedGrades = new Set([...selectedGrades].filter((g) => distinctGrades.includes(g)));
      if (selectedGrades.size === 0 && allRows.length) {
        DEFAULT_GRADES.forEach((g) => {
          if (distinctGrades.includes(g)) selectedGrades.add(g);
        });
      }
      selectedReadiness = new Set(
        [...selectedReadiness].filter((r) => distinctReadiness.includes(r))
      );

      $("fsScanMeta").textContent =
        "Scan: " + (data.scan_time || "—") + (data.session_date ? " · " + data.session_date : "");
      $("fsUniverseHint").textContent =
        "Universe " + (data.total_symbols != null ? data.total_symbols : allRows.length) + " symbols · client filter";
      if (data.meta && data.meta.pct_column_label) {
        $("fsPctHeader").textContent = data.meta.pct_column_label;
      }

      rebuildFilterUi();
      renderTable();
    } catch (e) {
      $("fsLoading").hidden = true;
      $("fsError").hidden = false;
      $("fsError").textContent = "Error loading screener: " + (e && e.message ? e.message : e);
    }
  }

  function init() {
    $("fsRefreshBtn").addEventListener("click", load);
    $("fsClearBtn").addEventListener("click", clearFilters);
    $("fsScoreMin").addEventListener("input", renderTable);
    $("fsScoreMax").addEventListener("input", renderTable);
    document.querySelectorAll("#fsTable th[data-sort]").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.getAttribute("data-sort");
        if (sortKey === key) sortAsc = !sortAsc;
        else {
          sortKey = key;
          sortAsc = key === "trade_score" || key === "adx" || key === "pct_from_open" ? false : true;
        }
        renderTable();
      });
    });
    load();
  }

  document.addEventListener("DOMContentLoaded", init);
})();
