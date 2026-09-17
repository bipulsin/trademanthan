"""TV symbol parse → fixed MCX underlyings + Upstox FUT resolve by contract month.

No user mapping table required. Webhook / desk underlyings (TV alerts):
  CRUDEOIL, NATURALGAS, COPPER, GOLDPETAL, SILVERMINI

Manual free-text also accepts GOLD, GOLDM, SILVER, CRUDEOILM, ZINC, LEAD,
NICKEL, MENTHAOIL, ALUMINIUM, etc.

TradingView webhook symbols encode the futures month in the trailing 5 chars
(letter + 4-digit year), e.g. NATURALGASV2026 → NATURALGAS Oct 2026.
Continuous forms (CRUDEOIL1!) fall back to front-month.
Free-text forms (COPPER SEP FUT / COPPERSEPFUT) map via month name → same resolve.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Resolve is expensive (full Upstox master scan). Cache per underlying + expiry month
# so concurrent DIV+GO webhooks do not each re-scan / re-download.
_RESOLVE_TTL_SEC = 6 * 3600
_resolve_lock = threading.Lock()
_resolve_cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}

_EXCHANGE_PREFIX = re.compile(
    r"^(NSE|BSE|NFO|MCX|BINANCE|BYBIT|COINBASE|CRYPTO|NYSE|NASDAQ|AMEX)\s*:\s*",
    re.I,
)
_CONT_FUT = re.compile(r"\d+!$")
# Trailing futures month letter + 4-digit year: Z2026, M2025, …
_LETTER_YEAR = re.compile(r"[A-Z]\d{4}$")
# Also accept letter + 2-digit year: Z26
_LETTER_YY = re.compile(r"[A-Z]\d{2}$")
_FUT_TOKEN = re.compile(r"^(FUT|FUTURE|FUTURES)$", re.I)
_YEAR_TOKEN = re.compile(r"^(20\d{2}|\d{2})$")
_DAY_TOKEN = re.compile(r"^([12]\d|3[01]|0?[1-9])$")

# Standard CME/TV futures month codes (full set for robustness).
FUTURES_MONTH_CODES: Dict[str, int] = {
    "F": 1,   # January
    "G": 2,   # February
    "H": 3,   # March
    "J": 4,   # April
    "K": 5,   # May
    "M": 6,   # June
    "N": 7,   # July
    "Q": 8,   # August
    "U": 9,   # September
    "V": 10,  # October
    "X": 11,  # November
    "Z": 12,  # December
}

# Calendar month names / abbreviations → 1-12 (longest keys first for concat match).
MONTH_NAME_TO_NUM: Dict[str, int] = {
    "JANUARY": 1,
    "FEBRUARY": 2,
    "MARCH": 3,
    "APRIL": 4,
    "MAY": 5,
    "JUNE": 6,
    "JULY": 7,
    "AUGUST": 8,
    "SEPTEMBER": 9,
    "OCTOBER": 10,
    "NOVEMBER": 11,
    "DECEMBER": 12,
    "SEPT": 9,
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_MONTH_NAME_KEYS: Tuple[str, ...] = tuple(
    sorted(MONTH_NAME_TO_NUM.keys(), key=len, reverse=True)
)
_MONTH_NUM_TO_CODE: Dict[int, str] = {v: k for k, v in FUTURES_MONTH_CODES.items()}

# Canonical desk names for TV webhooks (longest first for prefix match).
WEBHOOK_UNDERLYINGS: Tuple[str, ...] = (
    "NATURALGAS",
    "SILVERMINI",
    "GOLDPETAL",
    "CRUDEOIL",
    "COPPER",
)

# Broader MCX underlyings for manual free-text (longest first).
# Includes webhook set plus GOLD / SILVER / base metals / mini crude / etc.
ALLOWED_UNDERLYINGS: Tuple[str, ...] = (
    "NATURALGAS",
    "SILVERMINI",
    "GOLDPETAL",
    "MENTHAOIL",
    "ALUMINIUM",
    "CRUDEOILM",
    "CRUDEOIL",
    "SILVER",
    "COPPER",
    "NICKEL",
    "GOLDM",
    "GOLD",
    "ZINC",
    "LEAD",
)

# TV / MCX short cores → desk canonical (SILVERMX2026 → SILVERM → SILVERMINI).
TV_CORE_ALIASES: Dict[str, str] = {
    "SILVERM": "SILVERMINI",
    "NATGAS": "NATURALGAS",
    "ALUMINUM": "ALUMINIUM",
}

# Lookup keys tried against Upstox master (SILVERMINI → SILVERM on Upstox).
UPSTOX_RESOLVE_ALIASES: Dict[str, List[str]] = {
    "CRUDEOIL": ["CRUDEOIL"],
    "CRUDEOILM": ["CRUDEOILM"],
    "NATURALGAS": ["NATURALGAS"],
    "COPPER": ["COPPER"],
    "GOLDPETAL": ["GOLDPETAL"],
    "GOLDM": ["GOLDM"],
    "GOLD": ["GOLD"],
    "SILVERMINI": ["SILVERM", "SILVERMINI"],
    "SILVER": ["SILVER"],
    "ZINC": ["ZINC"],
    "LEAD": ["LEAD"],
    "NICKEL": ["NICKEL"],
    "MENTHAOIL": ["MENTHAOIL"],
    "ALUMINIUM": ["ALUMINIUM"],
}

# Exact underlying_symbol to keep on Upstox master.
# Desk webhook set still prefers full CRUDEOIL / NATURALGAS (not mini variants).
PREFERRED_UNDERLYING_SYMBOL: Dict[str, str] = {
    "CRUDEOIL": "CRUDEOIL",
    "CRUDEOILM": "CRUDEOILM",
    "NATURALGAS": "NATURALGAS",
    "COPPER": "COPPER",
    "GOLDPETAL": "GOLDPETAL",
    "GOLDM": "GOLDM",
    "GOLD": "GOLD",
    "SILVERMINI": "SILVERM",
    "SILVER": "SILVER",
    "ZINC": "ZINC",
    "LEAD": "LEAD",
    "NICKEL": "NICKEL",
    "MENTHAOIL": "MENTHAOIL",
    "ALUMINIUM": "ALUMINIUM",
}


def _strip_to_core(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip().strip('"').strip("'").upper()
    if not s or ("{{" in s and "}}" in s):
        return None
    s = _EXCHANGE_PREFIX.sub("", s).strip()
    if ":" in s:
        s = s.split(":")[-1].strip()
    s = re.sub(r"\s+", "", s)
    # Spaces already removed — strip trailing FUT / FUTURES (COPPERSEPFUT → COPPERSEP).
    for suffix in ("FUTURES", "FUTURE", "FUT"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[: -len(suffix)]
            break
    return s or None


def _normalize_year_token(tok: str) -> Optional[int]:
    if not _YEAR_TOKEN.fullmatch(tok):
        return None
    try:
        y = int(tok)
    except ValueError:
        return None
    if y < 100:
        y = 2000 + y
    if y < 2000 or y > 2100:
        return None
    return y


def infer_contract_year(
    month: int, *, as_of: Optional[datetime] = None, year_hint: Optional[int] = None
) -> int:
    """
    Pick contract year for a calendar month when user omitted / partially gave year.

    Prefer current calendar year when that month is still upcoming or current;
    otherwise next year (nearest upcoming). Explicit year_hint wins when valid.
    """
    if year_hint is not None:
        y = int(year_hint)
        if y < 100:
            y = 2000 + y
        return y
    now = as_of or datetime.now(timezone.utc)
    m = int(month)
    if m < 1 or m > 12:
        return int(now.year)
    if m < int(now.month):
        return int(now.year) + 1
    return int(now.year)


def _match_underlying_prefix(blob: str) -> Optional[Tuple[str, str]]:
    """
    Longest-first underlying match against a compacted string.
    Returns (canonical, remainder) or None.
    """
    if not blob:
        return None
    if blob in TV_CORE_ALIASES:
        return TV_CORE_ALIASES[blob], ""
    if blob in ALLOWED_UNDERLYINGS:
        return blob, ""

    candidates: List[Tuple[str, str, str]] = []
    for name in ALLOWED_UNDERLYINGS:
        if blob.startswith(name):
            candidates.append((name, name, blob[len(name) :]))
    for alias, canon in TV_CORE_ALIASES.items():
        if blob.startswith(alias):
            candidates.append((alias, canon, blob[len(alias) :]))
    if not candidates:
        # Letter+year / continuous already stripped elsewhere — try canonicalize.
        canon = _canonicalize_core(blob)
        if canon:
            return canon, ""
        return None
    # Longest matched key wins (CRUDEOILM before CRUDEOIL, GOLDPETAL before GOLD).
    candidates.sort(key=lambda t: len(t[0]), reverse=True)
    _key, canon, rest = candidates[0]
    return canon, rest


def _extract_month_from_blob(blob: str) -> Optional[Tuple[int, str, str]]:
    """
    Find a month name/abbr inside compacted text.
    Returns (month_num, before, after) for the leftmost longest match, or None.
    """
    if not blob:
        return None
    best: Optional[Tuple[int, int, int, str]] = None  # start, end, month, key
    for key in _MONTH_NAME_KEYS:
        idx = blob.find(key)
        if idx < 0:
            continue
        end = idx + len(key)
        # Avoid matching inside longer tokens when possible — prefer start/boundary.
        month = MONTH_NAME_TO_NUM[key]
        if best is None or idx < best[0] or (idx == best[0] and len(key) > best[3].__len__()):
            best = (idx, end, month, key)
    if best is None:
        return None
    idx, end, month, _key = best
    return month, blob[:idx], blob[end:]


def parse_free_text_commodity(
    raw: Optional[str], *, as_of: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Parse flexible manual-entry commodity text into underlying + optional month/year.

    Accepts forms like:
      COPPER SEP FUT / COPPER SEPT FUT / COPPER SEPTEMBER FUT
      COPPER FUT SEP / SEP COPPER FUT
      COPPERSEPFUT
      COPPER 30 SEP 26 / COPPER FUT 30 SEP 26
      COPPER (front-month)
      NATURALGAS OCT FUT / SILVERM NOV / GOLD DEC 26
    """
    out: Dict[str, Any] = {
        "underlying": None,
        "month": None,
        "year": None,
        "day": None,
        "month_code": None,
        "contract_code": None,
        "parse_mode": None,
    }
    if raw is None:
        return out
    s = str(raw).strip().strip('"').strip("'").upper()
    if not s or ("{{" in s and "}}" in s):
        return out
    s = _EXCHANGE_PREFIX.sub("", s).strip()
    if ":" in s:
        s = s.split(":")[-1].strip()
    s = s.replace("!", " ")
    # Tokenize on non-alnum; keep alnum runs.
    tokens = [t for t in re.split(r"[^A-Z0-9]+", s) if t]
    if not tokens:
        return out

    month: Optional[int] = None
    year: Optional[int] = None
    day: Optional[int] = None
    leftover: List[str] = []

    for tok in tokens:
        if _FUT_TOKEN.fullmatch(tok):
            continue
        if month is None and tok in MONTH_NAME_TO_NUM:
            month = MONTH_NAME_TO_NUM[tok]
            continue
        y = _normalize_year_token(tok)
        if y is not None and year is None and (
            len(tok) == 4 or (month is not None or day is not None or leftover)
        ):
            # Bare 2-digit year only after we already saw commodity/month/day context
            # (avoid treating GOLD "26" alone as year without month — still OK with month).
            if len(tok) == 4 or month is not None or day is not None:
                year = y
                continue
        if day is None and _DAY_TOKEN.fullmatch(tok) and not tok.isalpha():
            # Prefer day when token looks like 1-31 and month not yet from this token.
            d = int(tok)
            if 1 <= d <= 31 and (month is not None or leftover):
                # Defer: 26 could be year; if month already set and no year, prefer year for 2-digit.
                if month is not None and year is None and len(tok) == 2 and d >= 20:
                    year = 2000 + d
                    continue
                day = d
                continue
        leftover.append(tok)

    # Second pass on leftover: 2-digit years that were deferred, or day before month order.
    cleaned: List[str] = []
    for tok in leftover:
        y = _normalize_year_token(tok)
        if y is not None and year is None and month is not None and len(tok) == 2:
            year = y
            continue
        if day is None and _DAY_TOKEN.fullmatch(tok) and tok.isdigit():
            d = int(tok)
            if 1 <= d <= 31:
                day = d
                continue
        cleaned.append(tok)
    leftover = cleaned

    underlying: Optional[str] = None
    # Prefer joined leftover tokens as underlying (COPPER, NATURAL GAS → NATURALGAS).
    joined = "".join(leftover)
    if joined:
        hit = _match_underlying_prefix(joined)
        if hit and hit[1] == "":
            underlying = hit[0]
        elif hit and hit[1]:
            # e.g. leftover compacted still has month residue
            underlying = hit[0]
            rest = hit[1]
            if month is None:
                extracted = _extract_month_from_blob(rest)
                if extracted:
                    month, before, after = extracted
                    rem = (before + after).strip()
                    if rem and year is None:
                        y = _normalize_year_token(rem)
                        if y:
                            year = y
                        elif rem.isdigit() and day is None and 1 <= int(rem) <= 31:
                            day = int(rem)
        else:
            underlying = _canonicalize_core(joined)

    # Concatenated full string fallback (COPPERSEPFUT / COPPER30SEP26).
    if underlying is None or month is None:
        compact = re.sub(r"[^A-Z0-9]", "", s)
        for suffix in ("FUTURES", "FUTURE", "FUT"):
            if compact.endswith(suffix) and len(compact) > len(suffix):
                compact = compact[: -len(suffix)]
                break
        # Strip TV letter+year if present — defer to TV path normally, but tolerate mix.
        if underlying is None:
            tv_u = parse_underlying(raw)
            if tv_u:
                underlying = tv_u
        if month is None or underlying is None:
            hit = _match_underlying_prefix(compact)
            if hit:
                und, rest = hit
                if underlying is None:
                    underlying = und
                if month is None and rest:
                    # Optional leading day digits: 30SEP26
                    m_day = re.match(r"^(\d{1,2})([A-Z].*)$", rest)
                    if m_day:
                        d = int(m_day.group(1))
                        if 1 <= d <= 31 and day is None:
                            day = d
                        rest = m_day.group(2)
                    extracted = _extract_month_from_blob(rest)
                    if extracted:
                        month, before, after = extracted
                        rem = (before + after).strip()
                        if rem and year is None:
                            y = _normalize_year_token(rem)
                            if y:
                                year = y
                            elif rem.isdigit() and len(rem) <= 2 and day is None:
                                d = int(rem)
                                if 1 <= d <= 31:
                                    day = d

    if underlying is None:
        # Last resort: TV / continuous forms already handled by parse_underlying.
        underlying = parse_underlying(raw)
        if underlying and month is None:
            mi = parse_tv_month_code(raw)
            if mi:
                out.update(
                    {
                        "underlying": underlying,
                        "month": mi.get("month"),
                        "year": mi.get("year"),
                        "month_code": mi.get("month_code"),
                        "contract_code": mi.get("contract_code"),
                        "parse_mode": "tv",
                    }
                )
                return out

    if not underlying:
        return out

    if month is not None and year is None:
        year = infer_contract_year(month, as_of=as_of)
    month_code = _MONTH_NUM_TO_CODE.get(int(month)) if month else None
    contract_code = f"{month_code}{year}" if month_code and year else None
    out.update(
        {
            "underlying": underlying,
            "month": int(month) if month else None,
            "year": int(year) if year else None,
            "day": int(day) if day else None,
            "month_code": month_code,
            "contract_code": contract_code,
            "parse_mode": "free_text" if month or day else "free_text_front",
        }
    )
    return out


def parse_tv_month_code(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Extract trailing futures month letter + year from a TV symbol.

    Returns dict with month_code, month (1-12), year, contract_code (e.g. V2026),
    or None when absent / invalid letter.
    """
    s = _strip_to_core(raw)
    if not s:
        return None
    s = _CONT_FUT.sub("", s).replace("!", "").strip()
    m4 = _LETTER_YEAR.search(s)
    if m4:
        token = m4.group(0)
        letter, year_s = token[0], token[1:]
        month = FUTURES_MONTH_CODES.get(letter)
        if month is None:
            return None
        try:
            year = int(year_s)
        except ValueError:
            return None
        if year < 2000 or year > 2100:
            return None
        return {
            "month_code": letter,
            "month": month,
            "year": year,
            "contract_code": token,
        }
    m2 = _LETTER_YY.search(s)
    if m2 and len(s) > 3:
        token = m2.group(0)
        letter, yy = token[0], token[1:]
        month = FUTURES_MONTH_CODES.get(letter)
        if month is None:
            return None
        try:
            year = 2000 + int(yy)
        except ValueError:
            return None
        # Only treat as month code when stripping leaves a plausible core.
        core = _LETTER_YY.sub("", s)
        if not (
            core in ALLOWED_UNDERLYINGS
            or core in TV_CORE_ALIASES
            or any(core == u or core.startswith(u) for u in ALLOWED_UNDERLYINGS)
        ):
            return None
        return {
            "month_code": letter,
            "month": month,
            "year": year,
            "contract_code": f"{letter}{year}",
        }
    return None


def normalize_tv_ticker(raw: Optional[str]) -> Optional[str]:
    """Strip exchange / continuous / letter+year → best-effort core ticker."""
    s = _strip_to_core(raw)
    if not s:
        return None
    s = _CONT_FUT.sub("", s).replace("!", "").strip()
    if _LETTER_YEAR.search(s):
        s = _LETTER_YEAR.sub("", s)
    elif _LETTER_YY.search(s) and len(s) > 3:
        candidate = _LETTER_YY.sub("", s)
        if any(candidate == u or candidate.startswith(u) for u in ALLOWED_UNDERLYINGS):
            s = candidate
    return s or None


def _canonicalize_core(core: str) -> Optional[str]:
    """Map a stripped ticker core to an allowed underlying (incl. SILVERM alias)."""
    if not core:
        return None
    if core in ALLOWED_UNDERLYINGS:
        return core
    alias = TV_CORE_ALIASES.get(core)
    if alias and alias in ALLOWED_UNDERLYINGS:
        return alias
    for u in ALLOWED_UNDERLYINGS:
        if not core.startswith(u):
            continue
        rest = core[len(u) :]
        if rest == "":
            return u
        if _LETTER_YEAR.fullmatch(rest) or _LETTER_YY.fullmatch(rest):
            return u
        if re.fullmatch(r"[A-Z]", rest):
            return u
    # SILVERMX2026 / SILVERMZ26 → strip month code → SILVERM → SILVERMINI
    stripped = core
    if _LETTER_YEAR.search(stripped):
        stripped = _LETTER_YEAR.sub("", stripped)
    elif _LETTER_YY.search(stripped) and len(stripped) > 3:
        stripped = _LETTER_YY.sub("", stripped)
    if stripped != core:
        return _canonicalize_core(stripped)
    return None


def parse_underlying(symbol_raw: str) -> Optional[str]:
    """
    Map TradingView symbol to one of ALLOWED_UNDERLYINGS, else None.

    Accepts:
      CRUDEOIL1!, MCX:CRUDEOIL1!, CRUDEOILZ2026, CRUDEOIL, NATURALGAS1!, …
      SILVERMX2026, SILVERM (→ SILVERMINI / Upstox SILVERM)
    """
    core = _strip_to_core(symbol_raw)
    if not core:
        return None
    core = _CONT_FUT.sub("", core).replace("!", "").strip()
    return _canonicalize_core(core)


def parse_tv_symbol(symbol_raw: str) -> Dict[str, Any]:
    """
    Parse TV symbol into underlying + optional contract month/year.

    Example: NATURALGASV2026 → underlying=NATURALGAS, month=10, year=2026.
    Falls back to free-text month names (COPPER SEP FUT) when letter+year absent.
    """
    underlying = parse_underlying(symbol_raw)
    month_info = parse_tv_month_code(symbol_raw) if underlying else None
    out: Dict[str, Any] = {
        "underlying": underlying,
        "month_code": None,
        "month": None,
        "year": None,
        "contract_code": None,
        "parse_mode": "tv" if underlying else None,
    }
    if month_info:
        out.update(month_info)
        out["parse_mode"] = "tv"
        return out

    # Free-text: month names / concatenated forms / broader underlyings.
    ft = parse_free_text_commodity(symbol_raw)
    if ft.get("underlying"):
        out["underlying"] = ft["underlying"]
        out["parse_mode"] = ft.get("parse_mode") or "free_text"
        if ft.get("month"):
            out["month"] = ft.get("month")
            out["year"] = ft.get("year")
            out["month_code"] = ft.get("month_code")
            out["contract_code"] = ft.get("contract_code")
    return out


def _cache_key(
    canonical: str,
    *,
    expiry_year: Optional[int] = None,
    expiry_month: Optional[int] = None,
) -> str:
    base = (canonical or "").strip().upper()
    if expiry_year and expiry_month:
        return f"{base}:{int(expiry_year):04d}-{int(expiry_month):02d}"
    return f"{base}:FRONT"


def _pick_front_month(futures: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not futures:
        return None
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    live = [f for f in futures if int(f.get("expiry_ms") or 0) >= now_ms - 7 * 86400_000]
    return (live or futures)[0]


def _pick_exact_month(
    futures: List[Dict[str, Any]], *, year: int, month: int
) -> Optional[Dict[str, Any]]:
    """Pick MCX FUT whose expiry calendar month/year matches the TV contract month."""
    matches = [
        f
        for f in futures
        if _expiry_ym(f.get("expiry_ms")) == (int(year), int(month))
    ]
    if not matches:
        return None
    # Prefer earliest expiry within that month (standard monthly contract).
    matches.sort(key=lambda f: int(f.get("expiry_ms") or 0))
    return matches[0]


def _expiry_ym(expiry_ms: Any) -> Optional[Tuple[int, int]]:
    try:
        ms = int(expiry_ms or 0)
    except (TypeError, ValueError):
        return None
    if ms <= 0:
        return None
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return (dt.year, dt.month)


def _result_from_pick(
    pick: Dict[str, Any],
    *,
    prefer_und: str,
    match_mode: str,
    requested_ym: Optional[str] = None,
    fallback_from: Optional[str] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "instrument_key": pick.get("instrument_key"),
        "trading_symbol": pick.get("trading_symbol"),
        "lot_size": int(pick.get("lot_size") or 1),
        "segment": pick.get("segment"),
        "expiry": pick.get("expiry"),
        "resolved_as": prefer_und,
        "underlying_symbol": pick.get("underlying_symbol"),
        "match_mode": match_mode,
        "requested_expiry_ym": requested_ym,
        "expiry_fallback": bool(fallback_from),
        "fallback_from": fallback_from,
    }
    return out


def resolve_mcx_instrument(
    upstox_symbol: str,
    exchange: str = "MCX",
    *,
    expiry_year: Optional[int] = None,
    expiry_month: Optional[int] = None,
    allow_front_month_fallback: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Resolve MCX FUT via Upstox complete master.

    When expiry_year + expiry_month are set, prefer the contract whose expiry
    falls in that calendar month (TV letter+year). Otherwise front-month.
    If exact month is missing and allow_front_month_fallback is True, fall back
    to front-month with expiry_fallback=True and a warning log — never silent.
    """
    sym = (upstox_symbol or "").strip().upper()
    if not sym:
        return None
    prefer_und = PREFERRED_UNDERLYING_SYMBOL.get(sym, sym)
    want_exact = expiry_year is not None and expiry_month is not None
    requested_ym = (
        f"{int(expiry_year):04d}-{int(expiry_month):02d}" if want_exact else None
    )
    try:
        from backend.services.divtest.instruments import _parse_expiry_ms, ensure_instrument_master

        rows = ensure_instrument_master()
        fut_rows: List[Dict[str, Any]] = []
        for r in rows:
            t = str(r.get("instrument_type") or "").upper()
            seg = str(r.get("segment") or "").upper()
            if "FUT" not in t or "MCX" not in seg or r.get("weekly"):
                continue
            und = str(r.get("underlying_symbol") or "").strip().upper()
            if und == prefer_und:
                fut_rows.append(r)

        futures: List[Dict[str, Any]] = []
        for r in fut_rows:
            exp_ms = _parse_expiry_ms(r.get("expiry"))
            if not exp_ms:
                continue
            futures.append(
                {
                    "instrument_key": r.get("instrument_key"),
                    "trading_symbol": r.get("trading_symbol"),
                    "segment": r.get("segment"),
                    "lot_size": int(r.get("lot_size") or r.get("minimum_lot") or 1),
                    "expiry": datetime.fromtimestamp(
                        exp_ms / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d"),
                    "expiry_ms": exp_ms,
                    "underlying_symbol": r.get("underlying_symbol"),
                }
            )
        futures.sort(key=lambda f: f["expiry_ms"])
        if not futures:
            return None

        if want_exact:
            pick = _pick_exact_month(
                futures, year=int(expiry_year), month=int(expiry_month)
            )
            if pick:
                return _result_from_pick(
                    pick,
                    prefer_und=prefer_und,
                    match_mode="exact_month",
                    requested_ym=requested_ym,
                )
            if not allow_front_month_fallback:
                logger.warning(
                    "commodities_div MCX exact month not found for %s %s "
                    "(no front-month fallback)",
                    prefer_und,
                    requested_ym,
                )
                return None
            front = _pick_front_month(futures)
            if not front:
                return None
            logger.warning(
                "commodities_div MCX exact month missing for %s %s; "
                "falling back to front-month %s (%s) — wrong month risk",
                prefer_und,
                requested_ym,
                front.get("expiry"),
                front.get("trading_symbol"),
            )
            return _result_from_pick(
                front,
                prefer_und=prefer_und,
                match_mode="front_month_fallback",
                requested_ym=requested_ym,
                fallback_from=requested_ym,
            )

        pick = _pick_front_month(futures)
        if not pick:
            return None
        return _result_from_pick(
            pick,
            prefer_und=prefer_und,
            match_mode="front_month",
            requested_ym=None,
        )
    except Exception as e:
        logger.warning(
            "commodities_div MCX resolve failed for %s ym=%s: %s",
            sym,
            requested_ym,
            e,
        )
        return None


def resolve_underlying_instrument(
    canonical: str,
    *,
    expiry_year: Optional[int] = None,
    expiry_month: Optional[int] = None,
    allow_front_month_fallback: bool = True,
) -> Optional[Dict[str, Any]]:
    """Resolve desk canonical name → MCX FUT for optional expiry month (cached)."""
    key = (canonical or "").strip().upper()
    if not key:
        return None
    cache_key = _cache_key(key, expiry_year=expiry_year, expiry_month=expiry_month)
    now = time.time()
    with _resolve_lock:
        hit = _resolve_cache.get(cache_key)
        if hit and now - float(hit[0]) < _RESOLVE_TTL_SEC:
            cached = hit[1]
            return dict(cached) if cached else None

    prefer = PREFERRED_UNDERLYING_SYMBOL.get(key, key)
    resolve_kwargs = {
        "expiry_year": expiry_year,
        "expiry_month": expiry_month,
        "allow_front_month_fallback": allow_front_month_fallback,
    }
    inst = resolve_mcx_instrument(prefer, **resolve_kwargs)
    if inst and inst.get("instrument_key"):
        inst["canonical"] = key
    else:
        inst = None
        for name in UPSTOX_RESOLVE_ALIASES.get(key, [key]):
            if name == prefer:
                continue
            cand = resolve_mcx_instrument(name, **resolve_kwargs)
            if cand and cand.get("instrument_key"):
                cand["canonical"] = key
                inst = cand
                break

    with _resolve_lock:
        _resolve_cache[cache_key] = (now, dict(inst) if inst else None)
    return dict(inst) if inst else None


def display_symbol_for(
    *,
    contract: Any = None,
    trading_symbol: Any = None,
    symbol_mapped: Any = None,
    symbol_raw: Any = None,
) -> str:
    """
    UI label for CommDiv rows: prefer Upstox MCX FUT trading_symbol (stored as contract).

    Fallback order when resolve failed / blank: mapped underlying → TV raw → "".
    """
    for cand in (contract, trading_symbol, symbol_mapped, symbol_raw):
        s = str(cand or "").strip()
        if s:
            return s
    return ""


def attach_instrument_fields(
    symbol_raw: str, *, resolve_contract: bool = True
) -> Dict[str, Any]:
    """
    Parse TV / free-text symbol → underlying + optional month-coded MCX FUT.

    When TV encodes letter+year (e.g. V2026) or free-text encodes a month name
    (SEP / SEPT / SEPTEMBER), resolve that exact expiry month.
    Continuous / bare underlyings resolve front-month.
    underlying_matched=False when symbol is not a known MCX commodity name.
    Set resolve_contract=False for webhook fast-path (no Upstox master I/O).
    """
    parsed = parse_tv_symbol(symbol_raw)
    underlying = parsed.get("underlying")
    out: Dict[str, Any] = {
        "symbol_mapped": underlying or (normalize_tv_ticker(symbol_raw) or ""),
        "underlying_matched": underlying is not None,
        "mapping_found": underlying is not None,
        "instrument_key": None,
        "contract": None,
        "trading_symbol": None,
        "lot_size": None,
        "exchange": "MCX",
        "resolved_as": None,
        "display_symbol": "",
        "month_code": parsed.get("month_code"),
        "contract_month": parsed.get("month"),
        "contract_year": parsed.get("year"),
        "contract_code": parsed.get("contract_code"),
        "match_mode": None,
        "expiry_fallback": False,
        "requested_expiry_ym": None,
        "parse_mode": parsed.get("parse_mode"),
    }
    if not underlying:
        out["display_symbol"] = display_symbol_for(
            symbol_mapped=out["symbol_mapped"], symbol_raw=symbol_raw
        )
        return out
    out["symbol_mapped"] = underlying
    if not resolve_contract:
        out["display_symbol"] = display_symbol_for(
            symbol_mapped=underlying, symbol_raw=symbol_raw
        )
        return out

    ey = parsed.get("year")
    em = parsed.get("month")
    inst = resolve_underlying_instrument(
        underlying,
        expiry_year=int(ey) if ey else None,
        expiry_month=int(em) if em else None,
    )
    if not inst:
        out["display_symbol"] = display_symbol_for(
            symbol_mapped=underlying, symbol_raw=symbol_raw
        )
        if ey and em:
            out["requested_expiry_ym"] = f"{int(ey):04d}-{int(em):02d}"
            logger.warning(
                "commodities_div attach: no MCX instrument for %s %s (input %s)",
                underlying,
                out["requested_expiry_ym"],
                symbol_raw,
            )
        return out
    tsym = inst.get("trading_symbol")
    out["instrument_key"] = inst.get("instrument_key")
    out["contract"] = tsym
    out["trading_symbol"] = tsym
    out["lot_size"] = inst.get("lot_size")
    out["resolved_as"] = inst.get("resolved_as")
    out["match_mode"] = inst.get("match_mode")
    out["expiry_fallback"] = bool(inst.get("expiry_fallback"))
    out["requested_expiry_ym"] = inst.get("requested_expiry_ym")
    out["display_symbol"] = display_symbol_for(
        contract=tsym,
        trading_symbol=tsym,
        symbol_mapped=underlying,
        symbol_raw=symbol_raw,
    )
    return out
