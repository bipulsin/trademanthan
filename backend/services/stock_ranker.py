"""
Stock Ranking System
Scores and ranks stocks to select the best trading candidates when capacity is limited
"""

import csv
import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

# Project root: backend/services/stock_ranker.py -> parents[2] = repo root
_RANKING_EXPORT_DIR = Path(__file__).resolve().parent.parent.parent / "logs" / "scan_rankings"


def _safe_ts_for_filename(s: str) -> str:
    return re.sub(r"[^\w\-]+", "_", (s or "na")[: 40])


def _flatten_breakdown(bd: Dict) -> Dict[str, float]:
    """Normalize breakdown dict for tabular export."""
    out = {
        "momentum": float(bd.get("momentum") or 0),
        "liquidity": float(bd.get("liquidity") or 0),
        "premium": float(bd.get("premium") or 0),
        "strike": float(bd.get("strike") or 0),
        "completeness": float(bd.get("completeness") or 0),
        "extreme_bonus": float(bd.get("extreme_bonus") or 0),
    }
    hb = bd.get("hold_bonus_total")
    if hb is None:
        hb = sum(
            float(bd.get(k) or 0)
            for k in ("hold_bonus_premium", "hold_bonus_stable", "hold_bonus_liquidity")
        )
    out["hold_bonus"] = float(hb or 0)
    return out


def _schedule_ranking_email(csv_path: Path) -> None:
    """Background thread sends CSV (see chartink_ranking_email)."""
    try:
        from backend.services.chartink_ranking_email import schedule_chartink_ranking_email
    except ImportError:
        try:
            from services.chartink_ranking_email import schedule_chartink_ranking_email
        except ImportError as e:
            logger.warning("chartink_ranking_email not available: %s", e)
            return
    schedule_chartink_ranking_email(csv_path)


def log_and_export_full_ranking(
    sorted_scored: List[Tuple[float, Dict]],
    alert_type: Optional[str] = None,
    export_meta: Optional[Dict[str, Any]] = None,
    send_email: bool = True,
) -> Optional[Path]:
    """
    Log ASCII table + write JSON + CSV under logs/scan_rankings/ for ALL stocks (e.g. 36 rows).
    sorted_scored: list of (composite_score, stock_dict with _score_breakdown).
    Returns path to CSV if written. If send_email, ranking CSV is sent in a background thread (non-daemon so CLI scripts wait for SMTP).
    """
    if not sorted_scored:
        return None

    _RANKING_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    rows_out: List[Dict[str, Any]] = []
    lines = []
    hdr = (
        f"{'#':>4}  {'Symbol':<14}  {'Total':>7}  {'Mom':>5}  {'Liq':>5}  {'Prem':>5}  "
        f"{'Strk':>5}  {'Cmp':>5}  {'ExB':>5}  {'HB':>5}"
    )
    lines.append(hdr)
    lines.append("-" * len(hdr))

    for rank, (score, stock) in enumerate(sorted_scored, start=1):
        name = str(stock.get("stock_name") or "?")[:14]
        bd = stock.get("_score_breakdown") or {}
        f = _flatten_breakdown(bd)
        total = round(float(score), 2)
        line = (
            f"{rank:>4}  {name:<14}  {total:>7.2f}  {f['momentum']:>5.0f}  {f['liquidity']:>5.0f}  "
            f"{f['premium']:>5.0f}  {f['strike']:>5.0f}  {f['completeness']:>5.1f}  "
            f"{f['extreme_bonus']:>5.0f}  {f['hold_bonus']:>5.0f}"
        )
        lines.append(line)
        sym = stock.get("stock_name") or ""
        row = {
            "stock_symbol": sym,
            "rank": rank,
            "composite_score": total,
            **{k: round(v, 2) if k != "completeness" else round(v, 2) for k, v in f.items()},
            "breakdown_raw": bd,
        }
        rows_out.append(row)

    table = "\n".join(lines)
    logger.info("\n📋 FULL STOCK RANKING (all symbols, composite + factor scores):\n%s", table)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    disp = (export_meta or {}).get("triggered_at_display") or ""
    ts_part = _safe_ts_for_filename(disp.replace(" ", "_")) if disp else ts
    prefix = f"scan_ranking_{ts}_{ts_part}_{alert_type or 'NA'}"

    payload = {
        "exported_at_utc": datetime.utcnow().isoformat() + "Z",
        "alert_type": alert_type,
        "meta": export_meta or {},
        "stock_count": len(rows_out),
        "rows": rows_out,
    }
    json_path = _RANKING_EXPORT_DIR / f"{prefix}.json"
    try:
        with open(json_path, "w", encoding="utf-8") as jf:
            json.dump(payload, jf, indent=2, default=str)
        logger.info("📁 Full ranking JSON: %s", json_path)
    except OSError as e:
        logger.warning("Could not write ranking JSON: %s", e)

    csv_path = _RANKING_EXPORT_DIR / f"{prefix}.csv"
    try:
        fieldnames = [
            "stock_symbol",
            "rank",
            "composite_score",
            "momentum",
            "liquidity",
            "premium",
            "strike",
            "completeness",
            "extreme_bonus",
            "hold_bonus",
        ]
        with open(csv_path, "w", encoding="utf-8", newline="") as cf:
            w = csv.DictWriter(cf, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows_out:
                w.writerow({k: r.get(k) for k in fieldnames})
        logger.info("📁 Full ranking CSV (open in Excel): %s", csv_path)
        if send_email:
            _schedule_ranking_email(csv_path)
    except OSError as e:
        logger.warning("Could not write ranking CSV: %s", e)
        csv_path = None

    return csv_path


def export_full_ranking_only(
    stocks: List[Dict],
    alert_type: Optional[str] = None,
    export_meta: Optional[Dict[str, Any]] = None,
    send_email: bool = True,
) -> List[Dict[str, Any]]:
    """
    Score every stock and export table/JSON/CSV (used when count <= MAX_STOCKS_PER_ALERT, no trimming).
    Returns row dicts for testing.
    """
    if not stocks:
        return []
    ranker = StockRanker(max_stocks=10**9)
    scored: List[Tuple[float, Dict]] = []
    for stock in stocks:
        score, breakdown = ranker.calculate_score(stock)
        stock_copy = stock.copy()
        stock_copy["_rank_score"] = score
        stock_copy["_score_breakdown"] = breakdown
        scored.append((score, stock_copy))
    scored.sort(key=lambda x: x[0], reverse=True)
    log_and_export_full_ranking(
        scored, alert_type=alert_type, export_meta=export_meta, send_email=send_email
    )
    return [
        {
            "rank": i,
            "stock_name": s[1].get("stock_name"),
            "composite_score": round(s[0], 2),
            **_flatten_breakdown(s[1].get("_score_breakdown") or {}),
        }
        for i, s in enumerate(scored, start=1)
    ]

class StockRanker:
    """
    Ranks stocks based on multiple factors to prioritize high-probability trades
    """
    
    def __init__(self, max_stocks: int = 15):
        """
        Initialize the ranker
        
        Args:
            max_stocks: Maximum number of stocks to select
        """
        self.max_stocks = max_stocks
        
    def calculate_score(self, stock: Dict) -> Tuple[float, Dict]:
        """
        Calculate a composite score for a stock based on MOMENTUM STRENGTH
        
        Focus: Find stocks with STRONGEST momentum, regardless of size/price
        
        Args:
            stock: Dictionary containing stock data
            
        Returns:
            Tuple of (score, breakdown_dict)
        """
        score = 0.0
        breakdown = {}
        
        stock_ltp = stock.get('last_traded_price', 0.0)
        stock_vwap = stock.get('stock_vwap', 0.0)
        option_type = stock.get('option_type', '')
        option_ltp = stock.get('option_ltp', 0.0)
        qty = stock.get('qty', 0)
        
        # =====================================================================
        # Factor 1: MOMENTUM STRENGTH (40 pts) - MOST IMPORTANT!
        # =====================================================================
        # Distance from VWAP = Momentum strength
        # Bigger distance = Stronger momentum
        if stock_ltp > 0 and stock_vwap > 0:
            vwap_diff_pct = abs((stock_ltp - stock_vwap) / stock_vwap) * 100
            
            # Check if momentum is in correct direction
            is_correct_direction = False
            if option_type == 'PE' and stock_ltp < stock_vwap:  # Bearish momentum
                is_correct_direction = True
            elif option_type == 'CE' and stock_ltp > stock_vwap:  # Bullish momentum
                is_correct_direction = True
            
            if is_correct_direction:
                # REWARD STRONG MOMENTUM regardless of stock price!
                if vwap_diff_pct >= 3:  # SUPER STRONG momentum (3%+)
                    momentum_score = 40  # MAX SCORE!
                elif vwap_diff_pct >= 2:  # Very strong momentum (2-3%)
                    momentum_score = 35
                elif vwap_diff_pct >= 1.5:  # Strong momentum (1.5-2%)
                    momentum_score = 30
                elif vwap_diff_pct >= 1:  # Good momentum (1-1.5%)
                    momentum_score = 25
                elif vwap_diff_pct >= 0.5:  # Moderate momentum (0.5-1%)
                    momentum_score = 18
                else:  # Weak momentum (<0.5%)
                    momentum_score = 10
            else:
                # WRONG DIRECTION - this is critical!
                momentum_score = 0
        else:
            momentum_score = 0  # No VWAP data
        
        score += momentum_score
        breakdown['momentum'] = momentum_score
        
        # =====================================================================
        # Factor 2: LIQUIDITY/EXECUTABILITY (25 pts)
        # =====================================================================
        # Can we actually execute this trade?
        # Even penny stocks need minimum liquidity
        if qty > 0:
            if qty >= 1000:  # Excellent liquidity
                liquidity_score = 25
            elif qty >= 500:  # Very good
                liquidity_score = 22
            elif qty >= 300:  # Good
                liquidity_score = 20
            elif qty >= 150:  # Adequate
                liquidity_score = 17
            elif qty >= 75:  # Minimum acceptable
                liquidity_score = 15
            else:  # Very low (but not disqualifying)
                liquidity_score = 10
        else:
            liquidity_score = 0
        
        score += liquidity_score
        breakdown['liquidity'] = liquidity_score
        
        # =====================================================================
        # Factor 3: OPTION PREMIUM QUALITY (20 pts)
        # =====================================================================
        # Premium should allow for good % gains
        # But don't overly penalize cheap options if momentum is strong!
        if option_ltp > 0:
            if 2 <= option_ltp <= 30:  # Optimal range
                premium_score = 20
            elif 1 <= option_ltp < 2:  # Cheap but tradeable
                premium_score = 18  # Only slight penalty
            elif 30 < option_ltp <= 60:  # Higher priced
                premium_score = 17
            elif 0.5 <= option_ltp < 1:  # Very cheap (penny option)
                # Still give decent score if it's tradeable
                premium_score = 15
            elif 60 < option_ltp <= 100:  # Expensive
                premium_score = 12
            elif option_ltp > 100:  # Very expensive
                premium_score = 8
            else:  # < ₹0.50 (too illiquid)
                premium_score = 5
        else:
            premium_score = 0
        
        score += premium_score
        breakdown['premium'] = premium_score
        
        # =====================================================================
        # Factor 4: STRIKE SELECTION (10 pts)
        # =====================================================================
        # Reasonable strike distance
        option_strike = stock.get('otm1_strike', 0.0)
        if option_strike > 0 and stock_ltp > 0:
            strike_diff_pct = abs((option_strike - stock_ltp) / stock_ltp) * 100
            
            if 0.5 <= strike_diff_pct <= 4:  # Reasonable OTM range
                strike_score = 10
            elif 4 < strike_diff_pct <= 7:  # Further OTM
                strike_score = 8
            elif strike_diff_pct < 0.5:  # Very near ATM
                strike_score = 7
            else:  # Too far OTM (>7%)
                strike_score = 4
        else:
            strike_score = 0
        
        score += strike_score
        breakdown['strike'] = strike_score
        
        # =====================================================================
        # Factor 5: DATA COMPLETENESS (5 pts)
        # =====================================================================
        # Must have critical data to make informed decision
        complete_fields = 0
        
        # Check numeric fields
        numeric_fields = ['option_ltp', 'qty', 'stock_vwap', 'otm1_strike']
        for field in numeric_fields:
            try:
                value = stock.get(field, 0)
                if isinstance(value, (int, float)) and value > 0:
                    complete_fields += 1
            except (TypeError, ValueError):
                pass  # Skip invalid values
        
        # Check string fields
        string_fields = ['option_contract']
        for field in string_fields:
            try:
                value = stock.get(field, '')
                if value and str(value).strip():
                    complete_fields += 1
            except (TypeError, ValueError):
                pass  # Skip invalid values
        
        required_fields_count = len(numeric_fields) + len(string_fields)
        completeness_score = (complete_fields / required_fields_count) * 5
        
        score += completeness_score
        breakdown['completeness'] = round(completeness_score, 1)
        
        # =====================================================================
        # BONUS 1: EXTREME MOMENTUM MULTIPLIER
        # =====================================================================
        # If stock has EXTREME momentum (>4%), give bonus regardless of other factors!
        if stock_ltp > 0 and stock_vwap > 0:
            vwap_diff_pct = abs((stock_ltp - stock_vwap) / stock_vwap) * 100
            is_correct_direction = (
                (option_type == 'PE' and stock_ltp < stock_vwap) or
                (option_type == 'CE' and stock_ltp > stock_vwap)
            )
            
            if is_correct_direction:
                if vwap_diff_pct >= 5:  # EXTREME momentum (5%+)
                    extreme_bonus = 10
                    score += extreme_bonus
                    breakdown['extreme_bonus'] = extreme_bonus
                    logger.info(f"🚀 EXTREME MOMENTUM: {stock.get('stock_name')} - {vwap_diff_pct:.2f}% from VWAP!")
        
        # =====================================================================
        # BONUS 2: "LIKELY TO HOLD" CHARACTERISTICS
        # =====================================================================
        # Nov 7 analysis: Winners were stocks that held till time-based exit (50% win rate)
        # vs VWAP cross exits (16% win rate)
        # 
        # Characteristics of stocks likely to hold momentum:
        # - Moderate lot sizes (not extreme high which indicates retail frenzy)
        # - Mid-range premiums (₹10-60 tend to be more stable)
        # - Not penny options (< ₹2 are too volatile)
        
        hold_bonus = 0
        
        # Factor: Stable premium range
        if 10 <= option_ltp <= 60:
            hold_bonus += 5  # These premiums tend to hold better
            breakdown['hold_bonus_premium'] = 5
        
        # Factor: Not penny option (too volatile)
        if option_ltp >= 2:
            hold_bonus += 3  # Avoid ultra-cheap volatile options
            breakdown['hold_bonus_stable'] = 3
        
        # Factor: Reasonable liquidity (not too high = retail frenzy)
        if 150 <= qty <= 800:
            hold_bonus += 2  # Sweet spot - not too hot, not too cold
            breakdown['hold_bonus_liquidity'] = 2
        
        if hold_bonus > 0:
            score += hold_bonus
            breakdown['hold_bonus_total'] = hold_bonus
        
        return score, breakdown
    
    def rank_stocks(
        self,
        stocks: List[Dict],
        alert_type: str = None,
        export_meta: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """
        Rank stocks and return top N based on scoring
        
        Args:
            stocks: List of stock dictionaries
            alert_type: 'Bullish' or 'Bearish' (optional, for logging)
            export_meta: Optional context (triggered_at_display, scan_name) for JSON/CSV filename and payload
            
        Returns:
            List of top N ranked stocks with scores
        """
        if not stocks:
            return []
        
        logger.info(f"📊 Ranking {len(stocks)} stocks (Alert Type: {alert_type})")
        
        # Calculate scores for all stocks
        scored_stocks = []
        for stock in stocks:
            score, breakdown = self.calculate_score(stock)
            stock_copy = stock.copy()
            stock_copy['_rank_score'] = score
            stock_copy['_score_breakdown'] = breakdown
            scored_stocks.append((score, stock_copy))
        
        # Sort by score (descending)
        scored_stocks.sort(key=lambda x: x[0], reverse=True)

        # Full table + JSON + CSV for every symbol (e.g. all 36 before trimming to top N)
        log_and_export_full_ranking(scored_stocks, alert_type=alert_type, export_meta=export_meta)
        
        # Select top N
        selected_stocks = [stock for score, stock in scored_stocks[:self.max_stocks]]
        rejected_stocks = [stock for score, stock in scored_stocks[self.max_stocks:]]
        
        # Log results
        logger.info(f"✅ Selected top {len(selected_stocks)} stocks")
        logger.info(f"❌ Rejected {len(rejected_stocks)} stocks")
        
        if selected_stocks:
            avg_score = sum(s['_rank_score'] for s in selected_stocks) / len(selected_stocks)
            logger.info(f"📈 Average score of selected: {avg_score:.1f}")
            
            # Log top 5 with details
            logger.info(f"\n🏆 Top 5 Selected Stocks:")
            for i, stock in enumerate(selected_stocks[:5], 1):
                breakdown = stock['_score_breakdown']
                logger.info(f"  {i}. {stock['stock_name']}: Score {stock['_rank_score']:.1f} "
                          f"(Liq:{breakdown.get('liquidity',0)}, Prem:{breakdown.get('premium',0)}, "
                          f"VWAP:{breakdown.get('vwap',0)}, Strike:{breakdown.get('strike',0)})")
        
        if rejected_stocks and len(rejected_stocks) <= 10:
            logger.info(f"\n❌ Rejected Stocks:")
            for stock in rejected_stocks:
                logger.info(f"  • {stock['stock_name']}: Score {stock['_rank_score']:.1f}")
        
        return selected_stocks
    
    def get_selection_summary(self, selected: List[Dict], total: int) -> Dict:
        """
        Get summary statistics of selection
        
        Args:
            selected: List of selected stocks
            total: Total number of stocks available
            
        Returns:
            Dictionary with summary stats
        """
        if not selected:
            return {
                'total_available': total,
                'total_selected': 0,
                'total_rejected': total,
                'avg_score': 0,
                'min_score': 0,
                'max_score': 0
            }
        
        scores = [s['_rank_score'] for s in selected]
        
        return {
            'total_available': total,
            'total_selected': len(selected),
            'total_rejected': total - len(selected),
            'avg_score': round(sum(scores) / len(scores), 2),
            'min_score': round(min(scores), 2),
            'max_score': round(max(scores), 2),
            'selection_rate': round((len(selected) / total) * 100, 1) if total > 0 else 0
        }


# Global ranker instance
stock_ranker = StockRanker(max_stocks=15)


def rank_and_select_stocks(
    stocks: List[Dict],
    max_stocks: int = 15,
    alert_type: str = None,
    export_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict], Dict]:
    """
    Convenience function to rank and select stocks
    
    Args:
        stocks: List of stock dictionaries
        max_stocks: Maximum number to select
        alert_type: Alert type for logging
        export_meta: Optional context written into ranking JSON/CSV
        
    Returns:
        Tuple of (selected_stocks, summary_dict)
    """
    ranker = StockRanker(max_stocks=max_stocks)
    selected = ranker.rank_stocks(stocks, alert_type=alert_type, export_meta=export_meta)
    summary = ranker.get_selection_summary(selected, len(stocks))
    
    return selected, summary

