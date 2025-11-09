"""
Stock Ranking System
Scores and ranks stocks to select the best trading candidates when capacity is limited
"""

import logging
from typing import List, Dict, Tuple
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

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
        required_fields = ['option_ltp', 'qty', 'stock_vwap', 'option_contract', 'otm1_strike']
        complete_fields = sum(1 for field in required_fields if stock.get(field, 0) > 0 or stock.get(field, '') != '')
        completeness_score = (complete_fields / len(required_fields)) * 5
        
        score += completeness_score
        breakdown['completeness'] = round(completeness_score, 1)
        
        # =====================================================================
        # BONUS: EXTREME MOMENTUM MULTIPLIER
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
        
        return score, breakdown
    
    def rank_stocks(self, stocks: List[Dict], alert_type: str = None) -> List[Dict]:
        """
        Rank stocks and return top N based on scoring
        
        Args:
            stocks: List of stock dictionaries
            alert_type: 'Bullish' or 'Bearish' (optional, for logging)
            
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


def rank_and_select_stocks(stocks: List[Dict], max_stocks: int = 15, alert_type: str = None) -> Tuple[List[Dict], Dict]:
    """
    Convenience function to rank and select stocks
    
    Args:
        stocks: List of stock dictionaries
        max_stocks: Maximum number to select
        alert_type: Alert type for logging
        
    Returns:
        Tuple of (selected_stocks, summary_dict)
    """
    ranker = StockRanker(max_stocks=max_stocks)
    selected = ranker.rank_stocks(stocks, alert_type=alert_type)
    summary = ranker.get_selection_summary(selected, len(stocks))
    
    return selected, summary

