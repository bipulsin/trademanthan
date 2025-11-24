#!/usr/bin/env python3
"""
Analyze trades from 4-Nov-2025 to 14-Nov-2025
Provide insights and recommendations to improve win rate
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pytz
from collections import defaultdict

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from database import SessionLocal
from models.trading import IntradayStockOption
from sqlalchemy import and_, func

def analyze_trades():
    """
    Analyze trades from 4-Nov-2025 to 14-Nov-2025
    """
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    
    try:
        # Date range: 4-Nov-2025 to 14-Nov-2025
        start_date = datetime(2025, 11, 4, tzinfo=ist).replace(hour=0, minute=0, second=0)
        end_date = datetime(2025, 11, 14, tzinfo=ist).replace(hour=23, minute=59, second=59)
        
        print("=" * 80)
        print("TRADE ANALYSIS: 4-Nov-2025 to 14-Nov-2025")
        print("=" * 80)
        print()
        
        # Get all trades in date range
        all_trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= start_date,
            IntradayStockOption.trade_date <= end_date
        ).order_by(IntradayStockOption.trade_date, IntradayStockOption.alert_time).all()
        
        total_trades = len(all_trades)
        print(f"📊 Total Records: {total_trades}")
        print()
        
        # Filter trades that were entered (status != 'no_entry')
        entered_trades = [t for t in all_trades if t.status != 'no_entry' and t.buy_price and t.buy_price > 0]
        no_entry_trades = [t for t in all_trades if t.status == 'no_entry']
        
        print(f"📈 TRADE BREAKDOWN:")
        print(f"   Total Records: {total_trades}")
        print(f"   Entered Trades: {len(entered_trades)}")
        print(f"   No Entry Trades: {len(no_entry_trades)}")
        print()
        
        if len(entered_trades) == 0:
            print("⚠️  No entered trades found in this period")
            return
        
        # Analyze entered trades
        winning_trades = []
        losing_trades = []
        break_even_trades = []
        
        for trade in entered_trades:
            if trade.pnl is not None:
                if trade.pnl > 0:
                    winning_trades.append(trade)
                elif trade.pnl < 0:
                    losing_trades.append(trade)
                else:
                    break_even_trades.append(trade)
        
        # Calculate statistics
        total_pnl = sum(t.pnl for t in entered_trades if t.pnl is not None)
        win_rate = (len(winning_trades) / len(entered_trades) * 100) if entered_trades else 0
        avg_win = sum(t.pnl for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t.pnl for t in losing_trades) / len(losing_trades) if losing_trades else 0
        profit_factor = abs(sum(t.pnl for t in winning_trades) / sum(t.pnl for t in losing_trades)) if losing_trades and sum(t.pnl for t in losing_trades) != 0 else 0
        
        print("=" * 80)
        print("📊 PERFORMANCE METRICS")
        print("=" * 80)
        print(f"   Total Entered Trades: {len(entered_trades)}")
        print(f"   Winning Trades: {len(winning_trades)} ({len(winning_trades)/len(entered_trades)*100:.1f}%)")
        print(f"   Losing Trades: {len(losing_trades)} ({len(losing_trades)/len(entered_trades)*100:.1f}%)")
        print(f"   Break-even Trades: {len(break_even_trades)} ({len(break_even_trades)/len(entered_trades)*100:.1f}%)")
        print(f"   Win Rate: {win_rate:.2f}%")
        print(f"   Total PnL: ₹{total_pnl:,.2f}")
        print(f"   Average Win: ₹{avg_win:,.2f}")
        print(f"   Average Loss: ₹{avg_loss:,.2f}")
        print(f"   Profit Factor: {profit_factor:.2f}")
        print()
        
        # Analyze by exit reason
        print("=" * 80)
        print("📊 EXIT REASON ANALYSIS")
        print("=" * 80)
        exit_reasons = defaultdict(lambda: {'count': 0, 'win': 0, 'loss': 0, 'pnl': 0})
        
        for trade in entered_trades:
            if trade.exit_reason:
                reason = trade.exit_reason
                exit_reasons[reason]['count'] += 1
                if trade.pnl and trade.pnl > 0:
                    exit_reasons[reason]['win'] += 1
                elif trade.pnl and trade.pnl < 0:
                    exit_reasons[reason]['loss'] += 1
                if trade.pnl:
                    exit_reasons[reason]['pnl'] += trade.pnl
        
        for reason, stats in sorted(exit_reasons.items(), key=lambda x: x[1]['count'], reverse=True):
            win_rate_reason = (stats['win'] / stats['count'] * 100) if stats['count'] > 0 else 0
            print(f"   {reason.upper()}:")
            print(f"      Count: {stats['count']}, Win: {stats['win']}, Loss: {stats['loss']}")
            print(f"      Win Rate: {win_rate_reason:.1f}%")
            print(f"      Total PnL: ₹{stats['pnl']:,.2f}")
            print()
        
        # Analyze by alert time
        print("=" * 80)
        print("📊 ALERT TIME ANALYSIS")
        print("=" * 80)
        time_slots = defaultdict(lambda: {'count': 0, 'win': 0, 'loss': 0, 'pnl': 0})
        
        for trade in entered_trades:
            if trade.alert_time:
                hour = trade.alert_time.hour
                minute = trade.alert_time.minute
                slot = f"{hour:02d}:{minute:02d}"
                time_slots[slot]['count'] += 1
                if trade.pnl and trade.pnl > 0:
                    time_slots[slot]['win'] += 1
                elif trade.pnl and trade.pnl < 0:
                    time_slots[slot]['loss'] += 1
                if trade.pnl:
                    time_slots[slot]['pnl'] += trade.pnl
        
        for slot in sorted(time_slots.keys()):
            stats = time_slots[slot]
            win_rate_slot = (stats['win'] / stats['count'] * 100) if stats['count'] > 0 else 0
            if stats['count'] >= 3:  # Only show slots with 3+ trades
                print(f"   {slot}: Count={stats['count']}, Win Rate={win_rate_slot:.1f}%, PnL=₹{stats['pnl']:,.2f}")
        print()
        
        # Analyze by option type
        print("=" * 80)
        print("📊 OPTION TYPE ANALYSIS")
        print("=" * 80)
        option_types = defaultdict(lambda: {'count': 0, 'win': 0, 'loss': 0, 'pnl': 0})
        
        for trade in entered_trades:
            opt_type = trade.option_type or 'UNKNOWN'
            option_types[opt_type]['count'] += 1
            if trade.pnl and trade.pnl > 0:
                option_types[opt_type]['win'] += 1
            elif trade.pnl and trade.pnl < 0:
                option_types[opt_type]['loss'] += 1
            if trade.pnl:
                option_types[opt_type]['pnl'] += trade.pnl
        
        for opt_type, stats in sorted(option_types.items(), key=lambda x: x[1]['count'], reverse=True):
            win_rate_type = (stats['win'] / stats['count'] * 100) if stats['count'] > 0 else 0
            print(f"   {opt_type}: Count={stats['count']}, Win Rate={win_rate_type:.1f}%, PnL=₹{stats['pnl']:,.2f}")
        print()
        
        # Analyze by day of week
        print("=" * 80)
        print("📊 DAY OF WEEK ANALYSIS")
        print("=" * 80)
        days = defaultdict(lambda: {'count': 0, 'win': 0, 'loss': 0, 'pnl': 0})
        
        for trade in entered_trades:
            if trade.trade_date:
                day_name = trade.trade_date.strftime('%A')
                days[day_name]['count'] += 1
                if trade.pnl and trade.pnl > 0:
                    days[day_name]['win'] += 1
                elif trade.pnl and trade.pnl < 0:
                    days[day_name]['loss'] += 1
                if trade.pnl:
                    days[day_name]['pnl'] += trade.pnl
        
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        for day in day_order:
            if day in days:
                stats = days[day]
                win_rate_day = (stats['win'] / stats['count'] * 100) if stats['count'] > 0 else 0
                print(f"   {day}: Count={stats['count']}, Win Rate={win_rate_day:.1f}%, PnL=₹{stats['pnl']:,.2f}")
        print()
        
        # Top winners and losers
        print("=" * 80)
        print("📊 TOP WINNERS (Top 10)")
        print("=" * 80)
        sorted_winners = sorted(winning_trades, key=lambda x: x.pnl or 0, reverse=True)[:10]
        for i, trade in enumerate(sorted_winners, 1):
            sell_price_display = trade.sell_price if trade.sell_price else 0.0
            print(f"   {i}. {trade.stock_name} - {trade.option_contract or 'N/A'}")
            print(f"      PnL: ₹{trade.pnl:,.2f}, Entry: ₹{trade.buy_price:.2f}, Exit: ₹{sell_price_display:.2f}")
            trade_date_str = trade.trade_date.strftime('%Y-%m-%d') if trade.trade_date else 'N/A'
            print(f"      Date: {trade_date_str}, Exit: {trade.exit_reason or 'N/A'}")
        print()
        
        print("=" * 80)
        print("📊 TOP LOSERS (Top 10)")
        print("=" * 80)
        sorted_losers = sorted(losing_trades, key=lambda x: x.pnl or 0)[:10]
        for i, trade in enumerate(sorted_losers, 1):
            sell_price_display = trade.sell_price if trade.sell_price else 0.0
            print(f"   {i}. {trade.stock_name} - {trade.option_contract or 'N/A'}")
            print(f"      PnL: ₹{trade.pnl:,.2f}, Entry: ₹{trade.buy_price:.2f}, Exit: ₹{sell_price_display:.2f}")
            trade_date_str = trade.trade_date.strftime('%Y-%m-%d') if trade.trade_date else 'N/A'
            print(f"      Date: {trade_date_str}, Exit: {trade.exit_reason or 'N/A'}")
        print()
        
        # Analyze stop loss effectiveness
        print("=" * 80)
        print("📊 STOP LOSS ANALYSIS")
        print("=" * 80)
        stop_loss_trades = [t for t in entered_trades if t.exit_reason == 'stop_loss']
        sl_wins = [t for t in stop_loss_trades if t.pnl and t.pnl > 0]
        sl_losses = [t for t in stop_loss_trades if t.pnl and t.pnl < 0]
        
        print(f"   Stop Loss Exits: {len(stop_loss_trades)}")
        if stop_loss_trades:
            print(f"   Stop Loss Wins: {len(sl_wins)} ({len(sl_wins)/len(stop_loss_trades)*100:.1f}%)")
            print(f"   Stop Loss Losses: {len(sl_losses)} ({len(sl_losses)/len(stop_loss_trades)*100:.1f}%)")
            if stop_loss_trades:
                avg_sl_pnl = sum(t.pnl for t in stop_loss_trades if t.pnl) / len([t for t in stop_loss_trades if t.pnl])
                print(f"   Average SL PnL: ₹{avg_sl_pnl:,.2f}")
        print()
        
        # VWAP cross exits
        vwap_exits = [t for t in entered_trades if t.exit_reason == 'stock_vwap_cross']
        if vwap_exits:
            print("=" * 80)
            print("📊 VWAP CROSS EXITS")
            print("=" * 80)
            vwap_wins = [t for t in vwap_exits if t.pnl and t.pnl > 0]
            vwap_losses = [t for t in vwap_exits if t.pnl and t.pnl < 0]
            print(f"   VWAP Cross Exits: {len(vwap_exits)}")
            print(f"   VWAP Wins: {len(vwap_wins)} ({len(vwap_wins)/len(vwap_exits)*100:.1f}%)")
            print(f"   VWAP Losses: {len(vwap_losses)} ({len(vwap_losses)/len(vwap_exits)*100:.1f}%)")
            if vwap_exits:
                avg_vwap_pnl = sum(t.pnl for t in vwap_exits if t.pnl) / len([t for t in vwap_exits if t.pnl])
                print(f"   Average VWAP PnL: ₹{avg_vwap_pnl:,.2f}")
            print()
        
        # Recommendations
        print("=" * 80)
        print("💡 RECOMMENDATIONS TO IMPROVE WIN RATE")
        print("=" * 80)
        
        recommendations = []
        
        # 1. Win rate analysis
        if win_rate < 50:
            recommendations.append(f"⚠️  Low Win Rate ({win_rate:.1f}%): Focus on improving entry selection")
        elif win_rate >= 60:
            recommendations.append(f"✅ Good Win Rate ({win_rate:.1f}%): Maintain current strategy")
        else:
            recommendations.append(f"📊 Moderate Win Rate ({win_rate:.1f}%): Room for improvement")
        
        # 2. Profit factor analysis
        if profit_factor < 1.0:
            recommendations.append(f"⚠️  Profit Factor < 1.0 ({profit_factor:.2f}): Average losses exceed average wins - need better risk management")
        elif profit_factor < 1.5:
            recommendations.append(f"📊 Profit Factor {profit_factor:.2f}: Consider improving risk-reward ratio")
        else:
            recommendations.append(f"✅ Good Profit Factor ({profit_factor:.2f}): Risk-reward is favorable")
        
        # 3. Stop loss effectiveness
        if stop_loss_trades and len(sl_wins) > 0:
            recommendations.append(f"❌ Stop Loss triggering wins is unusual - review SL logic")
        
        # 4. Exit reason analysis
        if exit_reasons:
            best_exit = max(exit_reasons.items(), key=lambda x: (x[1]['win']/x[1]['count'] if x[1]['count'] > 0 else 0))
            worst_exit = min(exit_reasons.items(), key=lambda x: (x[1]['win']/x[1]['count'] if x[1]['count'] > 0 else 0))
            if best_exit[1]['count'] >= 5:
                recommendations.append(f"✅ Best Exit: {best_exit[0]} with {best_exit[1]['win']/best_exit[1]['count']*100:.1f}% win rate")
            if worst_exit[1]['count'] >= 5:
                recommendations.append(f"⚠️  Review Exit: {worst_exit[0]} with {worst_exit[1]['win']/worst_exit[1]['count']*100:.1f}% win rate")
        
        # 5. Time slot analysis
        best_time = max(time_slots.items(), key=lambda x: (x[1]['win']/x[1]['count'] if x[1]['count'] > 0 else 0))
        if best_time[1]['count'] >= 5:
            recommendations.append(f"✅ Best Entry Time: {best_time[0]} with {best_time[1]['win']/best_time[1]['count']*100:.1f}% win rate - consider focusing on this time")
        
        # 6. Option type analysis
        if option_types:
            best_type = max(option_types.items(), key=lambda x: (x[1]['win']/x[1]['count'] if x[1]['count'] > 0 else 0))
            worst_type = min(option_types.items(), key=lambda x: (x[1]['win']/x[1]['count'] if x[1]['count'] > 0 else 0))
            if best_type[1]['count'] >= 10:
                recommendations.append(f"✅ Best Option Type: {best_type[0]} with {best_type[1]['win']/best_type[1]['count']*100:.1f}% win rate")
            if worst_type[1]['count'] >= 10:
                recommendations.append(f"⚠️  Review Option Type: {worst_type[0]} with {worst_type[1]['win']/worst_type[1]['count']*100:.1f}% win rate")
        
        # 7. Risk management
        if avg_loss and avg_win:
            risk_reward = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            if risk_reward < 1.0:
                recommendations.append(f"⚠️  Risk-Reward Ratio {risk_reward:.2f}: Average loss exceeds average win - tighten stop losses or widen profit targets")
            elif risk_reward < 1.5:
                recommendations.append(f"📊 Risk-Reward Ratio {risk_reward:.2f}: Aim for at least 1.5:1 ratio")
        
        # 8. VWAP cross analysis
        if vwap_exits:
            vwap_win_rate = len(vwap_wins) / len(vwap_exits) * 100
            if vwap_win_rate < 40:
                recommendations.append(f"⚠️  VWAP Cross Exit Win Rate {vwap_win_rate:.1f}%: Consider adjusting VWAP exit logic or timing")
        
        for i, rec in enumerate(recommendations, 1):
            print(f"   {i}. {rec}")
        
        print()
        print("=" * 80)
        
        return True
        
    except Exception as e:
        print(f"❌ Error analyzing trades: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = analyze_trades()
    sys.exit(0 if success else 1)

