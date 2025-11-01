from datetime import datetime
from sqlalchemy.orm import Session
from models.strategy import Strategy

def create_default_strategies_for_user(db: Session, user_id: int):
    """Create 3 default strategies for a new user"""
    
    default_strategies = [
        {
            "name": "Supertrend RSI Combo",
            "description": "Combines Supertrend trend direction with RSI oversold/overbought signals for entry/exit decisions",
            "indicators": ["Supertrend", "RSI"],
            "logic_operator": "AND",
            "parameters": {
                "Supertrend": {
                    "period": 10,
                    "multiplier": 3.0
                },
                "RSI": {
                    "period": 14,
                    "overbought": 70,
                    "oversold": 30
                }
            },
            "entry_criteria": "Enter long when Supertrend shows uptrend AND RSI is oversold (<30). Enter short when Supertrend shows downtrend AND RSI is overbought (>70)",
            "exit_criteria": "Exit long when Supertrend flips to downtrend OR RSI becomes overbought. Exit short when Supertrend flips to uptrend OR RSI becomes oversold"
        },
        {
            "name": "Triple EMA Trend",
            "description": "Uses three Exponential Moving Averages (short, medium, long) to identify trend direction and momentum",
            "indicators": ["EMA"],
            "logic_operator": "AND",
            "parameters": {
                "EMA": {
                    "short_period": 9,
                    "medium_period": 21,
                    "long_period": 50
                }
            },
            "entry_criteria": "Enter long when short EMA > medium EMA > long EMA (bullish alignment). Enter short when short EMA < medium EMA < long EMA (bearish alignment)",
            "exit_criteria": "Exit when EMA alignment changes (trend reversal) or when price crosses the medium EMA"
        },
        {
            "name": "BB Squeeze Momentum",
            "description": "Identifies low volatility periods (squeeze) using Bollinger Bands and enters on momentum breakout",
            "indicators": ["Bollinger Bands", "RSI"],
            "logic_operator": "AND",
            "parameters": {
                "Bollinger Bands": {
                    "period": 20,
                    "std_dev": 2.0
                },
                "RSI": {
                    "period": 14,
                    "overbought": 70,
                    "oversold": 30
                }
            },
            "entry_criteria": "Enter long when price breaks above upper Bollinger Band AND RSI > 50. Enter short when price breaks below lower Bollinger Band AND RSI < 50",
            "exit_criteria": "Exit when price returns to Bollinger Band middle line or when RSI reaches extreme levels"
        }
    ]
    
    created_strategies = []
    
    for strategy_data in default_strategies:
        try:
            strategy = Strategy(
                user_id=user_id,
                broker_id=None,  # No broker connected initially
                name=strategy_data["name"],
                description=strategy_data["description"],
                indicators=strategy_data["indicators"],
                logic_operator=strategy_data["logic_operator"],
                parameters=strategy_data["parameters"],
                entry_criteria=strategy_data["entry_criteria"],
                exit_criteria=strategy_data["exit_criteria"],
                is_active=True,
                is_live=False,
                is_backtested=False,
                broker_connected=False,
                execution_status="STOPPED",
                total_pnl=0.0,
                total_trades=0,
                winning_trades=0,
                losing_trades=0,
                win_rate=0.0,
                max_drawdown=0.0,
                sharpe_ratio=0.0,
                last_trade_pnl=0.0,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            db.add(strategy)
            created_strategies.append(strategy)
            
        except Exception as e:
            print(f"Warning: Failed to create strategy '{strategy_data['name']}': {e}")
            # Continue with other strategies even if one fails
    
    try:
        db.commit()
        print(f"Successfully created {len(created_strategies)} default strategies for user {user_id}")
        return created_strategies
    except Exception as e:
        print(f"Error committing default strategies: {e}")
        db.rollback()
        return []
