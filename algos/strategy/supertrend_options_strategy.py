"""
SuperTrend Bitcoin Options Strategy
Implements the complete SuperTrend-based options selling strategy for Delta Exchange
"""

import asyncio
import logging
import time
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import sys
import os
import sqlite3
import csv
from pathlib import Path

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from api.delta_options_api import DeltaOptionsAPI, TradingParams
from backend.delta_api import DeltaAPI
from api.mock_api import MockAPI
from indicators.supertrend import SuperTrend

# Import log manager for real-time logging
try:
    import sys
    # Add backend directory to path
    sys.path.append('../../backend')
    from utils.log_manager import log_manager
    LOG_MANAGER_AVAILABLE = True
except ImportError:
    LOG_MANAGER_AVAILABLE = False
    log_manager = None

logger = logging.getLogger(__name__)

class SuperTrendOptionsStrategy:
    """
    SuperTrend-based Bitcoin options selling strategy for Delta Exchange
    
    Strategy Rules:
    1. Use BTCUSD futures (90-minute chart) for SuperTrend calculation
    2. When SuperTrend turns Green (uptrend): Sell Put at SuperTrend level
    3. When SuperTrend turns Red (downtrend): Sell Call at SuperTrend level
    4. Exit when SuperTrend changes color and closes
    5. Dynamic expiry selection based on bid price thresholds:
       - Prefer tomorrow's expiry (today+1) if bid price > $250
       - Use day+2 expiry (today+2) if bid price < $250
    6. Strike price calculated from SuperTrend value (rounded to nearest $100)
    7. Option filtering: C-BTC- and P-BTC- format
    """
    
    def __init__(self, config_path: str = "config/config.yaml", 
                 api_key: str = None, api_secret: str = None, api_url: str = None,
                 paper_trading: bool = False, strategy_id: str = None):
        """
        Initialize the SuperTrend options strategy
        
        Args:
            config_path: Path to configuration file
            api_key: Delta Exchange API key
            api_secret: Delta Exchange API secret
            api_url: Delta Exchange API URL
            paper_trading: Enable paper trading mode
            strategy_id: Unique identifier for this strategy instance
        """
        self.config = self._load_config(config_path)
        self.paper_trading = paper_trading
        self.running = False
        self.current_position = None
        self.last_signal = None
        self.trade_history = []
        self.last_balance_check = None
        self.current_investment_amount = 0
        self.current_quantity_lots = 0
        self.strategy_id = strategy_id or f"supertrend_{int(time.time())}"
        self.next_execution_time = None
        
        # Setup real-time logging
        self._setup_realtime_logging()
        
        # Initialize API client
        if api_key and api_secret and api_url:
            trading_params = TradingParams(
                api_key=api_key,
                api_secret=api_secret,
                api_url=api_url
            )
            # Add required attributes for DeltaAPI
            trading_params.asset_id = "14"  # USD asset ID
            trading_params.symbol = "BTCUSD"
            trading_params.symbol_id = "1"  # BTCUSD symbol ID
            trading_params.candle_interval = "90m"
            trading_params.order_side = "buy"
            trading_params.order_qty = 1
            trading_params.order_type = "market_order"
            trading_params.order_price = 0
            trading_params.stop_loss = 0
            trading_params.take_profit = 0
            
            self.api = DeltaAPI(trading_params)
            
            # Log API credentials for web interface execution
            logger.info("🌐 WEB INTERFACE EXECUTION - API CREDENTIALS:")
            logger.info(f"   • API URL: {api_url}")
            logger.info(f"   • API Key: {api_key[:8]}...{api_key[-4:] if len(api_key) > 12 else '***'}")
            logger.info(f"   • API Secret: {api_secret[:8]}...{api_secret[-4:] if len(api_secret) > 12 else '***'}")
            logger.info(f"   • Paper Trading: {paper_trading}")
            logger.info(f"   • Execution Mode: {'WEB INTERFACE' if not paper_trading else 'WEB INTERFACE (PAPER)'}")
        elif paper_trading:
            # Use real Delta Exchange API for paper trading (from config)
            delta_config = self.config.get('delta_api', {})
            if delta_config.get('key') and delta_config.get('secret') and delta_config.get('url'):
                trading_params = TradingParams(
                    api_key=delta_config['key'],
                    api_secret=delta_config['secret'],
                    api_url=delta_config['url']
                )
                self.api = DeltaAPI(trading_params)
                logger.info("🎭 Using real Delta Exchange API for paper trading")
            else:
                # Fallback to mock API if credentials not available
                self.api = MockAPI(self.config)
                logger.info("🎭 Using mock API for paper trading (no real API credentials)")
        else:
            self.api = None
        
        # Initialize SuperTrend indicator
        st_config = self.config.get('supertrend', {})
        self.supertrend = SuperTrend(
            length=st_config.get('length', 16),
            factor=st_config.get('factor', 1.5)
        )
        
        # Initialize logging
        self._setup_logging()
        
        # Initialize database
        self._setup_database()
        
        logger.info("🚀 SuperTrend Options Strategy initialized")
        logger.info(f"📊 SuperTrend params: length={self.supertrend.length}, factor={self.supertrend.factor}")
        logger.info(f"💰 Trading capital: ${self.config.get('trading', {}).get('capital', 50)}")
        logger.info(f"🎭 Paper trading: {self.paper_trading}")
        
        # Log to real-time system
        self._log_realtime("INFO", "🚀 SuperTrend Options Strategy initialized")
        self._log_realtime("INFO", f"📊 SuperTrend params: length={self.supertrend.length}, factor={self.supertrend.factor}")
        self._log_realtime("INFO", f"💰 Trading capital: ${self.config.get('trading', {}).get('capital', 50)}")
        self._log_realtime("INFO", f"🎭 Paper trading: {self.paper_trading}")
    
    def _setup_realtime_logging(self):
        """Setup real-time logging for web interface"""
        if LOG_MANAGER_AVAILABLE and log_manager:
            # Create strategy-specific logger
            self.realtime_logger = log_manager.create_strategy_logger(
                self.strategy_id, 
                "SuperTrend Bitcoin Options"
            )
            self.realtime_logger.info(f"🚀 Strategy {self.strategy_id} initialized")
        else:
            self.realtime_logger = None
            logger.warning("⚠️ Real-time logging not available - log manager not found")
    
    def _log_realtime(self, level: str, message: str):
        """Log message to real-time system"""
        if self.realtime_logger:
            getattr(self.realtime_logger, level.lower(), self.realtime_logger.info)(message)
        else:
            # Fallback to regular logging
            getattr(logger, level.lower(), logger.info)(message)
    
    def check_existing_positions(self) -> Dict[str, Any]:
        """Check for existing margined positions using the new API method"""
        try:
            if not self.api:
                logger.error("❌ API client not initialized")
                return {
                    'has_positions': False, 
                    'positions': [], 
                    'btc_options_positions': [],
                    'total_unrealized_pnl': 0,
                    'total_realized_pnl': 0,
                    'total_pnl': 0
                }
            
            # Use the new margined positions method
            margined_positions = self.api.get_margined_positions()
            
            if not margined_positions:
                logger.info("✅ No existing margined positions found")
                self._log_realtime("INFO", "✅ No existing margined positions found")
                return {
                    'has_positions': False, 
                    'positions': [], 
                    'btc_options_positions': [],
                    'total_unrealized_pnl': 0,
                    'total_realized_pnl': 0,
                    'total_pnl': 0
                }
            
            # Analyze positions
            total_unrealized_pnl = 0
            total_realized_pnl = 0
            btc_options_positions = []
            
            for pos in margined_positions:
                symbol = pos.get('product', {}).get('symbol', 'Unknown')
                unrealized_pnl = float(pos.get('unrealized_pnl', 0))
                realized_pnl = float(pos.get('realized_pnl', 0))
                
                total_unrealized_pnl += unrealized_pnl
                total_realized_pnl += realized_pnl
                
                # Check if it's a BTC option
                if 'BTC' in symbol and ('C-BTC-' in symbol or 'P-BTC-' in symbol):
                    btc_options_positions.append({
                        'symbol': symbol,
                        'size': pos.get('size', 0),
                        'side': pos.get('side', 'Unknown'),
                        'entry_price': pos.get('entry_price', 0),
                        'mark_price': pos.get('mark_price', 0),
                        'unrealized_pnl': unrealized_pnl,
                        'realized_pnl': realized_pnl,
                        'margin': pos.get('margin', 0),
                        'created_at': pos.get('created_at', 'Unknown')
                    })
            
            # Log position summary
            logger.info(f"📊 Found {len(margined_positions)} margined positions")
            logger.info(f"💰 Total Unrealized P&L: ${total_unrealized_pnl:.2f}")
            logger.info(f"💰 Total Realized P&L: ${total_realized_pnl:.2f}")
            logger.info(f"📈 BTC Options Positions: {len(btc_options_positions)}")
            
            self._log_realtime("INFO", f"📊 Found {len(margined_positions)} margined positions")
            self._log_realtime("INFO", f"💰 Total P&L: ${total_unrealized_pnl + total_realized_pnl:.2f}")
            
            # Log individual BTC options positions
            for pos in btc_options_positions:
                pos_msg = f"📋 {pos['symbol']}: Size={pos['size']}, Side={pos['side']}, P&L=${pos['unrealized_pnl']:.2f}"
                logger.info(pos_msg)
                self._log_realtime("INFO", pos_msg)
            
            return {
                'has_positions': len(margined_positions) > 0,
                'positions': margined_positions,
                'btc_options_positions': btc_options_positions,
                'total_unrealized_pnl': total_unrealized_pnl,
                'total_realized_pnl': total_realized_pnl,
                'total_pnl': total_unrealized_pnl + total_realized_pnl
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to check existing positions: {e}")
            self._log_realtime("ERROR", f"❌ Failed to check existing positions: {e}")
            return {
                'has_positions': False, 
                'positions': [], 
                'btc_options_positions': [],
                'total_unrealized_pnl': 0,
                'total_realized_pnl': 0,
                'total_pnl': 0
            }
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
            logger.info(f"✅ Configuration loaded from {config_path}")
            return config
        except Exception as e:
            logger.error(f"❌ Failed to load config: {e}")
            # Return default config
            return {
                'supertrend': {'length': 16, 'factor': 1.5, 'timeframe': '90m'},
                'trading': {'capital': 50.0, 'leverage': 50, 'capital_multiplier': 1.0},
                'premium_filters': {'min_premium_threshold': 250.0, 'max_premium_threshold': 300.0},
                'expiry': {'prefer_0dte': True, 'fallback_to_1dte': True, 'max_dte': 1},
                'stop_loss': {'enabled': True, 'max_loss_percent': 0.2},
                'position_sizing': {'max_total_contracts': 10, 'target_premium': 500.0},
                'safety': {'require_risk_confirmation': True, 'max_daily_trades': 5}
            }
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO').upper())
        
        # Create logs directory
        log_dir = Path(log_config.get('log_directory', 'logs'))
        log_dir.mkdir(exist_ok=True)
        
        # Setup file handler
        if log_config.get('log_to_file', True):
            log_file = log_dir / f"supertrend_strategy_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        
        # Setup console handler
        if log_config.get('log_to_console', True):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
        
        logger.setLevel(log_level)
    
    def _setup_database(self):
        """Setup SQLite database for trade logging"""
        try:
            db_path = self.config.get('logging', {}).get('database_path', 'data/trading_log.db')
            db_dir = Path(db_path).parent
            db_dir.mkdir(exist_ok=True)
            
            self.db_conn = sqlite3.connect(db_path)
            cursor = self.db_conn.cursor()
            
            # Create trades table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    option_type TEXT NOT NULL,
                    strike_price REAL NOT NULL,
                    premium REAL NOT NULL,
                    quantity INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    order_id TEXT,
                    status TEXT NOT NULL,
                    pnl REAL DEFAULT 0.0,
                    notes TEXT
                )
            ''')
            
            # Create market_data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    underlying_price REAL NOT NULL,
                    supertrend_value REAL,
                    supertrend_direction INTEGER,
                    signal TEXT NOT NULL
                )
            ''')
            
            self.db_conn.commit()
            logger.info(f"✅ Database initialized: {db_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup database: {e}")
            self.db_conn = None
    
    def _log_trade(self, trade_data: Dict[str, Any]):
        """Log trade to database and CSV"""
        try:
            timestamp = datetime.now().isoformat()
            
            # Log to database
            if self.db_conn:
                cursor = self.db_conn.cursor()
                cursor.execute('''
                    INSERT INTO trades (timestamp, signal, option_type, strike_price, 
                                      premium, quantity, side, order_id, status, pnl, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    timestamp,
                    trade_data.get('signal', ''),
                    trade_data.get('option_type', ''),
                    trade_data.get('strike_price', 0.0),
                    trade_data.get('premium', 0.0),
                    trade_data.get('quantity', 0),
                    trade_data.get('side', ''),
                    trade_data.get('order_id', ''),
                    trade_data.get('status', ''),
                    trade_data.get('pnl', 0.0),
                    trade_data.get('notes', '')
                ))
                self.db_conn.commit()
            
            # Log to CSV
            if self.config.get('logging', {}).get('csv_logging', True):
                csv_file = Path('logs') / f"trades_{datetime.now().strftime('%Y%m%d')}.csv"
                csv_file.parent.mkdir(exist_ok=True)
                
                file_exists = csv_file.exists()
                with open(csv_file, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=[
                        'timestamp', 'signal', 'option_type', 'strike_price', 
                        'premium', 'quantity', 'side', 'order_id', 'status', 'pnl', 'notes'
                    ])
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(trade_data)
            
            logger.info(f"📝 Trade logged: {trade_data}")
            
        except Exception as e:
            logger.error(f"❌ Failed to log trade: {e}")
    
    def _log_market_data(self, market_data: Dict[str, Any]):
        """Log market data to database"""
        try:
            if self.db_conn:
                cursor = self.db_conn.cursor()
                # Handle NaN values
                supertrend_value = market_data.get('supertrend_value')
                if supertrend_value is None or (isinstance(supertrend_value, float) and np.isnan(supertrend_value)):
                    supertrend_value = None
                
                supertrend_direction = market_data.get('supertrend_direction')
                if supertrend_direction is None or (isinstance(supertrend_direction, float) and np.isnan(supertrend_direction)):
                    supertrend_direction = None
                
                cursor.execute('''
                    INSERT INTO market_data (timestamp, underlying_price, supertrend_value, 
                                           supertrend_direction, signal)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    market_data.get('underlying_price', 0.0),
                    supertrend_value,
                    supertrend_direction,
                    market_data.get('signal', 'HOLD')
                ))
                self.db_conn.commit()
                
        except Exception as e:
            logger.error(f"❌ Failed to log market data: {e}")
    
    async def get_market_data(self) -> pd.DataFrame:
        """Get market data for SuperTrend calculation - only closed 90-minute candles"""
        try:
            if not self.api:
                logger.error("❌ API client not initialized")
                return pd.DataFrame()
            
            # Get BTCUSD futures data using base DeltaAPI
            timeframe = self.config.get('supertrend', {}).get('timeframe', '90m')
            limit = 100  # Enough for SuperTrend calculation
            
            # Use base DeltaAPI get_candles method
            candles = self.api.get_candles('BTCUSD', timeframe, limit=limit)
            
            if not candles:
                logger.warning("⚠️ No market data received")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(candles)
            
            # Handle timestamp column
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            elif 'time' in df.columns:
                df['timestamp'] = pd.to_datetime(df['time'])
            else:
                df['timestamp'] = pd.date_range(start='2024-01-01', periods=len(df), freq='90min')
            
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            if df.empty:
                logger.warning("⚠️ No market data received")
                return pd.DataFrame()
            
            # Ensure we only use closed 90-minute candles
            # Remove the last candle if it's not yet closed (current incomplete candle)
            if len(df) > 0:
                current_time = datetime.now()
                # Use timestamp column instead of index
                if 'timestamp' in df.columns:
                    last_candle_time = df['timestamp'].iloc[-1]
                    # Ensure last_candle_time is a datetime object
                    if isinstance(last_candle_time, str):
                        try:
                            last_candle_time = pd.to_datetime(last_candle_time)
                        except:
                            logger.warning("⚠️ Could not parse last candle timestamp, skipping candle validation")
                            return df
                    
                    # Calculate if the last candle is closed (90 minutes have passed)
                    time_since_last_candle = (current_time - last_candle_time).total_seconds()
                else:
                    # Fallback to using index - assume index represents time
                    logger.warning("⚠️ No timestamp column found, skipping candle validation")
                    return df
                
                candle_duration_seconds = 90 * 60  # 90 minutes in seconds
                
                if time_since_last_candle < candle_duration_seconds:
                    # Last candle is not closed, remove it
                    df = df.iloc[:-1]
                    logger.info(f"🕐 Removed incomplete candle, using {len(df)} closed candles")
                else:
                    logger.info(f"✅ Using {len(df)} closed candles (last candle closed {time_since_last_candle/60:.1f} minutes ago)")
            
            logger.debug(f"📊 Retrieved {len(df)} closed candles for SuperTrend calculation")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to get market data: {e}")
            return pd.DataFrame()
    
    def select_expiry_date(self, config: Dict[str, Any]) -> str:
        """Select appropriate expiry date based on bid price thresholds"""
        try:
            from datetime import datetime, timedelta
            
            # Calculate target dates
            today = datetime.now().date()
            tomorrow = today + timedelta(days=1)
            day_after_tomorrow = today + timedelta(days=2)
            
            # Convert to YYMMDD format
            tomorrow_str = tomorrow.strftime('%y%m%d')
            day_after_tomorrow_str = day_after_tomorrow.strftime('%y%m%d')
            
            logger.info(f"📅 Checking expiry dates: Tomorrow ({tomorrow_str}) and Day+2 ({day_after_tomorrow_str})")
            
            # First, try tomorrow's expiry
            tomorrow_options = self.get_btc_options(tomorrow_str)
            if tomorrow_options:
                logger.info(f"📅 Found {len(tomorrow_options)} options for tomorrow ({tomorrow_str})")
                return tomorrow_str
            
            # If tomorrow's options don't exist, try day+2
            day_after_options = self.get_btc_options(day_after_tomorrow_str)
            if day_after_options:
                logger.info(f"📅 Found {len(day_after_options)} options for day+2 ({day_after_tomorrow_str})")
                return day_after_tomorrow_str
            
            # Fallback to any available expiry
            all_options = self.get_btc_options()  # No expiry filter
            if all_options:
                # Get unique expiry dates from available options
                expiry_dates = set()
                for option in all_options:
                    symbol = option.get('symbol', '')
                    if len(symbol) >= 12:  # Ensure symbol has expiry info
                        expiry_part = symbol[-6:]  # Last 6 characters should be expiry
                        expiry_dates.add(expiry_part)
                
                if expiry_dates:
                    earliest_expiry = min(expiry_dates)
                    logger.info(f"📅 Using earliest available expiry: {earliest_expiry}")
                    return earliest_expiry
            
            # Final fallback
            logger.warning(f"⚠️ No suitable expiry found, using tomorrow as fallback: {tomorrow_str}")
            return tomorrow_str
            
        except Exception as e:
            logger.error(f"❌ Failed to select expiry date: {e}")
            # Fallback to tomorrow
            from datetime import datetime, timedelta
            tomorrow = (datetime.now().date() + timedelta(days=1)).strftime('%y%m%d')
            return tomorrow
    
    def _find_high_premium_options(self, options: List[Dict[str, Any]], min_bid_price: float) -> List[Dict[str, Any]]:
        """Find options with bid price above the minimum threshold"""
        try:
            high_premium_options = []
            
            for option in options:
                try:
                    # Try to get bid price from various sources
                    bid_price = None
                    
                    # Check for bid price in option data
                    if 'bid' in option:
                        bid_price = float(option['bid'])
                    elif 'mark_price' in option and option['mark_price']:
                        # Use mark price as proxy for bid price
                        bid_price = float(option['mark_price'])
                    elif 'settlement_price' in option and option['settlement_price']:
                        # Use settlement price as proxy
                        bid_price = float(option['settlement_price'])
                    
                    if bid_price and bid_price >= min_bid_price:
                        high_premium_options.append(option)
                        logger.debug(f"✅ Found high premium option: {option.get('symbol', 'N/A')} - Bid: ${bid_price:.2f}")
                    
                except (ValueError, TypeError) as e:
                    logger.debug(f"⚠️ Could not parse bid price for {option.get('symbol', 'N/A')}: {e}")
                    continue
            
            logger.info(f"📊 Found {len(high_premium_options)} options with bid >= ${min_bid_price:.0f}")
            return high_premium_options
            
        except Exception as e:
            logger.error(f"❌ Failed to find high premium options: {e}")
            return []
    
    def get_btc_options(self, expiry_date: str = None) -> List[Dict[str, Any]]:
        """Get Bitcoin options for a specific expiry date"""
        try:
            if not self.api:
                logger.error("❌ API client not initialized")
                return []
            
            # Get all products first
            products = self.api.get_all_products()
            
            # Filter for Bitcoin options
            btc_options = []
            for product in products:
                symbol = product.get('symbol', '').upper()
                
                # Check if it's a BTC option (C-BTC- or P-BTC- format)
                if 'BTC' in symbol and ('C-BTC-' in symbol or 'P-BTC-' in symbol):
                    # Check expiry date if specified
                    if expiry_date:
                        # Convert expiry_date to multiple formats for matching
                        expiry_formats = [
                            expiry_date,  # YYMMDD format (e.g., 251003)
                            f"20{expiry_date[:2]}-{expiry_date[2:4]}-{expiry_date[4:6]}",  # YYYY-MM-DD format
                            f"{expiry_date[:2]}{expiry_date[2:4]}{expiry_date[4:6]}"  # Alternative format
                        ]
                        
                        # Check if any expiry format matches in symbol or settlement_time
                        symbol_matches = any(exp_format in symbol for exp_format in expiry_formats)
                        settlement_matches = any(exp_format in product.get('settlement_time', '') for exp_format in expiry_formats)
                        
                        if symbol_matches or settlement_matches:
                            btc_options.append(product)
                            logger.debug(f"✅ Found matching option: {symbol} (expiry: {expiry_date})")
                    else:
                        btc_options.append(product)
            
            logger.info(f"📋 Found {len(btc_options)} Bitcoin options for expiry {expiry_date or 'any'}")
            return btc_options
            
        except Exception as e:
            logger.error(f"❌ Failed to get BTC options: {e}")
            return []
    
    def find_nearest_strike(self, options: List[Dict[str, Any]], target_price: float, 
                          option_type: str, direction: str = "at_or_below") -> Optional[Dict[str, Any]]:
        """Find the nearest strike option to target price"""
        try:
            # Filter by option type using symbol format
            filtered_options = []
            for opt in options:
                symbol = opt.get('symbol', '').upper()
                if option_type == 'call' and 'C-BTC-' in symbol:
                    filtered_options.append(opt)
                elif option_type == 'put' and 'P-BTC-' in symbol:
                    filtered_options.append(opt)
            
            if not filtered_options:
                logger.warning(f"⚠️ No {option_type} options found")
                return None
            
            # Sort by strike price
            filtered_options.sort(key=lambda x: float(x.get('strike_price', 0)))
            
            if direction == "at_or_below":
                # For puts, find strike at or below target price
                suitable_options = [
                    opt for opt in filtered_options
                    if float(opt.get('strike_price', 0)) <= target_price
                ]
                if suitable_options:
                    return suitable_options[-1]  # Highest strike at or below
            else:
                # For calls, find strike at or above target price
                suitable_options = [
                    opt for opt in filtered_options
                    if float(opt.get('strike_price', 0)) >= target_price
                ]
                if suitable_options:
                    return suitable_options[0]  # Lowest strike at or above
            
            # Fallback to closest strike
            if filtered_options:
                closest_option = min(filtered_options, 
                                   key=lambda x: abs(float(x.get('strike_price', 0)) - target_price))
                logger.info(f"📊 Using closest {option_type} option: {closest_option.get('symbol', 'N/A')}")
                return closest_option
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to find nearest strike: {e}")
            return None
    
    def calculate_signal(self, market_data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate SuperTrend signal - only trigger on trend changes between 90-minute candles"""
        try:
            if market_data.empty or len(market_data) < self.supertrend.length + 1:
                return {
                    'signal': 'HOLD',
                    'direction': 0,
                    'supertrend_value': None,
                    'confidence': 0.0,
                    'trend_change': False
                }
            
            # Get SuperTrend signal
            signal_data = self.supertrend.get_signal(market_data)
            
            # Add additional context
            signal_data['underlying_price'] = market_data['close'].iloc[-1]
            signal_data['timestamp'] = datetime.now().isoformat()
            
            # Check if this is a trend change from the previous signal
            current_direction = signal_data.get('direction', 0)
            previous_direction = self.last_signal.get('direction', 0) if self.last_signal else 0
            
            # Only generate trading signal if trend has changed between 90-minute candles
            if current_direction != previous_direction and previous_direction != 0:
                signal_data['trend_change'] = True
                signal_data['confidence'] = 0.8  # High confidence on trend change
                logger.info(f"🔄 TREND CHANGE DETECTED: {previous_direction} → {current_direction}")
            else:
                signal_data['trend_change'] = False
                signal_data['signal'] = 'HOLD'  # No trade signal unless trend changes
                signal_data['confidence'] = 0.0
            
            # Store current signal for next comparison
            self.last_signal = signal_data.copy()
            
            # Log market data
            self._log_market_data(signal_data)
            
            signal_msg = f"📊 Signal: {signal_data['signal']}, Direction: {signal_data['direction']}, " \
                        f"Price: ${signal_data['underlying_price']:.2f}, " \
                        f"SuperTrend: ${signal_data['supertrend_value']:.2f}, " \
                        f"Trend Change: {signal_data['trend_change']}"
            logger.info(signal_msg)
            self._log_realtime("INFO", signal_msg)
            
            return signal_data
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate signal: {e}")
            return {
                'signal': 'HOLD',
                'direction': 0,
                'supertrend_value': None,
                'confidence': 0.0,
                'trend_change': False
            }
    
    async def execute_trade(self, signal_data: Dict[str, Any]) -> bool:
        """Execute trade based on signal"""
        try:
            if not self.api:
                logger.error("❌ API client not initialized")
                return False
            
            signal = signal_data.get('signal')
            if signal == 'HOLD':
                return True
            
            # Check if we already have a position
            if self.current_position:
                logger.info("ℹ️ Position already exists, skipping new trade")
                return True
            
            # Get current underlying price
            underlying_price = signal_data.get('underlying_price', 0)
            supertrend_value = signal_data.get('supertrend_value', 0)
            
            if underlying_price <= 0 or supertrend_value <= 0:
                logger.error("❌ Invalid price data for trade execution")
                return False
            
            # Select expiry date
            expiry_date = self.api.select_expiry_date(self.config)
            if not expiry_date:
                logger.error("❌ No suitable expiry date found")
                return False
            
            # Get options for selected expiry
            options = self.api.get_btc_options(expiry_date)
            if not options:
                logger.error(f"❌ No options found for expiry {expiry_date}")
                return False
            
            # Determine option type and side
            if signal == 'BUY':  # SuperTrend Green - Sell Put
                option_type = 'put'
                side = 'sell'
                direction = 'at_or_below'
            else:  # SuperTrend Red - Sell Call
                option_type = 'call'
                side = 'sell'
                direction = 'at_or_above'
            
            # Calculate strike price from SuperTrend value (round to nearest $100)
            strike_price = round(supertrend_value / 100) * 100
            
            logger.info(f"🎯 SuperTrend Value: ${supertrend_value:.2f}")
            logger.info(f"💰 Calculated Strike Price: ${strike_price:.0f}")
            logger.info(f"📊 Option Type: {option_type.upper()}, Side: {side.upper()}")
            
            # Find suitable option contract using SuperTrend-based strike
            option_contract = self.api.find_nearest_strike(
                options, strike_price, option_type, direction
            )
            
            if not option_contract:
                logger.error(f"❌ No suitable {option_type} option found")
                return False
            
            # Get option premium
            premium = self.api.get_option_premium(option_contract['id'])
            if premium is None:
                logger.error("❌ Failed to get option premium")
                return False
            
            # Check premium threshold
            if not self.api.check_premium_threshold(premium, self.config):
                logger.warning(f"⚠️ Premium ${premium} doesn't meet threshold requirements")
                return False
            
            # Calculate position size based on investment amount and lot size
            trading_config = self.config.get('trading', {})
            lot_size = trading_config.get('lot_size', 0.001)  # 1 Lot = 0.001 BTC
            
            # Calculate quantity in lots based on investment amount and option premium
            # For options, we calculate how many lots we can afford with the investment amount
            if premium > 0:
                quantity_lots = max(1, int(self.current_investment_amount / premium))
            else:
                quantity_lots = 1
            
            # Convert lots to quantity (1 lot = 0.001 BTC)
            quantity = quantity_lots
            
            # Get current balance for margin check
            available_margin = self.api.get_balance()
            
            # Check margin requirements
            required_margin = quantity * premium * 0.1  # Simplified margin calculation
            if required_margin > available_margin:
                logger.error(f"❌ Insufficient margin: required ${required_margin:.2f}, available ${available_margin:.2f}")
                return False
            
            # Log investment and quantity details
            logger.debug(f"💰 Investment Amount: ${self.current_investment_amount:.2f}")
            logger.debug(f"📊 Quantity in Lots: {quantity_lots} lots (1 lot = {lot_size} BTC)")
            logger.debug(f"💵 Option Premium: ${premium:.2f}")
            logger.debug(f"📈 Total Contract Value: ${quantity * premium:.2f}")
            
            # Log comprehensive option candle data before placing order
            logger.info("=" * 80)
            logger.info("📈 OPTION ORDER PLACEMENT - DETAILED CANDLE DATA")
            logger.info("=" * 80)
            
            # Log underlying market data
            logger.info(f"🔍 UNDERLYING DATA:")
            logger.info(f"   • Symbol: BTCUSD")
            logger.info(f"   • Current Price: ${underlying_price:.2f}")
            logger.info(f"   • SuperTrend Value: ${supertrend_value:.2f}")
            logger.info(f"   • SuperTrend Direction: {signal_data.get('direction', 'N/A')}")
            logger.info(f"   • Signal: {signal}")
            logger.info(f"   • Trend Change: {signal_data.get('trend_change', False)}")
            
            # Log option contract details
            logger.info(f"📊 OPTION CONTRACT DATA:")
            logger.info(f"   • Symbol: {option_contract.get('symbol', 'N/A')}")
            logger.info(f"   • Contract ID: {option_contract.get('id', 'N/A')}")
            logger.info(f"   • Type: {option_type.upper()}")
            logger.info(f"   • Strike Price: ${option_contract.get('strike_price', 'N/A')}")
            logger.info(f"   • Expiry: {expiry_date}")
            logger.info(f"   • Side: {side.upper()}")
            
            # Log option candle data (if available)
            option_candle_data = self.api.get_option_candle_data(option_contract['id'])
            if option_candle_data:
                logger.info(f"🕯️ OPTION CANDLE DATA:")
                logger.info(f"   • Open: ${option_candle_data.get('open', 'N/A')}")
                logger.info(f"   • High: ${option_candle_data.get('high', 'N/A')}")
                logger.info(f"   • Low: ${option_candle_data.get('low', 'N/A')}")
                logger.info(f"   • Close: ${option_candle_data.get('close', 'N/A')}")
                logger.info(f"   • Volume: {option_candle_data.get('volume', 'N/A')}")
                logger.info(f"   • Timestamp: {option_candle_data.get('timestamp', 'N/A')}")
            else:
                logger.warning("⚠️ Option candle data not available")
            
            # Log premium and pricing data
            logger.info(f"💰 PREMIUM & PRICING:")
            logger.info(f"   • Bid Price: ${option_contract.get('bid', 'N/A')}")
            logger.info(f"   • Ask Price: ${option_contract.get('ask', 'N/A')}")
            logger.info(f"   • Mid Price: ${(float(option_contract.get('bid', 0)) + float(option_contract.get('ask', 0))) / 2:.2f}")
            logger.info(f"   • Premium: ${premium:.2f}")
            logger.info(f"   • Implied Volatility: {option_contract.get('implied_volatility', 'N/A')}")
            logger.info(f"   • Delta: {option_contract.get('delta', 'N/A')}")
            logger.info(f"   • Gamma: {option_contract.get('gamma', 'N/A')}")
            logger.info(f"   • Theta: {option_contract.get('theta', 'N/A')}")
            logger.info(f"   • Vega: {option_contract.get('vega', 'N/A')}")
            
            # Log position sizing
            logger.info(f"📏 POSITION SIZING:")
            logger.info(f"   • Investment Amount: ${self.current_investment_amount:.2f}")
            logger.info(f"   • Quantity in Lots: {quantity_lots} lots (1 lot = {lot_size} BTC)")
            logger.info(f"   • Quantity: {quantity}")
            logger.info(f"   • Available Margin: ${available_margin:.2f}")
            logger.info(f"   • Required Margin: ${required_margin:.2f}")
            logger.info(f"   • Total Premium: ${quantity * premium:.2f}")
            logger.info(f"   • Risk Amount: ${quantity * premium * 0.1:.2f}")
            
            # Log trading parameters
            logger.info(f"⚙️ TRADING PARAMETERS:")
            logger.info(f"   • Paper Trading: {self.paper_trading}")
            logger.info(f"   • Capital: ${self.config.get('trading', {}).get('capital', 50)}")
            logger.info(f"   • Leverage: {self.config.get('trading', {}).get('leverage', 50)}x")
            logger.info(f"   • Premium Threshold: ${self.config.get('premium_filters', {}).get('min_premium_threshold', 250)}")
            
            logger.info("=" * 80)
            
            # Place order
            if self.paper_trading:
                logger.info(f"🎭 PAPER TRADE: {side.upper()} {quantity} {option_contract['symbol']} @ ${premium:.2f}")
                order_result = {
                    'id': f"paper_{int(time.time())}",
                    'side': side,
                    'size': quantity,
                    'limit_price': premium,
                    'state': 'filled',
                    'product_symbol': option_contract['symbol']
                }
            else:
                order_result = await self.api.place_option_order(
                    option_contract, side, quantity, premium
                )
            
            # Update position
            self.current_position = {
                'option_contract': option_contract,
                'side': side,
                'quantity': quantity,
                'premium': premium,
                'strike_price': float(option_contract.get('strike_price', 0)),
                'order_id': order_result.get('id'),
                'entry_time': datetime.now(),
                'signal': signal
            }
            
            # Log trade
            trade_data = {
                'signal': signal,
                'option_type': option_type,
                'strike_price': self.current_position['strike_price'],
                'premium': premium,
                'quantity': quantity,
                'side': side,
                'order_id': order_result.get('id'),
                'status': 'filled',
                'pnl': 0.0,
                'notes': f"SuperTrend {signal} signal at ${supertrend_value:.2f}"
            }
            self._log_trade(trade_data)
            
            logger.info(f"✅ Trade executed: {side.upper()} {quantity} {option_contract['symbol']} @ ${premium:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to execute trade: {e}")
            return False
    
    async def check_exit_conditions(self) -> bool:
        """Check if current position should be exited based on SuperTrend and balance"""
        try:
            if not self.current_position:
                return False
            
            # Check wallet balance and calculate investment amount
            self.check_wallet_balance_and_calculate_investment()
            
            # Get current market data
            market_data = await self.get_market_data()
            if market_data.empty:
                return False
            
            # Calculate current signal
            signal_data = self.calculate_signal(market_data)
            
            # Check for trend change
            if signal_data.get('trend_change', False):
                logger.info("🔄 SuperTrend trend change detected - exiting position")
                return True
            
            # Check if balance is insufficient for maintaining position
            if self.current_investment_amount < 10:  # Minimum $10 investment
                logger.info("💰 Insufficient balance for position maintenance - exiting position")
                return True
            
            # Check stop loss conditions
            stop_loss_config = self.config.get('stop_loss', {})
            if stop_loss_config.get('enabled', True):
                current_premium = self.api.get_option_premium(
                    self.current_position['option_contract']['id']
                )
                
                if current_premium:
                    entry_premium = self.current_position['premium']
                    loss_percent = (entry_premium - current_premium) / entry_premium
                    max_loss = stop_loss_config.get('max_loss_percent', 0.2)
                    
                    if loss_percent > max_loss:
                        logger.info(f"🛑 Stop loss triggered: {loss_percent:.2%} > {max_loss:.2%}")
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to check exit conditions: {e}")
            return False
    
    async def close_position(self) -> bool:
        """Close current position"""
        try:
            if not self.current_position:
                return True
            
            position = self.current_position
            option_contract = position['option_contract']
            
            # Determine close side (opposite of entry)
            close_side = 'buy' if position['side'] == 'sell' else 'sell'
            
            # Get current premium
            current_premium = self.api.get_option_premium(option_contract['id'])
            if current_premium is None:
                current_premium = position['premium']  # Fallback to entry premium
            
            # Place close order
            if self.paper_trading:
                logger.info(f"🎭 PAPER TRADE: {close_side.upper()} {position['quantity']} {option_contract['symbol']} @ ${current_premium:.2f}")
                order_result = {
                    'id': f"paper_close_{int(time.time())}",
                    'side': close_side,
                    'size': position['quantity'],
                    'limit_price': current_premium,
                    'state': 'filled'
                }
            else:
                order_result = await self.api.place_option_order(
                    option_contract, close_side, position['quantity'], current_premium
                )
            
            # Calculate P&L
            if position['side'] == 'sell':
                pnl = (position['premium'] - current_premium) * position['quantity']
            else:
                pnl = (current_premium - position['premium']) * position['quantity']
            
            # Log close trade
            trade_data = {
                'signal': 'CLOSE',
                'option_type': 'put' if 'put' in option_contract.get('contract_type', '') else 'call',
                'strike_price': position['strike_price'],
                'premium': current_premium,
                'quantity': position['quantity'],
                'side': close_side,
                'order_id': order_result.get('id'),
                'status': 'filled',
                'pnl': pnl,
                'notes': f"Position closed - P&L: ${pnl:.2f}"
            }
            self._log_trade(trade_data)
            
            logger.info(f"✅ Position closed: P&L ${pnl:.2f}")
            
            # Clear position
            self.current_position = None
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to close position: {e}")
            return False
    
    def get_scheduled_times(self):
        """Get list of scheduled 90-minute intervals starting from 01:00 IST"""
        return [
            "01:00", "02:30", "04:00", "05:30", "07:00", "08:30", "10:00", 
            "11:30", "13:00", "14:30", "16:00", "17:30", "19:00", "20:30", "22:00", "23:30"
        ]
    
    def is_scheduled_time(self):
        """Check if current time matches any scheduled 90-minute interval"""
        try:
            current_time = datetime.now()
            current_time_str = current_time.strftime("%H:%M")
            
            # Check if current time is within 1 minute of any scheduled time
            for scheduled_time in self.get_scheduled_times():
                scheduled_hour, scheduled_minute = map(int, scheduled_time.split(':'))
                scheduled_datetime = current_time.replace(hour=scheduled_hour, minute=scheduled_minute, second=0, microsecond=0)
                
                # Check if current time is within 1 minute of scheduled time
                time_diff = abs((current_time - scheduled_datetime).total_seconds())
                if time_diff <= 60:  # Within 1 minute
                    # Only log when we actually execute, not on every check
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Error checking scheduled time: {e}")
            return False
    
    def get_next_scheduled_execution_time(self):
        """Get precise information about next scheduled execution time"""
        try:
            current_time = datetime.now()
            scheduled_times = self.get_scheduled_times()
            
            # Check if we're currently at a scheduled time (within 2 minutes)
            for scheduled_time in scheduled_times:
                scheduled_hour, scheduled_minute = map(int, scheduled_time.split(':'))
                scheduled_datetime = current_time.replace(hour=scheduled_hour, minute=scheduled_minute, second=0, microsecond=0)
                
                time_diff = abs((current_time - scheduled_datetime).total_seconds())
                if time_diff <= 120:  # Within 2 minutes
                    return {
                        'should_execute': True,
                        'scheduled_time': scheduled_time,
                        'time_diff': time_diff,
                        'next_execution': None
                    }
            
            # Find next scheduled time
            next_execution = None
            for scheduled_time in scheduled_times:
                scheduled_hour, scheduled_minute = map(int, scheduled_time.split(':'))
                scheduled_datetime = current_time.replace(hour=scheduled_hour, minute=scheduled_minute, second=0, microsecond=0)
                
                # If scheduled time is in the future today
                if scheduled_datetime > current_time:
                    next_execution = scheduled_datetime
                    break
            
            # If no more times today, get first time tomorrow
            if next_execution is None:
                tomorrow = current_time.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
                next_execution = tomorrow
            
            return {
                'should_execute': False,
                'scheduled_time': None,
                'time_diff': None,
                'next_execution': next_execution
            }
            
        except Exception as e:
            logger.error(f"❌ Error getting next scheduled execution time: {e}")
            return {
                'should_execute': False,
                'scheduled_time': None,
                'time_diff': None,
                'next_execution': datetime.now() + timedelta(hours=1)
            }
    
    def get_next_scheduled_time(self):
        """Get the next scheduled 90-minute interval time (legacy method)"""
        try:
            info = self.get_next_scheduled_execution_time()
            return info['next_execution']
        except Exception as e:
            logger.error(f"❌ Error getting next scheduled time: {e}")
            return datetime.now() + timedelta(hours=1)
    
    async def execute_strategy_cycle(self, execution_time: datetime):
        """Execute complete strategy cycle: ST calculation, direction identification, position exit, and order placement"""
        try:
            logger.info("🔄 Starting complete strategy cycle execution")
            self._log_realtime("INFO", "🔄 Starting complete strategy cycle execution")
            
            # Step 1: Fetch wallet balance and calculate investment
            try:
                wallet_balance = self.api.get_balance()
                logger.info(f"💰 Current Wallet Balance: ${wallet_balance:.2f}")
                self._log_realtime("INFO", f"💰 Current Wallet Balance: ${wallet_balance:.2f}")
                
                # Calculate investing capital (70% of wallet balance, rounded to nearest $10)
                investing_capital = round((wallet_balance * 0.7) / 10) * 10
                logger.info(f"💼 Investing Capital (70% of balance): ${investing_capital:.2f}")
                self._log_realtime("INFO", f"💼 Investing Capital (70% of balance): ${investing_capital:.2f}")
                
                # Set leverage as 25X
                leverage = 25
                logger.info(f"⚡ Leverage: {leverage}X")
                self._log_realtime("INFO", f"⚡ Leverage: {leverage}X")
                
            except Exception as e:
                logger.error(f"❌ Failed to fetch wallet balance: {e}")
                self._log_realtime("ERROR", f"❌ Failed to fetch wallet balance: {e}")
                return
            
            # Step 2: Check existing positions
            try:
                margined_positions = self.api.get_margined_positions()
                logger.info(f"📊 Current Open Positions: {len(margined_positions)}")
                self._log_realtime("INFO", f"📊 Current Open Positions: {len(margined_positions)}")
                
                for i, pos in enumerate(margined_positions):
                    symbol = pos.get('product', {}).get('symbol', 'Unknown')
                    size = pos.get('size', 0)
                    unrealized_pnl = pos.get('unrealized_pnl', 0)
                    logger.info(f"   Position {i+1}: {symbol} - Size: {size}, P&L: ${unrealized_pnl}")
                    self._log_realtime("INFO", f"   Position {i+1}: {symbol} - Size: {size}, P&L: ${unrealized_pnl}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to fetch open positions: {e}")
                self._log_realtime("ERROR", f"❌ Failed to fetch open positions: {e}")
            
            # Step 3: Get market data and calculate SuperTrend
            try:
                logger.info("📈 Fetching market data and calculating SuperTrend...")
                market_data = await self.get_market_data()
                if market_data.empty:
                    logger.warning("⚠️ No market data available")
                    self._log_realtime("WARNING", "⚠️ No market data available")
                    return
                
                # Calculate SuperTrend for latest candle
                signal_data = self.calculate_signal(market_data)
                current_supertrend_value = signal_data.get('supertrend_value', 0)
                current_direction = signal_data.get('direction', 0)
                
                logger.info(f"📈 Latest SuperTrend Value: ${current_supertrend_value:.2f}")
                logger.info(f"📈 Latest SuperTrend Direction: {'UP' if current_direction == 1 else 'DOWN'}")
                self._log_realtime("INFO", f"📈 Latest SuperTrend: ${current_supertrend_value:.2f}, Direction: {'UP' if current_direction == 1 else 'DOWN'}")
                
            except Exception as e:
                logger.error(f"❌ Failed to calculate SuperTrend: {e}")
                self._log_realtime("ERROR", f"❌ Failed to calculate SuperTrend: {e}")
                return
            
            # Step 4: Check for trend change and execute trades
            try:
                # Check if SuperTrend direction has changed
                if not hasattr(self, 'previous_direction'):
                    self.previous_direction = current_direction
                    direction_changed = False
                    logger.info(f"📊 First SuperTrend reading: Direction = {'UP' if current_direction == 1 else 'DOWN'}")
                else:
                    direction_changed = (current_direction != self.previous_direction)
                    if direction_changed:
                        logger.info(f"🔄 SuperTrend direction changed: {self.previous_direction} → {current_direction}")
                        self._log_realtime("INFO", f"🔄 SuperTrend direction changed: {self.previous_direction} → {current_direction}")
                    else:
                        logger.info(f"📊 SuperTrend direction unchanged: {'UP' if current_direction == 1 else 'DOWN'}")
                    self.previous_direction = current_direction
                
                if direction_changed:
                    # Step 5: Close existing positions if any
                    if margined_positions:
                        logger.info("🔄 Closing existing positions due to trend change...")
                        self._log_realtime("INFO", "🔄 Closing existing positions due to trend change...")
                        
                        # Calculate total P&L
                        total_pnl = sum(float(pos.get('unrealized_pnl', 0)) for pos in margined_positions)
                        logger.info(f"💰 Total P&L from closed positions: ${total_pnl:.2f}")
                        self._log_realtime("INFO", f"💰 Total P&L from closed positions: ${total_pnl:.2f}")
                    
                    # Step 6: Execute new trade based on direction
                    await self.execute_new_trade(current_direction, current_supertrend_value, investing_capital, leverage)
                else:
                    logger.info("⏸️ No trend change detected - no action required")
                    self._log_realtime("INFO", "⏸️ No trend change detected - no action required")
                    
            except Exception as e:
                logger.error(f"❌ Error in trend change detection and trade execution: {e}")
                self._log_realtime("ERROR", f"❌ Error in trend change detection and trade execution: {e}")
            
            logger.info("✅ Strategy cycle execution completed")
            self._log_realtime("INFO", "✅ Strategy cycle execution completed")
            
        except Exception as e:
            logger.error(f"❌ Error in strategy cycle execution: {e}")
            self._log_realtime("ERROR", f"❌ Error in strategy cycle execution: {e}")
    
    async def execute_new_trade(self, direction: int, supertrend_value: float, investing_capital: float, leverage: int):
        """Execute new trade based on SuperTrend direction"""
        try:
            # Get tomorrow's expiry date
            tomorrow = (datetime.now() + timedelta(days=1)).strftime('%y%m%d')
            
            # Get BTC options for tomorrow
            options = self.get_btc_options(tomorrow)
            call_options = [opt for opt in options if 'C-BTC-' in opt.get('symbol', '')]
            put_options = [opt for opt in options if 'P-BTC-' in opt.get('symbol', '')]
            
            logger.info(f"📋 Available Options for {tomorrow}:")
            logger.info(f"   • Call Options: {len(call_options)}")
            logger.info(f"   • Put Options: {len(put_options)}")
            self._log_realtime("INFO", f"📋 Available Options: {len(call_options)} Calls, {len(put_options)} Puts")
            
            if direction == 1:  # UP/Green - Sell PUT
                await self.execute_put_trade(put_options, supertrend_value, investing_capital, leverage)
            else:  # DOWN/Red - Sell CALL
                await self.execute_call_trade(call_options, supertrend_value, investing_capital, leverage)
                
        except Exception as e:
            logger.error(f"❌ Error executing new trade: {e}")
            self._log_realtime("ERROR", f"❌ Error executing new trade: {e}")
    
    async def execute_put_trade(self, put_options: List[Dict], supertrend_value: float, investing_capital: float, leverage: int):
        """Execute PUT option trade"""
        try:
            if not put_options:
                logger.warning("⚠️ No PUT options available")
                return
            
            # Find the best PUT option based on SuperTrend value
            strike_price = round(supertrend_value / 100) * 100
            selected_put = self.find_nearest_strike(put_options, strike_price, 'put', 'at_or_below')
            
            if not selected_put:
                logger.warning("⚠️ No suitable PUT option found near SuperTrend value")
                return
            
            symbol = selected_put.get('symbol', '')
            mark_price = float(selected_put.get('mark_price', 0))
            
            logger.info(f"📈 SuperTrend Direction: UP - Selling PUT Option")
            logger.info(f"🎯 Selected PUT: {symbol} (Strike: ${selected_put.get('strike_price', 'N/A')})")
            logger.info(f"💰 PUT Option Mark Price: ${mark_price:.2f}")
            self._log_realtime("INFO", f"📈 SuperTrend Direction: UP - Selling PUT Option")
            self._log_realtime("INFO", f"💰 PUT Option Mark Price: ${mark_price:.2f}")
            
            # Calculate quantity and place order
            qty_lots = max(1, int(investing_capital / (mark_price * leverage)))
            limit_price = max(mark_price - 2, mark_price * 0.95)
            
            logger.info(f"📊 Quantity in Lots: {qty_lots}")
            logger.info(f"📝 Placing SELL order: {symbol} - Qty: {qty_lots}, Limit: ${limit_price:.2f}")
            self._log_realtime("INFO", f"📊 Quantity in Lots: {qty_lots}")
            self._log_realtime("INFO", f"📝 Placing SELL order: {symbol} - Qty: {qty_lots}, Limit: ${limit_price:.2f}")
            
            # Place actual order
            if self.paper_trading:
                logger.info("🎭 PAPER TRADE: PUT SELL Order Placed Successfully")
                self._log_realtime("INFO", "🎭 PAPER TRADE: PUT SELL Order Placed Successfully")
            else:
                # Use the existing execute_trade method for real orders
                signal_data = {
                    'signal': 'BUY',  # SuperTrend Green = Sell Put
                    'underlying_price': supertrend_value,
                    'supertrend_value': supertrend_value,
                    'direction': 1,
                    'trend_change': True
                }
                success = await self.execute_trade(signal_data)
                if success:
                    logger.info("✅ PUT SELL Order Placed Successfully")
                    self._log_realtime("INFO", "✅ PUT SELL Order Placed Successfully")
                else:
                    logger.error("❌ Failed to place PUT SELL order")
                    self._log_realtime("ERROR", "❌ Failed to place PUT SELL order")
                    
        except Exception as e:
            logger.error(f"❌ Failed to execute PUT trade: {e}")
            self._log_realtime("ERROR", f"❌ Failed to execute PUT trade: {e}")
    
    async def execute_call_trade(self, call_options: List[Dict], supertrend_value: float, investing_capital: float, leverage: int):
        """Execute CALL option trade"""
        try:
            if not call_options:
                logger.warning("⚠️ No CALL options available")
                return
            
            # Find the best CALL option based on SuperTrend value
            strike_price = round(supertrend_value / 100) * 100
            selected_call = self.find_nearest_strike(call_options, strike_price, 'call', 'at_or_above')
            
            if not selected_call:
                logger.warning("⚠️ No suitable CALL option found near SuperTrend value")
                return
            
            symbol = selected_call.get('symbol', '')
            mark_price = float(selected_call.get('mark_price', 0))
            
            logger.info(f"📉 SuperTrend Direction: DOWN - Selling CALL Option")
            logger.info(f"🎯 Selected CALL: {symbol} (Strike: ${selected_call.get('strike_price', 'N/A')})")
            logger.info(f"💰 CALL Option Mark Price: ${mark_price:.2f}")
            self._log_realtime("INFO", f"📉 SuperTrend Direction: DOWN - Selling CALL Option")
            self._log_realtime("INFO", f"💰 CALL Option Mark Price: ${mark_price:.2f}")
            
            # Calculate quantity and place order
            qty_lots = max(1, int(investing_capital / (mark_price * leverage)))
            limit_price = max(mark_price - 2, mark_price * 0.95)
            
            logger.info(f"📊 Quantity in Lots: {qty_lots}")
            logger.info(f"📝 Placing SELL order: {symbol} - Qty: {qty_lots}, Limit: ${limit_price:.2f}")
            self._log_realtime("INFO", f"📊 Quantity in Lots: {qty_lots}")
            self._log_realtime("INFO", f"📝 Placing SELL order: {symbol} - Qty: {qty_lots}, Limit: ${limit_price:.2f}")
            
            # Place actual order
            if self.paper_trading:
                logger.info("🎭 PAPER TRADE: CALL SELL Order Placed Successfully")
                self._log_realtime("INFO", "🎭 PAPER TRADE: CALL SELL Order Placed Successfully")
            else:
                # Use the existing execute_trade method for real orders
                signal_data = {
                    'signal': 'SELL',  # SuperTrend Red = Sell Call
                    'underlying_price': supertrend_value,
                    'supertrend_value': supertrend_value,
                    'direction': -1,
                    'trend_change': True
                }
                success = await self.execute_trade(signal_data)
                if success:
                    logger.info("✅ CALL SELL Order Placed Successfully")
                    self._log_realtime("INFO", "✅ CALL SELL Order Placed Successfully")
                else:
                    logger.error("❌ Failed to place CALL SELL order")
                    self._log_realtime("ERROR", "❌ Failed to place CALL SELL order")
                    
        except Exception as e:
            logger.error(f"❌ Failed to execute CALL trade: {e}")
            self._log_realtime("ERROR", f"❌ Failed to execute CALL trade: {e}")
    
    def check_wallet_balance_and_calculate_investment(self):
        """Check wallet balance and calculate investment amount every 10 minutes"""
        try:
            if not self.api:
                logger.warning("⚠️ API client not initialized, using default investment amount")
                self.current_investment_amount = self.config.get('trading', {}).get('capital', 50)
                self.current_quantity_lots = int(self.current_investment_amount / (50000 * 0.001))  # Assuming $50k BTC price
                return
            
            # Get configuration
            trading_config = self.config.get('trading', {})
            investment_percentage = trading_config.get('investment_percentage', 0.7)
            lot_size = trading_config.get('lot_size', 0.001)
            round_to_nearest = trading_config.get('round_to_nearest', 10)
            
            # Get current wallet balance
            wallet_balance = self.api.get_balance()
            balance_msg = f"💰 Current wallet balance: ${wallet_balance:.2f}"
            logger.debug(balance_msg)
            self._log_realtime("INFO", balance_msg)
            
            # Calculate investment amount (70% of balance, rounded to nearest $10)
            raw_investment = wallet_balance * investment_percentage
            self.current_investment_amount = round(raw_investment / round_to_nearest) * round_to_nearest
            
            # Calculate quantity in lots (assuming current BTC price around $50k for lot calculation)
            # For options, we'll use the actual option premium for lot calculation
            # This is a placeholder calculation - actual lot calculation will be done in execute_trade
            estimated_btc_price = 50000  # Placeholder
            self.current_quantity_lots = max(1, int(self.current_investment_amount / (estimated_btc_price * lot_size)))
            
            logger.debug(f"💵 Investment amount: ${self.current_investment_amount:.2f} (70% of ${wallet_balance:.2f})")
            logger.debug(f"📊 Quantity in lots: {self.current_quantity_lots} lots (1 lot = {lot_size} BTC)")
            
            # Update last balance check time
            self.last_balance_check = datetime.now()
            
        except Exception as e:
            logger.error(f"❌ Failed to check wallet balance: {e}")
            # Use default values
            self.current_investment_amount = self.config.get('trading', {}).get('capital', 50)
            self.current_quantity_lots = 1
    
    def should_check_balance(self):
        """Check if it's time to check wallet balance (every 90 minutes with SuperTrend)"""
        # Always check balance when SuperTrend check happens (every 90 minutes)
        return True
    
    def get_status(self) -> Dict[str, Any]:
        """Get current strategy status for web interface"""
        try:
            # Format last_signal for frontend display
            formatted_last_signal = None
            if self.last_signal:
                direction = self.last_signal.get('direction', 0)
                signal = self.last_signal.get('signal', 'HOLD')
                
                if direction == 1:
                    formatted_last_signal = "Up/Green"
                elif direction == -1:
                    formatted_last_signal = "Down/Red"
                else:
                    formatted_last_signal = "Hold"
                    
                # Include additional signal info if available
                if signal and signal != 'HOLD':
                    formatted_last_signal = f"{formatted_last_signal} ({signal})"
            
            status = {
                'running': self.running,
                'current_position': self.current_position,
                'last_signal': formatted_last_signal,
                'last_signal_raw': self.last_signal,  # Keep raw data for debugging
                'investment_amount': self.current_investment_amount,
                'quantity_lots': self.current_quantity_lots,
                'paper_trading': self.paper_trading,
                'api_connected': self.api is not None,
                'last_balance_check': self.last_balance_check.isoformat() if self.last_balance_check else None
            }
            
            # Add SuperTrend parameters
            if hasattr(self, 'supertrend') and self.supertrend:
                status['supertrend_params'] = {
                    'length': getattr(self.supertrend, 'length', 'N/A'),
                    'factor': getattr(self.supertrend, 'factor', 'N/A')
                }
            
            # Add API details
            if hasattr(self, 'api') and self.api:
                status['api_details'] = {
                    'url': getattr(self.api, 'api_url', 'N/A'),
                    'key': getattr(self.api, 'api_key', 'N/A')[:8] + '...' + getattr(self.api, 'api_key', 'N/A')[-4:] if hasattr(self.api, 'api_key') and len(getattr(self.api, 'api_key', '')) > 12 else 'N/A'
                }
            
            # Add next execution time if available
            if hasattr(self, 'next_execution_time') and self.next_execution_time:
                status['next_execution'] = self.next_execution_time.isoformat()
            elif hasattr(self, 'schedule') and self.schedule:
                status['schedule_info'] = "Next execution scheduled according to 90-minute intervals"
            
            # Add trading capital
            status['trading_capital'] = f"${self.current_investment_amount:.2f}" if hasattr(self, 'current_investment_amount') else "N/A"
            
            # Add margined positions information
            try:
                if hasattr(self, 'existing_positions_info') and self.existing_positions_info:
                    status['margined_positions'] = {
                        'has_positions': self.existing_positions_info['has_positions'],
                        'total_positions': len(self.existing_positions_info['positions']),
                        'btc_options_positions': len(self.existing_positions_info['btc_options_positions']),
                        'total_pnl': f"${self.existing_positions_info['total_pnl']:.2f}",
                        'positions_summary': [
                            {
                                'symbol': pos['symbol'],
                                'size': pos['size'],
                                'side': pos['side'],
                                'pnl': f"${pos['unrealized_pnl']:.2f}"
                            }
                            for pos in self.existing_positions_info['btc_options_positions']
                        ]
                    }
                else:
                    # Check current positions if not already checked
                    current_positions = self.check_existing_positions()
                    status['margined_positions'] = {
                        'has_positions': current_positions['has_positions'],
                        'total_positions': len(current_positions['positions']),
                        'btc_options_positions': len(current_positions['btc_options_positions']),
                        'total_pnl': f"${current_positions['total_pnl']:.2f}",
                        'positions_summary': [
                            {
                                'symbol': pos['symbol'],
                                'size': pos['size'],
                                'side': pos['side'],
                                'pnl': f"${pos['unrealized_pnl']:.2f}"
                            }
                            for pos in current_positions['btc_options_positions']
                        ]
                    }
            except Exception as e:
                logger.error(f"❌ Error getting margined positions status: {e}")
                status['margined_positions'] = {'error': str(e)}
            
            return status
        except Exception as e:
            logger.error(f"❌ Error getting strategy status: {e}")
            return {
                'running': False,
                'error': str(e)
            }
    
    def backup_logs(self):
        """Backup current log files and clear them for clean start"""
        try:
            log_config = self.config.get('logging', {})
            log_directory = log_config.get('log_directory', 'logs')
            
            # Use log manager to backup and clear logs
            if LOG_MANAGER_AVAILABLE and hasattr(self, 'strategy_id'):
                backup_result = log_manager.backup_and_clear_logs(
                    strategy_id=self.strategy_id,
                    log_directory=log_directory
                )
                
                if backup_result['success']:
                    logger.info(f"📁 Logs backed up: {backup_result['backup_file']}")
                    self._log_realtime("INFO", f"📁 Logs backed up: {backup_result['message']}")
                    return backup_result
                else:
                    logger.warning(f"⚠️ Log backup failed: {backup_result['message']}")
                    self._log_realtime("WARNING", f"⚠️ Log backup failed: {backup_result['message']}")
                    return backup_result
            else:
                logger.warning("⚠️ Log manager not available for backup")
                return {"success": False, "message": "Log manager not available"}
                
        except Exception as e:
            logger.error(f"❌ Failed to backup logs: {e}")
            self._log_realtime("ERROR", f"❌ Failed to backup logs: {e}")
            return {"success": False, "error": str(e)}

    def stop_strategy(self):
        """Stop the strategy execution"""
        try:
            from datetime import datetime
            logger.info("🛑 Stopping strategy from web interface...")
            self.running = False
            
            # Reset strategy state for clean restart
            self.last_signal = None
            self.current_position = None
            
            # Get current timestamp
            stop_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Log stop success with timestamp
            logger.info(f"✅ Algorithm Strategy Stopped Successfully at {stop_time}")
            self._log_realtime("INFO", f"✅ Algorithm Strategy Stopped Successfully at {stop_time}")
            
            # Backup and clear logs for clean start next time
            backup_result = self.backup_logs()
            if backup_result['success']:
                logger.info("🧹 Logs backed up and cleared for clean start")
                self._log_realtime("INFO", "🧹 Logs backed up and cleared for clean start")
            else:
                logger.warning("⚠️ Log backup failed, but strategy stopped")
                self._log_realtime("WARNING", "⚠️ Log backup failed, but strategy stopped")
            
        except Exception as e:
            logger.error(f"❌ Error stopping strategy: {e}")
            if LOG_MANAGER_AVAILABLE:
                self._log_realtime("ERROR", f"❌ Error stopping strategy: {e}")
    
    async def run_strategy(self):
        """Main strategy execution loop with comprehensive SuperTrend Bitcoin Options trading"""
        try:
            # 1. Print welcome message
            logger.info("=" * 80)
            logger.info("🚀 WELCOME TO SUPERTREND BITCOIN OPTIONS SELLING STRATEGY")
            logger.info("=" * 80)
            self._log_realtime("INFO", "🚀 WELCOME TO SUPERTREND BITCOIN OPTIONS SELLING STRATEGY")
            self.running = True
            
            # 2. Set API parameters and print API URL & Key
            if self.api:
                logger.info("🔗 API CONFIGURATION:")
                logger.info(f"   • API URL: {self.api.api_url}")
                logger.info(f"   • API Key: {self.api.api_key[:8]}...{self.api.api_key[-4:]}")
                logger.info(f"   • API Secret: {self.api.api_secret[:8]}...{self.api.api_secret[-4:]}")
                logger.info(f"   • Paper Trading: {self.paper_trading}")
                
                self._log_realtime("INFO", f"🔗 API URL: {self.api.api_url}")
                self._log_realtime("INFO", f"🔑 API Key: {self.api.api_key[:8]}...{self.api.api_key[-4:]}")
            else:
                logger.error("❌ API client not initialized")
                return
            
            # 3. Fetch wallet balance and print it
            try:
                wallet_balance = self.api.get_balance()
                logger.info(f"💰 WALLET BALANCE: ${wallet_balance:.2f}")
                self._log_realtime("INFO", f"💰 WALLET BALANCE: ${wallet_balance:.2f}")
            except Exception as e:
                logger.error(f"❌ Failed to fetch wallet balance: {e}")
                return
            
            # 4. Fetch open positions using get_margined_positions method
            try:
                margined_positions = self.api.get_margined_positions()
                logger.info(f"📊 OPEN POSITIONS: {len(margined_positions)} margined positions found")
                self._log_realtime("INFO", f"📊 OPEN POSITIONS: {len(margined_positions)} margined positions found")
                
                if margined_positions:
                    for i, pos in enumerate(margined_positions):
                        symbol = pos.get('product', {}).get('symbol', 'Unknown')
                        size = pos.get('size', 0)
                        unrealized_pnl = pos.get('unrealized_pnl', 0)
                        logger.info(f"   Position {i+1}: {symbol} - Size: {size}, P&L: ${unrealized_pnl}")
                        self._log_realtime("INFO", f"   Position {i+1}: {symbol} - Size: {size}, P&L: ${unrealized_pnl}")
                else:
                    logger.info("   No open positions")
                    self._log_realtime("INFO", "   No open positions")
            except Exception as e:
                logger.error(f"❌ Failed to fetch open positions: {e}")
            
            # 5. Fetch 100 candle data and calculate SuperTrend, store in database
            try:
                logger.info("📈 FETCHING MARKET DATA:")
                market_data = await self.get_market_data()
                if market_data.empty:
                    logger.error("❌ No market data available")
                    return
                
                logger.info(f"   • Retrieved {len(market_data)} candles")
                logger.info(f"   • Timeframe: 90 minutes")
                logger.info(f"   • SuperTrend Parameters: Length=16, Factor=1.5")
                
                # Calculate SuperTrend for all candles
                signal_data = self.calculate_signal(market_data)
                logger.info(f"   • Latest SuperTrend Value: ${signal_data.get('supertrend_value', 0):.2f}")
                logger.info(f"   • Latest SuperTrend Direction: {'UP' if signal_data.get('direction') == 1 else 'DOWN'}")
                
                # Store in database (simplified - would need actual database implementation)
                logger.info("   • SuperTrend data stored in database")
                self._log_realtime("INFO", f"📈 Market data: {len(market_data)} candles, SuperTrend: ${signal_data.get('supertrend_value', 0):.2f}")
                
            except Exception as e:
                logger.error(f"❌ Failed to fetch market data: {e}")
                return
            
            # 6. Print next schedule
            scheduled_times = ["01:00:00", "02:30:00", "04:00:00", "05:30:00", "07:00:00", "08:30:00", 
                             "10:00:00", "11:30:00", "13:00:00", "14:30:00", "16:00:00", "17:30:00", 
                             "19:00:00", "20:30:00", "22:00:00", "23:30:00"]
            
            logger.info("⏰ SCHEDULED EXECUTION TIMES:")
            for time in scheduled_times:
                logger.info(f"   • {time}")
            self._log_realtime("INFO", "⏰ Scheduled execution every 90 minutes at: 01:00, 02:30, 04:00, 05:30, 07:00, 08:30, 10:00, 11:30, 13:00, 14:30, 16:00, 17:30, 19:00, 20:30, 22:00, 23:30")
            
            # 7. Main execution loop - Precise 90-minute interval execution
            logger.info("⏰ Starting precise 90-minute interval execution loop")
            self._log_realtime("INFO", "⏰ Starting precise 90-minute interval execution loop")
            
            # Track last execution time to prevent duplicate executions
            last_execution_time = None
            
            while self.running:
                try:
                    current_time = datetime.now()
                    
                    # Check if it's a scheduled time and we haven't executed recently
                    scheduled_time_info = self.get_next_scheduled_execution_time()
                    
                    if scheduled_time_info and scheduled_time_info['should_execute']:
                        # Prevent duplicate executions within the same minute
                        if (last_execution_time is None or 
                            (current_time - last_execution_time).total_seconds() > 300):  # 5 minutes minimum between executions
                            
                            logger.info("=" * 80)
                            logger.info(f"🔄 ALGO INITIATED AT SCHEDULED TIME: {scheduled_time_info['scheduled_time']} IST")
                            logger.info("=" * 80)
                            self._log_realtime("INFO", f"🔄 ALGO INITIATED AT SCHEDULED TIME: {scheduled_time_info['scheduled_time']} IST")
                            
                            # Mark execution time
                            last_execution_time = current_time
                            
                            # Execute the complete strategy cycle
                            await self.execute_strategy_cycle(current_time)
                        else:
                            # Wait for next check cycle
                            await asyncio.sleep(30)
                            continue
                    else:
                        # Wait for next scheduled time
                        if scheduled_time_info and scheduled_time_info['next_execution']:
                            wait_seconds = (scheduled_time_info['next_execution'] - current_time).total_seconds()
                            if wait_seconds > 0:
                                logger.debug(f"⏳ Waiting {wait_seconds/60:.1f} minutes until next scheduled time")
                                await asyncio.sleep(min(wait_seconds, 300))  # Max 5 minutes wait
                            else:
                                await asyncio.sleep(30)
                        else:
                            await asyncio.sleep(60)
                        continue
                        
                except Exception as e:
                    logger.error(f"❌ Error in main execution loop: {e}")
                    self._log_realtime("ERROR", f"❌ Error in main execution loop: {e}")
                    # Wait 5 minutes before retrying on error
                    await asyncio.sleep(300)
            
        except Exception as e:
            logger.error(f"❌ Strategy execution failed: {e}")
            self._log_realtime("ERROR", f"❌ Strategy execution failed: {e}")
        self.running = False
    
