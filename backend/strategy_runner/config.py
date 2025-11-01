"""
Configuration file for the Strategy Runner.

This module contains all configuration settings for the strategy runner,
including database connections, API settings, and trading parameters.
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database Configuration
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'trademanthan'),
    'user': os.getenv('DB_USER', 'trademanthan'),
    'password': os.getenv('DB_PASSWORD', 'trademanthan123'),
    'url': os.getenv('DATABASE_URL', 'postgresql://trademanthan:trademanthan123@localhost/trademanthan')
}

# API Configuration
API_CONFIG = {
    'base_url': os.getenv('API_BASE_URL', 'https://trademanthan.in'),
    'timeout': int(os.getenv('API_TIMEOUT', 30)),
    'max_retries': int(os.getenv('API_MAX_RETRIES', 3)),
    'retry_delay': float(os.getenv('API_RETRY_DELAY', 1.0))
}

# Trading Configuration
TRADING_CONFIG = {
    'default_timeframe': os.getenv('DEFAULT_TIMEFRAME', '5m'),
    'max_position_size': float(os.getenv('MAX_POSITION_SIZE', 1000.0)),
    'default_stop_loss': float(os.getenv('DEFAULT_STOP_LOSS', 0.02)),  # 2%
    'default_take_profit': float(os.getenv('DEFAULT_TAKE_PROFIT', 0.04)),  # 4%
    'max_slippage': float(os.getenv('MAX_SLIPPAGE', 0.001)),  # 0.1%
    'min_volume': float(os.getenv('MIN_VOLUME', 100.0)),
    'max_spread': float(os.getenv('MAX_SPREAD', 0.005))  # 0.5%
}

# Risk Management Configuration
RISK_CONFIG = {
    'max_daily_loss': float(os.getenv('MAX_DAILY_LOSS', 0.05)),  # 5%
    'max_portfolio_risk': float(os.getenv('MAX_PORTFOLIO_RISK', 0.02)),  # 2%
    'position_sizing_method': os.getenv('POSITION_SIZING_METHOD', 'fixed'),  # fixed, kelly, risk_based
    'correlation_threshold': float(os.getenv('CORRELATION_THRESHOLD', 0.7)),
    'max_concurrent_positions': int(os.getenv('MAX_CONCURRENT_POSITIONS', 5))
}

# Strategy Configuration
STRATEGY_CONFIG = {
    'default_indicators': [
        'rsi', 'ema', 'supertrend', 'bb_squeeze', 'triple_ema'
    ],
    'indicator_parameters': {
        'rsi': {
            'length': 14,
            'overbought': 70,
            'oversold': 30
        },
        'ema': {
            'length': 20
        },
        'supertrend': {
            'atr_period': 10,
            'multiplier': 3.0
        },
        'bb_squeeze': {
            'bb_length': 20,
            'bb_std': 2.0,
            'kc_length': 20,
            'kc_std': 1.5,
            'squeeze_threshold': 0.5
        },
        'triple_ema': {
            'short_ema_period': 9,
            'medium_ema_period': 21,
            'long_ema_period': 50
        }
    },
    'signal_thresholds': {
        'min_confidence': float(os.getenv('MIN_SIGNAL_CONFIDENCE', 0.6)),
        'min_signal_strength': float(os.getenv('MIN_SIGNAL_STRENGTH', 0.5)),
        'max_false_signals': int(os.getenv('MAX_FALSE_SIGNALS', 3))
    }
}

# Execution Configuration
EXECUTION_CONFIG = {
    'execution_mode': os.getenv('EXECUTION_MODE', 'paper'),  # paper, live, backtest
    'order_type': os.getenv('ORDER_TYPE', 'LIMIT'),  # MARKET, LIMIT, STOP, STOP_LIMIT
    'fill_strategy': os.getenv('FILL_STRATEGY', 'aggressive'),  # aggressive, conservative
    'max_order_retries': int(os.getenv('MAX_ORDER_RETRIES', 3)),
    'order_timeout': int(os.getenv('ORDER_TIMEOUT', 60)),  # seconds
    'partial_fill_threshold': float(os.getenv('PARTIAL_FILL_THRESHOLD', 0.8))
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
    'file_rotation': {
        'max_bytes': int(os.getenv('LOG_MAX_BYTES', 5_000_000)),  # 5MB
        'backup_count': int(os.getenv('LOG_BACKUP_COUNT', 5))
    },
    'log_to_file': os.getenv('LOG_TO_FILE', 'true').lower() == 'true',
    'log_to_console': os.getenv('LOG_TO_CONSOLE', 'true').lower() == 'true'
}

# Performance Configuration
PERFORMANCE_CONFIG = {
    'max_execution_time': float(os.getenv('MAX_EXECUTION_TIME', 30.0)),  # seconds
    'memory_limit': int(os.getenv('MEMORY_LIMIT', 512)),  # MB
    'cpu_limit': float(os.getenv('CPU_LIMIT', 0.8)),  # 80%
    'enable_profiling': os.getenv('ENABLE_PROFILING', 'false').lower() == 'true',
    'performance_metrics': {
        'track_latency': True,
        'track_memory': True,
        'track_cpu': True,
        'track_throughput': True
    }
}

# Monitoring Configuration
MONITORING_CONFIG = {
    'health_check_interval': int(os.getenv('HEALTH_CHECK_INTERVAL', 60)),  # seconds
    'metrics_collection_interval': int(os.getenv('METRICS_COLLECTION_INTERVAL', 300)),  # seconds
    'alert_thresholds': {
        'max_memory_usage': float(os.getenv('MAX_MEMORY_USAGE', 0.9)),  # 90%
        'max_cpu_usage': float(os.getenv('MAX_CPU_USAGE', 0.9)),  # 90%
        'max_latency': float(os.getenv('MAX_LATENCY', 5.0)),  # seconds
        'max_error_rate': float(os.getenv('MAX_ERROR_RATE', 0.05))  # 5%
    },
    'notifications': {
        'email': os.getenv('ALERT_EMAIL'),
        'slack_webhook': os.getenv('SLACK_WEBHOOK'),
        'telegram_bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID')
    }
}

# Backtesting Configuration
BACKTEST_CONFIG = {
    'start_date': os.getenv('BACKTEST_START_DATE', '2024-01-01'),
    'end_date': os.getenv('BACKTEST_END_DATE', '2024-12-31'),
    'initial_capital': float(os.getenv('BACKTEST_INITIAL_CAPITAL', 100000.0)),
    'commission_rate': float(os.getenv('BACKTEST_COMMISSION_RATE', 0.001)),  # 0.1%
    'slippage_model': os.getenv('BACKTEST_SLIPPAGE_MODEL', 'fixed'),  # fixed, percentage, random
    'data_source': os.getenv('BACKTEST_DATA_SOURCE', 'local'),  # local, api, database
    'resample_frequency': os.getenv('BACKTEST_RESAMPLE_FREQUENCY', '1min')
}

# Notification Configuration
NOTIFICATION_CONFIG = {
    'enable_notifications': os.getenv('ENABLE_NOTIFICATIONS', 'true').lower() == 'true',
    'notification_channels': os.getenv('NOTIFICATION_CHANNELS', 'email,slack').split(','),
    'notification_events': {
        'trade_executed': True,
        'signal_generated': True,
        'risk_limit_breached': True,
        'system_error': True,
        'performance_alert': True
    },
    'notification_frequency': os.getenv('NOTIFICATION_FREQUENCY', 'immediate')  # immediate, hourly, daily
}

# Security Configuration
SECURITY_CONFIG = {
    'api_key_encryption': os.getenv('API_KEY_ENCRYPTION', 'true').lower() == 'true',
    'encryption_key': os.getenv('ENCRYPTION_KEY'),
    'rate_limiting': {
        'enabled': os.getenv('RATE_LIMITING_ENABLED', 'true').lower() == 'true',
        'max_requests_per_minute': int(os.getenv('MAX_REQUESTS_PER_MINUTE', 100)),
        'max_requests_per_hour': int(os.getenv('MAX_REQUESTS_PER_HOUR', 1000))
    },
    'ip_whitelist': os.getenv('IP_WHITELIST', '').split(',') if os.getenv('IP_WHITELIST') else []
}

# Development Configuration
DEV_CONFIG = {
    'debug_mode': os.getenv('DEBUG_MODE', 'false').lower() == 'true',
    'enable_hot_reload': os.getenv('ENABLE_HOT_RELOAD', 'false').lower() == 'true',
    'mock_data': os.getenv('MOCK_DATA', 'false').lower() == 'true',
    'test_mode': os.getenv('TEST_MODE', 'false').lower() == 'true',
    'development_features': [
        'strategy_validation',
        'performance_profiling',
        'detailed_logging',
        'mock_trading'
    ]
}

# Get all configuration as a single dictionary
def get_config() -> Dict[str, Any]:
    """Get all configuration settings as a single dictionary."""
    return {
        'database': DATABASE_CONFIG,
        'api': API_CONFIG,
        'trading': TRADING_CONFIG,
        'risk': RISK_CONFIG,
        'strategy': STRATEGY_CONFIG,
        'execution': EXECUTION_CONFIG,
        'logging': LOGGING_CONFIG,
        'performance': PERFORMANCE_CONFIG,
        'monitoring': MONITORING_CONFIG,
        'backtest': BACKTEST_CONFIG,
        'notifications': NOTIFICATION_CONFIG,
        'security': SECURITY_CONFIG,
        'development': DEV_CONFIG
    }

# Validate configuration
def validate_config() -> bool:
    """Validate that all required configuration values are present."""
    required_vars = [
        'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
        'API_BASE_URL'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        return False
    
    return True

# Print configuration summary
def print_config_summary():
    """Print a summary of the current configuration."""
    config = get_config()
    
    print("🔧 Strategy Runner Configuration Summary")
    print("=" * 50)
    
    print(f"📊 Execution Mode: {config['execution']['execution_mode']}")
    print(f"💾 Database: {config['database']['database']} on {config['database']['host']}")
    print(f"🌐 API Base URL: {config['api']['base_url']}")
    print(f"📈 Default Timeframe: {config['trading']['default_timeframe']}")
    print(f"⚠️  Max Daily Loss: {config['risk']['max_daily_loss']*100}%")
    print(f"🔍 Log Level: {config['logging']['level']}")
    print(f"📝 Log to File: {config['logging']['log_to_file']}")
    print(f"🚨 Notifications: {config['notifications']['enable_notifications']}")
    print(f"🔒 Security: Rate Limiting {'Enabled' if config['security']['rate_limiting']['enabled'] else 'Disabled'}")
    print(f"🐛 Debug Mode: {config['development']['debug_mode']}")
    
    print("\n📋 Available Indicators:")
    for indicator in config['strategy']['default_indicators']:
        print(f"   • {indicator}")
    
    print("\n⚙️  Configuration loaded successfully!")

if __name__ == "__main__":
    # Print configuration summary when run directly
    print_config_summary()
