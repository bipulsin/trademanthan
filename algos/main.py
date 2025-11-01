#!/usr/bin/env python3
"""
SuperTrend Bitcoin Options Strategy - Main Entry Point
Command-line interface for running the strategy
"""

import asyncio
import argparse
import logging
import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from strategy.supertrend_options_strategy import SuperTrendOptionsStrategy

def setup_logging(level: str = "INFO"):
    """Setup logging configuration"""
    log_level = getattr(logging, level.upper())
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/strategy.log')
        ]
    )

def print_banner():
    """Print application banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                SuperTrend Bitcoin Options Strategy           ║
    ║                                                              ║
    ║  🚀 Automated Bitcoin Options Trading using SuperTrend      ║
    ║  📊 Delta Exchange India Integration                         ║
    ║  ⚠️  HIGH RISK - Options selling involves unlimited risk    ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def print_risk_warning():
    """Print risk warning"""
    warning = """
    ⚠️  RISK WARNING ⚠️
    
    Options selling involves UNLIMITED RISK for naked positions.
    You can lose more than your initial investment.
    
    This strategy is for EDUCATIONAL PURPOSES only.
    Only trade with capital you can afford to lose.
    
    By continuing, you acknowledge and accept these risks.
    """
    print(warning)

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="SuperTrend Bitcoin Options Strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Paper trading mode
  python main.py --paper-trading
  
  # Live trading with API credentials
  python main.py --api-key YOUR_KEY --api-secret YOUR_SECRET --api-url https://api.delta.exchange
  
  # Live trading with risk confirmation
  python main.py --api-key YOUR_KEY --api-secret YOUR_SECRET --api-url https://api.delta.exchange --confirm-risk
        """
    )
    
    parser.add_argument('--api-key', help='Delta Exchange API key')
    parser.add_argument('--api-secret', help='Delta Exchange API secret')
    parser.add_argument('--api-url', help='Delta Exchange API URL', default='https://api.delta.exchange')
    parser.add_argument('--config', help='Configuration file path', default='config/config.yaml')
    parser.add_argument('--paper-trading', action='store_true', help='Enable paper trading mode')
    parser.add_argument('--confirm-risk', action='store_true', help='Confirm understanding of risks')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                       default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    # Print banner
    print_banner()
    
    # Check for risk confirmation for live trading
    if not args.paper_trading and not args.confirm_risk:
        print_risk_warning()
        response = input("\nDo you understand and accept the risks? (yes/no): ")
        if response.lower() != 'yes':
            print("❌ Risk confirmation required for live trading. Exiting.")
            sys.exit(1)
    
    try:
        # Create logs directory
        Path('logs').mkdir(exist_ok=True)
        
        # Initialize strategy
        logger.info("🚀 Initializing SuperTrend Options Strategy...")
        
        strategy = SuperTrendOptionsStrategy(
            config_path=args.config,
            api_key=args.api_key,
            api_secret=args.api_secret,
            api_url=args.api_url,
            paper_trading=args.paper_trading
        )
        
        if args.paper_trading:
            logger.info("🎭 Running in PAPER TRADING mode")
        else:
            logger.info("💰 Running in LIVE TRADING mode")
        
        # Run strategy
        logger.info("🚀 Starting strategy execution...")
        await strategy.run_strategy()
        
    except KeyboardInterrupt:
        logger.info("🛑 Strategy stopped by user")
    except Exception as e:
        logger.error(f"❌ Strategy error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 or higher is required")
        sys.exit(1)
    
    # Run main function
    asyncio.run(main())
