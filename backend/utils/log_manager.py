"""
Real-time Log Manager for Strategy Execution
Handles log collection, storage, and retrieval for web interface
"""

import logging
from collections import deque
from typing import Dict, List, Any, Optional
import threading
from datetime import datetime
import os
import shutil
from pathlib import Path

class StrategyLogHandler(logging.Handler):
    def __init__(self, log_manager_instance, strategy_id):
        super().__init__()
        self.log_manager = log_manager_instance
        self.strategy_id = strategy_id

    def emit(self, record):
        log_entry = self.format(record)
        self.log_manager.add_log(self.strategy_id, record.levelname, log_entry)

class LogManager:
    def __init__(self, max_logs_per_strategy: int = 2000):
        self.logs: Dict[str, deque] = {}
        self.max_logs_per_strategy = max_logs_per_strategy
        self.lock = threading.Lock()
        self.strategy_loggers: Dict[str, logging.Logger] = {}
        self.latest_log_id: Dict[str, str] = {}  # Tracks latest log ID per strategy

    def create_strategy_logger(self, strategy_id: str, name: str) -> logging.Logger:
        with self.lock:
            if strategy_id not in self.strategy_loggers:
                # Create a new logger for this strategy
                strategy_logger = logging.getLogger(f"strategy.{strategy_id}")
                strategy_logger.propagate = False  # Prevent logs from going to root logger
                strategy_logger.setLevel(logging.INFO)

                # Add custom handler to capture logs
                handler = StrategyLogHandler(self, strategy_id)
                formatter = logging.Formatter('%(message)s')  # We'll format the message later
                handler.setFormatter(formatter)
                strategy_logger.addHandler(handler)
                self.strategy_loggers[strategy_id] = strategy_logger
                self.logs[strategy_id] = deque(maxlen=self.max_logs_per_strategy)
                self.latest_log_id[strategy_id] = "0_0"
            return self.strategy_loggers[strategy_id]

    def add_log(self, strategy_id: str, level: str, message: str):
        with self.lock:
            if strategy_id not in self.logs:
                self.logs[strategy_id] = deque(maxlen=self.max_logs_per_strategy)
                self.latest_log_id[strategy_id] = "0_0"

            timestamp = datetime.now().isoformat()
            # Generate a unique ID for each log entry
            log_id = f"{int(datetime.now().timestamp() * 1000)}_{len(self.logs[strategy_id])}"
            entry = {
                "id": log_id,
                "timestamp": timestamp,
                "level": level,
                "message": message,
                "strategy_id": strategy_id
            }
            self.logs[strategy_id].append(entry)
            self.latest_log_id[strategy_id] = log_id

    def get_logs(self, strategy_id: str, since_id: str = "0_0", limit: int = 100) -> List[Dict[str, Any]]:
        with self.lock:
            if strategy_id not in self.logs:
                return []

            all_logs = list(self.logs[strategy_id])
            
            # Find the index of the log entry with since_id
            start_index = 0
            if since_id != "0_0":
                for i, log_entry in enumerate(all_logs):
                    if log_entry["id"] == since_id:
                        start_index = i + 1
                        break
            
            # Return logs from start_index up to the limit
            return all_logs[start_index:start_index + limit]

    def get_latest_log_id(self, strategy_id: str = None) -> str:
        with self.lock:
            if strategy_id and strategy_id in self.latest_log_id:
                return self.latest_log_id[strategy_id]
            # If no strategy_id, return the latest ID from any strategy (or a default)
            if self.latest_log_id:
                return max(self.latest_log_id.values())  # This might not be ideal for multi-strategy
            return "0_0"

    def clear_logs(self, strategy_id: str = None):
        with self.lock:
            if strategy_id and strategy_id in self.logs:
                self.logs[strategy_id].clear()
                self.latest_log_id[strategy_id] = "0_0"
            elif not strategy_id:
                self.logs.clear()
                self.latest_log_id.clear()

    def backup_and_clear_logs(self, strategy_id: str, log_directory: str = "logs"):
        """
        Backup current logs to a backup file and clear the main log file
        This is called when strategy is stopped from web interface
        
        Process:
        1. Creates a backup directory if it doesn't exist
        2. Finds the current log file (e.g., supertrend_strategy_20241201.log)
        3. Copies it to backup with timestamp (e.g., supertrend_strategy_12345_20241201_143022.log)
        4. Clears the main log file (truncates to 0 bytes)
        5. Clears in-memory logs for clean start
        
        Returns:
            dict: Result with success status, backup file path, and message
        """
        try:
            with self.lock:
                # Create backup directory if it doesn't exist
                backup_dir = Path(log_directory) / "backups"
                backup_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate backup filename with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_filename = f"supertrend_strategy_{strategy_id}_{timestamp}.log"
                backup_path = backup_dir / backup_filename
                
                # Find the current log file
                log_dir = Path(log_directory)
                current_log_pattern = f"supertrend_strategy_{datetime.now().strftime('%Y%m%d')}.log"
                current_log_files = list(log_dir.glob(current_log_pattern))
                
                if current_log_files:
                    current_log_file = current_log_files[0]
                    
                    # Copy current log file to backup
                    shutil.copy2(current_log_file, backup_path)
                    
                    # Clear the current log file (truncate to 0 bytes)
                    with open(current_log_file, 'w') as f:
                        f.write("")
                    
                    # Also clear the in-memory logs
                    if strategy_id in self.logs:
                        self.logs[strategy_id].clear()
                        self.latest_log_id[strategy_id] = "0_0"
                    
                    return {
                        "success": True,
                        "backup_file": str(backup_path),
                        "message": f"Logs backed up to {backup_filename} and cleared"
                    }
                else:
                    return {
                        "success": False,
                        "message": "No current log file found to backup"
                    }
                    
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to backup logs: {e}"
            }

log_manager = LogManager()