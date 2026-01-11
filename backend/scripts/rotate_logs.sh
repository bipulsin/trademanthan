#!/bin/bash
#
# Log Rotation Script for TradeManthan
# Rotates scan_st1_algo.log and trademanthan.log daily at midnight
# - Moves current logs to date-based backup files
# - Clears main log files for next day
# - Deletes backup files older than 7 days
#

set -euo pipefail

# Configuration
LOG_DIR="/home/ubuntu/trademanthan/logs"
DATE_SUFFIX=$(date +%Y-%m-%d)
RETENTION_DAYS=7

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "=========================================="
echo "TradeManthan Log Rotation - $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# Function to rotate a log file
rotate_log() {
    local log_file="$1"
    local backup_file="${log_file}.${DATE_SUFFIX}"
    
    if [ ! -f "$log_file" ]; then
        echo -e "${YELLOW}⚠️  Log file not found: $log_file${NC}"
        return 0
    fi
    
    local file_size=$(du -h "$log_file" | cut -f1)
    
    # Check if file is empty or very small (less than 1KB)
    if [ ! -s "$log_file" ] || [ "$(stat -f%z "$log_file" 2>/dev/null || stat -c%s "$log_file" 2>/dev/null)" -lt 1024 ]; then
        echo -e "${YELLOW}⚠️  Skipping empty/small log file: $log_file (size: $file_size)${NC}"
        return 0
    fi
    
    # Copy current log to backup (preserves file for backup)
    echo -e "📋 Rotating ${GREEN}$(basename $log_file)${NC} (size: $file_size)..."
    cp "$log_file" "$backup_file" 2>/dev/null || {
        echo -e "${RED}❌ Failed to create backup: $backup_file${NC}"
        return 1
    }
    
    # Truncate the original file (safer than deletion - keeps file handle valid)
    > "$log_file" || {
        echo -e "${RED}❌ Failed to truncate log file: $log_file${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ Rotated: $(basename $log_file) → $(basename $backup_file)${NC}"
    return 0
}

# Function to delete old backup files
cleanup_old_backups() {
    local log_name="$1"
    local deleted_count=0
    
    echo -e "🧹 Cleaning up old backups for ${GREEN}$log_name${NC} (older than $RETENTION_DAYS days)..."
    
    # Find and delete backup files older than retention period
    find "$LOG_DIR" -name "${log_name}.*" -type f -mtime +$RETENTION_DAYS -delete 2>/dev/null | while read -r file; do
        if [ -n "$file" ]; then
            echo -e "  🗑️  Deleted: $(basename $file)"
            ((deleted_count++)) || true
        fi
    done
    
    # Count remaining backups
    local remaining=$(find "$LOG_DIR" -name "${log_name}.*" -type f 2>/dev/null | wc -l)
    echo -e "${GREEN}✅ Cleanup complete. Remaining backups: $remaining${NC}"
}

# Main rotation process
main() {
    cd "$LOG_DIR" || {
        echo -e "${RED}❌ Cannot access log directory: $LOG_DIR${NC}"
        exit 1
    }
    
    echo ""
    echo "🔄 Starting log rotation..."
    echo ""
    
    # Rotate scan_st1_algo.log
    rotate_log "$LOG_DIR/scan_st1_algo.log"
    
    # Rotate trademanthan.log
    rotate_log "$LOG_DIR/trademanthan.log"
    
    echo ""
    echo "🧹 Cleaning up old backup files..."
    echo ""
    
    # Cleanup old backups
    cleanup_old_backups "scan_st1_algo.log"
    cleanup_old_backups "trademanthan.log"
    
    echo ""
    echo "=========================================="
    echo -e "${GREEN}✅ Log rotation completed successfully${NC}"
    echo "=========================================="
    echo ""
    echo "Current log files:"
    ls -lh "$LOG_DIR"/*.log 2>/dev/null | awk '{print "  " $9 " (" $5 ")"}'
    echo ""
    echo "Backup files (last 7 days):"
    find "$LOG_DIR" -name "*.log.*" -type f -mtime -$RETENTION_DAYS 2>/dev/null | sort | while read -r file; do
        echo "  $(basename $file) ($(du -h "$file" | cut -f1))"
    done
    echo ""
}

# Run main function
main

exit 0
