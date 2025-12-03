#!/bin/bash
# Remote Backend Control Script
# Wraps SSH commands with timeouts to prevent hanging
# Usage: remote_backend_control.sh [start|stop|restart|status|check]

set -e

ACTION="${1:-status}"
EC2_KEY="/Users/bipulsahay/TradeManthan/TradeM.pem"
EC2_HOST="13.234.119.21"
EC2_USER="ubuntu"
SSH_TIMEOUT=10
SCRIPT_PATH="/home/ubuntu/trademanthan/backend/scripts/backend_control.sh"

# Function to run SSH command with timeout using background process
run_ssh_with_timeout() {
    local timeout=$1
    shift
    local ssh_cmd="$@"
    local output_file=$(mktemp)
    local error_file=$(mktemp)
    
    # Run SSH in background
    ssh -i "$EC2_KEY" \
        -o ConnectTimeout=5 \
        -o ServerAliveInterval=2 \
        -o ServerAliveCountMax=3 \
        -o StrictHostKeyChecking=no \
        "$EC2_USER@$EC2_HOST" "$ssh_cmd" > "$output_file" 2> "$error_file" &
    
    local ssh_pid=$!
    
    # Monitor with timeout
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if ! kill -0 $ssh_pid 2>/dev/null; then
            # Process finished
            wait $ssh_pid
            local exit_code=$?
            cat "$output_file"
            cat "$error_file" >&2
            rm -f "$output_file" "$error_file"
            return $exit_code
        fi
        sleep 0.5
        elapsed=$((elapsed + 1))
    done
    
    # Timeout reached
    kill $ssh_pid 2>/dev/null || true
    kill -9 $ssh_pid 2>/dev/null || true
    rm -f "$output_file" "$error_file"
    echo "ERROR: Command timed out after ${timeout}s" >&2
    return 124
}

# Run the action with timeout
echo "Executing: $ACTION (timeout: ${SSH_TIMEOUT}s)"
run_ssh_with_timeout $SSH_TIMEOUT "bash $SCRIPT_PATH $ACTION"

exit $?

