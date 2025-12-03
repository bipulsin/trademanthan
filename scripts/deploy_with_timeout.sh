#!/bin/bash
# Deployment Script with Timeouts
# Ensures all operations complete within time limits

set -e

EC2_KEY="/Users/bipulsahay/TradeManthan/TradeM.pem"
EC2_HOST="13.234.119.21"
EC2_USER="ubuntu"
PROJECT_DIR="/home/ubuntu/trademanthan"
TIMEOUT=15

echo "🚀 Starting deployment with timeout protection..."
echo ""

# Function to run SSH command with timeout
run_ssh_timeout() {
    local timeout=$1
    shift
    local cmd="$@"
    local output_file=$(mktemp)
    local error_file=$(mktemp)
    
    ssh -i "$EC2_KEY" \
        -o ConnectTimeout=5 \
        -o ServerAliveInterval=2 \
        -o ServerAliveCountMax=3 \
        -o StrictHostKeyChecking=no \
        "$EC2_USER@$EC2_HOST" "$cmd" > "$output_file" 2> "$error_file" &
    
    local ssh_pid=$!
    local elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        if ! kill -0 $ssh_pid 2>/dev/null; then
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
    
    kill $ssh_pid 2>/dev/null || true
    kill -9 $ssh_pid 2>/dev/null || true
    rm -f "$output_file" "$error_file"
    echo "ERROR: Command timed out after ${timeout}s" >&2
    return 124
}

# Step 1: Push to GitHub
echo "📤 Step 1: Pushing to GitHub..."
if git push origin main; then
    echo "✅ Pushed to GitHub successfully"
else
    echo "❌ Failed to push to GitHub"
    exit 1
fi

echo ""

# Step 2: Pull on server
echo "📥 Step 2: Pulling latest code on server..."
if run_ssh_timeout $TIMEOUT "cd $PROJECT_DIR && git pull origin main"; then
    echo "✅ Code pulled successfully"
else
    echo "❌ Failed to pull code"
    exit 1
fi

echo ""

# Step 3: Restart backend using timeout-enabled script
echo "🔄 Step 3: Restarting backend..."
if run_ssh_timeout $TIMEOUT "bash $PROJECT_DIR/backend/scripts/backend_control.sh restart"; then
    echo "✅ Backend restarted successfully"
else
    echo "❌ Failed to restart backend"
    exit 1
fi

echo ""
echo "✅ Deployment completed successfully!"

