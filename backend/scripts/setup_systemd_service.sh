#!/bin/bash
# Setup Systemd Service for TradeManthan Backend
# This ensures the backend runs continuously with auto-restart

set -e

SERVICE_NAME="trademanthan-backend"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
PROJECT_DIR="/home/ubuntu/trademanthan"
BACKEND_DIR="${PROJECT_DIR}/backend"

echo "🔧 Setting up systemd service for TradeManthan Backend..."

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then 
    echo "❌ This script must be run with sudo"
    echo "Usage: sudo bash $0"
    exit 1
fi

# Stop any existing backend process (non-systemd)
echo "Stopping any existing backend processes..."
pkill -f "uvicorn.*main:app" 2>/dev/null || true
pkill -f "uvicorn backend.main:app" 2>/dev/null || true
sleep 2

# Create systemd service file
echo "Creating systemd service file..."
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=TradeManthan Backend API
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=${BACKEND_DIR}
Environment="PATH=${BACKEND_DIR}/venv/bin"
Environment="PYTHONUNBUFFERED=1"
ExecStart=${BACKEND_DIR}/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Security settings
NoNewPrivileges=true
PrivateTmp=true

# Resource limits
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

echo "✅ Service file created at $SERVICE_FILE"

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload

# Enable service to start on boot
echo "Enabling service to start on boot..."
systemctl enable "$SERVICE_NAME"

# Start the service
echo "Starting backend service..."
systemctl start "$SERVICE_NAME"

# Wait a moment for startup
sleep 3

# Check service status
echo ""
echo "📊 Service Status:"
systemctl status "$SERVICE_NAME" --no-pager -l || true

# Verify it's running
if systemctl is-active --quiet "$SERVICE_NAME"; then
    echo ""
    echo "✅ Backend service is running!"
    echo ""
    echo "Useful commands:"
    echo "  Status:   sudo systemctl status $SERVICE_NAME"
    echo "  Logs:     sudo journalctl -u $SERVICE_NAME -f"
    echo "  Restart:  sudo systemctl restart $SERVICE_NAME"
    echo "  Stop:     sudo systemctl stop $SERVICE_NAME"
    echo "  Start:    sudo systemctl start $SERVICE_NAME"
else
    echo ""
    echo "❌ Service failed to start. Check logs with:"
    echo "  sudo journalctl -u $SERVICE_NAME -n 50"
    exit 1
fi

