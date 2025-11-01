#!/bin/bash

# TradeManthan EC2 Deployment Script
# This script pulls the latest code from GitHub and restarts the services

set -e  # Exit on any error

echo "=========================================="
echo "TradeManthan EC2 Deployment"
echo "=========================================="

# Configuration
REPO_URL="https://github.com/bipulsin/trademanthan.git"
APP_DIR="/home/ubuntu/trademanthan"
BACKEND_DIR="$APP_DIR/backend"
FRONTEND_DIR="$APP_DIR/frontend/public"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check if we're on EC2
if [ ! -f /home/ubuntu/.ssh/authorized_keys ]; then
    print_warning "This script is designed for EC2 Ubuntu instances"
fi

# Pull latest code from GitHub
echo ""
echo "Step 1: Pulling latest code from GitHub..."
if [ ! -d "$APP_DIR" ]; then
    print_warning "App directory doesn't exist. Cloning repository..."
    cd /home/ubuntu
    git clone $REPO_URL trademanthan
    print_success "Repository cloned"
else
    cd $APP_DIR
    print_warning "Stashing any local changes..."
    git stash
    print_success "Pulling latest changes..."
    git pull origin main
fi

# Install/Update backend dependencies
echo ""
echo "Step 2: Installing backend dependencies..."
cd $BACKEND_DIR

if [ ! -d "venv" ]; then
    print_warning "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
print_success "Virtual environment activated"

pip install --upgrade pip -q
pip install -r requirements.txt -q
print_success "Backend dependencies installed"

# Check if .env file exists
if [ ! -f "$BACKEND_DIR/.env" ]; then
    print_error ".env file not found!"
    print_warning "Please create .env file with required credentials:"
    echo "  cp env.example .env"
    echo "  nano .env  # Edit with your credentials"
    exit 1
else
    print_success ".env file found"
fi

# Restart backend service (if using systemd)
echo ""
echo "Step 3: Restarting backend service..."
if systemctl is-active --quiet trademanthan-backend 2>/dev/null; then
    sudo systemctl restart trademanthan-backend
    print_success "Backend service restarted"
else
    print_warning "Backend service not found. To create it:"
    echo ""
    echo "Create /etc/systemd/system/trademanthan-backend.service:"
    echo "---"
    cat << 'EOF'
[Unit]
Description=TradeManthan Backend API
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/trademanthan/backend
Environment="PATH=/home/ubuntu/trademanthan/backend/venv/bin"
ExecStart=/home/ubuntu/trademanthan/backend/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    echo "---"
    echo "Then run:"
    echo "  sudo systemctl daemon-reload"
    echo "  sudo systemctl enable trademanthan-backend"
    echo "  sudo systemctl start trademanthan-backend"
fi

# Setup Nginx for frontend (if not already configured)
echo ""
echo "Step 4: Checking Nginx configuration..."
if command -v nginx &> /dev/null; then
    if [ ! -f /etc/nginx/sites-available/trademanthan ]; then
        print_warning "Nginx config not found. Creating..."
        cat << 'EOF' | sudo tee /etc/nginx/sites-available/trademanthan > /dev/null
server {
    listen 80;
    server_name trademanthan.in www.trademanthan.in;

    # Frontend
    location / {
        root /home/ubuntu/trademanthan/frontend/public;
        index index.html;
        try_files $uri $uri/ /index.html;
    }

    # Backend API
    location /api/ {
        proxy_pass http://localhost:8000/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Static files
    location /static/ {
        alias /home/ubuntu/trademanthan/frontend/public/;
    }
}
EOF
        sudo ln -sf /etc/nginx/sites-available/trademanthan /etc/nginx/sites-enabled/
        sudo nginx -t && sudo systemctl reload nginx
        print_success "Nginx configured and reloaded"
    else
        sudo nginx -t && sudo systemctl reload nginx
        print_success "Nginx configuration valid and reloaded"
    fi
else
    print_warning "Nginx not installed. Install it with:"
    echo "  sudo apt update && sudo apt install nginx -y"
fi

# Display service status
echo ""
echo "=========================================="
echo "Deployment Summary"
echo "=========================================="

if systemctl is-active --quiet trademanthan-backend 2>/dev/null; then
    print_success "Backend service: RUNNING"
    echo "  View logs: sudo journalctl -u trademanthan-backend -f"
else
    print_warning "Backend service: NOT RUNNING"
fi

if systemctl is-active --quiet nginx 2>/dev/null; then
    print_success "Nginx: RUNNING"
else
    print_warning "Nginx: NOT RUNNING"
fi

# Check if ports are open
if netstat -tuln | grep -q ":8000 "; then
    print_success "Backend API listening on port 8000"
else
    print_warning "Backend API not listening on port 8000"
fi

if netstat -tuln | grep -q ":80 "; then
    print_success "Nginx listening on port 80"
else
    print_warning "Nginx not listening on port 80"
fi

echo ""
echo "=========================================="
echo "Useful Commands:"
echo "=========================================="
echo "Backend logs:    sudo journalctl -u trademanthan-backend -f"
echo "Backend status:  sudo systemctl status trademanthan-backend"
echo "Nginx logs:      sudo tail -f /var/log/nginx/error.log"
echo "Nginx reload:    sudo systemctl reload nginx"
echo ""
print_success "Deployment complete!"

