#!/bin/bash

# TradeManthan - Sync Local Changes to EC2
# This script pushes to GitHub and then triggers deployment on EC2

set -e  # Exit on any error

# Configuration (Edit these values)
EC2_HOST="YOUR_EC2_IP_OR_DOMAIN"  # e.g., "13.233.123.45" or "trademanthan.in"
EC2_USER="ubuntu"
EC2_KEY="TradeM.pem"
APP_DIR="/home/ubuntu/trademanthan"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠ $1${NC}"; }
print_error() { echo -e "${RED}✗ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ $1${NC}"; }

echo "=========================================="
echo "TradeManthan Deployment Pipeline"
echo "=========================================="

# Check if EC2_HOST is configured
if [ "$EC2_HOST" == "YOUR_EC2_IP_OR_DOMAIN" ]; then
    print_error "Please configure EC2_HOST in this script first!"
    echo "Edit sync_to_ec2.sh and set EC2_HOST to your EC2 IP or domain"
    exit 1
fi

# Check if SSH key exists
if [ ! -f "$EC2_KEY" ]; then
    print_error "SSH key not found: $EC2_KEY"
    echo "Please ensure TradeM.pem is in the current directory"
    exit 1
fi

# Step 1: Check for uncommitted changes
echo ""
print_info "Step 1: Checking for uncommitted changes..."
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    print_warning "You have uncommitted changes"
    read -p "Do you want to commit them? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git add -A
        read -p "Enter commit message: " commit_msg
        git commit -m "$commit_msg"
        print_success "Changes committed"
    else
        print_error "Please commit or stash your changes first"
        exit 1
    fi
else
    print_success "Working directory clean"
fi

# Step 2: Push to GitHub
echo ""
print_info "Step 2: Pushing to GitHub..."
if git push origin main; then
    print_success "Pushed to GitHub successfully"
else
    print_error "Failed to push to GitHub"
    exit 1
fi

# Step 3: Deploy to EC2
echo ""
print_info "Step 3: Deploying to EC2 server..."
echo "Connecting to $EC2_HOST..."

# SSH into EC2 and run deployment
if ssh -i "$EC2_KEY" -o StrictHostKeyChecking=no "$EC2_USER@$EC2_HOST" "bash -s" << 'ENDSSH'
    set -e
    echo "Connected to EC2 server"
    
    # Navigate to app directory
    cd /home/ubuntu/trademanthan
    
    echo "Pulling latest changes from GitHub..."
    git stash > /dev/null 2>&1 || true
    git pull origin main
    
    echo "Installing/updating backend dependencies..."
    cd backend
    source venv/bin/activate
    pip install -r requirements.txt -q
    
    echo "Restarting backend service..."
    sudo systemctl restart trademanthan-backend
    
    echo "Reloading Nginx..."
    sudo systemctl reload nginx
    
    echo "Checking service status..."
    if systemctl is-active --quiet trademanthan-backend; then
        echo "✓ Backend service is running"
    else
        echo "✗ Backend service failed to start"
        exit 1
    fi
    
    if systemctl is-active --quiet nginx; then
        echo "✓ Nginx is running"
    else
        echo "✗ Nginx is not running"
        exit 1
    fi
    
    echo ""
    echo "Deployment completed successfully!"
ENDSSH
then
    echo ""
    print_success "Deployment to EC2 completed successfully!"
    echo ""
    echo "Your application is now live at:"
    echo "  http://$EC2_HOST"
    echo ""
    echo "Useful commands:"
    echo "  View logs:    ssh -i $EC2_KEY $EC2_USER@$EC2_HOST 'sudo journalctl -u trademanthan-backend -f'"
    echo "  SSH to EC2:   ssh -i $EC2_KEY $EC2_USER@$EC2_HOST"
else
    print_error "Deployment to EC2 failed!"
    echo ""
    echo "To troubleshoot:"
    echo "  1. SSH into server: ssh -i $EC2_KEY $EC2_USER@$EC2_HOST"
    echo "  2. Check logs: sudo journalctl -u trademanthan-backend -f"
    echo "  3. Run deployment manually: cd $APP_DIR && ./deploy_to_ec2.sh"
    exit 1
fi

echo ""
echo "=========================================="
print_success "All Done! 🚀"
echo "=========================================="

