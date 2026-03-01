#!/bin/bash
# Setup SSL for tradentical.com + tradewithcto.com on EC2
# Run this script ON THE EC2 SERVER: bash setup_ssl_tradentical.sh
# Prerequisite: tradentical.com, www.tradentical.com, tradewithcto.com and www.tradewithcto.com must point to this server's IP (A/CNAME records)

set -e

echo "=========================================="
echo "SSL Setup for tradentical.com + tradewithcto.com"
echo "=========================================="

# Check if running on EC2 (ubuntu user)
if [ "$(whoami)" != "ubuntu" ]; then
    echo "Warning: This script is designed to run as ubuntu on EC2"
fi

# Install certbot if not installed
if ! command -v certbot &> /dev/null; then
    echo "Installing Certbot..."
    sudo apt-get update -qq
    sudo apt-get install -y certbot python3-certbot-nginx
    echo "Certbot installed."
fi

# Copy nginx config - scripts are in /home/ubuntu/trademanthan/scripts/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_SRC="$SCRIPT_DIR/nginx-tradentical.conf"
if [ ! -f "$CONF_SRC" ]; then
    CONF_SRC="/home/ubuntu/trademanthan/scripts/nginx-tradentical.conf"
fi
if [ ! -f "$CONF_SRC" ]; then
    echo "Error: nginx-tradentical.conf not found at $CONF_SRC"
    echo "Ensure you have pulled latest from GitHub: cd /home/ubuntu/trademanthan && git pull origin main"
    exit 1
fi

echo "Copying Nginx config..."
sudo cp "$CONF_SRC" /etc/nginx/sites-available/tradentical
sudo ln -sf /etc/nginx/sites-available/tradentical /etc/nginx/sites-enabled/

echo "Testing Nginx config..."
sudo nginx -t

echo "Reloading Nginx (HTTP only for now)..."
sudo systemctl reload nginx

echo ""
echo "Obtaining SSL certificate from Let's Encrypt..."
# Non-interactive: use --register-unsafely-without-email (or set CERTBOT_EMAIL=your@email.com)
if [ -n "$CERTBOT_EMAIL" ]; then
    sudo certbot --nginx -d tradentical.com -d www.tradentical.com -d tradewithcto.com -d www.tradewithcto.com --non-interactive --agree-tos -m "$CERTBOT_EMAIL"
else
    sudo certbot --nginx -d tradentical.com -d www.tradentical.com -d tradewithcto.com -d www.tradewithcto.com --non-interactive --agree-tos --register-unsafely-without-email
fi

echo ""
echo "Reloading Nginx with SSL..."
sudo systemctl reload nginx

echo ""
echo "=========================================="
echo "SSL setup complete!"
echo "https://tradentical.com, https://www.tradentical.com, https://tradewithcto.com and https://www.tradewithcto.com should now work."
echo "=========================================="
