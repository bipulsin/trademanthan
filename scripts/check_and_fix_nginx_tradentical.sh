#!/bin/bash
# Run this ON the EC2 server to check and fix nginx (canonical: www.tradewithcto.com)
# Usage: ssh -i TradeM.pem ubuntu@3.6.199.247 "bash -s" < scripts/check_and_fix_nginx_tradentical.sh
# Or: scp this file to EC2 and run: bash check_and_fix_nginx_tradentical.sh

set -e
cd /home/ubuntu/trademanthan

echo "=== Nginx tradewithcto.com Configuration Check ==="
echo ""

# Check current config
if [ -f /etc/nginx/sites-enabled/tradentical ] || [ -f /etc/nginx/sites-available/tradentical ]; then
    echo "✓ tradentical config exists"
    grep -q "cargpt" /etc/nginx/sites-available/tradentical 2>/dev/null && echo "✓ /cargpt/ location present" || echo "✗ /cargpt/ location MISSING"
else
    echo "✗ tradentical config NOT FOUND"
fi

echo ""
echo "Enabled sites:"
ls -la /etc/nginx/sites-enabled/ 2>/dev/null || true

echo ""
echo "=== Applying tradentical config from repo ==="
if [ -f scripts/nginx-tradentical.conf ]; then
    sudo cp scripts/nginx-tradentical.conf /etc/nginx/sites-available/tradentical
    sudo ln -sf /etc/nginx/sites-available/tradentical /etc/nginx/sites-enabled/
    echo "✓ Config copied and enabled"
    sudo nginx -t && sudo systemctl reload nginx && echo "✓ Nginx reloaded" || echo "✗ Nginx reload failed"
else
    echo "✗ scripts/nginx-tradentical.conf not found"
    exit 1
fi

echo ""
echo "=== Done ==="
