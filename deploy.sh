#!/bin/bash
# Quick deployment script for local use
# Uses the backend API endpoint for non-blocking deployment

set -e

API_URL="https://trademanthan.in/scan/deploy-backend"

echo "🚀 Initiating backend deployment..."
echo ""

# Trigger deployment via API (non-blocking)
RESPONSE=$(curl -s -X POST "$API_URL" -H "Content-Type: application/json")

if echo "$RESPONSE" | grep -q '"success":true'; then
    echo "✅ Deployment initiated successfully"
    echo ""
    echo "Deployment is running in the background."
    echo "Check status with: curl -s $API_URL/../deployment-status | python3 -m json.tool"
    echo ""
    echo "Or check logs on server: ssh -i TradeM.pem ubuntu@13.234.119.21 'tail -f /tmp/deploy_backend.log'"
else
    echo "❌ Failed to initiate deployment"
    echo "Response: $RESPONSE"
    exit 1
fi

