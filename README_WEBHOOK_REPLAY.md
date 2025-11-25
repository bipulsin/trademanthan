# Webhook Replay and Monitoring Guide

## Overview
This guide explains how to use the webhook replay script and monitoring system to ensure webhooks are captured and processed correctly.

## 1. Backend Monitoring

The backend is now monitored automatically via a cron job that runs every 5 minutes.

### Monitor Script Location
`backend/scripts/monitor_backend.sh`

### What It Does
- Checks if backend process is running
- Verifies backend health endpoint responds
- Automatically restarts backend if it's down
- Logs all actions to `/tmp/backend_monitor.log`

### Manual Check
```bash
bash /home/ubuntu/trademanthan/backend/scripts/monitor_backend.sh
```

### View Monitor Logs
```bash
tail -f /tmp/backend_monitor.log
```

## 2. Webhook Replay Script

Use this script to manually replay webhooks that were missed or to test webhook processing.

### Location
`backend/scripts/replay_webhook.py`

### Usage Examples

#### Replay from Command Line Arguments
```bash
cd /home/ubuntu/trademanthan
python3 backend/scripts/replay_webhook.py \
  --type bullish \
  --stocks "RELIANCE,TATAMOTORS,INFY" \
  --prices "2500.0,800.0,1500.0" \
  --time "10:15 am"
```

#### Replay from JSON File
Create a file `webhook_payload.json`:
```json
{
  "stocks": "RELIANCE,TATAMOTORS,INFY",
  "trigger_prices": "2500.0,800.0,1500.0",
  "triggered_at": "10:15 am",
  "scan_name": "Bullish Breakout",
  "scan_url": "bullish-breakout",
  "alert_name": "Alert for Bullish Breakout"
}
```

Then replay:
```bash
python3 backend/scripts/replay_webhook.py --file webhook_payload.json
```

#### Replay Bearish Webhook
```bash
python3 backend/scripts/replay_webhook.py \
  --type bearish \
  --stocks "STOCK1,STOCK2" \
  --prices "100.0,200.0" \
  --time "10:15 am"
```

### Options
- `--type`: Alert type (`bullish` or `bearish`)
- `--stocks`: Comma-separated stock names
- `--prices`: Comma-separated trigger prices
- `--time`: Alert time (e.g., "10:15 am")
- `--file`: Path to JSON file with webhook payload
- `--url`: Backend URL (default: http://localhost:8000)
- `--scan-name`: Custom scan name

## 3. Enhanced Webhook Logging

All webhook payloads are now logged with full details:

### Log Locations
- Console output: `/tmp/uvicorn.log`
- Application logs: Check backend logs

### What's Logged
- Full webhook payload (JSON)
- Stock count
- Timestamp
- Processing status
- Any errors or warnings

### View Recent Webhooks
```bash
grep "Received.*webhook" /tmp/uvicorn.log | tail -20
grep "Full webhook payload" /tmp/uvicorn.log | tail -20
```

## 4. Troubleshooting

### Backend Not Running
1. Check monitor logs: `tail -f /tmp/backend_monitor.log`
2. Manually restart: `bash /home/ubuntu/trademanthan/backend/scripts/monitor_backend.sh`
3. Check process: `ps aux | grep uvicorn`

### Webhook Not Received
1. Verify Chartink webhook URL is correct:
   - Bullish: `https://trademanthan.in/scan/chartink-webhook-bullish`
   - Bearish: `https://trademanthan.in/scan/chartink-webhook-bearish`
2. Check webhook logs: `grep webhook /tmp/uvicorn.log`
3. Test endpoint manually:
   ```bash
   curl -X POST http://localhost:8000/scan/chartink-webhook-bullish \
     -H "Content-Type: application/json" \
     -d '{"stocks":"TEST","trigger_prices":"100","triggered_at":"10:15 am"}'
   ```

### Webhook Received But No Stocks Saved
1. Check logs for "No stocks to save" warning
2. Verify webhook payload format matches expected structure
3. Check if stocks were filtered out (index names, etc.)
4. Review processing logs for errors

## 5. Recovering Missed Alerts

If an alert was missed (e.g., backend was down):

1. Get the webhook payload from Chartink logs or recreate it
2. Save to a JSON file
3. Replay using the replay script:
   ```bash
   python3 backend/scripts/replay_webhook.py --file webhook_payload.json
   ```

The replayed webhook will be processed exactly as if it came from Chartink.

