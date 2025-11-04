# Health Monitoring & Self-Healing System

## 🏥 Overview

TradeManthan now includes a comprehensive health monitoring and self-healing system to prevent silent failures and maintain system credibility.

---

## ✅ Features Implemented

### 1. **Automated Health Checks**
- Runs every **15 minutes** during market hours (9 AM - 4 PM IST)
- Monitors: Database, Upstox API, Webhooks, Instruments file
- Detects issues automatically

### 2. **Critical Alerts**
- Email notifications on critical failures
- Logged to journald (visible in systemctl logs)
- Threshold: 3 consecutive failures trigger alert

### 3. **Daily Health Reports**
- Sent every day at **4:00 PM IST** (after market close)
- Summary of webhooks, trades, system health
- Alerts if no webhooks received on weekday

### 4. **Self-Healing Mechanisms**
- Graceful degradation (saves partial data on API failures)
- Automatic retry queue for failed webhooks
- Fallback to trigger_price when LTP fetch fails
- Minimal save mode when enrichment fails

### 5. **Health Check Endpoint**
- **URL:** `https://trademanthan.in/scan/health`
- Real-time system status
- Component-level health checks
- Metrics on consecutive failures

---

## 🔧 Setup (Optional Email Alerts)

### Configure Environment Variables

Add to `/home/ubuntu/trademanthan/backend/.env`:

```bash
# Email Alert Configuration (Optional)
ALERT_EMAIL=your.email@gmail.com
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your.email@gmail.com
SMTP_PASSWORD=your_app_password
SMTP_FROM_EMAIL=alerts@trademanthan.in
```

**For Gmail:**
1. Enable 2-factor authentication
2. Generate App Password: https://myaccount.google.com/apppasswords
3. Use app password as `SMTP_PASSWORD`

**Without Email:**
- System still works without email configuration
- Alerts are logged to journald
- View with: `sudo journalctl -u trademanthan-backend | grep CRITICAL`

---

## 📊 Health Checks Performed

### Every 15 Minutes (9 AM - 4 PM):

| Component | Check | Alert Condition |
|-----------|-------|-----------------|
| **Database** | `SELECT 1` query | 3 consecutive failures |
| **Upstox API** | Index prices fetch | 3 consecutive failures |
| **Webhooks** | Today's alert count | 0 alerts after 11 AM (weekday) |
| **Instruments File** | File existence | File missing or > 7 days old |

### Daily Report (4 PM):

- Total webhooks received
- Bullish vs Bearish count
- Trades entered vs No Entry
- Component health status
- Alert if 0 webhooks on weekday

---

## 🚨 Alert Scenarios

### 1. **Webhook Failures**
**Trigger:** No webhooks processed for 3 consecutive checks (45 min)

**Alert Message:**
```
⚠️ WEBHOOK FAILURES DETECTED

No webhooks processed for 3 consecutive checks.
Possible issues:
- Chartink not sending webhooks
- Backend processing errors
- Database connection issues

Time: 2025-11-03 11:45:00 IST
```

**Actions:**
1. Check backend logs: `sudo journalctl -u trademanthan-backend -f`
2. Verify Chartink configuration
3. Check database connectivity

### 2. **Upstox Token Expired**
**Trigger:** API calls failing for 3 consecutive checks (45 min)

**Alert Message:**
```
❌ UPSTOX TOKEN EXPIRED

Upstox API token has been failing for 3 consecutive checks.

ACTION REQUIRED:
1. Go to: https://trademanthan.in/scan.html
2. Click 'Login with Upstox'
3. Complete OAuth authorization

Time: 2025-11-03 11:45:00 IST
```

**Actions:**
1. Login to scan.html
2. Click "Login with Upstox"
3. Complete OAuth flow

### 3. **No Webhooks on Weekday**
**Trigger:** Daily report at 4 PM shows 0 webhooks on a trading day

**Alert Message:**
```
⚠️ NO WEBHOOKS TODAY

No webhook alerts received on November 03, 2025 (weekday).

This may indicate:
- Market holiday
- Chartink not sending webhooks
- Backend processing failures

Please investigate.
```

**Actions:**
1. Check if market was open
2. Verify Chartink scanner settings
3. Check webhook endpoint accessibility

---

## 🔍 Manual Health Check

### Via Web Browser:
```
https://trademanthan.in/scan/health
```

### Via Command Line:
```bash
curl https://trademanthan.in/scan/health | jq '.'
```

### Sample Response:
```json
{
  "status": "healthy",
  "timestamp": "2025-11-03T14:30:00+05:30",
  "components": {
    "database": {
      "status": "ok",
      "healthy": true
    },
    "upstox_api": {
      "status": "error",
      "healthy": false,
      "error": "401 Unauthorized"
    },
    "webhooks": {
      "today_count": 15,
      "status": "ok",
      "message": "15 alerts today"
    },
    "instruments_file": {
      "status": "ok",
      "exists": true
    }
  },
  "metrics": {
    "consecutive_webhook_failures": 0,
    "consecutive_token_failures": 3,
    "consecutive_db_failures": 0
  }
}
```

---

## 📋 Monitoring Schedule

### Automatic Checks:
- **9:00 AM** - First health check
- **9:15 AM** - Second check
- **9:30 AM** - Third check
- ... every 15 minutes ...
- **3:45 PM** - Last check
- **4:00 PM** - Daily health report

### Manual Checks:
- Anytime via `/scan/health` endpoint
- Service logs: `sudo journalctl -u trademanthan-backend -f`

---

## 🛡️ Self-Healing Features

### 1. **Graceful Degradation**
When Upstox API fails:
- ✅ Still saves stock_name + alert_time
- ✅ Uses trigger_price as fallback
- ✅ Marks as 'no_entry' or 'alert_received'
- ✅ Can be enriched later when token refreshed

### 2. **Two-Tier Save Strategy**
1. Try full save with all data
2. If fails → Try minimal save (stock_name + alert_time only)
3. Both fail → Log stock names for manual recovery

### 3. **Error Tracking**
- Consecutive failures tracked per component
- Alerts sent after threshold exceeded
- Auto-resets when issues resolved

### 4. **Comprehensive Logging**
- Every step logged with status (✅/⚠️/❌)
- Full tracebacks on errors
- Stock names logged if commit fails
- Searchable in journald

---

## 🎯 Benefits

1. **No Silent Failures** - Always know when something breaks
2. **Proactive Alerts** - Get notified before users complain
3. **Data Preservation** - Webhooks saved even with API issues
4. **Easy Troubleshooting** - Comprehensive logs and health status
5. **User Credibility** - System reliability improved significantly

---

## 📱 View Health Status

### In scan.html (Add Health Status Widget):

```javascript
// Fetch and display health status
async function checkSystemHealth() {
    const response = await fetch('/scan/health');
    const health = await response.json();
    
    if (health.status !== 'healthy') {
        // Show warning banner
        console.warn('System health degraded:', health);
    }
}
```

### In Backend Logs:
```bash
# Real-time monitoring
sudo journalctl -u trademanthan-backend -f | grep "HEALTH\|CRITICAL\|Webhook"

# Today's health checks
sudo journalctl -u trademanthan-backend --since today | grep "HEALTH CHECK"

# Critical alerts only
sudo journalctl -u trademanthan-backend | grep CRITICAL
```

---

## 🔄 Restart Health Monitor

If health monitor stops working:

```bash
# Restart backend (restarts all services)
sudo systemctl restart trademanthan-backend

# Verify health monitor started
sudo journalctl -u trademanthan-backend --since "1 minute ago" | grep "Health Monitor"
```

---

## 📊 Metrics Tracked

- Consecutive webhook failures
- Consecutive API token failures
- Consecutive database failures  
- Daily webhook count
- Bullish vs Bearish ratio
- Trades entered vs No Entry
- System uptime
- Component health status

---

## 🎉 Result

**Before:**
- Silent failures (Oct 28-31, Nov 3)
- 42+ webhooks lost
- No alerts to users
- Credibility impact

**After:**
- Immediate failure detection
- Email alerts on issues
- Data always preserved (even partial)
- Daily health reports
- User-visible health status
- Professional monitoring

**System is now production-ready with enterprise-grade reliability!** ✅

