# WhatsApp Alert Setup for TradeManthan

## Overview

TradeManthan now sends critical alerts to both **Email** AND **WhatsApp** for instant notifications.

**Service Used:** CallMeBot (Free WhatsApp API)

---

## 🚀 Quick Setup (5 Minutes)

### **Step 1: Register Your WhatsApp Number with CallMeBot**

1. **Save CallMeBot's number** to your phone contacts:
   ```
   +34 644 44 71 67
   ```
   (Or use the number for your region from https://www.callmebot.com/blog/free-api-whatsapp-messages/)

2. **Send this EXACT message** to CallMeBot on WhatsApp:
   ```
   I allow callmebot to send me messages
   ```

3. **You'll receive your API Key** in reply:
   ```
   API Activated for your phone number
   Your APIKEY is 123456
   ```
   **Save this API key** - you'll need it!

---

### **Step 2: Add Configuration to EC2 Server**

Connect to your EC2 server and add WhatsApp config to `.env`:

```bash
# Connect to EC2
ssh -i TradeM.pem ubuntu@13.234.119.21

# Edit .env file
cd /home/ubuntu/trademanthan/backend
nano .env
```

**Add these lines at the end:**

```bash
# WhatsApp Alert Configuration (CallMeBot)
WHATSAPP_PHONE=+919876543210        # Your phone number (with country code)
WHATSAPP_APIKEY=123456               # API key from CallMeBot
```

**Replace:**
- `+919876543210` → Your actual phone number (format: +91XXXXXXXXXX for India)
- `123456` → Your actual API key from CallMeBot

**Save and exit:** `Ctrl+X`, then `Y`, then `Enter`

---

### **Step 3: Restart Backend Service**

```bash
sudo systemctl restart trademanthan-backend

# Verify service is running
sudo systemctl status trademanthan-backend
```

---

### **Step 4: Test WhatsApp Alerts (Optional)**

```bash
cd /home/ubuntu/trademanthan/backend
source venv/bin/activate

python3 << 'PYTHON'
import os
os.environ['WHATSAPP_PHONE'] = '+919876543210'  # Your number
os.environ['WHATSAPP_APIKEY'] = '123456'         # Your API key

import sys
sys.path.insert(0, '/home/ubuntu/trademanthan/backend')

from services.health_monitor import health_monitor

# Send test WhatsApp message
message = "🚨 *TradeManthan Test Alert*\n\nThis is a test WhatsApp notification.\n\nIf you received this, WhatsApp alerts are working! ✅"
result = health_monitor.send_whatsapp_message(message)

if result:
    print("✅ WhatsApp test message sent! Check your phone.")
else:
    print("❌ Failed to send WhatsApp message. Check your configuration.")
PYTHON
```

**Check your WhatsApp** - you should receive a message from CallMeBot!

---

## 📱 What You'll Get on WhatsApp

### **Critical Alerts (within 45 minutes of issues):**

**Example 1: Token Expired**
```
🚨 *TradeManthan Alert*

*❌ UPSTOX TOKEN EXPIRED*

Upstox API token has been failing for 3 consecutive checks.

ACTION REQUIRED:
1. Go to: https://trademanthan.in/scan.html
2. Click 'Login with Upstox'
3. Complete OAuth authorization

_Time: 11:45 IST_
```

**Example 2: No Webhooks**
```
🚨 *TradeManthan Alert*

*⚠️ WEBHOOK FAILURES DETECTED*

No webhooks processed for 3 consecutive checks.

Possible issues:
- Chartink not sending webhooks
- Backend processing errors
- Database connection issues

_Time: 10:30 IST_
```

**Example 3: Daily Report (4:00 PM)**
```
📊 *TradeManthan Daily Report*

Date: November 04, 2025

WEBHOOKS: 15 alerts
• Bullish: 8
• Bearish: 7

TRADES: 12 entered, 3 no-entry

SYSTEM: ✅ All OK

_Time: 16:00 IST_
```

---

## 🎯 Alert Types

| Alert | Trigger | Sent Via |
|-------|---------|----------|
| **🚨 Token Expired** | 3 consecutive API failures (45 min) | Email + WhatsApp |
| **🚨 No Webhooks** | 3 checks with 0 data (45 min) | Email + WhatsApp |
| **🚨 Database Down** | 3 consecutive DB failures (45 min) | Email + WhatsApp |
| **📊 Daily Report** | Every day at 4:00 PM IST | Email + WhatsApp |
| **⚠️ No Data Alert** | 0 webhooks on weekday (4 PM) | Email + WhatsApp |

---

## 🔧 Troubleshooting

### **Not Receiving WhatsApp Messages?**

1. **Check CallMeBot Registration:**
   - Did you send "I allow callmebot to send me messages" EXACTLY as written?
   - Did you receive the API key reply?
   - Try re-sending the activation message

2. **Check Phone Number Format:**
   - Must include country code: `+919876543210` (India)
   - No spaces or dashes: ❌ `+91 98765 43210` ✅ `+919876543210`
   - Plus sign required: `+91`

3. **Check API Key:**
   - Copy EXACTLY from CallMeBot's reply message
   - No extra spaces or characters

4. **Check .env File:**
   ```bash
   cd /home/ubuntu/trademanthan/backend
   tail -5 .env
   ```
   Should show:
   ```
   WHATSAPP_PHONE=+919876543210
   WHATSAPP_APIKEY=123456
   ```

5. **Check Service Logs:**
   ```bash
   sudo journalctl -u trademanthan-backend -f | grep -i whatsapp
   ```
   Should show: `✅ WhatsApp alert sent to +919876543210`

6. **Test Manually:**
   Visit in browser:
   ```
   https://api.callmebot.com/whatsapp.php?phone=919876543210&text=Test&apikey=123456
   ```
   (Replace with your number and API key)

7. **CallMeBot Limits:**
   - Free tier: ~50 messages per day
   - If exceeded, you won't receive messages until next day
   - Our system sends: ~4-8 messages per day max (health checks + alerts)

---

## 🔐 Security Notes

1. **API Key is NOT a Password:**
   - CallMeBot API key is specific to your phone number
   - It can only send messages to YOUR number
   - Even if exposed, it can't be misused to spam others

2. **.env File Security:**
   - File is already secured with `chmod 600`
   - Only accessible by ubuntu user on EC2
   - Not tracked in Git (in .gitignore)

3. **Phone Number Privacy:**
   - Stored only in .env on EC2 server
   - Not visible in logs
   - Not sent to any third party (except CallMeBot for delivery)

---

## 📊 Current Configuration

**Email Alerts:**
```
To: bipulsin@gmail.com
From: bipulsin@gmail.com
SMTP: smtp.gmail.com:587
Status: ✅ Configured & Tested
```

**WhatsApp Alerts:**
```
Phone: (To be configured)
Service: CallMeBot
Status: ⚠️ Awaiting setup
```

**Once configured:**
```
Phone: +919876543210
Service: CallMeBot
Status: ✅ Active
```

---

## ✅ Benefits of WhatsApp Alerts

| Feature | Email Only | Email + WhatsApp |
|---------|------------|------------------|
| **Delivery Time** | 1-5 minutes | Instant (< 10 seconds) |
| **Notification** | Silent (desktop) | Push notification (mobile) |
| **Read Receipt** | No | Yes (via WhatsApp) |
| **Accessibility** | Requires email app | WhatsApp (always on phone) |
| **Spam Risk** | High (may go to spam) | Low (personal WhatsApp) |
| **During Travel** | May not check | Always notified |

---

## 🎉 Why This is Awesome

### **Before (Email Only):**
- ⏰ Email might arrive late
- 📥 May land in spam folder
- 📧 Might not check email during day
- ⚠️ Could miss critical alerts

### **After (Email + WhatsApp):**
- ⚡ **Instant notification** on phone
- 📱 **Push notification** wakes you up
- ✅ **Always available** (WhatsApp always on phone)
- 🚨 **Can't miss critical alerts**
- 📊 **Daily summary** right in WhatsApp

---

## 🚀 Alternative WhatsApp Services (Advanced)

If CallMeBot doesn't work for you:

### **Option 1: Twilio (Paid, Most Reliable)**
- Cost: ~$0.005 per message
- Requires: Twilio account + WhatsApp Business API approval
- Setup: https://www.twilio.com/whatsapp

### **Option 2: Green API (Freemium)**
- Free tier: 100 messages/month
- Requires: Account signup
- Setup: https://green-api.com/

### **Option 3: Ultramsg (Freemium)**
- Free tier: 10 messages/day
- Requires: Account signup + instance creation
- Setup: https://ultramsg.com/

**For now, CallMeBot is recommended** - it's free, instant setup, and perfect for personal alerts.

---

## 📞 Support

If you need help setting up WhatsApp alerts:

1. **Check CallMeBot website:**
   https://www.callmebot.com/blog/free-api-whatsapp-messages/

2. **Test API directly:**
   https://api.callmebot.com/whatsapp.php?phone=YOUR_PHONE&text=Test&apikey=YOUR_KEY

3. **Check system logs:**
   ```bash
   sudo journalctl -u trademanthan-backend | grep -i whatsapp
   ```

---

## ✅ Setup Checklist

- [ ] Saved CallMeBot number (+34 644 44 71 67) to contacts
- [ ] Sent activation message to CallMeBot
- [ ] Received API key from CallMeBot
- [ ] Added WHATSAPP_PHONE to .env
- [ ] Added WHATSAPP_APIKEY to .env
- [ ] Restarted trademanthan-backend service
- [ ] Sent test WhatsApp message
- [ ] Received test message on phone

**Once all checked, you're ready to receive instant WhatsApp alerts!** 🎉

---

**Last Updated:** November 3, 2025  
**Status:** Ready to Configure

