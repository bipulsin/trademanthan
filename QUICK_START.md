# TradeManthan - Quick Start Guide

## 🚀 GitHub Repository

Your code is now live at: **https://github.com/bipulsin/trademanthan**

## 📋 What Was Done

1. ✅ Created `.gitignore` to exclude sensitive files (venv, __pycache__, .env, *.pem, etc.)
2. ✅ Removed hardcoded Google OAuth credentials (now uses environment variables)
3. ✅ Created clean git history without sensitive data
4. ✅ Pushed complete codebase to GitHub
5. ✅ Created EC2 deployment automation scripts

## 🔧 EC2 Deployment - Quick Commands

### First-Time Setup on EC2

```bash
# 1. SSH into your EC2 server
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>

# 2. Clone the repository
cd /home/ubuntu
git clone https://github.com/bipulsin/trademanthan.git
cd trademanthan

# 3. Run the deployment script
chmod +x deploy_to_ec2.sh
./deploy_to_ec2.sh
```

### Regular Updates (After Pushing to GitHub)

```bash
# Option 1: Automated (recommended)
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>
cd /home/ubuntu/trademanthan
./deploy_to_ec2.sh

# Option 2: Manual
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>
cd /home/ubuntu/trademanthan
git pull origin main
sudo systemctl restart trademanthan-backend
sudo systemctl reload nginx
```

## 🔐 Important: Environment Variables

Before deploying to EC2, you MUST set these environment variables in `/home/ubuntu/trademanthan/backend/.env`:

```env
DATABASE_URL=postgresql://trademanthan:trademanthan123@localhost/trademanthan
SECRET_KEY=<GENERATE_A_SECURE_RANDOM_KEY>
GOOGLE_CLIENT_ID=<YOUR_GOOGLE_CLIENT_ID>
GOOGLE_CLIENT_SECRET=<YOUR_GOOGLE_CLIENT_SECRET>
REDIS_URL=redis://localhost:6379
DOMAIN=trademanthan.in
ENVIRONMENT=production
DEBUG=false
ACCESS_TOKEN_EXPIRE_MINUTES=4320
```

**Generate a secure SECRET_KEY:**
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

## 📂 Repository Structure

```
trademanthan/
├── backend/              # FastAPI backend
│   ├── routers/         # API endpoints
│   ├── models/          # Database models
│   ├── services/        # Business logic
│   ├── strategy_runner/ # Trading strategy engine
│   └── requirements.txt # Python dependencies
├── frontend/            # Web frontend
│   └── public/          # Static HTML/JS/CSS
├── algos/              # Trading algorithms
│   ├── api/            # Broker APIs
│   ├── indicators/     # Technical indicators
│   └── strategy/       # Trading strategies
├── deploy_to_ec2.sh    # Automated deployment script
└── ec2_sync_instructions.md  # Detailed EC2 setup guide
```

## 🔄 Development Workflow

### 1. Make Local Changes

Edit your code locally in the TradeManthan workspace.

### 2. Commit and Push to GitHub

```bash
cd /Users/bipulsahay/TradeManthan
git add .
git commit -m "Your commit message"
git push origin main
```

### 3. Deploy to EC2

```bash
# SSH and run deployment script
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>
cd /home/ubuntu/trademanthan
./deploy_to_ec2.sh
```

## 🛠️ Useful Commands

### Local Development

```bash
# Backend
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Access API: http://localhost:8000
# Access Docs: http://localhost:8000/docs
```

### EC2 Server Management

```bash
# Check backend service status
sudo systemctl status trademanthan-backend

# View backend logs
sudo journalctl -u trademanthan-backend -f

# Restart backend
sudo systemctl restart trademanthan-backend

# Check Nginx status
sudo systemctl status nginx

# View Nginx logs
sudo tail -f /var/log/nginx/error.log

# Reload Nginx (after config changes)
sudo systemctl reload nginx
```

## 🔍 Troubleshooting

### Backend not starting?
```bash
# Check logs
sudo journalctl -u trademanthan-backend -f

# Common issues:
# 1. Missing .env file → Copy env.example to .env and fill in values
# 2. Database connection → Check DATABASE_URL in .env
# 3. Port already in use → Kill process: sudo fuser -k 8000/tcp
```

### Can't connect to website?
```bash
# Check if Nginx is running
sudo systemctl status nginx

# Check if port 80 is open
sudo netstat -tuln | grep :80

# Check firewall
sudo ufw status

# Allow HTTP/HTTPS if needed
sudo ufw allow 80
sudo ufw allow 443
```

### Changes not reflecting?
```bash
# Make sure you pulled latest code
cd /home/ubuntu/trademanthan
git status
git pull origin main

# Restart services
sudo systemctl restart trademanthan-backend
sudo systemctl reload nginx

# Clear browser cache or use Ctrl+F5
```

## 📚 Additional Resources

- **Detailed EC2 Setup**: See `ec2_sync_instructions.md`
- **API Documentation**: http://trademanthan.in/api/docs (after deployment)
- **GitHub Repository**: https://github.com/bipulsin/trademanthan

## 🔒 Security Notes

1. **Never commit** `.env` files or `*.pem` files to GitHub
2. **Always use** environment variables for secrets
3. **Setup SSL** for production: `sudo certbot --nginx -d trademanthan.in`
4. **Keep software updated**: `sudo apt update && sudo apt upgrade`
5. **Backup database** regularly: `pg_dump -U trademanthan trademanthan > backup.sql`

## 💡 Next Steps

1. ✅ Code pushed to GitHub
2. ⬜ Setup EC2 server (follow ec2_sync_instructions.md)
3. ⬜ Configure environment variables
4. ⬜ Deploy using deploy_to_ec2.sh
5. ⬜ Setup SSL certificate
6. ⬜ Test the application
7. ⬜ Setup monitoring and backups

---

**Need Help?** Refer to `ec2_sync_instructions.md` for detailed setup instructions.

