# EC2 Server Sync Instructions

This guide explains how to sync your EC2 server with the GitHub repository.

## Prerequisites

1. EC2 instance running Ubuntu
2. SSH access to your EC2 instance
3. Domain pointed to EC2 IP address (if using trademanthan.in)

## One-Time Setup on EC2

### 1. Connect to your EC2 server

```bash
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>
```

### 2. Install required software

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python, pip, and git
sudo apt install -y python3 python3-pip python3-venv git nginx

# Install Node.js (optional, for future frontend builds)
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs
```

### 3. Clone the repository

```bash
cd /home/ubuntu
git clone https://github.com/bipulsin/trademanthan.git
cd trademanthan
```

### 4. Setup Backend

```bash
cd /home/ubuntu/trademanthan/backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Setup environment variables
cp env.example .env
nano .env  # Edit with your actual credentials
```

**Important**: Edit `.env` file with your actual credentials:
- `GOOGLE_CLIENT_ID` - Your Google OAuth Client ID
- `GOOGLE_CLIENT_SECRET` - Your Google OAuth Client Secret
- `SECRET_KEY` - Generate a secure random key
- `DATABASE_URL` - Use PostgreSQL for production or SQLite for testing

### 5. Setup Database

```bash
# If using PostgreSQL (recommended for production)
sudo apt install -y postgresql postgresql-contrib
sudo -u postgres psql -c "CREATE DATABASE trademanthan;"
sudo -u postgres psql -c "CREATE USER trademanthan WITH PASSWORD 'trademanthan123';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE trademanthan TO trademanthan;"

# Update .env with PostgreSQL connection:
# DATABASE_URL=postgresql://trademanthan:trademanthan123@localhost/trademanthan
```

### 6. Create Backend Systemd Service

```bash
sudo nano /etc/systemd/system/trademanthan-backend.service
```

Add the following content:

```ini
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
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable trademanthan-backend
sudo systemctl start trademanthan-backend
sudo systemctl status trademanthan-backend
```

### 7. Setup Nginx

```bash
sudo nano /etc/nginx/sites-available/trademanthan
```

Add the following configuration:

```nginx
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
```

Enable the site:

```bash
sudo ln -s /etc/nginx/sites-available/trademanthan /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

### 8. Setup SSL (Optional but Recommended)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d trademanthan.in -d www.trademanthan.in
```

## Regular Sync Process

Whenever you push changes to GitHub, run the deployment script on EC2:

### Option 1: Using the deployment script

```bash
# On your local machine, copy the script to EC2
scp -i TradeM.pem deploy_to_ec2.sh ubuntu@<YOUR_EC2_IP>:/home/ubuntu/trademanthan/

# On EC2, run the script
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>
cd /home/ubuntu/trademanthan
chmod +x deploy_to_ec2.sh
./deploy_to_ec2.sh
```

### Option 2: Manual sync

```bash
# SSH into EC2
ssh -i TradeM.pem ubuntu@<YOUR_EC2_IP>

# Pull latest changes
cd /home/ubuntu/trademanthan
git pull origin main

# Update backend dependencies (if requirements.txt changed)
cd backend
source venv/bin/activate
pip install -r requirements.txt

# Restart backend service
sudo systemctl restart trademanthan-backend

# Reload Nginx (if frontend changed)
sudo systemctl reload nginx
```

## Automated Sync with GitHub Actions (Optional)

You can set up GitHub Actions to automatically deploy on push. Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy to EC2

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to EC2
        uses: appleboy/ssh-action@master
        with:
          host: ${{ secrets.EC2_HOST }}
          username: ubuntu
          key: ${{ secrets.EC2_SSH_KEY }}
          script: |
            cd /home/ubuntu/trademanthan
            git pull origin main
            cd backend
            source venv/bin/activate
            pip install -r requirements.txt
            sudo systemctl restart trademanthan-backend
            sudo systemctl reload nginx
```

Add secrets to your GitHub repository:
- `EC2_HOST`: Your EC2 public IP or domain
- `EC2_SSH_KEY`: Contents of your TradeM.pem file

## Monitoring and Troubleshooting

### Check Backend Logs
```bash
sudo journalctl -u trademanthan-backend -f
```

### Check Nginx Logs
```bash
sudo tail -f /var/log/nginx/error.log
sudo tail -f /var/log/nginx/access.log
```

### Check Service Status
```bash
sudo systemctl status trademanthan-backend
sudo systemctl status nginx
```

### Restart Services
```bash
sudo systemctl restart trademanthan-backend
sudo systemctl restart nginx
```

### Database Management
```bash
# Access PostgreSQL
sudo -u postgres psql trademanthan

# Backup database
pg_dump -U trademanthan trademanthan > backup.sql

# Restore database
psql -U trademanthan trademanthan < backup.sql
```

## Security Checklist

- [ ] Change default database password
- [ ] Set strong SECRET_KEY in .env
- [ ] Setup SSL/HTTPS with certbot
- [ ] Configure firewall (UFW)
  ```bash
  sudo ufw allow 22
  sudo ufw allow 80
  sudo ufw allow 443
  sudo ufw enable
  ```
- [ ] Keep .env file secure (never commit to git)
- [ ] Regular security updates: `sudo apt update && sudo apt upgrade`
- [ ] Setup log rotation
- [ ] Configure backup strategy

## Performance Optimization

### For t2.micro instances:

1. **Add swap space**:
```bash
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

2. **Optimize Nginx**:
```bash
sudo nano /etc/nginx/nginx.conf
# Set worker_processes to 1 for t2.micro
# worker_processes 1;
```

3. **Setup Redis for caching** (optional):
```bash
sudo apt install redis-server
```

## Support

For issues or questions:
- Check logs first
- Review GitHub repository issues
- Contact: [your-email@example.com]

