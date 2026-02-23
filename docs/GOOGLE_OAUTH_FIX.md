# Google OAuth Setup for tradentical.com

Configure Google OAuth 2.0 to use **tradentical.com** and **tradentical@gmail.com**.

## Step 1: Google Cloud Console

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with **tradentical@gmail.com** (or the account that owns the OAuth project)
3. Select your project (or create one for Tradentical)

## Step 2: OAuth Consent Screen

1. Navigate to **APIs & Services** → **OAuth consent screen**
2. Update:
   - **User support email**: tradentical@gmail.com
   - **Developer contact information**: tradentical@gmail.com
3. Under **Test users** (if in Testing mode): Add all emails that need to sign in
4. Click **Save**

## Step 3: Credentials – Authorized JavaScript Origins

1. Go to **APIs & Services** → **Credentials**
2. Click your **OAuth 2.0 Client ID** (Web application)
3. Under **Authorized JavaScript origins**, set:
   - `https://tradentical.com`
   - `https://www.tradentical.com` (if you use www)
   - `http://localhost` (for local dev)
4. Remove any `trademanthan.in` entries

## Step 4: Credentials – Authorized Redirect URIs

1. In the same OAuth client, under **Authorized redirect URIs**, set:
   - `https://tradentical.com/login.html`
   - `https://tradentical.com` (if login is on index)
   - `http://localhost/login.html` (for local dev)
2. Remove any `trademanthan.in` redirect URIs
3. Click **Save**

## Step 5: Nginx – Auth Proxy (EC2/Production)

Ensure `/auth/` requests are proxied to the backend. The `scripts/nginx-tradentical.conf` includes:

```nginx
location ^~ /auth/ {
    proxy_pass http://localhost:8000/auth/;
    ...
}
```

After editing nginx config:
```bash
sudo nginx -t && sudo systemctl reload nginx
```

## Step 6: Environment Variables (EC2/Production)

Ensure your `.env` or environment has:

```
DOMAIN=tradentical.com
GOOGLE_REDIRECT_URI=https://tradentical.com/login.html
GOOGLE_CLIENT_ID=428560418671-t59riis4gqkhavnevt9ve6km54ltsba7.apps.googleusercontent.com
GOOGLE_CLIENT_SECRET=your_client_secret
```

## Step 7: Create New OAuth Client (Optional)

If you prefer a new OAuth client for tradentical.com:

1. **Credentials** → **+ CREATE CREDENTIALS** → **OAuth client ID**
2. Application type: **Web application**
3. Name: e.g. "Tradentical Web"
4. Authorized JavaScript origins: `https://tradentical.com`, `https://www.tradentical.com`
5. Authorized redirect URIs: `https://tradentical.com/login.html`
6. Copy the new **Client ID** and **Client Secret**
7. Update your `.env` with the new values
8. Update the frontend `data-client_id` in `index.html` and `login.html` with the new Client ID

---

## After Changes

1. Wait 1–2 minutes for Google to propagate changes
2. Restart your backend
3. Clear browser cache or use incognito
4. Try signing in again
