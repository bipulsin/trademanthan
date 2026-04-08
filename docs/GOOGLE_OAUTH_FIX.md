# Google OAuth Setup for TradeWithCTO (www.tradewithcto.com)

The **canonical** site and API host is **`https://www.tradewithcto.com`**.  
Legacy hostnames **`trademanthan.in`** and **`tradentical.com`** should only **301 redirect** to `https://www.tradewithcto.com` (see `scripts/nginx-tradentical.conf`).

Configure Google OAuth 2.0 for the hostname users actually open in the browser (typically `www.tradewithcto.com`).

## Step 1: Google Cloud Console

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with the account that owns the OAuth project
3. Select your project

## Step 2: OAuth Consent Screen

1. Navigate to **APIs & Services** → **OAuth consent screen**
2. Set support and developer contact emails as needed
3. Under **Test users** (if in Testing mode): add emails that need to sign in
4. Click **Save**

## Step 3: Credentials – Authorized JavaScript Origins

1. Go to **APIs & Services** → **Credentials**
2. Open your **OAuth 2.0 Client ID** (Web application)
3. Under **Authorized JavaScript origins**, include at minimum:
   - `https://www.tradewithcto.com`
   - `https://tradewithcto.com` (apex; nginx redirects to `www`)
   - `http://localhost` (optional, local dev)
4. Optionally keep legacy origins during DNS migration (until all traffic uses `www`):
   - `https://trademanthan.in`, `https://www.trademanthan.in`
   - `https://tradentical.com`, `https://www.tradentical.com`

## Step 4: Credentials – Authorized Redirect URIs

1. Under **Authorized redirect URIs**, set at minimum:
   - `https://www.tradewithcto.com/login.html`
   - `https://tradewithcto.com/login.html`
   - `http://localhost/login.html` (optional, local dev)
2. Optionally add legacy login URLs if OAuth callbacks still hit those hosts during migration.
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

### Apex `tradewithcto.com` vs `www.tradewithcto.com`

Google Sign-In checks the **exact** origin. List **both** `https://tradewithcto.com` and `https://www.tradewithcto.com` in origins and redirect URIs if users may land on either. The repo nginx config redirects apex → `www` where applicable.

## Step 6: Environment Variables (EC2/Production)

Ensure `.env` or systemd environment has:

```
DOMAIN=www.tradewithcto.com
GOOGLE_REDIRECT_URI=https://www.tradewithcto.com/login.html
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
```

## Step 7: Create New OAuth Client (Optional)

If you create a new OAuth client:

1. Application type: **Web application**
2. Authorized JavaScript origins: at least `https://www.tradewithcto.com` and `https://tradewithcto.com`
3. Authorized redirect URIs: at least `https://www.tradewithcto.com/login.html` and `https://tradewithcto.com/login.html`
4. Copy **Client ID** and **Client Secret** into `.env`
5. Update frontend `data-client_id` in `index.html` and `login.html` if the Client ID changed

---

## After Changes

1. Wait 1–2 minutes for Google to propagate changes
2. Restart your backend
3. Clear browser cache or use incognito
4. Try signing in again
