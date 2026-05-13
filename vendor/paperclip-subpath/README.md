# Paperclip UI at a subpath (`/paperclip/`)

Upstream [paperclipai/paperclip](https://github.com/paperclipai/paperclip) defaults to hosting the Vite UI at `/`. This folder holds a **minimal patch** so the SPA respects a configurable base path (for example behind nginx at `https://www.tradentical.com/paperclip/`).

**Pinned upstream commit:** `b947a7d76c331b3ce4069d3be0ade25cc89b1b90` (matches `0001-spa-subpath.patch`; bump when you upgrade Paperclip).

## What the patch changes

| File | Change |
|------|--------|
| `ui/vite.config.ts` | `vitePublicBase()` from `PAPERCLIP_UI_BASE_PATH` or `BASE_PATH`, default `/`; non-root bases get a trailing slash; `base: vitePublicBase()`. |
| `ui/src/main.tsx` | `BrowserRouter` `basename` from `import.meta.env.BASE_URL`; service worker registered under the same base. |
| `Dockerfile` | `ARG` / `ENV` `PAPERCLIP_UI_BASE_PATH` before `pnpm --filter @paperclipai/ui build`. |

## Manual build (clone + patch)

```bash
git clone https://github.com/paperclipai/paperclip.git
cd paperclip
git fetch --depth 1 origin b947a7d76c331b3ce4069d3be0ade25cc89b1b90
git checkout FETCH_HEAD
git apply /path/to/TradeManthan/vendor/paperclip-subpath/patches/0001-spa-subpath.patch
docker build --build-arg PAPERCLIP_UI_BASE_PATH=/paperclip/ -t paperclip:subpath .
```

For a **root** install, omit the build-arg or use `--build-arg PAPERCLIP_UI_BASE_PATH=/`.

## One-command image (this repo)

From the **TradeManthan** repository root:

```bash
docker build -f vendor/paperclip-subpath/Dockerfile \
  --build-arg PAPERCLIP_UPSTREAM_REF=b947a7d76c331b3ce4069d3be0ade25cc89b1b90 \
  --build-arg PAPERCLIP_UI_BASE_PATH=/paperclip/ \
  -t paperclip:subpath \
  vendor/paperclip-subpath
```

`PAPERCLIP_UPSTREAM_REF` can be any commit SHA or tag on the upstream repo; re-test `git apply` after changing it (line numbers may drift).

## EC2 deploy (this project)

The production container on `3.6.199.247` is named `paperclip`, listens on `127.0.0.1:3100`, and bind-mounts data from `/home/ubuntu/paperclip-docker/paperclip/data/docker-paperclip` → `/paperclip`. Nginx (`scripts/nginx-tradentical.conf`) proxies `https://www.tradentical.com/paperclip/` to it after stripping the `/paperclip/` prefix, and also forwards root-level `/api/`, `/assets/`, `/invite/`, `/sw.js`, and favicons to the same container.

If the running image is upstream `ghcr.io/paperclipai/paperclip:latest`, the served `index.html` references `/assets/...` (root-relative) and the SPA's `BrowserRouter` has no basename. Any visit to `https://www.tradentical.com/paperclip/invite/<token>` then falls through React Router's catch-all inside `CloudAccessGate` and renders the **"Instance setup required"** bootstrap page instead of the invite landing. The fix is to build and run the patched image below.

### 1. Sync repo and build on the server

Run from the workspace root on your laptop:

```bash
ssh -i ./TradeM.pem -o ConnectTimeout=20 ubuntu@3.6.199.247 '
  set -euo pipefail
  cd /home/ubuntu/trademanthan
  git fetch --quiet origin main
  git checkout main
  git pull --ff-only origin main
  docker build \
    -f vendor/paperclip-subpath/Dockerfile \
    --build-arg PAPERCLIP_UPSTREAM_REF=b947a7d76c331b3ce4069d3be0ade25cc89b1b90 \
    --build-arg PAPERCLIP_UI_BASE_PATH=/paperclip/ \
    -t paperclip:subpath \
    vendor/paperclip-subpath
'
```

### 2. Replace the running container

`PAPERCLIP_PUBLIC_URL`, `PAPERCLIP_BIND`, deployment mode, and the data bind mount must match the existing container so signed URLs, sessions, and the embedded database are preserved. Token is **not** rotated.

```bash
ssh -i ./TradeM.pem -o ConnectTimeout=20 ubuntu@3.6.199.247 '
  set -euo pipefail
  docker rm -f paperclip 2>/dev/null || true
  docker run -d --name paperclip --restart unless-stopped \
    -p 127.0.0.1:3100:3100 \
    -v /home/ubuntu/paperclip-docker/paperclip/data/docker-paperclip:/paperclip \
    -e PAPERCLIP_PUBLIC_URL=https://www.tradentical.com/paperclip \
    -e PAPERCLIP_BIND=lan \
    -e PAPERCLIP_DEPLOYMENT_MODE=authenticated \
    -e PAPERCLIP_DEPLOYMENT_EXPOSURE=private \
    -e PAPERCLIP_DATA_DIR=/home/ubuntu/paperclip-docker/paperclip/data/docker-paperclip \
    -e SERVE_UI=true \
    -e HOST=0.0.0.0 \
    -e PORT=3100 \
    paperclip:subpath
'
```

### 3. Verify

```bash
# Expect: /paperclip/assets/index-<hash>.js (NOT /assets/index-...).
curl -sS --max-time 10 https://www.tradentical.com/paperclip/ \
  | grep -Eo '(href|src)="/(paperclip/)?assets/[^"]+"' | head -5

# Expect: HTTP/2 200 (SPA fallback returns index.html for the invite URL).
curl -sSI --max-time 10 https://www.tradentical.com/paperclip/invite/YOUR_INVITE_TOKEN | head -3
```

Then open the invite URL in a browser; the InviteLanding panel ("Set up Paperclip" or "Join …") should render. If you still see "Instance setup required", reload with cache disabled — the previous image's `index.html` is served with `Cache-Control: no-cache` and the assets under `/assets/*` have a 1y `immutable` cache, so a stale service worker or asset can pin the old bundle in some browsers.

### 4. Keep secrets out of the repo

Token, TLS certs, and any `.env` with credentials must stay on the host. `*.pem` is already gitignored; do not paste invite tokens, session cookies, or API keys into commit messages or this README.

## Why the patch is sufficient (route audit)

Pinned upstream commit `b947a7d`:

- `ui/src/App.tsx` declares `<Route path="invite/:token" element={<InviteLandingPage />} />` **above** the `<Route element={<CloudAccessGate />}>` wrapper, so the invite landing is **not** gated by `CloudAccessGate` (it is `CloudAccessGate.tsx → BootstrapPendingPage` that renders "Instance setup required" when `health.bootstrapStatus === "bootstrap_pending"`).
- With the patch, `BrowserRouter` uses `basename="/paperclip"` (derived from `import.meta.env.BASE_URL`). React Router strips the basename from `window.location.pathname`, so `/paperclip/invite/<token>` matches the top-level `invite/:token` route directly. No further patch is needed to render the invite UI.
- Asset and `sw.js` paths are emitted by Vite under the same base (`/paperclip/assets/...`, `/paperclip/sw.js`). Nginx's `^~ /paperclip/` location wins by longest-prefix match and forwards to the container after stripping the prefix, so Express's existing `app.use("/assets", ...)` and SPA fallback continue to serve the right files.
- API and auth calls (`/api/...`, `/api/auth/...`) are root-relative in the UI; they are handled by the dedicated `^~ /api/` location in `scripts/nginx-tradentical.conf` and do **not** need to be rewritten under `/paperclip/`.

## Verify patch before upgrading

```bash
git checkout FETCH_HEAD   # pinned commit
git apply --check vendor/paperclip-subpath/patches/0001-spa-subpath.patch
```

If the check fails, regenerate the diff against the new upstream tree and update `PAPERCLIP_UPSTREAM_REF` in this README and in `Dockerfile` defaults.
