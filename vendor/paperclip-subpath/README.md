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

## EC2 deploy (outline)

1. **Build** the image on the server (or in CI), with `PAPERCLIP_UI_BASE_PATH=/paperclip/` if nginx serves the app under that prefix.
2. **Run** the container (example only — set real volumes and env for your deployment):

   ```bash
   docker run -d --name paperclip -p 127.0.0.1:3101:3100 \
     -v paperclip-data:/paperclip \
     paperclip:subpath
   ```

3. **Nginx** should `proxy_pass` `/paperclip/` to the Paperclip HTTP port (see your existing vhost). Keep TLS certificates and API keys out of this repo; use env files or a secrets manager on the host — **do not** commit `.env` with credentials.

4. **Backend URL**: Paperclip’s UI still expects its API where configured for your instance; subpath deployment does not replace Paperclip server configuration.

## Verify patch before upgrading

```bash
git checkout FETCH_HEAD   # pinned commit
git apply --check vendor/paperclip-subpath/patches/0001-spa-subpath.patch
```

If the check fails, regenerate the diff against the new upstream tree and update `PAPERCLIP_UPSTREAM_REF` in this README and in `Dockerfile` defaults.
