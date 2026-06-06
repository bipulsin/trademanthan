# Production deploy — paperclip-vm / twcto_docker

Production runs on **paperclip-vm** (`140.245.14.17`) using [bipulsin/twcto_docker](https://github.com/bipulsin/twcto_docker).

## Repositories

| Repo | Role |
|------|------|
| [bipulsin/trademanthan](https://github.com/bipulsin/trademanthan) | Application source (FastAPI, frontend, algos) |
| [bipulsin/twcto_docker](https://github.com/bipulsin/twcto_docker) | Docker Compose, Dockerfiles, GHCR images, deploy scripts |

## SSH

```bash
ssh paperclip
# or
./scripts/paperclip-ssh.sh
```

Requires `~/.ssh/paperclip_key` or `Host paperclip` in `~/.ssh/config`.

## Deploy after app changes (TradeManthan)

```bash
git push origin main
REBUILD=1 ./scripts/trigger-paperclip-deploy.sh
```

Or one step:

```bash
./scripts/release-push-and-deploy.sh -m "your commit message"
```

`REBUILD=1` (default) rebuilds `app` and `nginx` on paperclip-vm cloning latest `trademanthan` `main`.  
Paperclip-vm is **arm64**; GHCR images from CI are **amd64**, so local build is required after app changes.

## Deploy after Docker/compose changes (twcto_docker)

1. Push to `bipulsin/twcto_docker` `main` (GitHub Actions publishes GHCR images).
2. On paperclip-vm:

```bash
./scripts/trigger-paperclip-deploy.sh
```

## Verify

```bash
curl -s https://www.tradewithcto.com/scan/health
```

Expect `healthy` and response headers `via: Caddy`, `server: nginx/1.27.5`.

## Retired

- EC2 `3.6.199.247` / `TradeM.pem` — stopped; do not deploy there.
- `POST /scan/deploy-backend` — EC2 systemd path; not used on Docker production.
