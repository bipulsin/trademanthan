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

GHCR images are **multi-arch** (amd64 + arm64). Fast path: CI builds in GitHub, paperclip **pulls** (~1 min).

```bash
export GITHUB_TOKEN=ghp_your_pat   # repo scope for bipulsin/*
./scripts/release-push-and-deploy.sh -m "your commit message"
```

Or step by step:

```bash
git push origin main
./scripts/trigger-twcto-docker-build.sh "$(git rev-parse HEAD)"
./scripts/wait-twcto-docker-build.sh
REBUILD=0 ./scripts/trigger-paperclip-deploy.sh
```

Fallback if CI unavailable: `REBUILD=1 ./scripts/trigger-paperclip-deploy.sh` (~4 min local build on paperclip).

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
