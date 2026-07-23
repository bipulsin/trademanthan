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

### Cloud Agents / CI SSH

1. Add **Runtime Secret** `PAPERCLIP_SSH_PRIVATE_KEY` in [Cursor Cloud Agents → Secrets](https://cursor.com/dashboard/cloud-agents) (raw PEM contents of `paperclip_key`).
2. `.cursor/environment.json` runs `./scripts/setup-paperclip-ssh.sh` on start to write `~/.ssh/paperclip_key` and `Host paperclip` in `~/.ssh/config`.
3. Allow Cursor Cloud Agent egress IPs on paperclip-vm port 22 — see [Cursor egress IP ranges](https://cursor.com/docs/cloud-agent/egress-ip-ranges).

### GitHub Actions deploy

Repository secrets (one-time, from a machine with `~/.ssh/paperclip_key`):

```bash
./scripts/configure-paperclip-github-secret.sh --deploy
```

Or manually:

```bash
gh secret set PAPERCLIP_SSH_PRIVATE_KEY < ~/.ssh/paperclip_key -R bipulsin/trademanthan
gh workflow run deploy-paperclip.yml -R bipulsin/trademanthan -f rebuild=true -f run_rs_scan=true
```

**paperclip-vm firewall:** allow inbound SSH (port 22) from GitHub Actions runners (or use a self-hosted runner on paperclip). GitHub-hosted runner IPs change; many setups allow `0.0.0.0/0` on port 22 for `ubuntu` key-only auth.

Workflow also runs on `main` pushes that touch `backend/`, `frontend/`, or deploy scripts.

## Deploy after app changes (TradeManthan)

GHCR images are **multi-arch** (amd64 + arm64). Fast path: CI builds in GitHub, paperclip **pulls** (~1 min).

```bash
# Preferred: token auto-loaded from ~/.config/trademanthan/github_token
# (provision once: ./scripts/provision-github-token.sh)
./scripts/release-push-and-deploy.sh -m "your commit message"
```

Or step by step:

```bash
git push origin main
./scripts/trigger-twcto-docker-build.sh "$(git rev-parse HEAD)"
./scripts/wait-twcto-docker-build.sh
REBUILD=0 ./scripts/trigger-paperclip-deploy.sh
```

### One-time: provision `GITHUB_TOKEN` (REBUILD=0 CI dispatch)

CI dispatch + wait need a GitHub PAT with **`repo`** (+ **`workflow`** helpful). Paperclip GHCR **pull** itself does not need the token when packages are public; the token is for `repository_dispatch` / Actions API from the release scripts.

```bash
./scripts/provision-github-token.sh
# Writes:
#   ~/.config/trademanthan/github_token
#   paperclip:/home/ubuntu/.config/trademanthan/github_token  (mode 600)
./scripts/provision-github-token.sh --verify-only
```

Do **not** commit the token file. Fallback if CI unavailable: `REBUILD=1 ./scripts/trigger-paperclip-deploy.sh`.

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
