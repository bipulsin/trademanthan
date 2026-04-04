#!/usr/bin/env bash
# TradeManthan — SSH to production EC2 using workspace key (TradeM.pem in repo root).
# Usage:
#   ./scripts/ec2-ssh.sh                    # interactive login
#   ./scripts/ec2-ssh.sh 'uptime'           # run one command
# Override: EC2_HOST, EC2_USER, EC2_KEY

set -e
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export EC2_HOST="${EC2_HOST:-3.6.199.247}"
export EC2_USER="${EC2_USER:-ubuntu}"
export EC2_KEY="${EC2_KEY:-$ROOT/TradeM.pem}"

if [[ ! -f "$EC2_KEY" ]]; then
  echo "EC2 key not found: $EC2_KEY" >&2
  echo "Place TradeM.pem in the repo root or set EC2_KEY." >&2
  exit 1
fi

exec ssh -i "$EC2_KEY" -o ConnectTimeout=25 -o ServerAliveInterval=30 \
  "${EC2_USER}@${EC2_HOST}" "$@"
