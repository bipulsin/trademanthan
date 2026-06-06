#!/usr/bin/env bash
# Deprecated: EC2 systemd deploy. Production is paperclip-vm Docker (twcto_docker).
exec "$(cd "$(dirname "$0")" && pwd)/trigger-paperclip-deploy.sh" "$@"
