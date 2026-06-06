#!/usr/bin/env bash
# Deprecated alias — production is paperclip-vm, not EC2.
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/paperclip-ssh.sh" "$@"
