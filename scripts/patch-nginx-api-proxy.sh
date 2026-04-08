#!/usr/bin/env bash
# Run ON the web server (sudo). Fixes /api/ proxy_pass so FastAPI receives paths under /api/...
# Without this, nginx sends /api/foo → upstream /foo and routes like /api/smart-futures/* 404.
#
# Usage:
#   sudo bash scripts/patch-nginx-api-proxy.sh
#   sudo nginx -t && sudo systemctl reload nginx

set -euo pipefail

patch_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  if grep -q 'location.*/api/' "$f" && grep -q 'proxy_pass http://localhost:8000/;' "$f"; then
    # Only the generic root proxy (wrong); leaves proxy_pass http://localhost:8000/cargpt/ etc. unchanged
    if grep -q 'location /api/ {' "$f" 2>/dev/null || grep -q 'location ^~ /api/ {' "$f" 2>/dev/null; then
      sed -i.bak-api-proxy \
        '/location.*\/api\/ {/,/^[[:space:]]*}/ s|proxy_pass http://localhost:8000/;$|proxy_pass http://localhost:8000/api/;|' \
        "$f"
      echo "Patched: $f (backup: ${f}.bak-api-proxy)"
    fi
  fi
}

for f in /etc/nginx/sites-available/trademanthan /etc/nginx/sites-available/tradentical; do
  patch_file "$f"
done

echo "Done. Run: sudo nginx -t && sudo systemctl reload nginx"
