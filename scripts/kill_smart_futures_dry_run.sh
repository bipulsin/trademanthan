#!/usr/bin/env bash
# Stop any Smart Futures layered dry-run processes (manual / EC2 / nohup / screen).
# Safe to run when nothing is running (no-op).

set +e

pkill -f "smart_futures_dry_run_layers" 2>/dev/null
pkill -f "tmp_sf_dry_run_layers" 2>/dev/null
# Optional screen session name if someone wrapped the script
screen -S sf_dry_run -X quit 2>/dev/null
screen -wipe 2>/dev/null

if pgrep -af "smart_futures_dry_run_layers|tmp_sf_dry_run_layers" 2>/dev/null; then
  echo "kill_smart_futures_dry_run: still running — sending SIGKILL"
  pkill -9 -f "smart_futures_dry_run_layers" 2>/dev/null
  pkill -9 -f "tmp_sf_dry_run_layers" 2>/dev/null
fi

if pgrep -af "smart_futures_dry_run_layers|tmp_sf_dry_run_layers" 2>/dev/null; then
  echo "kill_smart_futures_dry_run: WARNING — processes may still exist"
  exit 1
fi

echo "kill_smart_futures_dry_run: no matching processes (dry run is stopped)."
exit 0
