#!/bin/bash
# Safe tail wrapper with timeout
# Usage: safe_tail.sh <file> <lines> <timeout_seconds>

FILE="${1:-/tmp/uvicorn.log}"
LINES="${2:-10}"
TIMEOUT="${3:-5}"

timeout "${TIMEOUT}" tail -n "${LINES}" "${FILE}" 2>/dev/null || echo "Timeout or file not found"

