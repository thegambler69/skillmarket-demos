#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PIDFILE="outputs/aitrader.pid"
if [[ ! -f "$PIDFILE" ]]; then echo "not running"; exit 0; fi
pid=$(cat "$PIDFILE")
kill "$pid" 2>/dev/null || true
rm -f "$PIDFILE"
echo "stopped $pid"
