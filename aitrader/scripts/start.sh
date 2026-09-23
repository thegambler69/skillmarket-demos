#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PIDFILE="outputs/aitrader.pid"
mkdir -p outputs
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then echo "already running"; exit 0; fi
nohup .venv/bin/uvicorn app:app --host 127.0.0.1 --port 8011 >> outputs/aitrader.log 2>&1 &
echo $! > "$PIDFILE"
echo "started PID $(cat "$PIDFILE")"
