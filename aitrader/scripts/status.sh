#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f outputs/aitrader.pid ]] && kill -0 "$(cat outputs/aitrader.pid)" 2>/dev/null; then echo "running PID $(cat outputs/aitrader.pid)"; else echo "stopped"; fi
curl --fail --silent http://127.0.0.1:8011/api/research/status || true
