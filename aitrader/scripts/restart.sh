#!/usr/bin/env bash
set -euo pipefail
"$(dirname "$0")/stop.sh"
exec "$(dirname "$0")/start.sh"
