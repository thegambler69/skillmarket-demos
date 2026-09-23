#!/usr/bin/env bash
set -euo pipefail
project_dir="$(cd "$(dirname "$0")/.." && pwd)"
service_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$service_dir"
sed "s#%h/gmgn-demos/aitrader#$project_dir#g" "$project_dir/scripts/gmgn-browser-terminal.service" > "$service_dir/gmgn-browser-terminal.service"
echo "Installed $service_dir/gmgn-browser-terminal.service"
echo "Review it, then run: systemctl --user daemon-reload && systemctl --user enable --now gmgn-browser-terminal.service"
