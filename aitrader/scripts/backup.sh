#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p outputs/backups
exec .venv/bin/python -c 'from pathlib import Path; from research_storage import ResearchStore; import time; p=Path("outputs/gmgn.db"); stamp=time.strftime("%Y-%m-%d-%H%M%S"); print(ResearchStore(p).backup(Path("outputs/backups") / f"gmgn-{stamp}.db"))'
