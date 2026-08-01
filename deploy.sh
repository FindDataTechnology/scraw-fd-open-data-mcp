#!/usr/bin/env bash
# Build egg + deploy to the shared scrapyd (scraw-project-template).
set -euo pipefail
SCRAPYD_URL="${SCRAPYD_URL:-http://localhost:6800}"
cd "$(dirname "$0")"
# ensure the scrapy entry point is importable for egg activation
pip install -e . -q 2>/dev/null || true
scrapyd-deploy --target production --version "$(date +%s)" --url "$SCRAPYD_URL" --project scraw_fd_open_data_mcp
