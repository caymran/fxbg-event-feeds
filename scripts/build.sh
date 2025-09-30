#!/usr/bin/env bash
set -Eeuo pipefail

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Create venv if missing
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# Install deps (same as CI)
python -m pip install --upgrade pip
pip install -r "$REPO_ROOT/requirements.txt"
pip install playwright
python -m playwright install chromium

# (Optional) cache location for Playwright browsers
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$HOME/.cache/ms-playwright}"

# Load .env if present (tokens, etc.)
if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$REPO_ROOT/.env"
  set +a
fi

# Optional: clear HTTP cache
if [[ "${CLEAR_CACHE:-0}" == "1" ]]; then
  rm -f "$REPO_ROOT/data/cache.json" || true
fi

# Debug like CI
export FEEDS_DEBUG="${FEEDS_DEBUG:-1}"

# Run the build
python "$REPO_ROOT/src/main.py"

echo
echo "Artifacts:"
ls -lh "$REPO_ROOT/docs/"*.ics 2>/dev/null || true
ls -lh "$REPO_ROOT/data/events.json" 2>/dev/null || true
