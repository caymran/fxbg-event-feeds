#!/usr/bin/env bash
set -Eeuo pipefail

# Run the build locally the same way CI does.
# Usage:
#   ./build.sh
# Optional env:
#   FEEDS_DEBUG=1 CLEAR_CACHE=1 EVENTBRITE_TOKEN=... BANDSINTOWN_APP_ID=...

cd "$(dirname "$0")"

# Create venv if missing
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

# Install deps (same as CI)
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install playwright
python -m playwright install chromium

# Speed up future runs (optional)
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$HOME/.cache/ms-playwright}"

# Load .env if present (tokens, etc.)
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

# Optional: clear HTTP cache
if [[ "${CLEAR_CACHE:-0}" == "1" ]]; then
  rm -f data/cache.json || true
fi

# Debug like CI
export FEEDS_DEBUG="${FEEDS_DEBUG:-1}"

# Run the build
python src/main.py

echo
echo "Artifacts:"
ls -lh docs/*.ics 2>/dev/null || true
ls -lh data/events.json 2>/dev/null || true
