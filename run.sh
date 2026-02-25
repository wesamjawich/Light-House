#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -d ".venv" ]]; then
  echo "Missing .venv in $SCRIPT_DIR"
  echo "Create it with: python3 -m venv .venv"
  exit 1
fi

# shellcheck disable=SC1091
source ".venv/bin/activate"

exec lighthouse serve --host 127.0.0.1 --port 8787 "$@"

