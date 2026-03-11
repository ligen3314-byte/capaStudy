#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE="$BASE_DIR/run_pipeline.py"

if [[ ! -f "$PIPELINE" ]]; then
  echo "[ERROR] Missing file: $PIPELINE"
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "[ERROR] python3 or python is required."
  exit 1
fi

"$PYTHON_BIN" "$PIPELINE" "$@"
