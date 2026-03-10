#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MSC_DIR="$BASE_DIR/MSC FETCH"
MSK_DIR="$BASE_DIR/MSK FETCH"
CSL_DIR="$BASE_DIR/CSL FETCH"
MERGE_SCRIPT="$BASE_DIR/merge_all_carriers.py"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "[ERROR] python3 or python is required."
  exit 1
fi

timestamp() {
  date '+%F %T'
}

run_python_step() {
  local label="$1"
  local work_dir="$2"
  local script_path="$3"

  echo
  echo "$label"

  (
    cd "$work_dir"
    "$PYTHON_BIN" "$script_path"
  ) || {
    echo "[ERROR] ${label#*] } failed."
    exit 1
  }
}

echo "========================================"
echo "[START] Full query pipeline"
echo "Base: $BASE_DIR"
echo "Time: $(timestamp)"
echo "========================================"

run_python_step "[1/4] Run MSC full query..." "$MSC_DIR" "$MSC_DIR/MSC_FETCH.py"
run_python_step "[2/4] Run MSK full query..." "$MSK_DIR" "$MSK_DIR/MSK_FETCH.py"
run_python_step "[3/4] Run CSL full query..." "$CSL_DIR" "$CSL_DIR/CSL_FETCH.py"
run_python_step "[4/4] Merge latest outputs..." "$BASE_DIR" "$MERGE_SCRIPT"

echo
echo "========================================"
echo "[DONE] Full query pipeline finished."
echo "Time: $(timestamp)"
echo "========================================"
