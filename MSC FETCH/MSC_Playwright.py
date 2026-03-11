from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from capastudy.automation.msc_playwright import update_service_workbook


if __name__ == "__main__":
    log_lines, output_path = update_service_workbook()
    print(f"Updated workbook: {output_path}")
    for line in log_lines:
        print(line)
