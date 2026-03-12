from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


BASE_DIR = Path(__file__).resolve().parent
RESULT_DIR = BASE_DIR / "csl_group_results"
LOG_DIR = RESULT_DIR / "logs"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Monitor CSL group query progress.")
    p.add_argument("--run-tag", default="", help="Specific run tag like 260311231906. Default: latest.")
    p.add_argument("--group-count", type=int, default=20, help="Group count (default: 20).")
    p.add_argument("--watch", action="store_true", help="Refresh output continuously.")
    p.add_argument("--interval-sec", type=int, default=15, help="Refresh interval for --watch (default: 15).")
    return p.parse_args()


def latest_run_tag() -> str:
    tags = []
    for p in LOG_DIR.glob("group_*_*.log"):
        stem = p.stem
        parts = stem.split("_")
        if len(parts) >= 3:
            tags.append(parts[-1])
    return sorted(tags)[-1] if tags else ""


def parse_log_progress(log_path: Path) -> Tuple[int, int]:
    if not log_path.exists():
        return (0, 0)
    done, total = 0, 0
    try:
        for line in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.startswith("[group ") and "/" in line:
                # e.g. [group 1 53/878] ...
                try:
                    token = line.split("]")[0].split()[-1]  # 53/878
                    d, t = token.split("/", 1)
                    done, total = int(d), int(t)
                except Exception:
                    continue
    except Exception:
        pass
    return (done, total)


def extract_result_ts(path: Path) -> str:
    # csl_group_01_result_260311231906.json
    name = path.stem
    parts = name.split("_")
    return parts[-1] if parts else ""


def load_rows_count(group_idx: int, run_tag: str) -> int:
    files = sorted(RESULT_DIR.glob(f"csl_group_{group_idx:02d}_result_*.json"))
    if run_tag:
        files = [p for p in files if extract_result_ts(p) >= run_tag]
    if not files:
        return 0
    path = files[-1]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return len(data)
    except Exception:
        pass
    return 0


def print_once(tag: str, group_count: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n[{now}] CSL group progress | run_tag={tag or 'N/A'}")
    print("group | progress | log | result_rows")
    done_groups = 0
    for i in range(1, group_count + 1):
        log_path = LOG_DIR / f"group_{i:02d}_{tag}.log" if tag else Path("")
        d, t = parse_log_progress(log_path) if tag else (0, 0)
        has_log = "Y" if (tag and log_path.exists()) else "N"
        rows = load_rows_count(i, tag)
        is_done = rows > 0 and t > 0 and d >= t
        if is_done:
            done_groups += 1
        prog = f"{d}/{t}" if t > 0 else "-"
        print(f"{i:>5} | {prog:<8} | {has_log}   | {rows}")
    print(f"done_groups={done_groups}/{group_count}")


def main() -> None:
    args = parse_args()
    tag = args.run_tag.strip() or latest_run_tag()
    interval = max(2, int(args.interval_sec))
    if not args.watch:
        print_once(tag, args.group_count)
        return
    while True:
        print_once(tag, args.group_count)
        time.sleep(interval)


if __name__ == "__main__":
    main()
