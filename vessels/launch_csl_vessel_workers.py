from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


BASE_DIR = Path(__file__).resolve().parent
CSL_VESSELS_JSON = BASE_DIR / "csl_vessels.json"
WORKER = BASE_DIR / "update_vessels_from_csl_group.py"
GROUP_DIR = BASE_DIR / "csl_group_results" / "groups"
RESULT_DIR = BASE_DIR / "csl_group_results"
LOG_DIR = RESULT_DIR / "logs"


def normalize_name(v: str) -> str:
    return "".join(str(v).strip().upper().split())


def load_names(path: Path) -> List[str]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    vessels = obj.get("vessels", []) if isinstance(obj, dict) else []
    seen = set()
    names: List[str] = []
    for item in vessels:
        if isinstance(item, dict):
            n = str(item.get("Name") or item.get("name") or item.get("vesselName") or "").strip()
        else:
            n = str(item).strip()
        if not n:
            continue
        k = normalize_name(n)
        if k and k not in seen:
            seen.add(k)
            names.append(n)
    return names


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Launch CSL vessel workers in sequential groups.")
    p.add_argument("--group-count", type=int, default=20, help="Number of groups (default: 20).")
    p.add_argument(
        "--group-size",
        type=int,
        default=0,
        help="Max names per group. 0 means auto split by group-count (default: 0).",
    )
    p.add_argument(
        "--cooldown-sec",
        type=int,
        default=600,
        help="Cooldown seconds when blocked/rejected (default: 600 = 10 min).",
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max retries for one group when blocked (default: 5).",
    )
    return p.parse_args()


def list_result_jsons(result_dir: Path, group_index: int) -> List[Path]:
    return sorted(result_dir.glob(f"csl_group_{group_index:02d}_result_*.json"))


def load_rows(path: Path) -> List[Dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
    except Exception:
        return []
    return []


def is_blocked(rows: List[Dict[str, Any]]) -> bool:
    if not rows:
        return True

    block_status = 0
    error_like = 0
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        note = str(row.get("note") or "").strip().lower()
        if status == "cookie_invalid":
            block_status += 1
            continue
        if status == "error":
            if re.search(r"(403|429|forbidden|too many|unexpected content-type|text/html|cloudflare|captcha)", note):
                error_like += 1

    n = len(rows)
    if block_status > 0:
        return True
    if n >= 20 and (error_like / n) >= 0.70:
        return True
    return False


def main() -> None:
    args = parse_args()

    if not CSL_VESSELS_JSON.exists():
        raise FileNotFoundError(f"Not found: {CSL_VESSELS_JSON}")
    if not WORKER.exists():
        raise FileNotFoundError(f"Not found: {WORKER}")

    group_count = max(1, int(args.group_count))
    raw_group_size = int(args.group_size)
    cooldown_sec = max(1, int(args.cooldown_sec))
    max_retries = max(1, int(args.max_retries))

    names = load_names(CSL_VESSELS_JSON)
    total = len(names)
    group_size = max(1, math.ceil(total / group_count)) if raw_group_size <= 0 else max(1, raw_group_size)

    GROUP_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%y%m%d%H%M%S")
    group_files: List[Path] = []
    for i in range(group_count):
        start = i * group_size
        end = start + group_size if i < group_count - 1 else total
        chunk = names[start:end]
        gfile = GROUP_DIR / f"group_{i+1:02d}_{ts}.json"
        gfile.write_text(json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8")
        group_files.append(gfile)
        print(f"group {i+1:02d}: size={len(chunk)} file={gfile}")

    print(
        f"start sequential run: total={total}, group_count={group_count}, "
        f"group_size={group_size}, cooldown={cooldown_sec}s, max_retries={max_retries}"
    )

    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    any_failed = False

    for i, gfile in enumerate(group_files, start=1):
        if not gfile.exists():
            print(f"[group {i:02d}] missing file, skip: {gfile}")
            any_failed = True
            continue

        if gfile.stat().st_size == 0:
            print(f"[group {i:02d}] empty group file, skip")
            continue

        retries = 0
        while retries <= max_retries:
            log_file = LOG_DIR / f"group_{i:02d}_{ts}.log"
            before = set(list_result_jsons(RESULT_DIR, i))
            cmd = [
                sys.executable,
                str(WORKER),
                "--group-file",
                str(gfile),
                "--group-index",
                str(i),
                "--result-dir",
                str(RESULT_DIR),
                "--non-interactive",
                "--log-file",
                str(log_file),
            ]
            print(f"[group {i:02d}] run attempt {retries + 1}/{max_retries + 1}")
            rc = subprocess.run(
                cmd,
                cwd=str(BASE_DIR),
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            ).returncode

            after = set(list_result_jsons(RESULT_DIR, i))
            new_files = sorted(after - before)
            latest: Optional[Path] = new_files[-1] if new_files else (sorted(after)[-1] if after else None)
            rows = load_rows(latest) if latest else []
            blocked = is_blocked(rows)

            if rc == 0 and not blocked:
                print(f"[group {i:02d}] done. result={latest}")
                break

            retries += 1
            reason = "blocked/rejected" if blocked else f"worker_rc={rc}"
            if retries > max_retries:
                print(f"[group {i:02d}] failed after retries. reason={reason}")
                any_failed = True
                break
            print(f"[group {i:02d}] {reason}. sleep {cooldown_sec}s then retry...")
            time.sleep(cooldown_sec)

    if any_failed:
        raise SystemExit(2)
    print("all groups finished successfully")


if __name__ == "__main__":
    main()

