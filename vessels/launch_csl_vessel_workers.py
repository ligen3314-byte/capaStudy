from __future__ import annotations

import json
import os
import string
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List


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


def main() -> None:
    if not CSL_VESSELS_JSON.exists():
        raise FileNotFoundError(f"Not found: {CSL_VESSELS_JSON}")
    if not WORKER.exists():
        raise FileNotFoundError(f"Not found: {WORKER}")

    group_count = 10
    group_size = 1900
    names = load_names(CSL_VESSELS_JSON)
    total = len(names)

    GROUP_DIR.mkdir(parents=True, exist_ok=True)
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%y%m%d%H%M%S")
    group_files: List[Path] = []
    for i in range(group_count):
        start = i * group_size
        end = start + group_size
        chunk = names[start:end]
        gfile = GROUP_DIR / f"group_{i+1:02d}_{ts}.json"
        gfile.write_text(json.dumps(chunk, ensure_ascii=False, indent=2), encoding="utf-8")
        group_files.append(gfile)
        print(f"group {i+1:02d}: size={len(chunk)} file={gfile}")

    procs = []
    for i, gfile in enumerate(group_files, start=1):
        log_file = LOG_DIR / f"group_{i:02d}_{ts}.log"
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
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        p = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        procs.append((p, log_file))
        print(f"started group {i:02d} pid={p.pid} log={log_file}")

    print("all workers started")
    print(f"total source names={total}, group_count={group_count}, group_size={group_size}")
    print(f"logs dir: {LOG_DIR}")
    print("use Windows Task Manager or check logs to monitor progress")


if __name__ == "__main__":
    main()
