from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Sequence

from capastudy.settings import LOGS_DIR


BASE_DIR = Path(__file__).resolve().parents[2]
MSC_DIR = BASE_DIR / "MSC FETCH"
MSK_DIR = BASE_DIR / "MSK FETCH"
CSL_DIR = BASE_DIR / "CSL FETCH"
MERGE_SCRIPT = BASE_DIR / "merge_all_carriers.py"


@dataclass
class Step:
    label: str
    work_dir: Path
    script: Path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run capaStudy fetch/merge pipeline.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--only-fetch", action="store_true", help="Run carrier fetch steps only.")
    mode.add_argument("--only-merge", action="store_true", help="Run merge step only.")
    return parser.parse_args(list(argv) if argv is not None else None)


def resolve_mode(args: argparse.Namespace) -> str:
    if args.only_fetch:
        return "fetch"
    if args.only_merge:
        return "merge"
    return "all"


def require_path(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required path not found: {path}")


def log_line(message: str, log_path: Path) -> None:
    print(message)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(message + "\n")


def run_step(step: Step, log_path: Path) -> None:
    log_line("", log_path)
    log_line(step.label, log_path)

    cmd = [sys.executable, str(step.script)]
    process = subprocess.Popen(
        cmd,
        cwd=str(step.work_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert process.stdout is not None
    stdout_encoding = sys.stdout.encoding or "utf-8"
    for line in process.stdout:
        line = line.rstrip("\n")
        try:
            print(line)
        except UnicodeEncodeError:
            safe = line.encode(stdout_encoding, errors="backslashreplace").decode(stdout_encoding, errors="ignore")
            print(safe)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    rc = process.wait()
    if rc != 0:
        raise RuntimeError(f"Step failed ({rc}): {step.label}")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    mode = resolve_mode(args)

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%y%m%d%H%M%S")
    log_path = LOGS_DIR / f"full_pipeline_{ts}.log"

    try:
        require_path(MSC_DIR / "MSC_FETCH.py")
        require_path(MSK_DIR / "MSK_FETCH.py")
        require_path(CSL_DIR / "CSL_FETCH.py")
        require_path(MERGE_SCRIPT)
    except Exception as exc:
        log_line(f"[ERROR] {exc}", log_path)
        log_line(f"Log: {log_path}", log_path)
        return 1

    log_line("========================================", log_path)
    log_line("[START] Full query pipeline", log_path)
    log_line(f"Mode: {mode}", log_path)
    log_line(f"Base: {BASE_DIR}", log_path)
    log_line(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", log_path)
    log_line(f"Log:  {log_path}", log_path)
    log_line("========================================", log_path)

    fetch_steps: List[Step] = [
        Step("[1/4] Run MSC full query...", MSC_DIR, MSC_DIR / "MSC_FETCH.py"),
        Step("[2/4] Run MSK full query...", MSK_DIR, MSK_DIR / "MSK_FETCH.py"),
        Step("[3/4] Run CSL full query (BACK flow)...", CSL_DIR, CSL_DIR / "CSL_FETCH.py"),
    ]
    merge_step = Step("[4/4] Merge latest outputs...", BASE_DIR, MERGE_SCRIPT)

    try:
        if mode != "merge":
            for step in fetch_steps:
                run_step(step, log_path)
        else:
            log_line("", log_path)
            log_line("[1-3/4] Fetch steps skipped for --only-merge.", log_path)

        if mode != "fetch":
            run_step(merge_step, log_path)
        else:
            log_line("", log_path)
            log_line("[4/4] Merge step skipped for --only-fetch.", log_path)

        log_line("", log_path)
        log_line("========================================", log_path)
        log_line("[DONE] Full query pipeline finished.", log_path)
        log_line(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", log_path)
        log_line("========================================", log_path)
        return 0
    except Exception as exc:
        log_line(f"[ERROR] {exc}", log_path)
        log_line(f"Log: {log_path}", log_path)
        return 1
