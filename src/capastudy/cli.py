from __future__ import annotations

import argparse
import asyncio
import importlib
import sys
from contextlib import contextmanager
from typing import Iterable, Sequence


@contextmanager
def temporary_argv(args: Sequence[str]):
    old_argv = sys.argv[:]
    sys.argv = [old_argv[0], *args]
    try:
        yield
    finally:
        sys.argv = old_argv


def run_sync_main(main_func, args: Sequence[str]) -> int:
    with temporary_argv(args):
        main_func()
    return 0


def run_async_main(main_func, args: Sequence[str]) -> int:
    with temporary_argv(args):
        asyncio.run(main_func())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified CLI for the capaStudy project.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    pipeline_parser = subparsers.add_parser("pipeline", help="Run the full fetch/merge pipeline.")
    pipeline_parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments passed through to the pipeline.")

    fetch_parser = subparsers.add_parser("fetch", help="Run a carrier-specific fetch job.")
    fetch_subparsers = fetch_parser.add_subparsers(dest="carrier", required=True)

    csl_parser = fetch_subparsers.add_parser("csl", help="Fetch CSL schedules.")
    csl_parser.add_argument(
        "--mode",
        choices=["back", "reload", "direct"],
        default="back",
        help="Choose the CSL fetch flow to run.",
    )
    csl_parser.add_argument("services", nargs="*", help="Optional service codes to fetch.")

    msc_parser = fetch_subparsers.add_parser("msc", help="Fetch MSC schedules.")
    msc_parser.add_argument("services", nargs="*", help="Optional service codes to fetch.")

    msk_parser = fetch_subparsers.add_parser("msk", help="Fetch MSK schedules.")
    msk_parser.add_argument("ports", nargs="*", help="Optional port names to fetch.")

    merge_parser = subparsers.add_parser("merge", help="Merge the latest carrier outputs.")
    merge_parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments passed through to merge.")

    sync_parser = subparsers.add_parser("sync", help="Sync current/history workbooks to RDS.")
    sync_parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments passed through to sync.")

    return parser


def normalize_passthrough(args: Iterable[str]) -> list[str]:
    items = list(args)
    if items and items[0] == "--":
        return items[1:]
    return items


def load_callable(module_name: str, attr_name: str = "main"):
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.command == "pipeline":
        return run_sync_main(load_callable("capastudy.pipeline"), normalize_passthrough(args.args))

    if args.command == "fetch":
        if args.carrier == "csl":
            csl_main_by_mode = {
                "back": "capastudy.carriers.csl_fetch_back_test",
                "reload": "capastudy.carriers.csl_fetch_reuse_test",
                "direct": "capastudy.carriers.csl_fetch",
            }
            return run_async_main(load_callable(csl_main_by_mode[args.mode]), list(args.services))
        if args.carrier == "msc":
            return run_sync_main(load_callable("capastudy.carriers.msc_fetch"), list(args.services))
        if args.carrier == "msk":
            return run_sync_main(load_callable("capastudy.carriers.msk_fetch"), list(args.ports))

    if args.command == "merge":
        return run_sync_main(load_callable("capastudy.merge_all_carriers"), normalize_passthrough(args.args))

    if args.command == "sync":
        return run_sync_main(load_callable("capastudy.sync_to_rds"), normalize_passthrough(args.args))

    parser.error(f"Unsupported command: {args.command}")
    return 2
