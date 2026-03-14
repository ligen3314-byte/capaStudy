from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Awaitable, Callable, Iterable, Mapping, Sequence, TypeVar

import pandas as pd

T = TypeVar("T")


VOYAGE_COLUMNS = [
    "LoopAbbrv",
    "VesselCode",
    "VesselName",
    "Voyage",
    "Direction",
    "PortCallCount",
    "FirstPort",
    "LastPort",
    "FirstArrDtlocAct",
    "FirstDepDtlocAct",
    "LastArrDtlocAct",
    "LastDepDtlocAct",
    "FirstArrDtlocCos",
    "FirstDepDtlocCos",
    "LastArrDtlocCos",
    "LastDepDtlocCos",
    "PortCallPath",
]

PORT_CALL_COLUMNS = [
    "LoopAbbrv",
    "VesselCode",
    "VesselName",
    "Voyage",
    "PortCallSeq",
    "PortName",
    "ArrDtlocAct",
    "DepDtlocAct",
    "ArrDtlocCos",
    "DepDtlocCos",
    "Direction",
]


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def timestamp_string() -> str:
    return datetime.now().strftime("%y%m%d%H%M%S")


def rows_to_dataframe(rows: Iterable[object], columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(rows).reindex(columns=columns)


def write_excel_sheets(output_path: Path, sheets: Mapping[str, pd.DataFrame]) -> Path:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output_path


def save_voyage_portcall_workbook(
    output_path: Path,
    voyage_rows: Iterable[object],
    port_call_rows: Iterable[object],
    voyage_sheet_name: str,
    port_call_sheet_name: str,
) -> str:
    write_excel_sheets(
        output_path,
        {
            voyage_sheet_name: rows_to_dataframe(voyage_rows, VOYAGE_COLUMNS),
            port_call_sheet_name: rows_to_dataframe(port_call_rows, PORT_CALL_COLUMNS),
        },
    )
    return str(output_path)


def save_timestamped_voyage_portcall_workbook(
    output_dir: Path,
    file_prefix: str,
    voyage_rows: Iterable[object],
    port_call_rows: Iterable[object],
    voyage_sheet_name: str,
    port_call_sheet_name: str,
) -> str:
    ensure_directory(output_dir)
    output_path = output_dir / f"{file_prefix}_{timestamp_string()}.xlsx"
    return save_voyage_portcall_workbook(
        output_path,
        voyage_rows=voyage_rows,
        port_call_rows=port_call_rows,
        voyage_sheet_name=voyage_sheet_name,
        port_call_sheet_name=port_call_sheet_name,
    )


def save_summary_workbook(
    output_dir: Path,
    file_prefix: str,
    rows: Iterable[object],
    sheet_name: str = "Summary",
) -> str:
    ensure_directory(output_dir)
    output_path = output_dir / f"{file_prefix}_{timestamp_string()}.xlsx"
    write_excel_sheets(output_path, {sheet_name: pd.DataFrame(rows)})
    return str(output_path)


def collect_batch_result(
    result: Mapping[str, object],
    batch_voyages: list[object],
    batch_port_calls: list[object],
) -> dict[str, object]:
    result_copy = dict(result)
    batch_voyages.extend(result_copy.pop("total_voyages", []))
    batch_port_calls.extend(result_copy.pop("total_port_calls", []))
    return result_copy


def run_item_batch(
    items: Iterable[T],
    process_item: Callable[[T], Mapping[str, object]],
    item_label: str,
) -> tuple[list[dict[str, object]], list[object], list[object]]:
    results: list[dict[str, object]] = []
    batch_voyages: list[object] = []
    batch_port_calls: list[object] = []
    for item in items:
        try:
            result = process_item(item)
            results.append(collect_batch_result(result, batch_voyages, batch_port_calls))
        except Exception as exc:
            print(f"{item_label} {item} failed: {exc}")
            results.append({item_label.lower(): item, "error": str(exc)})
    return results, batch_voyages, batch_port_calls


async def run_async_item_batch(
    items: Iterable[T],
    process_item: Callable[[T], Awaitable[Mapping[str, object]]],
    item_label: str,
    after_success: Callable[[T, dict[str, object]], Awaitable[None]] | None = None,
) -> tuple[list[dict[str, object]], list[object], list[object]]:
    results: list[dict[str, object]] = []
    batch_voyages: list[object] = []
    batch_port_calls: list[object] = []
    for item in items:
        try:
            result = await process_item(item)
            summary = collect_batch_result(result, batch_voyages, batch_port_calls)
            results.append(summary)
            if after_success is not None:
                await after_success(item, summary)
        except Exception as exc:
            print(f"{item_label} {item} failed: {exc}")
            results.append({item_label.lower(): item, "error": str(exc)})
    return results, batch_voyages, batch_port_calls


def normalize_cli_tokens(argv: Sequence[str] | None) -> list[str]:
    if argv is None:
        return []
    return [str(item).strip() for item in argv if str(item).strip()]


def choose_requested_items(
    available_items: Iterable[str],
    argv: Sequence[str] | None = None,
    *,
    normalize: Callable[[str], str] | None = None,
    missing_message: Callable[[list[str]], str] | None = None,
) -> list[str]:
    available_list = list(available_items)
    if not available_list:
        return []

    normalize_fn = normalize or (lambda value: value)
    tokens = normalize_cli_tokens(argv)
    if not tokens:
        return available_list

    available_map = {normalize_fn(item): item for item in available_list}
    missing = [token for token in tokens if normalize_fn(token) not in available_map]
    if missing:
        if missing_message is None:
            raise ValueError(f"Requested items not found: {', '.join(sorted(missing))}")
        raise ValueError(missing_message(missing))

    return [available_map[normalize_fn(token)] for token in tokens]
