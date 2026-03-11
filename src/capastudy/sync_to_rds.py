from __future__ import annotations

import argparse
import json
import math
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import psycopg
from dotenv import load_dotenv
from psycopg import sql

from capastudy.settings import DATA_STATE_DIR, PROJECT_ROOT


DEFAULT_ENV = PROJECT_ROOT / ".env"
DEFAULT_CURRENT_XLSX = DATA_STATE_DIR / "ALL_CARRIERS_CURRENT.xlsx"
DEFAULT_HISTORY_XLSX = DATA_STATE_DIR / "ALL_CARRIERS_HISTORY.xlsx"
RESERVED_COLUMNS = {"payload", "updated_at", "created_at"}
NUMERIC_HINT_COLUMNS = {
    "teu",
    "weeknum",
    "ana_etd_weeknum",
    "firstetdweeknum",
    "portcallcount",
    "portcallseq",
    "imo",
    "is_active",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync capaStudy state workbooks into PostgreSQL RDS.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV), help="Path to .env file with RDS connection config.")
    parser.add_argument(
        "--mode",
        choices=["current", "history", "both"],
        default="both",
        help="Sync current tables, history tables, or both.",
    )
    parser.add_argument("--current-xlsx", default=str(DEFAULT_CURRENT_XLSX), help="Path to ALL_CARRIERS_CURRENT.xlsx")
    parser.add_argument("--history-xlsx", default=str(DEFAULT_HISTORY_XLSX), help="Path to ALL_CARRIERS_HISTORY.xlsx")
    return parser.parse_args()


def clean_cell(value):
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


def row_to_payload(row: Dict[str, object]) -> Dict[str, object]:
    return {k: clean_cell(v) for k, v in row.items()}


def normalize_column_name(name: str) -> str:
    text = str(name).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    if not text:
        text = "col"
    if text[0].isdigit():
        text = f"c_{text}"
    return text


def build_column_mapping(columns: Iterable[str], key_column: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    used: set[str] = set()
    for col in columns:
        if col == key_column:
            mapping[col] = key_column
            used.add(key_column)
            continue
        base = normalize_column_name(col)
        if base in RESERVED_COLUMNS or base == key_column:
            base = f"{base}_v"
        candidate = base
        idx = 2
        while candidate in used:
            candidate = f"{base}_{idx}"
            idx += 1
        used.add(candidate)
        mapping[col] = candidate
    return mapping


def infer_pg_type(original_col: str, normalized_col: str) -> str:
    key = normalized_col.lower()
    if key in NUMERIC_HINT_COLUMNS:
        return "BIGINT"
    if any(token in key for token in ["dtloc", "date", "timestamp", "time"]):
        return "TIMESTAMPTZ"
    return "TEXT"


def ensure_columns(cur, table_name: str, mapping: Dict[str, str], key_column: str) -> Dict[str, str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        """,
        (table_name,),
    )
    existing = {r[0] for r in cur.fetchall()}
    pg_types: Dict[str, str] = {}
    for src, dst in mapping.items():
        if dst == key_column:
            continue
        pg_type = infer_pg_type(src, dst)
        pg_types[dst] = pg_type
        if dst in existing:
            continue
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                sql.Identifier(table_name),
                sql.Identifier(dst),
                sql.SQL(pg_type),
            )
        )
    return pg_types


def convert_value(value, pg_type: str):
    v = clean_cell(value)
    if v is None:
        return None
    if pg_type == "BIGINT":
        try:
            return int(float(v))
        except Exception:
            return None
    if pg_type == "TIMESTAMPTZ":
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    return str(v)


def upsert_structured_rows(
    cur,
    table_name: str,
    rows: List[Dict[str, object]],
    mapping: Dict[str, str],
    pg_types: Dict[str, str],
    key_column: str,
) -> int:
    data_columns = [mapping[c] for c in mapping.keys() if mapping[c] != key_column]
    all_insert_columns = [key_column, "payload", "updated_at"] + data_columns
    update_columns = ["payload", "updated_at"] + data_columns

    insert_sql = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES ({vals}) "
        "ON CONFLICT ({key}) DO UPDATE SET {updates}"
    ).format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in all_insert_columns),
        vals=sql.SQL(", ").join(sql.Placeholder() for _ in all_insert_columns),
        key=sql.Identifier(key_column),
        updates=sql.SQL(", ").join(
            sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c)) for c in update_columns
        ),
    )

    values = []
    for r in rows:
        key_val = r.get(next(k for k, v in mapping.items() if v == key_column))
        if key_val in (None, ""):
            continue
        payload = json.dumps(row_to_payload(r), ensure_ascii=False)
        row_values = [str(key_val), payload, datetime.now()]
        for src_col, dst_col in mapping.items():
            if dst_col == key_column:
                continue
            row_values.append(convert_value(r.get(src_col), pg_types.get(dst_col, "TEXT")))
        values.append(tuple(row_values))

    if values:
        cur.executemany(insert_sql, values)
    return len(values)


def connect_from_env(env_file: Path):
    load_dotenv(env_file)
    return psycopg.connect(
        host=os.getenv("RDS_HOST"),
        port=os.getenv("RDS_PORT"),
        dbname=os.getenv("RDS_DB"),
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASSWORD"),
        sslmode=os.getenv("RDS_SSLMODE", "disable"),
    )


def read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Workbook not found: {path}")
    return pd.read_excel(path, sheet_name=sheet_name)


def sync_current(conn, current_xlsx: Path) -> Tuple[int, int]:
    voyages_df = read_sheet(current_xlsx, "Total Voyages")
    portcalls_df = read_sheet(current_xlsx, "Total PortCalls")

    if "voyage_id" not in voyages_df.columns:
        raise ValueError("Total Voyages sheet missing required column: voyage_id")
    if "portcall_key" not in portcalls_df.columns:
        raise ValueError("Total PortCalls sheet missing required column: portcall_key")

    voyage_rows = voyages_df.to_dict(orient="records")
    portcall_rows = portcalls_df.to_dict(orient="records")

    with conn.cursor() as cur:
        voyages_mapping = build_column_mapping(voyages_df.columns.tolist(), key_column="voyage_id")
        portcalls_mapping = build_column_mapping(portcalls_df.columns.tolist(), key_column="portcall_key")

        voyages_types = ensure_columns(cur, "voyages_current", voyages_mapping, key_column="voyage_id")
        portcalls_types = ensure_columns(cur, "portcalls_current", portcalls_mapping, key_column="portcall_key")

        upsert_structured_rows(
            cur,
            table_name="voyages_current",
            rows=voyage_rows,
            mapping=voyages_mapping,
            pg_types=voyages_types,
            key_column="voyage_id",
        )
        upsert_structured_rows(
            cur,
            table_name="portcalls_current",
            rows=portcall_rows,
            mapping=portcalls_mapping,
            pg_types=portcalls_types,
            key_column="portcall_key",
        )
    return len(voyage_rows), len(portcall_rows)


def _delete_existing_history_snapshot(cur, table_name: str, snapshot_dates: Iterable[date]) -> None:
    dates = sorted({d for d in snapshot_dates if d is not None})
    if not dates:
        return
    cur.execute(
        f"DELETE FROM {table_name} WHERE snapshot_date = ANY(%s)",
        (dates,),
    )


def sync_history(conn, history_xlsx: Path) -> Tuple[int, int]:
    voyages_df = read_sheet(history_xlsx, "VoyagesHistory")
    portcalls_df = read_sheet(history_xlsx, "PortCallsHistory")

    if "voyage_id" not in voyages_df.columns:
        raise ValueError("VoyagesHistory sheet missing required column: voyage_id")
    if "portcall_key" not in portcalls_df.columns:
        raise ValueError("PortCallsHistory sheet missing required column: portcall_key")
    if "snapshot_date" not in voyages_df.columns or "snapshot_date" not in portcalls_df.columns:
        raise ValueError("History sheets missing required column: snapshot_date")

    voyage_rows = voyages_df.to_dict(orient="records")
    portcall_rows = portcalls_df.to_dict(orient="records")

    voyage_dates = pd.to_datetime(voyages_df["snapshot_date"], errors="coerce").dt.date.tolist()
    portcall_dates = pd.to_datetime(portcalls_df["snapshot_date"], errors="coerce").dt.date.tolist()

    with conn.cursor() as cur:
        # Idempotent behavior: replace same snapshot_date partitions.
        _delete_existing_history_snapshot(cur, "voyages_history", voyage_dates)
        _delete_existing_history_snapshot(cur, "portcalls_history", portcall_dates)

        cur.executemany(
            """
            INSERT INTO voyages_history (snapshot_date, voyage_id, payload, created_at)
            VALUES (%s, %s, %s::jsonb, NOW())
            """,
            [
                (
                    clean_cell(r.get("snapshot_date")),
                    str(r.get("voyage_id")) if r.get("voyage_id") not in (None, "") else None,
                    json.dumps(row_to_payload(r), ensure_ascii=False),
                )
                for r in voyage_rows
            ],
        )
        cur.executemany(
            """
            INSERT INTO portcalls_history (snapshot_date, portcall_key, payload, created_at)
            VALUES (%s, %s, %s::jsonb, NOW())
            """,
            [
                (
                    clean_cell(r.get("snapshot_date")),
                    str(r.get("portcall_key")) if r.get("portcall_key") not in (None, "") else None,
                    json.dumps(row_to_payload(r), ensure_ascii=False),
                )
                for r in portcall_rows
            ],
        )
    return len(voyage_rows), len(portcall_rows)


def main() -> None:
    args = parse_args()
    env_file = Path(args.env_file)
    current_xlsx = Path(args.current_xlsx)
    history_xlsx = Path(args.history_xlsx)

    if not env_file.exists():
        raise FileNotFoundError(f".env file not found: {env_file}")

    with connect_from_env(env_file) as conn:
        with conn:
            if args.mode in {"current", "both"}:
                v_cnt, p_cnt = sync_current(conn, current_xlsx)
                print(f"Synced current: voyages={v_cnt}, portcalls={p_cnt}")
            if args.mode in {"history", "both"}:
                vh_cnt, ph_cnt = sync_history(conn, history_xlsx)
                print(f"Synced history: voyages={vh_cnt}, portcalls={ph_cnt}")
    print("SYNC_DONE")


if __name__ == "__main__":
    main()
