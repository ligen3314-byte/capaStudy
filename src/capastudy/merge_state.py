from __future__ import annotations

import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from capastudy.merge_common import AUDIT_COLUMNS, normalize_text, stable_cell_to_str
from capastudy.settings import DATA_MERGED_DIR, DATA_STATE_DIR, LEGACY_MERGED_DIR, LEGACY_STATE_DIR


OUTPUT_DIR = DATA_MERGED_DIR
UPDATE_DIR = DATA_STATE_DIR
LEGACY_OUTPUT_DIR = LEGACY_MERGED_DIR
LEGACY_UPDATE_DIR = LEGACY_STATE_DIR


def save_merged(voyages: pd.DataFrame, port_calls: pd.DataFrame, selected: Dict[str, Path]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LEGACY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%y%m%d%H%M%S")
    output_path = OUTPUT_DIR / f"ALL_CARRIERS_MERGED_{ts}.xlsx"
    summary_rows = [{"Carrier": carrier, "SourceFile": path.name, "SourcePath": str(path)} for carrier, path in selected.items()]

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows).to_excel(writer, index=False, sheet_name="Sources")
        voyage_cols = list(voyages.columns)
        if "voyage_id" in voyage_cols:
            voyage_cols.remove("voyage_id")
            voyage_cols.insert(0, "voyage_id")
            voyages = voyages.reindex(columns=voyage_cols)
        port_call_cols = list(port_calls.columns)
        if "voyage_id" in port_call_cols:
            port_call_cols.remove("voyage_id")
            port_call_cols.insert(0, "voyage_id")
            port_calls = port_calls.reindex(columns=port_call_cols)
        voyages.to_excel(writer, index=False, sheet_name="Total Voyages")
        port_calls.to_excel(writer, index=False, sheet_name="Total PortCalls")

    shutil.copy2(output_path, LEGACY_OUTPUT_DIR / output_path.name)
    return output_path


def build_row_hash(df: pd.DataFrame, compare_cols: List[str]) -> pd.Series:
    def _row_hash(row: pd.Series) -> str:
        text = "|".join(stable_cell_to_str(row.get(c)) for c in compare_cols)
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    return df.apply(_row_hash, axis=1)


def build_portcall_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["voyage_id"].map(stable_cell_to_str)
        + "|"
        + df["PortCallSeq"].map(stable_cell_to_str)
        + "|"
        + df["PortName"].map(normalize_text)
        + "|"
        + df["ArrDtlocCos"].map(stable_cell_to_str)
        + "|"
        + df["DepDtlocCos"].map(stable_cell_to_str)
    )


def ensure_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in AUDIT_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out


def merge_current_entity(entity_name: str, new_df: pd.DataFrame, current_df: pd.DataFrame, key_col: str, snapshot_date: str, updated_at: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    new_core = new_df.copy().drop_duplicates(subset=[key_col], keep="first")
    cur = ensure_audit_columns(current_df.copy())
    compare_cols = [c for c in new_core.columns if c not in AUDIT_COLUMNS]
    new_core["_row_hash"] = build_row_hash(new_core, compare_cols)
    for col in compare_cols + AUDIT_COLUMNS:
        if col not in cur.columns:
            cur[col] = pd.NA
    cur = cur.reindex(columns=compare_cols + AUDIT_COLUMNS)
    new_core = new_core.reindex(columns=compare_cols + ["_row_hash"])
    cur_idx = cur.set_index(key_col, drop=False)
    new_idx = new_core.set_index(key_col, drop=False)
    cur_keys = set(cur_idx.index.tolist())
    new_keys = set(new_idx.index.tolist())
    change_rows: List[Dict[str, object]] = []
    out = cur_idx.copy()

    for k in sorted(new_keys - cur_keys):
        row = new_idx.loc[k]
        out.loc[k, compare_cols] = row[compare_cols]
        out.loc[k, "first_seen_date"] = snapshot_date
        out.loc[k, "last_seen_date"] = snapshot_date
        out.loc[k, "snapshot_date"] = snapshot_date
        out.loc[k, "updated_at"] = updated_at
        out.loc[k, "is_active"] = 1
        out.loc[k, "_row_hash"] = row["_row_hash"]
        change_rows.append({"entity": entity_name, "change_type": "insert", key_col: k})

    for k in sorted(new_keys & cur_keys):
        old_hash = stable_cell_to_str(out.at[k, "_row_hash"])
        new_hash = stable_cell_to_str(new_idx.at[k, "_row_hash"])
        out.loc[k, "snapshot_date"] = snapshot_date
        out.loc[k, "last_seen_date"] = snapshot_date
        out.loc[k, "is_active"] = 1
        if old_hash != new_hash:
            out.loc[k, compare_cols] = new_idx.loc[k, compare_cols]
            out.loc[k, "_row_hash"] = new_hash
            out.loc[k, "updated_at"] = updated_at
            change_rows.append({"entity": entity_name, "change_type": "update", key_col: k})

    for k in sorted(cur_keys - new_keys):
        active_num = pd.to_numeric(out.at[k, "is_active"], errors="coerce")
        was_active = int(active_num) if pd.notna(active_num) else 1
        out.loc[k, "snapshot_date"] = snapshot_date
        out.loc[k, "is_active"] = 0
        if was_active == 1:
            out.loc[k, "updated_at"] = updated_at
            change_rows.append({"entity": entity_name, "change_type": "disappear", key_col: k})

    updated = out.reset_index(drop=True)
    ordered = list(updated.columns)
    for col in [key_col] + AUDIT_COLUMNS:
        if col in ordered:
            ordered.remove(col)
    updated = updated.reindex(columns=[key_col] + AUDIT_COLUMNS + ordered)
    return updated, pd.DataFrame(change_rows)


def load_sheet_or_empty(path: Path, sheet: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()


def save_update_outputs(voyages_new: pd.DataFrame, port_calls_new: pd.DataFrame, selected: Dict[str, Path]) -> Dict[str, Path]:
    UPDATE_DIR.mkdir(parents=True, exist_ok=True)
    LEGACY_UPDATE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%y%m%d%H%M%S")
    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_path = UPDATE_DIR / "ALL_CARRIERS_CURRENT.xlsx"
    history_path = UPDATE_DIR / "ALL_CARRIERS_HISTORY.xlsx"
    changes_path = UPDATE_DIR / f"ALL_CARRIERS_CHANGES_{ts}.xlsx"
    snapshot_path = UPDATE_DIR / f"ALL_CARRIERS_SNAPSHOT_{ts}.xlsx"
    voyages_cur = load_sheet_or_empty(current_path, "Total Voyages")
    portcalls_cur = load_sheet_or_empty(current_path, "Total PortCalls")
    port_calls_new = port_calls_new.copy()
    port_calls_new["portcall_key"] = build_portcall_key(port_calls_new)
    voyages_updated, v_changes = merge_current_entity("voyages", voyages_new, voyages_cur, "voyage_id", snapshot_date, updated_at)
    portcalls_updated, p_changes = merge_current_entity("portcalls", port_calls_new, portcalls_cur, "portcall_key", snapshot_date, updated_at)
    source_df = pd.DataFrame([{"Carrier": c, "SourceFile": p.name, "SourcePath": str(p)} for c, p in selected.items()])
    with pd.ExcelWriter(current_path, engine="openpyxl") as writer:
        source_df.to_excel(writer, index=False, sheet_name="Sources")
        voyages_updated.to_excel(writer, index=False, sheet_name="Total Voyages")
        portcalls_updated.to_excel(writer, index=False, sheet_name="Total PortCalls")
    hist_v = load_sheet_or_empty(history_path, "VoyagesHistory")
    hist_p = load_sheet_or_empty(history_path, "PortCallsHistory")
    v_snap = voyages_new.copy()
    v_snap["snapshot_date"] = snapshot_date
    v_snap["snapshot_ts"] = ts
    p_snap = port_calls_new.copy()
    p_snap["snapshot_date"] = snapshot_date
    p_snap["snapshot_ts"] = ts
    hist_v = pd.concat([hist_v, v_snap], ignore_index=True)
    hist_p = pd.concat([hist_p, p_snap], ignore_index=True)
    with pd.ExcelWriter(history_path, engine="openpyxl") as writer:
        hist_v.to_excel(writer, index=False, sheet_name="VoyagesHistory")
        hist_p.to_excel(writer, index=False, sheet_name="PortCallsHistory")
    with pd.ExcelWriter(snapshot_path, engine="openpyxl") as writer:
        source_df.to_excel(writer, index=False, sheet_name="Sources")
        voyages_new.to_excel(writer, index=False, sheet_name="Total Voyages")
        port_calls_new.to_excel(writer, index=False, sheet_name="Total PortCalls")
    with pd.ExcelWriter(changes_path, engine="openpyxl") as writer:
        v_changes.to_excel(writer, index=False, sheet_name="VoyageChanges")
        p_changes.to_excel(writer, index=False, sheet_name="PortCallChanges")
    for p in [current_path, history_path, snapshot_path, changes_path]:
        shutil.copy2(p, LEGACY_UPDATE_DIR / p.name)
    return {"current": current_path, "history": history_path, "snapshot": snapshot_path, "changes": changes_path}
