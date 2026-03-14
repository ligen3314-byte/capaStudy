from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from capastudy.carriers.common import PORT_CALL_COLUMNS, VOYAGE_COLUMNS
from capastudy.merge_common import normalize_text
from capastudy.settings import CSL_QUERY_DIR, MSC_QUERY_DIR, MSK_QUERY_DIR, SERVICE_META_XLSX, VESSEL_DB_XLSX


CARRIER_CONFIG = {
    "CSL": CSL_QUERY_DIR,
    "MSC": MSC_QUERY_DIR,
    "MSK": MSK_QUERY_DIR,
}

DETAIL_GLOB_BY_CARRIER = {
    "CSL": "CSL_FETCH_BATCH_DETAIL_*.xlsx",
    "MSC": "MSC_FETCH_BATCH_DETAIL_*.xlsx",
    "MSK": "MSK_FETCH_BATCH_DETAIL_*.xlsx",
}


def load_vessel_maps(path: Path = VESSEL_DB_XLSX) -> Tuple[Dict[str, int], Dict[str, int]]:
    if not path.exists():
        raise FileNotFoundError(f"vessel DB file not found: {path}")
    df = pd.read_excel(path)
    required = {"vesselName", "TEU", "IMO"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"vessel DB missing columns: {sorted(missing)}")

    teu_map: Dict[str, int] = {}
    imo_map: Dict[str, int] = {}
    for row in df.to_dict(orient="records"):
        name = normalize_text(row.get("vesselName"))
        teu = pd.to_numeric(row.get("TEU"), errors="coerce")
        imo = pd.to_numeric(row.get("IMO"), errors="coerce")
        if name:
            if pd.notna(teu):
                teu_map[name] = int(teu)
            if pd.notna(imo):
                imo_map[name] = int(imo)
    return teu_map, imo_map


def load_service_meta_map(path: Path = SERVICE_META_XLSX) -> Dict[str, Tuple[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"service meta file not found: {path}")
    df = pd.read_excel(path)
    required = {"service", "alliance", "trade"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"service meta missing columns: {sorted(missing)}")
    mapping: Dict[str, Tuple[str, str]] = {}
    for row in df.to_dict(orient="records"):
        service = normalize_text(row.get("service"))
        if not service:
            continue
        alliance = normalize_text(row.get("alliance"))
        trade = normalize_text(row.get("trade"))
        mapping[service] = (alliance, trade)
    return mapping


def find_latest_detail_file(carrier: str, query_dir: Path) -> Path:
    pattern = DETAIL_GLOB_BY_CARRIER[carrier]
    candidates = sorted(query_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No batch detail file found for {carrier} in {query_dir}")
    return candidates[0]


def read_and_normalize_sheet(path: Path, sheet: str, columns: List[str], carrier: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet)
    df = df.reindex(columns=columns)
    df.insert(0, "Carrier", carrier)
    df.insert(1, "SourceFile", path.name)
    return df


def load_latest_all() -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Path]]:
    voyages_all: List[pd.DataFrame] = []
    port_calls_all: List[pd.DataFrame] = []
    selected: Dict[str, Path] = {}

    for carrier, query_dir in CARRIER_CONFIG.items():
        latest = find_latest_detail_file(carrier, query_dir)
        selected[carrier] = latest

        voyages_df = read_and_normalize_sheet(latest, "Total Voyages", VOYAGE_COLUMNS, carrier)
        port_calls_df = read_and_normalize_sheet(latest, "Total PortCalls", PORT_CALL_COLUMNS, carrier)

        voyages_all.append(voyages_df)
        port_calls_all.append(port_calls_df)

    return pd.concat(voyages_all, ignore_index=True), pd.concat(port_calls_all, ignore_index=True), selected
