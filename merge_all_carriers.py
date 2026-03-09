from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import re
import time

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "merged_query"
VESSEL_DB_XLSX = BASE_DIR / "vessels" / "vessels_db.xlsx"
ENV_PATH = BASE_DIR / "vessels" / ".env"
MYVESSEL_API1_URL = "https://market.myvessel.cn/sdc/v1/mkt/vessels/fuzzy"
MYVESSEL_API2_URL = "https://market.myvessel.cn/sdc/v1/mkt/vessels/detail/mmsi"
SERVICE_META_XLSX = BASE_DIR / "services" / "service_alliance_trade.xlsx"
ANA_PORT_PRIORITY = [
    "SHANGHAI",
    "NINGBO",
    "SHEKOU",
    "YANTIAN",
    "NANSHA",
    "XIAMEN",
    "QINGDAO",
    "TIANJIN",
    "TIANJINXINGANG",
    "XINGANG",
    "DALIAN",
]

CARRIER_CONFIG = {
    "CSL": BASE_DIR / "CSL FETCH" / "csl_query",
    "MSC": BASE_DIR / "MSC FETCH" / "msc_query",
    "MSK": BASE_DIR / "MSK FETCH" / "msk_query",
}

DETAIL_GLOB_BY_CARRIER = {
    "CSL": "CSL_FETCH_BATCH_DETAIL_*.xlsx",
    "MSC": "MSC_FETCH_BATCH_DETAIL_*.xlsx",
    "MSK": "MSK_FETCH_BATCH_DETAIL_*.xlsx",
}

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


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return " ".join(str(value).strip().upper().split())


def normalize_port_key(value: object) -> str:
    return re.sub(r"[^A-Z0-9]", "", normalize_text(value))


def load_env(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def save_env(path: Path, env: Dict[str, str]) -> None:
    lines = [f"{k}={v}" for k, v in env.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def to_int_or_none(value: object) -> Optional[int]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def walk_dicts(obj: object):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


def get_first(d: Dict[str, object], keys: List[str]) -> object:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    lower_map = {k.lower(): k for k in d.keys()}
    for key in keys:
        real = lower_map.get(key.lower())
        if real and d[real] not in (None, ""):
            return d[real]
    return None


def find_fuzzy_candidates(payload: object) -> List[Dict[str, object]]:
    candidates: List[Dict[str, object]] = []
    for d in walk_dicts(payload):
        lower_keys = {k.lower() for k in d.keys()}
        if lower_keys & {"vesselname", "shipname", "name", "imo", "imono", "mmsi"}:
            candidates.append(d)
    return candidates


def choose_best_candidate(query_name: str, candidates: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
    q = re.sub(r"\s+", "", normalize_text(query_name))
    best: Optional[Dict[str, object]] = None
    best_score = -1
    for c in candidates:
        name = str(get_first(c, ["vesselName", "shipName", "name", "vslName", "enName", "cnName"]) or "")
        n = re.sub(r"\s+", "", normalize_text(name))
        score = 0
        if n == q:
            score = 300
        elif q and q in n:
            score = 200
        elif n and n in q:
            score = 150
        if to_int_or_none(get_first(c, ["mmsi", "vesselMmsi", "vesselMMSI"])):
            score += 40
        if to_int_or_none(get_first(c, ["imo", "imoNo", "vesselImo", "vesselIMO"])):
            score += 20
        if score > best_score:
            best_score = score
            best = c
    return best


def extract_teu_from_detail(payload: object) -> Optional[int]:
    best: Optional[int] = None
    for d in walk_dicts(payload):
        for k, v in d.items():
            key_norm = re.sub(r"[^a-z0-9]", "", str(k).lower())
            if ("teu" in key_norm and "rate" not in key_norm) or key_norm in {
                "teucapacity",
                "nominalteu",
                "capacityteu",
                "containercapacity",
            }:
                num = to_int_or_none(v)
                if num and (best is None or num > best):
                    best = num
    return best


def build_myvessel_headers(token: str, referer: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Referer": referer,
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "accept": "application/json",
        "Content-Type": "application/json",
    }


def fetch_missing_vessel_rows(
    names: List[str], token: str, referer: str, record_num: int = 10, timeout: int = 30
) -> Tuple[List[Dict[str, object]], bool]:
    headers = build_myvessel_headers(token, referer)
    session = requests.Session()

    rows: List[Dict[str, object]] = []
    unauthorized = False

    for idx, name in enumerate(names, start=1):
        imo: Optional[int] = None
        teu: Optional[int] = None
        mmsi: Optional[int] = None
        status = "ok"
        try:
            r1 = session.post(
                MYVESSEL_API1_URL,
                headers=headers,
                json={"kw": str(name), "recordNum": record_num},
                timeout=timeout,
            )
            r1.raise_for_status()
            p1 = r1.json()
            if isinstance(p1, dict) and p1.get("code") == 401:
                unauthorized = True
                status = "unauthorized"
            else:
                cands = find_fuzzy_candidates(p1)
                best = choose_best_candidate(str(name), cands)
                if best is None:
                    status = "api1_no_candidate"
                else:
                    imo = to_int_or_none(get_first(best, ["imo", "imoNo", "vesselImo", "vesselIMO"]))
                    mmsi = to_int_or_none(get_first(best, ["mmsi", "vesselMmsi", "vesselMMSI"]))
                    if mmsi:
                        r2 = session.get(
                            MYVESSEL_API2_URL,
                            headers={k: v for k, v in headers.items() if k != "Content-Type"},
                            params={"mmsi": str(mmsi)},
                            timeout=timeout,
                        )
                        r2.raise_for_status()
                        p2 = r2.json()
                        if isinstance(p2, dict) and p2.get("code") == 401:
                            unauthorized = True
                            status = "unauthorized"
                        else:
                            teu = extract_teu_from_detail(p2)
                            if teu is None:
                                status = "api2_no_teu"
                    else:
                        status = "api1_no_mmsi"
        except Exception as exc:
            status = f"error:{type(exc).__name__}"

        rows.append(
            {
                "vesselName": str(name).strip(),
                "IMO": imo,
                "TEU": teu,
                "_mmsi": mmsi,
                "_status": status,
            }
        )
        print(
            f"[{idx}/{len(names)}] {name} -> IMO={imo} MMSI={mmsi} TEU={teu} status={status}"
        )
        if unauthorized:
            break
        time.sleep(0.15)
    return rows, unauthorized


def excel_weeknum_type16(dep_dt: object) -> Optional[int]:
    if dep_dt is None or (isinstance(dep_dt, float) and pd.isna(dep_dt)):
        return None
    dt = pd.to_datetime(dep_dt, errors="coerce")
    if pd.isna(dt):
        return None
    d = dt.date()

    jan1 = d.replace(month=1, day=1)
    # Excel WEEKNUM(...,16): week starts on Saturday.
    start_weekday = 5  # Monday=0 ... Saturday=5
    offset = (jan1.weekday() - start_weekday) % 7
    week1_start = jan1 - timedelta(days=offset)
    return ((d - week1_start).days // 7) + 1


def load_vessel_maps(path: Path) -> Tuple[Dict[str, int], Dict[str, int]]:
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


def load_service_meta_map(path: Path) -> Dict[str, Tuple[str, str]]:
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


def add_alliance_trade_columns(
    voyages: pd.DataFrame, port_calls: pd.DataFrame, service_meta: Dict[str, Tuple[str, str]]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    def _alliance(loop: object) -> Optional[str]:
        return service_meta.get(normalize_text(loop), ("", ""))[0] or None

    def _trade(loop: object) -> Optional[str]:
        return service_meta.get(normalize_text(loop), ("", ""))[1] or None

    v = voyages.copy()
    p = port_calls.copy()
    v["Alliance"] = v["LoopAbbrv"].map(_alliance)
    v["Trade"] = v["LoopAbbrv"].map(_trade)
    p["Alliance"] = p["LoopAbbrv"].map(_alliance)
    p["Trade"] = p["LoopAbbrv"].map(_trade)

    def _reorder(df: pd.DataFrame) -> pd.DataFrame:
        cols = list(df.columns)
        for col in ["Alliance", "Trade"]:
            cols.remove(col)
        insert_at = cols.index("LoopAbbrv") + 1 if "LoopAbbrv" in cols else len(cols)
        cols[insert_at:insert_at] = ["Alliance", "Trade"]
        return df.reindex(columns=cols)

    return _reorder(v), _reorder(p)


def add_ana_etd_weeknum(voyages: pd.DataFrame, port_calls: pd.DataFrame) -> pd.DataFrame:
    if "voyage_id" not in voyages.columns or "voyage_id" not in port_calls.columns:
        return voyages
    p = port_calls.copy()
    p["_port_key"] = p["PortName"].map(normalize_port_key)
    p["_dep_dt"] = pd.to_datetime(p["DepDtlocCos"], errors="coerce")
    priority_index = {k: i for i, k in enumerate(ANA_PORT_PRIORITY)}
    p = p[p["_port_key"].isin(priority_index.keys())].copy()
    if p.empty:
        out = voyages.copy()
        out["Ana_ETD_WeekNum"] = pd.array([pd.NA] * len(out), dtype="Int64")
        return out
    p["_prio"] = p["_port_key"].map(priority_index)
    p["_seq"] = pd.to_numeric(p.get("PortCallSeq"), errors="coerce")
    p["_ana_week"] = p["DepDtlocCos"].map(excel_weeknum_type16)
    p = p.sort_values(
        ["voyage_id", "_prio", "_seq", "_dep_dt"],
        kind="stable",
    )
    first = p.drop_duplicates(subset=["voyage_id"], keep="first")
    week_map = {str(r["voyage_id"]): r["_ana_week"] for r in first.to_dict(orient="records")}

    out = voyages.copy()
    out["Ana_ETD_WeekNum"] = out["voyage_id"].map(lambda x: week_map.get(str(x))).astype("Int64")
    cols = list(out.columns)
    cols.remove("Ana_ETD_WeekNum")
    insert_at = cols.index("FirstETDWeekNum") + 1 if "FirstETDWeekNum" in cols else len(cols)
    cols.insert(insert_at, "Ana_ETD_WeekNum")
    return out.reindex(columns=cols)


def vessel_key_from_row(row: pd.Series, imo_map: Dict[str, int]) -> str:
    vessel_code = normalize_text(row.get("VesselCode"))
    if vessel_code:
        return vessel_code
    vessel_name = normalize_text(row.get("VesselName"))
    imo = imo_map.get(vessel_name)
    return str(imo) if imo else "NA"


def build_core_key(carrier: object, loop_abbrv: object, vessel_key: object, voyage: object) -> str:
    return "|".join(
        [
            normalize_text(carrier) or "NA",
            normalize_text(loop_abbrv) or "NA",
            normalize_text(vessel_key) or "NA",
            normalize_text(voyage) or "NA",
        ]
    )


def enrich_voyages_with_ids(voyages: pd.DataFrame, imo_map: Dict[str, int]) -> pd.DataFrame:
    out = voyages.copy()
    out["IMO"] = out["VesselName"].map(lambda x: imo_map.get(normalize_text(x))).astype("Int64")
    out["VesselKey"] = out.apply(lambda r: vessel_key_from_row(r, imo_map), axis=1)
    out["_core_key"] = out.apply(
        lambda r: build_core_key(r.get("Carrier"), r.get("LoopAbbrv"), r.get("VesselKey"), r.get("Voyage")),
        axis=1,
    )
    # Unified rule: cycle_no is ordered by tail-port ETD.
    out["_tail_dep_dt"] = pd.to_datetime(out["LastDepDtlocCos"], errors="coerce")
    out = out.sort_values(["_core_key", "_tail_dep_dt", "SourceFile"], kind="stable").reset_index(drop=True)
    out["_cycle_no"] = out.groupby("_core_key").cumcount() + 1
    out["voyage_id"] = out.apply(
        lambda r: f"{r['_core_key']}|{int(r['_cycle_no']):03d}",
        axis=1,
    )
    out = out.drop(columns=["_tail_dep_dt", "_core_key", "_cycle_no"])

    ordered = list(out.columns)
    for col in ["IMO", "VesselKey", "voyage_id"]:
        ordered.remove(col)
    insert_at = ordered.index("Voyage") + 1 if "Voyage" in ordered else len(ordered)
    ordered[insert_at:insert_at] = ["IMO", "VesselKey", "voyage_id"]
    return out.reindex(columns=ordered)


def attach_ids_to_port_calls(port_calls: pd.DataFrame, voyages: pd.DataFrame, imo_map: Dict[str, int]) -> pd.DataFrame:
    out = port_calls.copy()
    out["IMO"] = out["VesselName"].map(lambda x: imo_map.get(normalize_text(x))).astype("Int64")
    out["VesselKey"] = out.apply(lambda r: vessel_key_from_row(r, imo_map), axis=1)
    out["_core_key"] = out.apply(
        lambda r: build_core_key(r.get("Carrier"), r.get("LoopAbbrv"), r.get("VesselKey"), r.get("Voyage")),
        axis=1,
    )

    candidates: Dict[Tuple[str, str, str, str], List[Tuple[pd.Timestamp, str]]] = {}
    v = voyages.copy()
    v["_tail_dep_dt"] = pd.to_datetime(v["LastDepDtlocCos"], errors="coerce")
    for row in v.to_dict(orient="records"):
        base_key = (
            normalize_text(row.get("Carrier")),
            normalize_text(row.get("LoopAbbrv")),
            normalize_text(row.get("VesselKey")),
            normalize_text(row.get("Voyage")),
        )
        dep_dt = pd.to_datetime(row.get("_tail_dep_dt"), errors="coerce")
        candidates.setdefault(base_key, []).append((dep_dt, str(row.get("voyage_id"))))

    for key in list(candidates.keys()):
        candidates[key].sort(key=lambda x: (pd.Timestamp.max if pd.isna(x[0]) else x[0], x[1]))

    voyage_ids: List[str] = []
    dep_series = pd.to_datetime(out["DepDtlocCos"], errors="coerce")
    for idx, row in out.iterrows():
        base_key = (
            normalize_text(row.get("Carrier")),
            normalize_text(row.get("LoopAbbrv")),
            normalize_text(row.get("VesselKey")),
            normalize_text(row.get("Voyage")),
        )
        items = candidates.get(base_key, [])
        if not items:
            voyage_ids.append("")
            continue
        if len(items) == 1:
            voyage_ids.append(items[0][1])
            continue

        dep_dt = dep_series.iloc[idx]
        if pd.isna(dep_dt):
            voyage_ids.append(items[0][1])
            continue
        picked = min(
            items,
            key=lambda x: abs((dep_dt - x[0]).total_seconds()) if not pd.isna(x[0]) else float("inf"),
        )
        voyage_ids.append(picked[1])

    out["voyage_id"] = voyage_ids
    out = out.drop(columns=["_core_key"])

    ordered = list(out.columns)
    for col in ["IMO", "VesselKey", "voyage_id"]:
        ordered.remove(col)
    insert_at = ordered.index("Voyage") + 1 if "Voyage" in ordered else len(ordered)
    ordered[insert_at:insert_at] = ["IMO", "VesselKey", "voyage_id"]
    return out.reindex(columns=ordered)


def enrich_port_calls(port_calls: pd.DataFrame, teu_map: Dict[str, int]) -> pd.DataFrame:
    out = port_calls.copy()
    out["weekNum"] = out["DepDtlocCos"].map(excel_weeknum_type16).astype("Int64")
    out["TEU"] = out["VesselName"].map(lambda x: teu_map.get(normalize_text(x))).astype("Int64")

    ordered_cols = list(out.columns)
    if "weekNum" in ordered_cols:
        ordered_cols.remove("weekNum")
    if "TEU" in ordered_cols:
        ordered_cols.remove("TEU")
    insert_at = ordered_cols.index("DepDtlocCos") + 1 if "DepDtlocCos" in ordered_cols else len(ordered_cols)
    ordered_cols[insert_at:insert_at] = ["weekNum", "TEU"]
    return out.reindex(columns=ordered_cols)


def enrich_voyages_with_teu(voyages: pd.DataFrame, teu_map: Dict[str, int]) -> pd.DataFrame:
    out = voyages.copy()
    out["TEU"] = out["VesselName"].map(lambda x: teu_map.get(normalize_text(x))).astype("Int64")
    out["FirstETDWeekNum"] = out["FirstDepDtlocCos"].map(excel_weeknum_type16).astype("Int64")
    ordered_cols = list(out.columns)
    if "TEU" in ordered_cols:
        ordered_cols.remove("TEU")
    if "FirstETDWeekNum" in ordered_cols:
        ordered_cols.remove("FirstETDWeekNum")
    insert_at = ordered_cols.index("VesselName") + 1 if "VesselName" in ordered_cols else len(ordered_cols)
    ordered_cols.insert(insert_at, "TEU")
    first_dep_idx = ordered_cols.index("FirstDepDtlocCos") + 1 if "FirstDepDtlocCos" in ordered_cols else len(ordered_cols)
    ordered_cols.insert(first_dep_idx, "FirstETDWeekNum")
    return out.reindex(columns=ordered_cols)


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


def ensure_vessel_db_coverage(voyages: pd.DataFrame, port_calls: pd.DataFrame) -> None:
    if not VESSEL_DB_XLSX.exists():
        raise FileNotFoundError(f"vessel DB file not found: {VESSEL_DB_XLSX}")

    xl = pd.ExcelFile(VESSEL_DB_XLSX)
    if "vessels" not in xl.sheet_names:
        raise ValueError("vessels_db.xlsx missing required sheet: vessels")

    sheets = {s: pd.read_excel(VESSEL_DB_XLSX, sheet_name=s) for s in xl.sheet_names}
    vessels_df = sheets["vessels"].copy()
    if "vesselName" not in vessels_df.columns:
        raise ValueError("vessels sheet missing column: vesselName")

    source_names = set()
    for df in (voyages, port_calls):
        if "VesselName" not in df.columns:
            continue
        for v in df["VesselName"].tolist():
            n = normalize_text(v)
            if n:
                source_names.add(n)

    existing_names = {normalize_text(v) for v in vessels_df["vesselName"].tolist() if normalize_text(v)}
    missing_names = sorted(source_names - existing_names)
    if not missing_names:
        print("Vessel DB check: no missing vessel names.")
        return

    print(f"Vessel DB check: {len(missing_names)} missing vessel names. Querying APIs...")

    env = load_env(ENV_PATH)
    token = env.get("MYVESSEL_BEARER_TOKEN", "")
    referer = env.get("MYVESSEL_REFERER", "https://market.myvessel.cn/")
    record_num = int(env.get("MYVESSEL_RECORD_NUM", "10") or 10)
    timeout = int(env.get("MYVESSEL_TIMEOUT", "30") or 30)

    if not token:
        raise RuntimeError(
            f"MYVESSEL_BEARER_TOKEN is empty. Please fill token in {ENV_PATH} and rerun."
        )

    max_rounds = 2
    all_rows: List[Dict[str, object]] = []
    for round_idx in range(1, max_rounds + 1):
        rows, unauthorized = fetch_missing_vessel_rows(
            missing_names, token=token, referer=referer, record_num=record_num, timeout=timeout
        )
        all_rows.extend(rows)
        if not unauthorized:
            break
        if round_idx >= max_rounds:
            raise RuntimeError("MyVessel token unauthorized. Please update .env token and rerun.")
        print("MyVessel token expired/unauthorized. Please input a new token now.")
        new_token = input("MYVESSEL_BEARER_TOKEN=").strip()
        if not new_token:
            raise RuntimeError("Empty token input. Aborted.")
        token = new_token
        env["MYVESSEL_BEARER_TOKEN"] = token
        save_env(ENV_PATH, env)
        print(f"Saved new token to {ENV_PATH}. Retrying missing vessel query...")

    add_df = pd.DataFrame(all_rows)
    if add_df.empty:
        print("Vessel DB update: no rows returned.")
        return

    append_df = add_df[["vesselName", "IMO", "TEU"]].copy()
    merged = pd.concat([vessels_df, append_df], ignore_index=True)
    merged["_k"] = merged["vesselName"].map(normalize_text)
    merged = merged.drop_duplicates(subset=["_k"], keep="first").drop(columns=["_k"])
    sheets["vessels"] = merged

    with pd.ExcelWriter(VESSEL_DB_XLSX, engine="openpyxl", mode="w") as writer:
        for s in xl.sheet_names:
            sheets[s].to_excel(writer, index=False, sheet_name=s)

    debug_path = VESSEL_DB_XLSX.parent / f"vessels_update_debug_{datetime.now().strftime('%y%m%d%H%M%S')}.xlsx"
    add_df.to_excel(debug_path, index=False)
    unresolved = int((add_df["_status"] != "ok").sum())
    print(f"Vessel DB updated: {len(merged)} rows in vessels sheet.")
    print(f"Vessel DB debug file: {debug_path}")
    print(f"Unresolved new vessels this round: {unresolved}")


def save_merged(voyages: pd.DataFrame, port_calls: pd.DataFrame, selected: Dict[str, Path]) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%y%m%d%H%M%S")
    output_path = OUTPUT_DIR / f"ALL_CARRIERS_MERGED_{ts}.xlsx"

    summary_rows = [
        {
            "Carrier": carrier,
            "SourceFile": path.name,
            "SourcePath": str(path),
        }
        for carrier, path in selected.items()
    ]

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

    return output_path


def main() -> None:
    voyages, port_calls, selected = load_latest_all()
    ensure_vessel_db_coverage(voyages, port_calls)
    teu_map, imo_map = load_vessel_maps(VESSEL_DB_XLSX)
    service_meta = load_service_meta_map(SERVICE_META_XLSX)
    voyages = enrich_voyages_with_ids(voyages, imo_map)
    voyages = enrich_voyages_with_teu(voyages, teu_map)
    port_calls = attach_ids_to_port_calls(port_calls, voyages, imo_map)
    port_calls = enrich_port_calls(port_calls, teu_map)
    voyages = add_ana_etd_weeknum(voyages, port_calls)
    voyages, port_calls = add_alliance_trade_columns(voyages, port_calls, service_meta)
    output = save_merged(voyages, port_calls, selected)

    print("Selected source files:")
    for carrier, path in selected.items():
        print(f"- {carrier}: {path}")
    print(f"Merged voyages: {len(voyages)}")
    print(f"Merged port calls: {len(port_calls)}")
    print(f"Merged output: {output}")


if __name__ == "__main__":
    main()
