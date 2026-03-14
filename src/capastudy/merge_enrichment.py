from __future__ import annotations

import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from capastudy.merge_common import (
    ANA_PORT_PRIORITY,
    excel_weeknum_type16,
    get_first,
    load_env,
    normalize_port_key,
    normalize_text,
    save_env,
    to_int_or_none,
    walk_dicts,
)
from capastudy.settings import VESSEL_DB_XLSX, VESSEL_ENV_PATH


ENV_PATH = VESSEL_ENV_PATH
MYVESSEL_API1_URL = "https://market.myvessel.cn/sdc/v1/mkt/vessels/fuzzy"
MYVESSEL_API2_URL = "https://market.myvessel.cn/sdc/v1/mkt/vessels/detail/mmsi"


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
            if ("teu" in key_norm and "rate" not in key_norm) or key_norm in {"teucapacity", "nominalteu", "capacityteu", "containercapacity"}:
                num = to_int_or_none(v)
                if num and (best is None or num > best):
                    best = num
    return best


def build_myvessel_headers(token: str, referer: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Referer": referer,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "accept": "application/json",
        "Content-Type": "application/json",
    }


def fetch_missing_vessel_rows(names: List[str], token: str, referer: str, record_num: int = 10, timeout: int = 30) -> Tuple[List[Dict[str, object]], bool]:
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
            r1 = session.post(MYVESSEL_API1_URL, headers=headers, json={"kw": str(name), "recordNum": record_num}, timeout=timeout)
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

        rows.append({"vesselName": str(name).strip(), "IMO": imo, "TEU": teu, "_mmsi": mmsi, "_status": status})
        print(f"[{idx}/{len(names)}] {name} -> IMO={imo} MMSI={mmsi} TEU={teu} status={status}")
        if unauthorized:
            break
        time.sleep(0.15)
    return rows, unauthorized


def add_alliance_trade_columns(voyages: pd.DataFrame, port_calls: pd.DataFrame, service_meta: Dict[str, Tuple[str, str]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
    p = p.sort_values(["voyage_id", "_prio", "_seq", "_dep_dt"], kind="stable")
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
    return "|".join([normalize_text(carrier) or "NA", normalize_text(loop_abbrv) or "NA", normalize_text(vessel_key) or "NA", normalize_text(voyage) or "NA"])


def enrich_voyages_with_ids(voyages: pd.DataFrame, imo_map: Dict[str, int]) -> pd.DataFrame:
    out = voyages.copy()
    out["IMO"] = out["VesselName"].map(lambda x: imo_map.get(normalize_text(x))).astype("Int64")
    out["VesselKey"] = out.apply(lambda r: vessel_key_from_row(r, imo_map), axis=1)
    out["_core_key"] = out.apply(lambda r: build_core_key(r.get("Carrier"), r.get("LoopAbbrv"), r.get("VesselKey"), r.get("Voyage")), axis=1)
    out["_tail_dep_dt"] = pd.to_datetime(out["LastDepDtlocCos"], errors="coerce")
    out = out.sort_values(["_core_key", "_tail_dep_dt", "SourceFile"], kind="stable").reset_index(drop=True)
    out["_cycle_no"] = out.groupby("_core_key").cumcount() + 1
    out["voyage_id"] = out.apply(lambda r: f"{r['_core_key']}|{int(r['_cycle_no']):03d}", axis=1)
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
    candidates: Dict[Tuple[str, str, str, str], List[Tuple[pd.Timestamp, str]]] = {}
    v = voyages.copy()
    v["_tail_dep_dt"] = pd.to_datetime(v["LastDepDtlocCos"], errors="coerce")
    for row in v.to_dict(orient="records"):
        base_key = (normalize_text(row.get("Carrier")), normalize_text(row.get("LoopAbbrv")), normalize_text(row.get("VesselKey")), normalize_text(row.get("Voyage")))
        dep_dt = pd.to_datetime(row.get("_tail_dep_dt"), errors="coerce")
        candidates.setdefault(base_key, []).append((dep_dt, str(row.get("voyage_id"))))
    for key in list(candidates.keys()):
        candidates[key].sort(key=lambda x: (pd.Timestamp.max if pd.isna(x[0]) else x[0], x[1]))
    voyage_ids: List[str] = []
    dep_series = pd.to_datetime(out["DepDtlocCos"], errors="coerce")
    for idx, row in out.iterrows():
        base_key = (normalize_text(row.get("Carrier")), normalize_text(row.get("LoopAbbrv")), normalize_text(row.get("VesselKey")), normalize_text(row.get("Voyage")))
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
        picked = min(items, key=lambda x: abs((dep_dt - x[0]).total_seconds()) if not pd.isna(x[0]) else float("inf"))
        voyage_ids.append(picked[1])
    out["voyage_id"] = voyage_ids
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
    non_interactive = str(env.get("MYVESSEL_NON_INTERACTIVE", "")).strip().lower() in {"1", "true", "yes"}
    if not non_interactive:
        non_interactive = str(os.environ.get("GITHUB_ACTIONS", "")).strip().lower() == "true"
    if not token:
        raise RuntimeError(f"MYVESSEL_BEARER_TOKEN is empty. Please fill token in {ENV_PATH} and rerun.")
    all_rows: List[Dict[str, object]] = []
    for round_idx in range(1, 3):
        rows, unauthorized = fetch_missing_vessel_rows(missing_names, token=token, referer=referer, record_num=record_num, timeout=timeout)
        all_rows.extend(rows)
        if not unauthorized:
            break
        if non_interactive:
            raise RuntimeError("TOKEN_UNAUTHORIZED: MyVessel token expired/unauthorized in non-interactive mode. Please update MYVESSEL_BEARER_TOKEN and rerun merge.")
        if round_idx >= 2:
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
