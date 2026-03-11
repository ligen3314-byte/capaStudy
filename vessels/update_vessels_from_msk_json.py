from __future__ import annotations

import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
VESSEL_DB_XLSX = BASE_DIR / "vessels_db.xlsx"
MSK_VESSELS_JSON = BASE_DIR / "msk_vessels.json"

API1_URL = "https://market.myvessel.cn/sdc/v1/mkt/vessels/fuzzy"
API2_URL = "https://market.myvessel.cn/sdc/v1/mkt/vessels/detail/mmsi"


def load_env(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not env_path.exists():
        return env
    for raw in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def save_env(path: Path, env: Dict[str, str]) -> None:
    path.write_text("".join(f"{k}={v}\n" for k, v in env.items()), encoding="utf-8")


def normalize_name(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return re.sub(r"\s+", "", str(value).strip().upper())


def to_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def walk_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


def get_first(d: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in d and d[key] not in (None, ""):
            return d[key]
    lower_map = {k.lower(): k for k in d.keys()}
    for key in keys:
        real = lower_map.get(key.lower())
        if real and d[real] not in (None, ""):
            return d[real]
    return None


def find_candidates(payload: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for d in walk_dicts(payload):
        lower_keys = {k.lower() for k in d.keys()}
        if lower_keys & {"vesselname", "shipname", "name", "imo", "imono", "mmsi", "vesselmmsi"}:
            out.append(d)
    return out


def choose_best_candidate(query_name: str, candidates: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    q = normalize_name(query_name)
    best: Optional[Dict[str, Any]] = None
    best_score = -1
    best_type = "no_match"
    for c in candidates:
        raw_name = get_first(c, ["vesselName", "shipName", "name", "vslName", "cnName", "enName"]) or ""
        n = normalize_name(raw_name)
        score = 0
        match_type = "weak"
        if n == q:
            score = 300
            match_type = "exact"
        elif q and q in n:
            score = 200
            match_type = "fuzzy"
        elif n and n in q:
            score = 150
            match_type = "contains"

        if to_int_or_none(get_first(c, ["mmsi", "vesselMmsi", "vesselMMSI"])):
            score += 40
        if to_int_or_none(get_first(c, ["imo", "imoNo", "vesselImo", "vesselIMO"])):
            score += 20

        if score > best_score:
            best = c
            best_score = score
            best_type = match_type
    return best, best_type


def extract_teu(detail_payload: Any) -> Optional[int]:
    best: Optional[int] = None
    for d in walk_dicts(detail_payload):
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


def build_headers(token: str, referer: str) -> Dict[str, str]:
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


def is_unauthorized(resp_json: Any, status_code: int) -> bool:
    if status_code in {401, 403}:
        return True
    if isinstance(resp_json, dict) and resp_json.get("code") == 401:
        return True
    return False


def load_msk_names_and_imo(path: Path) -> Tuple[List[str], Dict[str, Optional[int]]]:
    if not path.exists():
        raise FileNotFoundError(f"MSK vessels file not found: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    vessels = obj.get("vessels", []) if isinstance(obj, dict) else []
    by_norm: Dict[str, Tuple[str, Optional[int]]] = {}
    for v in vessels:
        name = str(v.get("vesselName") or "").strip()
        if not name:
            continue
        imo = to_int_or_none(v.get("vesselIMONumber"))
        k = normalize_name(name)
        if k not in by_norm:
            by_norm[k] = (name, imo)
    names = [x[0] for x in by_norm.values()]
    imo_map = {x[0]: x[1] for x in by_norm.values()}
    names.sort()
    return names, imo_map


def write_workbook(
    target_xlsx: Path,
    base_sheet_order: List[str],
    sheets: Dict[str, pd.DataFrame],
) -> None:
    with pd.ExcelWriter(target_xlsx, engine="openpyxl", mode="w") as writer:
        for s in base_sheet_order:
            if s in sheets:
                sheets[s].to_excel(writer, index=False, sheet_name=s)
        for s in sheets:
            if s not in base_sheet_order:
                sheets[s].to_excel(writer, index=False, sheet_name=s)


def main() -> None:
    if not VESSEL_DB_XLSX.exists():
        raise FileNotFoundError(f"vessels_db.xlsx not found: {VESSEL_DB_XLSX}")

    env = load_env(ENV_PATH)
    token = env.get("MYVESSEL_BEARER_TOKEN", "").strip()
    referer = env.get("MYVESSEL_REFERER", "https://market.myvessel.cn/").strip()
    record_num = int(env.get("MYVESSEL_RECORD_NUM", "10") or 10)
    timeout = int(env.get("MYVESSEL_TIMEOUT", "30") or 30)
    sleep_sec = float(env.get("MYVESSEL_SLEEP_SECONDS", "0.12") or 0.12)

    if not token:
        raise RuntimeError("MYVESSEL_BEARER_TOKEN is empty. Please fill .env first.")

    all_names, msk_imo_map = load_msk_names_and_imo(MSK_VESSELS_JSON)
    print(f"MSK unique vessel names: {len(all_names)}")

    xl = pd.ExcelFile(VESSEL_DB_XLSX)
    sheets = {s: pd.read_excel(VESSEL_DB_XLSX, sheet_name=s) for s in xl.sheet_names}
    vessels_df = sheets.get("vessels", pd.DataFrame(columns=["vesselName", "IMO", "TEU"])).copy()
    if not {"vesselName", "IMO", "TEU"}.issubset(set(vessels_df.columns)):
        raise ValueError("vessels sheet must include: vesselName, IMO, TEU")

    existing = {}
    for row in vessels_df.to_dict(orient="records"):
        n = str(row.get("vesselName") or "").strip()
        if not n:
            continue
        existing[normalize_name(n)] = {
            "vesselName": n,
            "IMO": to_int_or_none(row.get("IMO")),
            "TEU": to_int_or_none(row.get("TEU")),
        }

    headers = build_headers(token, referer)
    session = requests.Session()
    debug_rows = []

    total = len(all_names)
    checkpoint_every = 200
    progress_every = 10

    def checkpoint_write(processed_count: int) -> None:
        out_vessels = pd.DataFrame(sorted(existing.values(), key=lambda x: normalize_name(x["vesselName"])))
        out_vessels = out_vessels.reindex(columns=["vesselName", "IMO", "TEU"])
        sheets["vessels"] = out_vessels
        unresolved = [
            {"VesselName": r["vesselName"]}
            for r in debug_rows
            if r.get("status") not in {"ok", "skip_existing_complete"} or not to_int_or_none(r.get("TEU"))
        ]
        sheets["new_vessels"] = (
            pd.DataFrame(unresolved).drop_duplicates(subset=["VesselName"]).reset_index(drop=True)
            if unresolved
            else pd.DataFrame(columns=["VesselName"])
        )
        write_workbook(VESSEL_DB_XLSX, xl.sheet_names, sheets)
        print(f"[checkpoint] saved workbook at {processed_count}/{total}: {VESSEL_DB_XLSX}")

    for idx, vessel_name in enumerate(all_names, start=1):
        key = normalize_name(vessel_name)
        cur = existing.get(key, {"vesselName": vessel_name, "IMO": None, "TEU": None})
        if cur.get("IMO") and cur.get("TEU"):
            debug_rows.append(
                {"vesselName": vessel_name, "status": "skip_existing_complete", "IMO": cur["IMO"], "TEU": cur["TEU"]}
            )
            continue

        status = "ok"
        note = ""
        imo = cur.get("IMO") or msk_imo_map.get(vessel_name)
        teu = cur.get("TEU")
        mmsi = None

        # Retry current vessel if token expired and user provides new token.
        while True:
            try:
                r1 = session.post(
                    API1_URL,
                    headers=headers,
                    json={"kw": vessel_name, "recordNum": record_num},
                    timeout=timeout,
                )
                p1 = r1.json()
                if is_unauthorized(p1, r1.status_code):
                    print("MyVessel token expired. Please input a new token to continue.")
                    new_token = input("MYVESSEL_BEARER_TOKEN=").strip()
                    if not new_token:
                        raise RuntimeError("Empty token input. Aborted by user.")
                    token = new_token
                    env["MYVESSEL_BEARER_TOKEN"] = token
                    save_env(ENV_PATH, env)
                    headers = build_headers(token, referer)
                    continue

                r1.raise_for_status()
                cands = find_candidates(p1)
                best, match_type = choose_best_candidate(vessel_name, cands)
                if best is None:
                    status = "api1_no_match"
                    note = "no candidate"
                    break

                imo = imo or to_int_or_none(get_first(best, ["imo", "imoNo", "vesselImo", "vesselIMO"]))
                mmsi = to_int_or_none(get_first(best, ["mmsi", "vesselMmsi", "vesselMMSI"]))
                if not mmsi:
                    status = "api1_no_mmsi"
                    note = f"match={match_type}"
                    break

                r2 = session.get(
                    API2_URL,
                    headers={k: v for k, v in headers.items() if k != "Content-Type"},
                    params={"mmsi": str(mmsi)},
                    timeout=timeout,
                )
                p2 = r2.json()
                if is_unauthorized(p2, r2.status_code):
                    print("MyVessel token expired. Please input a new token to continue.")
                    new_token = input("MYVESSEL_BEARER_TOKEN=").strip()
                    if not new_token:
                        raise RuntimeError("Empty token input. Aborted by user.")
                    token = new_token
                    env["MYVESSEL_BEARER_TOKEN"] = token
                    save_env(ENV_PATH, env)
                    headers = build_headers(token, referer)
                    continue

                r2.raise_for_status()
                teu = teu or extract_teu(p2)
                if teu is None:
                    status = "api2_no_teu"
                    note = f"match={match_type}"
                break
            except requests.HTTPError as exc:
                status = "http_error"
                note = f"{exc.response.status_code}" if exc.response is not None else "http"
                break
            except Exception as exc:
                status = "error"
                note = str(exc)
                break

        existing[key] = {"vesselName": vessel_name, "IMO": imo, "TEU": teu}
        debug_rows.append({"vesselName": vessel_name, "status": status, "IMO": imo, "MMSI": mmsi, "TEU": teu, "note": note})
        if idx % progress_every == 0 or status not in {"ok", "skip_existing_complete"}:
            print(f"[{idx}/{total}] {vessel_name} -> IMO={imo} MMSI={mmsi} TEU={teu} status={status}")

        if idx % checkpoint_every == 0:
            checkpoint_write(idx)
        time.sleep(sleep_sec)

    checkpoint_write(total)

    ts = datetime.now().strftime("%y%m%d%H%M%S")
    debug_path = BASE_DIR / f"vessels_update_debug_msk_{ts}.xlsx"
    pd.DataFrame(debug_rows).to_excel(debug_path, index=False)

    ok_count = sum(1 for r in debug_rows if r.get("status") in {"ok", "skip_existing_complete"})
    print(f"Processed vessels: {len(debug_rows)}")
    print(f"Successful/kept vessels: {ok_count}")
    print(f"Updated DB file: {VESSEL_DB_XLSX}")
    print(f"Debug file: {debug_path}")


if __name__ == "__main__":
    main()
