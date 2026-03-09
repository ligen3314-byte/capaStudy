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
INPUT_XLSX = BASE_DIR / "vessels.xlsx"
ENV_PATH = BASE_DIR / ".env"

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


def normalize_name(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).upper().strip()
    return re.sub(r"\s+", "", text)


def to_int_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def walk_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_dicts(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from walk_dicts(item)


def find_candidate_dicts(payload: Any) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for node in walk_dicts(payload):
        lower_keys = {k.lower() for k in node.keys()}
        if lower_keys & {
            "vesselname",
            "shipname",
            "name",
            "imo",
            "imono",
            "mmsi",
            "vesselmmsi",
        }:
            candidates.append(node)
    return candidates


def get_first(node: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in node and node[key] not in (None, ""):
            return node[key]
    lower_map = {k.lower(): k for k in node.keys()}
    for key in keys:
        real = lower_map.get(key.lower())
        if real and node[real] not in (None, ""):
            return node[real]
    return None


def choose_best_fuzzy_match(query_name: str, candidates: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    q = normalize_name(query_name)
    if not q or not candidates:
        return None, "no_candidates"

    scored: List[Tuple[int, Dict[str, Any]]] = []
    for c in candidates:
        name = get_first(c, ["vesselName", "shipName", "name", "vslName", "cnName", "enName"]) or ""
        n = normalize_name(name)
        score = 0
        if n == q:
            score = 300
        elif q in n:
            score = 200
        elif n in q and n:
            score = 150

        imo = to_int_or_none(get_first(c, ["imo", "imoNo", "vesselImo", "vesselIMO"]))
        mmsi = to_int_or_none(get_first(c, ["mmsi", "vesselMmsi", "vesselMMSI"]))
        if imo:
            score += 20
        if mmsi:
            score += 30
        scored.append((score, c))

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best = scored[0]
    if best_score <= 0:
        return best, "weak_match"
    if best_score >= 300:
        return best, "exact"
    return best, "fuzzy"


def request_api1(session: requests.Session, headers: Dict[str, str], vessel_name: str, record_num: int, timeout: int) -> Any:
    payload = {"kw": vessel_name, "recordNum": record_num}
    resp = session.post(API1_URL, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def request_api2(session: requests.Session, headers: Dict[str, str], mmsi: int, timeout: int) -> Any:
    resp = session.get(API2_URL, headers=headers, params={"mmsi": str(mmsi)}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def extract_imo_mmsi_from_candidate(candidate: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], str]:
    matched_name = str(
        get_first(candidate, ["vesselName", "shipName", "name", "vslName", "cnName", "enName"]) or ""
    ).strip()
    imo = to_int_or_none(get_first(candidate, ["imo", "imoNo", "vesselImo", "vesselIMO"]))
    mmsi = to_int_or_none(get_first(candidate, ["mmsi", "vesselMmsi", "vesselMMSI"]))
    return imo, mmsi, matched_name


def extract_teu(detail_payload: Any) -> Optional[int]:
    key_priority = [
        "teu",
        "teucapacity",
        "teu_capacity",
        "nominalteu",
        "nominal_teu",
        "capacityteu",
        "containercapacity",
        "container_capacity",
    ]

    best: Optional[int] = None
    for node in walk_dicts(detail_payload):
        for k, v in node.items():
            k_norm = re.sub(r"[^a-z0-9]", "", str(k).lower())
            if k_norm in key_priority or ("teu" in k_norm and "rate" not in k_norm):
                num = to_int_or_none(v)
                if num and num > 0:
                    if best is None or num > best:
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


def main() -> None:
    env = load_env(ENV_PATH)
    token = env.get("MYVESSEL_BEARER_TOKEN", "").strip()
    referer = env.get("MYVESSEL_REFERER", "https://market.myvessel.cn/").strip()
    record_num = int(env.get("MYVESSEL_RECORD_NUM", "10") or 10)
    timeout = int(env.get("MYVESSEL_TIMEOUT", "30") or 30)

    if not token:
        raise RuntimeError("MYVESSEL_BEARER_TOKEN is empty. Please fill it in .env first.")
    if not INPUT_XLSX.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_XLSX}")

    df = pd.read_excel(INPUT_XLSX)
    if "vesselName" not in df.columns:
        raise ValueError("vessels.xlsx missing required column: vesselName")

    session = requests.Session()
    headers = build_headers(token, referer)

    result_rows: List[Dict[str, Any]] = []
    debug_rows: List[Dict[str, Any]] = []

    vessel_names = [str(v).strip() for v in df["vesselName"].tolist() if str(v).strip()]
    total = len(vessel_names)

    for idx, vessel_name in enumerate(vessel_names, start=1):
        print(f"[{idx}/{total}] {vessel_name}")
        imo: Optional[int] = None
        mmsi: Optional[int] = None
        teu: Optional[int] = None
        status = "ok"
        note = ""
        matched_name = ""

        try:
            fuzzy_payload = request_api1(session, headers, vessel_name, record_num, timeout)
            candidates = find_candidate_dicts(fuzzy_payload)
            best, match_type = choose_best_fuzzy_match(vessel_name, candidates)
            if best is None:
                status = "api1_no_match"
                note = "no candidate dict"
            else:
                imo, mmsi, matched_name = extract_imo_mmsi_from_candidate(best)
                if not mmsi:
                    status = "api1_no_mmsi"
                    note = f"match_type={match_type}"
                else:
                    detail_payload = request_api2(session, headers, mmsi, timeout)
                    teu = extract_teu(detail_payload)
                    if teu is None:
                        status = "api2_no_teu"
                        note = f"match_type={match_type}"
        except requests.HTTPError as exc:
            status = "http_error"
            note = f"{exc.response.status_code}: {exc.response.text[:200]}"
        except Exception as exc:
            status = "error"
            note = str(exc)

        result_rows.append(
            {
                "vesselName": vessel_name,
                "IMO": imo,
                "TEU": teu,
            }
        )
        debug_rows.append(
            {
                "vesselName": vessel_name,
                "matchedName": matched_name,
                "IMO": imo,
                "MMSI": mmsi,
                "TEU": teu,
                "status": status,
                "note": note,
            }
        )

        time.sleep(0.01)

    ts = datetime.now().strftime("%y%m%d%H%M%S")
    out_main = BASE_DIR / f"vessels_db_{ts}.xlsx"
    out_debug = BASE_DIR / f"vessels_db_debug_{ts}.xlsx"

    pd.DataFrame(result_rows, columns=["vesselName", "IMO", "TEU"]).to_excel(out_main, index=False)
    pd.DataFrame(debug_rows).to_excel(out_debug, index=False)

    print(f"Saved main output: {out_main}")
    print(f"Saved debug output: {out_debug}")


if __name__ == "__main__":
    main()
