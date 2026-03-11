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
CSL_VESSELS_JSON = BASE_DIR / "csl_vessels.json"

SEARCH_URL = "https://elines.coscoshipping.com/ebbase/public/vesselParticulars/search"
DETAIL_URL = "https://elines.coscoshipping.com/ebbase/public/general/findVesselByCode"


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


def build_headers(cookie: str, client_ts: int) -> Dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://elines.coscoshipping.com/ebusiness/vesselParticulars/vesselParticularsVesselName",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        ),
        "X-Client-Timestamp": str(client_ts),
        "language": "zh_CN",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sys": "eb",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def parse_json_response(resp: requests.Response) -> Any:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "json" not in ctype:
        # Sometimes session invalid returns html
        raise ValueError(f"Unexpected content-type: {ctype}")
    return resp.json()


def response_ok(payload: Any) -> bool:
    if isinstance(payload, dict):
        code = str(payload.get("code", "")).strip()
        if code and code != "200":
            return False
    return True


def extract_search_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data", payload)
        if isinstance(data, dict):
            content = data.get("content")
            if isinstance(content, list):
                return [x for x in content if isinstance(x, dict)]
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    return []


def choose_best_row(query_name: str, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    q = normalize_name(query_name)
    best = None
    best_score = -1
    for r in rows:
        candidate = (
            r.get("shipsName")
            or r.get("shipName")
            or r.get("vesselName")
            or r.get("name")
            or r.get("description")
            or ""
        )
        n = normalize_name(candidate)
        score = 0
        if n == q:
            score = 300
        elif q and q in n:
            score = 200
        elif n and n in q:
            score = 150
        if r.get("vesselCode") or r.get("code"):
            score += 30
        if score > best_score:
            best = r
            best_score = score
    return best


def extract_teu(payload: Any) -> Optional[int]:
    best = None
    for d in walk_dicts(payload):
        for k, v in d.items():
            key_norm = re.sub(r"[^a-z0-9]", "", str(k).lower())
            if key_norm in {"totteucap", "teucapacity", "teu", "nominalteu", "capacityteu"} or (
                "teu" in key_norm and "rate" not in key_norm
            ):
                num = to_int_or_none(v)
                if num and (best is None or num > best):
                    best = num
    return best


def extract_imo(payload: Any) -> Optional[int]:
    for d in walk_dicts(payload):
        for key in ["imo", "imoNo", "vesselImo", "lloydsNumber", "lloydsNo"]:
            for k, v in d.items():
                if re.sub(r"[^a-z0-9]", "", k.lower()) == re.sub(r"[^a-z0-9]", "", key.lower()):
                    num = to_int_or_none(v)
                    if num:
                        return num
    return None


def load_csl_names(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"CSL vessels file not found: {path}")
    obj = json.loads(path.read_text(encoding="utf-8"))
    vessels = obj.get("vessels", []) if isinstance(obj, dict) else []
    names: Dict[str, str] = {}
    for item in vessels:
        if isinstance(item, dict):
            name = str(item.get("Name") or item.get("name") or item.get("vesselName") or "").strip()
        else:
            name = str(item).strip()
        if not name:
            continue
        key = normalize_name(name)
        if key and key not in names:
            names[key] = name
    out = sorted(names.values(), key=normalize_name)
    return out


def write_workbook(target_xlsx: Path, base_sheet_order: List[str], sheets: Dict[str, pd.DataFrame]) -> None:
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
    cookie = env.get("CSL_COOKIE", "").strip()
    timeout = int(env.get("CSL_TIMEOUT", "30") or 30)
    delay_sec = float(env.get("CSL_DELAY_SECONDS", "0.12") or 0.12)
    progress_every = int(env.get("CSL_PROGRESS_EVERY", "10") or 10)
    checkpoint_every = int(env.get("CSL_CHECKPOINT_EVERY", "200") or 200)
    page_size = int(env.get("CSL_SEARCH_PAGE_SIZE", "20") or 20)

    if not cookie:
        raise RuntimeError("CSL_COOKIE is empty. Please fill it in vessels/.env first.")

    all_names = load_csl_names(CSL_VESSELS_JSON)
    print(f"CSL vessel names to process: {len(all_names)}")

    xl = pd.ExcelFile(VESSEL_DB_XLSX)
    sheets = {s: pd.read_excel(VESSEL_DB_XLSX, sheet_name=s) for s in xl.sheet_names}
    vessels_df = sheets.get("vessels", pd.DataFrame(columns=["vesselName", "IMO", "TEU"])).copy()
    if not {"vesselName", "IMO", "TEU"}.issubset(set(vessels_df.columns)):
        raise ValueError("vessels sheet must include: vesselName, IMO, TEU")

    existing = {}
    for row in vessels_df.to_dict(orient="records"):
        name = str(row.get("vesselName") or "").strip()
        if not name:
            continue
        existing[normalize_name(name)] = {
            "vesselName": name,
            "IMO": to_int_or_none(row.get("IMO")),
            "TEU": to_int_or_none(row.get("TEU")),
        }

    session = requests.Session()
    debug_rows = []
    total = len(all_names)

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
        if cur.get("TEU"):
            debug_rows.append(
                {"vesselName": vessel_name, "status": "skip_existing_complete", "IMO": cur.get("IMO"), "TEU": cur.get("TEU")}
            )
            if idx % progress_every == 0:
                print(f"[{idx}/{total}] {vessel_name} -> skip_existing_complete")
            if idx % checkpoint_every == 0:
                checkpoint_write(idx)
            continue

        status = "ok"
        note = ""
        vessel_code = None
        imo = cur.get("IMO")
        teu = cur.get("TEU")

        while True:
            try:
                now_ts = int(time.time() * 1000)
                headers = build_headers(cookie, now_ts + 1)
                search_params = {
                    "pageSize": str(page_size),
                    "pageNum": "1",
                    "status": "shipsName",
                    "shipName": vessel_name,
                    "timestamp": str(now_ts),
                }
                r1 = session.get(SEARCH_URL, params=search_params, headers=headers, timeout=timeout)
                p1 = parse_json_response(r1)
                if not response_ok(p1):
                    # likely cookie/session expired
                    print(f"CSL session may be invalid for vessel '{vessel_name}'. Please input new CSL_COOKIE.")
                    new_cookie = input("CSL_COOKIE=").strip()
                    if not new_cookie:
                        raise RuntimeError("Empty CSL_COOKIE input. Aborted by user.")
                    cookie = new_cookie
                    env["CSL_COOKIE"] = cookie
                    save_env(ENV_PATH, env)
                    continue

                rows = extract_search_rows(p1)
                best = choose_best_row(vessel_name, rows)
                if not best:
                    status = "search_no_match"
                    note = "no row in search response"
                    break

                vessel_code = str(best.get("vesselCode") or best.get("code") or "").strip()
                if not vessel_code:
                    status = "search_no_vessel_code"
                    note = "missing vesselCode"
                    break

                now_ts2 = int(time.time() * 1000)
                headers2 = build_headers(cookie, now_ts2 + 1)
                r2 = session.get(
                    DETAIL_URL,
                    params={"code": vessel_code, "timestamp": str(now_ts2)},
                    headers=headers2,
                    timeout=timeout,
                )
                p2 = parse_json_response(r2)
                if not response_ok(p2):
                    print(f"CSL session may be invalid for vessel '{vessel_name}'. Please input new CSL_COOKIE.")
                    new_cookie = input("CSL_COOKIE=").strip()
                    if not new_cookie:
                        raise RuntimeError("Empty CSL_COOKIE input. Aborted by user.")
                    cookie = new_cookie
                    env["CSL_COOKIE"] = cookie
                    save_env(ENV_PATH, env)
                    continue

                teu = teu or extract_teu(p2)
                imo = imo or extract_imo(p2)
                if teu is None:
                    status = "detail_no_teu"
                    note = f"vesselCode={vessel_code}"
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
        debug_rows.append(
            {
                "vesselName": vessel_name,
                "vesselCode": vessel_code,
                "status": status,
                "IMO": imo,
                "TEU": teu,
                "note": note,
            }
        )

        if idx % progress_every == 0 or status not in {"ok", "skip_existing_complete"}:
            print(f"[{idx}/{total}] {vessel_name} -> code={vessel_code} IMO={imo} TEU={teu} status={status}")
        if idx % checkpoint_every == 0:
            checkpoint_write(idx)
        time.sleep(delay_sec)

    checkpoint_write(total)
    ts = datetime.now().strftime("%y%m%d%H%M%S")
    debug_path = BASE_DIR / f"vessels_update_debug_csl_{ts}.xlsx"
    pd.DataFrame(debug_rows).to_excel(debug_path, index=False)

    ok_count = sum(1 for r in debug_rows if r.get("status") in {"ok", "skip_existing_complete"})
    print(f"Processed vessels: {len(debug_rows)}")
    print(f"Successful/kept vessels: {ok_count}")
    print(f"Updated DB file: {VESSEL_DB_XLSX}")
    print(f"Debug file: {debug_path}")


if __name__ == "__main__":
    main()

