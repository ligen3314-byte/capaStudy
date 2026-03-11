from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

SEARCH_URL = "https://elines.coscoshipping.com/ebbase/public/vesselParticulars/search"
DETAIL_URL = "https://elines.coscoshipping.com/ebbase/public/general/findVesselByCode"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Process one CSL vessel group and output result file.")
    p.add_argument("--group-file", required=True, help="JSON file with vessel names list.")
    p.add_argument("--group-index", type=int, required=True, help="1-based group index.")
    p.add_argument("--result-dir", default=str(BASE_DIR / "csl_group_results"), help="Result directory.")
    p.add_argument("--non-interactive", action="store_true", help="Do not ask for new cookie when invalid.")
    p.add_argument("--log-file", default="", help="Optional log file path (line-buffered append).")
    return p.parse_args()


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


def normalize_name(v: Any) -> str:
    if v is None:
        return ""
    return re.sub(r"\s+", "", str(v).strip().upper())


def to_int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    text = str(v).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def walk_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for x in obj.values():
            yield from walk_dicts(x)
    elif isinstance(obj, list):
        for x in obj:
            yield from walk_dicts(x)


def build_headers(cookie: str, client_ts: int) -> Dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://elines.coscoshipping.com/ebusiness/vesselParticulars/vesselParticularsVesselName",
        "User-Agent": "Mozilla/5.0",
        "X-Client-Timestamp": str(client_ts),
        "language": "zh_CN",
        "sys": "eb",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def parse_json_response(resp: requests.Response) -> Any:
    ctype = (resp.headers.get("content-type") or "").lower()
    if "json" not in ctype:
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
            c = data.get("content")
            if isinstance(c, list):
                return [x for x in c if isinstance(x, dict)]
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
            key = re.sub(r"[^a-z0-9]", "", str(k).lower())
            if key in {"totteucap", "teucapacity", "teu", "nominalteu", "capacityteu"} or (
                "teu" in key and "rate" not in key
            ):
                num = to_int_or_none(v)
                if num and (best is None or num > best):
                    best = num
    return best


def extract_imo(payload: Any) -> Optional[int]:
    keys = {"imo", "imono", "vesselimo", "lloydsnumber", "lloydsno"}
    for d in walk_dicts(payload):
        for k, v in d.items():
            key = re.sub(r"[^a-z0-9]", "", str(k).lower())
            if key in keys:
                num = to_int_or_none(v)
                if num:
                    return num
    return None


def main() -> None:
    args = parse_args()
    group_file = Path(args.group_file)
    result_dir = Path(args.result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)

    env = load_env(ENV_PATH)
    cookie = env.get("CSL_COOKIE", "").strip()
    timeout = int(env.get("CSL_TIMEOUT", "30") or 30)
    delay_sec = float(env.get("CSL_DELAY_SECONDS", "0.12") or 0.12)

    if not cookie:
        raise RuntimeError("CSL_COOKIE is empty in vessels/.env.")
    if not group_file.exists():
        raise FileNotFoundError(f"group file not found: {group_file}")

    names = json.loads(group_file.read_text(encoding="utf-8"))
    if not isinstance(names, list):
        raise ValueError("group file should contain a JSON list of vessel names")

    session = requests.Session()
    rows: List[Dict[str, Any]] = []
    total = len(names)
    log_handle = None
    if args.log_file:
        log_path = Path(args.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_handle = log_path.open("a", encoding="utf-8", buffering=1)

    def safe_print(msg: str) -> None:
        try:
            print(msg)
        except UnicodeEncodeError:
            enc = sys.stdout.encoding or "utf-8"
            fixed = msg.encode(enc, errors="backslashreplace").decode(enc, errors="ignore")
            print(fixed)
            msg = fixed
        if log_handle is not None:
            log_handle.write(msg + "\n")
            log_handle.flush()

    for idx, vessel_name in enumerate(names, start=1):
        vessel_name = str(vessel_name).strip()
        if not vessel_name:
            continue
        status = "ok"
        note = ""
        vessel_code = None
        imo = None
        teu = None

        while True:
            try:
                ts = int(time.time() * 1000)
                h1 = build_headers(cookie, ts + 1)
                q1 = {
                    "pageSize": "20",
                    "pageNum": "1",
                    "status": "shipsName",
                    "shipName": vessel_name,
                    "timestamp": str(ts),
                }
                r1 = session.get(SEARCH_URL, params=q1, headers=h1, timeout=timeout)
                p1 = parse_json_response(r1)
                if not response_ok(p1):
                    if args.non_interactive:
                        status = "cookie_invalid"
                        note = "search response code != 200"
                        break
                    safe_print(f"group {args.group_index}: cookie invalid, input new CSL_COOKIE")
                    new_cookie = input("CSL_COOKIE=").strip()
                    if not new_cookie:
                        status = "cookie_invalid"
                        note = "empty new cookie"
                        break
                    cookie = new_cookie
                    env["CSL_COOKIE"] = cookie
                    save_env(ENV_PATH, env)
                    continue

                best = choose_best_row(vessel_name, extract_search_rows(p1))
                if not best:
                    status = "search_no_match"
                    note = "no matched row"
                    break
                vessel_code = str(best.get("vesselCode") or best.get("code") or "").strip()
                if not vessel_code:
                    status = "search_no_vessel_code"
                    note = "missing vesselCode"
                    break

                ts2 = int(time.time() * 1000)
                h2 = build_headers(cookie, ts2 + 1)
                r2 = session.get(DETAIL_URL, params={"code": vessel_code, "timestamp": str(ts2)}, headers=h2, timeout=timeout)
                p2 = parse_json_response(r2)
                if not response_ok(p2):
                    if args.non_interactive:
                        status = "cookie_invalid"
                        note = "detail response code != 200"
                        break
                    safe_print(f"group {args.group_index}: cookie invalid, input new CSL_COOKIE")
                    new_cookie = input("CSL_COOKIE=").strip()
                    if not new_cookie:
                        status = "cookie_invalid"
                        note = "empty new cookie"
                        break
                    cookie = new_cookie
                    env["CSL_COOKIE"] = cookie
                    save_env(ENV_PATH, env)
                    continue

                teu = extract_teu(p2)
                imo = extract_imo(p2)
                if teu is None:
                    status = "detail_no_teu"
                    note = f"vesselCode={vessel_code}"
                break
            except Exception as exc:
                status = "error"
                note = str(exc)
                break

        rows.append(
            {
                "vesselName": vessel_name,
                "vesselCode": vessel_code,
                "IMO": imo,
                "TEU": teu,
                "status": status,
                "note": note,
            }
        )
        if idx % 10 == 0 or status != "ok":
            safe_print(
                f"[group {args.group_index} {idx}/{total}] {vessel_name} -> "
                f"code={vessel_code} IMO={imo} TEU={teu} status={status}"
            )
        time.sleep(delay_sec)

    ts_out = datetime.now().strftime("%y%m%d%H%M%S")
    xlsx_path = result_dir / f"csl_group_{args.group_index:02d}_result_{ts_out}.xlsx"
    json_path = result_dir / f"csl_group_{args.group_index:02d}_result_{ts_out}.json"
    df = pd.DataFrame(rows)
    df.to_excel(xlsx_path, index=False)
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    safe_print(f"group {args.group_index}: saved xlsx -> {xlsx_path}")
    safe_print(f"group {args.group_index}: saved json -> {json_path}")
    if log_handle is not None:
        log_handle.close()


if __name__ == "__main__":
    main()
