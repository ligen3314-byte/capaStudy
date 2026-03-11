from __future__ import annotations

import json
import string
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
OUTPUT_JSON = BASE_DIR / "csl_vessels.json"
OUTPUT_XLSX = BASE_DIR / "csl_vessels.xlsx"

URL = "https://elines.coscoshipping.com/ebbase/public/general/findVesselByPrefix"


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


def extract_names(payload: Any) -> List[str]:
    names: List[str] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, str):
                n = item.strip()
                if n:
                    names.append(n)
            elif isinstance(item, dict):
                for key in ["vesselName", "name", "label", "value", "description", "chineseDescription"]:
                    value = item.get(key)
                    if isinstance(value, str) and value.strip():
                        names.append(value.strip())
                        break
    elif isinstance(payload, dict):
        # Typical wrappers: {"data":{"content":[...]}} / {"result":[...]} / nested dicts.
        for key in ["data", "result", "rows", "list", "content"]:
            if key in payload:
                names.extend(extract_names(payload[key]))
        if not names:
            for value in payload.values():
                names.extend(extract_names(value))
    return names


def build_headers(cookie: str) -> Dict[str, str]:
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
        "language": "zh_CN",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sys": "eb",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def main() -> None:
    env = load_env(ENV_PATH)
    cookie = env.get("CSL_COOKIE", "").strip()
    delay_sec = float(env.get("CSL_PREFIX_DELAY_SECONDS", "0.3") or 0.3)
    timeout = int(env.get("CSL_PREFIX_TIMEOUT", "30") or 30)

    session = requests.Session()
    headers = build_headers(cookie)

    all_names: List[str] = []
    by_prefix: Dict[str, List[str]] = {}
    ts = int(time.time() * 1000)

    for i, prefix in enumerate(string.ascii_lowercase, start=1):
        req_ts = ts + i
        params = {
            "prefix": prefix,
            "timestamp": str(req_ts),
        }
        local_headers = dict(headers)
        local_headers["X-Client-Timestamp"] = str(req_ts + 1)
        try:
            resp = session.get(URL, params=params, headers=local_headers, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            names = sorted(set(extract_names(payload)))
            by_prefix[prefix] = names
            all_names.extend(names)
            print(f"[{i:02d}/26] prefix={prefix} -> {len(names)} names")
        except Exception as exc:
            by_prefix[prefix] = []
            print(f"[{i:02d}/26] prefix={prefix} -> failed: {type(exc).__name__}: {exc}")
        time.sleep(delay_sec)

    unique_names = sorted(set(n.strip() for n in all_names if n and n.strip()))
    out = {
        "source": URL,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_unique": len(unique_names),
        "vessels": [{"Name": n} for n in unique_names],
        "by_prefix": by_prefix,
    }

    OUTPUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame({"Name": unique_names}).to_excel(OUTPUT_XLSX, index=False)

    print(f"Unique vessels: {len(unique_names)}")
    print(f"Saved JSON: {OUTPUT_JSON}")
    print(f"Saved XLSX: {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
