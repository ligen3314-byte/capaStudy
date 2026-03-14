from __future__ import annotations

import re
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


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

AUDIT_COLUMNS = [
    "first_seen_date",
    "last_seen_date",
    "snapshot_date",
    "updated_at",
    "is_active",
    "_row_hash",
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


def excel_weeknum_type16(dep_dt: object) -> Optional[int]:
    if dep_dt is None or (isinstance(dep_dt, float) and pd.isna(dep_dt)):
        return None
    dt = pd.to_datetime(dep_dt, errors="coerce")
    if pd.isna(dt):
        return None
    d = dt.date()
    jan1 = d.replace(month=1, day=1)
    start_weekday = 5
    offset = (jan1.weekday() - start_weekday) % 7
    week1_start = jan1 - timedelta(days=offset)
    return ((d - week1_start).days // 7) + 1


def stable_cell_to_str(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    ts = pd.to_datetime(value, errors="coerce")
    if pd.notna(ts):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(value).strip()
