import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from capastudy.settings import (
    MSC_QUERY_DIR as QUERY_DIR,
    MSC_SERVICE_RULES_XLSX as SERVICE_RULES_XLSX_PRIMARY,
    MSC_SERVICE_RULES_XLSX_FALLBACK as SERVICE_RULES_XLSX_FALLBACK,
)

SEARCH_URL = "https://www.msc.com/api/feature/tools/SearchSailingRoutes"
DATA_SOURCE_ID = "{E9CCBD25-6FBA-4C5C-85F6-FC4F9E5A931F}"

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

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.msc.com/en/search-a-schedule",
}


def normalize_text(value):
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().upper()


def normalize_service_name(value):
    return re.sub(r"[^A-Z0-9]", "", normalize_text(value))


def service_matches_loading(service_code, loading_service):
    service_norm = normalize_service_name(service_code)
    loading_norm = normalize_service_name(loading_service)
    if not service_norm or not loading_norm:
        return False
    return loading_norm in {service_norm, f"{service_norm}SERVICE"}


def parse_msc_datetime(date_text, hour_text=None):
    date_part = str(date_text).strip() if date_text else ""
    hour_part = str(hour_text).strip() if hour_text else ""
    if not date_part:
        return None
    cleaned = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", date_part)
    candidate = f"{cleaned} {hour_part}".strip()
    for fmt in ("%a %d %b %Y %H:%M", "%a %d %b %Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue
    return None


def format_msc_datetime(date_text, hour_text=None):
    dt = parse_msc_datetime(date_text, hour_text)
    return dt.strftime("%Y-%m-%d %H:%M") if dt else None


def format_route_datetime(route_text):
    return format_msc_datetime(route_text)


def ensure_query_dir():
    QUERY_DIR.mkdir(parents=True, exist_ok=True)


def choose_service_rules_file():
    candidates = [p for p in (SERVICE_RULES_XLSX_PRIMARY, SERVICE_RULES_XLSX_FALLBACK) if p.exists()]
    if not candidates:
        raise FileNotFoundError("No MSC service rules workbook found.")
    # Prefer the most recently updated workbook.
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def load_service_rules():
    workbook = choose_service_rules_file()
    print(f"Using service rules workbook: {workbook}")
    df = pd.read_excel(workbook)
    rules = {}
    for row in df.to_dict(orient="records"):
        service = normalize_text(row.get("SERVICE"))
        if not service:
            continue

        fallback_start_id = {
            1: row.get("START_PORT_ID"),
            2: row.get("ALT1_PORT_ID"),
            3: row.get("ALT2_PORT_ID"),
        }
        fallback_end_id = {
            1: row.get("END_PORT_ID"),
            2: row.get("END2_PORT_ID"),
            3: row.get("END3_PORT_ID"),
        }

        starts = []
        for idx in (1, 2, 3):
            name = normalize_text(row.get(f"START{idx}"))
            port_id = row.get(f"START{idx}_PORT_ID")
            if pd.isna(port_id):
                port_id = fallback_start_id.get(idx)
            port_id = int(port_id) if pd.notna(port_id) else None
            if name and port_id:
                starts.append((name, port_id))

        ends = []
        for idx in (1, 2, 3):
            name = normalize_text(row.get(f"END{idx}"))
            port_id = row.get(f"END{idx}_PORT_ID")
            if pd.isna(port_id):
                port_id = fallback_end_id.get(idx)
            port_id = int(port_id) if pd.notna(port_id) else None
            if name and port_id:
                ends.append((name, port_id))

        # Backward compatibility for old sheets (START/ALT/END columns).
        if not starts:
            for name_key, id_key in (("START", "START_PORT_ID"), ("ALT1", "ALT1_PORT_ID"), ("ALT2", "ALT2_PORT_ID")):
                name = normalize_text(row.get(name_key))
                port_id = row.get(id_key)
                port_id = int(port_id) if pd.notna(port_id) else None
                if name and port_id:
                    starts.append((name, port_id))
        if not ends:
            name = normalize_text(row.get("END"))
            port_id = row.get("END_PORT_ID")
            port_id = int(port_id) if pd.notna(port_id) else None
            if name and port_id:
                ends.append((name, port_id))

        rules[service] = {
            "starts": starts,
            "ends": ends,
        }
    return rules


def get_target_services(service_rules):
    if len(sys.argv) > 1:
        requested = [item.strip().upper() for item in sys.argv[1:] if item.strip()]
        missing = [item for item in requested if item not in service_rules]
        if missing:
            raise ValueError(f"Services not found: {', '.join(sorted(missing))}")
        return requested
    return list(service_rules.keys())


def dedupe_name_id_pairs(items):
    deduped = []
    seen = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def build_payload(from_port_id, to_port_id, from_date):
    return {
        "FromDate": from_date,
        "fromPortId": int(from_port_id),
        "toPortId": int(to_port_id),
        "language": "en",
        "dataSourceId": DATA_SOURCE_ID,
    }


def request_schedule(payload):
    last_error = None
    for attempt in range(1, 4):
        try:
            response = requests.post(SEARCH_URL, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            obj = response.json()
            if not obj.get("IsSuccess", True):
                raise RuntimeError(f"MSC response IsSuccess=false for payload {payload}")
            return obj
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(2 * attempt)
            else:
                raise last_error


def extract_route_rows(service_code, query_port_name, end_port_name, response_json):
    voyage_rows = []
    port_call_rows = []
    for sailing in response_json.get("Data") or []:
        if not isinstance(sailing, dict):
            continue
        if not service_matches_loading(service_code, sailing.get("LoadingService")):
            continue

        for route in sailing.get("Routes") or []:
            if not isinstance(route, dict):
                continue
            legs = route.get("RouteScheduleLegDetails") or []
            if not legs:
                continue
            leg = legs[0]
            vessel = leg.get("Vessel") or {}
            port_calls = leg.get("PortCalls") or []
            if not port_calls:
                continue

            port_path = [query_port_name]
            port_path.extend([pc.get("PortName") for pc in port_calls if pc.get("PortName")])
            port_path.append(end_port_name)

            voyage_rows.append(
                {
                    "LoopAbbrv": service_code,
                    "VesselCode": str(vessel.get("VesselImoCode") or ""),
                    "VesselName": vessel.get("VesselName") or route.get("VesselName"),
                    "Voyage": route.get("DepartureVoyageNo"),
                    "Direction": "W",
                    "QueryPort": query_port_name,
                    "PortCallCount": len(port_calls) + 2,
                    "FirstPort": query_port_name,
                    "LastPort": end_port_name,
                    "FirstArrDtlocAct": None,
                    "FirstDepDtlocAct": None,
                    "LastArrDtlocAct": None,
                    "LastDepDtlocAct": None,
                    "FirstArrDtlocCos": None,
                    # MSC route ETD/ETA belong to the queried OD, not portCalls[0/-1].
                    "FirstDepDtlocCos": format_route_datetime(route.get("EstimatedDepartureDate")),
                    "LastArrDtlocCos": format_route_datetime(route.get("EstimatedArrivalDate")),
                    "LastDepDtlocCos": None,
                    "PortCallPath": " > ".join(port_path),
                }
            )

            port_call_rows.append(
                {
                    "LoopAbbrv": service_code,
                    "VesselCode": str(vessel.get("VesselImoCode") or ""),
                    "VesselName": vessel.get("VesselName") or route.get("VesselName"),
                    "Voyage": route.get("DepartureVoyageNo"),
                    "QueryPort": query_port_name,
                    "PortCallSeq": 1,
                    "PortName": query_port_name,
                    "ArrDtlocAct": None,
                    "DepDtlocAct": None,
                    "ArrDtlocCos": None,
                    "DepDtlocCos": format_route_datetime(route.get("EstimatedDepartureDate")),
                    "Direction": "W",
                }
            )

            for seq, pc in enumerate(port_calls, start=2):
                port_call_rows.append(
                    {
                        "LoopAbbrv": service_code,
                        "VesselCode": str(vessel.get("VesselImoCode") or ""),
                        "VesselName": vessel.get("VesselName") or route.get("VesselName"),
                        "Voyage": route.get("DepartureVoyageNo"),
                        "QueryPort": query_port_name,
                        "PortCallSeq": seq,
                        "PortName": pc.get("PortName"),
                        "ArrDtlocAct": None,
                        "DepDtlocAct": None,
                        "ArrDtlocCos": format_msc_datetime(
                            pc.get("EstimatedArrivalDate"), pc.get("EstimatedArrivalHour")
                        ),
                        "DepDtlocCos": format_msc_datetime(
                            pc.get("EstimatedDepartureDate"), pc.get("EstimatedDepartureHour")
                        ),
                        "Direction": "W",
                    }
                )

            port_call_rows.append(
                {
                    "LoopAbbrv": service_code,
                    "VesselCode": str(vessel.get("VesselImoCode") or ""),
                    "VesselName": vessel.get("VesselName") or route.get("VesselName"),
                    "Voyage": route.get("DepartureVoyageNo"),
                    "QueryPort": query_port_name,
                    "PortCallSeq": len(port_calls) + 2,
                    "PortName": end_port_name,
                    "ArrDtlocAct": None,
                    "DepDtlocAct": None,
                    "ArrDtlocCos": format_route_datetime(route.get("EstimatedArrivalDate")),
                    "DepDtlocCos": None,
                    "Direction": "W",
                }
            )
    return voyage_rows, port_call_rows


def dedupe_voyages(voyage_rows):
    def score(row):
        port_call_count = row.get("PortCallCount") or 0
        last_arrival = parse_msc_datetime(row.get("LastArrDtlocCos"))
        path_len = len(row.get("PortCallPath") or "")
        return (
            int(port_call_count),
            last_arrival or datetime.min,
            path_len,
        )

    unique = {}
    for row in voyage_rows:
        key = (row.get("LoopAbbrv"), row.get("VesselCode"), row.get("Voyage"))
        if key not in unique:
            unique[key] = dict(row)
            continue

        existing = unique[key]
        existing_ports = set(filter(None, str(existing.get("QueryPort", "")).split(" | ")))
        current_ports = set(filter(None, str(row.get("QueryPort", "")).split(" | ")))

        if score(row) > score(existing):
            merged = dict(row)
        else:
            merged = dict(existing)

        merged["QueryPort"] = " | ".join(sorted(existing_ports | current_ports))
        unique[key] = merged
    rows = list(unique.values())
    rows.sort(
        key=lambda x: (
            x.get("LoopAbbrv") or "",
            x.get("FirstDepDtlocCos") or "",
            x.get("VesselCode") or "",
            x.get("Voyage") or "",
        )
    )
    return rows


def dedupe_port_calls(port_call_rows, voyage_rows):
    selected_start_ports = {
        (row.get("LoopAbbrv"), row.get("VesselCode"), row.get("Voyage")): row.get("FirstPort")
        for row in voyage_rows
    }
    unique = {}
    for row in port_call_rows:
        voyage_key = (row.get("LoopAbbrv"), row.get("VesselCode"), row.get("Voyage"))
        if voyage_key not in selected_start_ports:
            continue
        if row.get("QueryPort") != selected_start_ports[voyage_key]:
            continue
        key = (
            row.get("LoopAbbrv"),
            row.get("VesselCode"),
            row.get("Voyage"),
            row.get("PortName"),
            row.get("ArrDtlocCos"),
            row.get("DepDtlocCos"),
        )
        if key not in unique:
            unique[key] = row
            continue
        existing_ports = set(filter(None, str(unique[key].get("QueryPort", "")).split(" | ")))
        current_ports = set(filter(None, str(row.get("QueryPort", "")).split(" | ")))
        merged = dict(unique[key])
        merged["QueryPort"] = " | ".join(sorted(existing_ports | current_ports))
        unique[key] = merged

    grouped = {}
    for row in unique.values():
        group_key = (row.get("LoopAbbrv"), row.get("VesselCode"), row.get("Voyage"))
        grouped.setdefault(group_key, []).append(row)

    rows = []
    for _, group in grouped.items():
        group.sort(key=lambda x: (x.get("ArrDtlocCos") or x.get("DepDtlocCos") or "", x.get("PortName") or ""))
        for seq, row in enumerate(group, start=1):
            merged = dict(row)
            merged["PortCallSeq"] = seq
            rows.append(merged)
    rows.sort(key=lambda x: (x.get("LoopAbbrv") or "", x.get("VesselCode") or "", x.get("Voyage") or "", x.get("PortCallSeq") or 0))
    return rows


def save_detail(voyage_rows, port_call_rows):
    ensure_query_dir()
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    output_path = QUERY_DIR / f"MSC_FETCH_BATCH_DETAIL_{timestamp}.xlsx"
    df_voyages = pd.DataFrame(voyage_rows).reindex(columns=VOYAGE_COLUMNS)
    df_port_calls = pd.DataFrame(port_call_rows).reindex(columns=PORT_CALL_COLUMNS)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_voyages.to_excel(writer, index=False, sheet_name="Total Voyages")
        df_port_calls.to_excel(writer, index=False, sheet_name="Total PortCalls")
    return str(output_path)


def process_service(service_code, service_rule, from_date):
    all_voyages = []
    all_port_calls = []

    starts = dedupe_name_id_pairs(service_rule.get("starts", []))
    ends = dedupe_name_id_pairs(service_rule.get("ends", []))
    if not starts or not ends:
        return [], []

    for start_name, start_id in starts:
        for end_name, end_id in ends:
            payload = build_payload(start_id, end_id, from_date)
            response_json = request_schedule(payload)
            voyage_rows, port_call_rows = extract_route_rows(
                service_code,
                start_name,
                end_name,
                response_json,
            )
            all_voyages.extend(voyage_rows)
            all_port_calls.extend(port_call_rows)

    deduped_voyages = dedupe_voyages(all_voyages)
    deduped_port_calls = dedupe_port_calls(all_port_calls, deduped_voyages)
    return deduped_voyages, deduped_port_calls


def main():
    service_rules = load_service_rules()
    target_services = get_target_services(service_rules)
    from_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Services to process: {target_services}")
    print(f"FromDate: {from_date}")

    batch_voyages = []
    batch_port_calls = []
    for service_code in target_services:
        print(f"Target service: {service_code}")
        voyages, port_calls = process_service(service_code, service_rules[service_code], from_date)
        print(f"Retained voyages: {len(voyages)}")
        print(f"Retained port calls: {len(port_calls)}")
        batch_voyages.extend(voyages)
        batch_port_calls.extend(port_calls)

    detail_path = save_detail(batch_voyages, batch_port_calls)
    print(f"Batch detail tables saved: {detail_path}")


if __name__ == "__main__":
    main()

