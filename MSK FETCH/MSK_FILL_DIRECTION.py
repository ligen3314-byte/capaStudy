from datetime import datetime, timedelta
from pathlib import Path
import time

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_XLSX = SCRIPT_DIR / "msk_service_port_seq_filled.xlsx"
OUTPUT_XLSX = SCRIPT_DIR / "msk_service_port_seq_direction.xlsx"
QUERY_URL = "https://api.maersk.com/routing-unified/routing/routings-queries"

HEADERS = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9",
    "akamai-bm-telemetry": "09E8A1671BD5D5DE73E633F6D421E62D...",
    "api-version": "1",
    "consumer-key": "uXe7bxTHLY0yY0e8jnS6kotShkLuAAqG",
    "content-type": "application/json",
    "origin": "https://www.maersk.com",
    "referer": "https://www.maersk.com/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}


def build_payload(start_geo_id, end_geo_id):
    today = datetime.now().date()
    latest = today + timedelta(days=56)
    return {
        "requestType": "DATED_SCHEDULES",
        "includeFutureSchedules": True,
        "routingCondition": "PREFERRED",
        "exportServiceType": "CY",
        "importServiceType": "CY",
        "brandCode": "MSL",
        "startLocation": {
            "dataObject": "CITY",
            "alternativeCodes": [{"alternativeCodeType": "GEO_ID", "alternativeCode": start_geo_id}],
        },
        "endLocation": {
            "dataObject": "CITY",
            "alternativeCodes": [{"alternativeCodeType": "GEO_ID", "alternativeCode": end_geo_id}],
        },
        "timeRange": {
            "routingsBasedOn": "DEPARTURE_DATE",
            "earliestTime": today.isoformat(),
            "latestTime": latest.isoformat(),
        },
        "cargo": {"cargoType": "DRY", "isTemperatureControlRequired": False},
        "carriage": {"vessel": {"flagCountryCode": ""}},
        "equipment": {
            "equipmentSizeCode": "20",
            "equipmentTypeCode": "DRY",
            "isEmpty": False,
            "isShipperOwned": False,
        },
        "IsUseOfInternetMarkedRoutesOnly": False,
    }


def request_adjacent(start_geo_id, end_geo_id):
    payload = build_payload(start_geo_id, end_geo_id)
    last_error = None
    for attempt in range(1, 4):
        try:
            response = requests.post(QUERY_URL, headers=HEADERS, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            return data.get("routings") if isinstance(data, dict) else []
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt * 2)
            else:
                raise last_error


def pick_routing(service_code, routings):
    if not isinstance(routings, list):
        return None

    exact = []
    for route in routings:
        if not isinstance(route, dict):
            continue
        legs = route.get("routingLegs") or []
        if not legs:
            continue
        leg = legs[0]
        carriage = leg.get("carriage") or {}
        start = carriage.get("vesselPortCallStart") or {}
        end = carriage.get("vesselPortCallEnd") or {}
        start_service = ((start.get("departureService") or {}).get("serviceName")) or ""
        end_service = ((end.get("arrivalService") or {}).get("serviceName")) or ""
        direction = route.get("routeCodeDirection") or start.get("departureDirection") or end.get("arrivalDirection")
        candidate = {
            "direction": direction,
            "route_code": route.get("routeCode"),
            "matched_service": start_service or end_service,
            "start_service": start_service,
            "end_service": end_service,
            "voyage": start.get("departureVoyageNumber") or end.get("arrivalVoyageNumber"),
        }
        if service_code in {start_service, end_service}:
            exact.append(candidate)

    if exact:
        return exact[0]
    return None


def fill_directions(df):
    result_rows = []
    cache = {}

    for service, group in df.groupby("service", sort=False):
        group = group.sort_values("service_seq").copy()
        group["pair_direction"] = None
        group["pair_route_code"] = None
        group["pair_service_name"] = None
        group["pair_voyage"] = None
        group["direction"] = None
        group["is_westbound_end"] = False

        indices = list(group.index)
        for i in range(len(indices) - 1):
            idx = indices[i]
            next_idx = indices[i + 1]
            start_geo = group.at[idx, "geo_id"]
            end_geo = group.at[next_idx, "geo_id"]
            key = (start_geo, end_geo)
            if key not in cache:
                routings = request_adjacent(start_geo, end_geo)
                cache[key] = pick_routing(service, routings)
            picked = cache[key]
            if not picked:
                continue

            group.at[idx, "pair_direction"] = picked.get("direction")
            group.at[idx, "pair_route_code"] = picked.get("route_code")
            group.at[idx, "pair_service_name"] = picked.get("matched_service")
            group.at[idx, "pair_voyage"] = picked.get("voyage")

        for i, idx in enumerate(indices):
            out_direction = group.at[idx, "pair_direction"]
            if pd.notna(out_direction):
                group.at[idx, "direction"] = out_direction
                continue
            if i > 0:
                prev_idx = indices[i - 1]
                prev_direction = group.at[prev_idx, "pair_direction"]
                if pd.notna(prev_direction):
                    group.at[idx, "direction"] = prev_direction

        directions = list(group["direction"])
        for i, idx in enumerate(indices[:-1]):
            current = directions[i]
            nxt = directions[i + 1]
            if current == "W" and nxt != "W":
                group.at[idx, "is_westbound_end"] = True

        result_rows.append(group)

    return pd.concat(result_rows, ignore_index=True)


def main():
    df = pd.read_excel(INPUT_XLSX)
    filled = fill_directions(df)
    filled.to_excel(OUTPUT_XLSX, index=False)
    print(f"Direction table saved: {OUTPUT_XLSX}")
    print(filled[['service', 'service_seq', 'port', 'direction', 'pair_service_name', 'pair_route_code', 'is_westbound_end']].to_string(index=False))


if __name__ == "__main__":
    main()
