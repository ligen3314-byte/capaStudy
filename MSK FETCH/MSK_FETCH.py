import json
import re
from datetime import datetime
from pathlib import Path

import requests

try:
    import pandas as pd
except ImportError as exc:
    raise ImportError("缺少依赖 pandas/openpyxl，请先执行: pip install pandas openpyxl") from exc

# 1. 目标 URL
url = "https://api.maersk.com/routing-unified/routing/routings-queries"

# 2. Headers
headers = {
    "accept": "*/*",
    "accept-language": "zh-CN,zh;q=0.9",
    "akamai-bm-telemetry": "09E8A1671BD5D5DE73E633F6D421E62D...",
    "api-version": "1",
    "consumer-key": "uXe7bxTHLY0yY0e8jnS6kotShkLuAAqG",
    "content-type": "application/json",
    "origin": "https://www.maersk.com",
    "referer": "https://www.maersk.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}

# 3. Payload（查询参数）
payload = {
    "requestType": "DATED_SCHEDULES",
    "includeFutureSchedules": True,
    "routingCondition": "PREFERRED",
    "exportServiceType": "CY",
    "importServiceType": "CY",
    "brandCode": "MSL",
    "startLocation": {
        "dataObject": "CITY",
        "alternativeCodes": [{"alternativeCodeType": "GEO_ID", "alternativeCode": "2IW9P6J7XAW72"}],
    },
    "endLocation": {
        "dataObject": "CITY",
        "alternativeCodes": [{"alternativeCodeType": "GEO_ID", "alternativeCode": "1JUKNJGWHQBNJ"}],
    },
    "timeRange": {
        "routingsBasedOn": "DEPARTURE_DATE",
        "earliestTime": "2026-02-28",
        "latestTime": "2026-04-10",
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


def sanitize_filename(name):
    cleaned = re.sub(r'[\\/:*?"<>|]', "_", str(name)).strip()
    return cleaned or "UNKNOWN"


def get_alt_code(location, code_type="GEO_ID"):
    if not isinstance(location, dict):
        return None

    for item in location.get("alternativeCodes", []):
        if isinstance(item, dict) and item.get("alternativeCodeType") == code_type:
            return item.get("alternativeCode")
    return None


def get_location_fields(port_call):
    if not isinstance(port_call, dict):
        return {
            "FacilityCode": None,
            "GeoId": None,
            "ETA": None,
            "ETD": None,
            "VoyageNumber": None,
            "Direction": None,
            "CarrierCode": None,
            "ServiceCode": None,
            "ServiceName": None,
        }

    location = (port_call.get("location") or {}).get("facility") or {}
    arrival_service = port_call.get("arrivalService") or {}

    return {
        "FacilityCode": location.get("facilityCode"),
        "GeoId": get_alt_code(location),
        "ETA": port_call.get("estimatedTimeOfArrival"),
        "ETD": port_call.get("estimatedTimeOfDeparture"),
        "VoyageNumber": port_call.get("departureVoyageNumber") or port_call.get("arrivalVoyageNumber"),
        "Direction": port_call.get("departureDirection") or port_call.get("arrivalDirection"),
        "CarrierCode": port_call.get("carrierCode"),
        "ServiceCode": arrival_service.get("serviceCode"),
        "ServiceName": arrival_service.get("serviceName"),
    }


def parse_voyage_rows(response_json):
    routings = response_json.get("routings") if isinstance(response_json, dict) else []
    if not isinstance(routings, list):
        return []

    rows = []
    for route in routings:
        if not isinstance(route, dict):
            continue

        legs = route.get("routingLegs") if isinstance(route.get("routingLegs"), list) else []
        first_leg = legs[0] if legs else {}
        last_leg = legs[-1] if legs else {}

        first_carriage = first_leg.get("carriage") if isinstance(first_leg, dict) else {}
        last_carriage = last_leg.get("carriage") if isinstance(last_leg, dict) else {}

        start_info = get_location_fields(first_carriage.get("vesselPortCallStart") if isinstance(first_carriage, dict) else {})
        end_info = get_location_fields(last_carriage.get("vesselPortCallEnd") if isinstance(last_carriage, dict) else {})

        vessel = first_carriage.get("vessel") if isinstance(first_carriage, dict) and isinstance(first_carriage.get("vessel"), dict) else {}

        rows.append(
            {
                "RoutingIdentifier": route.get("routingIdentifier"),
                "RouteId": route.get("routeId"),
                "RouteCode": route.get("routeCode"),
                "RouteCodeDirection": route.get("routeCodeDirection"),
                "RouteSequenceNumber": route.get("routeSequenceNumber"),
                "Priority": route.get("priority"),
                "EstimatedTransitTime": route.get("estimatedTransitTime"),
                "SourceSystem": route.get("sourceSystem"),
                "RouteProviders": ",".join(route.get("routeProviders", [])) if isinstance(route.get("routeProviders"), list) else None,
                "LegCount": len(legs),
                "TransshipmentCount": max(len(legs) - 1, 0),
                "VesselName": vessel.get("vesselName"),
                "VesselMaerskCode": vessel.get("vesselMaerskCode"),
                "VesselFlagCountryCode": vessel.get("flagCountryCode"),
                "StartFacilityCode": start_info["FacilityCode"],
                "StartGeoId": start_info["GeoId"],
                "StartETA": start_info["ETA"],
                "StartETD": start_info["ETD"],
                "StartVoyageNumber": start_info["VoyageNumber"],
                "StartDirection": start_info["Direction"],
                "StartCarrierCode": start_info["CarrierCode"],
                "StartServiceCode": start_info["ServiceCode"],
                "StartServiceName": start_info["ServiceName"],
                "EndFacilityCode": end_info["FacilityCode"],
                "EndGeoId": end_info["GeoId"],
                "EndETA": end_info["ETA"],
                "EndETD": end_info["ETD"],
                "EndVoyageNumber": end_info["VoyageNumber"],
                "EndDirection": end_info["Direction"],
                "EndCarrierCode": end_info["CarrierCode"],
                "EndServiceCode": end_info["ServiceCode"],
                "EndServiceName": end_info["ServiceName"],
            }
        )
    return rows


def parse_leg_rows(response_json):
    routings = response_json.get("routings") if isinstance(response_json, dict) else []
    if not isinstance(routings, list):
        return []

    rows = []
    for route in routings:
        if not isinstance(route, dict):
            continue

        route_id = route.get("routingIdentifier")
        legs = route.get("routingLegs") if isinstance(route.get("routingLegs"), list) else []
        for idx, leg in enumerate(legs, start=1):
            if not isinstance(leg, dict):
                continue

            carriage = leg.get("carriage") if isinstance(leg.get("carriage"), dict) else {}
            start_info = get_location_fields(carriage.get("vesselPortCallStart"))
            end_info = get_location_fields(carriage.get("vesselPortCallEnd"))
            vessel = carriage.get("vessel") if isinstance(carriage.get("vessel"), dict) else {}
            transport_mode = leg.get("transportMode") if isinstance(leg.get("transportMode"), dict) else {}

            rows.append(
                {
                    "RoutingIdentifier": route_id,
                    "LegIndex": idx,
                    "RoutingLegIdentifier": leg.get("routingLegIdentifier"),
                    "JourneyType": leg.get("journeyType"),
                    "ShipmentRoutingType": leg.get("shipmentRoutingType"),
                    "TransportModeCode": transport_mode.get("transportModeCode"),
                    "LegEstimatedTransitTime": leg.get("estimatedTransitTime"),
                    "CarriageType": carriage.get("carriageType"),
                    "VesselName": vessel.get("vesselName"),
                    "VesselMaerskCode": vessel.get("vesselMaerskCode"),
                    "StartFacilityCode": start_info["FacilityCode"],
                    "StartGeoId": start_info["GeoId"],
                    "StartETA": start_info["ETA"],
                    "StartETD": start_info["ETD"],
                    "EndFacilityCode": end_info["FacilityCode"],
                    "EndGeoId": end_info["GeoId"],
                    "EndETA": end_info["ETA"],
                    "EndETD": end_info["ETD"],
                    "DepartureVoyageNumber": start_info["VoyageNumber"],
                    "ArrivalVoyageNumber": end_info["VoyageNumber"],
                }
            )
    return rows


def get_filename_ports(voyage_rows, payload_data):
    if voyage_rows:
        from_port = voyage_rows[0].get("StartFacilityCode") or voyage_rows[0].get("StartGeoId")
        to_port = voyage_rows[0].get("EndFacilityCode") or voyage_rows[0].get("EndGeoId")
    else:
        from_location = payload_data.get("startLocation", {})
        to_location = payload_data.get("endLocation", {})
        from_port = get_alt_code(from_location) or "FROM"
        to_port = get_alt_code(to_location) or "TO"
    return from_port, to_port


def save_to_excel(response_json, payload_data):
    voyage_rows = parse_voyage_rows(response_json)
    leg_rows = parse_leg_rows(response_json)

    df_voyages = pd.DataFrame(voyage_rows)
    df_legs = pd.DataFrame(leg_rows)

    from_port, to_port = get_filename_ports(voyage_rows, payload_data)
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    file_name = f"MSK_FETCH_{sanitize_filename(from_port)}_{sanitize_filename(to_port)}_{timestamp}.xlsx"
    output_path = Path(__file__).resolve().parent / file_name

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_voyages.to_excel(writer, index=False, sheet_name="Voyages")
        df_legs.to_excel(writer, index=False, sheet_name="Legs")

    return str(output_path), len(df_voyages), len(df_legs)


try:
    # 发送 POST 请求
    response = requests.post(url, headers=headers, json=payload, timeout=15)

    print(f"状态码: {response.status_code}")

    if response.status_code == 200:
        print("请求成功")
        response_json = response.json()

        print(json.dumps(response_json, ensure_ascii=False, indent=2)[:500])

        output_file, voyage_count, leg_count = save_to_excel(response_json, payload)
        print(f"Excel 已保存: {output_file}")
        print(f"Voyages 行数: {voyage_count}, Legs 行数: {leg_count}")
    else:
        print("请求失败，可能是 Header 过期或被风控拦截")
        print(f"错误响应: {response.text}")

except Exception as e:
    print(f"执行出错: {e}")
