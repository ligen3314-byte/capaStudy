import json
import re
from datetime import datetime

import requests

try:
    import pandas as pd
except ImportError as exc:
    raise ImportError("缺少依赖 pandas/openpyxl，请先执行: pip install pandas openpyxl") from exc

# 1. 请求地址
url = "https://www.msc.com/api/feature/tools/SearchSailingRoutes"

# 2. 模拟最精简的 Headers
headers = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.msc.com/en/search-a-schedule",
}

# 3. 请求 Payload 参数
payload = {
    "FromDate": "2026-02-28",
    "fromPortId": 444,
    "toPortId": 208,
    "language": "en",
    "dataSourceId": "{E9CCBD25-6FBA-4C5C-85F6-FC4F9E5A931F}",
}


def sanitize_filename(name):
    cleaned = re.sub(r'[\\/:*?"<>|]', "_", str(name)).strip()
    return cleaned or "UNKNOWN"


def parse_voyage_rows(response_json):
    data = []
    if isinstance(response_json, dict):
        data = response_json.get("Data") or response_json.get("data") or []

    if not isinstance(data, list):
        return []

    rows = []
    for sailing in data:
        if not isinstance(sailing, dict):
            continue

        base = {
            "RouteHeaderId": sailing.get("RouteHeaderId"),
            "PortOfLoadId": sailing.get("PortOfLoadId"),
            "PortOfLoad": sailing.get("PortOfLoad"),
            "PortOfLoadUnCode": sailing.get("PortOfLoadUnCode"),
            "PortOfDischargeId": sailing.get("PortOfDischargeId"),
            "PortOfDischarge": sailing.get("PortOfDischarge"),
            "PortOfDischargeUnCode": sailing.get("PortOfDischargeUnCode"),
            "TransitTime": sailing.get("TransitTime"),
            "TransitTimeHours": sailing.get("TransitTimeHours"),
            "CO2FootPrint": sailing.get("CO2FootPrint"),
            "EstimatedDepartureTime": sailing.get("EstimatedDepartureTime"),
            "EstimatedDepartureTimeFormatted": sailing.get("EstimatedDepartureTimeFormatted"),
            "LoadingService": sailing.get("LoadingService"),
            "IsDirectRoute": sailing.get("IsDirectRoute"),
            "RoutingType": sailing.get("RoutingType"),
        }

        routes = sailing.get("Routes")
        if not isinstance(routes, list) or not routes:
            rows.append(base)
            continue

        for idx, route in enumerate(routes, start=1):
            if not isinstance(route, dict):
                row = dict(base)
                row["RouteIndex"] = idx
                rows.append(row)
                continue

            cutoffs = route.get("CutOffs") if isinstance(route.get("CutOffs"), dict) else {}
            leg_details = route.get("RouteScheduleLegDetails")
            leg_details = leg_details if isinstance(leg_details, list) else []
            first_leg = leg_details[0] if leg_details else {}
            last_leg = leg_details[-1] if leg_details else {}
            vessel = first_leg.get("Vessel") if isinstance(first_leg.get("Vessel"), dict) else {}

            row = dict(base)
            row.update(
                {
                    "RouteIndex": idx,
                    "RouteVesselName": route.get("VesselName"),
                    "DepartureVoyageNo": route.get("DepartureVoyageNo"),
                    "RouteEstimatedDepartureDate": route.get("EstimatedDepartureDate"),
                    "RouteEstimatedDepartureDateFormatted": route.get(
                        "EstimatedDepartureDateFormatted"
                    ),
                    "RouteEstimatedArrivalDate": route.get("EstimatedArrivalDate"),
                    "RouteEstimatedArrivalDateFormatted": route.get(
                        "EstimatedArrivalDateFormatted"
                    ),
                    "RouteTotalTransitTime": route.get("TotalTransitTime"),
                    "RouteTotalTransitTimeHours": route.get("TotalTransitTimeHours"),
                    "RouteCO2FootPrint": route.get("CO2FootPrint"),
                    "ContainerYardCutOffDate": cutoffs.get("ContainerYardCutOffDate"),
                    "ReeferCutOffDate": cutoffs.get("ReeferCutOffDate"),
                    "DangerousCargoCutOffDate": cutoffs.get("DangerousCargoCutOffDate"),
                    "ShippingInstructionsCutOffDate": cutoffs.get(
                        "ShippingInstructionsCutOffDate"
                    ),
                    "VerifiedGrossMassCutOffDate": cutoffs.get("VerifiedGrossMassCutOffDate"),
                    "LegCount": len(leg_details),
                    "TransshipmentCount": max(len(leg_details) - 1, 0),
                    "FirstLegDeparturePort": first_leg.get("DeparturePortName"),
                    "FirstLegDepartureTime": first_leg.get("EstimatedDepartureTime"),
                    "LastLegArrivalPort": last_leg.get("ArrivalPortName"),
                    "LastLegArrivalTime": last_leg.get("EstimatedArrivalTime"),
                    "VesselName": vessel.get("VesselName") or route.get("VesselName"),
                    "VesselImoCode": vessel.get("VesselImoCode"),
                    "VesselBuiltYear": vessel.get("VesselBuiltYear"),
                    "VesselFlag": vessel.get("VesselFlag"),
                }
            )
            rows.append(row)

    return rows


def save_json_to_excel(response_json, payload_data):
    rows = parse_voyage_rows(response_json)
    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.json_normalize(response_json, sep=".") if isinstance(response_json, dict) else pd.DataFrame()

    from_port = payload_data.get("fromPortId")
    to_port = payload_data.get("toPortId")
    if rows:
        from_port = rows[0].get("PortOfLoad") or from_port
        to_port = rows[0].get("PortOfDischarge") or to_port

    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    file_name = (
        f"MSC_FETCH_{sanitize_filename(from_port)}_{sanitize_filename(to_port)}_{timestamp}.xlsx"
    )
    df.to_excel(file_name, index=False)
    return file_name


try:
    # 发送 POST 请求
    response = requests.post(url, headers=headers, json=payload, timeout=15)

    print(f"状态码: {response.status_code}")

    if response.status_code == 200:
        print("请求成功")
        response_json = response.json()
        print(json.dumps(response_json, indent=4, ensure_ascii=False)[:500])

        output_file = save_json_to_excel(response_json, payload)
        print(f"Excel 已保存: {output_file}")
    else:
        print("请求失败，服务端可能要求 Cookie 或存在其他防护")
        print(f"响应内容: {response.text}")

except Exception as e:
    print(f"发生错误: {e}")
