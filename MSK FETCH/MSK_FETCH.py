import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
QUERY_DIR = SCRIPT_DIR / 'msk_query'
PORTS_XLSX = SCRIPT_DIR / 'msk_ports.xlsx'
PORT_CALLS_URL = 'https://api.maersk.com/synergy/schedules/port-calls'

VOYAGE_COLUMNS = [
    'LoopAbbrv',
    'VesselCode',
    'VesselName',
    'Voyage',
    'Direction',
    'PortCallCount',
    'FirstPort',
    'LastPort',
    'FirstArrDtlocAct',
    'FirstDepDtlocAct',
    'LastArrDtlocAct',
    'LastDepDtlocAct',
    'FirstArrDtlocCos',
    'FirstDepDtlocCos',
    'LastArrDtlocCos',
    'LastDepDtlocCos',
    'PortCallPath',
]

PORT_CALL_COLUMNS = [
    'LoopAbbrv',
    'VesselCode',
    'VesselName',
    'Voyage',
    'PortCallSeq',
    'PortName',
    'ArrDtlocAct',
    'DepDtlocAct',
    'ArrDtlocCos',
    'DepDtlocCos',
    'Direction',
]

HEADERS = {
    'accept': 'application/json',
    'accept-language': 'zh-CN,zh;q=0.9',
    'consumer-key': 'uXe7bxTHLY0yY0e8jnS6kotShkLuAAqG',
    'origin': 'https://www.maersk.com',
    'referer': 'https://www.maersk.com/',
    'user-agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/145.0.0.0 Safari/537.36'
    ),
}


def normalize_text(value):
    if value is None or pd.isna(value):
        return ''
    return str(value).replace('\xa0', ' ').strip().upper()



def guess_direction(voyage):
    text = (voyage or '').strip().upper()
    m = re.search(r'([A-Z])$', text)
    return m.group(1) if m else None



def format_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).strftime('%Y-%m-%d %H:%M')
    except ValueError:
        return str(value)



def ensure_query_dir():
    QUERY_DIR.mkdir(parents=True, exist_ok=True)



def load_ports():
    df = pd.read_excel(PORTS_XLSX, sheet_name='ports')
    required = {'city', 'geoid'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f'Missing required columns in {PORTS_XLSX.name}: {sorted(missing)}')
    df['city'] = df['city'].astype(str).str.replace('\xa0', ' ', regex=False).str.strip()
    df['geoid'] = df['geoid'].astype(str).str.strip()
    df = df[df['city'].ne('') & df['geoid'].ne('')].drop_duplicates(subset=['city', 'geoid']).reset_index(drop=True)
    return df



def load_allowed_services():
    df = pd.read_excel(PORTS_XLSX, sheet_name='services', header=None)
    values = []
    for item in df.iloc[:, 0].tolist():
        norm = normalize_text(item)
        if norm:
            values.append(norm)
    return sorted(set(values))



def get_target_ports(port_df):
    if len(sys.argv) <= 1:
        return port_df
    requested = {normalize_text(arg) for arg in sys.argv[1:] if normalize_text(arg)}
    result = port_df[port_df['city'].map(normalize_text).isin(requested)].copy()
    missing = requested - set(result['city'].map(normalize_text))
    if missing:
        raise ValueError(f'Ports not found: {", ".join(sorted(missing))}')
    return result.reset_index(drop=True)



def build_params(port_code, from_date, to_date):
    return {
        'portCode': port_code,
        'fromDate': from_date,
        'toDate': to_date,
        'carrierCodes': 'MAEU',
    }



def request_port_calls(port_code, from_date, to_date):
    params = build_params(port_code, from_date, to_date)
    last_error = None
    for attempt in range(1, 4):
        try:
            response = requests.get(PORT_CALLS_URL, headers=HEADERS, params=params, timeout=60)
            response.raise_for_status()
            obj = response.json()
            return obj.get('portCalls') if isinstance(obj, dict) else []
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(attempt * 2)
            else:
                raise last_error



def choose_matched_service_and_voyage(item, allowed_services):
    allowed_services = {normalize_text(x) for x in allowed_services}
    candidates = []

    arrival_service_name = normalize_text(item.get('arrivalServiceName'))
    arrival_service_code = normalize_text(item.get('arrivalServiceCode'))
    arrival_voyage = (item.get('arrivalVoyageNumber') or '').strip()
    arrival_direction = guess_direction(arrival_voyage)

    departure_service_name = normalize_text(item.get('departureServiceName'))
    departure_service_code = normalize_text(item.get('departureServiceCode'))
    departure_voyage = (item.get('departureVoyageNumber') or '').strip()
    departure_direction = guess_direction(departure_voyage)

    if {arrival_service_name, arrival_service_code} & allowed_services and arrival_voyage:
        candidates.append(
            {
                'service': item.get('arrivalServiceName') or item.get('arrivalServiceCode'),
                'voyage': arrival_voyage,
                'direction': arrival_direction,
                'matched_side': 'arrival',
            }
        )

    if {departure_service_name, departure_service_code} & allowed_services and departure_voyage:
        candidates.append(
            {
                'service': item.get('departureServiceName') or item.get('departureServiceCode'),
                'voyage': departure_voyage,
                'direction': departure_direction,
                'matched_side': 'departure',
            }
        )

    westbound = [c for c in candidates if c['direction'] == 'W']
    if westbound:
        for side in ('departure', 'arrival'):
            for candidate in westbound:
                if candidate['matched_side'] == side:
                    return candidate

    return None



def parse_port_call_rows(port_df, from_date, to_date, allowed_services):
    rows = []
    allowed_services = {normalize_text(x) for x in allowed_services}
    for rec in port_df.to_dict(orient='records'):
        city = rec['city']
        geoid = rec['geoid']
        print(f'Querying {city} ({geoid})')
        for item in request_port_calls(geoid, from_date, to_date) or []:
            matched = choose_matched_service_and_voyage(item, allowed_services)
            if not matched:
                continue
            rows.append(
                {
                    'LoopAbbrv': matched['service'],
                    'VesselCode': (item.get('vesselMaerskCode') or '').strip(),
                    'VesselName': item.get('vesselName'),
                    'Voyage': matched['voyage'],
                    'PortCallSeq': None,
                    'PortName': city,
                    'ArrDtlocAct': None,
                    'DepDtlocAct': None,
                    'ArrDtlocCos': format_iso_datetime(item.get('arrivalTime')),
                    'DepDtlocCos': format_iso_datetime(item.get('departureTime')),
                    'Direction': matched['direction'],
                    'MatchedSide': matched['matched_side'],
                    'QueryGeoId': geoid,
                    'ArrivalVoyageNumber': item.get('arrivalVoyageNumber'),
                    'DepartureVoyageNumber': item.get('departureVoyageNumber'),
                    'ArrivalServiceName': item.get('arrivalServiceName'),
                    'ArrivalServiceCode': item.get('arrivalServiceCode'),
                    'DepartureServiceName': item.get('departureServiceName'),
                    'DepartureServiceCode': item.get('departureServiceCode'),
                    'MarineContainerTerminalName': item.get('marineContainerTerminalName'),
                    'MarineContainerTerminalRKSTCode': item.get('marineContainerTerminalRKSTCode'),
                    'MarineContainerTerminalGeoCode': item.get('marineContainerTerminalGeoCode'),
                    'VesselIMONumber': item.get('vesselIMONumber'),
                }
            )
        time.sleep(1)
    return rows


def dedupe_port_calls(rows):
    unique = {}
    for row in rows:
        key = (
            row.get('LoopAbbrv'),
            row.get('VesselCode'),
            row.get('Voyage'),
            row.get('PortName'),
            row.get('ArrDtlocCos'),
            row.get('DepDtlocCos'),
        )
        if key not in unique:
            unique[key] = row
    result = list(unique.values())
    grouped = {}
    for row in result:
        key = (row.get('LoopAbbrv'), row.get('VesselCode'), row.get('Voyage'))
        grouped.setdefault(key, []).append(row)

    sequenced = []
    for _, group in grouped.items():
        group.sort(key=lambda x: (x.get('ArrDtlocCos') or x.get('DepDtlocCos') or '', x.get('PortName') or ''))
        for seq, row in enumerate(group, start=1):
            merged = dict(row)
            merged['PortCallSeq'] = seq
            sequenced.append(merged)

    sequenced.sort(key=lambda x: (x.get('LoopAbbrv') or '', x.get('Voyage') or '', x.get('PortCallSeq') or 0, x.get('VesselCode') or ''))
    return sequenced



def build_voyage_rows(port_call_rows):
    grouped = {}
    for row in port_call_rows:
        key = (row.get('LoopAbbrv'), row.get('VesselCode'), row.get('Voyage'))
        grouped.setdefault(key, []).append(row)

    voyage_rows = []
    for _, group in grouped.items():
        group.sort(key=lambda x: (x.get('PortCallSeq') or 0, x.get('ArrDtlocCos') or '', x.get('DepDtlocCos') or ''))
        first = group[0]
        last = group[-1]
        voyage_rows.append(
            {
                'LoopAbbrv': first.get('LoopAbbrv'),
                'VesselCode': first.get('VesselCode'),
                'VesselName': first.get('VesselName'),
                'Voyage': first.get('Voyage'),
                'Direction': first.get('Direction'),
                'PortCallCount': len(group),
                'FirstPort': first.get('PortName'),
                'LastPort': last.get('PortName'),
                'FirstArrDtlocAct': None,
                'FirstDepDtlocAct': None,
                'LastArrDtlocAct': None,
                'LastDepDtlocAct': None,
                'FirstArrDtlocCos': first.get('ArrDtlocCos'),
                'FirstDepDtlocCos': first.get('DepDtlocCos'),
                'LastArrDtlocCos': last.get('ArrDtlocCos'),
                'LastDepDtlocCos': last.get('DepDtlocCos'),
                'PortCallPath': ' > '.join([item.get('PortName') for item in group if item.get('PortName')]),
                'ArrivalServiceName': first.get('ArrivalServiceName'),
                'ArrivalServiceCode': first.get('ArrivalServiceCode'),
                'DepartureServiceName': first.get('DepartureServiceName'),
                'DepartureServiceCode': first.get('DepartureServiceCode'),
            }
        )

    voyage_rows.sort(key=lambda x: (x.get('LoopAbbrv') or '', x.get('FirstDepDtlocCos') or '', x.get('VesselCode') or '', x.get('Voyage') or ''))
    return voyage_rows



def save_detail(voyage_rows, port_call_rows):
    ensure_query_dir()
    timestamp = datetime.now().strftime('%y%m%d%H%M%S')
    output_path = QUERY_DIR / f'MSK_FETCH_BATCH_DETAIL_{timestamp}.xlsx'
    df_voyages = pd.DataFrame(voyage_rows).reindex(columns=VOYAGE_COLUMNS)
    df_port_calls = pd.DataFrame(port_call_rows).reindex(columns=PORT_CALL_COLUMNS)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df_voyages.to_excel(writer, index=False, sheet_name='Total Voyages')
        df_port_calls.to_excel(writer, index=False, sheet_name='Total PortCalls')

    return output_path



def main():
    port_df = load_ports()
    target_ports = get_target_ports(port_df)
    allowed_services = load_allowed_services()
    from_date = date.today().isoformat()
    to_date = (date.today() + timedelta(days=42)).isoformat()

    print(f'Ports to process: {target_ports["city"].tolist()}')
    print(f'Allowed services: {allowed_services}')
    print(f'fromDate: {from_date}')
    print(f'toDate: {to_date}')

    port_call_rows = parse_port_call_rows(target_ports, from_date, to_date, allowed_services)
    port_call_rows = dedupe_port_calls(port_call_rows)
    voyage_rows = build_voyage_rows(port_call_rows)

    print(f'Retained voyages: {len(voyage_rows)}')
    print(f'Retained port calls: {len(port_call_rows)}')
    detail_path = save_detail(voyage_rows, port_call_rows)
    print(f'Batch detail tables saved: {detail_path}')


if __name__ == '__main__':
    main()
