import re
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from capastudy.carriers.common import (
    PORT_CALL_COLUMNS,
    VOYAGE_COLUMNS,
    choose_requested_items,
    ensure_directory,
    run_item_batch,
    save_timestamped_voyage_portcall_workbook,
)
from capastudy.settings import (
    MSK_PORTS_XLSX as PORTS_XLSX,
    MSK_QUERY_DIR as QUERY_DIR,
)

PORT_CALLS_URL = 'https://api.maersk.com/synergy/schedules/port-calls'

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
    ensure_directory(QUERY_DIR)



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


def count_unique_voyages(rows):
    return len(
        {
            (
                row.get('LoopAbbrv'),
                row.get('VesselCode'),
                row.get('Voyage'),
            )
            for row in rows
        }
    )



def get_target_ports(port_df, argv=None):
    requested = choose_requested_items(
        port_df['city'].tolist(),
        argv=argv,
        normalize=normalize_text,
        missing_message=lambda missing: f"Ports not found: {', '.join(sorted(missing))}",
    )
    if len(requested) == len(port_df):
        return port_df.reset_index(drop=True)
    requested_set = {normalize_text(item) for item in requested}
    result = port_df[port_df['city'].map(normalize_text).isin(requested_set)].copy()
    missing = requested_set - set(result['city'].map(normalize_text))
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



def process_port_record(rec, from_date, to_date, allowed_services):
    allowed_services = {normalize_text(x) for x in allowed_services}
    city = rec['city']
    geoid = rec['geoid']
    print(f'Querying {city} ({geoid})')
    port_rows = []
    for item in request_port_calls(geoid, from_date, to_date) or []:
        matched = choose_matched_service_and_voyage(item, allowed_services)
        if not matched:
            continue
        port_rows.append(
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
    return {
        'port': city,
        'queried_voyages': count_unique_voyages(port_rows),
        'queried_port_calls': len(port_rows),
        'total_voyages': [],
        'total_port_calls': port_rows,
    }


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
    return save_timestamped_voyage_portcall_workbook(
        QUERY_DIR,
        "MSK_FETCH_BATCH_DETAIL",
        voyage_rows=voyage_rows,
        port_call_rows=port_call_rows,
        voyage_sheet_name="Total Voyages",
        port_call_sheet_name="Total PortCalls",
    )



def build_query_window(reference_date=None):
    if reference_date is None:
        reference_date = date.today()
    # Keep consistent with project week convention: Saturday is week start.
    week_start_day = 5  # Monday=0 ... Saturday=5
    delta = (reference_date.weekday() - week_start_day) % 7
    current_week_start = reference_date - timedelta(days=delta)
    current_week_end = current_week_start + timedelta(days=6)
    from_date = current_week_start - timedelta(weeks=8)
    to_date = current_week_end + timedelta(weeks=12)
    return from_date.isoformat(), to_date.isoformat(), current_week_start.isoformat(), current_week_end.isoformat()


def main():
    port_df = load_ports()
    target_ports = get_target_ports(port_df, argv=sys.argv[1:])
    allowed_services = load_allowed_services()
    from_date, to_date, week_start, week_end = build_query_window()

    print(f'Ports to process: {target_ports["city"].tolist()}')
    print(f'Allowed services: {allowed_services}')
    print(f'Current week: {week_start} ~ {week_end} (week start=Saturday)')
    print(f'fromDate: {from_date}')
    print(f'toDate: {to_date}')

    def _run_port(rec):
        return process_port_record(rec, from_date, to_date, allowed_services)

    port_results, _ignored_voyages, raw_port_call_rows = run_item_batch(
        target_ports.to_dict(orient='records'),
        _run_port,
        item_label='Port',
    )
    running_rows = []
    for result in port_results:
        port_name = result.get('port')
        if port_name is None:
            continue
        matched_rows = [
            row
            for row in raw_port_call_rows
            if normalize_text(row.get('PortName')) == normalize_text(port_name)
        ]
        running_rows.extend(matched_rows)
        retained_rows = dedupe_port_calls(running_rows)
        print(
            f"Port {port_name}: "
            f"queried voyages={result.get('queried_voyages', 0)}, "
            f"queried port calls={result.get('queried_port_calls', 0)}, "
            f"retained voyages={count_unique_voyages(retained_rows)}, "
            f"retained port calls={len(retained_rows)}"
        )

    port_call_rows = dedupe_port_calls(raw_port_call_rows)
    voyage_rows = build_voyage_rows(port_call_rows)

    print(f'Retained voyages: {len(voyage_rows)}')
    print(f'Retained port calls: {len(port_call_rows)}')
    detail_path = save_detail(voyage_rows, port_call_rows)
    print(f'Batch detail tables saved: {detail_path}')


if __name__ == '__main__':
    main()

