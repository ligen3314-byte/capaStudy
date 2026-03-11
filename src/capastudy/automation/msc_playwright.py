from pathlib import Path
import json

import pandas as pd
from playwright.sync_api import sync_playwright

from capastudy.settings import (
    MSC_SERVICE_XLSX as SERVICE_XLSX,
    MSC_SERVICE_XLSX_FALLBACK as SERVICE_XLSX_FALLBACK,
)

TARGET_URL = "https://www.msc.com/en/search-a-schedule"
COOKIE_ACCEPT_SELECTOR = "#onetrust-accept-btn-handler"

# MSC port names are mostly exact matches, but a few ports in the workbook use aliases.
PORT_ALIASES = {
    "FOS": "FOS-SUR-MER",
    "TIANJIN": "TIANJINXINGANG",
    "XINGANG": "TIANJINXINGANG",
}

PORT_COLUMNS = ["START1", "START2", "START3", "END1", "END2", "END3"]


def normalize_name(value):
    return str(value).strip().upper() if pd.notna(value) and str(value).strip() else ""


def fetch_msc_ports():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)

        try:
            page.locator(COOKIE_ACCEPT_SELECTOR).click(timeout=3000)
            page.wait_for_timeout(1000)
        except Exception:
            pass

        payload = page.evaluate(
            """
            async () => {
                const response = await fetch('/api/feature/tools/GetAllAvailableCountriesAndPorts', {
                    credentials: 'include',
                });
                return {
                    status: response.status,
                    data: await response.json(),
                };
            }
            """
        )
        browser.close()

    if payload["status"] != 200:
        raise RuntimeError(f"MSC port catalog request failed with status {payload['status']}")

    data = payload["data"]
    if isinstance(data, str):
        data = json.loads(data)

    ports = data.get("Ports") or []
    if not ports:
        raise RuntimeError("MSC port catalog is empty.")
    return ports


def build_port_index(ports):
    exact_index = {}
    contains_index = {}
    for port in ports:
        name = normalize_name(port.get("LocationName"))
        code = normalize_name(port.get("LocationCode"))
        if name:
            exact_index[name] = port

        for key in filter(None, {name, code}):
            contains_index.setdefault(key, []).append(port)

    return exact_index, contains_index


def resolve_port(port_name, exact_index, ports):
    lookup = normalize_name(port_name)
    if not lookup:
        return None, "empty"

    canonical_lookup = PORT_ALIASES.get(lookup, lookup)
    if canonical_lookup in exact_index:
        source = "exact" if canonical_lookup == lookup else f"alias:{canonical_lookup}"
        return exact_index[canonical_lookup], source

    contains_matches = [
        port
        for port in ports
        if canonical_lookup in normalize_name(port.get("LocationName"))
        or canonical_lookup in normalize_name(port.get("LocationCode"))
    ]
    if len(contains_matches) == 1:
        return contains_matches[0], f"contains:{canonical_lookup}"
    if len(contains_matches) > 1:
        return contains_matches[0], f"ambiguous:{canonical_lookup}"
    return None, "missing"


def update_service_workbook():
    workbook_path = SERVICE_XLSX if SERVICE_XLSX.exists() else SERVICE_XLSX_FALLBACK
    if not workbook_path.exists():
        raise FileNotFoundError("No MSC service workbook found.")

    df = pd.read_excel(workbook_path)
    ports = fetch_msc_ports()
    exact_index, _ = build_port_index(ports)

    for column in PORT_COLUMNS:
        df[f"{column}_PORT_ID"] = ""
        df[f"{column}_MSC_CODE"] = ""

    # Backward-compatible migration for old column names if needed.
    legacy_to_new = {"START": "START1", "ALT1": "START2", "ALT2": "START3", "END": "END1"}
    for legacy, new in legacy_to_new.items():
        if legacy in df.columns and new not in df.columns:
            df[new] = df[legacy]

    resolution_log = []
    for row_idx, row in df.iterrows():
        service = row.get("SERVICE")
        for column in PORT_COLUMNS:
            port_name = row.get(column)
            port, source = resolve_port(port_name, exact_index, ports)
            if port is None:
                resolution_log.append(f"{service} {column} {port_name}: {source}")
                continue

            df.at[row_idx, f"{column}_PORT_ID"] = port.get("PortId")
            df.at[row_idx, f"{column}_MSC_CODE"] = port.get("LocationCode")
            resolution_log.append(
                f"{service} {column} {port_name}: PortId={port.get('PortId')} "
                f"Code={port.get('LocationCode')} Source={source}"
            )

    output_path = workbook_path
    try:
        df.to_excel(output_path, index=False)
    except PermissionError:
        output_path = workbook_path.with_name(f"{workbook_path.stem}_filled{workbook_path.suffix}")
        df.to_excel(output_path, index=False)

    return resolution_log, output_path


if __name__ == "__main__":
    log_lines, output_path = update_service_workbook()
    print(f"Updated workbook: {output_path}")
    for line in log_lines:
        print(line)

