import asyncio
import json
import os
import re
import sys

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from capastudy.carriers.common import (
    PORT_CALL_COLUMNS,
    VOYAGE_COLUMNS,
    choose_requested_items,
    ensure_directory,
    run_async_item_batch,
    save_summary_workbook,
    save_timestamped_voyage_portcall_workbook,
)
from capastudy.settings import (
    CSL_ARTIFACT_DIR as ARTIFACT_DIR,
    CSL_QUERY_DIR as QUERY_DIR,
    CSL_SERVICE_RULES_XLSX as SERVICE_RULES_XLSX,
)

TARGET_URL = "https://elines.coscoshipping.com/ebusiness/sailingSchedule/searchByService"
DEFAULT_SERVICE_CODE = "SERVICE"
DEFAULT_PORT_CODE = "PORT"
FETCH_RETRY_ATTEMPTS = 5
RETRY_BASE_DELAY_SECONDS = 2
HEADLESS_ENV_VAR = "CSL_HEADLESS"

def sanitize_filename(name):
    cleaned = re.sub(r'[\\/:*?"<>|]', "_", str(name)).strip()
    return cleaned or "UNKNOWN"


def ensure_query_dir():
    ensure_directory(QUERY_DIR)


def is_headless_enabled():
    value = os.getenv(HEADLESS_ENV_VAR, "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def debug_log(message):
    return None


def normalize_port_name(name):
    return str(name).strip().upper() if name is not None else ""


def prettify_port_name(port_name):
    words = str(port_name).strip().split()
    return " ".join(word.capitalize() for word in words)


def extract_westbound_voyage(voyage):
    text = str(voyage).strip() if voyage is not None else ""
    if not text:
        return ""
    for part in text.split("/"):
        part = part.strip()
        if part.upper().endswith("W"):
            return part
    return text.split("/")[0].strip()


def load_service_rules():
    df = pd.read_excel(SERVICE_RULES_XLSX)
    rules = {}
    for row in df.to_dict(orient="records"):
        service = normalize_port_name(row.get("SERVICE"))
        start_port = normalize_port_name(row.get("START"))
        end_port = normalize_port_name(row.get("END"))
        alt1 = normalize_port_name(row.get("ALT1"))
        alt2 = normalize_port_name(row.get("ALT2"))
        if service and start_port and end_port:
            rules[service] = {
                "start": start_port,
                "alt1": alt1,
                "alt2": alt2,
                "end": end_port,
            }
    return rules


def get_target_services(service_rules, argv=None):
    return choose_requested_items(
        service_rules.keys(),
        argv=argv,
        normalize=lambda value: str(value).strip().upper(),
        missing_message=lambda missing: f"Services not found in {SERVICE_RULES_XLSX.name}: {', '.join(missing)}",
    )


def normalize_service_group(service_code):
    service_code = service_code.upper()
    if service_code.startswith("AEU"):
        return "远东-西北欧"
    if service_code.startswith("AEM"):
        return "远东-地中海"
    raise ValueError(f"Unsupported service group for service: {service_code}")


def build_query_ports(service_rule, include_alternatives):
    ordered_ports = [service_rule["start"]]
    if include_alternatives:
        for key in ("alt1", "alt2"):
            port = service_rule.get(key)
            if port and port not in ordered_ports:
                ordered_ports.append(port)
    return [prettify_port_name(port) for port in ordered_ports if port]


def extract_port_call_rows(response_json):
    data = (((response_json.get("data") or {}).get("content") or {}).get("data") or [])
    return data if isinstance(data, list) else []


def completeness_score(row):
    score_fields = [
        row.get("arrDtlocAct"),
        row.get("depDtlocAct"),
        row.get("arrDtlocCos"),
        row.get("depDtlocCos"),
    ]
    return sum(1 for value in score_fields if value not in (None, ""))


def dedupe_port_calls(port_calls):
    if not port_calls:
        return []

    unique = {}
    for row in port_calls:
        key = (
            row.get("loopAbbrv"),
            row.get("vesselCode"),
            row.get("vesselName"),
            row.get("voy"),
            row.get("protName"),
            row.get("arrDtlocCos") or row.get("arrDtlocAct"),
            row.get("depDtlocCos") or row.get("depDtlocAct"),
        )
        existing = unique.get(key)
        if existing is None:
            unique[key] = row
            continue

        existing_score = completeness_score(existing)
        new_score = completeness_score(row)
        if new_score > existing_score:
            unique[key] = row
        elif new_score == existing_score:
            existing_sources = set(existing.get("QueryPorts", []))
            new_sources = set(row.get("QueryPorts", []))
            merged = dict(existing)
            merged["QueryPorts"] = sorted(existing_sources | new_sources)
            unique[key] = merged

    deduped = list(unique.values())
    deduped.sort(
        key=lambda x: (
            str(x.get("loopAbbrv") or ""),
            str(x.get("vesselCode") or ""),
            str(x.get("voy") or ""),
            str(x.get("arrDtlocCos") or x.get("arrDtlocAct") or ""),
            str(x.get("protName") or ""),
        )
    )
    return deduped


def slice_westbound_calls(calls, service_rule):
    if not calls:
        return []
    if not service_rule:
        return calls

    end_port = service_rule["end"]
    end_idx = None
    for idx, call in enumerate(calls):
        if normalize_port_name(call.get("PortName")) == end_port:
            end_idx = idx
            break
    if end_idx is None:
        return []

    westbound_calls = []
    for seq, call in enumerate(calls[: end_idx + 1], start=1):
        sliced_call = dict(call)
        sliced_call["PortCallSeq"] = seq
        sliced_call["Direction"] = "W"
        westbound_calls.append(sliced_call)
    return westbound_calls


def parse_tables_from_rows(raw_port_calls, service_rules):
    if not raw_port_calls:
        return [], []

    voyage_groups = {}
    voyage_order = []
    for row in raw_port_calls:
        if not isinstance(row, dict):
            continue

        group_key = (
            row.get("loopAbbrv"),
            row.get("vesselCode"),
            row.get("vesselName"),
            row.get("voy"),
        )
        if group_key not in voyage_groups:
            voyage_groups[group_key] = []
            voyage_order.append(group_key)

        voyage_groups[group_key].append(
            {
                "LoopAbbrv": row.get("loopAbbrv"),
                "VesselCode": row.get("vesselCode"),
                "VesselName": row.get("vesselName"),
                "Voyage": extract_westbound_voyage(row.get("voy")),
                "PortCallSeq": len(voyage_groups[group_key]) + 1,
                "PortName": row.get("protName"),
                "ArrDtlocAct": row.get("arrDtlocAct"),
                "DepDtlocAct": row.get("depDtlocAct"),
                "ArrDtlocCos": row.get("arrDtlocCos"),
                "DepDtlocCos": row.get("depDtlocCos"),
            }
        )

    voyage_rows = []
    port_call_rows = []
    for group_key in voyage_order:
        calls = voyage_groups[group_key]
        service_code = normalize_port_name(group_key[0])
        westbound_calls = slice_westbound_calls(calls, service_rules.get(service_code))
        if not westbound_calls:
            continue

        first_call = westbound_calls[0]
        last_call = westbound_calls[-1]
        port_names = [call["PortName"] for call in westbound_calls if call.get("PortName")]
        voyage_rows.append(
            {
                "LoopAbbrv": first_call.get("LoopAbbrv"),
                "VesselCode": first_call.get("VesselCode"),
                "VesselName": first_call.get("VesselName"),
                "Voyage": first_call.get("Voyage"),
                "Direction": "W",
                "PortCallCount": len(westbound_calls),
                "FirstPort": first_call.get("PortName"),
                "LastPort": last_call.get("PortName"),
                "FirstArrDtlocAct": first_call.get("ArrDtlocAct"),
                "FirstDepDtlocAct": first_call.get("DepDtlocAct"),
                "LastArrDtlocAct": last_call.get("ArrDtlocAct"),
                "LastDepDtlocAct": last_call.get("DepDtlocAct"),
                "FirstArrDtlocCos": first_call.get("ArrDtlocCos"),
                "FirstDepDtlocCos": first_call.get("DepDtlocCos"),
                "LastArrDtlocCos": last_call.get("ArrDtlocCos"),
                "LastDepDtlocCos": last_call.get("DepDtlocCos"),
                "PortCallPath": " > ".join(port_names),
            }
        )
        port_call_rows.extend(westbound_calls)

    return voyage_rows, port_call_rows


def save_tables_to_excel(voyage_rows, port_call_rows, service_code, port_label, suffix):
    ensure_query_dir()
    file_prefix = (
        f"CSL_FETCH_{sanitize_filename(service_code)}_{sanitize_filename(port_label)}_"
        f"{sanitize_filename(suffix)}"
    )
    return save_timestamped_voyage_portcall_workbook(
        QUERY_DIR,
        file_prefix,
        voyage_rows=voyage_rows,
        port_call_rows=port_call_rows,
        voyage_sheet_name="Voyages",
        port_call_sheet_name="PortCalls",
    )


def save_batch_detail_tables(voyage_rows, port_call_rows):
    return save_timestamped_voyage_portcall_workbook(
        QUERY_DIR,
        "CSL_FETCH_BATCH_DETAIL",
        voyage_rows=voyage_rows,
        port_call_rows=port_call_rows,
        voyage_sheet_name="Total Voyages",
        port_call_sheet_name="Total PortCalls",
    )


async def prepare_page(page):
    await page.set_viewport_size({"width": 1600, "height": 900})


async def open_search_page(page):
    debug_log(f"Opening: {TARGET_URL}")
    await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=60000)
    debug_log(f"Page title: {await page.title()}")


async def click_by_text(page, text, timeout=500):
    candidates = [
        page.get_by_role("button", name=text),
        page.get_by_role("link", name=text),
        page.get_by_text(text, exact=True),
        page.get_by_text(text),
        page.locator(f"xpath=//*[normalize-space()='{text}']"),
        page.locator(f"xpath=//*[contains(normalize-space(), '{text}')]"),
    ]
    last_error = None
    for locator in candidates:
        try:
            await locator.first.wait_for(state="visible", timeout=timeout)
            await locator.first.scroll_into_view_if_needed(timeout=timeout)
            await locator.first.click(timeout=timeout)
            debug_log(f"Clicked: {text}")
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Could not click element with text '{text}': {last_error}")


async def select_service_in_group(page, service_group, service_code, timeout=2000):
    group_item = page.locator(".ser-group .ivu-collapse-item", has_text=service_group).first
    await group_item.wait_for(state="visible", timeout=timeout)

    group_class = await group_item.get_attribute("class") or ""
    if "ivu-collapse-item-active" not in group_class:
        header = group_item.locator(".ivu-collapse-header").first
        await header.scroll_into_view_if_needed(timeout=timeout)
        await header.click(timeout=timeout)
        debug_log(f"Expanded group: {service_group}")

    active_group = page.locator(".ser-group .ivu-collapse-item.ivu-collapse-item-active", has_text=service_group).first
    await active_group.wait_for(state="visible", timeout=timeout)
    service_item = active_group.locator(".feeder-line-lis", has_text=service_code).first
    await service_item.wait_for(state="visible", timeout=timeout)
    await service_item.scroll_into_view_if_needed(timeout=timeout)
    await service_item.click(timeout=timeout)
    debug_log(f"Selected service: {service_code}")


async def choose_port(page, port_name, timeout=2500):
    port_input = page.locator("input[placeholder='港口名称 (城市,省,国家/地区)']").first
    await port_input.wait_for(state="visible", timeout=timeout)
    await port_input.click(timeout=timeout)
    await port_input.fill(port_name, timeout=timeout)
    debug_log(f"Typed port keyword: {port_name}")
    suggestion_candidates = [
        page.locator(".ivu-select-dropdown .ivu-select-item", has_text=port_name).first,
        page.locator(".ivu-select-dropdown .ivu-select-item", has_text=port_name.split()[0]).first,
        page.locator(".ivu-select-dropdown .ivu-select-item").first,
    ]
    last_error = None
    for suggestion in suggestion_candidates:
        try:
            await suggestion.wait_for(state="visible", timeout=timeout)
            await suggestion.click(timeout=timeout)
            chosen_text = (await suggestion.inner_text()).strip()
            debug_log(f"Selected suggestion: {chosen_text}")
            return
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"Could not select suggestion for port '{port_name}': {last_error}")


async def trigger_search(page, timeout=500):
    search_button = page.locator(".search-port-row .btnSearch").first
    await search_button.wait_for(state="visible", timeout=timeout)
    await search_button.click(timeout=timeout)
    debug_log("Triggered search.")


async def choose_period(page, period_text, timeout=3000):
    period_dropdown = page.locator(".filter-selects .ivu-select-selection", has_text="四周内").first
    await period_dropdown.wait_for(state="visible", timeout=timeout)
    await period_dropdown.click(timeout=timeout)
    debug_log("Opened period dropdown.")
    period_option = page.locator(".ivu-select-dropdown .ivu-select-item", has_text=period_text).first
    await period_option.wait_for(state="visible", timeout=timeout)
    await period_option.click(timeout=timeout)
    debug_log(f"Selected period: {period_text}")


async def fetch_response_text_in_page(page, url):
    return await page.evaluate(
        """
        async (targetUrl) => {
            const response = await fetch(targetUrl, {
                method: 'GET',
                credentials: 'include',
            });
            return await response.text();
        }
        """,
        url,
    )


async def choose_period_and_capture(page, timeout=3000):
    matched_responses = []

    def on_response(response):
        if "/ebschedule/public/purpoShipment/service/port" not in response.url:
            return
        matched_responses.append(
            {
                "url": response.url,
                "method": response.request.method,
                "resource_type": response.request.resource_type,
                "status": response.status,
                "content_type": response.headers.get("content-type", ""),
            }
        )

    page.on("response", on_response)
    period_error = None
    for _ in range(3):
        try:
            await choose_period(page, "八周内", timeout=timeout)
            period_error = None
            break
        except Exception as exc:
            period_error = exc
            await page.wait_for_timeout(1200)
    if period_error is not None:
        page.remove_listener("response", on_response)
        raise period_error
    await page.wait_for_timeout(3000)
    page.remove_listener("response", on_response)

    if not matched_responses:
        raise RuntimeError("No matching schedule responses were observed after selecting the period.")

    target_response = None
    for item in reversed(matched_responses):
        if "period=56" in item["url"]:
            target_response = item
            break
    if target_response is None:
        target_response = matched_responses[-1]

    response_text = await fetch_response_text_in_page(page, target_response["url"])
    debug_log(f"Captured response URL: {target_response['url']}")
    debug_log(f"Captured response preview: {response_text[:100]}")
    return json.loads(response_text)


async def fetch_response_json(service_code, query_port):
    service_group = normalize_service_group(service_code)
    debug_log(f"Service group: {service_group}")
    debug_log(f"Query port: {query_port}")
    headless = is_headless_enabled()
    debug_log(f"Headless mode: {headless}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
        )
        await Stealth().apply_stealth_async(context)
        page = await context.new_page()
        try:
            await prepare_page(page)
            await open_search_page(page)
            for step_text in ["允许全部", "欧洲航线"]:
                await click_by_text(page, step_text, timeout=1200)
                await page.wait_for_timeout(1500)
            await select_service_in_group(page, service_group, service_code, timeout=3000)
            await page.wait_for_timeout(1500)
            await choose_port(page, query_port, timeout=2500)
            await page.wait_for_timeout(1000)
            await trigger_search(page, timeout=1200)
            await page.wait_for_timeout(1000)
            return await choose_period_and_capture(page, timeout=3000)
        except PlaywrightTimeoutError as exc:
            raise RuntimeError(f"Timed out while loading or waiting for the page: {exc}") from exc
        finally:
            await browser.close()


async def fetch_response_json_with_retry(
    service_code,
    query_port,
    max_attempts=FETCH_RETRY_ATTEMPTS,
    base_delay_seconds=RETRY_BASE_DELAY_SECONDS,
):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await fetch_response_json(service_code, query_port)
        except Exception as exc:
            last_error = exc
            print(f"Fetch failed for {service_code}/{query_port} attempt {attempt}/{max_attempts}: {exc}")
            if attempt < max_attempts:
                await asyncio.sleep(base_delay_seconds * attempt)
    raise RuntimeError(f"Failed to fetch {service_code}/{query_port} after {max_attempts} attempts: {last_error}")


async def process_service(service_code, service_rules):
    service_rule = service_rules[service_code]
    multi_ports = build_query_ports(service_rule, include_alternatives=True)
    print(f"Target service: {service_code}")

    multi_raw_rows = []
    for port in multi_ports:
        try:
            print(f"Query {service_code} / {port}")
            response_json = await fetch_response_json_with_retry(service_code, port)
        except Exception as exc:
            print(f"Skipped port {port} for {service_code}: {exc}")
            continue
        rows = extract_port_call_rows(response_json)
        port_voyages, port_calls = parse_tables_from_rows(rows, service_rules)
        print(f"Result {service_code} / {port}: voyages={len(port_voyages)}, port calls={len(port_calls)}")
        for row in rows:
            cloned = dict(row)
            cloned["QueryPorts"] = [port]
            multi_raw_rows.append(cloned)

    deduped_rows = dedupe_port_calls(multi_raw_rows)
    multi_voyages, multi_calls = parse_tables_from_rows(deduped_rows, service_rules)
    multi_file = save_tables_to_excel(multi_voyages, multi_calls, service_code, "MULTIPORT", "DEDUPED")
    print(f"起运港+备选港去重结果已保存: {multi_file}")

    total_voyages = []
    for row in multi_voyages:
        enriched = dict(row)
        enriched["Service"] = service_code
        total_voyages.append(enriched)

    total_port_calls = []
    for row in multi_calls:
        enriched = dict(row)
        enriched["Service"] = service_code
        total_port_calls.append(enriched)

    return {
        "service": service_code,
        "multi_port_file": multi_file,
        "total_voyages": total_voyages,
        "total_port_calls": total_port_calls,
        "multi_port_voyages": len(multi_voyages),
        "multi_port_port_calls": len(multi_calls),
    }


async def main():
    service_rules = load_service_rules()
    target_services = get_target_services(service_rules, argv=sys.argv[1:])
    ensure_query_dir()
    print(f"Services to process: {target_services}")

    async def _run_service(service_code):
        return await process_service(service_code, service_rules)

    results, batch_voyages, batch_port_calls = await run_async_item_batch(
        target_services,
        _run_service,
        item_label="Service",
    )

    summary_path = save_summary_workbook(QUERY_DIR, "CSL_FETCH_BATCH_SUMMARY", results)
    detail_path = save_batch_detail_tables(batch_voyages, batch_port_calls)
    print(f"Batch summary saved: {summary_path}")
    print(f"Batch detail tables saved: {detail_path}")


if __name__ == "__main__":
    asyncio.run(main())

