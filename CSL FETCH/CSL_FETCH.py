import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

TARGET_URL = "https://elines.coscoshipping.com/ebusiness/sailingSchedule/searchByService"
SCRIPT_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = SCRIPT_DIR / "artifacts"
SERVICE_RULES_XLSX = SCRIPT_DIR / "csl_service_start_end.xlsx"
DEFAULT_SERVICE = "AEU1"
DEFAULT_SERVICE_CODE = "SERVICE"
DEFAULT_PORT_CODE = "PORT"


def sanitize_filename(name):
    cleaned = re.sub(r'[\\/:*?"<>|]', "_", str(name)).strip()
    return cleaned or "UNKNOWN"


def normalize_port_name(name):
    return str(name).strip().upper() if name is not None else ""


def extract_westbound_voyage(voyage):
    text = str(voyage).strip() if voyage is not None else ""
    if not text:
        return ""

    for part in text.split("/"):
        part = part.strip()
        if part.upper().endswith("W"):
            return part

    return text.split("/")[0].strip()


def get_target_service():
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1].strip().upper()
    return DEFAULT_SERVICE


def normalize_service_group(service_code):
    service_code = service_code.upper()
    if service_code.startswith("AEU"):
        return "远东-西北欧"
    if service_code.startswith("AEM"):
        return "远东-地中海"
    raise ValueError(f"Unsupported service group for service: {service_code}")


def prettify_port_name(port_name):
    words = str(port_name).strip().split()
    return " ".join(word.capitalize() for word in words)


def load_service_rules():
    df = pd.read_excel(SERVICE_RULES_XLSX)
    rules = {}
    for row in df.to_dict(orient="records"):
        service = normalize_port_name(row.get("SERVICE"))
        start_port = normalize_port_name(row.get("START"))
        end_port = normalize_port_name(row.get("END"))
        if service and start_port and end_port:
            rules[service] = {"start": start_port, "end": end_port}
    return rules


def load_service_start_port(service_code):
    rules = load_service_rules()
    service_rule = rules.get(service_code.upper())
    if service_rule is None:
        raise ValueError(f"Service {service_code} was not found in {SERVICE_RULES_XLSX.name}")
    return prettify_port_name(service_rule["start"])


def extract_port_call_rows(response_json):
    data = (((response_json.get("data") or {}).get("content") or {}).get("data") or [])
    return data if isinstance(data, list) else []


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


def parse_tables(response_json):
    port_calls = extract_port_call_rows(response_json)
    if not port_calls:
        return [], []

    service_rules = load_service_rules()
    voyage_groups = {}
    voyage_order = []

    for row in port_calls:
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


def save_to_excel(response_json):
    voyage_rows, port_call_rows = parse_tables(response_json)
    df_voyages = pd.DataFrame(voyage_rows)
    df_port_calls = pd.DataFrame(port_call_rows)

    first_row = port_call_rows[0] if port_call_rows else {}
    service_code = first_row.get("LoopAbbrv") or DEFAULT_SERVICE_CODE
    port_code = first_row.get("PortName") or DEFAULT_PORT_CODE
    timestamp = datetime.now().strftime("%y%m%d%H%M%S")
    file_name = f"CSL_FETCH_{sanitize_filename(service_code)}_{sanitize_filename(port_code)}_{timestamp}.xlsx"
    output_path = SCRIPT_DIR / file_name

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_voyages.to_excel(writer, index=False, sheet_name="Voyages")
        df_port_calls.to_excel(writer, index=False, sheet_name="PortCalls")

    return str(output_path), len(df_voyages), len(df_port_calls)


async def prepare_page(page):
    await page.set_viewport_size({"width": 1600, "height": 900})


async def open_search_page(page):
    print(f"Opening: {TARGET_URL}")
    await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=60000)
    print(f"Page title: {await page.title()}")


async def save_debug_artifacts(page, prefix="csl_search_page"):
    ARTIFACT_DIR.mkdir(exist_ok=True)
    await page.screenshot(path=str(ARTIFACT_DIR / f"{prefix}.png"), full_page=True)
    html = await page.content()
    (ARTIFACT_DIR / f"{prefix}.html").write_text(html, encoding="utf-8")
    print(f"Artifacts saved to: {ARTIFACT_DIR}")


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
            print(f"Clicked: {text}")
            return
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Could not click element with text '{text}': {last_error}")


async def choose_port(page, port_name, timeout=500):
    port_input = page.locator("input[placeholder='港口名称 (城市,省,国家/地区)']").first
    await port_input.wait_for(state="visible", timeout=timeout)
    await port_input.click(timeout=timeout)
    await port_input.fill(port_name, timeout=timeout)
    print(f"Typed port keyword: {port_name}")

    suggestion = page.locator(".ivu-select-dropdown .ivu-select-item", has_text=port_name).first
    await suggestion.wait_for(state="visible", timeout=timeout)
    await suggestion.click(timeout=timeout)
    print(f"Selected suggestion: {port_name}")


async def trigger_search(page, timeout=500):
    search_button = page.locator(".search-port-row .btnSearch").first
    await search_button.wait_for(state="visible", timeout=timeout)
    await search_button.click(timeout=timeout)
    print("Triggered search.")


async def choose_period(page, period_text, timeout=2000):
    period_dropdown = page.locator(".filter-selects .ivu-select-selection", has_text="四周内").first
    await period_dropdown.wait_for(state="visible", timeout=timeout)
    await period_dropdown.click(timeout=timeout)
    print("Opened period dropdown.")

    period_option = page.locator(".ivu-select-dropdown .ivu-select-item", has_text=period_text).first
    await period_option.wait_for(state="visible", timeout=timeout)
    await period_option.click(timeout=timeout)
    print(f"Selected period: {period_text}")


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


async def choose_period_and_capture(page, service_code, period_text="八周内", timeout=2000):
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
    await choose_period(page, period_text, timeout=timeout)
    await page.wait_for_timeout(3000)
    page.remove_listener("response", on_response)

    if not matched_responses:
        raise RuntimeError("No matching schedule responses were observed after selecting the period.")

    diagnostics_lines = []
    for idx, item in enumerate(matched_responses, start=1):
        line = (
            f"[{idx}] method={item['method']} status={item['status']} "
            f"resource_type={item['resource_type']} content_type={item['content_type']} "
            f"url={item['url']}"
        )
        diagnostics_lines.append(line)
        print(line)

    ARTIFACT_DIR.mkdir(exist_ok=True)
    diagnostics_path = ARTIFACT_DIR / f"csl_schedule_response_{service_code}_8weeks_diagnostics.txt"
    diagnostics_path.write_text("\n".join(diagnostics_lines), encoding="utf-8")

    target_response = None
    for item in reversed(matched_responses):
        if "period=56" in item["url"]:
            target_response = item
            break
    if target_response is None:
        target_response = matched_responses[-1]

    response_text = await fetch_response_text_in_page(page, target_response["url"])
    print(f"Captured response URL: {target_response['url']}")
    print(f"Captured response preview: {response_text[:100]}")

    response_path = ARTIFACT_DIR / f"csl_schedule_response_{service_code}_8weeks.json"
    response_path.write_text(response_text, encoding="utf-8")
    return json.loads(response_text)


async def fetch_response_json(service_code):
    service_group = normalize_service_group(service_code)
    start_port = load_service_start_port(service_code)
    print(f"Target service: {service_code}")
    print(f"Service group: {service_group}")
    print(f"Start port: {start_port}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
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
            await save_debug_artifacts(page, prefix="csl_search_page")

            for step_text in ["允许全部", "欧洲航线", service_group, service_code]:
                await click_by_text(page, step_text, timeout=500)
                await page.wait_for_timeout(1500)

            await choose_port(page, start_port)
            await page.wait_for_timeout(1000)
            await trigger_search(page)
            await page.wait_for_timeout(1000)
            response_json = await choose_period_and_capture(page, service_code)
            await page.wait_for_timeout(1000)
            await save_debug_artifacts(page, prefix=f"csl_search_after_{service_code.lower()}")
            return response_json
        except PlaywrightTimeoutError as exc:
            await save_debug_artifacts(page, prefix="csl_timeout")
            raise RuntimeError(f"Timed out while loading or waiting for the page: {exc}") from exc
        except Exception:
            await save_debug_artifacts(page, prefix="csl_failure")
            raise
        finally:
            await browser.close()


async def main():
    service_code = get_target_service()
    response_json = await fetch_response_json(service_code)
    output_file, voyage_count, port_call_count = save_to_excel(response_json)
    print(f"Excel 已保存: {output_file}")
    print(f"Voyages 行数: {voyage_count}, PortCalls 行数: {port_call_count}")


if __name__ == "__main__":
    asyncio.run(main())
