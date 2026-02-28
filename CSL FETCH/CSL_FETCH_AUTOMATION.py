import asyncio
import sys
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


def load_service_start_port(service_code):
    df = pd.read_excel(SERVICE_RULES_XLSX)
    df["SERVICE"] = df["SERVICE"].astype(str).str.strip().str.upper()
    matched = df.loc[df["SERVICE"] == service_code]
    if matched.empty:
        raise ValueError(f"Service {service_code} was not found in {SERVICE_RULES_XLSX.name}")
    start_port = str(matched.iloc[0]["START"]).strip()
    return prettify_port_name(start_port)


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


async def choose_period_and_capture(page, period_text, output_name, diagnostics_name, timeout=2000):
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

    lines = []
    for idx, item in enumerate(matched_responses, start=1):
        line = (
            f"[{idx}] method={item['method']} status={item['status']} "
            f"resource_type={item['resource_type']} content_type={item['content_type']} "
            f"url={item['url']}"
        )
        lines.append(line)
        print(line)

    ARTIFACT_DIR.mkdir(exist_ok=True)
    (ARTIFACT_DIR / diagnostics_name).write_text("\n".join(lines), encoding="utf-8")

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
    (ARTIFACT_DIR / output_name).write_text(response_text, encoding="utf-8")
    return response_text


async def perform_query(page, service_code):
    service_group = normalize_service_group(service_code)
    start_port = load_service_start_port(service_code)
    print(f"Service group: {service_group}")
    print(f"Start port: {start_port}")

    steps = [
        "允许全部",
        "欧洲航线",
        service_group,
        service_code,
    ]

    for step_text in steps:
        await click_by_text(page, step_text, timeout=500)
        await page.wait_for_timeout(1500)

    await choose_port(page, start_port)
    await page.wait_for_timeout(1000)
    await trigger_search(page)
    await page.wait_for_timeout(1000)
    await choose_period_and_capture(
        page,
        "八周内",
        f"csl_schedule_response_{service_code}_8weeks.json",
        f"csl_schedule_response_{service_code}_8weeks_diagnostics.txt",
    )
    await page.wait_for_timeout(1000)
    await save_debug_artifacts(page, prefix=f"csl_search_after_{service_code.lower()}")
    print("Initial navigation steps completed.")


async def run():
    service_code = get_target_service()
    print(f"Target service: {service_code}")

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
            await save_debug_artifacts(page)
            await perform_query(page, service_code)
            print("Browser will remain open for 5 seconds for inspection.")
            await asyncio.sleep(5)
        except PlaywrightTimeoutError as exc:
            print(f"Timed out while loading or waiting for the page: {exc}")
            await save_debug_artifacts(page, prefix="csl_timeout")
        except Exception as exc:
            print(f"Automation failed: {exc}")
            await save_debug_artifacts(page, prefix="csl_failure")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run())
