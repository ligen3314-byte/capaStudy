import asyncio
import json
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from capastudy.carriers.common import (
    run_async_item_batch,
    save_summary_workbook,
    save_timestamped_voyage_portcall_workbook,
)
from capastudy.carriers.csl_fetch import (
    QUERY_DIR,
    RETRY_BASE_DELAY_SECONDS,
    TARGET_URL,
    build_query_ports,
    choose_port,
    click_by_text,
    dedupe_port_calls,
    ensure_query_dir,
    extract_port_call_rows,
    get_target_services,
    is_headless_enabled,
    load_service_rules,
    normalize_service_group,
    parse_tables_from_rows,
    prepare_page,
    save_tables_to_excel,
    select_service_in_group,
    trigger_search,
)

QUERY_RETRY_ATTEMPTS = 4


async def ensure_search_root(page, cookie_state):
    if page.is_closed():
        raise RuntimeError("Playwright page is closed.")
    current_url = page.url or ""
    # Recover from blank/unknown pages.
    if (not current_url) or current_url.startswith("about:blank") or ("searchByService" not in current_url):
        await open_search_once(page)
        await maybe_accept_cookie_once(page, cookie_state)
        return


async def wait_result_ready(page, timeout=12000):
    # Wait until any typical result container appears after search.
    candidates = [
        page.locator("tr.ivu-table-row").first,
        page.locator(".result-service-content").first,
        page.locator(".time-line-wrap").first,
    ]
    for locator in candidates:
        try:
            await locator.wait_for(state="visible", timeout=timeout)
            return
        except Exception:
            continue
    # Soft fallback: keep flow moving while still giving page time to render.
    await page.wait_for_timeout(1500)


async def open_search_once(page):
    await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_load_state("networkidle", timeout=60000)


async def maybe_accept_cookie_once(page, cookie_state):
    if cookie_state["attempted"]:
        return
    cookie_state["attempted"] = True
    try:
        await click_by_text(page, "允许全部", timeout=2500)
        await page.wait_for_timeout(500)
        cookie_state["accepted"] = True
    except Exception:
        cookie_state["accepted"] = False


async def ensure_search_form_visible(page, timeout=6000):
    await page.locator("input[placeholder='港口名称 (城市,省,国家/地区)']").first.wait_for(
        state="visible", timeout=timeout
    )


async def back_to_search(page):
    try:
        await page.go_back(wait_until="domcontentloaded", timeout=8000)
        await page.wait_for_load_state("networkidle", timeout=8000)
        await page.wait_for_timeout(600)
    except Exception:
        # Fallback to fresh search page when history is not usable.
        await open_search_once(page)
        await page.wait_for_timeout(500)


async def back_to_service_selection(page):
    try:
        # 1st back: resultByServicePorts -> serviceDetails
        await page.go_back(wait_until="domcontentloaded", timeout=8000)
        await page.wait_for_load_state("networkidle", timeout=8000)
        await page.wait_for_timeout(500)
        # 2nd back: serviceDetails -> searchByService
        await page.go_back(wait_until="domcontentloaded", timeout=8000)
        await page.wait_for_load_state("networkidle", timeout=8000)
        await page.wait_for_timeout(700)
    except Exception:
        await open_search_once(page)
        await page.wait_for_timeout(500)


async def enter_service(page, service_code):
    service_group = normalize_service_group(service_code)
    await click_by_text(page, "欧洲航线", timeout=1500)
    # await page.wait_for_timeout(300)
    await click_by_text(page, service_group, timeout=3000)
    # await page.wait_for_timeout(500)
    await select_service_in_group(page, service_group, service_code, timeout=3500)
    await ensure_search_form_visible(page, timeout=5000)
    await page.wait_for_timeout(400)


async def query_first_port(page, service_code, port):
    await choose_port(page, port, timeout=3000)
    await page.wait_for_timeout(400)
    await trigger_search(page, timeout=1500)
    await wait_result_ready(page, timeout=5000)
    return await select_period_and_capture_from_details(page, timeout=4000)


async def query_port_from_details_placeholder(page, port):
    input_locator = page.locator("input.ivu-select-input").first
    await input_locator.wait_for(state="visible", timeout=5000)
    await input_locator.click(timeout=2000)
    await input_locator.fill(port, timeout=2000)
    await page.wait_for_timeout(300)

    suggestion_candidates = [
        page.locator(".ivu-select-dropdown .ivu-select-item", has_text=port).first,
        page.locator(".ivu-select-dropdown .ivu-select-item", has_text=port.split()[0]).first,
        page.locator(".ivu-select-dropdown .ivu-select-item").first,
    ]
    for item in suggestion_candidates:
        try:
            await item.wait_for(state="visible", timeout=1500)
            await item.click(timeout=1500)
            break
        except Exception:
            continue
    else:
        await page.keyboard.press("Enter")

    btn = page.locator(".btnSearch").first
    await btn.wait_for(state="visible", timeout=5000)
    await btn.click(timeout=3000)
    await wait_result_ready(page, timeout=12000)
    return await select_period_and_capture_from_details(page, timeout=4000)


async def select_period_and_capture_from_details(page, timeout=4000):
    matched_responses = []

    def on_response(response):
        if "/ebschedule/public/purpoShipment/service/port" not in response.url:
            return
        matched_responses.append(
            {
                "url": response.url,
                "status": response.status,
                "content_type": response.headers.get("content-type", ""),
            }
        )

    page.on("response", on_response)
    try:
        # Prefer the period selector right next to "查询期间" on detail page.
        candidates = [
            page.locator("xpath=//*[contains(normalize-space(),'查询期间')]/following::*[contains(@class,'ivu-select-selection')][1]").first,
            page.locator(".filter-selects .ivu-select-selection", has_text="四周内").first,
            page.locator(".ivu-select-selection", has_text="四周内").first,
        ]

        opened = False
        for cand in candidates:
            try:
                await cand.wait_for(state="visible", timeout=timeout)
                await cand.click(timeout=timeout)
                opened = True
                break
            except Exception:
                continue
        if not opened:
            raise RuntimeError("Could not open period dropdown near 查询期间.")

        option = page.locator(".ivu-select-dropdown .ivu-select-item", has_text="八周内").first
        await option.wait_for(state="visible", timeout=timeout)
        await option.click(timeout=timeout)
        await page.wait_for_timeout(2500)
    finally:
        page.remove_listener("response", on_response)

    if not matched_responses:
        raise RuntimeError("No schedule response captured after selecting 八周内.")

    target = None
    for item in reversed(matched_responses):
        if "period=56" in item["url"]:
            target = item
            break
    if target is None:
        target = matched_responses[-1]

    response_text = await page.evaluate(
        """
        async (targetUrl) => {
            const response = await fetch(targetUrl, {
                method: 'GET',
                credentials: 'include',
            });
            return await response.text();
        }
        """,
        target["url"],
    )
    return json.loads(response_text)


async def query_with_retry(page, service_code, port, use_placeholder):
    last_error = None
    for attempt in range(1, QUERY_RETRY_ATTEMPTS + 1):
        try:
            if use_placeholder:
                return await query_port_from_details_placeholder(page, port)
            return await query_first_port(page, service_code, port)
        except Exception as exc:
            last_error = exc
            print(f"Fetch failed for {service_code}/{port} attempt {attempt}/{QUERY_RETRY_ATTEMPTS}: {exc}")
            if attempt < QUERY_RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Failed to fetch {service_code}/{port}: {last_error}")


async def process_service(page, service_code, service_rules):
    service_rule = service_rules[service_code]
    ports = build_query_ports(service_rule, include_alternatives=True)
    print(f"Target service: {service_code}")

    raw_rows = []
    for idx, port in enumerate(ports):
        try:
            print(f"Query {service_code} / {port}")
            if idx == 0:
                await enter_service(page, service_code)
                response_json = await query_with_retry(page, service_code, port, use_placeholder=False)
            else:
                try:
                    await back_to_search(page)
                    response_json = await query_with_retry(page, service_code, port, use_placeholder=True)
                except Exception:
                    # Recover by re-entering service from root and querying as first port style.
                    await open_search_once(page)
                    await enter_service(page, service_code)
                    response_json = await query_with_retry(page, service_code, port, use_placeholder=False)
            rows = extract_port_call_rows(response_json)
            port_voyages, port_calls = parse_tables_from_rows(rows, service_rules)
            print(f"Result {service_code} / {port}: voyages={len(port_voyages)}, port calls={len(port_calls)}")
            for row in rows:
                cloned = dict(row)
                cloned["QueryPorts"] = [port]
                raw_rows.append(cloned)
        except Exception as exc:
            print(f"Skipped port {port} for {service_code}: {exc}")

    deduped_rows = dedupe_port_calls(raw_rows)
    voyages, calls = parse_tables_from_rows(deduped_rows, service_rules)
    result_file = save_tables_to_excel(voyages, calls, service_code, "MULTIPORT", "BACK_DEDUPED")
    print(f"后退按钮多港去重结果已保存: {result_file}")

    total_voyages = []
    for row in voyages:
        enriched = dict(row)
        enriched["Service"] = service_code
        total_voyages.append(enriched)

    total_calls = []
    for row in calls:
        enriched = dict(row)
        enriched["Service"] = service_code
        total_calls.append(enriched)

    return {
        "service": service_code,
        "multi_port_file": result_file,
        "multi_port_voyages": len(voyages),
        "multi_port_port_calls": len(calls),
        "total_voyages": total_voyages,
        "total_port_calls": total_calls,
    }


async def main():
    service_rules = load_service_rules()
    target_services = get_target_services(service_rules)
    ensure_query_dir()
    print(f"Services to process: {target_services}")

    cookie_state = {"attempted": False, "accepted": False}
    headless = is_headless_enabled()

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
            await open_search_once(page)
            await maybe_accept_cookie_once(page, cookie_state)
            async def _run_service(service_code):
                await ensure_search_root(page, cookie_state)
                return await process_service(page, service_code, service_rules)

            async def _after_success(service_code, _summary):
                await page.wait_for_timeout(800)
                if service_code != target_services[-1]:
                    try:
                        await back_to_service_selection(page)
                        await ensure_search_root(page, cookie_state)
                    except Exception as exc:
                        # Transition failure should not invalidate already-saved service result.
                        print(f"Warning: service switch back-steps failed after {service_code}: {exc}")
                        await open_search_once(page)
                        await maybe_accept_cookie_once(page, cookie_state)

            results, batch_voyages, batch_port_calls = await run_async_item_batch(
                target_services,
                _run_service,
                item_label="Service",
                after_success=_after_success,
            )
        finally:
            await browser.close()

    summary_path = save_summary_workbook(QUERY_DIR, "CSL_FETCH_BATCH_SUMMARY_BACK", results)
    detail_path = save_timestamped_voyage_portcall_workbook(
        QUERY_DIR,
        "CSL_FETCH_BATCH_DETAIL_BACK",
        voyage_rows=batch_voyages,
        port_call_rows=batch_port_calls,
        voyage_sheet_name="Total Voyages",
        port_call_sheet_name="Total PortCalls",
    )

    print(f"Batch summary saved: {summary_path}")
    print(f"Batch detail tables saved: {detail_path}")


if __name__ == "__main__":
    asyncio.run(main())
