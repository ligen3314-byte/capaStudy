import asyncio
from datetime import datetime

import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

from capastudy.carriers.csl_fetch import (
    PORT_CALL_COLUMNS,
    QUERY_DIR,
    RETRY_BASE_DELAY_SECONDS,
    TARGET_URL,
    VOYAGE_COLUMNS,
    build_query_ports,
    choose_period_and_capture,
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


async def query_single_port(page, service_code, port, cookie_state):
    service_group = normalize_service_group(service_code)
    await open_search_once(page)
    await maybe_accept_cookie_once(page, cookie_state)

    await click_by_text(page, "欧洲航线", timeout=2500)
    await page.wait_for_timeout(300)
    await click_by_text(page, service_group, timeout=3000)
    await page.wait_for_timeout(500)
    await select_service_in_group(page, service_group, service_code, timeout=3500)
    await page.wait_for_timeout(800)
    await choose_port(page, port, timeout=3000)
    await page.wait_for_timeout(400)
    await trigger_search(page, timeout=1500)
    await page.wait_for_timeout(700)
    return await choose_period_and_capture(page, timeout=3500)


async def query_single_port_with_retry(page, service_code, port, cookie_state):
    last_error = None
    for attempt in range(1, QUERY_RETRY_ATTEMPTS + 1):
        try:
            return await query_single_port(page, service_code, port, cookie_state)
        except Exception as exc:
            last_error = exc
            print(f"Fetch failed for {service_code}/{port} attempt {attempt}/{QUERY_RETRY_ATTEMPTS}: {exc}")
            if attempt < QUERY_RETRY_ATTEMPTS:
                await asyncio.sleep(RETRY_BASE_DELAY_SECONDS * attempt)
    raise RuntimeError(f"Failed to fetch {service_code}/{port}: {last_error}")


async def process_service(page, service_code, service_rules, cookie_state):
    service_rule = service_rules[service_code]
    ports = build_query_ports(service_rule, include_alternatives=True)
    print(f"Target service: {service_code}")

    raw_rows = []
    for port in ports:
        try:
            print(f"Query {service_code} / {port}")
            response_json = await query_single_port_with_retry(page, service_code, port, cookie_state)
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
    result_file = save_tables_to_excel(voyages, calls, service_code, "MULTIPORT", "RELOAD_DEDUPED")
    print(f"重载页面多港去重结果已保存: {result_file}")

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

    results = []
    batch_voyages = []
    batch_port_calls = []
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
            for service_code in target_services:
                try:
                    result = await process_service(page, service_code, service_rules, cookie_state)
                    batch_voyages.extend(result.pop("total_voyages", []))
                    batch_port_calls.extend(result.pop("total_port_calls", []))
                    results.append(result)
                    await page.wait_for_timeout(800)
                except Exception as exc:
                    print(f"Service {service_code} failed: {exc}")
                    results.append({"service": service_code, "error": str(exc)})
        finally:
            await browser.close()

    summary_path = QUERY_DIR / f"CSL_FETCH_BATCH_SUMMARY_RELOAD_{datetime.now().strftime('%y%m%d%H%M%S')}.xlsx"
    pd.DataFrame(results).to_excel(summary_path, index=False)

    detail_path = QUERY_DIR / f"CSL_FETCH_BATCH_DETAIL_RELOAD_{datetime.now().strftime('%y%m%d%H%M%S')}.xlsx"
    df_voyages = pd.DataFrame(batch_voyages).reindex(columns=VOYAGE_COLUMNS)
    df_port_calls = pd.DataFrame(batch_port_calls).reindex(columns=PORT_CALL_COLUMNS)
    with pd.ExcelWriter(detail_path, engine="openpyxl") as writer:
        df_voyages.to_excel(writer, index=False, sheet_name="Total Voyages")
        df_port_calls.to_excel(writer, index=False, sheet_name="Total PortCalls")

    print(f"Batch summary saved: {summary_path}")
    print(f"Batch detail tables saved: {detail_path}")


if __name__ == "__main__":
    asyncio.run(main())
