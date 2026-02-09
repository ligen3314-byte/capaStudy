import re
from pathlib import Path

from openpyxl import Workbook
from playwright.sync_api import sync_playwright

URL = "https://www.ezocean.com/SCHEDULE/ScheduleSearch/BYPORT?sorigin=&sdest=&sorigin=&sdest=&svesselname=&HVESSEL=&sport=&porttrade=NEU&originval=&destval=&portval="
OUT_XLSX = Path("ezocean_table.xlsx")
TIMEOUT_MS = 120000
PAGE_WAIT_MS = 1200
INITIAL_WAIT_MS = 0
MAX_PAGES = 2
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def norm_text(value: str) -> str:
    return " ".join((value or "").split())


def extract_current_page(page) -> int:
    try:
        value = page.locator("#txtnowpage").input_value().strip()
        return int(value) if value.isdigit() else 1
    except Exception:
        return 1


def read_pagination(page):
    info = page.locator(".pagination-info").first
    if info.count() == 0:
        return None, None, None

    text = norm_text(info.inner_text())
    m = re.search(r"Showing\s+(\d+)\s+to\s+(\d+)\s+of\s+(\d+)\s+rows", text, flags=re.I)
    if not m:
        return None, None, None

    shown_from = int(m.group(1))
    shown_to = int(m.group(2))
    total = int(m.group(3))
    return shown_from, shown_to, total


def parse_initial_from_detail(detail_row):
    table = detail_row.locator("table").first
    if table.count() == 0:
        return "", ""

    data_rows = table.locator("tbody tr")
    if data_rows.count() == 0:
        data_rows = table.locator("tr")

    for i in range(data_rows.count()):
        r = data_rows.nth(i)
        cells = r.locator("td")
        if cells.count() < 2:
            continue
        calling_port = norm_text(cells.nth(0).inner_text())
        arrival_date = norm_text(cells.nth(1).inner_text())
        if calling_port and calling_port.upper() != "PORT":
            return calling_port, arrival_date

    return "", ""


def normalize_initial_time(value: str) -> str:
    text = norm_text(value)
    if not text:
        return ""

    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", text)
    if not m:
        return ""

    year = int(m.group(1))
    month = int(m.group(2))
    day = int(m.group(3))
    return f"{year:04d}{month:02d}{day:02d}"


def ensure_detail_loaded(main_row, detail_row, page):
    calling_port, arrival_date = parse_initial_from_detail(detail_row)
    if calling_port or arrival_date:
        return calling_port, normalize_initial_time(arrival_date)

    toggle_candidates = [
        ":scope > td.iconcolumn a",
        ":scope > td.iconcolumn",
        ":scope > td:nth-last-child(2)",
    ]
    for selector in toggle_candidates:
        target = main_row.locator(selector).first
        if target.count() == 0:
            continue
        try:
            target.click(timeout=2000)
            page.wait_for_timeout(250)
            calling_port, arrival_date = parse_initial_from_detail(detail_row)
            if calling_port or arrival_date:
                return calling_port, normalize_initial_time(arrival_date)
        except Exception:
            continue

    return "", ""


def extract_table_from_page(page):
    table = page.locator("table#ScheduleResult").first
    header_cells = table.locator(":scope > thead > tr > th")
    raw_headers = [norm_text(header_cells.nth(i).inner_text()) for i in range(header_cells.count())]

    # Drop blank/icon columns and CMNT column.
    keep_idx = [i for i, name in enumerate(raw_headers) if name and name.upper() != "CMNT"]
    headers = [raw_headers[i] for i in keep_idx] + ["Initial Port", "Initial Time"]

    row_locator = page.locator("tbody#tbbody > tr:not(.Detail)")
    rows = []
    for i in range(row_locator.count()):
        row = row_locator.nth(i)
        detail_row = row.locator("xpath=following-sibling::tr[1][contains(@class,'Detail')]").first
        cells = row.locator(":scope > td, :scope > th")
        raw_values = [norm_text(cells.nth(j).inner_text()) for j in range(cells.count())]
        values = [raw_values[j] if j < len(raw_values) else "" for j in keep_idx]

        # Skip accidental repeated header lines inside body.
        if (
            len(values) >= 2
            and len(headers) >= 2
            and values[0].upper() == headers[0].upper()
            and values[1].upper().startswith("CARRIER")
        ):
            continue

        initial_port, initial_time = ensure_detail_loaded(row, detail_row, page)
        values.extend([initial_port, initial_time])

        if any(values):
            rows.append(values)

    return headers, rows


def collect_page_rows(page, expected_count: int, retries: int = 4):
    headers, best_rows = extract_table_from_page(page)
    best_len = len(best_rows)

    for _ in range(retries):
        if expected_count > 0 and best_len >= expected_count:
            break
        page.wait_for_timeout(500)
        _, rows = extract_table_from_page(page)
        if len(rows) > best_len:
            best_rows = rows
            best_len = len(rows)

    return headers, best_rows


def split_vessel_voyage(value: str):
    s = norm_text(value)
    if not s:
        return "", ""
    if " " not in s:
        return s, ""
    vessel, voyage = s.rsplit(" ", 1)
    return vessel.strip(), voyage.strip()


def transform_vessel_voyage_columns(headers, rows):
    try:
        idx = headers.index("Vessel Voyage")
    except ValueError:
        return headers, rows

    new_headers = headers[:idx] + ["Vessel", "Voyage"] + headers[idx + 1 :]
    new_rows = []
    for row in rows:
        vv = row[idx] if idx < len(row) else ""
        vessel, voyage = split_vessel_voyage(vv)
        new_row = row[:idx] + [vessel, voyage] + row[idx + 1 :]
        new_rows.append(new_row)

    return new_headers, new_rows


def write_xlsx(headers, rows, output_path: Path):
    target = output_path
    for _ in range(2):
        wb = Workbook()
        ws = wb.active
        ws.title = "Schedule"
        if headers:
            ws.append(headers)
        for row in rows:
            ws.append(row)
        try:
            wb.save(target)
            return target
        except PermissionError:
            target = output_path.with_name(f"{output_path.stem}_new{output_path.suffix}")

    raise PermissionError(f"????????: {output_path}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(user_agent=USER_AGENT, locale="en-US")
        page = context.new_page()

        page.goto(URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        page.wait_for_timeout(INITIAL_WAIT_MS)
        page.wait_for_selector("table#ScheduleResult", state="attached", timeout=TIMEOUT_MS)
        page.wait_for_timeout(PAGE_WAIT_MS)

        shown_from, shown_to, _ = read_pagination(page)
        expected_count = (shown_to - shown_from + 1) if shown_from and shown_to else 0
        headers, page_rows = collect_page_rows(page, expected_count=expected_count)
        shown_from, shown_to, total_rows = read_pagination(page)
        expected_count = (shown_to - shown_from + 1) if shown_from and shown_to else len(page_rows)
        all_rows = page_rows[:expected_count]
        current_page = extract_current_page(page)
        pages_collected = 1

        while True:
            if pages_collected >= MAX_PAGES:
                break

            shown_from, shown_to, total_rows = read_pagination(page)
            if shown_to is not None and total_rows is not None and shown_to >= total_rows:
                break

            next_link = page.get_by_role("link", name="next page")
            if next_link.count() == 0:
                break

            next_link.first.click()

            try:
                page.wait_for_function(
                    """
                    (oldPage) => {
                        const el = document.querySelector('#txtnowpage');
                        if (!el) return false;
                        const now = parseInt(el.value || '0', 10);
                        return now > oldPage;
                    }
                    """,
                    arg=current_page,
                    timeout=TIMEOUT_MS,
                )
            except Exception:
                page.wait_for_timeout(PAGE_WAIT_MS)

            new_page = extract_current_page(page)
            if new_page <= current_page:
                break

            current_page = new_page
            pages_collected += 1
            shown_from, shown_to, total_rows = read_pagination(page)
            expected_count = (shown_to - shown_from + 1) if shown_from and shown_to else len(page_rows)
            _, page_rows = collect_page_rows(page, expected_count=expected_count)
            all_rows.extend(page_rows[:expected_count])

            if total_rows is not None and len(all_rows) >= total_rows:
                break

        context.close()
        browser.close()

    if not all_rows:
        raise RuntimeError("未抓取到表格行，请检查 URL 或页面参数是否有效。")

    headers, all_rows = transform_vessel_voyage_columns(headers, all_rows)
    output_path = write_xlsx(headers, all_rows, OUT_XLSX)
    print(f"抓取完成：{len(all_rows)} 行")
    print(f"输出文件：{output_path.resolve()}")


if __name__ == "__main__":
    main()
