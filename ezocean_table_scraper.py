import os
import os
import random
import re
import datetime as dt
from pathlib import Path

from openpyxl import Workbook
from playwright.sync_api import sync_playwright

DEFAULT_PORTTRADE = "NEU"
PORTTRADE = os.getenv("PORTTRADE", DEFAULT_PORTTRADE).strip().upper()
URL = (
    "https://www.ezocean.com/SCHEDULE/ScheduleSearch/BYPORT?"
    "sorigin=&sdest=&sorigin=&sdest=&svesselname=&HVESSEL=&sport=&"
    f"porttrade={PORTTRADE}&originval=&destval=&portval="
)
OUT_XLSX = Path("ezocean_table.xlsx")
TIMEOUT_MS = 150000

# Slow down knobs (reduce ban probability)
INITIAL_WAIT_MS_MIN = 300
INITIAL_WAIT_MS_MAX = 800
PAGE_WAIT_MS_MIN = 300
PAGE_WAIT_MS_MAX = 800
DETAIL_WAIT_MS_MIN = 300
DETAIL_WAIT_MS_MAX = 800

# 0 means no page limit.
MAX_PAGES = int(os.getenv("MAX_PAGES", "0"))
# Debug throttles (to reduce load / bans while debugging)
MAX_ROWS_PER_PAGE = int(os.getenv("MAX_ROWS_PER_PAGE", "0"))  # 0 means no limit
FETCH_DETAIL = os.getenv("FETCH_DETAIL", "1").strip() not in ("0", "false", "False", "no", "NO")

# Date filter + chunking
# - START_DATE/END_DATE: yyyymmdd / yyyy-mm-dd / yyyy/mm/dd
# - CHUNK_DAYS: 0 means no chunking
START_DATE = os.getenv("START_DATE", "20260210").strip()
END_DATE = os.getenv("END_DATE", "20260322").strip()
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "0"))
DATE_INPUT_FORMAT = "%Y/%m/%d"  # ezocean UI usually shows yyyy/mm/dd
AUTO_FILTER_WAIT_MS = 500
DEBUG_DATE = os.getenv("DEBUG_DATE", "0").strip() in ("1", "true", "True", "yes", "YES")
HEADLESS = os.getenv("HEADLESS", "0").strip() in ("1", "true", "True", "yes", "YES")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def norm_text(value: str) -> str:
    return " ".join((value or "").split())


def random_wait(page, min_ms: int, max_ms: int):
    page.wait_for_timeout(random.randint(min_ms, max_ms))


def is_blocked_page(page) -> bool:
    title = norm_text(page.title()).lower()
    body = norm_text(page.locator("body").inner_text()).lower()
    block_markers = [
        "access denied",
        "forbidden",
        "too many requests",
        "temporarily unavailable",
        "未将对象引用设置到对象的实例",
        "cloudflare",
        "captcha",
    ]
    return any(marker in title or marker in body for marker in block_markers)


def build_context_kwargs():
    return {"user_agent": USER_AGENT, "locale": "en-US"}


def parse_date(value: str):
    s = (value or "").strip()
    if not s:
        return None

    if re.fullmatch(r"\d{8}", s):
        return dt.date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    raise ValueError(f"无法解析日期: {value} (支持 yyyymmdd / yyyy-mm-dd / yyyy/mm/dd)")


def iter_date_chunks(start: dt.date, end: dt.date, chunk_days: int):
    if start is None or end is None:
        yield None, None
        return

    if end < start:
        raise ValueError(f"END_DATE({end}) 不能早于 START_DATE({start})")

    if chunk_days <= 0:
        yield start, end
        return

    cur = start
    while cur <= end:
        chunk_end = min(end, cur + dt.timedelta(days=chunk_days - 1))
        yield cur, chunk_end
        cur = chunk_end + dt.timedelta(days=1)


def _next_sunday_on_or_after(d: dt.date) -> dt.date:
    # Python weekday(): Mon=0 .. Sun=6
    return d + dt.timedelta(days=(6 - d.weekday()) % 7)


def iter_week_chunks(start: dt.date, end: dt.date):
    """
    Chunking rule:
    - Each chunk is Monday..Sunday (inclusive).
    - First chunk starts at START_DATE and ends at min(END_DATE, first Sunday on/after START_DATE).
    - Last chunk ends at END_DATE.
    """
    if start is None or end is None:
        yield None, None
        return

    if end < start:
        raise ValueError(f"END_DATE({end}) 不能早于 START_DATE({start})")

    first_end = min(end, _next_sunday_on_or_after(start))
    yield start, first_end

    cur = first_end + dt.timedelta(days=1)  # typically Monday
    while cur <= end:
        # End at Sunday of this week (or END_DATE).
        week_end = cur + dt.timedelta(days=(6 - cur.weekday()))
        chunk_end = min(end, week_end)
        yield cur, chunk_end
        cur = chunk_end + dt.timedelta(days=1)


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


def _set_input_value(locator, value: str):
    # Some date inputs are readonly (datepicker). Fallback to JS set + dispatch events.
    try:
        locator.fill(value)
    except Exception:
        pass
    else:
        try:
            current = locator.input_value()
        except Exception:
            current = ""
        if current and value in current:
            return

    locator.evaluate(
        """(el, v) => {
            el.value = v;
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
        }""",
        value,
    )


MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_FULL = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def _click_first_visible(locator, timeout_ms: int = 20000) -> bool:
    try:
        n = locator.count()
    except Exception:
        n = 0
    for i in range(min(n, 20)):
        cand = locator.nth(i)
        try:
            if cand.is_visible():
                cand.click(timeout=timeout_ms)
                return True
        except Exception:
            continue
    return False


def _resolve_datepicker_scope(page):
    # Try common popup containers first.
    for sel in [
        ".flatpickr-calendar:visible",
        ".ui-datepicker:visible",
        ".datepicker:visible",
        ".k-calendar:visible",
        "[role='dialog']:visible",
    ]:
        loc = page.locator(sel).first
        try:
            if loc.count() > 0 and loc.is_visible():
                return loc
        except Exception:
            continue
    # Fallback: no scoping possible.
    return page


def _select_date_via_role_datepicker(page, textbox_name: str, date_value: dt.date) -> bool:
    """
    Minimal codegen-style clicks only (no validation/retry/scope tricks):
    - Click DATE FROM/DATE TO textbox
    - Click month header twice
    - Click year header, then click target year
    - Click target month (Jan/Feb/Mar...)
    - Click day cell (sometimes twice)
    """
    tb = page.get_by_role("textbox", name=textbox_name).first
    if tb.count() == 0:
        return False

    month_abbr = MONTH_ABBR[date_value.month - 1]
    year_str = str(date_value.year)
    day_str = str(int(date_value.day))

    try:
        tb.click(timeout=10000)
        page.wait_for_timeout(150)

        month_header = page.get_by_role("columnheader", name=re.compile("|".join(MONTH_FULL), re.I)).first
        if month_header.count() > 0:
            month_header.click(timeout=10000)
            page.wait_for_timeout(150)
            month_header = page.get_by_role("columnheader", name=re.compile("|".join(MONTH_FULL), re.I)).first
            if month_header.count() > 0:
                month_header.click(timeout=10000)
                page.wait_for_timeout(150)

        year_header = page.get_by_role("columnheader", name=re.compile(r"^\\d{4}$")).first
        if year_header.count() > 0:
            year_header.click(timeout=10000)
            page.wait_for_timeout(150)
            _click_first_visible(page.get_by_text(year_str, exact=True), timeout_ms=10000)
            page.wait_for_timeout(150)

        _click_first_visible(page.get_by_text(month_abbr, exact=True), timeout_ms=10000)
        page.wait_for_timeout(150)

        day_cell = page.get_by_role("cell", name=day_str, exact=True)
        if not _click_first_visible(day_cell, timeout_ms=20000):
            return False
        page.wait_for_timeout(150)
        _click_first_visible(day_cell, timeout_ms=10000)
        return True
    except Exception:
        return False


def _element_signature(locator):
    try:
        el_id = locator.get_attribute("id") or ""
        el_name = locator.get_attribute("name") or ""
        el_ph = locator.get_attribute("placeholder") or ""
        el_aria = locator.get_attribute("aria-label") or ""
        return (el_id.strip(), el_name.strip(), el_ph.strip(), el_aria.strip())
    except Exception:
        return ("", "", "", "")


def _extract_ymd_date(text: str):
    s = norm_text(text)
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", s)
    if not m:
        return None
    try:
        return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except Exception:
        return None


def _textbox_has_date(tb, expected: dt.date) -> bool:
    try:
        v = tb.input_value()
    except Exception:
        try:
            v = tb.inner_text()
        except Exception:
            v = ""
    d = _extract_ymd_date(v)
    return d == expected


def set_date_range_filters(page, start: dt.date, end: dt.date):
    if start is None or end is None:
        return False

    # Prefer the simplest, most deterministic interaction:
    # fill yyyy/mm/dd into DATE FROM/DATE TO and press Tab to commit.
    start_str = start.strftime(DATE_INPUT_FORMAT)
    end_str = end.strftime(DATE_INPUT_FORMAT)

    from_tb = page.get_by_role("textbox", name="DATE FROM").first
    to_tb = page.get_by_role("textbox", name="DATE TO").first
    if from_tb.count() == 0 or to_tb.count() == 0:
        return False

    try:
        from_tb.click(timeout=10000)
    except Exception:
        pass
    try:
        from_tb.fill("", timeout=10000)
        from_tb.type(start_str, delay=80, timeout=10000)
        page.wait_for_timeout(500)
        from_tb.press("Enter", timeout=10000)
        from_tb.press("Tab", timeout=10000)
    except Exception:
        return False

    try:
        to_tb.fill("", timeout=10000)
        to_tb.type(end_str, delay=80, timeout=10000)
        page.wait_for_timeout(500)
        to_tb.press("Enter", timeout=10000)
        to_tb.press("Tab", timeout=10000)
    except Exception:
        return False

    # Optional extra Tab to move focus away (best-effort).
    try:
        page.get_by_role("link", name=re.compile(r".*\\bTop\\b.*", re.I)).first.press("Tab", timeout=2000)
    except Exception:
        pass

    # The page auto-filters after setting DATE FROM/TO. Do not click SEARCH.
    page.wait_for_timeout(AUTO_FILTER_WAIT_MS)

    return True


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
    if not FETCH_DETAIL:
        return "", ""
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
            target.click(timeout=10000)
            random_wait(page, DETAIL_WAIT_MS_MIN, DETAIL_WAIT_MS_MAX)
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
    limit = row_locator.count()
    if MAX_ROWS_PER_PAGE > 0:
        limit = min(limit, MAX_ROWS_PER_PAGE)
    for i in range(limit):
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
        random_wait(page, 400, 900)
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


def write_xlsx_sheets(sheets, output_path: Path):
    target = output_path
    for _ in range(2):
        wb = Workbook()
        if wb.worksheets:
            wb.remove(wb.worksheets[0])

        for sheet_name, headers, rows in sheets:
            ws = wb.create_sheet(title=sheet_name)
            if headers:
                ws.append(headers)
            for row in rows:
                ws.append(row)

        try:
            wb.save(target)
            return target
        except PermissionError:
            target = output_path.with_name(f"{output_path.stem}_new{output_path.suffix}")

    raise PermissionError(f"无法写入输出文件: {output_path}")


def with_timestamp(path: Path) -> Path:
    # Timestamp format requested: yymmdd hhmm
    ts = dt.datetime.now().strftime("%y%m%d %H%M")
    return path.with_name(f"{path.stem}_{ts}{path.suffix}")


def get_porttrade_from_url(url: str) -> str:
    # e.g. ...&porttrade=NEU&... or ...&porttrade=MED&...
    m = re.search(r"(?:^|[?&])porttrade=([^&]+)", url, flags=re.I)
    if not m:
        return "UNK"
    return (m.group(1) or "UNK").strip().upper()


def build_output_path(base_path: Path, porttrade: str, start: dt.date, end: dt.date) -> Path:
    ts = dt.datetime.now().strftime("%y%m%d %H%M")
    if start and end:
        period = f"{start:%Y%m%d}-{end:%Y%m%d}"
    else:
        period = "NA-NA"
    porttrade = (porttrade or "UNK").strip().upper()
    # Format: 基本文件名_porttrade(查询起止日期)_时间戳.xlsx
    return base_path.with_name(f"{base_path.stem}_{porttrade}({period})_{ts}{base_path.suffix}")


def add_id_time_column(headers, rows):
    if "IDTime" in headers:
        return headers, rows

    idx_id = headers.index("ID") if "ID" in headers else None
    idx_time = headers.index("Initial Time") if "Initial Time" in headers else None
    idx_vessel = headers.index("Vessel") if "Vessel" in headers else None

    new_headers = list(headers) + ["IDTime"]
    new_rows = []
    for row in rows:
        rid = ""
        it = ""
        vessel = ""
        if idx_id is not None and idx_id < len(row):
            rid = str(row[idx_id] or "").strip()
        if idx_time is not None and idx_time < len(row):
            it = str(row[idx_time] or "").strip()
        if idx_vessel is not None and idx_vessel < len(row):
            vessel = str(row[idx_vessel] or "").strip()

        key = f"{rid}{it}{vessel}".strip()
        new_rows.append(list(row) + [key if key else ""])

    return new_headers, new_rows


def scrape_current_query(page, period_label: str = ""):
    page.wait_for_selector("table#ScheduleResult", state="attached", timeout=TIMEOUT_MS)
    random_wait(page, PAGE_WAIT_MS_MIN, PAGE_WAIT_MS_MAX)

    shown_from, shown_to, _ = read_pagination(page)
    expected_count = (shown_to - shown_from + 1) if shown_from and shown_to else 0
    headers, page_rows = collect_page_rows(page, expected_count=expected_count)

    shown_from, shown_to, total_rows = read_pagination(page)
    expected_count = (shown_to - shown_from + 1) if shown_from and shown_to else len(page_rows)
    all_rows = page_rows[:expected_count]

    # Paginate through result pages (MAX_PAGES=0 means no limit).
    current_page = extract_current_page(page)
    pages_collected = 1
    page_size = expected_count if expected_count else 0
    total_pages = (int((total_rows + page_size - 1) / page_size) if (total_rows and page_size) else None)

    if period_label:
        print(f"[INFO] {period_label} page {pages_collected}/{total_pages or '?'} rows={len(all_rows)}")
    else:
        print(f"[INFO] page {pages_collected}/{total_pages or '?'} rows={len(all_rows)}")

    while True:
        if MAX_PAGES > 0 and pages_collected >= MAX_PAGES:
            break

        shown_from, shown_to, total_rows = read_pagination(page)
        if shown_to is not None and total_rows is not None and shown_to >= total_rows:
            break

        next_link = page.get_by_role("link", name="next page")
        if next_link.count() == 0:
            break

        random_wait(page, PAGE_WAIT_MS_MIN, PAGE_WAIT_MS_MAX)
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
            random_wait(page, PAGE_WAIT_MS_MIN, PAGE_WAIT_MS_MAX)

        if is_blocked_page(page):
            raise RuntimeError("翻页过程中疑似触发风控，请降低频率或更换网络出口。")

        new_page = extract_current_page(page)
        if new_page <= current_page:
            break

        current_page = new_page
        pages_collected += 1

        shown_from, shown_to, total_rows = read_pagination(page)
        expected_count = (shown_to - shown_from + 1) if shown_from and shown_to else len(page_rows)
        _, page_rows = collect_page_rows(page, expected_count=expected_count)
        all_rows.extend(page_rows[:expected_count])

        if period_label:
            print(f"[INFO] {period_label} page {pages_collected}/{total_pages or '?'} rows={len(all_rows)}")
        else:
            print(f"[INFO] page {pages_collected}/{total_pages or '?'} rows={len(all_rows)}")

        if total_rows is not None and len(all_rows) >= total_rows:
            break

    headers, all_rows = transform_vessel_voyage_columns(headers, all_rows)
    return headers, all_rows


def main():
    start = parse_date(START_DATE)
    end = parse_date(END_DATE)
    porttrade = get_porttrade_from_url(URL)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(**build_context_kwargs())
        page = context.new_page()

        def reload_base_url():
            loaded_local = False
            for attempt in range(1, 4):
                page.goto(URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                random_wait(page, INITIAL_WAIT_MS_MIN, INITIAL_WAIT_MS_MAX)
                if not is_blocked_page(page):
                    loaded_local = True
                    break
                page.wait_for_timeout((2**attempt) * 5000)
            if not loaded_local:
                raise RuntimeError("页面疑似被风控或封禁，请降低频率或更换网络出口后重试。")

        loaded = False
        for attempt in range(1, 4):
            page.goto(URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            random_wait(page, INITIAL_WAIT_MS_MIN, INITIAL_WAIT_MS_MAX)
            if not is_blocked_page(page):
                loaded = True
                break
            page.wait_for_timeout((2 ** attempt) * 5000)

        if not loaded:
            raise RuntimeError("页面疑似被风控或封禁，请降低频率或更换网络出口后重试。")

        # Requirement: chunk by Monday..Sunday weeks and merge all chunks into ONE sheet.
        merged_headers = None
        merged_rows = []

        first_period = True
        for chunk_start, chunk_end in iter_week_chunks(start, end):
            if not first_period:
                # Reload page between periods to reset datepicker state and reduce mis-clicks.
                reload_base_url()
            first_period = False

            if chunk_start and chunk_end:
                period_label = f"{chunk_start:%Y%m%d}-{chunk_end:%Y%m%d}"
                print(f"[INFO] Query period: {period_label}")
                applied = set_date_range_filters(page, chunk_start, chunk_end)
                if applied:
                    random_wait(page, PAGE_WAIT_MS_MIN, PAGE_WAIT_MS_MAX)

            headers, rows = scrape_current_query(page, period_label=period_label if (chunk_start and chunk_end) else "")
            if merged_headers is None:
                merged_headers = headers
            merged_rows.extend(rows)

            # Gentle pause between chunks to reduce rate.
            if chunk_start and chunk_end:
                random_wait(page, 300, 500)

        context.close()
        browser.close()

    if not merged_rows:
        raise RuntimeError("未抓取到表格行，请检查日期筛选或页面参数是否有效。")

    merged_headers, merged_rows = add_id_time_column(merged_headers or [], merged_rows)
    output_path = write_xlsx_sheets(
        [("Schedule", merged_headers or [], merged_rows)],
        build_output_path(OUT_XLSX, porttrade, start, end),
    )
    print(f"[INFO] Total rows={len(merged_rows)}")
    print(f"[INFO] Output={output_path.resolve()}")

if __name__ == "__main__":
    main()
    
