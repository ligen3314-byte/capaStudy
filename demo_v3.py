# -*- coding: utf-8 -*-
"""
Ezocean 运力按周汇总（同步版，无 async）
需求：
- 查询 origin -> destination 的所有可见航期
- 将结果按“业务周（周六~周五）”归周
- 按 data.xlsx 给定航线清单（route_name, ID）匹配并累加 Capacity
- 周列：从当前周开始，向后 16 周（Wk ~ Wk+15）
- 输出：EZOceanResult.xlsx（仅统计矩阵）

周定义：
1) 每周：周六开始，至下周周五结束（含首尾）
2) 第1周起点：2026-01-03（周六）
3) 当前周：根据系统日期 dt.date.today() 计算得到
"""

import re
import argparse
import datetime as dt
import pandas as pd
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


DEFAULT_URL = "https://www.ezocean.com/SCHEDULE/ScheduleSearch"
DEFAULT_TEMPLATE = "data.xlsx"
DEFAULT_OUTPUT = "EZOceanResult.xlsx"

# 你定义的“第1周”起点（周六）
WEEK1_START = dt.date(2026, 1, 3)

# 固定统计窗口：当前周起，向后 16 周
WEEKS_AHEAD = 16


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_args():
    p = argparse.ArgumentParser(description="Ezocean weekly capacity summary (sync)")
    p.add_argument("--url", default=DEFAULT_URL, help="Ezocean schedule search URL")
    p.add_argument("--origin", default="SHANGHAI", help="Origin 关键词")
    p.add_argument("--destination", default="LONDON GATEWAY", help="Destination 关键词")
    p.add_argument("--template", default=DEFAULT_TEMPLATE, help="航线清单 xlsx（data.xlsx）")
    p.add_argument("--output", default=DEFAULT_OUTPUT, help="输出文件名（EZOceanResult.xlsx）")
    p.add_argument("--headless", action="store_true", help="无头模式（不显示浏览器）")
    p.add_argument("--timeout", type=int, default=120000, help="页面加载/操作超时（毫秒）")
    p.add_argument("--page-wait", type=int, default=1200, help="翻页后固定等待时间（毫秒）")
    return p.parse_args()


# ========= 周编号（业务周：周六~周五） =========
def week_no_for_date(d: dt.date) -> int:
    """把日期映射到业务周编号（第1周从 WEEK1_START 开始，每7天一周）"""
    delta = (d - WEEK1_START).days
    return (delta // 7) + 1


def current_week_no(today: dt.date) -> int:
    return week_no_for_date(today)


def build_week_columns(start_week_no: int, weeks_ahead: int = WEEKS_AHEAD):
    """生成周列名：W{week_no}(mm.dd-mm.dd)，显示每个业务周的起止日期。

    业务周定义：周六开始，周五结束。week_no 从 WEEK1_START 开始计数。
    """
    cols = []
    for i in range(weeks_ahead):
        wn = start_week_no + i
        # 计算该业务周的起始日期（周六）和结束日期（周五）
        wk_start = WEEK1_START + dt.timedelta(days=(wn - 1) * 7)
        wk_end = wk_start + dt.timedelta(days=6)
        label = f"W{wn}({wk_start.month:02d}.{wk_start.day:02d}-{wk_end.month:02d}.{wk_end.day:02d})"
        cols.append(label)
    return cols


# ========= 读取航线清单（route_name, ID） =========
def load_routes(template_path: str):
    """
    从 data.xlsx 读取航线清单（前四列）：
    - 第1列：route_name
    - 第2列：ID
    - 第3列：origin（查询用）
    - 第4列：destination（查询用）
    默认认为第1行是表头
    """
    df = pd.read_excel(template_path)

    if df.shape[1] < 4:
        raise ValueError("data.xlsx 至少需要四列：route_name, ID, origin, destination（origin/destination 为第3/第4列）")

    col_route = df.columns[0]
    col_id = df.columns[1]
    col_origin = df.columns[2]
    col_dest = df.columns[3]

    # 如果存在第5列，则认为是 consortium（可选）
    col_consortium = df.columns[4] if df.shape[1] >= 5 else None

    cols = [col_route, col_id, col_origin, col_dest]
    if col_consortium is not None:
        cols.append(col_consortium)

    routes = df[cols].copy()
    routes = routes.dropna(subset=[col_route, col_id])
    routes[col_route] = routes[col_route].astype(str).str.strip()

    def to_int(x):
        try:
            return int(str(x).strip())
        except:
            return None

    routes[col_id] = routes[col_id].map(to_int)
    routes = routes.dropna(subset=[col_id]).copy()
    routes[col_id] = routes[col_id].astype(int)

    # origin/destination 保证为字符串
    routes[col_origin] = routes[col_origin].astype(str).str.strip()
    routes[col_dest] = routes[col_dest].astype(str).str.strip()

    # consortium（若存在）也规范为字符串
    if col_consortium is not None:
        routes[col_consortium] = routes[col_consortium].astype(str).str.strip()

    return col_route, col_id, col_origin, col_dest, col_consortium, routes


# ========= Playwright：页面输入/查询/抓主表 =========
def find_origin_destination_inputs(page):
    origin = page.locator(
        "input[placeholder*='Origin' i], select[placeholder*='Origin' i], #sorigin, #sorigin2"
    ).first
    dest = page.locator(
        "input[placeholder*='Destination' i], select[placeholder*='Destination' i], #sdest, #sdest2"
    ).first
    return origin, dest


def pick_from_autocomplete(page, locator, text_value: str, timeout=4000):
    locator.click()
    page.wait_for_timeout(200)
    try:
        locator.fill("")
        locator.type(text_value, delay=40)
    except:
        page.keyboard.press("Control+A")
        page.keyboard.type(text_value, delay=40)

    page.wait_for_timeout(600)

    try:
        cand = page.locator(f"text=/{re.escape(text_value)}/i").first
        cand.wait_for(state="visible", timeout=timeout)
        cand.click()
        return
    except PlaywrightTimeoutError:
        pass

    fallback = page.locator("[role='option'], li, .dropdown-item, .tt-suggestion").first
    fallback.wait_for(state="visible", timeout=timeout)
    fallback.click()


def click_search_button(page):
    try:
        page.locator("#btnSearchSchedule").click()
    except:
        page.get_by_role("button", name=re.compile(r"^SEARCH$", re.I)).click()


def extract_main_table(page) -> pd.DataFrame:
    """
    只抓主结果表：table#ScheduleResult
    且只抓 tbody#tbbody > tr:not(.Detail)（跳过明细子表）
    """
    page.wait_for_selector("table#ScheduleResult", timeout=30000)
    table = page.locator("table#ScheduleResult").first

    headers = table.locator("thead tr th")
    header_names = [norm(headers.nth(i).inner_text()) for i in range(headers.count())]

    rows = table.locator("tbody#tbbody > tr:not(.Detail)")
    data = []
    for r in range(rows.count()):
        row = rows.nth(r)
        cells = row.locator(":scope > th, :scope > td")
        vals = [norm(cells.nth(c).inner_text()) for c in range(cells.count())]
        if any(vals):
            data.append(vals)

    if not data:
        return pd.DataFrame(columns=header_names)

    max_cols = max(len(x) for x in data)
    header_names = (header_names + [""] * max_cols)[:max_cols]
    data = [row + [""] * (max_cols - len(row)) for row in data]

    df = pd.DataFrame(data, columns=header_names)

    # 删除“空表头且整列空”的 icon 列
    drop_cols = []
    for col in df.columns:
        if not str(col).strip():
            if df[col].astype(str).str.strip().replace("nan", "").eq("").all():
                drop_cols.append(col)
    if drop_cols:
        df = df.drop(columns=drop_cols)

    return df


def make_unique_key(df: pd.DataFrame) -> pd.Series:
    """
    以 ID 和 Origin ETD 作为去重关键列（优先使用）。
    回退：若不存在这两列，则用前几列拼接（兼容旧逻辑）。
    Origin ETD 会优先抽取 yyyy/mm/dd 部分以保证稳定性。
    """
    if df.empty:
        return pd.Series([], dtype=str)

    key_cols = []
    for c in ("ID", "Origin ETD"):
        if c in df.columns:
            key_cols.append(c)

    # 回退到前几列（保证兼容性）
    if not key_cols:
        key_cols = list(df.columns[: min(6, len(df.columns))])

    parts = {}
    for c in key_cols:
        if c == "Origin ETD":
            def extract_etd(v):
                if pd.isna(v):
                    return ""
                s = str(v).strip()
                m = re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s)
                if m:
                    return m.group(0)
                return s.split()[0] if s else ""
            parts[c] = df[c].map(extract_etd)
        else:
            parts[c] = df[c].fillna("").astype(str).str.strip().str.lower()

    key_df = pd.DataFrame(parts)
    return key_df.agg(" | ".join, axis=1)


def parse_vessel_voyage(detail_df: pd.DataFrame, col_idx: int = 2, col_name: str = None) -> pd.DataFrame:
    """
    从 detail_df 的第 col_idx 列或指定列名中解析出 Vessel 与 Voyage 两列。
    逻辑：
    1. 优先用字符串的换行 splitlines()（Playwright inner_text 会把 <br> 转换为换行）
    2. 否则回退正则从尾部抽取短 token 作为 Voyage，剩余为 Vessel
    3. 清洗并写入 detail_df['Vessel'] 与 detail_df['Voyage']
    """
    if detail_df.empty:
        return detail_df

    # 确定列名
    if col_name and col_name in detail_df.columns:
        src_col = col_name
    else:
        # 以 0-based 索引选列，默认第3列（index=2）
        try:
            src_col = detail_df.columns[col_idx]
        except Exception:
            # 回退到查找可能的列名
            cand = [c for c in detail_df.columns if 'vessel' in str(c).lower()]
            src_col = cand[0] if cand else detail_df.columns[0]

    def split_one(val):
        s = '' if pd.isna(val) else str(val).strip()
        if not s:
            return '', ''
        # 优先按换行切分
        if '\n' in s:
            parts = [p.strip() for p in s.splitlines() if p.strip()]
            vessel = parts[0] if parts else ''
            voyage = parts[1] if len(parts) > 1 else ''
            return vessel, voyage

        # 回退：尝试用尾部短 token 作为 voyage
        m = re.search(r"([A-Za-z0-9/-]{2,})\s*$", s)
        if m:
            voyage = m.group(1).strip()
            vessel = s[:m.start()].strip()
            # 若 vessel 为空（整个字符串是单个 token），则把原始字符串作为 vessel，voyage 清空
            if not vessel:
                return s, ''
            return vessel, voyage

        # 最后回退：全部作为 vessel
        return s, ''

    vessels = []
    voyages = []
    for v in detail_df[src_col].astype(str).tolist():
        ve, vo = split_one(v)
        vessels.append(ve)
        voyages.append(vo)

    detail_df = detail_df.copy()
    detail_df['Vessel'] = vessels
    detail_df['Voyage'] = voyages
    return detail_df


def read_now_page(page) -> int:
    """读取隐藏字段 txtnowpage（用于判断最后一页点 next 会回到 1，从而停止）"""
    try:
        v = page.locator("#txtnowpage").input_value()
        v = re.sub(r"[^\d]", "", v or "")
        return int(v) if v else 1
    except:
        return 1


def fetch_all_pages(page, page_wait_ms: int) -> pd.DataFrame:
    """
    翻页直到最后：
    - next page 在最后一页仍可点，点了会回到第一页
    - 所以：若“翻页后 nowpage 从 >1 回到 1”，立刻停止
    """
    dfs = []
    seen_row_keys = set()

    while True:
        df = extract_main_table(page)

        # 跨页去重
        if not df.empty:
            keys = make_unique_key(df)
            keep = []
            for k in keys:
                if k in seen_row_keys:
                    keep.append(False)
                else:
                    keep.append(True)
                    seen_row_keys.add(k)
            df = df.loc[keep].reset_index(drop=True)

        dfs.append(df)

        current_page = read_now_page(page)

        next_link = page.get_by_role("link", name="next page")
        if next_link.count() == 0:
            break

        try:
            next_link.click()
        except:
            break

        page.wait_for_timeout(page_wait_ms)

        new_page = read_now_page(page)
        if current_page != 1 and new_page == 1:
            break

    out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    out = out.drop_duplicates()
    return out


# ========= 汇总 =========
def summarize_capacity(detail_df: pd.DataFrame, routes_df: pd.DataFrame,
                       col_route: str, col_id: str,
                       start_week_no: int, week_cols: list) -> pd.DataFrame:
    """
    detail_df：抓到的明细（至少要有 ID / Origin ETD / Capacity）
    routes_df：给定航线清单（route_name, ID）
    week_cols：Wk..Wk+7
    输出：统计矩阵（N行，2+16列）
    """
    needed = ["ID", "Origin ETD", "Capacity"]
    missing = [c for c in needed if c not in detail_df.columns]
    if missing:
        raise KeyError(
            f"网页结果缺少必要列 {missing}，请检查抓取到的表头名称是否与预期一致。\n"
            f"实际列：{list(detail_df.columns)}"
        )

    valid_ids = set(routes_df[col_id].astype(int).tolist())

    # 包含 Vessel（若存在），以便后续聚合船名
    cols_sel = ["ID", "Origin ETD", "Capacity"]
    if "Vessel" in detail_df.columns:
        cols_sel.append("Vessel")
    df = detail_df[cols_sel].copy()

    # ID -> int
    df["ID"] = df["ID"].astype(str).str.strip()
    df["ID"] = df["ID"].apply(lambda x: int(re.sub(r"[^\d]", "", x)) if re.sub(r"[^\d]", "", x) else None)
    df = df.dropna(subset=["ID"]).copy()
    df["ID"] = df["ID"].astype(int)

    # 只保留清单内 ID（你要求：不在清单的直接忽略）
    df = df[df["ID"].isin(valid_ids)].copy()

    # ETD -> date (支持 "yyyy/mm/dd" 或 "yyyy/mm/dd x day(s) delay" 等格式)
    def extract_date_str(s):
        if pd.isna(s):
            return ""
        s = str(s).strip()
        m = re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s)
        if m:
            return m.group(0)
        parts = s.split()
        return parts[0] if parts else ""

    date_strs = df["Origin ETD"].map(extract_date_str)
    # remove infer_datetime_format (some pandas versions no longer accept it)
    dt_parsed = pd.to_datetime(date_strs, errors="coerce")
    df = df[~dt_parsed.isna()].copy()
    df["ETD_date"] = dt_parsed.dt.date

    # Capacity -> int（网页是纯数字）
    df["Capacity"] = df["Capacity"].astype(str).str.strip()
    df["Capacity"] = df["Capacity"].apply(lambda x: int(re.sub(r"[^\d]", "", x)) if re.sub(r"[^\d]", "", x) else 0)

    # 周编号与窗口过滤
    df["week_no"] = df["ETD_date"].apply(week_no_for_date)
    end_week_no = start_week_no + len(week_cols) - 1
    df = df[(df["week_no"] >= start_week_no) & (df["week_no"] <= end_week_no)].copy()

    # week_idx：0..7
    df["week_idx"] = df["week_no"] - start_week_no

    # 汇总：按 (ID, week_idx) 求和，同时聚合 Vessel（多船用 '+' 连接，按出现顺序去重）
    g_cap = df.groupby(["ID", "week_idx"], as_index=False)["Capacity"].sum()
    vessel_present = "Vessel" in df.columns
    if vessel_present:
        def agg_vessels(x):
            seen = []
            for v in x.astype(str).map(lambda s: s.strip()):
                if not v:
                    continue
                if v not in seen:
                    seen.append(v)
            return "+".join(seen)

        g_vessel = df.groupby(["ID", "week_idx"], as_index=False)["Vessel"].agg(agg_vessels)
    else:
        g_vessel = pd.DataFrame(columns=["ID", "week_idx", "Vessel"]).copy()

    # 构造输出矩阵（以 routes_df 为骨架，并为每周增加对应的 Vessel 列）
    out = routes_df.copy()

    for i, c in enumerate(week_cols):
        # 周容量列
        out[c] = 0
    # 已移除每周 Vessel 列的生成（按要求不再输出每周 Vessel 列）

    # 写回累加（容量）
    for _, row in g_cap.iterrows():
        rid = int(row["ID"])
        idx = int(row["week_idx"])
        cap = int(row["Capacity"])
        out.loc[out[col_id] == rid, week_cols[idx]] = out.loc[out[col_id] == rid, week_cols[idx]] + cap

    # 写回 Vessel 聚合结果（已注释）：按要求不再输出每周 Vessel 列
    # if not g_vessel.empty:
    #     for _, row in g_vessel.iterrows():
    #         rid = int(row["ID"])
    #         idx = int(row["week_idx"])
    #         vessels = str(row.get("Vessel", "") or "")
    #         vcol = vessel_cols[idx]
    #         # 若已有值，合并去重并保持出现顺序
    #         existing = out.loc[out[col_id] == rid, vcol].iat[0]
    #         if pd.isna(existing) or str(existing).strip() == "":
    #             out.loc[out[col_id] == rid, vcol] = vessels
    #         else:
    #             parts = [p for p in str(existing).split("+") if p]
    #             for p in str(vessels).split("+"):
    #                 if p and p not in parts:
    #                     parts.append(p)
    #             out.loc[out[col_id] == rid, vcol] = "+".join(parts)

    # 列顺序：route, id, interleaved (week, week_vessel)
    # 仅输出每周容量列（不包含 Vessel 列）
    interleaved = week_cols.copy()

    out = out[[col_route, col_id] + interleaved]
    return out


def main():
    args = parse_args()

    template_path = Path(args.template)
    if not template_path.exists():
        raise FileNotFoundError(f"找不到航线清单文件：{args.template}")

    col_route, col_id, col_origin, col_dest, col_consortium, routes_df = load_routes(args.template)

    today = dt.date.today()
    start_week_no = current_week_no(today)
    week_cols = build_week_columns(start_week_no, WEEKS_AHEAD)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=args.headless)
        page = browser.new_page()
        page.goto(args.url, wait_until="networkidle", timeout=args.timeout)

        origin_loc, dest_loc = find_origin_destination_inputs(page)
        origin_loc.wait_for(state="visible", timeout=args.timeout)
        dest_loc.wait_for(state="visible", timeout=args.timeout)

        all_details = []
        for _, route in routes_df.iterrows():
            o_val = str(route[col_origin])
            d_val = str(route[col_dest])
            route_id = int(route[col_id])
            try:
                pick_from_autocomplete(page, origin_loc, o_val)
                pick_from_autocomplete(page, dest_loc, d_val)

                click_search_button(page)
                page.wait_for_selector("table#ScheduleResult", timeout=args.timeout)

                detail = fetch_all_pages(page, page_wait_ms=args.page_wait)
                if not detail.empty:
                    # 只保留本次查询对应的 route_id 的行，避免把其他查询结果混入
                    try:
                        # 提取 ID 中的数字并转为 int，兼容网页格式如 "ID123" 或带空白
                        detail_ids = (
                            detail["ID"]
                            .astype(str)
                            .str.strip()
                            .fillna("")
                            .str.replace(r"[^\d]", "", regex=True)
                        )
                        detail = detail[detail_ids == str(route_id)].copy()
                        # 若上面是数字型比较（detail_ids numeric), coerce:
                        if detail.empty:
                            # 尝试作为 int compare
                            detail_ids_int = pd.to_numeric(detail_ids, errors="coerce")
                            detail = detail[detail_ids_int == route_id].copy()
                    except Exception:
                        # 若没有 ID 列或处理失败，跳过此条路由（并记录警告）
                        print(f"[WARN] 无法按 ID 过滤查询结果，跳过 route {route[col_route]} ({route_id})")
                        detail = pd.DataFrame()

                    if not detail.empty:
                        all_details.append(detail)
            except Exception as e:
                print(f"[WARN] 查询路由失败：{route[col_route]}({route[col_id]}), origin={o_val}, dest={d_val} -> {e}")

            # 小延时，避免请求过快
            page.wait_for_timeout(300)

        browser.close()

    # 合并所有查询结果
    detail_df = pd.concat(all_details, ignore_index=True) if all_details else pd.DataFrame()

    # 去重：多次查询可能导致重复行，使用 make_unique_key 跨页/跨次查询去重
    if not detail_df.empty:
        # 生成唯一键并去重（保留第一次出现）
        try:
            detail_df["_row_key"] = make_unique_key(detail_df)
            before = len(detail_df)
            detail_df = detail_df.drop_duplicates(subset=["_row_key"]).drop(columns=["_row_key"]).reset_index(drop=True)
            after = len(detail_df)
            print(f"[INFO] 合并明细行数：{before} -> 去重后：{after}")
        except Exception:
            # 回退到按所有列去重（若 make_unique_key 出错）
            before = len(detail_df)
            detail_df = detail_df.drop_duplicates().reset_index(drop=True)
            after = len(detail_df)
            print(f"[INFO] 合并明细行数：{before} -> 全列去重后：{after}")

        # 解析 Vessel Voyage 列（在合并后统一解析更稳妥）
        try:
            detail_df = parse_vessel_voyage(detail_df, col_idx=2)
            # 可选：导出解析后的明细以供排查
            try:
                detail_out_path = Path(args.output).with_name(f"{Path(args.output).stem}_detail_parsed.xlsx")
                detail_df.to_excel(detail_out_path, index=False)
                print(f"[OK] 已将解析后的明细导出到：{detail_out_path}")
            except Exception:
                pass
        except Exception as e:
            print(f"[WARN] 解析 Vessel Voyage 失败：{e}")

    # # 新增：把合并后的 detail_df 导出为 xlsx 以便排查
    # try:
    #     detail_out_path = Path(args.output).with_name(f"{Path(args.output).stem}_detail.xlsx")
    #     detail_df.to_excel(detail_out_path, index=False)
    #     print(f"[OK] 已将合并后的明细导出到：{detail_out_path}")
    # except Exception as e:
    #     print(f"[WARN] 导出合并明细失败：{e}")

    result_df = summarize_capacity(
        detail_df=detail_df,
        routes_df=routes_df,
        col_route=col_route,
        col_id=col_id,
        start_week_no=start_week_no,
        week_cols=week_cols,
    )

    # 输出主表与按 consortium 汇总表（若 consortium 列存在）
    try:
        with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
            result_df.to_excel(writer, sheet_name="ByRoute", index=False)

            if col_consortium is not None:
                # 用 routes_df 将 route -> consortium 映射，并按 consortium 汇总周列
                merge_map = routes_df[[col_id, col_consortium]].drop_duplicates(subset=[col_id])
                merged = result_df.merge(merge_map, on=col_id, how="left")
                # groupby consortium 并对所有周列求和
                consortium_df = (
                    merged.groupby(col_consortium)[week_cols]
                    .sum()
                    .reset_index()
                )
                consortium_df.to_excel(writer, sheet_name="ByConsortium", index=False)

        print(f"[OK] 当前周=W{start_week_no}，输出周列：{week_cols[0]}~{week_cols[-1]}（共{len(week_cols)}周）")
        print(f"[OK] 输出：{args.output} | 航线数 N={len(result_df)}")
    except Exception as e:
        # 回退到单表输出
        result_df.to_excel(args.output, index=False)
        print(f"[WARN] 无法以多 sheet 输出（{e}），已回退为单表输出：{args.output}")


if __name__ == "__main__":
    main()
