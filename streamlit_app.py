import subprocess, os, re
from datetime import date
import streamlit as st
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
SCRAPER = APP_DIR / 'ezocean_table_scraper.py'

st.set_page_config(page_title='EZOcean Scraper', layout='wide')

st.title('EZOcean Schedule Scraper')

with st.sidebar:
    st.header('Query')
    porttrade = st.selectbox('Trade (porttrade)', ['NEU', 'MED'], index=0)
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input('Start date', value=date(2026, 2, 10))
    with col2:
        end_date = st.date_input('End date', value=date(2026, 3, 22))

    st.header('Options')
    headless = st.checkbox('Headless', value=False)
    fetch_detail = st.checkbox('Fetch detail (Initial Port/Time)', value=True)
    max_pages = st.number_input('Max pages (0 = no limit)', min_value=0, value=0, step=1)
    max_rows_per_page = st.number_input('Max rows per page (0 = no limit)', min_value=0, value=0, step=1)

    debug_date = st.checkbox('Debug date logs', value=False)

run = st.button('Run Scraper', type='primary', disabled=not SCRAPER.exists())

st.caption(f"Using scraper: `{SCRAPER}`")

log_box = st.empty()
out_box = st.empty()


def run_scraper():
    env = os.environ.copy()
    env['PORTTRADE'] = porttrade
    env['START_DATE'] = start_date.strftime('%Y%m%d')
    env['END_DATE'] = end_date.strftime('%Y%m%d')
    env['MAX_PAGES'] = str(int(max_pages))
    env['MAX_ROWS_PER_PAGE'] = str(int(max_rows_per_page))
    env['FETCH_DETAIL'] = '1' if fetch_detail else '0'
    env['HEADLESS'] = '1' if headless else '0'
    env['DEBUG_DATE'] = '1' if debug_date else '0'

    cmd = ['python', str(SCRAPER)]

    proc = subprocess.run(cmd, cwd=str(APP_DIR), env=env, capture_output=True, text=True)
    stdout = proc.stdout or ''
    stderr = proc.stderr or ''

    combined = stdout
    if stderr.strip():
        combined += '\n\n[stderr]\n' + stderr

    # Try to find output path from logs.
    out_path = None
    m = re.search(r"\[INFO\]\s+Output=(.+)$", stdout, re.M)
    if m:
        out_path = m.group(1).strip()

    return proc.returncode, combined, out_path


if run:
    rc, logs, out_path = run_scraper()
    log_box.text_area('Logs', value=logs, height=420)

    if out_path and Path(out_path).exists():
        p = Path(out_path)
        out_box.success(f"Done. Output: {p}")
        out_box.download_button('Download xlsx', data=p.read_bytes(), file_name=p.name, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    else:
        if rc == 0:
            out_box.warning('Finished but could not detect output file path from logs.')
        else:
            out_box.error('Scraper failed. Check logs.')
