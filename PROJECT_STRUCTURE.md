# capaStudy Project Structure (Refactored)

## 1) Code (`src/`)
- `src/capastudy/settings.py`
  - Shared project paths and file locations (single source of truth).
- `src/capastudy/merge_all_carriers.py`
  - Main merge + enrichment + incremental update pipeline.
- `src/capastudy/carriers/msc_fetch.py`
- `src/capastudy/carriers/msk_fetch.py`
- `src/capastudy/carriers/csl_fetch.py`
  - Carrier fetch implementations moved here.
- `src/capastudy/automation/msc_playwright.py`
- `src/capastudy/automation/csl_fetch_automation.py`
  - Browser automation helpers moved here.
- Root `merge_all_carriers.py`
  - Compatibility launcher; existing commands still work.
- Root `run_pipeline.py`
  - Cross-platform unified pipeline entry (`all` / `--only-fetch` / `--only-merge`).

## 2) Carrier Execution Directories (kept for compatibility)
- `CSL FETCH/` -> `CSL_FETCH.py`, `csl_query/`
- `MSC FETCH/` -> `MSC_FETCH.py`, `MSC_Playwright.py`, `msc_query/`
- `MSK FETCH/` -> `MSK_FETCH.py`, `msk_query/`

## 3) Config and Master Data
- `services/service_alliance_trade.xlsx`
- `vessels/vessels_db.xlsx`
- `vessels/.env`

## 4) Outputs
- New standard output:
  - `data/merged/` (timestamp merged files)
  - `data/state/` (`current`, `history`, `changes`, `snapshot`)
- Backward-compatible mirrors still written to:
  - `merged_query/`
  - `merged_query/update_state/`

## 5) Reserved Directories
- `config/` (for future centralized configs)
- `data/raw/`, `data/processed/` (future raw/processed split)
- `logs/`, `archive/`

## Notes
- Existing run scripts (`run_full_pipeline.bat/.sh`) do not need changes.
- Run modes:
  - default: full fetch + merge
  - `--only-fetch`: fetch only
  - `--only-merge`: merge only
- `run_full_pipeline.bat/.sh` now delegate to `run_pipeline.py`.
- Existing operator habits based on `merged_query/` continue to work.
- Existing entry scripts under `MSC FETCH/`, `MSK FETCH/`, `CSL FETCH/` are now thin launchers.
- Existing automation scripts under `MSC FETCH/` and `CSL FETCH/` are now thin launchers.
