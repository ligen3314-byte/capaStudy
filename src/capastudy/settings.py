from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RUNTIME_ROOT = PROJECT_ROOT.parent / f"{PROJECT_ROOT.name}_runtime"
RUNTIME_ROOT = Path(os.getenv("CAPASTUDY_RUNTIME_DIR", str(DEFAULT_RUNTIME_ROOT))).expanduser().resolve()

# Code/Data standard folders
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = RUNTIME_ROOT / "data"
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_PROCESSED_DIR = DATA_DIR / "processed"
DATA_MERGED_DIR = DATA_DIR / "merged"
DATA_STATE_DIR = DATA_DIR / "state"
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = RUNTIME_ROOT / "logs"
ARCHIVE_DIR = RUNTIME_ROOT / "archive"

# Legacy compatibility output folders
LEGACY_ROOT_DIR = RUNTIME_ROOT / "legacy"
LEGACY_MERGED_DIR = LEGACY_ROOT_DIR / "merged_query"
LEGACY_STATE_DIR = LEGACY_MERGED_DIR / "update_state"

# Existing carrier folders
CSL_DIR = PROJECT_ROOT / "CSL FETCH"
MSC_DIR = PROJECT_ROOT / "MSC FETCH"
MSK_DIR = PROJECT_ROOT / "MSK FETCH"

# Carrier runtime output folders
CARRIERS_RUNTIME_DIR = RUNTIME_ROOT / "carriers"
CSL_RUNTIME_DIR = CARRIERS_RUNTIME_DIR / "csl"
MSC_RUNTIME_DIR = CARRIERS_RUNTIME_DIR / "msc"
MSK_RUNTIME_DIR = CARRIERS_RUNTIME_DIR / "msk"

CSL_QUERY_DIR = CSL_RUNTIME_DIR / "query"
MSC_QUERY_DIR = MSC_RUNTIME_DIR / "query"
MSK_QUERY_DIR = MSK_RUNTIME_DIR / "query"

# Common master/config files
VESSELS_DIR = PROJECT_ROOT / "vessels"
VESSEL_DB_XLSX = VESSELS_DIR / "vessels_db.xlsx"
VESSEL_ENV_PATH = VESSELS_DIR / ".env"
SERVICE_META_XLSX = PROJECT_ROOT / "services" / "service_alliance_trade.xlsx"

# Carrier-specific config files
CSL_SERVICE_RULES_XLSX = CSL_DIR / "csl_service_start_end.xlsx"
CSL_ARTIFACT_DIR = CSL_RUNTIME_DIR / "artifacts"

MSC_SERVICE_RULES_XLSX = MSC_DIR / "msc_service_start_end_filled.xlsx"
MSC_SERVICE_RULES_XLSX_FALLBACK = MSC_DIR / "msc_service_start_end_filled_filled.xlsx"
MSC_SERVICE_XLSX = MSC_DIR / "msc_service_start_end.xlsx"
MSC_SERVICE_XLSX_FALLBACK = MSC_DIR / "msc_service_start_end_filled.xlsx"

MSK_PORTS_XLSX = MSK_DIR / "msk_ports.xlsx"

