from __future__ import annotations

import multiprocessing
import os
from datetime import datetime
from pathlib import Path

TESTING = True

INDEX_SHEET_ID = "1D3SPxVV7x4gAVhH8u5RJDmu6y_IEO4etkjTQkQXExG8"
TEMPLATE_SHEET_ID = "1TP9lPQ3doKS5M_ljmU9UrFmro05yJw_vKiXYeDwP7zU"
SHARED_DRIVE_FOLDER_ID: str | None = None
ENFORCE_SHARED_DRIVE_DESTINATION = False

MAX_ROWS_PER_SHEET = 35_000

SYSTEM_SHARD_ID = int(os.environ.get("SYSTEM_SHARD_ID", 1))
CONFIG_WORKSHEET_NAME = "config"
CONFIG_SHEET_HEADERS_ROW: tuple[str, ...] = (
    "total_systems",
    "dc_write_lock",
    "hc_write_lock",
    "sc_write_lock",
)
WRITE_LOCK_POLL_SECONDS = 10.0
CLUSTER_WORKER_ID = f"s{SYSTEM_SHARD_ID}"

# ═══════════════════════════════════════════════════════════════
#  CONCURRENCY — env-driven, auto-scales to available CPU cores
# ═══════════════════════════════════════════════════════════════
_CPU_COUNT = int(
    os.environ.get("WORKER_CPU_COUNT", multiprocessing.cpu_count() or 4)
)

DETAIL_SESSION_POOL_SIZE = int(
    os.environ.get("DETAIL_SESSION_POOL_SIZE", max(20, _CPU_COUNT * 5))
)
SC_SEARCH_WORKERS = int(
    os.environ.get("SC_SEARCH_WORKERS", max(15, _CPU_COUNT * 3))
)
HC_SEARCH_WORKERS = int(
    os.environ.get("HC_SEARCH_WORKERS", max(8, _CPU_COUNT * 2))
)
DC_SEARCH_WORKERS = int(
    os.environ.get("DC_SEARCH_WORKERS", max(8, _CPU_COUNT * 2))
)
HC_DETAIL_WORKERS = int(
    os.environ.get("HC_DETAIL_WORKERS", max(40, _CPU_COUNT * 10))
)
DC_DETAIL_WORKERS = int(
    os.environ.get("DC_DETAIL_WORKERS", max(40, _CPU_COUNT * 10))
)

_pdir = Path(__file__).resolve().parent
DC_PROGRESS_FILE = str(
    _pdir / "district_court" / f"dc_progress_s{SYSTEM_SHARD_ID}.json"
)
HC_PROGRESS_FILE = str(
    _pdir / "high_court" / f"hc_progress_s{SYSTEM_SHARD_ID}.json"
)
SC_PROGRESS_FILE = str(
    _pdir / "supreme_court" / f"sc_progress_s{SYSTEM_SHARD_ID}.json"
)

_CURRENT_YEAR = datetime.today().year

DC_START_YEAR = 1950
DC_END_YEAR = _CURRENT_YEAR

HC_START_YEAR = 1950
HC_END_YEAR = _CURRENT_YEAR

SC_START_YEAR = 1950
SC_END_YEAR = _CURRENT_YEAR

# ═══════════════════════════════════════════════════════════════
#  BATCH SIZES — 5000 reduces Google Sheets API calls
# ═══════════════════════════════════════════════════════════════
WRITE_BATCH_SIZE = int(os.environ.get("WRITE_BATCH_SIZE", 5000))
DETAIL_CHUNK_SIZE = 100
DC_DETAIL_CHUNK_SIZE = DETAIL_CHUNK_SIZE
HC_DETAIL_CHUNK_SIZE = DETAIL_CHUNK_SIZE
SC_WRITE_BATCH_SIZE = int(os.environ.get("SC_WRITE_BATCH_SIZE", 5000))
HC_WRITE_BATCH_SIZE = int(os.environ.get("HC_WRITE_BATCH_SIZE", 5000))
DC_WRITE_BATCH_SIZE = int(os.environ.get("DC_WRITE_BATCH_SIZE", 5000))

DC_MIN_WRITE_SIZE = int(os.environ.get("DC_MIN_WRITE_SIZE", 5000))

SC_MAX_CONSECUTIVE_FAILURES = 10000
HC_MAX_DETAIL_RETRIES = 20
HC_TELEMETRY_EVERY = 100
DC_TELEMETRY_EVERY = 100
