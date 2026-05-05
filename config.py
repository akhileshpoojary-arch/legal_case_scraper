"""
Centralized configuration — sheet IDs, toggles, status colors, columns.

Toggle which scrapers run via ACTIVE_SCRAPERS.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from enum import Enum
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
#  MODE
# ═══════════════════════════════════════════════════════════════
def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


TESTING = _env_bool("TESTING", False)

# ═══════════════════════════════════════════════════════════════
#  ACTIVE SCRAPERS  (only these run when main.py is executed)
# ═══════════════════════════════════════════════════════════════
ACTIVE_SCRAPERS: list[str] = [
    "drt",
    "drat",
    "nclt",
    "ncdrc",
    "scdrc",
    "dcdrc",
    "high_court",
    "district_court",
    "supreme_court",
]

E_JAGRITI_DATE_FROM = "1900-01-01"
E_JAGRITI_DATE_TO = "2026-12-31"

SCI_YEAR_FROM = 2025
SCI_YEAR_TO = datetime.today().year

# ═══════════════════════════════════════════════════════════════
#  SCRAPER-SPECIFIC DATE RANGES
# ═══════════════════════════════════════════════════════════════
NCLT_YEAR_FROM = 2007
NCLT_YEAR_TO = datetime.today().year

HC_YEAR_FROM = 2025
HC_YEAR_TO = datetime.today().year

DC_YEAR_FROM = 2026
DC_YEAR_TO = datetime.today().year

# DC test limits (set to None for full production runs)
DC_LIMIT_ESTABLISHMENTS: int | None = None
DC_LIMIT_CASES: int | None = None

# ═══════════════════════════════════════════════════════════════
#  RESULT CELL references (where count is written in the Sheet)
# ═══════════════════════════════════════════════════════════════
SCRAPER_RESULT_CELLS: dict[str, str] = {
    "drt": "B11",
    "drat": "B11",
    "nclt": "B6",
    "ncdrc": "B10",
    "scdrc": "B10",
    "dcdrc": "B10",
    "high_court": "B8",
    "district_court": "B9",
    "supreme_court": "B7",
}

# ═══════════════════════════════════════════════════════════════
#  CONCURRENCY  (env-driven for Railway auto-scaling)
# ═══════════════════════════════════════════════════════════════
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", 20))
REQUEST_DELAY = float(os.environ.get("REQUEST_DELAY", "0.03"))
MAX_RETRIES = 5
RETRY_DELAY = 1.0

MAX_FAILURES_BEFORE_ROTATE = 5

HTTP_CLIENT = "aiohttp"

# ═══════════════════════════════════════════════════════════════
#  PROXY ROTATION  (env var PROXY_LIST or local file)
# ═══════════════════════════════════════════════════════════════
def _resolve_proxy_file() -> Path:
    """Load proxy list from PROXY_LIST env var or fall back to local file."""
    env_proxies = os.environ.get("PROXY_LIST")
    if env_proxies:
        tmp = Path(tempfile.gettempdir()) / "webshare_proxies.txt"
        tmp.write_text(env_proxies)
        return tmp
    return Path(__file__).resolve().parent / "Webshare proxies.txt"

PROXY_FILE = _resolve_proxy_file()
PROXY_BAN_DURATION = 300
MAX_PROXY_FAILURES = 10

# ═══════════════════════════════════════════════════════════════
#  PARALLEL WORKERS (District Court)
# ═══════════════════════════════════════════════════════════════
DC_PARALLEL_WORKERS = MAX_CONCURRENT

# ═══════════════════════════════════════════════════════════════
#  GOOGLE SHEETS  (env var SERVICE_ACCOUNT_JSON for Railway)
# ═══════════════════════════════════════════════════════════════
def _resolve_service_account() -> Path:
    """Load from SERVICE_ACCOUNT_JSON env var or fall back to local file."""
    env_json = os.environ.get("SERVICE_ACCOUNT_JSON")
    if env_json:
        data = json.loads(env_json)
        tmp = Path(tempfile.gettempdir()) / "service_account.json"
        tmp.write_text(json.dumps(data))
        return tmp
    return Path(__file__).resolve().parent / "service_account.json"

SERVICE_ACCOUNT_FILE = _resolve_service_account()

GOOGLE_SHEET_ID = "1zaiFbh8keYHZwOoVWU4I5A_QDnkQ1RAND3MhWR0i5bc"
INPUT_TAB = "Search Console"
OUTPUT_TAB = "Output Format"

# Party name in B1; entity type in B2; command in C1; status in E1
INPUT_PARTY_COL = "B"
INPUT_COMMAND_COL = "C"
INPUT_STATUS_COL = "E"
OUTPUT_START_ROW = 2

# "append" keeps existing data; "clear" wipes output tab before writing
OUTPUT_MODE: str = "clear"

# "company" triggers name augmentation in sheet search; "individual" uses raw name
ENTITY_TYPE: str = "individual"


class SheetStatus(Enum):
    """Statuses active for the scraper."""

    WAITING = (
        "WAITING",
        {"red": 1.0, "green": 0.85, "blue": 0.0},
    )
    RUNNING = (
        "RUNNING",
        {"red": 0.0, "green": 0.8, "blue": 0.2},
    )
    ERROR = (
        "ERROR",
        {"red": 0.92, "green": 0.30, "blue": 0.30},
    )


# ═══════════════════════════════════════════════════════════════
#  OUTPUT COLUMNS (Unified schema)
# ═══════════════════════════════════════════════════════════════
CSV_COLUMNS = [
    "partyName",
    "caseNumber",
    "courtNumber",
    "registrationDate",
    "nextListingDate",
    "respondent",
    "otherRespondent",
    "respondentAdvocate",
    "petitioner",
    "otherPetitioner",
    "petitionerAdvocate",
    "location",
    "courtType",
    "benchName",
    "caseType",
    "caseStatus",
    "uniqueness",
    "listingHistory",
    "applicationDetails",
    "status",
]

# ═══════════════════════════════════════════════════════════════
#  COMMON HTTP HEADERS
# ═══════════════════════════════════════════════════════════════
COMMON_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}
