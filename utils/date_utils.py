"""
Date formatting utility for all legal scrapers.
==============================================
Standard format: DD MMM YYYY (e.g. 09 JAN 2025)
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger("legal_scraper.date")

_MONTH_NAMES = [
    "", "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
]


def format_date(raw: str | None) -> str:
    """
    Parse 'DD-MM-YYYY' or 'YYYY-MM-DD' (optionally with trailing time) into
    'DD MMM YYYY' string, e.g. '09 JAN 2025'.  Returns '' on failure.
    """
    if not raw:
        return ""
    
    s = str(raw).strip()
    if s.lower() in ("", "na", "null", "none"):
        return ""
    
    # Strip trailing timestamps if present (look for colon)
    if ":" in s:
        s = s.split(" ")[0].strip()
    
    for sep in ("-", "/"):
        parts = s.split(sep)
        if len(parts) == 3:
            try:
                # Handle leading zeros and different lengths (e.g. "9-1-2025")
                p1, p2, p3 = int(parts[0]), int(parts[1]), int(parts[2])
                
                # Detect format: YYYY-MM-DD vs DD-MM-YYYY
                if p1 > 31:                 # YYYY-MM-DD
                    day, month, year = p3, p2, p1
                else:                      # DD-MM-YYYY
                    day, month, year = p1, p2, p3
                
                if 1 <= month <= 12:
                    return f"{day:02d} {_MONTH_NAMES[month]} {year}"
            except ValueError:
                continue
                
    # If the above fails, try a simple regex for common space-separated formats
    # like "09 JAN 2025" or "09-JAN-2025"
    m = re.search(r'(\d{1,2})[-\s\/]?([A-Za-z]{3})[-\s\/]?(\d{4})', s)
    if m:
        day_str, mon_str, year_str = m.groups()
        try:
            day = int(day_str)
            mon_str = mon_str.upper()
            if mon_str in _MONTH_NAMES:
                return f"{day:02d} {mon_str} {year_str}"
        except ValueError:
            pass

    logger.debug("Could not parse date: %r", raw)
    return ""
