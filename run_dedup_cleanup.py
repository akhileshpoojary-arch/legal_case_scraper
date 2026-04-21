"""Audit and optionally remove duplicate daily-run rows from Google Sheets."""

from __future__ import annotations

import argparse
import sys

from daily_run.sheets_manager import DailyRunSheetsManager
from utils.logging_utils import setup_logger

logger = setup_logger()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit duplicates across the daily-run DC/HC/SC paginated sheets by "
            "the `uniqueness` column (with fallback key fields)."
        )
    )
    parser.add_argument(
        "--court",
        choices=("all", "dc", "hc", "sc"),
        default="all",
        help="Which court sheets to audit or clean.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete later duplicate row groups instead of audit-only mode.",
    )
    return parser.parse_args()


def _selected_courts(raw: str) -> list[str]:
    if raw == "all":
        return ["dc", "hc", "sc"]
    return [raw]


def main() -> int:
    args = _parse_args()
    manager = DailyRunSheetsManager()

    total_duplicate_groups = 0
    total_deleted_rows = 0
    total_deleted_groups = 0

    for court_type in _selected_courts(args.court):
        summary = manager.cleanup_duplicate_groups(
            court_type,
            apply_delete=bool(args.apply),
        )
        total_duplicate_groups += int(summary.get("duplicate_groups", 0))
        total_deleted_rows += int(summary.get("deleted_rows", 0))
        total_deleted_groups += int(summary.get("deleted_groups", 0))

        logger.info(
            "[%s] Duplicate audit: spreadsheets=%d logical_rows=%d unique_keys=%d duplicate_groups=%d delete_ranges=%d applied=%s",
            court_type.upper(),
            int(summary.get("spreadsheets", 0)),
            int(summary.get("logical_rows", 0)),
            int(summary.get("unique_keys", 0)),
            int(summary.get("duplicate_groups", 0)),
            int(summary.get("delete_range_count", 0)),
            "yes" if args.apply else "no",
        )

        duplicates = list(summary.get("duplicates", []))
        for dup in duplicates[:10]:
            logger.info(
                "[%s] Duplicate key=%s keep=%s:%s-%s drop=%s:%s-%s",
                court_type.upper(),
                str(dup.get("dedup_key", "")),
                str(dup.get("first_sheet_id", ""))[:12],
                dup.get("first_start_row", ""),
                dup.get("first_end_row", ""),
                str(dup.get("sheet_id", ""))[:12],
                dup.get("start_row", ""),
                dup.get("end_row", ""),
            )

        if len(duplicates) > 10:
            logger.info(
                "[%s] Duplicate sample truncated: %d more groups not shown.",
                court_type.upper(),
                len(duplicates) - 10,
            )

    logger.info(
        "Duplicate cleanup summary: duplicate_groups=%d deleted_groups=%d deleted_rows=%d mode=%s",
        total_duplicate_groups,
        total_deleted_groups,
        total_deleted_rows,
        "apply" if args.apply else "audit",
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(130)
