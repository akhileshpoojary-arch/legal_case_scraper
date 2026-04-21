from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import gspread
import gspread
from google.oauth2.service_account import Credentials

import config
from config import (
    CSV_COLUMNS,
    GOOGLE_SHEET_ID,
    INPUT_PARTY_COL,
    INPUT_COMMAND_COL,
    INPUT_STATUS_COL,
    INPUT_TAB,
    OUTPUT_START_ROW,
    OUTPUT_TAB,
    SERVICE_ACCOUNT_FILE,
    SheetStatus,
)

logger = logging.getLogger("legal_scraper.sheets")

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SheetsClient:
    """Handles all Google Sheets read/write operations with status coloring."""

    def __init__(self, credentials_file: Path | None = None) -> None:
        creds_path = credentials_file or SERVICE_ACCOUNT_FILE
        self._gc = gspread.service_account(filename=str(creds_path), scopes=_SCOPES)
        logger.info("Google Sheets client authorized")

    def _open_sheet(self, sheet_id: str | None = None) -> gspread.Spreadsheet:
        sid = sheet_id or GOOGLE_SHEET_ID
        if not sid:
            raise ValueError("GOOGLE_SHEET_ID is not set in config.py")
        return self._gc.open_by_key(sid)

    def read_parties(
        self,
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> list[dict[str, str]]:
        """Read query data from the input sheet."""
        sh = self._open_sheet(sheet_id)
        ws = sh.worksheet(tab_name or INPUT_TAB)

        parties: list[dict[str, str]] = []
        try:
            b1_val = ws.acell("B1").value or ""
            c1_val = ws.acell("C1").value or ""
            party_name = str(b1_val).strip()
            command = str(c1_val).strip()

            b2_val = ws.acell("B2").value or ""
            entity_type = str(b2_val).strip().lower()
            if entity_type in ("company", "individual"):
                config.ENTITY_TYPE = entity_type
            else:
                config.ENTITY_TYPE = "individual"
            logger.info("Entity type from B2: %s", config.ENTITY_TYPE)

            if party_name:
                parties.append({
                    "row": 1,
                    "party": party_name,
                    "status": command,
                })
        except Exception as exc:
            logger.error("Failed to read cells from sheet: %s", exc)

        logger.info(
            "Read %d party query entries from '%s'",
            len(parties),
            tab_name or INPUT_TAB,
        )
        return parties

    def write_result_to_cell(
        self,
        cell_ref: str,
        result_text: str,
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> None:
        """Write result text arbitrarily to the specified cell reference."""
        sh = self._open_sheet(sheet_id)
        ws = sh.worksheet(tab_name or INPUT_TAB)

        try:
            ws.update(range_name=cell_ref, values=[[result_text]], value_input_option="RAW")
            logger.debug("Cell %s result updated -> %s", cell_ref, result_text)
        except Exception as exc:
            logger.error("Failed to write result to %s: %s", cell_ref, exc)

    def clear_result_cells(
        self,
        cell_refs: list[str],
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> None:
        """Erase data in the specified result cells (B6-B11 typically)."""
        if not cell_refs:
            return
        sh = self._open_sheet(sheet_id)
        ws = sh.worksheet(tab_name or INPUT_TAB)
        try:
            unique_refs = sorted(list(set(cell_refs)))
            ws.batch_clear(unique_refs)
            logger.info("Cleared result cells: %s", ", ".join(unique_refs))
        except Exception as exc:
            logger.error("Failed to clear result cells: %s", exc)

    def batch_write_result_cells(
        self,
        cell_values: dict[str, str],
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> None:
        """Write multiple cell values in one batch API call."""
        if not cell_values:
            return
        sh = self._open_sheet(sheet_id)
        ws = sh.worksheet(tab_name or INPUT_TAB)
        batch_data = [
            {"range": ref, "values": [[val]]}
            for ref, val in cell_values.items()
        ]
        attempt = 0
        while True:
            try:
                ws.batch_update(batch_data, value_input_option="RAW")
                logger.debug("Batch wrote %d result cells", len(cell_values))
                return
            except Exception as exc:
                err_str = str(exc)
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    attempt += 1
                    logger.info(
                        "Batch write quota hit, waiting 60s then retrying (attempt %d)",
                        attempt,
                    )
                    import time
                    time.sleep(60)
                    continue
                logger.error("Batch write failed for result cells: %s", exc)
                return

    _CELL_CHAR_LIMIT = 50_000
    _BATCH_SIZE = 10000  # rows per batch update to avoid API payload limits

    def write_results(
        self,
        rows: list[dict],
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> int:
        """Batch write scraped rows to the output tab."""
        if not rows:
            logger.warning("No rows to write")
            return 0

        sh = self._open_sheet(sheet_id)
        target_tab = tab_name or OUTPUT_TAB

        try:
            ws = sh.worksheet(target_tab)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(
                title=target_tab,
                rows=len(rows) * 3 + 20,
                cols=len(CSV_COLUMNS) + 2,
            )
            logger.info("Created new tab: %s", target_tab)

        grid_rows = self._build_grid_with_overflow(rows)

        mode = getattr(config, "OUTPUT_MODE", "clear").strip().lower()
        if mode == "append":
            try:
                all_vals = ws.col_values(1)
                write_start = max(len(all_vals) + 1, OUTPUT_START_ROW)
            except Exception:
                write_start = OUTPUT_START_ROW
            logger.info("Append mode: writing from row %d", write_start)
        else:
            try:
                ws.batch_clear([f"A{OUTPUT_START_ROW}:Z10000"])
            except Exception:
                pass
            write_start = OUTPUT_START_ROW

        needed = write_start + len(grid_rows)
        if ws.row_count < needed:
            ws.resize(rows=needed + 50)

        total_written = 0
        for i in range(0, len(grid_rows), self._BATCH_SIZE):
            batch = grid_rows[i : i + self._BATCH_SIZE]
            row_offset = write_start + i
            attempt = 0
            while True:
                try:
                    ws.update(
                        range_name=f"A{row_offset}",
                        values=batch,
                        value_input_option="RAW",
                    )
                    break
                except Exception as exc:
                    err_str = str(exc)
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        attempt += 1
                        logger.info(
                            "Output write quota hit, waiting 60s (attempt %d)", attempt,
                        )
                        import time
                        time.sleep(60)
                        continue
                    raise
            total_written += len(batch)
            logger.info(
                "  Sheets batch %d–%d of %d written",
                i + 1, i + len(batch), len(grid_rows),
            )

        logger.info(
            "Wrote %d data rows (%d sheet rows with overflow) to '%s' [mode=%s]",
            len(rows), len(grid_rows), target_tab, mode,
        )
        return len(rows)

    def _build_grid_with_overflow(
        self, rows: list[dict],
    ) -> list[list[str]]:
        """Convert rows into sheet grid with overflow splitting."""
        grid: list[list[str]] = []
        limit = self._CELL_CHAR_LIMIT

        for row in rows:
            cell_values = [str(row.get(col, "") or "") for col in CSV_COLUMNS]

            max_chunks = 1
            for val in cell_values:
                if len(val) > limit:
                    chunks_needed = (len(val) + limit - 1) // limit
                    max_chunks = max(max_chunks, chunks_needed)

            if max_chunks == 1:
                grid.append(cell_values)
            else:
                for chunk_idx in range(max_chunks):
                    sheet_row = []
                    for col_idx, val in enumerate(cell_values):
                        if len(val) <= limit:
                            sheet_row.append(val if chunk_idx == 0 else "")
                        else:
                            start = chunk_idx * limit
                            end = start + limit
                            sheet_row.append(val[start:end])
                    grid.append(sheet_row)

        return grid

    def set_party_status(
        self,
        row_number: int,
        status: SheetStatus,
        sheet_id: str | None = None,
        tab_name: str | None = None,
        label_override: str | None = None,
    ) -> None:
        """Update the status cell for a party row."""
        sh = self._open_sheet(sheet_id)
        ws = sh.worksheet(tab_name or INPUT_TAB)

        cell_ref = f"{INPUT_STATUS_COL}{row_number}"
        label, color = status.value
        if label_override:
            label = label_override
        ws.update(range_name=cell_ref, values=[[label]], value_input_option="RAW")
        ws.format(cell_ref, {"backgroundColor": color})

        logger.debug("Row %d status → %s", row_number, label)

    def set_all_waiting(
        self,
        party_rows: list[dict],
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> None:
        """Mark all party rows as WAITING (yellow) before a run."""
        for p in party_rows:
            self.set_party_status(
                p["row"],
                SheetStatus.WAITING,
                sheet_id=sheet_id,
                tab_name=tab_name,
            )

    def set_party_command(
        self,
        row_number: int,
        command: str,
        sheet_id: str | None = None,
        tab_name: str | None = None,
    ) -> None:
        """Update the command cell (column C)."""
        sh = self._open_sheet(sheet_id)
        ws = sh.worksheet(tab_name or INPUT_TAB)
        cell_ref = f"{INPUT_COMMAND_COL}{row_number}"
        try:
            ws.update(range_name=cell_ref, values=[[command]], value_input_option="RAW")
            logger.debug("Row %d command updated -> %s", row_number, command)
        except Exception as exc:
            logger.error("Failed to update command for %s: %s", cell_ref, exc)

    def load_dynamic_config(self, sheet_id: str | None = None) -> None:
        """Reads 'Config' tab and dynamically updates config.py module variables."""
        sh = self._open_sheet(sheet_id)
        try:
            ws = sh.worksheet("Config")
            rows = ws.get_all_values()
        except gspread.WorksheetNotFound:
            logger.warning("Config tab not found in Google Sheets. Using default config.py values.")
            return

        if rows and rows[0]:
            a1_val = str(rows[0][0]).strip().lower()
            if "append" in a1_val:
                config.OUTPUT_MODE = "append"
            else:
                config.OUTPUT_MODE = "clear"
            logger.info("Output mode from Config!A1: %s", config.OUTPUT_MODE)

        data_rows = []
        started = False
        for r in rows:
            if not r: continue
            if r[0] == "Courts":
                started = True
                continue
            if started and r[0]:
                data_rows.append(r)

        active_scrapers = set()

        def parse_year(date_str: str) -> int:
            if not date_str: return datetime.today().year
            parts = date_str.split("/")
            if len(parts) == 3:
                return int(parts[2])
            try:
                return int(date_str)
            except ValueError:
                return datetime.today().year

        def parse_full_date(date_str: str) -> str:
            if not date_str: return datetime.today().strftime("%Y-%m-%d")
            parts = date_str.split("/")
            if len(parts) == 3:
                m, d, y = parts[0], parts[1], parts[2]
                if int(m) > 12:
                    d, m, y = parts[0], parts[1], parts[2]
                return f"{y}-{int(m):02d}-{int(d):02d}"
            try:
                return f"{int(date_str)}-01-01"
            except:
                return datetime.today().strftime("%Y-%m-%d")

        for r in data_rows:
            court_name = r[0].strip()
            is_active = (r[1].strip().upper() == "TRUE") if len(r) > 1 else False
            start_date_str = r[2].strip() if len(r) > 2 else ""
            end_date_str = r[3].strip() if len(r) > 3 else ""

            if not is_active:
                continue

            if court_name == "Party Name | National Company Law Tribunal":
                active_scrapers.add("nclt")
                config.NCLT_YEAR_FROM = parse_year(start_date_str)
                config.NCLT_YEAR_TO = parse_year(end_date_str)
            elif court_name == "Party Name | Supreme Court of India":
                active_scrapers.add("supreme_court")
                config.SCI_YEAR_FROM = parse_year(start_date_str)
                config.SCI_YEAR_TO = parse_year(end_date_str)
            elif court_name == "High Court":
                active_scrapers.add("high_court")
                config.HC_YEAR_FROM = parse_year(start_date_str)
                config.HC_YEAR_TO = parse_year(end_date_str)
            elif court_name == "District Courts of India":
                active_scrapers.add("district_court")
                config.DC_YEAR_FROM = parse_year(start_date_str)
                config.DC_YEAR_TO = parse_year(end_date_str)
            elif court_name == "e-Jagriti":
                active_scrapers.update(["ncdrc", "scdrc", "dcdrc"])
                config.E_JAGRITI_DATE_FROM = parse_full_date(start_date_str)
                config.E_JAGRITI_DATE_TO = parse_full_date(end_date_str)
            elif court_name == "Debts Recovery Appellate Tribunals":
                active_scrapers.update(["drt", "drat"])
            elif court_name == "Debts Recovery Tribunals":
                active_scrapers.add("drt")

        if active_scrapers:
            new_active = []
            preferred_order = [
                "drt", "drat", "nclt", "ncdrc", "scdrc", "dcdrc",
                "high_court", "district_court", "supreme_court"
            ]
            for s in preferred_order:
                if s in active_scrapers:
                    new_active.append(s)

            config.ACTIVE_SCRAPERS = new_active
            logger.debug("ACTIVE_SCRAPERS: %s", config.ACTIVE_SCRAPERS)
        else:
            logger.warning("No active scrapers found in Config tab. Using existing config.")

