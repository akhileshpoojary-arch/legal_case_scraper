"""
Local SQLite index of collected court cases.

Why this module exists
----------------------
Searching a party name used to open *every* paginated Google Sheet (High Court
alone is ~22 sheets) and run several reads per sheet, which quickly blew past
Google's read quota.

Instead, the 24/7 collector now records every case it writes into this small
local SQLite file, and on-demand search queries it directly — instant, with
**zero Google Sheets reads**.

The same DB is the de-duplication source of truth: a case is identified by
``row_dedup_key()`` (see ``utils/sheet_dedup.py``) and stored at most once.

Zero setup: ``sqlite3`` ships with Python and the DB file is created
automatically the first time it is opened.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
import unicodedata
from pathlib import Path
from typing import Any, Iterable

from utils.sheet_dedup import row_dedup_key

logger = logging.getLogger("legal_scraper.case_index")

# DB lives under legal_case_scraper/data/ (gitignored; mounted as a Docker volume
# so it survives container restarts).
_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "legal_index.db"

# Common legal-name suffixes that carry little signal for candidate lookup.
# Dropping them keeps FTS recall high for both individuals and companies
# (e.g. a query for "PRIVATE LIMITED" still finds a row stored as "PVT LTD").
# The precise matchers (utils/party_search, utils/company_augment) run *after*
# this FTS pass and handle the exact abbreviation rules.
_FTS_STOPWORDS = {
    "PVT", "PRIVATE", "PRIV", "LTD", "LIMITED", "LIM", "CO", "COMPANY",
    "CORP", "CORPORATION", "LLP", "AND", "THE", "OF", "MS", "INC",
}


def _fts_query(party_name: str) -> str:
    """Build an FTS5 MATCH string of the distinctive (multi-char) name tokens.

    Tokens are double-quoted so punctuation can never be read as an FTS operator.
    Multiple quoted tokens are implicitly AND-ed by FTS5, which gives a small,
    high-recall candidate set for the precise matcher to filter.

    Accents are folded first (e.g. 'JOSÉ' -> 'JOSE') to match the FTS tokenizer,
    which also folds accents when it indexes the stored names.
    """
    folded = unicodedata.normalize("NFKD", party_name or "")
    folded = "".join(c for c in folded if not unicodedata.combining(c))
    cleaned = re.sub(r"[^0-9A-Za-z\s]", " ", folded.upper())
    tokens = [t for t in cleaned.split() if len(t) >= 2]
    distinctive = [t for t in tokens if t not in _FTS_STOPWORDS]
    use = distinctive or tokens
    if not use:
        return ""
    return " ".join('"' + t.replace('"', '""') + '"' for t in use)


class CaseIndex:
    """A tiny thread-safe wrapper around a single SQLite connection."""

    _instance: "CaseIndex | None" = None
    _instance_lock = threading.Lock()

    def __init__(self, db_path: Path | str = _DB_PATH) -> None:
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False: the collector calls us from executor threads.
        # A single lock serializes every DB operation, so this stays safe.
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._create_schema()
        logger.info("Case index ready at %s", self._path)

    @classmethod
    def get_or_create(cls, db_path: Path | str = _DB_PATH) -> "CaseIndex":
        """Return a shared, process-wide ``CaseIndex`` (created on first call)."""
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = cls(db_path)
            return cls._instance

    # ------------------------------------------------------------------ schema
    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS cases (
                    dedup_key        TEXT PRIMARY KEY,
                    court_type       TEXT NOT NULL,
                    sheet_id         TEXT,
                    case_number      TEXT,
                    reg_date         TEXT,
                    respondent       TEXT,
                    otherRespondent  TEXT,
                    petitioner       TEXT,
                    otherPetitioner  TEXT,
                    data             TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_cases_court ON cases(court_type);

                -- Full-text index over the four searchable party fields.
                -- 'content=cases' makes this an external-content table that
                -- mirrors the cases table; the triggers below keep it in sync.
                CREATE VIRTUAL TABLE IF NOT EXISTS cases_fts USING fts5(
                    respondent, otherRespondent, petitioner, otherPetitioner,
                    content='cases', content_rowid='rowid'
                );

                CREATE TRIGGER IF NOT EXISTS cases_ai AFTER INSERT ON cases BEGIN
                    INSERT INTO cases_fts(rowid, respondent, otherRespondent,
                                          petitioner, otherPetitioner)
                    VALUES (new.rowid, new.respondent, new.otherRespondent,
                            new.petitioner, new.otherPetitioner);
                END;
                CREATE TRIGGER IF NOT EXISTS cases_ad AFTER DELETE ON cases BEGIN
                    INSERT INTO cases_fts(cases_fts, rowid, respondent,
                                          otherRespondent, petitioner, otherPetitioner)
                    VALUES ('delete', old.rowid, old.respondent,
                            old.otherRespondent, old.petitioner, old.otherPetitioner);
                END;
                """
            )
            self._conn.commit()

    # --------------------------------------------------------------- dedup read
    def existing_keys(self, court_type: str, keys: Iterable[str]) -> set[str]:
        """Return the subset of ``keys`` already stored for this court."""
        key_list = [k for k in keys if k]
        if not key_list:
            return set()
        found: set[str] = set()
        with self._lock:
            cur = self._conn.cursor()
            # Chunk to stay under SQLite's bound-variable limit.
            for i in range(0, len(key_list), 500):
                chunk = key_list[i : i + 500]
                placeholders = ",".join("?" * len(chunk))
                rows = cur.execute(
                    f"SELECT dedup_key FROM cases "
                    f"WHERE court_type=? AND dedup_key IN ({placeholders})",
                    [court_type, *chunk],
                ).fetchall()
                found.update(r[0] for r in rows)
        return found

    # -------------------------------------------------------------- index write
    def add_cases(
        self,
        court_type: str,
        cases: list[dict[str, Any]],
        sheet_id: str | None = None,
    ) -> int:
        """Insert cases (deduplicated by ``row_dedup_key``). Returns rows added."""
        if not cases:
            return 0
        rows: list[tuple[Any, ...]] = []
        for case in cases:
            key = row_dedup_key(case)
            if not key:
                continue
            rows.append(
                (
                    key,
                    court_type,
                    sheet_id or "",
                    str(case.get("caseNumber", "")),
                    str(case.get("registrationDate", "")),
                    str(case.get("respondent", "")),
                    str(case.get("otherRespondent", "")),
                    str(case.get("petitioner", "")),
                    str(case.get("otherPetitioner", "")),
                    json.dumps(case, ensure_ascii=False),
                )
            )
        if not rows:
            return 0
        with self._lock:
            cur = self._conn.cursor()
            # rowcount uses sqlite3_changes(), which excludes the FTS trigger
            # writes, so it reports only real inserts into the cases table.
            cur.executemany(
                """
                INSERT OR IGNORE INTO cases
                  (dedup_key, court_type, sheet_id, case_number, reg_date,
                   respondent, otherRespondent, petitioner, otherPetitioner, data)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            self._conn.commit()
            return cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0

    # -------------------------------------------------------------- name search
    def search(
        self,
        party_name: str,
        court_type: str | None = None,
        match_fn: "Any" = None,
        limit: int = 20000,
    ) -> list[dict[str, Any]]:
        """Return full case dicts whose party fields match the query.

        FTS5 first narrows on the distinctive name tokens (fast). If
        ``match_fn`` is given it is applied to each candidate's four party
        fields (small strings) and only the matches have their full row loaded
        — so memory stays bounded even for common names. The precise matchers
        live in the caller (``daily_run/sheet_search.py``).
        """
        fts = _fts_query(party_name)
        if not fts:
            return []

        sql = (
            "SELECT f.rowid, f.respondent, f.otherRespondent, "
            "f.petitioner, f.otherPetitioner FROM cases_fts f"
        )
        where = ["cases_fts MATCH ?"]
        params: list[Any] = [fts]
        if court_type:
            sql += " JOIN cases c ON c.rowid = f.rowid"
            where.append("c.court_type = ?")
            params.append(court_type)
        sql += " WHERE " + " AND ".join(where) + " LIMIT ?"
        params.append(limit)

        with self._lock:
            cur = self._conn.cursor()
            try:
                candidates = cur.execute(sql, params).fetchall()
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "FTS query failed for %r (%s); returning no candidates",
                    party_name,
                    exc,
                )
                return []

            if match_fn is not None:
                matched_ids = [
                    row[0]
                    for row in candidates
                    if any(match_fn(row[i] or "") for i in range(1, 5))
                ]
            else:
                matched_ids = [row[0] for row in candidates]

            if not matched_ids:
                return []

            out: list[dict[str, Any]] = []
            for i in range(0, len(matched_ids), 500):
                chunk = matched_ids[i : i + 500]
                placeholders = ",".join("?" * len(chunk))
                for r in cur.execute(
                    f"SELECT data FROM cases WHERE rowid IN ({placeholders})", chunk
                ):
                    try:
                        out.append(json.loads(r[0]))
                    except Exception:
                        continue
        return out

    # ---------------------------------------------------------------- telemetry
    def count(self, court_type: str | None = None) -> int:
        with self._lock:
            cur = self._conn.cursor()
            if court_type:
                return cur.execute(
                    "SELECT COUNT(*) FROM cases WHERE court_type=?", (court_type,)
                ).fetchone()[0]
            return cur.execute("SELECT COUNT(*) FROM cases").fetchone()[0]


# ════════════════════════════════════════════════════════════════════════════
#  Backfill: import already-collected Google Sheet data into the index once.
# ════════════════════════════════════════════════════════════════════════════
# Field that marks the start of a new logical case. Overflow "continuation"
# rows (created when a cell exceeds the 50K Sheets limit) leave these blank, so
# a blank row is appended onto the previous case instead of starting a new one.
_ANCHOR_FIELDS = ("uniqueness", "caseNumber", "respondent", "petitioner")


def _reassemble_cases(header: list[str], values: list[list[str]]) -> list[dict[str, Any]]:
    """Turn raw sheet rows (with overflow continuation rows) into logical cases."""
    cols = [h.strip() for h in header]
    anchor_idx = [i for i, h in enumerate(cols) if h in _ANCHOR_FIELDS]
    cases: list[dict[str, Any]] = []
    for row in values[1:]:  # skip header
        is_anchor = any(
            idx < len(row) and str(row[idx]).strip() for idx in anchor_idx
        )
        if is_anchor or not cases:
            cases.append(
                {cols[i]: (row[i] if i < len(row) else "") for i in range(len(cols))}
            )
        else:
            # Continuation row: append each overflow chunk onto the open case.
            open_case = cases[-1]
            for i in range(len(cols)):
                chunk = row[i] if i < len(row) else ""
                if chunk:
                    open_case[cols[i]] = str(open_case.get(cols[i], "")) + str(chunk)
    return cases


def backfill(courts: tuple[str, ...] = ("dc", "hc", "sc")) -> None:
    """One-time import of existing sheet rows into the local index.

    Idempotent: re-running only adds rows the index has not seen. After this,
    on-demand search never needs to read Google Sheets again.
    """
    from daily_run.sheets_manager import DailyRunSheetsManager  # lazy: avoid cycle

    mgr = DailyRunSheetsManager()
    index = CaseIndex.get_or_create()

    for court in courts:
        sheet_ids = mgr._get_sheet_ids_for_court(court)
        logger.info("[%s] Backfill: %d spreadsheet(s).", court.upper(), len(sheet_ids))
        court_added = 0
        for sid in sheet_ids:
            try:
                ws = mgr._gc.open_by_key(sid).get_worksheet(0)
                values = ws.get_all_values()
            except Exception as exc:
                logger.warning("[%s] Backfill skip sheet %s: %s", court.upper(), sid, exc)
                continue
            if len(values) < 2:
                continue
            cases = _reassemble_cases(values[0], values)
            added = index.add_cases(court, cases, sid)
            court_added += added
            logger.info(
                "[%s] %s: %d rows -> %d new cases indexed.",
                court.upper(), sid[:8], len(cases), added,
            )
        logger.info(
            "[%s] Backfill done: +%d new, total %d cases indexed.",
            court.upper(), court_added, index.count(court),
        )


if __name__ == "__main__":
    import sys

    from utils.logging_utils import setup_logger

    setup_logger()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "backfill"
    if cmd == "backfill":
        backfill()
    elif cmd == "count":
        idx = CaseIndex.get_or_create()
        for c in ("dc", "hc", "sc"):
            print(f"{c}: {idx.count(c)}")
        print(f"total: {idx.count()}")
    else:
        print(f"Unknown command: {cmd!r}. Use 'backfill' or 'count'.")
