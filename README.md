# Legal Case Scraper

Scraper service for Indian legal case data. It has two run modes:

- `main.py`: on-demand party search from the Search Console Google Sheet.
- `run_all.py`: continuous 24/7 daily-run extraction for District Court, High Court, and Supreme Court.

## What It Does

- Searches DRT, DRAT, NCLT, e-Jagriti, High Court, District Court, and Supreme Court sources.
- Reads party name, entity type, command, and config from Google Sheets.
- Writes normalized case rows back to Google Sheets.
- Uses proxy/session rotation and bounded async workers.
- Solves captchas with bundled models and logs success rate.
- Resumes daily runs from progress files.

## Search Improvements

Company search now handles common court-data variations:

- `PRIVATE LIMITED`, `PVT LTD`, `PVT LIMITED`, `PRIVATE LTD`, `LTD`, `LIMITED`
- `COMPANY` / `CO`, `CORPORATION` / `CORP`, `LLP`
- `(I)`, `(IND)`, `(INDIA)` normalized as `INDIA`
- punctuation and dotted acronym variants such as `M.K.` / `MK` / `M K`
- Jaro-Winkler scoring for near matches

## Local Case Index (fast search, no quota)

DC/HC/SC data is collected once by the 24/7 collector and stored across many
paginated Google Sheets. Searching them directly used to open *every* sheet and
exhaust the Google Sheets read quota.

Instead, the collector now records every case it writes into a small local
SQLite index (`data/legal_index.db`, created automatically — `sqlite3` ships
with Python, nothing to install). On-demand search queries that index:

- **Near-instant** party search with **zero Google Sheets reads** for DC/HC/SC.
- The index is also the **de-duplication source of truth**, so restarts never
  re-collect the same case (the old multi-hundred-MB in-RAM dedup cache is gone).
- Full-text search (FTS5) narrows on the name tokens; the same precise,
  abbreviation-aware matchers as before decide the final results.

If you already have data in Sheets from before this change, it is imported into
the index automatically the **first time `run_all.py` starts** with an empty
index (one-time; it also stops the collector re-appending old cases). You can
also run it manually at any time — it is idempotent:

```bash
python -m daily_run.case_index backfill   # one-time; idempotent
python -m daily_run.case_index count      # show indexed counts per court
```

After that, search never needs to read Google Sheets again.

## Run Locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On-demand Search Console mode:

```bash
python main.py
```

Daily-run all courts:

```bash
python run_all.py
```

Run one daily scraper:

```bash
python run_dc.py
python run_hc.py
python run_sc.py
```

## Required Secrets

Use environment variables in production:

```bash
export SERVICE_ACCOUNT_JSON='{"type":"service_account", ...}'
export PROXY_LIST='ip:port:user:pass
ip:port:user:pass'
```

Local fallback files:

- `service_account.json`
- `Webshare proxies.txt`

## Run with Docker (one click)

The simplest way to run 24/7 on your own machine. Put `service_account.json`
and `Webshare proxies.txt` in this folder, then:

```bash
docker compose up -d              # builds once, starts both services
docker compose logs -f collector  # watch the 24/7 collector
docker compose logs -f search     # watch on-demand search
docker compose down               # stop everything
```

Two services run:

- `collector` — `run_all.py`, the 24/7 DC/HC/SC collector.
- `search` — `main.py`, answers party searches from the "Search Console" sheet.

Both bind-mount this folder, so the SQLite index, resume progress, captcha
stats and proxy list persist on the host across restarts and rebuilds. Run the
one-time `backfill` (see above) the first time if you have existing sheet data.

### Moving to a new machine

The only thing you must install on the new machine is **Docker** (Docker
Desktop on Mac/Windows, or Docker Engine + the Compose plugin on Linux). You do
**not** need Python or any `pip` packages on the host — the image builds
everything from `requirements.txt` inside the container.

1. Copy this `legal_case_scraper/` folder to the new machine. Make sure these
   come with it: `service_account.json`, `Webshare proxies.txt`, the code, and
   `captcha_solver/bundles/` (the captcha models).
2. You can safely delete these before copying to save space — they are
   regenerated/rebuilt automatically: `.dedup_cache/` (no longer used),
   `captcha_img/`, `__pycache__/`, and any `venv/`.
3. To resume collection exactly where the old machine left off, also copy the
   `daily_run/*/*_progress_s*.json` files. (If you don't, it restarts from the
   configured start years but still won't create duplicates.)
4. Start it:

   ```bash
   cd legal_case_scraper
   docker compose up -d --build     # builds the image, then runs 24/7
   docker compose logs -f collector
   ```

The `data/` folder (SQLite index + captcha stats) is created automatically and
persists on the host. The first start auto-imports existing Sheet data into the
index.

## Multiple machines (optional)

To split the collection load across several machines, set a shard id on each:

```bash
export SYSTEM_SHARD_ID=1   # machine 1
export SYSTEM_SHARD_ID=2   # machine 2
```

In the daily-run index sheet `config` tab, set `total_systems` to the number of
machines. Each worker processes its own slice and uses sheet write locks to
avoid write collisions. Note: the local SQLite index lives on each machine, so
run search on the same machine that collected the data (or run the collector on
a single machine if you want one complete index).

Recommended stable defaults for small Linux boxes:

```bash
export MAX_CONCURRENT=20
export DETAIL_SESSION_POOL_SIZE=4
export DC_DETAIL_WORKERS=8
export HC_DETAIL_WORKERS=8
export SC_SEARCH_WORKERS=4
export SHEET_FLUSH_CASES=1000
export WRITE_BATCH_SIZE=1000
export DEFAULT_EXECUTOR_WORKERS=4
```

Increase these only after RAM and Google Sheets quota look stable.

## Useful Environment Variables

Core:

- `SYSTEM_SHARD_ID`: worker shard id, default `1`
- `MAX_CONCURRENT`: HTTP concurrency, default `20`
- `REQUEST_DELAY`: small delay between HTTP requests, default `0.03`

Daily-run ranges:

- `DAILY_RUN_LOOKBACK_YEARS`: default `2`; used when a court-specific start year is not set
- `DC_START_YEAR`, `DC_END_YEAR`
- `HC_START_YEAR`, `HC_END_YEAR`
- `SC_START_YEAR`, `SC_END_YEAR`
- `SC_EMPTY_JUMP_ENABLED`: default `false`; set `true` only for faster sparse scans where skipped case numbers are acceptable

Batching and memory:

- `SHEET_FLUSH_CASES`: rows buffered before writing, default `1000`
- `WRITE_BATCH_SIZE`: Google Sheets append chunk size, default `1000`
- `DETAIL_SESSION_POOL_SIZE`: independent detail sessions, default auto-capped
- `DEFAULT_EXECUTOR_WORKERS`: default executor threads, default `4`

Search Console sheet search:

- `SHEET_SEARCH_DELAY_SECONDS`: delay between searched spreadsheets, default `2.0`
- `SHEET_SEARCH_MAX_QUOTA_RETRIES`: quota retry limit per sheet, default `8`

Captcha/logging:

- `VERBOSE_CAPTCHA_LOGS=true`: log every prediction and whether the site accepted it
- `CAPTCHA_SOLVER_MODE`: `ensemble`, `keras_only`, or `ddddocr_only`
- `CAPTCHA_PREPROCESS=true`: enable image preprocessing
- `CAPTCHA_SAVE_SUCCESS_IMAGES=false`: keep false in production
- `CAPTCHA_MIN_CONFIDENCE`: default `0` (off). Set e.g. `0.6` to skip submitting
  low-confidence guesses and fetch a fresh captcha instead (SC). Per-solver
  accuracy is persisted to `data/captcha_stats.json` and survives restarts.

## Logs

Daily-run logs use readable blocks:

- stage start: selected court/year/status and progress
- search summary: cases found and captcha acceptance rate
- pipeline telemetry: in-flight detail work and write buffer status
- stage summary: search count, detail success/failure, duplicate skips, written rows
- session rotation: session name, failure reason, proxy, and proxy-pool health

Set `VERBOSE_CAPTCHA_LOGS=true` only when debugging captcha quality. It logs each prediction and whether it succeeded.

## Project Layout

```text
main.py                  # Search Console party search loop
run_all.py               # DC + HC + SC 24/7 collector
run_dc.py                # District Court daily-run only
run_hc.py                # High Court daily-run only
run_sc.py                # Supreme Court daily-run only
config.py                # shared configuration
docker-compose.yml       # one-click collector + search
daily_run/               # 24/7 extraction pipelines
daily_run/case_index.py  # local SQLite index (search + dedup) + backfill
scrapers/                # on-demand party-search scrapers (live courts)
sheets/                  # Search Console Google Sheets client
utils/                   # matching, logging, sessions, captcha, normalization
captcha_solver/bundles/  # bundled captcha models
data/                    # SQLite index, captcha stats (auto-created, gitignored)
```

## Troubleshooting

- High RAM: lower `SHEET_FLUSH_CASES`, `WRITE_BATCH_SIZE`, detail workers, and search workers.
- Sheets 429: reduce write frequency or workers; the code retries quota errors.
- Too many captcha rejects: enable `VERBOSE_CAPTCHA_LOGS=true` briefly and check accept rate.
- Proxy failures: check session rotation logs and proxy-pool health.
- Need clean resume: inspect the progress JSON under `daily_run/*/*progress*.json`.
