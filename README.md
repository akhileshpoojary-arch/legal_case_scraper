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

Sheet search no longer loads the full spreadsheet grid. It first reads only party/search columns, finds matching row numbers, then fetches only matched rows and continuation rows. This reduces RAM usage for large daily-run sheets.

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

## Daily-Run Linux Setup

For multiple Linux systems, set the shard id on each machine:

```bash
export SYSTEM_SHARD_ID=1   # machine 1
export SYSTEM_SHARD_ID=2   # machine 2
export SYSTEM_SHARD_ID=3   # machine 3
```

In the daily-run index sheet `config` tab, set `total_systems` to the number of machines. Each worker processes its own slice and uses sheet write locks to avoid write collisions.

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

- `DC_START_YEAR`, `DC_END_YEAR`
- `HC_START_YEAR`, `HC_END_YEAR`
- `SC_START_YEAR`, `SC_END_YEAR`

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
run_all.py               # DC + HC + SC daily-run service
run_dc.py                # District Court daily-run only
run_hc.py                # High Court daily-run only
run_sc.py                # Supreme Court daily-run only
config.py                # shared configuration
daily_run/               # 24/7 extraction pipelines
scrapers/                # on-demand party-search scrapers
sheets/                  # Search Console Google Sheets client
utils/                   # matching, logging, sessions, captcha, normalization
captcha_solver/bundles/  # bundled captcha models
```

## Troubleshooting

- High RAM: lower `SHEET_FLUSH_CASES`, `WRITE_BATCH_SIZE`, detail workers, and search workers.
- Sheets 429: reduce write frequency or workers; the code retries quota errors.
- Too many captcha rejects: enable `VERBOSE_CAPTCHA_LOGS=true` briefly and check accept rate.
- Proxy failures: check session rotation logs and proxy-pool health.
- Need clean resume: inspect the progress JSON under `daily_run/*/*progress*.json`.
