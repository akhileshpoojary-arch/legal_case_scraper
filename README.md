# Legal Case Scraper

Async scraper pipeline for Indian courts:
- District Court (DC)
- High Court (HC)
- Supreme Court (SC)

It runs continuously, solves captchas with Keras models, fetches case details in parallel, and writes deduplicated output to Google Sheets.

## What This Project Does

- Runs DC + HC + SC workers together
- Uses session pools and proxy rotation for stable scraping
- Uses trained captcha models (no Tesseract)
- Retries on transient failures (network/captcha/session)
- Writes to Google Sheets in batches
- Keeps progress files so runs can resume

## Simple Flow

1. Start all three court scrapers in one asyncio loop.
2. For each court, collect case list from search pages.
3. Fetch detail pages in parallel.
4. Build normalized rows.
5. Write rows to Google Sheets.
6. Repeat continuously.

## Parallel Processing (Simple Explanation)

Parallelism happens at three levels:

1. Court-level parallelism
- `run_all.py` runs DC, HC, and SC together with `asyncio.gather`.

2. Search/detail worker parallelism inside each court
- Multiple search workers collect case candidates.
- Multiple detail workers fetch full case history pages at the same time.

3. Captcha inference parallelism (bounded)
- Captcha solving is CPU-heavy.
- It runs in a dedicated small thread pool, with explicit limits to prevent thread explosion.

This gives speed while keeping memory/thread usage stable in Railway.

## 5000-Case Write Policy

The project now uses a single flush policy:

- After every **5000 collected cases**, write to Google Sheets.
- Remaining rows (less than 5000) are written at stage end.

Config key:
- `SHEET_FLUSH_CASES` (default `5000`)

Used by:
- DC write buffer flush threshold
- HC write buffer flush threshold
- SC write batch threshold

## Captcha System

Models are in:
- `captcha_solver/bundles/type1` (DC/HC text captcha)
- `captcha_solver/bundles/type2` (SC math captcha)

Runtime behavior:
- Models loaded once (singleton)
- TensorFlow threads constrained for container safety
- Inference concurrency bounded with semaphore

## Retry Behavior

- Captcha rejected: refresh captcha and retry
- Session expired: refresh session and retry
- Transport failure: classify as retryable and retry same work block
- Rate-limit / transient Sheets errors: exponential backoff + jitter

Important:
- Retryable search failures do **not** silently advance progress.
- This avoids skipping cases when the site is unstable.

## Logging

Logs are concise and useful:

- Captcha summary:
  - attempts, solved, rejected, empty, no-image, exhausted
- Stage summary:
  - duration, search hits/total, detail success/fail, written rows
- Write summary:
  - duplicates skipped, written rows, overflow handling

## Project Structure

```text
legal_case_scraper/
├── run_all.py
├── run_dc.py
├── run_hc.py
├── run_sc.py
├── config.py
├── daily_run/
│   ├── config.py
│   ├── sheets_manager.py
│   ├── cluster.py
│   ├── district_court/
│   ├── high_court/
│   └── supreme_court/
├── utils/
│   ├── captcha.py
│   ├── captcha_model.py
│   ├── session_utils.py
│   ├── http_client.py
│   └── logging_utils.py
├── captcha_solver/
│   └── bundles/
└── requirements.txt
```

## Local Run

### 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run all courts

```bash
python3 run_all.py
```

### 3. Run single court (optional)

```bash
python3 run_dc.py
python3 run_hc.py
python3 run_sc.py
```

## Environment Variables

### Core

- `SERVICE_ACCOUNT_JSON` : Google service account JSON content
- `PROXY_LIST` : proxy list content
- `SYSTEM_SHARD_ID` : shard ID (default `1`)

### Concurrency / batching

- `MAX_CONCURRENT` (default `30`)
- `DETAIL_SESSION_POOL_SIZE`
- `SC_SEARCH_WORKERS`
- `DC_DETAIL_WORKERS`
- `HC_DETAIL_WORKERS`
- `SHEET_FLUSH_CASES` (default `5000`)
- `WRITE_BATCH_SIZE` (Sheets append chunking)
- `DC_START_YEAR` / `DC_END_YEAR`
- `HC_START_YEAR` / `HC_END_YEAR`
- `SC_START_YEAR` / `SC_END_YEAR`
- `SC_EMPTY_STREAK_STOP` (default `750`)
- `SC_DETAIL_TABS` (`required` by default, use `all` for every SCI detail tab)

### Logging / mode

- `TESTING` (`true`/`false`, default `false`)
- `VERBOSE_CAPTCHA_LOGS` (`false` by default; keep false in production to log summaries instead of every CAPTCHA attempt)

### TensorFlow safety (optional override)

- `TF_NUM_INTRAOP_THREADS` (default `1`)
- `TF_NUM_INTEROP_THREADS` (default `1`)
- `CAPTCHA_MODEL_MAX_CONCURRENCY` (default `2`)
- `CAPTCHA_EXECUTOR_WORKERS` (default `4`)
- `CAPTCHA_SOLVER_MODE` (`ensemble`, `keras_only`, or `ddddocr_only`; default `ensemble`)
- `CAPTCHA_PREPROCESS` (`true` by default)
- `CAPTCHA_TYPE1_MODEL_FILE` (default `model.keras`)
- `CAPTCHA_TYPE2_MODEL_FILE` (default `model_best.keras` when present)
- `CAPTCHA_SAVE_SUCCESS_IMAGES` (default `false`)
- `DEFAULT_EXECUTOR_WORKERS` (default `8`)

## Railway Deployment

1. Push code to GitHub.
2. Create Railway project from that repo.
3. Add required env vars (`SERVICE_ACCOUNT_JSON`, `PROXY_LIST`, etc.).
4. Deploy worker service.

`Procfile` uses:

```text
worker: python run_all.py
```

## GitHub Push

```bash
git add .
git commit -m "Stabilize parallel scraping, retries, and docs"
git push origin main
```

## Quick Troubleshooting

- Crash with `pthread_create failed`:
  - Reduce worker env values, keep TF thread limits at 1.
- Google Sheets 429:
  - Keep flush at 5000, avoid very high write frequency, allow retries.
- Too many captcha failures:
  - Check summary `accept_rate`, lower search worker counts, and check proxy quality.
- Need CAPTCHA training images:
  - Set `CAPTCHA_SAVE_SUCCESS_IMAGES=true`; keep it off for normal production runs.
- Very verbose logs:
  - Keep `TESTING=false` and `VERBOSE_CAPTCHA_LOGS=false` in production.

## Notes

- This is a long-running scraper service.
- Progress files under `daily_run/*_progress_*.json` are used for resume.
- Sheet writing includes dedup logic to avoid duplicates across runs.
