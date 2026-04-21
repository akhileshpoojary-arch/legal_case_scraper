# Legal Case Scraper

High-performance, async pipeline for scraping Indian court case records from District Courts (DC), High Courts (HC), and the Supreme Court (SC). Uses custom-trained Keras models for CAPTCHA solving instead of Tesseract OCR.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        run_all.py                               │
│     (Single event loop — asyncio.gather for all 3 courts)       │
├───────────────┬───────────────────┬─────────────────────────────┤
│  DC Scraper   │    HC Scraper     │        SC Scraper            │
│  (ecourts DC) │   (ecourts HC)    │       (sci.gov.in)           │
├───────────────┼───────────────────┼─────────────────────────────┤
│ SessionPool   │   SessionPool     │      SessionPool             │
│ (20+ sessions)│  (20+ sessions)   │     (20+ sessions)           │
│ each w/ own   │  each w/ own      │    each w/ own               │
│ cookies       │  cookies          │    cookies                   │
├───────────────┴───────────────────┴─────────────────────────────┤
│                     Captcha Solver                               │
│        Type 1 (CTC Model)  │  Type 2 (Classifier Model)         │
│        DC/HC text captchas  │  SC math captchas (0-20)          │
├─────────────────────────────────────────────────────────────────┤
│                    Google Sheets Writer                           │
│          5000-row batches • dedup cache • overflow mgmt          │
└─────────────────────────────────────────────────────────────────┘
```

## CAPTCHA Solving

| Court    | Model Type         | Input                  | Output               |
|----------|--------------------|-----------------------|----------------------|
| DC / HC  | Type 1 (CTC)      | 200×50 grayscale PNG  | 6-char alphanumeric  |
| SC       | Type 2 (Classifier)| 200×50 grayscale PNG  | Numeric answer (0-20)|

Models live in `captcha_solver/bundles/type1/` and `type2/`. They're loaded once at startup (~50ms inference per image on CPU).

**Retry logic:** If the court website rejects a captcha, the scraper automatically downloads a fresh captcha image and retries (up to 15 attempts for SC, 20 for DC/HC).

## Parallel Processing

Each scraper creates a pool of independent HTTP sessions, each with its own cookies. This means:
- Each session gets its own captcha challenge from the server
- Multiple captchas are solved simultaneously
- No session contention between parallel workers

**Default concurrency (8-core machine):**

| Setting                  | Value | Env Var Override          |
|--------------------------|-------|---------------------------|
| `DETAIL_SESSION_POOL_SIZE` | 40  | `DETAIL_SESSION_POOL_SIZE`|
| `SC_SEARCH_WORKERS`       | 24  | `SC_SEARCH_WORKERS`       |
| `HC_SEARCH_WORKERS`       | 16  | `HC_SEARCH_WORKERS`       |
| `DC_SEARCH_WORKERS`       | 16  | `DC_SEARCH_WORKERS`       |
| `MAX_CONCURRENT`           | 50  | `MAX_CONCURRENT`          |
| `WRITE_BATCH_SIZE`         | 5000| `WRITE_BATCH_SIZE`        |

All values auto-scale to your CPU core count.

---

## Project Structure

```
legal_case_scraper/
├── run_all.py                    # Entrypoint: runs DC + HC + SC concurrently
├── config.py                     # Global config (sheets, proxy, concurrency)
├── Procfile                      # Railway worker declaration
├── Dockerfile                    # Docker build for Railway
├── railway.json                  # Railway deployment config
├── requirements.txt              # Python dependencies
│
├── captcha_solver/
│   └── bundles/
│       ├── type1/                # CTC model (DC/HC)
│       │   ├── model.keras
│       │   └── vocab.json
│       └── type2/                # Classifier model (SC)
│           ├── model.keras
│           └── vocab.json
│
├── daily_run/
│   ├── config.py                 # Worker counts, batch sizes, progress files
│   ├── cluster.py                # Shard management for multi-system scaling
│   ├── sheets_manager.py         # Google Sheets writer (batched, deduped)
│   ├── district_court/
│   │   ├── extractor.py          # DC search + detail fetching
│   │   ├── scraper.py            # DC orchestrator (workers, queue, pipeline)
│   │   └── parser.py             # DC HTML → structured data
│   ├── high_court/
│   │   ├── extractor.py          # HC search + detail fetching
│   │   ├── scraper.py            # HC orchestrator
│   │   └── parser.py             # HC HTML → structured data
│   └── supreme_court/
│       ├── extractor.py          # SC search (model-based captcha, no brute-force)
│       ├── scraper.py            # SC orchestrator
│       └── parser.py             # SC HTML → structured data
│
└── utils/
    ├── captcha_model.py          # Keras model loader (singleton)
    ├── captcha.py                # solve() / download_and_solve() interface
    ├── session_utils.py          # Session rotation on failures
    ├── http_client.py            # aiohttp client abstraction
    ├── proxy.py                  # Webshare proxy rotator
    └── logging_utils.py          # Structured logging setup
```

---

## Local Setup

### 1. Prerequisites

- **Python 3.9+** (3.11 recommended)
- **pip** (or venv)
- **service_account.json** — Google Cloud service account with Sheets API enabled

### 2. Install Dependencies

```bash
cd legal_case_scraper

# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install all dependencies
pip install -r requirements.txt
```

> **Note:** `tensorflow-cpu` is ~500 MB. First install may take a few minutes.

### 3. Verify Installation

```bash
# Quick test: verify models load correctly
python3 -c "
from utils.captcha_model import get_solver
solver = get_solver()
print('✅ Both models loaded successfully!')
"
```

### 4. Run Locally

```bash
# Run all 3 scrapers concurrently
python3 run_all.py

# Or run just one court scraper for testing
python3 -c "
import asyncio
from daily_run.district_court.scraper import DCContinuousScraper
async def test():
    scraper = DCContinuousScraper()
    try:
        await scraper.run()
    finally:
        await scraper.close()
asyncio.run(test())
"
```

### 5. Environment Variables (optional for local)

| Variable                 | Description                                      | Default                |
|--------------------------|--------------------------------------------------|------------------------|
| `SERVICE_ACCOUNT_JSON`   | Full JSON content of service account credentials | Falls back to local file |
| `PROXY_LIST`             | Newline-separated proxy list (ip:port:user:pass) | Falls back to local file |
| `MAX_CONCURRENT`         | Global HTTP semaphore limit                      | `50`                   |
| `DETAIL_SESSION_POOL_SIZE`| Number of parallel detail-fetching sessions     | Auto (CPU × 5)         |
| `WRITE_BATCH_SIZE`       | Rows per Google Sheets API write                 | `5000`                 |
| `SYSTEM_SHARD_ID`        | Worker ID for multi-system deployment            | `1`                    |

---

## Deploy to Railway

### Step 1: Set Up Git & Push

```bash
# If git not linked yet, configure it:
git config --global user.name "Your Name"
git config --global user.email "your-email@example.com"

# Initialize repo (skip if already a git repo)
cd legal_case_scraper
git init

# Add all files
git add .
git commit -m "Railway deployment with Keras captcha models"

# Create repo on GitHub (via browser: github.com/new)
# Then link and push:
git remote add origin https://github.com/YOUR_USERNAME/legal_case_scraper.git
git branch -M main
git push -u origin main
```

> **First time on this device?** GitHub may ask you to authenticate.
> Use a **Personal Access Token** (Settings → Developer Settings → Tokens → Generate).
> When git asks for password, paste the token instead.

### Step 2: Connect to Railway

1. Go to [railway.app](https://railway.app) and sign in
2. Click **"New Project"** → **"Deploy from GitHub Repo"**
3. Select your `legal_case_scraper` repository
4. Railway will auto-detect the `Dockerfile` and start building

### Step 3: Add Environment Variables

In Railway dashboard → your service → **Variables** tab:

| Variable               | Value                                         |
|------------------------|-----------------------------------------------|
| `SERVICE_ACCOUNT_JSON` | *(paste entire content of service_account.json)* |
| `PROXY_LIST`           | *(paste proxy lines, one per line)*            |

### Step 4: Deploy & Monitor

- Railway auto-deploys on every `git push`
- View logs in Railway dashboard → **Deployments** → **View Logs**
- Look for:
  - `✓ Type 1 loaded` — CTC model ready
  - `✓ Type 2 loaded` — Classifier model ready
  - `Starting DC/HC/SC Continuous 24/7 Scraper` — all three running
  - `Wrote N new cases` — data flowing to Google Sheets

### Step 5: Restart Policy

The `railway.json` has `restartPolicyType: ON_FAILURE` with 10 max retries. If the process crashes, Railway will automatically restart it.

---

## Troubleshooting

| Issue | Solution |
|-------|---------|
| `ModuleNotFoundError: tensorflow` | Run `pip install tensorflow-cpu>=2.16.0` |
| `No module named 'bs4'` | Run `pip install beautifulsoup4` |
| CAPTCHA success rate low | Check `captcha_img/` folder for saved images; retrain model if needed |
| Google Sheets 429 errors | Reduce `WRITE_BATCH_SIZE` to 3000 or increase delays |
| Google Sheets 500 errors | Reduce batch size; these are transient, retries should handle them |
| Railway build fails | Check that `requirements.txt` has all deps; Docker logs show the error |
| `service_account.json` not found | Set `SERVICE_ACCOUNT_JSON` env var or place file in project root |

---

## License

Private / Internal Use Only.
