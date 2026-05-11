# Architecture

## System Overview

```
Tiingo REST API       GitHub (mh-guess/apex)
      │                      │
      ▼                      ▼ symbols.yaml
┌─────────────┐       ┌──────────────────────────────────┐
│ Prefect Cloud│──────▶│ AWS S3                           │
│ (orchestrate)│       │ mh-guess-data (raw JSON)         │
└─────────────┘       │ apex-market-data-raw (derived)    │
      │               └──────────────────────────────────┘
      ├── tiingo_to_s3_flow         (daily, 6 PM PT)
      ├── tiingo_backfill_flow      (retro, on-demand)
      ├── tiingo_fundamentals_flow  (daily, 6 PM PT)
      ├── tiingo_fundamentals_backfill_flow (on-demand)
      └── vol_table_flow            (daily, 6 PM ET)
```

## Shared Module (`shared.py`)

Common utilities imported by all pipelines:
- `S3_BUCKET_NAME`, `TICKERS_S3_KEY` -- constants
- `load_credentials()` -- loads Tiingo API token and AWS credentials from Prefect Cloud blocks
- `fetch_tickers_from_s3()` -- reads ticker list from S3
- `upload_json_to_s3()` -- writes raw JSON to S3

## Pipelines

### Daily Incremental (`tiingo_to_s3_flow.py`)

1. Load credentials from Prefect Cloud blocks
2. Fetch ticker list from `s3://mh-guess-data/adhoc/tickers.txt`
3. For each ticker: call Tiingo API for last 30 days of end-of-day prices
4. Write raw JSON to `price_eod/load_type=daily/date={YYYY-MM-DD}/{ticker}.json`

One API call per ticker per run. Runs at 6 PM Pacific on weekdays.

### Historical Backfill (`tiingo_backfill_flow.py`)

1. Load credentials from Prefect Cloud blocks
2. Fetch ticker list from S3 (or accept explicit list as parameter)
3. For each ticker x year: call Tiingo API for full year
4. Write raw JSON to `price_eod/load_type=retro/year={YYYY}/{ticker}.json`

One API call per ticker per year. Run on-demand.

### Fundamentals Daily (`tiingo_fundamentals_flow.py`)

1. Load credentials and tickers
2. Fetch metric definitions (1 bulk call)
3. Fetch company metadata (1 bulk call)
4. For each ticker: fetch daily metrics + statements (asReported=true and false)
5. Write raw JSON under `fundamentals/{endpoint}/load_type=daily/date={date}/`

Scheduled at 6 PM Pacific on weekdays. Per-run: 2 + (3 x num_tickers) API calls.

### Fundamentals Backfill (`tiingo_fundamentals_backfill_flow.py`)

1. Load credentials and tickers
2. For each ticker x year: fetch daily metrics + statements (both variants)
3. Write raw JSON under `fundamentals/{endpoint}/load_type=retro/year={year}/`

Rate limited at 3s between calls (~20/min). Run on-demand.

### APEX Volatility Table (`vol_table_flow.py`)

1. Load credentials (Tiingo token, AWS creds, GitHub PAT)
2. Fetch symbol list from `mh-guess/apex` repo on GitHub (via PAT auth)
3. For each symbol: call Tiingo API for 180 calendar days of EOD prices
4. Compute log-return volatility metrics (daily, annualized, per-minute)
5. Write parquet to `s3://apex-market-data-raw-220464759930/derived/volatility/`

Rate limited at 2s between calls. Scheduled at 6 PM Eastern on weekdays. Writes both a date-partitioned file and a `vol_table_latest.parquet` overwritten each run.

## S3 Data Layout

```
s3://mh-guess-data/tiingo/json/
├── price_eod/                          # End-of-day price data
│   ├── load_type=daily/
│   │   └── date=2025-01-15/
│   │       ├── AAPL.json
│   │       └── TSLA.json
│   └── load_type=retro/
│       ├── year=2020/
│       │   ├── AAPL.json
│       │   └── TSLA.json
│       └── year=2025/
│           └── ...
└── fundamentals/
    ├── daily/
    │   ├── load_type=daily/date={date}/{ticker}.json
    │   └── load_type=retro/year={year}/{ticker}.json
    ├── statements/
    │   ├── as_reported=true/
    │   │   ├── load_type=daily/date={date}/{ticker}.json
    │   │   └── load_type=retro/year={year}/{ticker}.json
    │   └── as_reported=false/
    │       ├── load_type=daily/date={date}/{ticker}.json
    │       └── load_type=retro/year={year}/{ticker}.json
    ├── definitions/
    │   └── date={date}/definitions.json
    └── meta/
        └── date={date}/meta.json
```

## Credential Management

All secrets stored in Prefect Cloud blocks (never in code or env vars):
- `tiingo-api-token` (Secret block) -- Tiingo API key
- `aws-credentials-tim` (AwsCredentials block) -- AWS access for S3 (both buckets)
- `github-pat-apex` (Secret block) -- GitHub classic PAT with `repo` scope for reading `symbols.yaml` from `mh-guess/apex`

## Ticker Management

**Tiingo pipelines:** Tickers are read dynamically from `s3://mh-guess-data/adhoc/tickers.txt` at runtime. To add or remove tickers, edit that file directly -- no code changes or redeployment needed.

**Vol table pipeline:** Tickers are read from `symbols.yaml` in the `mh-guess/apex` GitHub repo (`main` branch) at runtime. To update, commit changes to `symbols.yaml` in the APEX repo -- no changes needed in data-flow.
