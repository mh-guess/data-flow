# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Production ETL system for loading financial data from Tiingo API to AWS S3 using Prefect Cloud. The system consists of two pipelines:
1. **Daily incremental pipeline** - Scheduled to run 6 PM weekdays (Pacific Time)
2. **Historical backfill pipeline** - On-demand for loading historical data

## Current Status (as of session end)

### ✅ Deployed & Running
- **Daily Pipeline**: Scheduled 6 PM weekdays, fetches last 30 days of data
- **Backfill Pipeline**: On-demand, completed backfills for 2020-2025
- **Data in S3**: Type-partitioned structure (daily vs retro)
- **Logging**: Integrated with Prefect Cloud loggers (all logs visible in UI)

### 📊 Data Architecture

**Type-Partitioned Structure** (hybrid granularity):
```
s3://mh-guess-data/tiingo/json/
├── load_type=daily/
│   └── date=2025-01-15/
│       ├── AAPL.json      # Single day of data
│       ├── TSLA.json
│       └── ...
└── load_type=retro/
    ├── year=2020/
    │   ├── AAPL.json      # ~252 days of data
    │   ├── TSLA.json
    │   └── ...
    ├── year=2021/
    └── year=2025/
```

**Key Design Decisions**:
- **Daily**: Date-level granularity (1 file per ticker per day)
- **Retro**: Year-level granularity (1 file per ticker per year)
- **Why?**: Retro uses year-level to minimize API calls (25 calls for 5 years vs 6,300)
- **Raw data**: No transformation - saves Tiingo API response as-is
- **Compact JSON**: No pretty formatting to save storage

## Development Setup

```bash
# Navigate to data-flow directory
cd data-flow

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Login to Prefect Cloud (required)
prefect cloud login
```

## Key Files

### Pipeline Files
- **tiingo_to_s3_flow.py**: Daily incremental ETL pipeline
- **tiingo_backfill_flow.py**: Historical backfill pipeline (year-partitioned)
- **prefect.yaml**: Infrastructure as Code for deployments
- **requirements.txt**: Python dependencies

### Documentation
- **README.md**: Project overview and getting started
- **SETUP.md**: Prefect Cloud credential configuration
- **DEPLOYMENT.md**: Deployment management guide

## Common Operations

### Run Flows Manually

```bash
# Daily flow (fetches last 30 days)
uvx prefect-cloud run tiingo_to_s3_flow/tiingo_to_s3_flow

# Backfill 2020-2024 for all tickers
uvx prefect-cloud run tiingo_backfill_flow/tiingo_backfill_flow \
  --parameter start_year=2020 \
  --parameter end_year=2024

# Backfill specific tickers
uvx prefect-cloud run tiingo_backfill_flow/tiingo_backfill_flow \
  --parameter start_year=2023 \
  --parameter end_year=2024 \
  --parameter tickers='["AAPL","TSLA"]'
```

### Deploy Updates

```bash
# After making code changes:
git add .
git commit -m "Your message"
git push

# Deploy daily pipeline
uvx prefect-cloud deploy tiingo_to_s3_flow.py:tiingo_to_s3_flow \
  --from mh-guess/data-flow \
  --with-requirements requirements.txt

# Deploy backfill pipeline
uvx prefect-cloud deploy tiingo_backfill_flow.py:tiingo_backfill_flow \
  --from mh-guess/data-flow \
  --with-requirements requirements.txt
```

### Manage Schedule

```bash
# View current schedule in Prefect Cloud UI or:
# Change schedule (cron format: minute hour day month day-of-week)
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow "0 18 * * 1-5"

# Remove schedule
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow none
```

## Prefect Cloud Configuration

### Required Blocks (in Prefect Cloud UI)
1. **tiingo-api-token** (Secret): Tiingo API token
2. **aws-credentials-tim** (AwsCredentials): AWS access key, secret key, region

See `SETUP.md` for detailed setup instructions.

## Data Flow Architecture

### Daily Pipeline Flow
1. Load credentials from Prefect Cloud blocks
2. Fetch tickers from `s3://mh-guess-data/adhoc/tickers.txt`
3. For each ticker: fetch last 30 days from Tiingo API
4. Save raw data to `s3://mh-guess-data/tiingo/json/load_type=daily/date={YYYY-MM-DD}/{ticker}.json`

### Backfill Pipeline Flow
1. Load credentials from Prefect Cloud blocks
2. Fetch tickers from S3 (or use provided list)
3. For each ticker × year: fetch entire year in 1 API call
4. Save raw data to `s3://mh-guess-data/tiingo/json/load_type=retro/year={YYYY}/{ticker}.json`

### API Efficiency
- **Backfill**: 1 call per ticker per year (e.g., 5 tickers × 5 years = 25 calls)
- **Daily**: 1 call per ticker (fetches 30 days in single call)

## Important Implementation Details

### Logging
- **Uses Prefect loggers**: `get_run_logger()` instead of `print()`
- **Benefits**: All logs visible in Prefect Cloud UI, structured logging, searchable
- **Pattern**: `logger = get_run_logger()` then `logger.info("message")`

### Error Handling
- **Retries**: Tasks have automatic retry logic (2-3 retries with delays)
- **Credentials**: Loaded from Prefect Cloud blocks (never hardcoded)
- **API errors**: Handled by requests library with `response.raise_for_status()`

### Ticker Management
- **Source**: `s3://mh-guess-data/adhoc/tickers.txt` (one ticker per line)
- **Update tickers**: Just edit the S3 file, no code changes needed
- **Current tickers**: Check S3 file for latest list

## Next Steps / Future Work

### Immediate Next Steps
1. **Build processing layer**: Combine retro + daily data into queryable datasets
2. **Deduplication logic**: Handle overlaps between retro and daily data
3. **Data quality checks**: Validate completeness and accuracy
4. **Monitoring**: Set up alerts for pipeline failures

### Potential Enhancements
- Add data validation tasks
- Implement incremental updates for retro data
- Add support for other Tiingo endpoints (fundamentals, news, etc.)
- Create data catalog/metadata layer
- Build downstream analytics/visualization layer

## Troubleshooting

### Pipeline not running on schedule
- Check Prefect Cloud UI for deployment status
- Verify schedule is active: `uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow "0 18 * * 1-5"`
- Check timezone setting in Prefect Cloud UI

### Missing logs in Prefect Cloud
- Ensure using `get_run_logger()` not `print()`
- Check flow is running in Prefect Cloud (not locally)

### AWS/Tiingo credential errors
- Verify blocks exist in Prefect Cloud UI
- Check block names match exactly: `aws-credentials-tim`, `tiingo-api-token`
- Test credentials manually if needed

### Empty ticker list
- Verify `s3://mh-guess-data/adhoc/tickers.txt` exists
- Check file has one ticker per line (no empty lines)

## Links

- **Prefect Cloud Workspace**: https://app.prefect.cloud/account/dd30d6c9-aba2-4983-bdb6-aa698e89e845/workspace/426a0775-66ec-4813-a87c-c00c1bd2550b
- **GitHub Repo**: https://github.com/mh-guess/data-flow
- **Tiingo API Docs**: https://www.tiingo.com/documentation/end-of-day
