# Deployment Guide

This guide explains how to manage Prefect deployments as Infrastructure as Code using `prefect.yaml`.

## Overview

The `prefect.yaml` file defines our deployment configuration including:
- Flow entrypoint and metadata
- Schedule (6 PM weekdays, Pacific Time)
- GitHub repository configuration
- Python dependencies
- Tags and versioning

## Deploying with prefect.yaml

### Method 1: Using Prefect CLI (Recommended for IaC)

```bash
# Login to Prefect Cloud
prefect cloud login

# Deploy using prefect.yaml
prefect deploy --all

# Or deploy a specific deployment
prefect deploy -n tiingo-to-s3-production
```

This method:
- ✅ Uses version-controlled configuration
- ✅ Tracks all changes in Git
- ✅ Ensures consistency across environments
- ✅ Supports multiple deployments in one file

### Method 2: Using prefect-cloud CLI (Quick deployments)

```bash
# Quick deployment (what we used initially)
uvx prefect-cloud deploy tiingo_to_s3_flow.py:tiingo_to_s3_flow \
  --from mh-guess/data-flow \
  --with-requirements requirements.txt
```

This method:
- ⚠️ Creates deployments imperatively
- ⚠️ Configuration not tracked in Git
- ✅ Faster for one-off changes

## Updating Deployments

When you need to change the deployment (schedule, dependencies, etc.):

1. **Update `prefect.yaml`** with your changes
2. **Commit to Git**: `git add prefect.yaml && git commit -m "Update deployment config"`
3. **Push**: `git push`
4. **Redeploy**: `prefect deploy --all`

## Schedule Configuration

Current schedule in `prefect.yaml`:
```yaml
schedules:
  - cron: "0 18 * * 1-5"
    timezone: "America/Los_Angeles"
    active: true
```

- **Time**: 6:00 PM Pacific Time
- **Days**: Monday through Friday
- **Timezone**: America/Los_Angeles (handles PST/PDT automatically)

## Version Control Benefits

By using `prefect.yaml`, you get:
- 📝 Full deployment history in Git
- 🔄 Easy rollbacks to previous configurations
- 👥 Team collaboration on deployment changes
- 🔍 Clear audit trail of what changed and when
- 🚀 Reproducible deployments across environments

## Manual Flow Runs

### Daily Flow (Scheduled)

To manually trigger the daily flow:

```bash
# Using prefect-cloud CLI
uvx prefect-cloud run tiingo_to_s3_flow/tiingo_to_s3_flow

# Or using Prefect CLI
prefect deployment run tiingo-to-s3-production
```

### Backfill Flow (On-Demand)

The backfill flow fetches historical data with year-level partitioning for efficient API usage.

**Run with default parameters (2020-2024)**:
```bash
# Using Prefect CLI
prefect deployment run tiingo-backfill
```

**Run with custom year range**:
```bash
# Backfill 2015-2019
prefect deployment run tiingo-backfill \
  --param start_year=2015 \
  --param end_year=2019

# Backfill specific tickers for 2022-2024
prefect deployment run tiingo-backfill \
  --param start_year=2022 \
  --param end_year=2024 \
  --param tickers='["AAPL","TSLA","NVDA"]'
```

**Data Structure**:
- Daily flow: `s3://mh-guess-data/tiingo/json/load_type=daily/date={YYYY-MM-DD}/{ticker}.json`
- Backfill flow: `s3://mh-guess-data/tiingo/json/load_type=retro/year={YYYY}/{ticker}.json`

**API Efficiency**:
- 5-year backfill for 5 tickers = 25 API calls (1 per ticker per year)
- vs 6,300 calls if fetching day-by-day!

## Removing Schedule

To unschedule but keep the deployment:

```bash
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow none
```

Or update `prefect.yaml` to set `active: false` and redeploy.
