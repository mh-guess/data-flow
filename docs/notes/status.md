# Project Status

*Last updated: 2026-05-11*

## Pipelines

| Pipeline | File | Schedule | Status |
|----------|------|----------|--------|
| EOD Daily | `tiingo_to_s3_flow.py` | 6 PM weekdays (Pacific) | Needs redeploy (see below) |
| EOD Backfill | `tiingo_backfill_flow.py` | On-demand | Completed for 2020-2025 |
| Fundamentals Daily | `tiingo_fundamentals_flow.py` | 6 PM weekdays (Pacific) | Needs redeploy (see below) |
| Fundamentals Backfill | `tiingo_fundamentals_backfill_flow.py` | On-demand | Running (2020-2025, 1,872 API calls) |
| Test Flow | `test_flow.py` | None | Passed -- delete deployment when done |

## Action Items for Next Person

### 1. Redeploy all 4 production pipelines with final requirements

The backfill that's currently running was deployed with `prefect==3.6.29` (old standalone `prefect-aws`). The final requirements use `prefect[aws]==3.6.29` (built-in extra). All 4 production deployments need to be redeployed:

```bash
uvx prefect-cloud deploy tiingo_to_s3_flow.py:tiingo_to_s3_flow --from mh-guess/data-flow --with-requirements requirements.txt
uvx prefect-cloud deploy tiingo_backfill_flow.py:tiingo_backfill_flow --from mh-guess/data-flow --with-requirements requirements.txt
uvx prefect-cloud deploy tiingo_fundamentals_flow.py:tiingo_fundamentals_flow --from mh-guess/data-flow --with-requirements requirements.txt
uvx prefect-cloud deploy tiingo_fundamentals_backfill_flow.py:tiingo_fundamentals_backfill_flow --from mh-guess/data-flow --with-requirements requirements.txt
```

### 2. Delete test deployment to free up a slot

We're at the 5-deployment limit. After redeployment, delete the test flow:

```bash
uvx prefect-cloud delete test_flow/test_flow
```

### 3. Set schedules for the daily pipelines

The deployments were created via `prefect-cloud deploy` which doesn't carry over the schedule from `prefect.yaml`. Set schedules manually:

```bash
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow "0 18 * * 1-5"
uvx prefect-cloud schedule tiingo_fundamentals_flow/tiingo_fundamentals_flow "0 18 * * 1-5"
```

### 4. Verify the fundamentals backfill completed

Check the `belligerent-taipan` flow run in Prefect Cloud. It's processing 104 tickers x 6 years x 3 endpoints = 1,872 API calls at 3s each (~90 min total). Started ~15:52 UTC on May 11.

### 5. Clean up old S3 paths

After confirming the EOD daily pipeline writes to the new `price_eod/` path on its next successful scheduled run, delete the old objects:

```bash
aws s3 rm s3://mh-guess-data/tiingo/json/load_type=daily/ --recursive
aws s3 rm s3://mh-guess-data/tiingo/json/load_type=retro/ --recursive
```

## Known Issue: Prefect Version Pin

**`requirements.txt` pins `prefect[aws]==3.6.29`.** This is a workaround for a broken prefect 3.7.0 in the managed work pool base image (see `docs/knowledge/decisions.md` for full context). When Prefect releases a fix, test with `test_flow.py` before bumping:

```bash
# Update requirements.txt to new version, then:
uvx prefect-cloud deploy test_flow.py:test_flow --from mh-guess/data-flow --with-requirements requirements.txt
uvx prefect-cloud run test_flow/test_flow
# Check flow run status in Prefect Cloud. If it passes, redeploy all production pipelines.
```

## Infrastructure

- **Orchestration**: Prefect Cloud (managed work pool: `default-work-pool`)
- **Credentials**: Prefect Cloud blocks (`tiingo-api-token`, `aws-credentials-tim`)
- **Storage**: AWS S3 (`mh-guess-data` bucket)
- **Ticker config**: `s3://mh-guess-data/adhoc/tickers.txt` (104 tickers)
- **Source**: GitHub (`mh-guess/data-flow`, deploys from `main` branch)
- **Deployment limit**: 5 (current tier)

## Data in S3

All raw Tiingo API responses stored as compact JSON, no transformation:

| Dataset | S3 Prefix | Daily | Retro |
|---------|-----------|-------|-------|
| EOD Prices | `price_eod/` | `load_type=daily/date={date}/{TICKER}.json` | `load_type=retro/year={year}/{TICKER}.json` |
| Fund. Daily | `fundamentals/daily/` | same pattern | same pattern |
| Fund. Statements | `fundamentals/statements/as_reported={true\|false}/` | same pattern | same pattern |
| Fund. Definitions | `fundamentals/definitions/` | `date={date}/definitions.json` | N/A |
| Fund. Meta | `fundamentals/meta/` | `date={date}/meta.json` | N/A |

See `docs/knowledge/datasets.md` for full field-level schema documentation.
