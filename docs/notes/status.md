# Project Status

*Last updated: 2026-05-11*

## Pipelines

| Pipeline | File | Schedule | Status |
|----------|------|----------|--------|
| EOD Daily | `tiingo_to_s3_flow.py` | 6 PM weekdays (Pacific) | Active, scheduled |
| EOD Backfill | `tiingo_backfill_flow.py` | On-demand | Completed for 2020-2025 |
| Fundamentals Daily | `tiingo_fundamentals_flow.py` | 6 PM weekdays (Pacific) | Active, scheduled |
| Fundamentals Backfill | `tiingo_fundamentals_backfill_flow.py` | On-demand | Verify `belligerent-taipan` completed (see below) |
| APEX Vol Table | `vol_table_flow.py` | 6 PM weekdays (Eastern) | Active, scheduled |

## Action Items for Next Person

### 1. Verify the fundamentals backfill completed

Check the `belligerent-taipan` flow run in Prefect Cloud. It was processing 104 tickers x 6 years x 3 endpoints = 1,872 API calls at 3s each (~90 min total). Started ~15:52 UTC on May 11. As of 17:17 UTC it was ~90% done with 0 failures.

## Known Issue: Prefect Version Pin

**`requirements.txt` pins `prefect[aws]==3.6.29`.** This is a workaround for a broken prefect 3.7.0 in the managed work pool base image (see `docs/knowledge/decisions.md` for full context). When Prefect releases a fix, test with `test_flow.py` before bumping. Note: the test deployment was deleted to free up a slot -- recreate it first:

```bash
uvx prefect-cloud deploy test_flow.py:test_flow --from mh-guess/data-flow --with-requirements requirements.txt
uvx prefect-cloud run test_flow/test_flow
# Check flow run status in Prefect Cloud. If it passes, redeploy all production pipelines.
# Then delete the test deployment again to stay under the limit.
```

## Infrastructure

- **Orchestration**: Prefect Cloud (managed work pool: `default-work-pool`)
- **Credentials**: Prefect Cloud blocks (`tiingo-api-token`, `aws-credentials-tim`, `github-pat-apex`)
- **Storage**: AWS S3 (`mh-guess-data` for raw data, `apex-market-data-raw-220464759930` for derived)
- **Ticker config**: `s3://mh-guess-data/adhoc/tickers.txt` (104 tickers for Tiingo pipelines); `mh-guess/apex:symbols.yaml` (100 tickers for vol table)
- **Source**: GitHub (`mh-guess/data-flow`, deploys from `main` branch)
- **Deployment slots**: 5 of 5 used

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
