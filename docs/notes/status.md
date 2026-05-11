# Project Status

*Last updated: 2026-05-08*

## Deployed & Running

| Pipeline | Schedule | Status |
|----------|----------|--------|
| EOD Daily (`tiingo_to_s3_flow`) | 6 PM weekdays (Pacific) | Active |
| EOD Backfill (`tiingo_backfill_flow`) | On-demand | Completed for 2020-2025 |
| Fundamentals Daily (`tiingo_fundamentals_flow`) | 6 PM weekdays (Pacific) | New - needs deploy |
| Fundamentals Backfill (`tiingo_fundamentals_backfill_flow`) | On-demand | New - needs initial backfill |

## Infrastructure

- **Orchestration**: Prefect Cloud (deployments managed via `prefect.yaml`)
- **Credentials**: Prefect Cloud blocks (`tiingo-api-token`, `aws-credentials-tim`)
- **Storage**: AWS S3 (`mh-guess-data` bucket)
- **Ticker config**: `s3://mh-guess-data/adhoc/tickers.txt` (dynamic, no code changes needed)
- **Source**: GitHub (`mh-guess/data-flow`)

## Data in S3

All raw Tiingo API responses stored with no transformation:
- `price_eod/load_type=daily/date={YYYY-MM-DD}/{ticker}.json` -- rolling 30-day window per run
- `price_eod/load_type=retro/year={YYYY}/{ticker}.json` -- full year per file, 2020-2025 complete
