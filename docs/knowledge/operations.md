# Operations Runbook

## Deploying Changes

```bash
# After code changes are committed and pushed:
uvx prefect-cloud deploy tiingo_to_s3_flow.py:tiingo_to_s3_flow \
  --from mh-guess/data-flow --with-requirements requirements.txt

uvx prefect-cloud deploy tiingo_backfill_flow.py:tiingo_backfill_flow \
  --from mh-guess/data-flow --with-requirements requirements.txt
```

## Running Pipelines Manually

```bash
# Daily flow
uvx prefect-cloud run tiingo_to_s3_flow/tiingo_to_s3_flow

# Backfill a year range
uvx prefect-cloud run tiingo_backfill_flow/tiingo_backfill_flow \
  --parameter start_year=2020 --parameter end_year=2024

# Backfill specific tickers
uvx prefect-cloud run tiingo_backfill_flow/tiingo_backfill_flow \
  --parameter start_year=2023 --parameter end_year=2024 \
  --parameter tickers='["AAPL","TSLA"]'
```

## Managing the Schedule

```bash
# View or change the daily schedule
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow "0 18 * * 1-5"

# Disable schedule
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow none
```

## Adding / Removing Tickers

Edit `s3://mh-guess-data/adhoc/tickers.txt` directly (one ticker per line). No code change or redeployment needed. Takes effect on the next pipeline run.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Pipeline not running on schedule | Prefect Cloud UI deployment status; verify schedule is active |
| Missing logs in Prefect Cloud | Ensure code uses `get_run_logger()` not `print()` |
| Credential errors | Verify block names in Prefect Cloud UI: `aws-credentials-tim`, `tiingo-api-token` |
| Empty ticker list | Check `s3://mh-guess-data/adhoc/tickers.txt` exists and has content |
| API rate limits | Tiingo free tier allows 50 requests/hour; check Tiingo dashboard |

## Running Fundamentals Pipelines

```bash
# Daily fundamentals flow
uvx prefect-cloud run tiingo_fundamentals_flow/tiingo_fundamentals_flow

# Backfill fundamentals 2020-2024 for all tickers
uvx prefect-cloud run tiingo_fundamentals_backfill_flow/tiingo_fundamentals_backfill_flow \
  --parameter start_year=2020 --parameter end_year=2024

# Backfill specific tickers
uvx prefect-cloud run tiingo_fundamentals_backfill_flow/tiingo_fundamentals_backfill_flow \
  --parameter start_year=2023 --parameter end_year=2024 \
  --parameter tickers='["AAPL","TSLA"]'
```

## Deploying Fundamentals Pipelines

```bash
uvx prefect-cloud deploy tiingo_fundamentals_flow.py:tiingo_fundamentals_flow \
  --from mh-guess/data-flow --with-requirements requirements.txt

uvx prefect-cloud deploy tiingo_fundamentals_backfill_flow.py:tiingo_fundamentals_backfill_flow \
  --from mh-guess/data-flow --with-requirements requirements.txt
```

## Key Links

- [Prefect Cloud Workspace](https://app.prefect.cloud/account/dd30d6c9-aba2-4983-bdb6-aa698e89e845/workspace/426a0775-66ec-4813-a87c-c00c1bd2550b)
- [GitHub Repo](https://github.com/mh-guess/data-flow)
- [Tiingo API Docs](https://www.tiingo.com/documentation/end-of-day)
