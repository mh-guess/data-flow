# Operations Runbook

## Deploying Changes

All deployments are defined in `prefect.yaml` (schedules, work pool, pull steps). After code changes are committed and pushed:

```bash
# Deploy all pipelines (schedules applied automatically from prefect.yaml)
prefect deploy --all --no-prompt

# Deploy a single pipeline by name
prefect deploy -n vol_table_flow --no-prompt
```

**Important:** Do NOT use `uvx prefect-cloud deploy` — it ignores `prefect.yaml` and does not apply schedules. Always use `prefect deploy` so schedules stay in sync with the YAML definition.

## Running Pipelines Manually

```bash
# EOD daily flow
prefect deployment run 'Tiingo to S3 ETL/tiingo_to_s3_flow'

# EOD backfill (year range)
prefect deployment run 'Tiingo Historical Backfill/tiingo_backfill_flow' \
  -p start_year=2020 -p end_year=2024

# Fundamentals daily flow
prefect deployment run 'Tiingo Fundamentals Daily/tiingo_fundamentals_flow'

# Fundamentals backfill
prefect deployment run 'Tiingo Fundamentals Backfill/tiingo_fundamentals_backfill_flow' \
  -p start_year=2020 -p end_year=2024

# APEX volatility table
prefect deployment run 'APEX Volatility Table/vol_table_flow'
```

## Managing Schedules

Schedules are defined as code in `prefect.yaml`. To change a schedule:

1. Edit the `schedules` section in `prefect.yaml`
2. Run `prefect deploy -n <deployment_name> --no-prompt`

The schedule is applied automatically — no separate CLI command needed.

## Adding / Removing Tickers

**Tiingo pipelines:** Edit `s3://mh-guess-data/adhoc/tickers.txt` directly (one ticker per line). No code change or redeployment needed. Takes effect on the next pipeline run.

**Vol table pipeline:** Edit `symbols.yaml` in the `mh-guess/apex` GitHub repo. The pipeline fetches it from GitHub at runtime — no changes needed in data-flow.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Pipeline not running on schedule | Run `prefect deploy -n <name> --no-prompt` to re-apply schedule from prefect.yaml |
| Missing logs in Prefect Cloud | Ensure code uses `get_run_logger()` not `print()` |
| Credential errors | Verify block names in Prefect Cloud UI: `aws-credentials-tim`, `tiingo-api-token`, `github-pat-apex` |
| Empty ticker list | Check `s3://mh-guess-data/adhoc/tickers.txt` exists and has content |
| API rate limits | Tiingo free tier allows 50 requests/hour; check Tiingo dashboard |
| Vol table GitHub 404 | Check `github-pat-apex` Secret block has a valid PAT with `repo` scope |

## Key Links

- [Prefect Cloud Workspace](https://app.prefect.cloud/account/dd30d6c9-aba2-4983-bdb6-aa698e89e845/workspace/426a0775-66ec-4813-a87c-c00c1bd2550b)
- [GitHub Repo](https://github.com/mh-guess/data-flow)
- [Tiingo API Docs](https://www.tiingo.com/documentation/end-of-day)
