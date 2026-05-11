# Design Decisions

## Type-Partitioned S3 Structure (`load_type=daily` vs `load_type=retro`)

Daily and retro data have different granularities and update patterns. Keeping them in separate partitions avoids ambiguity about which data is authoritative for a given date range and makes it straightforward to build a processing layer that merges them with clear precedence rules.

## Year-Level Granularity for Retro

One file per ticker per year (~252 trading days) instead of one file per ticker per day. This keeps API calls minimal: 5 tickers x 5 years = 25 calls vs 5 x 1,260 trading days = 6,300 calls. The tradeoff is larger individual files, but at ~50KB per year-file this is negligible.

## Raw Data, No Transformation

Tiingo API responses are saved as-is with no schema mapping or field renaming. This preserves the source of truth and avoids encoding assumptions about downstream consumers into the ingestion layer. Transformation belongs in a separate processing step.

## Compact JSON (No Pretty Printing)

`json.dumps(data)` without indentation. Saves ~40% storage vs pretty-printed JSON. Readability is handled by tooling (`jq`, etc.) rather than file format.

## Dynamic Tickers from S3

Ticker list lives in `s3://mh-guess-data/adhoc/tickers.txt` rather than hardcoded in Python. This decouples ticker management from code deployment -- adding a ticker is an S3 edit, not a commit + deploy cycle.

## Prefect Cloud for Orchestration

Chosen for managed scheduling, retry logic, credential management (blocks), and log aggregation. Avoids self-hosting Airflow or similar. Deployments are managed via `prefect.yaml` (infrastructure as code).

## 30-Day Rolling Window for Daily

The daily pipeline fetches 30 days per run even though it runs daily. This provides natural overlap that guards against missed runs -- if a day is skipped, the next run covers the gap. The downstream processing layer will handle deduplication.
