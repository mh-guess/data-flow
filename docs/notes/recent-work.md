# Recent Work

*Last updated: 2026-05-08*

## Completed (reverse chronological)

1. **CLAUDE.md project summary** -- Documented full project state for future sessions
2. **Prefect logger migration** -- Replaced all `print()` with `get_run_logger()` for Prefect Cloud visibility
3. **Historical backfill pipeline** -- Built `tiingo_backfill_flow.py` with year-level partitioning, ran backfills for 2020-2025
4. **Removed example flow** -- Cleaned up starter code
5. **Infrastructure as Code** -- Added `prefect.yaml` for deployment management
6. **Compact JSON** -- Removed pretty formatting to save S3 storage
7. **Raw data preservation** -- Stopped transforming Tiingo responses, save as-is
8. **Dynamic tickers** -- Moved ticker list to S3 (`adhoc/tickers.txt`) instead of hardcoding
9. **Date-based partitioning** -- Refactored S3 folder structure for queryability
10. **Initial pipeline** -- Built daily Tiingo-to-S3 ETL with Prefect Cloud integration
