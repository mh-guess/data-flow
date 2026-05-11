# Backlog

*Last updated: 2026-05-11*

## Immediate

1. **Verify fundamentals backfill** completed successfully (check `belligerent-taipan` run)

## Short-term

2. **Bump prefect version pin** -- currently pinned to 3.6.29 due to broken 3.7.0 base image. Test newer versions periodically using `test_flow.py`
3. **File Prefect bug report** for the 3.7.0 `_aget_default_persist_result` import error in managed work pools
4. **Processing layer** -- combine retro + daily data into unified, queryable datasets
5. **Deduplication logic** -- handle overlapping date ranges between daily and retro loads

## Medium-term

6. **Data quality checks** -- validate completeness (missing dates, tickers) and accuracy
7. **Pipeline monitoring** -- alerts for failures, stale data, or missing tickers
8. **Downstream analytics** -- visualization or model consumption layer

## Later

9. **Other Tiingo endpoints** -- news, crypto, etc.
10. **Data catalog / metadata layer** -- schema registry or lightweight manifest
11. **Incremental retro updates** -- avoid re-fetching entire years when only appending
