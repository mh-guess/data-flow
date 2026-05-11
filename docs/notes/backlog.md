# Backlog

*Last updated: 2026-05-11*

## Immediate (unblock production)

1. **Redeploy all 4 production pipelines** with `prefect[aws]==3.6.29` requirements
2. **Delete test deployment** to free up the 5th deployment slot
3. **Set daily schedules** for EOD and fundamentals daily pipelines
4. **Verify fundamentals backfill** completed successfully (check `belligerent-taipan` run)
5. **Delete old S3 paths** (`tiingo/json/load_type=daily/`, `tiingo/json/load_type=retro/`) after confirming new paths work

## Short-term

6. **Bump prefect version pin** -- currently pinned to 3.6.29 due to broken 3.7.0 base image. Test newer versions periodically using `test_flow.py`
7. **File Prefect bug report** for the 3.7.0 `_aget_default_persist_result` import error in managed work pools
8. **Processing layer** -- combine retro + daily data into unified, queryable datasets
9. **Deduplication logic** -- handle overlapping date ranges between daily and retro loads

## Medium-term

10. **Data quality checks** -- validate completeness (missing dates, tickers) and accuracy
11. **Pipeline monitoring** -- alerts for failures, stale data, or missing tickers
12. **Downstream analytics** -- visualization or model consumption layer

## Later

13. **Other Tiingo endpoints** -- news, crypto, etc.
14. **Data catalog / metadata layer** -- schema registry or lightweight manifest
15. **Incremental retro updates** -- avoid re-fetching entire years when only appending
