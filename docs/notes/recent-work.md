# Recent Work

*Last updated: 2026-05-11*

## Session: 2026-05-11 (APEX volatility table pipeline)

### New pipeline: `vol_table_flow.py`
- Computes trailing volatility metrics (daily vol, annualized vol, per-minute vol) for the APEX symbol universe
- Fetches symbols from `mh-guess/apex` repo via GitHub API (classic PAT stored in `github-pat-apex` Prefect Secret block)
- Fetches 180 calendar days (~122 trading days) of EOD OHLCV from Tiingo API per ticker
- Uses `adjClose` (split/dividend adjusted) for log return calculation
- Writes parquet to `s3://apex-market-data-raw-220464759930/derived/volatility/{date}/vol_table.parquet` + a `vol_table_latest.parquet` overwritten each run
- Scheduled 6 PM ET weekdays (`America/New_York`)
- Deployed as 5th (final) Prefect Cloud deployment slot

### Validation against Alpaca SIP data
Cross-validated volatility output against Alpaca's `StockHistoricalDataClient` using `Adjustment.ALL` (split+dividend adjusted) and `DataFeed.SIP` (consolidated tape). Results for 5 tickers:

| Symbol | Tiingo Vol | Alpaca Vol | Diff |
|--------|-----------|-----------|------|
| MU | 0.7357 | 0.7372 | 0.21% |
| CRDO | 0.9054 | 0.9027 | 0.30% |
| AMD | 0.6813 | 0.6777 | 0.52% |
| DELL | 0.6166 | 0.6174 | 0.13% |
| FDX | 0.2906 | 0.2904 | 0.07% |

All within <1%. Residual differences are from minor variations in how Tiingo (multi-exchange aggregated EOD) and Alpaca (SIP consolidated tape) compute adjusted prices.

### Key decisions
- **adjClose over close**: prevents stock splits and dividends from creating artificial volatility spikes
- **GitHub PAT for symbols**: classic PAT with `repo` scope (fine-grained PATs require org opt-in for `mh-guess` org)
- **Tiingo data source**: EOD prices are aggregated from 3 exchanges (not IEX-only; IEX direct is only for their real-time product)

### First run results
- 100 symbols processed (symbols.yaml has grown from original 51)
- 99 valid, 1 no_data (`PBR.A` — Tiingo doesn't support `.A` share class suffix)
- 122 trading days per ticker, annualized vol range: 0.231–1.564
- Output: 9,793 bytes parquet

---

## Session: 2026-05-11 (production cleanup)

### Completed all 5 production action items
1. Redeployed all 4 production pipelines with final `prefect[aws]==3.6.29` requirements
2. Deleted test deployment (freed 1 of 5 deployment slots)
3. Set daily schedules for EOD and fundamentals pipelines (`0 18 * * 1-5`, America/Los_Angeles)
4. Verified fundamentals backfill (`belligerent-taipan`) ~90% complete with 0 failures as of 17:17 UTC
5. Deleted old S3 paths (`tiingo/json/load_type=daily/` and `tiingo/json/load_type=retro/`) after verifying object counts match new `price_eod/` paths

---

## Session: 2026-05-08 to 2026-05-11

### Documentation & Organization
- Created `docs/notes/` (short-term context) and `docs/knowledge/` (long-term reference)
- Seeded initial docs: architecture, design decisions, operations runbook, status, backlog
- Established decision log under `docs/knowledge/decisions.md`
- Created `docs/knowledge/datasets.md` with full schema documentation for all 5 datasets

### S3 Restructure (Phase 1)
- Extracted shared utilities into `shared.py` (credentials, ticker loading, S3 upload)
- Added `price_eod/` prefix to existing EOD price S3 paths
- Updated `tiingo_to_s3_flow.py` and `tiingo_backfill_flow.py` to import from shared module
- Created and ran `migrate_s3_eod_prefix.py` to copy 12,274 existing S3 objects to new paths
- Migration verified: object counts match at old and new paths

### Fundamentals Pipelines (Phase 2)
- Created `tiingo_fundamentals_flow.py`: daily scheduled pipeline for all 4 Tiingo fundamentals endpoints (definitions, meta, daily metrics, statements with both `asReported` variants)
- Created `tiingo_fundamentals_backfill_flow.py`: on-demand historical backfill with 3s rate limiting between API calls
- Added 2 new deployments to `prefect.yaml`
- Verified all 4 API endpoints return data (definitions: 85 metrics, meta: company info, daily: 5 ratios, statements: 76 fields across 4 sections)
- Created `test_flow.py` for validating the managed work pool environment without touching production data

### Prefect Version Conflict (Debugging & Fix)
- **Root cause discovered**: The standalone `prefect-aws` package was archived in March 2026 and folded into the main `prefect` package. Installing the archived package on top of Prefect Cloud's managed work pool base image caused internal import conflicts (`_aget_default_persist_result` missing from `prefect.results`).
- **Contributing factor**: Prefect 3.7.0 was released May 6 and the managed work pool base image updated to it. The combination of the new base image + archived `prefect-aws` package created the inconsistency.
- **Fix**: Replaced `prefect-aws>=0.4.0` with `prefect[aws]==3.6.29` in requirements.txt. This uses the built-in AWS integration extra (matched to the prefect version) and pins to 3.6.29 to avoid the broken 3.7.0 base image.
- **Verified**: Test flow passed all checks (S3 access, Tiingo API, credential loading) with the new requirements.
- This also fixed the pre-existing EOD daily pipeline failures that had been occurring since May 7.

### Deployment Status
- All 4 production pipelines deployed to Prefect Cloud
- Fundamentals backfill (2020-2025) running: 104 tickers x 6 years x 3 endpoints = 1,872 API calls
- 4 production deployments need redeployment with final `prefect[aws]==3.6.29` requirements (currently deployed with interim `prefect==3.6.29`)

---

## Prior Work (before this session)

1. CLAUDE.md project summary
2. Prefect logger migration (`print()` -> `get_run_logger()`)
3. Historical backfill pipeline with year-level partitioning
4. Removed example flow
5. Infrastructure as Code (`prefect.yaml`)
6. Compact JSON storage
7. Raw data preservation (no transformation)
8. Dynamic tickers from S3
9. Date-based partitioning
10. Initial Tiingo-to-S3 ETL pipeline
