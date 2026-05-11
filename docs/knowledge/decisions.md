# Decision Log

## 2026-05-08: Two-category documentation model (notes + knowledge)

**Context:** Setting up project documentation structure. Debated whether to use two categories (notes, knowledge) or three (notes, knowledge, decisions).

**Decision:** Two categories -- notes (short-term context) and knowledge (long-term reference). Decisions are tracked as a segment within knowledge rather than a standalone category.

**Argument for three:** Decisions have a different consumption pattern than reference knowledge. Knowledge gets looked up ("how does this work?"), decisions get reviewed for precedent ("have we been here before?"). Mixing them risks losing the browseable timeline that makes a decision log useful.

**Argument for two (accepted):** Every additional category adds cognitive overhead on every future write ("where does this go?"). Two categories is a near-zero-cost decision: is this ephemeral or durable? Adding a third means distinguishing "durable reference" from "durable precedent," which is a blurrier line. At current team size, the retrieval benefit doesn't outweigh the maintenance cost.

**Revisit signal:** If the team starts re-debating settled questions, that's the indicator that decisions need their own top-level surface.

## 2026-05-08: S3 restructure with source-level prefix (`price_eod/`)

**Context:** Adding fundamentals data ingestion alongside existing EOD price data. The existing S3 layout put `load_type=daily/` and `load_type=retro/` directly under `tiingo/json/`, leaving no room for a second data source without ambiguity.

**Decision:** Add a source-level prefix: `tiingo/json/price_eod/` for existing EOD data, `tiingo/json/fundamentals/` for new fundamentals data. Migrate existing objects rather than leaving them at the old path.

**Alternatives considered:**
- *Add fundamentals/ without moving existing data* -- avoids migration but creates asymmetry (`tiingo/json/load_type=daily/` alongside `tiingo/json/fundamentals/daily/`). Confusing as more sources are added.
- *Use `eod/` prefix* -- too generic. `price_eod` is explicit about what the data is (end-of-day prices), which matters when the data lake grows.

**Migration approach:** Copy-then-verify-then-delete. Migration script copies objects to new paths but does not delete originals. Manual cleanup after verification.

## 2026-05-08: Extract shared utilities into `shared.py`

**Context:** `fetch_tickers_from_s3` and credential-loading boilerplate were duplicated identically across `tiingo_to_s3_flow.py` and `tiingo_backfill_flow.py`. Adding fundamentals pipelines would create 4+ copies.

**Decision:** Extract shared constants, `fetch_tickers_from_s3` task, and `load_credentials()` helper into `shared.py`. Existing EOD-specific load tasks (`load_to_s3`, `load_year_to_s3`) stay in their respective files to minimize production blast radius.

**Revisit signal:** If more shared tasks accumulate, consider a `utils/` package instead of a single module.
