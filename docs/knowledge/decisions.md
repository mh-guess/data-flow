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

## 2026-05-08: Ingest both `asReported` variants for financial statements

**Context:** Tiingo's statements endpoint has an `asReported` parameter. `asReported=false` returns the latest revised values dated by fiscal period end. `asReported=true` returns values as originally filed with the SEC, dated by filing date. The revised version does NOT include a revision timestamp.

**Decision:** Ingest both variants as separate S3 partitions (`as_reported=true/` and `as_reported=false/`). This doubles the statements API calls but gives downstream consumers both views.

**Rationale:** Without a revision timestamp on the `asReported=false` data, there's no way to reconstruct point-in-time accuracy from revised data alone. The `asReported=true` variant is required to avoid look-ahead bias in any trading signal or backtesting use case.

## 2026-05-11: Pin `prefect[aws]==3.6.29` in requirements.txt

**Context:** All pipelines failed starting May 7 with `ImportError: cannot import name '_aget_default_persist_result' from 'prefect.results'`. Investigation revealed two compounding issues:

1. **Archived package conflict:** Our `requirements.txt` installed the standalone `prefect-aws` package, which was archived in March 2026 and folded into the main `prefect` package as an extra (`prefect[aws]`). Installing the archived package on top of the managed work pool's pre-installed prefect caused partial module overwrites.

2. **Broken base image:** Prefect 3.7.0 was released May 6 and the managed work pool's floating Docker tag picked it up. Even without the `prefect-aws` conflict, 3.7.0 has an internal inconsistency where `task_engine.py` imports `_aget_default_persist_result` from `results.py`, but that symbol doesn't exist. Confirmed by testing with `prefect[aws]` (no standalone package) -- still fails.

**Decision:** Pin `prefect[aws]==3.6.29` in requirements.txt. This:
- Uses the built-in AWS extra instead of the archived standalone package
- Forces pip to replace the broken 3.7.0 in the base image with the last stable 3.6.x

**Alternatives considered:**
- *Remove prefect entirely from requirements* -- doesn't work because the base image's broken 3.7.0 is what runs
- *Use `prefect[aws]` without pinning* -- doesn't work because it doesn't change the pre-installed broken version
- *File a bug report and wait* -- too slow, pipelines are down

**Verification:** Created `test_flow.py` that validates imports, S3 access, and Tiingo API access without touching production data. Passed all checks with `prefect[aws]==3.6.29`.

**Revisit signal:** Periodically test newer prefect versions using `test_flow.py`. When a version works, bump the pin. File a bug report with Prefect about the 3.7.0 issue.

## 2026-05-17: Migrate to `prefect deploy` from `uvx prefect-cloud deploy`

**Context:** The vol table pipeline's schedule kept disappearing after redeployments. Investigation revealed that `uvx prefect-cloud deploy` ignores `prefect.yaml` entirely — it doesn't apply schedules, tags, or descriptions defined there. Schedules had to be set manually after every deploy, which was error-prone and not infrastructure-as-code.

**Additional problem:** The two tools create deployments under different flow name identities. `uvx prefect-cloud deploy` uses the Python function name (e.g., `vol_table_flow`), while `prefect deploy` uses the `@flow(name=...)` decorator (e.g., `APEX Volatility Table`). This made them incompatible — couldn't switch from one to the other without hitting the 5-deployment limit.

**Decision:** Delete all 5 existing deployments and recreate them with `prefect deploy --all`. This is Prefect's standard deployment pattern: `prefect.yaml` is the single source of truth for deployment config including schedules, and `prefect deploy` applies it.

**Changes:**
- Fixed `prefect.yaml`: aligned work pool name (`default-work-pool`), deployment names match function names, removed redundant push/requirements sections
- Deployment command: `prefect deploy --all --no-prompt` (replaces `uvx prefect-cloud deploy`)
- Manual run command: `prefect deployment run 'Flow Name/deployment_name'` (replaces `uvx prefect-cloud run`)

**Revisit signal:** If `prefect deploy` breaks due to version mismatch (local CLI vs managed work pool), the fallback is `uvx prefect-cloud deploy` + manual schedule setting via the Prefect SDK.

## 2026-05-28: Pin `importlib_metadata` in requirements.txt

**Context:** All 3 scheduled flows started crashing 2026-05-25 with `ModuleNotFoundError: No module named 'importlib_metadata'`. The crash happened during flow load — `shared.py` does `from prefect_aws import AwsCredentials`, which triggers `prefect_aws/__init__.py` → `prefect.workers.base`, which does `from importlib_metadata import ...` (the backport package, not stdlib `importlib.metadata`). The managed work pool's base image stopped shipping the backport on or around 2026-05-25, so the import failed for every flow.

The last successful runs were 2026-05-22; the next scheduled runs all crashed. Same `prefect[aws]==3.6.29` pin, no code change — purely a base image regression.

**Decision:** Add `importlib_metadata>=8.0.0` to `requirements.txt` so the pull-step `pip install` always brings it in, regardless of what the base image ships.

**Alternatives considered:**
- *Replace `from prefect_aws import AwsCredentials` with `from prefect_aws.credentials import AwsCredentials`* — relies on Python's import system not eagerly running `prefect_aws/__init__.py`, which it does. Doesn't avoid the broken import path.
- *Build a custom Docker image with everything pinned* — most robust but slowest to set up; deferred until the base image breaks us a third time.
- *Upgrade to a newer Prefect version* — `3.7.0` is still broken (see prior decision), so this isn't a quick win.

**Verification:** After redeploying with the pin, manually triggered all 3 scheduled flows (`Tiingo to S3 ETL`, `Tiingo Fundamentals Daily`, `APEX Volatility Table`) — all completed successfully.

**Revisit signal:** Same as the prefect pin — every time the base image changes, something else may break similarly. Pattern: pin it in `requirements.txt`, redeploy, move on.

## 2026-05-28: Alert on flow failure via Prefect Cloud automation

**Context:** All 3 daily scheduled flows had been crashing for ~5 days before we noticed (silent failure). Without alerting, a broken pipeline can go unnoticed across multiple scheduled runs and we lose data freshness without realizing it.

**Decision:** Create a single Prefect Cloud automation **"Email on flow failure"** that triggers on any `prefect.flow-run.Failed` or `prefect.flow-run.Crashed` event and emails `timhuang.dev@gmail.com` via the Cloud-hosted email notification block `flow-failure-email`.

**Scope:** Covers *all* flow runs in the workspace (not just the 3 scheduled flows). Backfill flows are on-demand and could legitimately fail in ways the operator already knows about, but the cost of a few extra emails during a backfill is lower than the cost of building flow-specific scoping that we'd then have to maintain.

**Alternatives considered:**
- *Webhook → SES/SendGrid* — more control over sender identity, but adds infra we don't have.
- *Slack/Discord* — faster delivery, but no chat tool standardized for this project yet.
- *On-flow `on_crashed` / `on_failure` hooks in code* — only fires when the flow code itself runs, so misses the most common failure mode here (crash during import / pull step, before user code executes).

**Verification:** Block + automation were created programmatically via the Prefect SDK and confirmed enabled. The next time a flow crashes naturally, the email will validate end-to-end delivery.
