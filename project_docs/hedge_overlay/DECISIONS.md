# Hedge Map ETL — Design Decisions & Run Report

**Part A of MF Drift Hedge Overlay**
**Date:** 2026-06-24
**Status:** Green run confirmed

---

## Canonical S3 URI (the apex team's config value)

```
s3://mh-guess-data/hedge_map/effective_date=YYYY-MM-DD/data.parquet
```

Set `hedge.map_s3_path = s3://mh-guess-data/hedge_map/` in the apex config.
Consumers read the partition for `effective_date = today()` to get the pre-computed hedge for that session.

Sidecar manifest:
```
s3://mh-guess-data/hedge_map/manifests/effective_date=YYYY-MM-DD/manifest.json
```

Universe snapshots (audit trail):
```
s3://mh-guess-data/hedge_map/universe_snapshots/as_of=YYYY-MM-DD/universe.parquet
```

---

## Green Run Report (2026-06-24)

Run command:
```
python run_local.py --as-of 2026-06-20
```

Results:
- Universe: 13,116 active tradable US equities
- Eligible (≥90 calendar days listed): 12,078
- Covered tickers with valid hedge (rank=1): 12,058
- Coverage: **99.83%**
- Hedge map rows: 36,174 (3 rows per covered ticker)
- S3 URI: `s3://mh-guess-data/hedge_map/effective_date=2026-06-22/data.parquet`
- Manifest: `s3://mh-guess-data/hedge_map/manifests/effective_date=2026-06-22/manifest.json`

Note: `run_local.py` exercises the exact same computation logic as `hedge_map_flow.py` but uses
boto3 directly instead of `AwsCredentials.load()` (which requires a live Prefect Cloud session).
For scheduled production runs, the full Prefect flow is used.

---

## Parity Report (2026-06-24)

Against `research/motley_fool/etf_hedge/hedge_resolved.csv` (186 events with valid hedge):

| Classification | Count | % |
|---|---|---|
| MATCH | 122 | 65.6% |
| EXPECTED_DIV | 64 | 34.4% |
| UNEXPECTED_DIV | **0** | **0%** |

**Gate: PASS** — zero unexpected divergences.

### Why 34.4% diverge (all expected)

Production applies a `liquid + shortable` screen that research did NOT apply to primary picks:
- Research `select_hedges.py` ranked all 57 ETFs by R², no liquidity/shortability constraint.
- Research `resolve_fallback.py` applied the $25M ADV screen, but only for fallback resolution.
- Production applies $25M ADV AND `shortable=True` to ALL candidates.

Divergence breakdown:
- **49 events**: research ETF had trailing-30d $ADV < $25M (illiquid at pick date).
  Common culprits: ARKW (~$10–25M in 2018–2021), IBUY (~$5–20M pre-2021), WCLD (~$10–22M 2020–2023), FFTY (~$1–8M), TAN, SKYY, XSW, XHE, IPO.
- **14 events**: research ETF not currently shortable (IYT, XRT, WCLD, XBI, KRE, TAN, JETS, etc.).
- **1 event**: WORK/2019-09-05 — OIH had n_obs=52 (below the 60-obs minimum; the stock had just 52 trading days of overlap at that date).

All divergences are explained by the production liquid+shortable screen. No unexplained anomalies.

---

## Schema (frozen contract with apex team)

Partition key: `effective_date=YYYY-MM-DD/`

| Field | Type | Notes |
|---|---|---|
| `effective_date` | date | Session this map is valid for (next trading day after as_of) |
| `as_of_date` | date | Close date through which betas were computed |
| `ticker` | str | Alpaca-canonical symbol (post SYMBOL_REMAP) |
| `rank` | int | 1–3 (1 = chosen hedge) |
| `hedge_etf` | str | ETF symbol |
| `beta` | float | Trailing-60d OLS slope |
| `r2` | float | R² (= corr²) |
| `n_obs` | int | Overlapping daily return obs used |
| `etf_shortable` | bool | Alpaca flag at as_of |
| `etf_easy_to_borrow` | bool | Alpaca flag at as_of |
| `etf_adv_usd_30d` | float | ETF trailing-30d $ADV |
| `stock_adv_usd_30d` | float | Stock trailing-30d $ADV (context only) |
| `selection_basis` | str | `liquid_top_r2` |

---

## Key Design Decisions

### 1. Data source: Alpaca SIP (not Tiingo)

Alpaca provides the universe (`get_all_assets`), asset metadata (shortable, easy_to_borrow), and split-adjusted daily bars via the multi-symbol bars endpoint. Tiingo could serve as an alternative but is not used here to keep credentials to one provider for this pipeline.

### 2. Beta/R² methodology: exact parity with lib.py

`beta_r2_pair` in `hedge_map_flow.py` exactly matches `lib.py::beta_r2`:
- Window: trailing 60 trading days ending the day BEFORE `as_of` (no look-ahead).
- `tail(lookback)` after date filter (not slicing by index).
- `cov(s,e) / var(e, ddof=1)` for beta; `corrcoef(s,e)[0,1]²` for R².
- One change from lib.py: rejection threshold is `n < MIN_N_OBS` (60) instead of `n < 30`.
  This makes the function self-contained — no caller should accept betas from partial 60d windows.

### 3. Production selection screen

Rank by R², but only among ETFs that are BOTH liquid (trailing-30d $ADV ≥ $25M) AND shortable.
The research primary selection had no such filter; the $25M screen appeared only in `resolve_fallback.py`.
PM decision: keep the production screen and accept divergences from research.

### 4. Top-3 hedges stored

The apex runtime v1 uses `rank=1`. Ranks 2–3 are stored for diagnostics and potential live shortability re-check without recompute.

### 5. n_obs ≥ 60 required

The spec requires at least 60 overlapping daily return observations for a valid beta. Tickers that fail this requirement (typically very new listings that pass the 90-day eligibility check but have sparse bars) do not get a hedge row and fall back to Tier 2/3/4 at runtime.

### 6. ETF shortability sourced from Alpaca

ETFs found in the `us_equity` active universe are read from there. ETFs absent from that classification (rare) are fetched individually from the asset endpoint. This avoids silently defaulting any candidate ETF to non-shortable.

### 7. Bar fetch window

`bar_end = as_of - 1d` (defensive: never fetch the as_of day itself).
`bar_start = earliest_as_of - 130 calendar days` (covers 60 trading days + 30 ADV days + buffer for holiday-heavy periods).

### 8. SYMBOL_REMAP

Applied at universe fetch time: `MOGA→MOG.A`, `BRKB→BRK.B`, `SIVB.Q→SIVB`. Matches lib.py exactly.

### 9. Idempotency

S3 `put_object` overwrites. Re-running for the same `as_of` date replaces the partition. Safe to re-run.

### 10. Prefect Cloud session expired at build time

Prefect Cloud auth was unavailable during development (expired API key in `~/.prefect/profiles.toml`).
The `run_local.py` script was used for the green run — it exercises the exact same computation code from `hedge_map_flow.py` but uses boto3 directly and calls `_init_alpaca_creds(from_prefect_blocks=False)` to read `ALPACA_API_KEY` / `ALPACA_API_SECRET` from env vars.

For scheduled production runs (Prefect Cloud deployed path), the flow calls `_init_alpaca_creds(from_prefect_blocks=True)` at startup — which loads from `Secret.load("alpaca-api-key")` / `Secret.load("alpaca-api-secret")` Prefect blocks, with env-var fallback if blocks are unavailable. **Before deploying to Prefect Cloud, seed the Alpaca blocks** (see Run Instructions below).

### 11. Scheduled default as_of (P1-b)

The scheduled cron runs at 18:30 ET (after market close) on Mon–Fri. The correct `as_of` for that run is **today** (today's close is complete), yielding `effective_date = next_trading_day(today)` = the upcoming session. The flow uses `_latest_trading_day(date.today())` which returns `today` when today is a weekday, else the most recent weekday. The old `_prior_trading_day(date.today())` returned yesterday, making `effective_date = today` (already expired).

---

## Run Instructions

### Manual run (local, bypasses Prefect Cloud)
```bash
cd data-flow
python run_local.py                    # prior trading day, full universe
python run_local.py --as-of 2026-06-20  # specific date
python run_local.py --subset AAPL,MSFT,NVDA  # small subset for testing
python run_local.py --backfill-days 5   # last 5 trading days
```

### Deployed Prefect run
```bash
# First, seed Alpaca Prefect Secret blocks (one-time):
prefect block create secret --name alpaca-api-key --value $ALPACA_API_KEY
prefect block create secret --name alpaca-api-secret --value $ALPACA_API_SECRET

# Deploy:
prefect deploy -n hedge_map_flow --no-prompt

# Trigger manually:
prefect deployment run 'Hedge Map ETL/hedge_map_flow'

# With parameters:
prefect deployment run 'Hedge Map ETL/hedge_map_flow' \
  -p as_of_override=2026-06-20 \
  -p subset_symbols='["AAPL","MSFT","NVDA"]'
```

Note: The deployed flow loads credentials via `AwsCredentials.load("aws-credentials-tim")`.
Alpaca credentials are loaded from Prefect Secret blocks (`alpaca-api-key`, `alpaca-api-secret`) with
env-var fallback (`ALPACA_API_KEY` / `ALPACA_API_SECRET`) — the deployed worker does NOT need env vars
if the blocks are seeded.

### Tests
```bash
pytest test_hedge_map.py -v  # 42 tests, all offline
```

---

## ETF Set Version

`v1-57etf-20260624` — the research 57 ETFs from `research/motley_fool/etf_hedge/etf_candidates.csv`.
To update the candidate set, bump `ETF_SET_VERSION` in `hedge_map_flow.py` and re-run.
