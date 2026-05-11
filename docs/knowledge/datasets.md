# Datasets

## 1. EOD Prices (`price_eod`)

End-of-day price data from the Tiingo Daily Prices API.

**API endpoint:** `GET https://api.tiingo.com/tiingo/daily/{ticker}/prices`

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `date` | string (ISO 8601) | Trading date |
| `open` | float | Opening price |
| `high` | float | Intraday high |
| `low` | float | Intraday low |
| `close` | float | Closing price |
| `volume` | int | Shares traded |
| `adjOpen` | float | Split/dividend-adjusted open |
| `adjHigh` | float | Split/dividend-adjusted high |
| `adjLow` | float | Split/dividend-adjusted low |
| `adjClose` | float | Split/dividend-adjusted close |
| `adjVolume` | int | Adjusted volume |
| `divCash` | float | Cash dividend on this date (0.0 if none) |
| `splitFactor` | float | Split factor (1.0 if no split) |

### S3 Layout

```
tiingo/json/price_eod/
├── load_type=daily/                    # Incremental (scheduled 6 PM weekdays)
│   └── date={YYYY-MM-DD}/             # Date the pipeline ran
│       └── {TICKER}.json              # Array of records (last 30 days)
└── load_type=retro/                    # Historical backfill (on-demand)
    └── year={YYYY}/                    # One file per ticker per year
        └── {TICKER}.json              # Array of ~252 trading day records
```

**Daily ingestion:** Each run fetches the last 30 days per ticker. The overlap with previous runs is intentional -- guards against missed runs. Downstream deduplication is needed.

**Retro ingestion:** One API call per ticker per year. Completed for 2020-2025.

---

## 2. Fundamentals Daily Metrics (`fundamentals/daily`)

Price-dependent fundamental ratios from the Tiingo Fundamentals Daily API. Updated each trading day.

**API endpoint:** `GET https://api.tiingo.com/tiingo/fundamentals/{ticker}/daily`

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `date` | string (ISO 8601) | Trading date |
| `marketCap` | float | Market capitalization (USD) |
| `enterpriseVal` | float | Enterprise value (USD) |
| `peRatio` | float | Price-to-earnings ratio |
| `pbRatio` | float | Price-to-book ratio |
| `trailingPEG1Y` | float | Trailing 1-year PEG ratio |

### S3 Layout

```
tiingo/json/fundamentals/daily/
├── load_type=daily/date={YYYY-MM-DD}/{TICKER}.json
└── load_type=retro/year={YYYY}/{TICKER}.json
```

Same daily/retro pattern as EOD prices.

---

## 3. Financial Statements (`fundamentals/statements`)

Quarterly and annual financial statements from SEC filings. Includes income statement, balance sheet, cash flow, and overview metrics.

**API endpoint:** `GET https://api.tiingo.com/tiingo/fundamentals/{ticker}/statements`

### Top-Level Schema

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Fiscal period end date (`asReported=false`) or SEC filing date (`asReported=true`) |
| `year` | int | Fiscal year |
| `quarter` | int | Fiscal quarter (1-4, or 0 for annual) |
| `statementData` | object | Nested object with four sections (see below) |

### `statementData` Sections

**`incomeStatement`** (22 fields):
`revenue`, `epsDil`, `netinc`, `shareswaDil`, `eps`, `ebitda`, `rnd`, `sga`, `opinc`, `nonControllingInterests`, `grossProfit`, `shareswa`, `prefDVDs`, `opex`, `ebt`, `taxExp`, `costRev`, `netIncDiscOps`, `ebit`, `netIncComStock`, `intexp`, `consolidatedIncome`

**`balanceSheet`** (26 fields):
`sharesBasic`, `ppeq`, `investmentsCurrent`, `taxLiabilities`, `assetsNonCurrent`, `taxAssets`, `debtCurrent`, `retainedEarnings`, `liabilitiesNonCurrent`, `liabilitiesCurrent`, `acctRec`, `cashAndEq`, `accoci`, `assetsCurrent`, `investments`, `intangibles`, `inventory`, `deposits`, `investmentsNonCurrent`, `totalAssets`, `deferredRev`, `debt`, `acctPay`, `totalLiabilities`, `debtNonCurrent`, `equity`

**`cashFlow`** (14 fields):
`freeCashFlow`, `issrepayDebt`, `capex`, `payDiv`, `investmentsAcqDisposals`, `ncff`, `issrepayEquity`, `ncfx`, `ncfo`, `sbcomp`, `businessAcqDisposals`, `depamor`, `ncf`, `ncfi`

**`overview`** (14 fields):
`rps`, `roa`, `bookVal`, `bvps`, `profitMargin`, `revenueQoQ`, `debtEquity`, `grossMargin`, `roe`, `currentRatio`, `piotroskiFScore`, `longTermDebtEquity`, `epsQoQ`, `shareFactor`

Each field within a section is stored as `{"dataCode": "revenue", "value": 124300000000.0}`.

### `asReported` Variants

We ingest both variants for each ticker:

| Variant | `date` meaning | Values | Use case |
|---------|---------------|--------|----------|
| `asReported=false` | Fiscal period end date | Latest revised/corrected | Best-known truth |
| `asReported=true` | SEC filing date | Original as-filed values | Point-in-time accuracy (no look-ahead bias) |

### S3 Layout

```
tiingo/json/fundamentals/statements/
├── as_reported=true/
│   ├── load_type=daily/date={YYYY-MM-DD}/{TICKER}.json
│   └── load_type=retro/year={YYYY}/{TICKER}.json
└── as_reported=false/
    ├── load_type=daily/date={YYYY-MM-DD}/{TICKER}.json
    └── load_type=retro/year={YYYY}/{TICKER}.json
```

---

## 4. Metric Definitions (`fundamentals/definitions`)

Reference data describing all available fundamental metrics. 85 definitions as of May 2026.

**API endpoint:** `GET https://api.tiingo.com/tiingo/fundamentals/definitions`

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `dataCode` | string | Machine-readable metric ID (e.g., `revenue`, `peRatio`) |
| `name` | string | Human-readable name (e.g., "Revenue Per Share") |
| `description` | string | What the metric measures |
| `statementType` | string | Which statement section it belongs to (`overview`, `incomeStatement`, `balanceSheet`, `cashFlow`) |
| `units` | string or null | Unit of measurement (`$`, `%`, or null for ratios) |

### S3 Layout

```
tiingo/json/fundamentals/definitions/
└── date={YYYY-MM-DD}/definitions.json      # Single file, all definitions
```

Fetched daily. Changes very rarely -- acts as a snapshot of available metrics.

---

## 5. Company Metadata (`fundamentals/meta`)

Company reference data: sector, industry, location, filing info.

**API endpoint:** `GET https://api.tiingo.com/tiingo/fundamentals/meta?tickers=X,Y,Z`

### Schema

| Field | Type | Description |
|-------|------|-------------|
| `permaTicker` | string | Tiingo permanent ticker ID |
| `ticker` | string | Stock symbol (lowercase) |
| `name` | string | Company name |
| `isActive` | bool | Currently trading |
| `isADR` | bool | American Depositary Receipt |
| `sector` | string | Sector (e.g., "Technology") |
| `industry` | string | Industry (e.g., "Consumer Electronics") |
| `sicCode` | int | SIC classification code |
| `sicSector` | string | SIC sector name |
| `sicIndustry` | string | SIC industry name |
| `reportingCurrency` | string | Currency of financial reports (e.g., "usd") |
| `location` | string | Headquarters (e.g., "California, USA") |
| `companyWebsite` | string | URL |
| `secFilingWebsite` | string | SEC EDGAR URL |
| `statementLastUpdated` | string (ISO 8601) | Last statement data update |
| `dailyLastUpdated` | string (ISO 8601) | Last daily metrics update |
| `dataProviderPermaTicker` | string | Provider internal ID |

### S3 Layout

```
tiingo/json/fundamentals/meta/
└── date={YYYY-MM-DD}/meta.json             # Single file, all tickers
```

Fetched daily. Changes infrequently (when companies restructure, reclassify, etc.).

---

---

## 6. APEX Volatility Table (`derived/volatility`)

Computed trailing volatility metrics for the APEX trading symbol universe. Used by the APEX trade execution system to set limit order price thresholds calibrated to each stock's volatility.

**Pipeline:** `vol_table_flow.py` (scheduled 6 PM ET weekdays)

**Data source:** Tiingo EOD prices API (180 calendar days / ~122 trading days lookback, using `adjClose`)

**Symbol source:** `symbols.yaml` from `mh-guess/apex` GitHub repo (fetched at runtime via PAT)

### S3 Locations

```
s3://apex-market-data-raw-220464759930/derived/volatility/
├── {YYYY-MM-DD}/vol_table.parquet    # Date-partitioned (retained for history)
└── vol_table_latest.parquet          # Overwritten every run (use this for consumption)
```

**For downstream consumers:** read `vol_table_latest.parquet` — it always contains the most recent computation. Date-partitioned files are retained for debugging and auditing.

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Ticker symbol (e.g., "CRDO") |
| `daily_vol` | float64 | Daily standard deviation of log returns (non-annualized) |
| `daily_vol_annualized` | float64 | Annualized volatility (`daily_vol * sqrt(252)`) |
| `per_minute_vol` | float64 | Estimated per-minute volatility (`daily_vol / sqrt(390)`) |
| `lookback_days` | Int32 | Number of trading days actually used (typically ~122) |
| `data_start_date` | date | First date in the lookback window |
| `data_end_date` | date | Last date in the lookback window (prior trading day) |
| `short_history` | bool | True if `lookback_days < 90` |
| `no_data` | bool | True if ticker had zero data (all vol fields will be null) |
| `computed_at` | timestamp (UTC) | When this row was computed |

### Computation

For each ticker, using ~122 trading days of adjusted close prices:

1. Log returns: `ln(adjClose[t] / adjClose[t-1])`
2. `daily_vol` = sample standard deviation of log returns (`ddof=1`)
3. `daily_vol_annualized` = `daily_vol * sqrt(252)`
4. `per_minute_vol` = `daily_vol / sqrt(390)` (390 = trading minutes per day)

### Consuming the data

```python
import pandas as pd

df = pd.read_parquet("s3://apex-market-data-raw-220464759930/derived/volatility/vol_table_latest.parquet")

# Look up a ticker's per-minute volatility
ticker_vol = df.loc[df['symbol'] == 'CRDO', 'per_minute_vol'].iloc[0]

# Example: compute a limit price threshold
limit_price = ask_price * (1 + N * ticker_vol)
```

### Frequency and timing

- **Schedule:** 6 PM ET, Monday–Friday
- **Runtime:** ~4 minutes (100 tickers × 2s rate limit + setup)
- **Retention:** Date-partitioned files are kept indefinitely; `vol_table_latest.parquet` is overwritten each run

### Edge cases

- **`no_data = True`**: Ticker not found on Tiingo (e.g., `PBR.A` — share class suffix not supported). All vol fields are null. Consumer should fall back to a default threshold.
- **`short_history = True`**: Ticker has fewer than 90 trading days of data (e.g., recent IPO). Vol is computed but may be less reliable.
- **Adjusted close (`adjClose`)**: Used instead of raw `close` to prevent stock splits and dividends from creating artificial volatility spikes. Cross-validated against Alpaca SIP adjusted data (<0.5% difference).

### Access

- **S3 bucket:** `apex-market-data-raw-220464759930` (us-east-1)
- **Read access:** Requires `s3:GetObject` on `derived/volatility/*`
- **IAM:** Bucket policy managed in `apex/infra/main.tf`

---

## Complete S3 Tree

```
s3://mh-guess-data/tiingo/json/
├── price_eod/
│   ├── load_type=daily/date={date}/{TICKER}.json
│   └── load_type=retro/year={year}/{TICKER}.json
└── fundamentals/
    ├── daily/
    │   ├── load_type=daily/date={date}/{TICKER}.json
    │   └── load_type=retro/year={year}/{TICKER}.json
    ├── statements/
    │   ├── as_reported=true/
    │   │   ├── load_type=daily/date={date}/{TICKER}.json
    │   │   └── load_type=retro/year={year}/{TICKER}.json
    │   └── as_reported=false/
    │       ├── load_type=daily/date={date}/{TICKER}.json
    │       └── load_type=retro/year={year}/{TICKER}.json
    ├── definitions/date={date}/definitions.json
    └── meta/date={date}/meta.json

s3://apex-market-data-raw-220464759930/derived/volatility/
├── {YYYY-MM-DD}/vol_table.parquet
└── vol_table_latest.parquet
```

## Storage Format

Raw Tiingo data is stored as compact JSON (no indentation), one file per ticker per partition. Definitions and meta are single files containing all tickers/metrics. The volatility table is a single parquet file (snappy compression) with one row per ticker.
