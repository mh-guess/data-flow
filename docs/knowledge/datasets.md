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
```

## Storage Format

All files are raw Tiingo API responses stored as compact JSON (no indentation). One file per ticker per partition, except definitions and meta which are single files containing all tickers/metrics.
