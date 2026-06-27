# Hedge Map ETL v2 — Heuristic Sector/Industry Crosswalk

**Part A of MF Drift Hedge Overlay — selection redesign**
**Date:** 2026-06-26
**Status:** Implemented on `feat/hedge-map-heuristic`; local smoke run green.

---

## What changed (v1 → v2)

v1 selected each stock's hedge by ranking **all 57 research ETFs by trailing-60d
R²** and keeping the liquid+shortable top-3. That was noisy: the max-R² ETF for a
given 60-day window is often a broad-market or unrelated fund that happened to
correlate, and it churns window-to-window.

v2 replaces max-R² with a **deterministic sector/industry → ETF crosswalk**:

| rank | tier | ETF source |
|---|---|---|
| 1 | industry | pure-play industry ETF from the crosswalk (e.g. Semiconductors → SMH) |
| 2 | sector | SPDR sector ETF (e.g. Technology → XLK) |
| 3 | broad | SPY |

Beta still **sizes** each short via the existing point-in-time
`beta_r2_pair()` (trailing-60d OLS, `d <= as_of` inclusive, `MIN_N_OBS=60`), but
now against the **assigned** ETF only — one regression per tier, not 57.

The **output S3 path and row-per-rank schema are unchanged**, so the apex
resolver contract is untouched. v2 only **adds** columns.

---

## Components

| File | Role |
|---|---|
| `hedge_crosswalk.py` | The IP: `SECTOR_TO_ETF` (11 sectors, Tiingo + GICS variants), `INDUSTRY_TO_ETF` (~40 high-frequency Tiingo industries → pure-play ETFs), `classify()` ladder builder. No deps; import-safe. `CROSSWALK_VERSION = v2-heuristic-20260626`. |
| `hedge_classification.py` | Loads the **latest** Tiingo `meta.json` partition (`date <= as_of`) from S3, keyed by upper-cased + SYMBOL_REMAP'd ticker. |
| `hedge_selection.py` | `build_hedge_map()` — applies the crosswalk + the beta **quality gate** (demotion chain), emits the row-per-rank frame. `resolve_classification()` (Tiingo primary, SIC fallback). `R2_FLOOR = 0.05`. |
| `hedge_map_flow.py` | Flow rewired to the heuristic path; fetches bars only for the ~30 crosswalk ETFs; extended manifest. |
| `run_local.py` | Same, boto3-direct, with `--s3-prefix` for test runs. |
| `discovery_classification.py` | Read-only discovery: meta vocabulary frequency table. |
| `parity_heuristic.py` / `test_parity_heuristic.py` | Research-parity regression. |
| `test_hedge_heuristic.py` | Unit tests (crosswalk, ladder, classification, fallback chain, schema). |

---

## Classification source: Tiingo `meta`

- Primary fields: `sector`, `industry`.
- Fallback: `sicSector` / `sicIndustry`, used **only when Tiingo `sector` is
  null/blank**. We never mix a Tiingo sector with a SIC industry — the crosswalk
  was tuned against same-source pairs. `classification_source` records
  `tiingo` / `sic` / `none`.
- Tiingo tickers are lowercase; we upper-case + apply `SYMBOL_REMAP` to align
  with Alpaca-canonical symbols.

### Universe broadening (round 2)

The Tiingo fundamentals flow's **meta** partition was widened from the curated
~104-ticker `adhoc/tickers.txt` list to the **full Alpaca tradable us_equity
universe** (`get_all_assets(status=active, tradable=true, asset_class=us_equity)`,
~13k symbols). `tiingo_meta_universe.py` provides:

- `fetch_tradable_equity_universe()` — the broadened ticker source.
- `fetch_meta_batched()` — the Tiingo bulk `/fundamentals/meta?tickers=CSV`
  endpoint **batched** (150/call) and concatenated. A single giant GET fails
  (verified: 250/call returns 502; long URLs from suffixed symbols like
  `FOO.PRK` / `BAR.WS`); a failing batch is **retried once at half size** before
  its tickers are dropped (they fall through to SPY). 200/call is the empirical
  ceiling; 150 is the default for headroom.

`tiingo_fundamentals_flow.py` now drives the **meta** partition off the broadened
universe (`_resolve_meta_universe` unions the Alpaca universe with the curated
list, degrading to the curated list if Alpaca is unavailable). The expensive
per-ticker daily/statements calls stay on the curated list. ETFs / non-equity
that return no useful classification simply fall through to SPY.

(This was a data-source-LIST limit, not a Tiingo subscription cap: a probe of 15
oracle tickers absent from the old partition returned **15/15** full
sector+industry from the live endpoint.)

Coverage is fully **observable**: the manifest records counts/percentages of
`pure_play` / `sector_fallback` / `spy_fallback` and `tiingo` / `sic` / `none`.

### ⚠️ Ticker-reuse guard (round 2 — correctness-critical)

The Tiingo bulk meta endpoint returns **multiple rows per ticker** when a symbol
has been reused — the live company **and** delisted predecessors. Verified:

| ticker | active row | delisted predecessor (isActive=False) |
|---|---|---|
| U | Unity Software Inc → Software-Application | **US AIRWAYS GROUP INC → Airlines** |
| SNOW | Snowflake Inc → Software-Application | **Intrawest Resorts Holdings → Leisure** |

A naive "last row wins" load can pick the **delisted** predecessor and assign a
totally wrong industry (U → Airlines → JETS). `select_active_meta_row()` in
`hedge_classification.py` runs **before the crosswalk**:

1. Drop rows with `isActive != True`.
2. Among active rows, prefer the one whose `name` **reconciles** with the live
   Alpaca company name (`names_reconcile()` — significant-token overlap after
   stripping corporate/share-class noise; tolerates "Unity Software Inc" vs
   "Unity Software Inc." while rejecting "US Airways Group Inc").
3. `isActive=False`-only, or an active row that doesn't reconcile →
   **UNCLASSIFIED** (→ SPY). Never a wrong industry. The rank1 beta/R² gate
   stays as defense-in-depth.

**The guard RECOVERS the correct classification whenever a live active row
exists** — it does not merely fall back to SPY. Because the bulk endpoint
returns *both* the active company and the delisted predecessor, the guard keeps
the **active** row and the stock gets its proper industry/sector ETF (U → IGV,
SNOW → IGV). It only falls to SPY when **no** active row exists for the symbol
(`isActive=False`-only → `isactive_dropped`) or the sole active row doesn't match
the live company (`name_mismatch`). So the net effect is a *better* outcome than
the prior behavior — correct ETF, not SPY — for every reused ticker whose live
listing is present in the meta.

Each unclassified outcome is a **distinct, separately-counted bucket** (never
lumped into one generic 'none'), so the reuse blast radius is observable nightly:
`classification_source ∈ {tiingo, sic, no_meta, isactive_dropped, name_mismatch}`,
plus a `guard_drops` block in the manifest that counts `isactive_dropped` /
`name_mismatch` over the **eligible universe** (so the hazard is visible even for
names that then fall out on the SPY beta gate). Spikes in these counters are
alertable.

Demonstrated end-to-end on a broadened test partition carrying both rows for U
and SNOW: **U → IGV** and **SNOW → IGV** (Software-Application), not
Airlines/Leisure. UNP — previously dropped to SPY because it was off the curated
list — now classifies (Railroads → IYT). A synthetic `isActive=False`-only AAPL
row is correctly counted as `isactive_dropped` (→ SPY), not silently mapped.

**Blast radius (research scan, full Alpaca tradable us_equity, N=13,123):**
**2.39% (313)** tickers are naive-wrong collisions (the bare-ticker pick is a
delisted predecessor), of which **100 (31.9%)** would be *confidently wrong*
pure-play hedges pre-guard. The `isActive=True` filter **recovers 100% of the
313** — the live twin is always present in the meta — so the net effect is a
*better* outcome (correct ETF) for every one. The only irreducible residual is
**0.82% (108)** all-inactive tickers (the live issuer is absent from Tiingo
entirely) → SPY + warn. Within our own 135-name MF study set, **6** were
collisions (3 confidently wrong: U→JETS, PATH→XBI, CART→KRE) — all caught by the
guard. Full analysis: `docs/knowledge/tiingo_ticker_reuse.md`.

---

## Quality gate / fallback chain

After the crosswalk assigns the rank1 (industry) ETF, we compute its beta/R².
A tier **passes** iff:

- `n_obs >= MIN_N_OBS (60)`,
- `beta` finite and **non-negative**,
- `r2` finite and `>= R2_FLOOR (0.05)`.

If rank1 fails, we **demote** down the chain industry → sector → SPY and record
the actual basis in `selection_basis` (`heuristic_industry` /
`heuristic_sector` / `spy_fallback`). `industry_source` (`pure_play` /
`sector_fallback`) reflects what the crosswalk *intended*, independent of the
gate outcome. **Non-finite betas are never emitted.** If even SPY fails the gate
(e.g. too little history), the stock gets **no rows** and falls through to the
apex runtime's lower tiers — same as v1.

This gate is what protects against Tiingo mis-classifications (e.g. Tiingo labels
Unity `U` as "Airlines" → JETS; a bad JETS beta/R² demotes it).

---

## Output schema (frozen v1 columns + v2 additions)

Path unchanged: `hedge_map/effective_date=YYYY-MM-DD/data.parquet`, 3 rows/ticker.

v1 columns retained: `effective_date, as_of_date, ticker, rank, hedge_etf, beta,
r2, n_obs, etf_shortable, etf_easy_to_borrow, etf_adv_usd_30d,
stock_adv_usd_30d, selection_basis`.

**Added in v2:**

| column | values | meaning |
|---|---|---|
| `industry_source` | `pure_play` / `sector_fallback` | did the crosswalk find a pure-play for the industry |
| `classification_sector` | str / null | sector string used |
| `classification_industry` | str / null | industry string used |
| `classification_source` | `tiingo` / `sic` / `no_meta` / `isactive_dropped` / `name_mismatch` | classification provenance; the last three are the distinct ticker-reuse unclassified buckets |

`selection_basis` values change to `heuristic_industry` / `heuristic_sector` /
`spy_fallback` (was `liquid_top_r2` in v1).

Manifest adds: `crosswalk_version`, `classification_snapshot_date`, and the
`crosswalk_coverage` block, which now reports the classification buckets
separately (`tiingo` / `sic` / `no_meta` / `isactive_dropped` / `name_mismatch`)
plus a `guard_drops` sub-block counting `isactive_dropped` / `name_mismatch` over
the eligible universe (the nightly ticker-reuse blast radius; alertable).

---

## Crosswalk rationale (curate-top-fallback-rest)

- **Sector map (11):** every observed Tiingo sector string *and* the GICS names
  the research oracle uses, so one map serves both production (raw Tiingo) and
  the parity test (GICS). 100% match on the oracle's 135 GICS sectors.
- **Industry map (~40):** curated for the highest-frequency Tiingo industries
  that have a clean pure-play (semis→SMH, equipment→SOXX, software→IGV,
  biotech→XBI, pharma→IBB, devices→IHI, regional banks→KRE, A&D→ITA,
  airlines→JETS, rails/trucking→IYT, E&P→XOP, oil services→OIH, internet→FDN,
  retail→XRT, homebuilders→XHB, solar→TAN). Niche/ambiguous industries are left
  to the sector fallback rather than forced onto a loosely-related ETF. Every
  industry ETF is in the research `etf_candidates.csv` set.

---

## Research parity (acceptance test #2)

For the 135 oracle tickers in `ticker_classification.csv`:

- **Sector parity (offline):** `crosswalk.sector_etf(gics_sector)` ==
  `oracle.sector_etf` → **135/135 (100%)**.
- **Industry parity on raw Tiingo (30-name overlap with the current meta
  partition):** `crosswalk.classify(tiingo.sector, tiingo.industry).rank1_etf`
  == `oracle.industry_etf` → **26/30 (86.7%)**, with the 4 divergences all
  **documented** (gate = zero undocumented):

| ticker | Tiingo sector / industry | oracle | mine | reason |
|---|---|---|---|---|
| ABNB | Industrials / Travel Services | XLY | XLI | Tiingo sector wrong (oracle GICS = Consumer Discretionary); both sector_fallback |
| DASH | Communication Services / Internet Content & Information | ONLN | FDN | Tiingo sector wrong; FDN is the defensible map from the Tiingo input |
| GEHC | Healthcare / Health Information Services | IHI | XLV | Tiingo industry too generic to know it's imaging devices |
| SNPS | Technology / Software - Infrastructure | SOXX | IGV | EDA-vs-software is domain knowledge Tiingo's string doesn't encode |

These are **intentional**: the production crosswalk maps raw Tiingo faithfully;
the oracle injected company-specific GICS overrides. The beta quality gate is the
backstop for the cases where the Tiingo input is simply wrong.

---

## Smoke run report (2026-06-26)

```
python run_local.py --as-of 2026-06-24 \
  --subset AMD,MSFT,VRTX,EQT,TSLA,PYPL,NFLX,AAPL,AMZN,LRCX,XEL,COST,UNP,IDXX \
  --s3-prefix hedge_map_test
```

Writes to the **test** prefix `s3://mh-guess-data/hedge_map_test/...` (never the
prod `hedge_map` path). The parquet reads back with the full 17-column v2 schema.

- effective_date 2026-06-25, as_of 2026-06-24
- eligible 14/14, covered 13, rows 39
- industry_source: pure_play 9 / sector_fallback 4 (69.2% pure-play)
- selection_basis: heuristic_industry 8 / heuristic_sector 5 / spy_fallback 0
- classification_source: tiingo 13 / sic 0 / none 0
- ETF asset fetches: 30 crosswalk ETFs (was 57 in v1 — efficiency win)

Spot-checks: AMD→SMH (β1.36, R²0.68), LRCX→SOXX (β1.06, R²0.82),
MSFT→IGV (β0.74, R²0.69), EQT→XOP (β0.43), VRTX→XBI, IDXX→IHI, NFLX→FDN.
Sector-fallback names (no curated industry pure-play) correctly collapse rank1
to the sector ETF: PYPL/AAPL→XLK or XLF, TSLA→XLY, XEL→XLU — for these rank1 and
rank2 are the same sector ETF (apex reads rank1; rank2/3 are diagnostics).

### Worked example of the meta-coverage limit: UNP

UNP (Union Pacific) was the one eligible name **dropped** (13 of 14). It is NOT
in the curated meta partition → `classification_source="none"` → all three tiers
collapse to SPY. UNP's trailing-60d SPY beta is ~0.004 with R²≈0.00, which fails
`R2_FLOOR` → no usable hedge → the stock is omitted (it falls through to the apex
runtime's lower tiers, same as v1). Yet UNP is a railroad and the crosswalk maps
Railroads→IYT, against which UNP fits well (β0.60, R²0.26). So the moment the
meta universe is broadened to include UNP, it would get a real IYT hedge with no
crosswalk change. This is the clearest illustration of why the coverage fix is
broadening the meta list — and why we drop rather than emit a meaningless SPY
hedge with R²≈0.

---

## Design ambiguities resolved

1. **Meta covers ~104, not the universe.** Resolved: classify what we can,
   SPY-fallback the rest, report coverage in the manifest, and document that the
   fix is broadening the meta list (probe-confirmed). No crosswalk change needed.
2. **Oracle stores GICS + human-annotated industry, not raw Tiingo.** Resolved:
   parity is two layers — sector on GICS (135/135), industry on raw Tiingo for
   the overlap with documented divergences.
3. **Tiingo taxonomy errors (U→Airlines, SNOW→Leisure).** Resolved: the rank1
   beta/R² quality gate demotes a bad fit; we don't trust the label blindly.
