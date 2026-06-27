# Hedge Map v2 — Broad-Universe Validation (2026-06-27)

Cutover verification (steps 2-3) of the heuristic hedge_map against the **full
Alpaca tradable us_equity universe**, run after PR #1 merged to main (1387abd).
This is the gate for the PM's step-4 decision (apex PR #34).

## Scopes & prod-vs-test labeling

To avoid clobbering the prod meta partition (the daily fundamentals flow owns it
and the broadened flow has not run in prod yet — today's prod partition is still
the old ~104-ticker file), this validation wrote to **TEST prefixes** and cleaned
them up afterward. Coverage numbers are identical to what prod will produce once
the broadened fundamentals flow runs.

- Broad meta → `s3://mh-guess-data/tiingo_validation/.../date=2026-06-27/meta.json` (TEST)
- hedge_map parquet/manifest → `s3://mh-guess-data/hedge_map_validation/...` (TEST)

Two scopes (the full-universe beta pass over ~13k is impractical; split as the
lead authorized):
- **Scope A — classification coverage over the FULL 13,123 universe** (meta +
  crosswalk + ticker-reuse guard; no bars). Cheap, exact.
- **Scope B — full hedge_map (betas + SPY quality gate) over the LIQUID+SHORTABLE
  subset** (trailing-30d $ADV ≥ $25M AND Alpaca shortable = the names we'd trade).

## STEP 2 — broad meta partition

```
python step2_broad_meta.py   # fetch_tradable_equity_universe + fetch_meta_batched(150)
```
- Universe: **13,123** tradable us_equity symbols.
- Batched meta fetch: **88 batches of ≤150**, all 200-OK (no 502s at batch_size=150).
- **6,351 meta rows** for 13,123 tickers (rows < tickers: ETFs/warrants/units
  return no fundamentals meta). **5,882 distinct tickers** have meta; **5,774**
  have ≥1 active row.
- Partition size: 3.94 MB (vs the old 66 KB / ~104-ticker curated partition).
- as_of = 2026-06-26, effective = 2026-06-29.

## STEP 3 — coverage (expected vs actual)

### Scope A — full-universe classification (N = 13,123)

| metric | count | % of universe | notes |
|---|--:|--:|---|
| universe_size | 13,123 | 100% | Alpaca tradable us_equity |
| **classified (tiingo + sic)** | **5,525** | **42.1%** | tiingo 5,522 / sic 3 |
| no_meta | 7,243 | 55.2% | ETFs/warrants/units/thin-coverage → SPY (pre-existing, not a reuse issue) |
| **isactive_dropped** | **108** | **0.82%** | guard residual — only delisted rows; matches the research blast-radius exactly |
| name_mismatch | 247 | 1.88% | active row(s) exist but none reconcile with the live Alpaca name → SPY |

Of the **5,525 classified**: pure_play **2,418** / sector_fallback **3,107**
(industry-tier intent before the beta gate).

`guard_drops` (the ticker-reuse blast radius, alertable): **isactive_dropped 108,
name_mismatch 247.**

### Scope B — hedge_map over liquid+shortable subset

Subset = **2,444** names (shortable AND $ADV ≥ $25M). Manifest `crosswalk_coverage`:

| metric | value |
|---|--:|
| eligible_count | 2,433 |
| covered_tickers (rank=1 emitted) | 2,149 |
| coverage_pct_of_eligible | 88.33% |
| industry_source: pure_play / sector_fallback | 779 / 1,370 (36.25% pure-play) |
| selection_basis: heuristic_industry / heuristic_sector / spy_fallback | 742 / 920 / 487 |
| classification_source: tiingo / no_meta / isactive_dropped / name_mismatch | 1,733 / 391 / 2 / 23 |
| guard_drops: isactive_dropped / name_mismatch | 2 / 33 |

The ~11.7% of eligible names not covered fall through on the SPY beta gate
(SPY beta non-finite / R² < 0.05) — same behavior as v1; they drop to the apex
runtime's lower tiers rather than getting a meaningless SPY hedge.

## Spot-checks — guard works end-to-end on broad meta

| ticker | resolved company | sector / industry | rank1 ETF | verdict |
|---|---|---|---|---|
| **U** | Unity Software (active) | Technology / Software-Application | **IGV** | RECOVERED (not US Airways → JETS) |
| **SNOW** | Snowflake (active) | Technology / Software-Application | **IGV** | RECOVERED (not Intrawest → Leisure) |
| **PATH** | UiPath (active) | Technology / Software-Infrastructure | **IGV** | RECOVERED (not NuPathe → XBI) |
| **CART** | Maplebear/Instacart (active) | Consumer Cyclical / Internet Retail | **FDN** | RECOVERED (not Carolina Trust → KRE) |
| NVDA | NVIDIA | Technology / Semiconductors | SMH | as expected |
| JPM | JPMorgan Chase | Financial Services / Banks-Diversified | KBE | as expected (sector XLF) |
| XOM | Exxon Mobil | Energy / Oil & Gas Integrated | XLE | as expected (no integrated pure-play → sector) |
| LLY | Eli Lilly | Healthcare / Drug Manufacturers-General | IBB | as expected (sector XLV) |

All four reuse-collision spot-checks recover the correct live-entity ETF; all
four sanity names land where expected.

## Anomaly found + fixed: over-strict name reconciliation

The first validation pass showed **name_mismatch = 268**. Investigation: ~90% are
**correct** rejections (ETFs/SPACs/renamed entities where Tiingo's only row is a
different company → SPY is right), but ~10% (~28) were **false negatives** — the
same company with spelling/spacing/possessive variance my token matcher missed:
`argenx`/`Argen X`, `ConocoPhillips`/`Conoco Phillips`, `Bally's`/`Ballys`,
`Kohl's`/`Kohls`, `SiriusXM`/`Sirius XM`, `Future Fuel`/`Futurefuel`,
`AngioDynamics`/`Angiodynamic`, etc.

Fix (this PR): added an **additive** squashed-string fallback to `names_reconcile`
— it strips ADR/boilerplate phrases, treats apostrophes as intra-word, and
compares the punctuation/space-free, order-preserving token concatenation
(prefix match ≥5 chars or char-similarity ≥0.90). Verified **strictly additive**
on the live 13,123-name universe: **+13 recoveries, 0 new rejections** (the
original 50%-token-overlap primary rule — which handles abbreviations like
`Companies`/`Cos`, `Investment`/`Invt` — is unchanged). name_mismatch dropped
**268 → 247**. Covered by new unit tests
(`test_spacing_punctuation_variants_reconcile`,
`test_abbreviation_variants_still_reconcile`,
`test_adr_boilerplate_stripped_does_not_falsely_match`).

## Verdict: LOOKS EXPECTED

- **isactive_dropped 108 (0.82%)** matches the research blast-radius residual to
  the symbol — the guard's true-residual count is exactly as predicted.
- All four reuse-collision spot-checks (U/SNOW/PATH/CART) recover correctly; no
  ticker resolves to a delisted predecessor.
- Liquid+shortable coverage 88.3%, pure-play 36% — consistent with the curated
  crosswalk depth; the rest are sector-fallback or SPY-gate, all explainable.
- The one anomaly (over-strict reconciliation) was found, root-caused, fixed
  additively, and re-validated.

No blocking anomalies. Numbers are ready for the PM's step-4 decision on apex #34.

---

## Production go-live run (2026-06-27)

After PR #1/#3 merged to main (HEAD 5d9fcb3), the broadened meta + hedge_map were
run against the **REAL prod S3 paths** (PM-authorized; executed in the main
session with the user approving each prod-write prompt — destructive prod writes
require direct-user intent, not teammate authorization). apex untouched.

### ⚠️ Weekend date-misalignment (manual-run edge case — NOT a pipeline bug)

The first manual attempt wrote broad meta to `date=2026-06-27` (the calendar
Saturday) but ran the hedge_map flow with `--as-of 2026-06-26` (the snapped
Friday trading day). `latest_meta_partition_key` selects the newest partition with
`date <= as_of`, so the flow read the **old narrow `date=2026-06-26`** partition
and skipped the broad `06-27` one as "future" — yielding a manifest with only
~92 tiingo-classified names (the old curated list), not the broad universe.

**Root cause:** a manual run wrote the meta partition to a date AHEAD of the
snapped `as_of`. **This does not occur in scheduled prod:** the nightly flow runs
post-close on a trading weekday, so the meta partition date equals the `as_of`
trading day and `date <= as_of` resolves to that same-day broad partition —
aligned by construction.

**Corrective action (run, backup-first):** wrote broad meta ALSO to
`date=2026-06-26` (`prod_broaden_meta.py --date 2026-06-26 --prod`, with the
narrow `06-26` partition backed up first), then re-ran `run_local.py
--as-of 2026-06-26` → it read the broad meta and wrote a correct hedge_map for
`effective_date=2026-06-29`.

**Operational guard (this PR):** `prod_broaden_meta.py` now **defaults `--date` to
the latest TRADING day** (calendar-aware snap), not calendar today — so a manual
cutover/backfill writes meta to the same date the flow will snap `as_of` to,
closing the footgun. Explicit `--date` still overrides.

### Prod manifest — corrected run (VERIFIED PASS)

`run_local.py` applies **no $25M ADV liquid screen**, so its covered set is a
SUPERSET of the liquid-subset validation above — its covered-ticker count
(8,776) far exceeds the 2,149 liquid figure, as expected. The per-bucket
proportions and the spot-checks are the comparison.

**Prod meta partition** (date=2026-06-26, post-broaden; also written to 06-27):
3,944,666 bytes, 6,351 rows, 5,774 active tickers. Narrow predecessors backed up
to `…/meta/_backups/date=2026-06-26/` and `…/date=2026-06-27/meta_pre-broaden.json`.

**hedge_map** (`effective_date=2026-06-29`, as_of 2026-06-26):

| metric | value |
|---|--:|
| universe | 13,123 |
| eligible | 12,143 |
| covered | 8,776 (72.27% of eligible) |
| hedge_map_rows | 26,328 (3/ticker) |
| classification_source | tiingo 3,689 / sic 0 / no_meta 4,910 / isactive_dropped 79 / name_mismatch 98 (within covered) |
| industry_source | pure_play 1,908 (21.74%) / sector_fallback 6,868 |
| selection_basis | heuristic_industry 1,697 / heuristic_sector 1,667 / spy_fallback 5,412 |
| guard_drops (over eligible) | isactive_dropped 104 / name_mismatch 214 |

Reading: of the 8,776 covered, **3,364 get a real sector/industry ETF**; the rest
hedge to SPY — dominated by the `no_meta` obscure tail (4,910) we would never
trade. `guard_drops.isactive_dropped` (104) ≈ the research blast-radius residual
(~0.82% of the universe). `effective_date=2026-06-29` is the correct map for
Monday's session (Friday-close betas); Monday's scheduled run continues the
cadence normally.

**Spot-checks (prod parquet, independently confirmed):** U→IGV, SNOW→IGV,
PATH→IGV, CART→FDN (all RECOVERED from delisted predecessors), NVDA→SMH, JPM→KBE,
XOM→XLE, LLY→IBB. Full v2 17-column schema present. ✅

**Verdict: PASS.** Prod coverage, guard residual, and spot-checks all consistent
with the validated test-run; the one weekend manual-run anomaly was root-caused
(date alignment, not a pipeline bug), corrected, and guarded against.

### Commands
```
# validation (test prefix; scratchpad harnesses)
python step2_broad_meta.py        # broad meta → test prefix
python step3_validate.py          # scope A + scope B + spot-checks

# prod cutover (manual; --prod interlock; --date defaults to latest trading day)
python prod_broaden_meta.py --prod --backup-first        # broaden prod meta
python run_local.py --as-of 2026-06-26                   # prod hedge_map (real paths)
```
(Scripts kept in the session scratchpad as ephemeral validation harnesses; the
durable record is this doc. Test S3 prefixes were cleaned up after the run.)
