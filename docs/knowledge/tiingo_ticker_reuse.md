# Tiingo bare-ticker reuse / `isActive` resolution hazard

**Status:** measured 2026-06-26, framing aligned to the shipped guard
(`select_active_meta_row` in `hedge_classification.py`). Read-only blast-radius
study for the data-flow `hedge_map` ETF-classification pipeline.

**Provenance (external):** measured by `scan_ticker_reuse.py` in the *other* repo
— `algo-trade` at `research/motley_fool/etf_hedge/scan_ticker_reuse.py`. Its
intermediate CSVs (`alpaca_universe.csv`, `tiingo_meta_raw.csv`,
`ticker_buckets.csv`, `collisions_naive_wrong.csv`, `all_inactive.csv`,
`example_collisions.csv`) were ephemeral scratchpad outputs and are **not
committed**; the tallies below are the durable record.

## TL;DR
Querying Tiingo `fundamentals/meta` by **bare ticker** returns **one row per legal entity that ever
used that ticker** — for a reused ticker that means BOTH the live issuer (`isActive=True`) AND one or
more delisted predecessors (`isActive=False`). A consumer that takes a single row without filtering
(naive "first row" = oldest `permaTicker` sorts first, and that is the dead one) classifies the stock
into the **predecessor's** industry and shorts an unrelated ETF.

The shipped guard `select_active_meta_row` drops `isActive != True`, then picks the active row whose
`name` reconciles with the live Alpaca company. **Net effect: it RECOVERS the correct classification
whenever an active row exists** (U → Unity → IGV, SNOW → Snowflake → IGV), not merely SPY.

Across the full Alpaca tradable US-equity universe (N=13,123):
- **PRE-guard:** 313 tickers (2.39%) would resolve to the wrong (dead) entity; 100 of those (31.9%)
  would produce a *confidently wrong* pure-play hedge, 213 a harmless SPY fallback.
- **POST-guard:** all 313 carry a usable active-twin industry, so the guard **recovers 100% of them**.
- **TRUE residual risk = 108 tickers (0.82%)** that are *delisted-only* (no active row at all) →
  these are the genuine SPY-fallback cases the guard cannot classify.

## The hazard (pre-guard)
`hedge_map` classifies a stock to a sector/industry ETF from Tiingo `sector`/`industry`. If the meta
lookup resolves to a dead predecessor, the stock is shorted against the predecessor's ETF. Direct hits
inside our own 135-name MF study set — 6 of 135 are naive-wrong collisions, 3 confidently wrong:

| ticker | live company | dead predecessor (naive pick) | dead industry | pre-guard ETF | post-guard |
|---|---|---|---|---|---|
| **U** | Unity Software | US AIRWAYS GROUP INC | Airlines | **JETS** | recovered → IGV |
| **PATH** | UiPath | NuPathe Inc | Biotechnology | **XBI** | recovered → IGV |
| **CART** | Maplebear/Instacart | Carolina Trust Bank | Banks - Regional | **KRE** | recovered → XLY/IBUY |
| **SNOW** | Snowflake | Intrawest Resorts Holdings | Leisure | SPY | recovered → IGV |
| **DOCS** | Doximity | PC DOCS GROUP INTERNATIONAL | Foreign IT Services | SPY | recovered |
| **P** | (ticker now Everpure) | Pandora Media Inc | Broadcasting | SPY | recovered |

## Root cause
1. **Ticker reuse.** US tickers are recycled after delisting. Tiingo keys each legal entity by a
   stable `permaTicker` (`US000000078323` = US Airways, `US000000087700` = Unity) but both share the
   display `ticker` `U`.
2. **Bulk /meta returns multiple rows.** `GET fundamentals/meta?tickers=U` returns the Unity
   (`isActive=True`) AND US Airways (`isActive=False`) rows. Taking "the first" / sorting by
   `permaTicker` (older = smaller, sorts first) lands on the **dead** predecessor.
3. **No active guard (the bug).** Nothing checked `isActive`, so the collision was silent.

## Remediation (shipped: `select_active_meta_row`)
1. **Filter `isActive == True`** — recovers every case where a live row exists.
2. **Reconcile `name` against the live Alpaca company** to pick among multiple active rows
   (share classes / dual listings / restructures — e.g. `API` returns two same-named "Agora Inc" rows).
3. **(Future)** persist the resolved `permaTicker` and re-query by it, so a future recycle can't
   silently re-point the mapping (durable, not point-in-time). Not yet implemented.
4. **No active row → SPY fallback + warn.** Never classify from a dead row. This is the residual set,
   counted in the manifest as `isactive_dropped` / `name_mismatch`.

## Blast-radius tally (Alpaca active tradable us_equity, N=13,123)
| bucket | count | % | post-guard outcome |
|---|--:|--:|---|
| no Tiingo meta at all | 7,241 | 55.18% | SPY fallback (not a reuse issue; pre-existing) |
| clean single active row | 5,326 | 40.59% | correct |
| multi-row, naive pick already active | 135 | 1.03% | correct (guard confirms active row) |
| **collision: naive pick is DEAD, active twin exists** | **313** | **2.39%** | **RECOVERED — correct classification** |
| **all-inactive: only dead row(s), no live entity** | **108** | **0.82%** | **SPY fallback (TRUE RESIDUAL)** |
| (memo) tickers with ≥1 `isActive=False` row | 497 | 3.79% | — |

### Pre-guard wrong-hedge-vs-SPY (state BEFORE the guard, of the 313 collisions)
- **Confident WRONG hedge** (dead industry → pure-play ETF): **100 (31.9%)**
- Harmless SPY fallback (dead industry, no pure-play): **213 (68.1%)**
- Bogus-ETF mix of the 100: XBI 21, IGV 19, KRE 19, XOP 10, IHI 8, OIH 6, SMH 5, FDN 3, ITA 2,
  JETS 2, XRT 2, IBUY 1, IBB 1, IYT 1. (Dead biotechs, regional banks, and software cos dominate.)

### Post-guard recoverability split (the numbers the team cares about)
| | count | % of universe |
|---|--:|--:|
| **(a) recoverable** — active row for the live company also exists; guard fixes | **313** | **2.39%** |
| **(b) unrecoverable** — only dead row(s) exist → SPY fallback (**true residual risk**) | **108** | **0.82%** |

All 313 recoverable collisions have a **non-null active-twin industry**, so recovery yields a real
sector/industry ETF (not a degenerate SPY). The 108 unrecoverable are the entire post-guard residual.

**Short-ticker concentration** (of the 421 risky = 313 collision + 108 all-inactive): len-1: 4
(B, P, S, U — all collision/dead), len-2: 23, len-3: 146, len-4: 237, len-5: 11. Reused tickers skew
short, as expected (short symbols are prime real estate that gets recycled).

**Dead-entity sector mix** (collision set): Technology 55, Industrials 50, Financial Services 46,
Healthcare 41, Consumer Cyclical 27, Energy 22, Basic Materials 16, Real Estate 15, Utilities 10,
Consumer Defensive 10, Communication Services 9.

## Recovery — confirmed
**100% of the 313 collisions recover** via `isActive=True` + name reconciliation: every one has a live
twin under a different `permaTicker`, each with a usable industry. Verified on canonical cases:
`U` → {Unity active, US Airways inactive}; `SNOW` → {Snowflake active, Intrawest inactive}.
No external data source is needed for recovery.

## Example collisions (ticker → dead entity → pre-guard bogus ETF | live entity → post-guard)
| ticker | dead predecessor (naive pick) | dead industry | pre-guard ETF | live entity |
|---|---|---|---|---|
| U | US AIRWAYS GROUP INC | Airlines | JETS | Unity Software Inc |
| SNOW | Intrawest Resorts Holdings Inc | Leisure | SPY | Snowflake Inc - Class A |
| PATH | NuPathe Inc | Biotechnology | XBI | UiPath Inc - Class A |
| CART | Carolina Trust Bank | Banks - Regional | KRE | Maplebear Inc (Instacart) |
| ARRY | Array Biopharma Inc | Biotechnology | XBI | Array Technologies Inc (solar→TAN) |
| BHVN | Biohaven Pharmaceutical Holding | Biotechnology | XBI | Biohaven Ltd (still biotech*) |
| BOLT | Bolt Technology Corp | Oil & Gas Equip & Svcs | OIH | Bolt Biotherapeutics Inc |
| ABCL | ALLIANCE BANCORP | Banks - Regional | KRE | AbCellera Biologics Inc |
| ALAB | ALABAMA NATIONAL BANCORPORATION | Banks - Regional | KRE | Astera Labs Inc (semis) |
| APC | Anadarko Petroleum Corp | Oil & Gas E&P | XOP | ARKO Petroleum Corp |
| ATHR | ATHEROS COMMUNICATIONS INC | Semiconductors | SMH | Aether Holdings Inc |
| BFLY | Bluefly Inc | Internet Retail | IBUY | Butterfly Network Inc |
| BLZE | BLAZE SOFTWARE INC | Software - Application | IGV | Backblaze Inc - Class A |
| ANVS | Anv Sec Group Inc | Biotechnology | XBI | Annovis Bio Inc |
| API | Agora Inc | Semiconductors | SMH | Agora Inc (same name*) |

\* `BHVN` predecessor and live entity are both biotech, so the wrong pick is coincidentally
near-correct. `API` returns two same-named "Agora Inc" rows (restructure, not true reuse) — the
`name`-reconciliation tiebreak in remediation step 2 handles this. `ARRY` is the cleanest cautionary
tale: dead Array Biopharma (XBI) vs live Array Technologies (solar/TAN) — opposite sectors.

## Residual risk after the guard
1. **(b) all-inactive — 108 tickers (0.82%) — the real residual.** Live issuer absent from Tiingo
   fundamentals (newly listed, foreign, thin coverage). Guard yields no row → SPY + warn. Irreducible
   from Tiingo alone; would need a secondary classification source to close.
2. **Multiple active rows** (share classes / dual listings / restructures like `API`). `isActive`
   alone doesn't disambiguate; relies on the name-reconciliation tiebreak (and a future persisted
   `permaTicker`).
3. **Future recycles.** A clean ticker today can collide tomorrow when a live name delists and the
   symbol is reassigned; persisting `permaTicker` is what would make the fix durable.
4. **`no_meta` (55%).** Not a reuse hazard, but a reminder that bare-ticker coverage is partial and the
   pipeline already needs an SPY fallback for these.

## Manifest observability
The `hedge_map` manifest surfaces this residual as distinct nightly counters so a spike is alertable:
`classification_source ∈ {tiingo, sic, no_meta, isactive_dropped, name_mismatch}` plus a `guard_drops`
block counting `isactive_dropped` / `name_mismatch` over the eligible universe. These counters reflect
the TRUE residual (only-inactive or no-reconciling-active rows); **recovered** collisions classify
normally and are NOT counted.

## Security note: Tiingo token in error URLs
Tiingo error responses echo the full request URL **including the `token` query param**; a raw
`raise_for_status()` leaks the token into logs/tracebacks when query-param auth is used. The data-flow
Tiingo clients (incl. `tiingo_meta_universe.py`) authenticate via the **Authorization header**, so the
token is never in the URL; the new meta client additionally `_redact()`s any token before logging on
error as defense-in-depth. The other `tiingo_*_flow.py` clients are header-auth and do not put the
token in the URL.

---
See `project_docs/hedge_overlay/HEURISTIC_V2.md` for how the guard fits the hedge_map v2 pipeline.
