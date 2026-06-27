# P1 Backlog — ETF Crosswalk & Ticker-Universe Reconciliation

**Status:** Backlog (P1) — not yet scoped/designed. Logged 2026-06-27.
**Owner:** Data Engineering (TBD)
**Related:** data-flow `hedge_map` v2 (PR #1 `feat/hedge-overlay`→main, PR #2 merged), `docs/knowledge/tiingo_ticker_reuse.md`, apex PR #34 (consumer).

> This is a problem statement and scope sketch, NOT a design. The goal is to capture the
> long-term maintenance gap so it isn't forgotten, and to justify the P1 priority.

---

## 1. Why this matters (the long-term risk)

The MF Drift Hedge Overlay's `hedge_map` pipeline depends on two inputs that are **manually
maintained or fetched live-per-run, with no systematic reconciliation against reality**:

1. the **ticker universe** (which stocks we classify + hedge), and
2. the **ETF crosswalk dictionary** (which sector/industry → which hedge ETF).

Left as-is, both decay silently over time. New stocks and newly-launched ETFs go unhedged or
sub-optimally hedged; retired ETFs leave stale mappings; the curated fundamentals universe drifts
from the live market. None of this fails loudly — it degrades coverage and hedge quality quietly,
which is exactly why it deserves a P1 (important, not urgent) slot rather than being left implicit.

---

## 2. Current state (what exists today)

**Ticker universe**
- `hedge_map_flow.py::fetch_universe()` calls Alpaca `get_all_assets` (active + tradable us_equity)
  **live on every nightly run** (`cron 30 18 * * 1-5`). Self-updating, but in-process only — not
  persisted as a shared/versioned universe dataset.
- `adhoc/tickers.txt` (S3) is a **static, hand-maintained** curated list. Every Tiingo flow
  (`tiingo_to_s3`, `tiingo_fundamentals`, backfills) only *reads* it via `fetch_tickers_from_s3()`.
  **Nothing writes/refreshes it** — new listings/delistings never reach it without a manual edit.
  (It drives the expensive per-ticker fundamentals daily/statements pulls, deliberately not broadened.)
- v2 broadened the Tiingo **meta** classification universe to the live Alpaca tradable-equity set
  (`tiingo_meta_universe.py::fetch_tradable_equity_universe`), so *classification* coverage now
  self-updates — but it is still live-fetch-per-run, not a persisted/versioned snapshot.

**ETF crosswalk**
- `hedge_crosswalk.py` holds `SECTOR_TO_ETF` and `INDUSTRY_TO_ETF` as **static Python dicts** mapping
  classification strings → a closed set of ~30–40 ETF tickers (`all_referenced_etfs()`).
- **New ETFs:** not captured. A newly launched ETF is invisible to selection until someone manually
  adds a mapping entry (and ensures daily-bar coverage for its beta).
- **Retired ETFs:** degrade *safely* at runtime — `hedge_map_flow.py` applies a liquid + shortable
  screen on the referenced ETF and the beta/R² quality gate demotes rank1→sector→SPY — **but the
  stale mapping entry persists** in the dict; pruning is manual.
- There is **no flow that reconciles the crosswalk against the live ETF universe.**

---

## 3. The gap / problem statement

1. **No persisted, versioned ticker-universe artifact.** Each flow re-derives or hand-maintains its
   own universe; there is no single authoritative, periodically-snapshotted, diffable universe
   dataset on S3. Hard to audit "what changed in the tradable universe last week."
2. **No new-ETF discovery.** The crosswalk cannot capture ETFs released after it was authored.
3. **No retired-ETF pruning / staleness detection.** Delisted or no-longer-shortable ETFs remain in
   the crosswalk (they fail safe at runtime, but the config rots and hides intent).
4. **No reconciliation loop** that surfaces (a) crosswalk entries that no longer resolve and
   (b) liquid/high-AUM ETFs in a covered sector/industry that are *not* in the crosswalk.

---

## 4. Scope sketch (net-new work — to be designed later)

A periodic **universe + crosswalk reconciliation** capability, roughly:

- **Versioned universe snapshot flow.** List tradable equities **and ETFs** from Alpaca on a
  schedule; persist a versioned snapshot to S3 (hive-partitioned by date); diff vs the prior
  snapshot; emit adds/drops (new listings, delistings, shortability changes) as an observable
  artifact + alertable counters.
- **ETF crosswalk reconciliation.**
  - *Retirement / staleness:* flag every crosswalk ETF that is no longer tradable/shortable in the
    latest snapshot → propose pruning.
  - *Coverage gaps:* surface liquid / high-AUM ETFs (by sector/industry) **not** in the crosswalk →
    propose for human review/addition.
- **Governance — propose, don't auto-apply.** The crosswalk is curated IP (the whole point of the
  heuristic vs noisy max-R²); the flow should make drift *observable* and *propose* changes
  (e.g. a report or a draft PR), with a human approving crosswalk edits — never silently rewrite it.
- **Optional:** persist resolved Tiingo `permaTicker` per symbol (already noted as future work in
  `tiingo_ticker_reuse.md`) so the universe artifact is durable against ticker recycles.

---

## 5. Acceptance criteria (sketch)

- A scheduled flow produces a versioned universe snapshot (equities + ETFs) on S3 with a
  prior-vs-current diff.
- A reconciliation report lists: crosswalk ETFs that no longer resolve, and uncovered liquid ETFs by
  sector/industry, both ready for human review.
- Crosswalk changes remain human-approved (config PR), not auto-applied.
- Observability: nightly counters for universe adds/drops and crosswalk drift, alertable on spikes
  (consistent with the existing `guard_drops` / `crosswalk_coverage` manifest pattern).

---

## 6. Out of scope (for now)

- The actual design/implementation (this is a backlog placeholder).
- Auto-editing the crosswalk.
- Changing the runtime fail-safe behavior (the shortable/liquid screen + beta gate stays as the
  in-run safety net regardless).

---

## 7. Dependencies & sequencing

- Builds on the merged `hedge_map` v2 (the crosswalk + manifest-coverage plumbing it introduced).
- Lower urgency than the production cutover (overlay→main merge, broadened `tiingo_fundamentals`
  run, hedge_map scheduling, apex PR #34) because the runtime fail-safes prevent acute breakage —
  hence **P1 (important, plan it) rather than P0**.
