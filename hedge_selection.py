"""
Heuristic hedge-map selection — v2 core.

Given the eligible stock universe, their daily bars, a Tiingo classification
table, and ETF bars, this module produces the row-per-rank hedge_map DataFrame
using the deterministic crosswalk in `hedge_crosswalk.py` (NOT max-R²).

For each eligible stock:
  rank1 = industry pure-play ETF   (crosswalk; demoted by the quality gate)
  rank2 = sector SPDR ETF
  rank3 = SPY

Beta still SIZES each short via `beta_r2_pair()` vs the *assigned* ETF (one
regression per assigned ETF, not 57). The quality gate may demote rank1 when
its beta is non-finite / negative or R² is below R2_FLOOR.

Output schema (row-per-rank) extends v1 with:
  selection_basis        heuristic_industry | heuristic_sector | spy_fallback
  industry_source        pure_play | sector_fallback
  classification_sector  the sector string used
  classification_industry the industry string used
  classification_source  tiingo | sic | none
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

from hedge_crosswalk import (
    SPY,
    all_referenced_etfs,
    classify,
)
from hedge_map_flow import (
    MIN_N_OBS,
    _compute_returns,
    _next_trading_day,
    beta_r2_pair,
    compute_adv,
)

# Quality-gate floor: demote a tier whose R² is below this (start at 0.05).
R2_FLOOR = 0.05

# Output column order (frozen contract with the apex resolver + v2 additions).
HEDGE_MAP_COLUMNS = [
    "effective_date",
    "as_of_date",
    "ticker",
    "rank",
    "hedge_etf",
    "beta",
    "r2",
    "n_obs",
    "etf_shortable",
    "etf_easy_to_borrow",
    "etf_adv_usd_30d",
    "stock_adv_usd_30d",
    "selection_basis",
    "industry_source",
    "classification_sector",
    "classification_industry",
    "classification_source",
]

# Per-rank selection_basis labels.
_BASIS_BY_RANK = {1: "heuristic_industry", 2: "heuristic_sector", 3: "spy_fallback"}


def resolve_classification(meta_row: Optional[dict]) -> dict:
    """
    Resolve (sector, industry, source) for one stock from a Tiingo meta row.

    Primary: Tiingo `sector` / `industry`.
    Fallback: `sicSector` / `sicIndustry` ONLY when Tiingo `sector` is null/blank.
    Returns source='tiingo', 'sic', or 'none'.

    The sector and industry source are resolved together off the same primary
    field (Tiingo `sector` presence) so a row never mixes a Tiingo sector with
    a SIC industry — that combination is not something the crosswalk was tuned
    against.
    """
    if not meta_row:
        return {"sector": None, "industry": None, "source": "none"}

    def _clean(v):
        if v is None:
            return None
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none", "null"}:
            return None
        return s

    tii_sector = _clean(meta_row.get("sector"))
    tii_industry = _clean(meta_row.get("industry"))

    if tii_sector:
        return {"sector": tii_sector, "industry": tii_industry, "source": "tiingo"}

    sic_sector = _clean(meta_row.get("sicSector"))
    sic_industry = _clean(meta_row.get("sicIndustry"))
    if sic_sector:
        return {"sector": sic_sector, "industry": sic_industry, "source": "sic"}

    return {"sector": None, "industry": None, "source": "none"}


def _safe_beta_r2(stock_ret: pd.DataFrame, etf_ret: Optional[pd.DataFrame], as_of: date):
    """beta_r2_pair guarded against a missing ETF return frame."""
    if etf_ret is None or etf_ret.empty:
        return (np.nan, np.nan, 0)
    return beta_r2_pair(stock_ret, etf_ret, as_of)


def _tier_ok(beta: float, r2: float, n_obs: int) -> bool:
    """A tier passes the quality gate iff beta is finite & non-negative, R²
    finite & >= R2_FLOOR, and n_obs meets the minimum."""
    if n_obs < MIN_N_OBS:
        return False
    if not np.isfinite(beta) or beta < 0:
        return False
    if not np.isfinite(r2) or r2 < R2_FLOOR:
        return False
    return True


def build_hedge_map(
    eligible: pd.DataFrame,
    all_bars: dict[str, pd.DataFrame],
    etf_meta: dict[str, dict],
    classification: dict[str, dict],
    as_of: date,
    effective_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Build the heuristic hedge map (row-per-rank).

    Args:
        eligible:  DataFrame from compute_eligibility (needs `symbol`, `eligible`).
        all_bars:  {symbol -> bars DataFrame} for stocks AND every crosswalk ETF.
        etf_meta:  {etf -> {shortable, easy_to_borrow}}.
        classification: {TICKER_UPPER -> tiingo meta row dict}.
        as_of:     last close used for betas/ADV (inclusive).
        effective_date: session this partition is for (default next_trading_day).

    For every eligible stock we emit exactly 3 rows (rank1/2/3) when at least
    one tier passes the quality gate. rank1 = industry tier (possibly demoted to
    sector or SPY), rank2 = sector tier, rank3 = SPY. Non-finite betas are never
    emitted; a tier that fails the gate is replaced per the fallback chain.
    """
    if effective_date is None:
        effective_date = _next_trading_day(as_of)

    # Pre-compute returns + ADV for every ETF the crosswalk can emit (sector
    # SPDRs + industry pure-plays + SPY), independent of etf_meta membership.
    # An ETF with no bars is simply skipped here; _safe_beta_r2 then returns NaN
    # for it and the quality gate demotes that tier.
    etf_rets: dict[str, pd.DataFrame] = {}
    etf_advs: dict[str, float] = {}
    for etf in all_referenced_etfs():
        df = all_bars.get(etf, pd.DataFrame())
        if df is None or df.empty:
            continue
        r = _compute_returns(df)
        etf_rets[etf] = r
        etf_advs[etf] = compute_adv(r, as_of)

    rows: list[dict] = []
    elig_syms = eligible[eligible["eligible"]]["symbol"].tolist()

    for sym in elig_syms:
        sdf = all_bars.get(sym, pd.DataFrame())
        if sdf is None or sdf.empty:
            continue
        sret = _compute_returns(sdf)
        stock_adv = compute_adv(sret, as_of)

        cls = resolve_classification(classification.get(sym.upper()))
        ladder = classify(cls["sector"], cls["industry"])

        # ---- rank1: industry tier with quality-gate fallback chain ----
        # Candidate ETFs in demotion order: industry → sector → SPY.
        # We dedupe so we never regress the same ETF twice.
        rank1_chain: list[tuple[str, str]] = []  # (etf, basis_if_chosen)
        ind_etf = ladder["industry_etf"]
        sec_etf = ladder["sector_etf"]
        if ind_etf is not None:
            rank1_chain.append((ind_etf, "heuristic_industry"))
        if sec_etf is not None and (not rank1_chain or rank1_chain[-1][0] != sec_etf):
            rank1_chain.append((sec_etf, "heuristic_sector"))
        if not rank1_chain or rank1_chain[-1][0] != SPY:
            rank1_chain.append((SPY, "spy_fallback"))

        rank1_pick = None
        for etf, basis in rank1_chain:
            beta, r2, n_obs = _safe_beta_r2(sret, etf_rets.get(etf), as_of)
            if _tier_ok(beta, r2, n_obs):
                rank1_pick = (etf, basis, beta, r2, n_obs)
                break
        if rank1_pick is None:
            # No tier passed the gate (incl. SPY) — skip the stock entirely.
            # Non-finite / failing SPY means we have no usable hedge.
            continue

        # ---- rank2: sector tier (always the sector ETF; SPY when the sector is
        # unmappable, in which case the basis reflects spy_fallback) ----
        rank2_etf = sec_etf if sec_etf is not None else SPY
        rank2_basis = "heuristic_sector" if sec_etf is not None else "spy_fallback"
        b2, r2_2, n2 = _safe_beta_r2(sret, etf_rets.get(rank2_etf), as_of)

        # ---- rank3: SPY (always; broad-market floor) ----
        b3, r2_3, n3 = _safe_beta_r2(sret, etf_rets.get(SPY), as_of)

        tiers = [
            (1, rank1_pick[0], rank1_pick[1], rank1_pick[2], rank1_pick[3], rank1_pick[4]),
            (2, rank2_etf, rank2_basis, b2, r2_2, n2),
            (3, SPY, "spy_fallback", b3, r2_3, n3),
        ]

        for rank, etf, basis, beta, r2, n_obs in tiers:
            # Reject non-finite betas anywhere in the output (don't emit them).
            if not np.isfinite(beta):
                beta = np.nan
            m = etf_meta.get(etf, {})
            rows.append({
                "effective_date": effective_date,
                "as_of_date": as_of,
                "ticker": sym,
                "rank": rank,
                "hedge_etf": etf,
                "beta": beta,
                "r2": r2 if np.isfinite(r2) else np.nan,
                "n_obs": int(n_obs),
                "etf_shortable": bool(m.get("shortable", False)),
                "etf_easy_to_borrow": bool(m.get("easy_to_borrow", False)),
                "etf_adv_usd_30d": etf_advs.get(etf, np.nan),
                "stock_adv_usd_30d": stock_adv,
                "selection_basis": basis,
                "industry_source": ladder["industry_source"],
                "classification_sector": cls["sector"],
                "classification_industry": cls["industry"],
                "classification_source": cls["source"],
            })

    if not rows:
        return pd.DataFrame(columns=HEDGE_MAP_COLUMNS)
    return pd.DataFrame(rows)[HEDGE_MAP_COLUMNS]
