"""
Parity acceptance test: compare production hedge map against research hedge_resolved.csv.

For each overlapping event (stock, date), classify:
  MATCH          — same best_etf, beta within float tol, r2 within tol
  EXPECTED_DIV   — research pick was illiquid (<$25M ADV) or not shortable (production screens it out)
  UNEXPECTED_DIV — needs investigation

Gate: zero UNEXPECTED_DIV.
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import requests

import sys
sys.path.insert(0, os.path.dirname(__file__))

from hedge_map_flow import (
    ETF_CANDIDATES,
    SYMBOL_REMAP,
    _init_alpaca_creds,
    _alpaca_headers,
    _fetch_multi_bars_page,
    _bars_to_df,
    _compute_returns,
    beta_r2_pair,
    compute_adv,
    ADV_MIN_USD,
    ADV_LOOKBACK_DAYS,
    ALPACA_BATCH_SIZE,
    ALPACA_RATE_DELAY,
    MIN_N_OBS,
    _prior_trading_day,
)

HERE = os.path.dirname(__file__)

# Fallback resolution order for hedge_resolved.csv:
#   1. --hedge-resolved CLI arg (resolved in main())
#   2. HEDGE_RESOLVED_PATH env var
#   3. Research directory (developer machine)
#   4. Bundled fixture (CI / any machine)
_RESEARCH_DEFAULT = "/Users/timhuang/projects/algo-trade/research/motley_fool/etf_hedge/hedge_resolved.csv"
_FIXTURE_DEFAULT = os.path.join(HERE, "tests", "fixtures", "hedge_resolved.csv")

# Initialize Alpaca credentials from env vars (parity_check.py runs locally, not via Prefect).
_init_alpaca_creds(from_prefect_blocks=False)

BETA_TOL = 0.05    # tolerance for beta comparison (absolute)
R2_TOL = 0.02     # tolerance for r2 comparison (absolute)


def _resolve_hedge_resolved_path(cli_arg: Optional[str] = None) -> str:
    """Return the path to hedge_resolved.csv, trying sources in priority order."""
    if cli_arg:
        return cli_arg
    env = os.environ.get("HEDGE_RESOLVED_PATH")
    if env:
        return env
    if os.path.exists(_RESEARCH_DEFAULT):
        return _RESEARCH_DEFAULT
    if os.path.exists(_FIXTURE_DEFAULT):
        return _FIXTURE_DEFAULT
    raise FileNotFoundError(
        "hedge_resolved.csv not found. Pass --hedge-resolved <path>, set "
        "HEDGE_RESOLVED_PATH, or ensure the research directory is available."
    )


def fetch_bars_for_symbols(
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, pd.DataFrame]:
    """Fetch daily bars for a list of symbols."""
    all_bars: dict[str, pd.DataFrame] = {}
    start_str = f"{start.isoformat()}T00:00:00Z"
    end_str = f"{end.isoformat()}T23:59:59Z"
    batches = [symbols[i:i + ALPACA_BATCH_SIZE] for i in range(0, len(symbols), ALPACA_BATCH_SIZE)]
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(ALPACA_RATE_DELAY)
        raw = _fetch_multi_bars_page(batch, start_str, end_str)
        for sym in batch:
            all_bars[sym] = _bars_to_df(raw.get(sym, []), sym)
    return all_bars


def get_etf_shortability(etfs: list[str]) -> dict[str, bool]:
    """Fetch current shortability for each ETF candidate."""
    shortable: dict[str, bool] = {}
    url = "https://api.alpaca.markets/v2/assets"
    params = {"status": "active", "asset_class": "us_equity"}
    r = requests.get(url, headers=_alpaca_headers(), params=params, timeout=60)
    r.raise_for_status()
    assets = r.json()
    asset_map = {a["symbol"]: a for a in assets}
    for etf in etfs:
        a = asset_map.get(etf)
        if a:
            shortable[etf] = bool(a.get("shortable", False))
        else:
            # Try individual fetch.
            try:
                r2 = requests.get(
                    f"https://api.alpaca.markets/v2/assets/{etf}",
                    headers=_alpaca_headers(), timeout=30
                )
                if r2.status_code == 200:
                    shortable[etf] = bool(r2.json().get("shortable", False))
                else:
                    shortable[etf] = False
            except Exception:
                shortable[etf] = False
    return shortable


def main(hedge_resolved_path: Optional[str] = None):
    hedge_resolved = _resolve_hedge_resolved_path(hedge_resolved_path)
    print(f"Using hedge_resolved: {hedge_resolved}")
    # Load research resolved hedge.
    research = pd.read_csv(hedge_resolved)
    research["date"] = pd.to_datetime(research["date"]).dt.date
    # Only rows with a valid hedge.
    research = research[research["best_etf"].notna()].copy()
    print(f"Research events with valid hedge: {len(research)}")

    # --- Collect all symbols and dates needed ---
    events = research[["data_symbol", "date", "best_etf", "best_beta", "best_r2"]].copy()
    all_stock_syms = list(events["data_symbol"].unique())
    all_dates = sorted(events["date"].unique())
    min_date = min(all_dates)
    max_date = max(all_dates)

    # Fetch bars for ~130 calendar days before min_date through max_date.
    bar_start = min_date - timedelta(days=130)
    bar_end = max_date

    all_syms = list(dict.fromkeys(all_stock_syms + ETF_CANDIDATES))
    print(f"Fetching bars for {len(all_syms)} symbols from {bar_start} to {bar_end}...")
    all_bars = fetch_bars_for_symbols(all_syms, bar_start, bar_end)
    got = sum(1 for df in all_bars.values() if not df.empty)
    print(f"Bars available: {got}/{len(all_syms)}")

    # Fetch ETF shortability (current, used as proxy for historical).
    print("Fetching ETF shortability...")
    etf_shortable = get_etf_shortability(ETF_CANDIDATES)
    shortable_etfs = {e for e, s in etf_shortable.items() if s}
    print(f"Currently shortable ETFs: {len(shortable_etfs)}/{len(ETF_CANDIDATES)}")

    # Run parity.
    results = []
    for _, ev in events.iterrows():
        stock = ev["data_symbol"]
        pick_date = ev["date"]
        research_etf = ev["best_etf"]
        research_beta = ev["best_beta"]
        research_r2 = ev["best_r2"]

        # beta_r2_pair now uses d <= as_of (inclusive), so pass prior_trading_day(pick_date)
        # to reproduce the research window that ends at pick_date - 1 trading day.
        as_of = _prior_trading_day(pick_date)

        stock_bars = all_bars.get(stock, pd.DataFrame())
        if stock_bars.empty:
            results.append({
                "stock": stock, "date": pick_date, "research_etf": research_etf,
                "classification": "SKIP_NO_STOCK_BARS", "note": "no bars",
            })
            continue

        stock_ret = _compute_returns(stock_bars)

        # Compute production beta/r2 for each ETF.
        cands: list[dict] = []
        for etf in ETF_CANDIDATES:
            etf_bars = all_bars.get(etf, pd.DataFrame())
            if etf_bars.empty:
                continue
            etf_ret = _compute_returns(etf_bars)
            beta, r2, n_obs = beta_r2_pair(stock_ret, etf_ret, as_of)
            adv = compute_adv(etf_ret, as_of)
            cands.append({
                "etf": etf, "beta": beta, "r2": r2, "n_obs": n_obs,
                "adv": adv,
                "shortable": etf_shortable.get(etf, False),
            })

        if not cands:
            results.append({
                "stock": stock, "date": pick_date, "research_etf": research_etf,
                "classification": "SKIP_NO_ETF_CANDS", "note": "no ETF bars",
            })
            continue

        # Find what production would select (liquid + shortable).
        eligible = [c for c in cands
                    if np.isfinite(c["r2"]) and c["n_obs"] >= MIN_N_OBS
                    and c["adv"] >= ADV_MIN_USD and c["shortable"]]
        eligible.sort(key=lambda x: -x["r2"])
        prod_best = eligible[0] if eligible else None

        # Compute beta/r2 for the research ETF (regardless of screen).
        research_etf_row = next((c for c in cands if c["etf"] == research_etf), None)

        if prod_best and prod_best["etf"] == research_etf:
            # Same ETF selected — check beta and r2 match.
            beta_ok = abs(prod_best["beta"] - research_beta) < BETA_TOL
            r2_ok = abs(prod_best["r2"] - research_r2) < R2_TOL
            if beta_ok and r2_ok:
                cls = "MATCH"
                note = f"prod_beta={prod_best['beta']:.4f} research_beta={research_beta:.4f}"
            else:
                cls = "UNEXPECTED_DIV"
                note = (f"same ETF but values differ: "
                        f"beta {prod_best['beta']:.4f} vs {research_beta:.4f} "
                        f"r2 {prod_best['r2']:.4f} vs {research_r2:.4f}")
        else:
            # Different ETF or no ETF selected. Was the research pick excluded by the screen?
            if research_etf_row is None:
                cls = "EXPECTED_DIV"
                note = f"research ETF {research_etf} has no bar data → expected divergence"
            elif research_etf_row["adv"] < ADV_MIN_USD:
                cls = "EXPECTED_DIV"
                note = (f"research ETF {research_etf} illiquid: "
                        f"ADV={research_etf_row['adv']/1e6:.1f}M < $25M → screen excludes it")
            elif not research_etf_row["shortable"]:
                cls = "EXPECTED_DIV"
                note = f"research ETF {research_etf} not shortable → screen excludes it"
            elif research_etf_row.get("n_obs", 0) < MIN_N_OBS:
                cls = "EXPECTED_DIV"
                note = f"research ETF {research_etf} n_obs={research_etf_row['n_obs']} < {MIN_N_OBS}"
            else:
                cls = "UNEXPECTED_DIV"
                prod_etf = prod_best["etf"] if prod_best else "none"
                note = (f"production selected {prod_etf} "
                        f"(r2={prod_best['r2']:.4f}) vs research {research_etf} "
                        f"(r2={research_r2:.4f}); research pick passes screens")

        results.append({
            "stock": stock,
            "date": str(pick_date),  # report the original pick date, not the shifted as_of
            "research_etf": research_etf,
            "research_beta": research_beta,
            "research_r2": research_r2,
            "prod_etf": prod_best["etf"] if prod_best else None,
            "prod_beta": prod_best["beta"] if prod_best else None,
            "prod_r2": prod_best["r2"] if prod_best else None,
            "research_etf_adv": research_etf_row["adv"] if research_etf_row else None,
            "research_etf_shortable": research_etf_row["shortable"] if research_etf_row else None,
            "classification": cls,
            "note": note,
        })

    df = pd.DataFrame(results)
    print("\n" + "=" * 60)
    print("PARITY REPORT")
    print("=" * 60)
    counts = df["classification"].value_counts()
    print(counts.to_string())
    total = len(df)
    match_count = counts.get("MATCH", 0)
    expected_div = counts.get("EXPECTED_DIV", 0)
    unexpected_div = counts.get("UNEXPECTED_DIV", 0)
    skip = counts.get("SKIP_NO_STOCK_BARS", 0) + counts.get("SKIP_NO_ETF_CANDS", 0)

    print(f"\nTotal events checked: {total}")
    print(f"  MATCH:          {match_count}  ({match_count/total*100:.1f}%)")
    print(f"  EXPECTED_DIV:   {expected_div}  ({expected_div/total*100:.1f}%)")
    print(f"  UNEXPECTED_DIV: {unexpected_div}  ({unexpected_div/total*100:.1f}%)")
    print(f"  SKIP (no data): {skip}")

    if unexpected_div > 0:
        print("\nUNEXPECTED DIVERGENCES (investigate):")
        print(df[df["classification"] == "UNEXPECTED_DIV"][
            ["stock", "date", "research_etf", "research_r2", "prod_etf", "prod_r2", "note"]
        ].to_string(index=False))
        print("\nPARITY GATE: FAIL — unexpected divergences present")
    else:
        print("\nPARITY GATE: PASS — all divergences explained by liquid+shortable screen")

    if expected_div > 0:
        print("\nEXPECTED DIVERGENCES (research pick excluded by production screen):")
        print(df[df["classification"] == "EXPECTED_DIV"][
            ["stock", "date", "research_etf", "note"]
        ].to_string(index=False))

    return df


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Parity check against research hedge_resolved.csv")
    ap.add_argument(
        "--hedge-resolved",
        dest="hedge_resolved",
        default=None,
        help="Path to hedge_resolved.csv (overrides HEDGE_RESOLVED_PATH env var and defaults)",
    )
    args = ap.parse_args()

    df = main(hedge_resolved_path=args.hedge_resolved)
    df.to_csv("/tmp/parity_results.csv", index=False)
    print("\nFull results saved to /tmp/parity_results.csv")
