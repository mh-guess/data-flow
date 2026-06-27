"""
Discovery (read-only): build the sector/industry vocabulary for the heuristic
hedge-map crosswalk.

Pulls the latest Tiingo `meta` partition from S3, intersects with the liquid +
shortable Alpaca universe (the same `fetch_universe()` screen the production flow
uses, plus a daily-bar $ADV gate), and produces a frequency-ranked table of
DISTINCT (sector, industry) values.

Output artifact: discovery_sector_industry.csv in the worktree.

Run:
  python discovery_classification.py [--adv-min 5e6] [--top 60]
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, timedelta

import boto3
import pandas as pd
import requests

from hedge_map_flow import (
    S3_BUCKET,
    SYMBOL_REMAP,
    ALPACA_BATCH_SIZE,
    ALPACA_RATE_DELAY,
    _init_alpaca_creds,
    _alpaca_headers,
    _fetch_multi_bars_page,
    _bars_to_df,
    _compute_returns,
    compute_adv,
)

META_PREFIX = "tiingo/json/fundamentals/meta"


def latest_meta_key(s3) -> str:
    """Find the most recent meta.json partition under the meta prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{META_PREFIX}/date="):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("meta.json"):
                keys.append(obj["Key"])
    if not keys:
        raise FileNotFoundError(f"No meta.json found under s3://{S3_BUCKET}/{META_PREFIX}/")
    # Keys sort lexicographically; date=YYYY-MM-DD sorts chronologically.
    return sorted(keys)[-1]


def load_meta(s3, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj["Body"].read())
    df = pd.DataFrame(data)
    return df


def fetch_universe_local() -> pd.DataFrame:
    url = "https://api.alpaca.markets/v2/assets"
    params = {"status": "active", "asset_class": "us_equity"}
    r = requests.get(url, headers=_alpaca_headers(), params=params, timeout=60)
    r.raise_for_status()
    rows = []
    for a in r.json():
        if not a.get("tradable", False):
            continue
        sym = SYMBOL_REMAP.get(a["symbol"], a["symbol"])
        rows.append({
            "symbol": sym,
            "shortable": bool(a.get("shortable", False)),
            "easy_to_borrow": bool(a.get("easy_to_borrow", False)),
        })
    return pd.DataFrame(rows).drop_duplicates(subset=["symbol"]).reset_index(drop=True)


def fetch_all_bars(symbols: list[str], start: date, end: date) -> dict[str, pd.DataFrame]:
    start_str = f"{start.isoformat()}T00:00:00Z"
    end_str = f"{end.isoformat()}T23:59:59Z"
    batches = [symbols[i:i + ALPACA_BATCH_SIZE] for i in range(0, len(symbols), ALPACA_BATCH_SIZE)]
    all_bars: dict[str, pd.DataFrame] = {}
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(ALPACA_RATE_DELAY)
        raw = _fetch_multi_bars_page(batch, start_str, end_str)
        for sym in batch:
            all_bars[sym] = _bars_to_df(raw.get(sym, []), sym)
        if (i + 1) % 20 == 0 or (i + 1) == len(batches):
            print(f"  bars batch {i+1}/{len(batches)}")
    return all_bars


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--adv-min", type=float, default=5e6,
                    help="min trailing-30d $ADV for the 'liquid' frequency column")
    ap.add_argument("--top", type=int, default=60)
    args = ap.parse_args()

    _init_alpaca_creds(from_prefect_blocks=False)
    s3 = boto3.client("s3", region_name="us-east-1")

    key = latest_meta_key(s3)
    print(f"Latest meta partition: s3://{S3_BUCKET}/{key}")
    meta = load_meta(s3, key)
    print(f"Meta rows: {len(meta)}  columns: {list(meta.columns)}")

    # Tiingo ticker is lowercase; Alpaca is uppercase. Normalize for the join.
    meta["ticker_up"] = meta["ticker"].astype(str).str.upper()
    meta["ticker_up"] = meta["ticker_up"].map(lambda s: SYMBOL_REMAP.get(s, s))

    universe = fetch_universe_local()
    print(f"Universe (tradable us_equity): {len(universe)}")
    short_univ = universe[universe["shortable"]].copy()
    print(f"Shortable universe: {len(short_univ)}")

    joined = meta.merge(
        short_univ, left_on="ticker_up", right_on="symbol", how="inner"
    )
    print(f"Meta ∩ shortable universe: {len(joined)}")

    # Liquid gate: fetch bars for the joined names and compute $ADV.
    syms = sorted(joined["ticker_up"].unique())
    today = date.today()
    bar_start = today - timedelta(days=70)
    print(f"Fetching bars for {len(syms)} names to compute $ADV...")
    bars = fetch_all_bars(syms, bar_start, today)
    adv_map: dict[str, float] = {}
    for sym in syms:
        df = bars.get(sym, pd.DataFrame())
        if df.empty:
            adv_map[sym] = 0.0
            continue
        adv_map[sym] = compute_adv(_compute_returns(df), today) or 0.0
    joined["adv_usd_30d"] = joined["ticker_up"].map(adv_map).fillna(0.0)
    liquid = joined[joined["adv_usd_30d"] >= args.adv_min].copy()
    print(f"Liquid (>= ${args.adv_min:.0e} ADV) ∩ shortable: {len(liquid)}")

    # Effective classification: Tiingo sector/industry primary, SIC fallback.
    def eff(row, primary, fallback):
        v = row.get(primary)
        if v is None or (isinstance(v, float)) or str(v).strip() == "":
            f = row.get(fallback)
            return str(f).strip() if f and str(f).strip() else "(none)"
        return str(v).strip()

    for src_df in (joined, liquid):
        src_df["eff_sector"] = src_df.apply(lambda r: eff(r, "sector", "sicSector"), axis=1)
        src_df["eff_industry"] = src_df.apply(lambda r: eff(r, "industry", "sicIndustry"), axis=1)

    # Frequency table over DISTINCT (sector, industry).
    all_counts = joined.groupby(["eff_sector", "eff_industry"]).size().rename("n_all")
    liq_counts = liquid.groupby(["eff_sector", "eff_industry"]).size().rename("n_liquid")
    freq = pd.concat([all_counts, liq_counts], axis=1).fillna(0).astype(int)
    freq = freq.reset_index().sort_values(["n_liquid", "n_all"], ascending=False)

    out_path = os.path.join(os.path.dirname(__file__), "discovery_sector_industry.csv")
    freq.to_csv(out_path, index=False)
    print(f"\nWrote {out_path} ({len(freq)} distinct (sector, industry) pairs)")

    # Sector-level rollup.
    sector_roll = (
        liquid.groupby("eff_sector").size().sort_values(ascending=False)
    )
    print("\n=== Sector frequency (liquid+shortable) ===")
    print(sector_roll.to_string())

    print(f"\n=== Top {args.top} (sector, industry) by liquid count ===")
    print(freq.head(args.top).to_string(index=False))

    # How many names fall in industries that already appear in the research oracle?
    print(f"\nDistinct industries (liquid): {liquid['eff_industry'].nunique()}")
    print(f"Distinct sectors (liquid): {liquid['eff_sector'].nunique()}")
    # Null-sector diagnostics.
    null_sector = joined["sector"].isna() | (joined["sector"].astype(str).str.strip() == "")
    print(f"Names with null Tiingo sector (SIC fallback needed): {int(null_sector.sum())}")


if __name__ == "__main__":
    main()
