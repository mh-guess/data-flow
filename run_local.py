"""
Local runner for the hedge_map ETL — bypasses Prefect Cloud orchestration.

Executes the exact same logic as hedge_map_flow.py but uses boto3 directly
instead of AwsCredentials.load() (which requires a live Prefect Cloud session).
All task-level logic is unchanged; only the credential loading and Prefect task
decorator are bypassed.

Usage:
  python run_local.py [--subset SYM1,SYM2,...] [--as-of YYYY-MM-DD] [--backfill-days N]
"""

from __future__ import annotations

import argparse
import io
import json
import os
import time
from datetime import date, timedelta, datetime, timezone
from typing import Optional

import boto3
import numpy as np
import pandas as pd
import requests

# Import all computation helpers from the main flow (no Prefect decorators needed).
from hedge_map_flow import (
    S3_BUCKET,
    HEDGE_MAP_PREFIX,
    UNIVERSE_SNAPSHOT_PREFIX,
    ETF_CANDIDATES,
    ETF_SET_VERSION,
    SYMBOL_REMAP,
    TOP_N_HEDGES,
    ALPACA_BATCH_SIZE,
    ALPACA_RATE_DELAY,
    MIN_LISTING_DAYS,
    MIN_N_OBS,
    ADV_MIN_USD,
    _init_alpaca_creds,
    _alpaca_headers,
    _get,
    _fetch_multi_bars_page,
    _bars_to_df,
    _compute_returns,
    beta_r2_pair,
    compute_adv,
    compute_eligibility,
    compute_hedge_map,
    _next_trading_day,
    _latest_trading_day,
    _prior_trading_day,
    _trading_days_before,
)


def fetch_universe_local() -> pd.DataFrame:
    """Fetch Alpaca universe without Prefect task wrapper."""
    print("  Fetching universe from Alpaca get_all_assets...")
    url = "https://api.alpaca.markets/v2/assets"
    params = {"status": "active", "asset_class": "us_equity"}
    r = requests.get(url, headers=_alpaca_headers(), params=params, timeout=60)
    r.raise_for_status()
    assets = r.json()

    rows = []
    for a in assets:
        if not a.get("tradable", False):
            continue
        sym = a["symbol"]
        sym = SYMBOL_REMAP.get(sym, sym)
        rows.append({
            "symbol": sym,
            "shortable": bool(a.get("shortable", False)),
            "easy_to_borrow": bool(a.get("easy_to_borrow", False)),
        })

    df = pd.DataFrame(rows).drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    print(f"  Universe: {len(df)} active tradable US equities")
    return df


def fetch_all_bars_local(
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, pd.DataFrame]:
    """Fetch bars in batches without Prefect task wrapper."""
    print(f"  Fetching bars for {len(symbols)} symbols from {start} to {end}...")
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
        if (i + 1) % 10 == 0 or (i + 1) == len(batches):
            got = sum(1 for df in all_bars.values() if not df.empty)
            print(f"    Batch {i+1}/{len(batches)}: {got}/{len(all_bars)} symbols with data")

    return all_bars


def write_to_s3_local(
    s3_client,
    key: str,
    body: bytes,
    content_type: str,
) -> str:
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType=content_type)
    return f"s3://{S3_BUCKET}/{key}"


def build_etf_meta_local(universe: pd.DataFrame) -> dict[str, dict]:
    """Resolve shortability for all 57 ETF candidates."""
    meta = {
        row["symbol"]: {"shortable": row["shortable"], "easy_to_borrow": row["easy_to_borrow"]}
        for _, row in universe[universe["symbol"].isin(ETF_CANDIDATES)].iterrows()
    }
    missing = [e for e in ETF_CANDIDATES if e not in meta]
    if missing:
        print(f"  Fetching individual asset records for {len(missing)} ETFs: {missing}")
    for etf in missing:
        try:
            r = requests.get(
                f"https://api.alpaca.markets/v2/assets/{etf}",
                headers=_alpaca_headers(),
                timeout=30,
            )
            if r.status_code == 200:
                a = r.json()
                meta[etf] = {
                    "shortable": bool(a.get("shortable", False)),
                    "easy_to_borrow": bool(a.get("easy_to_borrow", False)),
                }
            else:
                print(f"    {etf}: {r.status_code} → non-shortable")
                meta[etf] = {"shortable": False, "easy_to_borrow": False}
        except Exception as exc:
            print(f"    {etf}: fetch error ({exc}) → non-shortable")
            meta[etf] = {"shortable": False, "easy_to_borrow": False}
    return meta


def run(
    as_of_override: Optional[str] = None,
    subset_symbols: Optional[list[str]] = None,
    backfill_days: int = 0,
) -> list[dict]:
    print("=" * 60)
    print("Hedge Map ETL — local run")
    print("=" * 60)

    # Initialize Alpaca creds from env vars (no Prefect blocks in local runner).
    _init_alpaca_creds(from_prefect_blocks=False)

    s3 = boto3.client("s3", region_name="us-east-1")

    # Dates: use _latest_trading_day() (same logic as the deployed flow default).
    as_of = date.fromisoformat(as_of_override) if as_of_override else _latest_trading_day(date.today())
    as_of_dates = _trading_days_before(as_of, backfill_days) if backfill_days > 0 else [as_of]
    print(f"as_of_date: {as_of}  effective_date: {_next_trading_day(as_of)}")
    if backfill_days > 0:
        print(f"Backfill: {len(as_of_dates)} days [{as_of_dates[-1]} → {as_of_dates[0]}]")

    # Universe.
    universe = fetch_universe_local()
    if subset_symbols:
        universe = universe[universe["symbol"].isin(subset_symbols)].reset_index(drop=True)
        print(f"Subset mode: {len(universe)} symbols")

    # Persist universe snapshot.
    snap = universe.copy()
    snap["as_of_date"] = as_of_dates[0]
    buf = io.BytesIO()
    snap.to_parquet(buf, compression="snappy", index=False)
    snap_key = f"{UNIVERSE_SNAPSHOT_PREFIX}/as_of={as_of_dates[0].isoformat()}/universe.parquet"
    write_to_s3_local(s3, snap_key, buf.getvalue(), "application/x-parquet")
    print(f"  Universe snapshot: s3://{S3_BUCKET}/{snap_key}")

    # ETF metadata.
    etf_meta = build_etf_meta_local(universe)

    # Bars.
    earliest_as_of = as_of_dates[-1]
    bar_end = as_of_dates[0] - timedelta(days=1)  # exclude as_of day (defensive)
    bar_start = earliest_as_of - timedelta(days=130)

    all_symbols = list(dict.fromkeys(universe["symbol"].tolist() + ETF_CANDIDATES))
    all_bars = fetch_all_bars_local(all_symbols, bar_start, bar_end)
    got = sum(1 for df in all_bars.values() if not df.empty)
    print(f"  Bars loaded: {got}/{len(all_symbols)} symbols with data")

    # Process each as_of date.
    results: list[dict] = []
    for run_as_of in as_of_dates:
        print(f"\nComputing hedge map for as_of={run_as_of}...")
        elig_df = compute_eligibility(universe, all_bars, run_as_of)
        eligible_count = int(elig_df["eligible"].sum())
        print(f"  Eligible: {eligible_count}/{len(universe)}")

        hedge_map = compute_hedge_map(elig_df, all_bars, etf_meta, run_as_of)
        covered = hedge_map[hedge_map["rank"] == 1]["ticker"].nunique() if not hedge_map.empty else 0
        print(f"  Covered tickers (rank=1): {covered}")

        if hedge_map.empty:
            print("  WARNING: empty hedge map!")
            results.append({"as_of": run_as_of.isoformat(), "error": "empty_hedge_map"})
            continue

        effective_date = _next_trading_day(run_as_of)

        # Write parquet.
        hm = hedge_map.copy()
        hm["effective_date"] = pd.to_datetime(hm["effective_date"]).dt.date
        hm["as_of_date"] = pd.to_datetime(hm["as_of_date"]).dt.date
        hm["n_obs"] = hm["n_obs"].astype(int)
        hm["rank"] = hm["rank"].astype(int)
        hm["etf_shortable"] = hm["etf_shortable"].astype(bool)
        hm["etf_easy_to_borrow"] = hm["etf_easy_to_borrow"].astype(bool)

        buf2 = io.BytesIO()
        hm.to_parquet(buf2, compression="snappy", index=False)
        parquet_key = f"{HEDGE_MAP_PREFIX}/effective_date={effective_date.isoformat()}/data.parquet"
        parquet_uri = write_to_s3_local(s3, parquet_key, buf2.getvalue(), "application/x-parquet")
        print(f"  Parquet written: {parquet_uri}")

        # Write manifest.
        coverage_pct = round(covered / eligible_count * 100.0, 2) if eligible_count > 0 else 0.0
        manifest = {
            "effective_date": effective_date.isoformat(),
            "as_of_date": run_as_of.isoformat(),
            "universe_size": len(universe),
            "eligible_count": eligible_count,
            "coverage_pct": coverage_pct,
            "etf_set_version": ETF_SET_VERSION,
            "row_count": len(hm),
            "run_utc": datetime.now(timezone.utc).isoformat(),
        }
        manifest_key = f"{HEDGE_MAP_PREFIX}/manifests/effective_date={effective_date.isoformat()}/manifest.json"
        write_to_s3_local(s3, manifest_key, json.dumps(manifest, indent=2).encode(), "application/json")
        print(f"  Manifest written: s3://{S3_BUCKET}/{manifest_key}")

        results.append({
            "as_of": run_as_of.isoformat(),
            "effective_date": effective_date.isoformat(),
            "universe_size": len(universe),
            "eligible_count": eligible_count,
            "covered_tickers": covered,
            "hedge_map_rows": len(hm),
            "coverage_pct": coverage_pct,
            "s3_uri": parquet_uri,
            "manifest_uri": f"s3://{S3_BUCKET}/{manifest_key}",
        })

    print("\n" + "=" * 60)
    print("Completed. Results:")
    for r in results:
        print(f"  {json.dumps(r, default=str)}")
    print("=" * 60)
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Local hedge-map ETL runner")
    ap.add_argument("--as-of", dest="as_of")
    ap.add_argument("--subset", help="Comma-separated symbols (testing)")
    ap.add_argument("--backfill-days", dest="backfill_days", type=int, default=0)
    args = ap.parse_args()

    subset = [s.strip() for s in args.subset.split(",")] if args.subset else None
    results = run(as_of_override=args.as_of, subset_symbols=subset, backfill_days=args.backfill_days)
    print("\nFinal result:", json.dumps(results[-1] if results else {}, default=str, indent=2))
