"""
Manual/backfill helper — write the Tiingo `meta` partition for a date with the
BROADENED universe (full Alpaca tradable us_equity ∪ curated adhoc list), batched.

Replicates tiingo_fundamentals_flow's broadened META path (fetch_tradable_equity_
universe → fetch_meta_batched) exactly, but writes via boto3 so it can run outside
Prefect Cloud (e.g. an initial cutover or a backfill). ONLY the meta partition is
touched; per-ticker daily/statements are NOT run here.

The scheduled production flow does NOT need this — it writes meta for the current
trading day as part of tiingo_fundamentals_flow. This is for manual cutover/backfill.

WEEKEND/ALIGNMENT GUARD: `--date` DEFAULTS to the latest TRADING day (snapped via
the exchange calendar), NOT calendar "today". This prevents the manual-run
footgun where meta is written to a future calendar date (e.g. a Saturday) while
the hedge_map flow snaps `--as-of` to the prior trading day and then reads the
older partition because `latest_meta_partition_key` selects `date <= as_of`.
Pass an explicit `--date YYYY-MM-DD` to override.

Usage (conda env `agent`, PYTHONPATH=<repo>):
  python prod_broaden_meta.py            # dry run, date=latest trading day
  python prod_broaden_meta.py --prod     # WRITE the prod meta key (interlock)
  python prod_broaden_meta.py --date 2026-06-26 --prod --backup-first

Env required: ALPACA_API_KEY, ALPACA_API_SECRET, TIINGO_API_TOKEN, AWS creds.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, timedelta

import boto3

from hedge_map_flow import (
    _init_alpaca_creds,
    _alpaca_headers,
    _init_trading_calendar,
    _latest_trading_day,
)
from tiingo_meta_universe import fetch_tradable_equity_universe, fetch_meta_batched

BUCKET = "mh-guess-data"
META_KEY_TMPL = "tiingo/json/fundamentals/meta/date={date}/meta.json"
BACKUP_KEY_TMPL = "tiingo/json/fundamentals/meta/_backups/date={date}/meta_pre-broaden.json"


def _default_date() -> date:
    """Latest trading day on/<= calendar today (calendar-aware; weekend-safe)."""
    today = date.today()
    _init_trading_calendar(today - timedelta(days=10), today + timedelta(days=3))
    return _latest_trading_day(today)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="partition date YYYY-MM-DD (default: latest trading day)")
    ap.add_argument("--prod", action="store_true",
                    help="REQUIRED to write the real prod key; without it this is a dry run")
    ap.add_argument("--backup-first", action="store_true",
                    help="copy the existing meta partition to a dated backup key before overwriting")
    args = ap.parse_args()

    _init_alpaca_creds(from_prefect_blocks=False)
    s3 = boto3.client("s3", region_name="us-east-1")

    part_date = date.fromisoformat(args.date) if args.date else _default_date()
    if not args.date:
        print(f"--date not given; defaulting to latest trading day = {part_date}")
    meta_key = META_KEY_TMPL.format(date=part_date.isoformat())
    backup_key = BACKUP_KEY_TMPL.format(date=part_date.isoformat())

    if args.backup_first:
        try:
            s3.copy_object(Bucket=BUCKET, Key=backup_key,
                           CopySource={"Bucket": BUCKET, "Key": meta_key})
            print(f"BACKUP: s3://{BUCKET}/{meta_key} -> s3://{BUCKET}/{backup_key}")
        except Exception as exc:  # noqa: BLE001
            print(f"BACKUP WARNING: could not copy existing partition ({exc}); "
                  "it may not exist yet. Continuing.")

    t0 = time.time()
    universe = fetch_tradable_equity_universe(_alpaca_headers())
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="adhoc/tickers.txt")
        curated = [t.strip() for t in obj["Body"].read().decode().split() if t.strip()]
    except Exception:
        curated = []
    merged = list(dict.fromkeys([u.upper() for u in universe] + [c.upper() for c in curated]))
    print(f"meta universe: {len(universe)} tradable + {len(curated)} curated = {len(merged)} unique")

    rows = fetch_meta_batched(os.environ["TIINGO_API_TOKEN"], merged,
                              batch_size=150, sleep_s=0.4, log=print)
    body = json.dumps(rows).encode()
    distinct = {str(r.get("ticker", "")).upper() for r in rows if r.get("ticker")}
    active = {str(r.get("ticker", "")).upper() for r in rows if r.get("isActive") is True}

    if not args.prod:
        print(f"\nDRY RUN (no --prod): would write s3://{BUCKET}/{meta_key} "
              f"size={len(body)} rows={len(rows)} distinct={len(distinct)} active={len(active)}")
        return

    s3.put_object(Bucket=BUCKET, Key=meta_key, Body=body, ContentType="application/json")
    print(f"\nWROTE s3://{BUCKET}/{meta_key}")
    print(f"  size={len(body)} bytes  rows={len(rows)}  distinct_tickers={len(distinct)}  "
          f"active_tickers={len(active)}  elapsed={time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
