"""
Tiingo `meta` classification loader for the heuristic hedge map.

Reads the LATEST available `meta.json` partition from S3 and returns:
  - a {TICKER_UPPER -> meta_row_dict} lookup for the selection step, and
  - the partition date (for the manifest).

Tiingo tickers are lowercase; we upper-case and apply SYMBOL_REMAP so the keys
line up with the Alpaca-canonical symbols used everywhere else in the pipeline.

NOTE (known coverage limitation): the production `meta` partition currently
covers only the curated Tiingo fundamentals universe (~100 tickers, driven by
`adhoc/tickers.txt`), NOT the full ~13k Alpaca universe. Stocks with no meta row
are classified source='none' and fall back to SPY. Coverage stats are reported
in the manifest so this is observable. Broadening the meta universe is a
data-source change tracked separately from this flow.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Optional

from hedge_map_flow import S3_BUCKET, SYMBOL_REMAP

META_PREFIX = "tiingo/json/fundamentals/meta"


def latest_meta_partition_key(s3_client, as_of: Optional[date] = None) -> str:
    """
    Return the S3 key of the most recent meta.json partition with
    date <= as_of (or the latest overall when as_of is None).

    Raises FileNotFoundError when no partition exists.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{META_PREFIX}/date="):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("meta.json"):
                keys.append(obj["Key"])
    if not keys:
        raise FileNotFoundError(
            f"No meta.json found under s3://{S3_BUCKET}/{META_PREFIX}/"
        )
    keys.sort()  # date=YYYY-MM-DD sorts chronologically
    if as_of is not None:
        cutoff = f"{META_PREFIX}/date={as_of.isoformat()}/meta.json"
        eligible = [k for k in keys if k <= cutoff]
        if eligible:
            return eligible[-1]
        # All partitions are newer than as_of — fall back to the earliest one so
        # a back-dated run still gets *some* classification rather than failing.
        return keys[0]
    return keys[-1]


def partition_date_from_key(key: str) -> Optional[date]:
    """Extract the YYYY-MM-DD partition date from a meta key."""
    for part in key.split("/"):
        if part.startswith("date="):
            try:
                return date.fromisoformat(part[len("date="):])
            except ValueError:
                return None
    return None


def load_classification(
    s3_client, as_of: Optional[date] = None
) -> tuple[dict[str, dict], Optional[date]]:
    """
    Load the latest meta partition and return ({TICKER_UPPER -> row}, partition_date).

    The ticker key is upper-cased and SYMBOL_REMAP-normalized so it matches the
    Alpaca-canonical symbols used by fetch_universe(). On a ticker collision
    (e.g. dual rows), the last row wins — meta is effectively one row per ticker.
    """
    key = latest_meta_partition_key(s3_client, as_of)
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj["Body"].read())

    lookup: dict[str, dict] = {}
    for row in data:
        raw = row.get("ticker")
        if not raw:
            continue
        sym = str(raw).strip().upper()
        sym = SYMBOL_REMAP.get(sym, sym)
        lookup[sym] = row

    return lookup, partition_date_from_key(key)
