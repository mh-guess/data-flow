"""
Tiingo `meta` classification loader for the heuristic hedge map.

Reads the LATEST available `meta.json` partition from S3 and returns:
  - a {TICKER_UPPER -> meta_row_dict} lookup for the selection step, and
  - the partition date (for the manifest).

Tiingo tickers are lowercase; we upper-case and apply SYMBOL_REMAP so the keys
line up with the Alpaca-canonical symbols used everywhere else in the pipeline.

TICKER-REUSE GUARD (critical correctness fix)
---------------------------------------------
The Tiingo bulk meta endpoint returns MULTIPLE rows per ticker when a symbol has
been reused: the live company AND delisted predecessors. Verified examples:
  U    -> 'Unity Software Inc' (isActive=True)  AND 'US AIRWAYS GROUP INC' (isActive=False)
  SNOW -> 'Snowflake Inc'      (isActive=True)  AND 'Intrawest Resorts...'  (isActive=False)
A naive "last row wins" load can pick the DELISTED predecessor and assign a
totally wrong sector/industry (e.g. U -> Airlines -> JETS).

`select_active_meta_row()` resolves this BEFORE the crosswalk:
  1. Drop rows with isActive != True.
  2. Among the remaining active rows, prefer the one whose `name` reconciles with
     the live Alpaca company name (token-overlap after suffix stripping).
  3. If exactly one active row remains, take it. If several remain and none
     reconcile, treat as UNCLASSIFIED (better SPY than a wrong industry).
A ticker with only inactive rows, or an active row whose name does not reconcile,
is treated as UNCLASSIFIED (classification_source='none' -> SPY fallback). The
rank1 beta/R² gate remains as defense-in-depth.

Coverage: the meta partition's breadth is set by the Tiingo fundamentals flow's
ticker source. When that source is the full tradable Alpaca us_equity universe
(see `tiingo_fundamentals_flow`), classification covers the universe; ETFs /
non-equity simply return no useful meta and fall through to SPY.
"""

from __future__ import annotations

import json
import re
from datetime import date
from typing import Optional

from hedge_map_flow import S3_BUCKET, SYMBOL_REMAP

META_PREFIX = "tiingo/json/fundamentals/meta"

# Corporate-suffix / share-class noise stripped before name reconciliation.
_NAME_NOISE = {
    "inc", "incorporated", "corp", "corporation", "co", "company", "ltd",
    "limited", "plc", "lp", "llc", "holdings", "holding", "group", "the",
    "class", "a", "b", "c", "common", "stock", "shares", "share", "ordinary",
    "voting", "subordinate", "par", "value", "sa", "ag", "nv", "se", "trust",
    "depositary", "receipt", "receipts", "adr", "ads", "new", "of",
}


def _name_tokens(name: Optional[str]) -> set[str]:
    """Normalize a company name to a set of significant lowercase tokens."""
    if not name:
        return set()
    cleaned = re.sub(r"[^a-z0-9 ]", " ", str(name).lower())
    return {t for t in cleaned.split() if t and t not in _NAME_NOISE}


def names_reconcile(tiingo_name: Optional[str], alpaca_name: Optional[str]) -> bool:
    """
    True if the Tiingo and Alpaca company names plausibly refer to the same firm.

    Uses significant-token overlap after stripping corporate/share-class noise.
    We require at least one shared significant token AND that the shorter token
    set is mostly contained in the other (>= 50%), which tolerates Alpaca's
    verbose names ("Unity Software Inc." vs Tiingo "Unity Software Inc") while
    rejecting unrelated predecessors ("US Airways Group" vs "Unity Software").
    """
    a = _name_tokens(tiingo_name)
    b = _name_tokens(alpaca_name)
    if not a or not b:
        return False
    overlap = a & b
    if not overlap:
        return False
    shorter = min(len(a), len(b))
    return len(overlap) / shorter >= 0.5


def select_active_meta_row(
    rows: list[dict], alpaca_name: Optional[str] = None
) -> Optional[dict]:
    """
    Pick the correct meta row for one ticker from possibly-many candidates.

    Returns the chosen active+reconciled row, or None when the ticker should be
    treated as UNCLASSIFIED (no active row, or ambiguity with no name match).
    """
    if not rows:
        return None

    active = [r for r in rows if r.get("isActive") is True]
    if not active:
        return None  # only delisted predecessors -> unclassified

    # If we know the live Alpaca name, prefer the active row that reconciles.
    if alpaca_name:
        reconciled = [r for r in active if names_reconcile(r.get("name"), alpaca_name)]
        if len(reconciled) >= 1:
            # If several reconcile (rare), the most-recently-updated wins.
            return _most_recent(reconciled)
        # No active row reconciles with the live company -> don't trust any.
        return None

    # No Alpaca name to reconcile against: only trust an unambiguous single
    # active row. Multiple active rows without a name anchor -> unclassified.
    if len(active) == 1:
        return active[0]
    return _most_recent(active)


def _most_recent(rows: list[dict]) -> dict:
    """Among rows, the one with the latest statementLastUpdated/dailyLastUpdated."""
    def _key(r: dict):
        return str(r.get("statementLastUpdated") or r.get("dailyLastUpdated") or "")
    return max(rows, key=_key)


def latest_meta_partition_key(
    s3_client, as_of: Optional[date] = None, meta_prefix: str = META_PREFIX
) -> str:
    """
    Return the S3 key of the most recent meta.json partition with
    date <= as_of (or the latest overall when as_of is None).

    `meta_prefix` lets smoke tests point at a test partition prefix.
    Raises FileNotFoundError when no partition exists.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{meta_prefix}/date="):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("meta.json"):
                keys.append(obj["Key"])
    if not keys:
        raise FileNotFoundError(
            f"No meta.json found under s3://{S3_BUCKET}/{meta_prefix}/"
        )
    keys.sort()  # date=YYYY-MM-DD sorts chronologically
    if as_of is not None:
        cutoff = f"{meta_prefix}/date={as_of.isoformat()}/meta.json"
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
    s3_client,
    as_of: Optional[date] = None,
    alpaca_names: Optional[dict[str, str]] = None,
    meta_prefix: str = META_PREFIX,
) -> tuple[dict[str, dict], Optional[date]]:
    """
    Load the latest meta partition and return ({TICKER_UPPER -> row}, partition_date).

    Rows are grouped by upper-cased + SYMBOL_REMAP-normalized ticker, then the
    ticker-reuse guard (`select_active_meta_row`) picks the single trustworthy
    active row per ticker (or drops the ticker -> unclassified). Pass
    `alpaca_names` ({TICKER_UPPER -> company_name}) to enable name reconciliation;
    without it, only unambiguous single-active-row tickers are trusted.
    `meta_prefix` lets smoke tests point at a test partition.
    """
    key = latest_meta_partition_key(s3_client, as_of, meta_prefix)
    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj["Body"].read())

    # Group all rows by canonical ticker (a reused symbol has multiple rows).
    grouped: dict[str, list[dict]] = {}
    for row in data:
        raw = row.get("ticker")
        if not raw:
            continue
        sym = str(raw).strip().upper()
        sym = SYMBOL_REMAP.get(sym, sym)
        grouped.setdefault(sym, []).append(row)

    names = alpaca_names or {}
    lookup: dict[str, dict] = {}
    for sym, rows in grouped.items():
        chosen = select_active_meta_row(rows, names.get(sym))
        if chosen is not None:
            lookup[sym] = chosen

    return lookup, partition_date_from_key(key)
