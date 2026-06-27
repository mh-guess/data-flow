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


# Boilerplate phrases stripped as a UNIT before tokenizing (so "American" inside
# the ADR phrase doesn't survive, while a real "American Express" is untouched).
_BOILERPLATE_PHRASES = [
    "american depositary shares", "american depositary receipts",
    "american depositary share", "american depositary receipt",
    "common stock", "ordinary shares", "ordinary share",
    "class a subordinate voting shares", "subordinate voting shares",
]


def _name_token_list(name: Optional[str]) -> list[str]:
    """Ordered significant lowercase tokens of a company name.

    Apostrophes are intra-word (Bally's -> ballys, Kohl's -> kohls) so possessives
    don't shatter into noise; ADR/boilerplate phrases are stripped as a unit; other
    punctuation separates. Corporate/share-class noise tokens are removed.
    """
    if not name:
        return []
    lowered = str(name).lower().replace("'", "").replace("`", "").replace("’", "")
    for phrase in _BOILERPLATE_PHRASES:
        lowered = lowered.replace(phrase, " ")
    cleaned = re.sub(r"[^a-z0-9 ]", " ", lowered)
    return [t for t in cleaned.split() if t and t not in _NAME_NOISE]


def _name_tokens(name: Optional[str]) -> set[str]:
    """Significant lowercase tokens of a company name, as a set."""
    return set(_name_token_list(name))


def _name_squash(name: Optional[str]) -> str:
    """Punctuation/space-free concatenation of significant tokens, in order.

    'argenx SE' and 'Argen X SE' both squash to 'argenx'; 'Conoco Phillips' and
    'ConocoPhillips' both to 'conocophillips'; 'Future Fuel' and 'Futurefuel' both
    to 'futurefuel' — so spacing/concatenation variance no longer defeats
    reconciliation. Order is PRESERVED (not sorted) so adjacency holds; single-
    character tokens (entity-suffix fragments like the N / V of "N.V.") are dropped.
    """
    return "".join(t for t in _name_token_list(name) if len(t) > 1)


def names_reconcile(tiingo_name: Optional[str], alpaca_name: Optional[str]) -> bool:
    """
    True if the Tiingo and Alpaca company names plausibly refer to the same firm.

    Primary: significant-token overlap after stripping corporate/share-class noise
    — at least one shared token AND the shorter token set >= 50% contained in the
    other. This handles "Unity Software Inc." vs "Unity Software Inc" and the common
    abbreviation case ("Greenbrier Companies"/"Greenbrier Cos", "CION Investment"/
    "CION Invt"), and rejects "US Airways Group" vs "Unity Software".

    Fallback (spacing/punctuation/possessive variance) — ONLY ADDITIVE, never
    overrides a primary accept: compare the squashed (punctuation- and space-free,
    order-preserving) significant-token strings. Accept when one is a prefix of the
    other (>= 6 chars) or their character-similarity ratio is >= 0.90. Recovers
    argenx/Argen X, Conoco Phillips/ConocoPhillips, Bally's/Ballys, SiriusXM/
    Sirius XM, Future Fuel/Futurefuel, ConocoPhillips — variants the token check
    misses because the brand is concatenated differently.
    """
    a = _name_tokens(tiingo_name)
    b = _name_tokens(alpaca_name)
    if not a or not b:
        return False
    overlap = a & b
    if overlap and len(overlap) / min(len(a), len(b)) >= 0.5:
        return True

    # Squashed-string fallback for spacing/concatenation/possessive variance.
    sa, sb = _name_squash(tiingo_name), _name_squash(alpaca_name)
    if len(sa) < 5 or len(sb) < 5:
        return False
    shorter, longer = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
    if len(shorter) >= 5 and longer.startswith(shorter):
        return True
    import difflib
    return difflib.SequenceMatcher(None, sa, sb).ratio() >= 0.90


# Outcome reasons returned alongside the chosen row, so the manifest can count
# the ticker-reuse blast radius as distinct buckets (not all lumped into 'none').
REASON_OK = "ok"
REASON_NO_META = "no_meta"               # ticker had no meta rows at all
REASON_ISACTIVE_DROPPED = "isactive_dropped"  # only isActive=False rows present
REASON_NAME_MISMATCH = "name_mismatch"   # active row(s) exist but none reconcile


def select_active_meta_row(
    rows: list[dict], alpaca_name: Optional[str] = None
) -> tuple[Optional[dict], str]:
    """
    Pick the correct meta row for one ticker from possibly-many candidates.

    Returns (chosen_row, reason). chosen_row is None when the ticker should be
    treated as UNCLASSIFIED; `reason` says WHY so the caller can count it:
      REASON_OK               — a trustworthy active row was chosen
      REASON_NO_META          — no rows for this ticker
      REASON_ISACTIVE_DROPPED — only delisted (isActive=False) rows present
      REASON_NAME_MISMATCH    — active row(s) exist but none reconcile with the
                                live Alpaca company name (likely a reused ticker
                                whose active row is a *different* listing)
    """
    if not rows:
        return None, REASON_NO_META

    active = [r for r in rows if r.get("isActive") is True]
    if not active:
        return None, REASON_ISACTIVE_DROPPED  # only delisted predecessors

    # If we know the live Alpaca name, prefer the active row that reconciles.
    if alpaca_name:
        reconciled = [r for r in active if names_reconcile(r.get("name"), alpaca_name)]
        if reconciled:
            # If several reconcile (rare), the most-recently-updated wins.
            return _most_recent(reconciled), REASON_OK
        # Active row(s) exist but none match the live company -> don't trust any.
        return None, REASON_NAME_MISMATCH

    # No Alpaca name to reconcile against: only trust an unambiguous single
    # active row. Multiple active rows without a name anchor -> most-recent wins.
    if len(active) == 1:
        return active[0], REASON_OK
    return _most_recent(active), REASON_OK


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
) -> tuple[dict[str, dict], Optional[date], dict[str, str]]:
    """
    Load the latest meta partition.

    Returns ({TICKER_UPPER -> chosen_row}, partition_date, drop_reasons), where
    drop_reasons maps {TICKER_UPPER -> REASON_*} for tickers that HAD meta rows
    but were dropped by the guard (REASON_ISACTIVE_DROPPED / REASON_NAME_MISMATCH).
    Tickers with no meta rows at all are simply absent from both maps (the caller
    treats absence as REASON_NO_META).

    Rows are grouped by upper-cased + SYMBOL_REMAP-normalized ticker, then the
    ticker-reuse guard (`select_active_meta_row`) picks the single trustworthy
    active row per ticker. Pass `alpaca_names` ({TICKER_UPPER -> company_name}) to
    enable name reconciliation; without it, only unambiguous single-active-row
    tickers are trusted. `meta_prefix` lets smoke tests point at a test partition.
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
    drop_reasons: dict[str, str] = {}
    for sym, rows in grouped.items():
        chosen, reason = select_active_meta_row(rows, names.get(sym))
        if chosen is not None:
            lookup[sym] = chosen
        else:
            drop_reasons[sym] = reason

    return lookup, partition_date_from_key(key), drop_reasons
