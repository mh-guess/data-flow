"""
Full-universe ticker source + batched Tiingo meta fetch.

Two helpers shared by the fundamentals flow (to broaden the nightly meta
partition to the full Alpaca tradable us_equity universe) and available to the
hedge_map flow:

  fetch_tradable_equity_universe()  -> list[str]
      All active, TRADABLE us_equity symbols from Alpaca get_all_assets
      (status=active, tradable=true, asset_class=us_equity), SYMBOL_REMAP-applied
      and de-duplicated. This is the ~13k universe, not the ~104 adhoc list.

  fetch_meta_batched(api_token, tickers, batch_size=...) -> list[dict]
      The Tiingo bulk /fundamentals/meta?tickers=CSV endpoint, BATCHED and
      concatenated. A single giant GET fails (URL length + payload); we chunk to
      ~250 symbols/call, sleep between calls for rate-limit headroom, and tolerate
      a failed batch (log + continue) so one bad chunk doesn't sink the run.

Both are import-light (requests only) so they can be reused without pulling in
the hedge_map flow.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

import requests

from hedge_map_flow import SYMBOL_REMAP

ALPACA_ASSETS_URL = "https://api.alpaca.markets/v2/assets"
TIINGO_META_URL = "https://api.tiingo.com/tiingo/fundamentals/meta"

# Tiingo meta batching. Empirically 200/call still 200-OKs but 250 returns 502s
# (long URLs from suffixed symbols like FOO.PRK / BAR.WS). 150 is a safe default
# with headroom; a failing batch is retried once at half size before being skipped.
META_BATCH_SIZE = 150
META_BATCH_SLEEP = 0.5  # seconds between batches


def fetch_tradable_equity_universe(alpaca_headers: dict[str, str]) -> list[str]:
    """Return all active+tradable us_equity symbols (SYMBOL_REMAP-applied, deduped)."""
    r = requests.get(
        ALPACA_ASSETS_URL,
        headers=alpaca_headers,
        params={"status": "active", "asset_class": "us_equity"},
        timeout=60,
    )
    r.raise_for_status()
    syms: list[str] = []
    seen: set[str] = set()
    for a in r.json():
        if not a.get("tradable", False):
            continue
        sym = SYMBOL_REMAP.get(a["symbol"], a["symbol"])
        if sym not in seen:
            seen.add(sym)
            syms.append(sym)
    return syms


def _tiingo_headers(api_token: str) -> dict[str, str]:
    return {"Content-Type": "application/json", "Authorization": f"Token {api_token}"}


def fetch_meta_batched(
    api_token: str,
    tickers: list[str],
    batch_size: int = META_BATCH_SIZE,
    sleep_s: float = META_BATCH_SLEEP,
    log: Optional[Callable[[str], None]] = None,
) -> list[dict]:
    """
    Fetch Tiingo company meta for `tickers` in batches and concatenate the rows.

    Tiingo's meta endpoint is lowercase-ticker based; we lowercase per-batch. A
    batch that errors is logged and skipped (its tickers simply get no meta and
    fall through to SPY). Returns the concatenated list of meta row dicts (which
    may contain multiple rows per reused ticker — the loader's active-row guard
    resolves that downstream).
    """
    _log = log or (lambda _m: None)
    headers = _tiingo_headers(api_token)
    batches = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
    _log(f"Fetching Tiingo meta for {len(tickers)} tickers in {len(batches)} batches "
         f"of <= {batch_size}...")

    rows: list[dict] = []
    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(sleep_s)
        got = _fetch_one_batch(batch, headers, sleep_s, _log)
        rows.extend(got)
        if (i + 1) % 10 == 0 or (i + 1) == len(batches):
            _log(f"  meta batch {i+1}/{len(batches)}: {len(rows)} rows so far")

    _log(f"Tiingo meta fetched: {len(rows)} rows for {len(tickers)} tickers "
         f"(rows may exceed tickers due to reused symbols; ETFs/non-equity return none)")
    return rows


def _fetch_one_batch(
    batch: list[str],
    headers: dict[str, str],
    sleep_s: float,
    log: Callable[[str], None],
) -> list[dict]:
    """
    Fetch one batch; on failure retry once by splitting the batch in half.

    A persistent failure on a single-ticker batch is logged and dropped (those
    tickers simply get no meta and fall through to SPY). Splitting recovers from
    URL-length / transient-502 issues that only affect large batches.
    """
    params = {"tickers": ",".join(t.lower() for t in batch)}
    try:
        resp = requests.get(TIINGO_META_URL, headers=headers, params=params, timeout=120)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:  # noqa: BLE001
        if len(batch) <= 1:
            log(f"  meta single-ticker batch {batch} FAILED ({exc}); skipping")
            return []
        mid = len(batch) // 2
        log(f"  meta batch of {len(batch)} FAILED ({exc}); retrying as 2 halves")
        time.sleep(sleep_s)
        left = _fetch_one_batch(batch[:mid], headers, sleep_s, log)
        time.sleep(sleep_s)
        right = _fetch_one_batch(batch[mid:], headers, sleep_s, log)
        return left + right
