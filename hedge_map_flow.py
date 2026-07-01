"""
Nightly Hedge-Map ETL — Part A of MF Drift Hedge Overlay (v2: HEURISTIC)

Produces a partitioned parquet on S3 that maps every eligible US equity ticker
to a 3-tier hedge ladder for the next trading session:

  rank1 = industry pure-play ETF  (heuristic crosswalk; demoted on a bad beta)
  rank2 = sector SPDR ETF
  rank3 = SPY

v2 replaces v1's max-R²-over-57-ETFs selection with a deterministic
sector/industry → ETF crosswalk (`hedge_crosswalk.py`) driven by Tiingo `meta`
classification. Beta still SIZES each short via `beta_r2_pair()` vs the assigned
ETF (one regression per assigned ETF, not 57). See project_docs/hedge_overlay.

S3 outputs (the apex team depends on the dated partitions; latest/ is additive):
  s3://mh-guess-data/hedge_map/effective_date=YYYY-MM-DD/data.parquet   (point-in-time)
  s3://mh-guess-data/hedge_map/manifests/effective_date=YYYY-MM-DD/manifest.json
  s3://mh-guess-data/hedge_map/latest/data.parquet           (stable key, overwritten each run)
  s3://mh-guess-data/hedge_map/manifests/latest/manifest.json

Failure mode: if the dated put succeeds but the latest/ put fails (and all task
retries are exhausted), latest/ silently points at the prior run's data until the
next successful run. The dated partition is always intact; only the stable-key copy
may lag. The task raises and logs logger.error before propagating so alerting fires.

Sidecar manifest fields: effective_date, as_of_date, universe_size,
  eligible_count, coverage_pct, etf_set_version, crosswalk_version, row_count,
  classification_snapshot_date, and crosswalk coverage stats.

Run manually:
  python hedge_map_flow.py

Schedule: 6 PM ET weekdays (after market close), defined in prefect.yaml.
"""

from __future__ import annotations

import io
import json
import os
import time
import warnings

# Suppress numpy divide-by-zero in corrcoef for degenerate (constant) return series.
# These arise for illiquid / non-trading symbols and are harmless — the var()==0 guard
# in beta_r2_pair returns NaN before the coefficient is used.
warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in divide")
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd
import requests
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials

# hedge_crosswalk has no dependency on this module, so importing it at top level
# is cycle-free. hedge_selection / hedge_classification DO import from this module,
# so they are imported lazily inside the flow body to avoid a circular import.
from hedge_crosswalk import CROSSWALK_VERSION, all_referenced_etfs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

S3_BUCKET = "mh-guess-data"
HEDGE_MAP_PREFIX = "hedge_map"
UNIVERSE_SNAPSHOT_PREFIX = "hedge_map/universe_snapshots"

# ETF candidate set version tag — retained for the v1 max-R² helpers / parity test.
ETF_SET_VERSION = "v1-57etf-20260624"

# The research 57 ETFs (from etf_candidates.csv, in canonical order).
# v2 no longer ranks across all of these per stock; the set is still used by the
# v1 helpers (compute_hedge_map / parity_check.py) and to resolve ETF shortability.
ETF_CANDIDATES: list[str] = [
    "SPY", "VOO", "VTI", "QQQ", "IWM", "MDY", "IWF", "IWD", "VUG", "MTUM", "RSP",
    "XLK", "XLF", "XLV", "XLY", "XLP", "XLE", "XLI", "XLB", "XLU", "XLRE", "XLC",
    "SMH", "SOXX", "IGV", "XSW", "SKYY", "WCLD", "FDN", "PNQI", "XBI", "IBB",
    "IHI", "XHE", "ITA", "ITB", "XHB", "KRE", "KBE", "XOP", "OIH", "JETS", "IYT",
    "XRT", "IBUY", "ONLN", "TAN", "ICLN", "KWEB", "EEM", "EFA",
    "ARKK", "ARKW", "ARKG", "ARKF", "IPO", "FFTY",
]

# Ticker canonicalization from lib.py.
SYMBOL_REMAP: dict[str, str] = {
    "MOGA": "MOG.A",
    "BRKB": "BRK.B",
    "SIVB.Q": "SIVB",
}

ALPACA_BASE = "https://data.alpaca.markets/v2/stocks"

# Alpaca Prefect Secret block names (set in Prefect Cloud; env vars used as fallback
# for local runs via run_local.py which never calls Secret.load()).
ALPACA_KEY_BLOCK = "alpaca-api-key"
ALPACA_SECRET_BLOCK = "alpaca-api-secret"

# Module-level credential storage populated by _init_alpaca_creds() at flow start.
# Never print or log these values.
_alpaca_key: str = ""
_alpaca_secret: str = ""

# Selection / eligibility parameters.
MIN_LISTING_DAYS = 90       # stock must be listed ≥ 90 calendar days before as_of
MIN_N_OBS = 60              # minimum overlapping returns for a valid beta
LOOKBACK_TRADING_DAYS = 60  # trailing window for beta regression
ADV_LOOKBACK_DAYS = 30      # trailing window for $ADV computation
ADV_MIN_USD = 25e6          # $25M/day minimum ETF liquidity
TOP_N_HEDGES = 3            # store top-3 hedges per ticker

# Alpaca SIP rate-limit headroom: 200 req/min on free SIP tier.
# Multi-symbol bars endpoint supports up to 100 symbols per call.
ALPACA_BATCH_SIZE = 100     # symbols per multi-bar request
ALPACA_RATE_DELAY = 0.35    # seconds between batches (~170 req/min, under 200)
ALPACA_RETRY_DELAYS = [10, 30, 60]  # 429/5xx back-off seconds
ALPACA_BROKER_BASE = "https://paper-api.alpaca.markets/v2"

# ETFs checked to verify that as_of bars have settled before committing to the date.
_SENTINEL_ETFS = ["SPY", "QQQ", "IWM"]

# Exchange calendar: populated by _init_trading_calendar() at flow start.
# Helpers fall back to weekday logic when empty (unit tests, offline mode).
_TRADING_DAYS: frozenset[date] = frozenset()


# ---------------------------------------------------------------------------
# Credential management
# ---------------------------------------------------------------------------

def _init_alpaca_creds(from_prefect_blocks: bool = True) -> None:
    """
    Populate module-level Alpaca credentials.

    In deployed Prefect flows (`from_prefect_blocks=True`): loads from
    `Secret.load(ALPACA_KEY_BLOCK)` / `Secret.load(ALPACA_SECRET_BLOCK)`.

    Fallback (or when `from_prefect_blocks=False`): reads from environment variables
    `ALPACA_API_KEY` / `ALPACA_API_SECRET` — used by run_local.py and CI.

    Call exactly once at flow start before any Alpaca request is made.
    Never log or print the values.
    """
    global _alpaca_key, _alpaca_secret

    if from_prefect_blocks:
        try:
            _alpaca_key = Secret.load(ALPACA_KEY_BLOCK).get()
            _alpaca_secret = Secret.load(ALPACA_SECRET_BLOCK).get()
            return
        except Exception as exc:
            # Block not found or Prefect Cloud unavailable — fall through to env vars.
            # This makes local development (prefect server or no server) work without
            # requiring block setup.
            import logging
            logging.getLogger(__name__).warning(
                f"Alpaca Prefect blocks unavailable ({exc}); falling back to env vars."
            )

    _alpaca_key = os.environ["ALPACA_API_KEY"]
    _alpaca_secret = os.environ["ALPACA_API_SECRET"]


# ---------------------------------------------------------------------------
# Low-level Alpaca helpers
# ---------------------------------------------------------------------------

def _alpaca_headers() -> dict[str, str]:
    """Return Alpaca auth headers. Requires _init_alpaca_creds() to have been called."""
    if not _alpaca_key or not _alpaca_secret:
        raise RuntimeError(
            "_alpaca_headers() called before _init_alpaca_creds(). "
            "Call _init_alpaca_creds() at flow start."
        )
    return {
        "APCA-API-KEY-ID": _alpaca_key,
        "APCA-API-SECRET-KEY": _alpaca_secret,
    }


def _fetch_alpaca_calendar(start: date, end: date) -> frozenset[date]:
    """
    Fetch NYSE/US market open sessions from Alpaca broker calendar API.

    Returns a frozenset of date objects for every trading session in [start, end].
    Uses two attempts with a 5-second delay before propagating the error.
    """
    url = f"{ALPACA_BROKER_BASE}/calendar"
    params = {"start": start.isoformat(), "end": end.isoformat()}
    for attempt in range(2):
        try:
            r = requests.get(url, headers=_alpaca_headers(), params=params, timeout=30)
            r.raise_for_status()
            sessions = r.json()  # list of {"date": "YYYY-MM-DD", "open": "HH:MM", "close": "HH:MM"}
            return frozenset(date.fromisoformat(s["date"]) for s in sessions)
        except Exception:
            if attempt == 0:
                time.sleep(5)
            else:
                raise
    return frozenset()  # unreachable


def _init_trading_calendar(start: date, end: date) -> None:
    """
    Populate the module-level _TRADING_DAYS cache from Alpaca's calendar API.

    Call once at flow/script start after _init_alpaca_creds(). All trading-day
    helpers (is_trading_day, _next_trading_day, etc.) use this cache and fall
    back to weekday logic when it's empty (unit tests, offline mode).
    """
    global _TRADING_DAYS
    _TRADING_DAYS = _fetch_alpaca_calendar(start, end)


def _is_trading_day(d: date) -> bool:
    """True if d is a known NYSE session, or a weekday when calendar is not loaded."""
    if _TRADING_DAYS:
        return d in _TRADING_DAYS
    return d.weekday() < 5  # Mon–Fri fallback for tests / offline use


def _get(url: str, params: dict, retries: int = 3) -> dict:
    """HTTP GET with retry on 429 / 5xx."""
    for attempt in range(retries + 1):
        r = requests.get(url, headers=_alpaca_headers(), params=params, timeout=60)
        if r.status_code == 429:
            delay = ALPACA_RETRY_DELAYS[min(attempt, len(ALPACA_RETRY_DELAYS) - 1)]
            time.sleep(delay)
            continue
        if r.status_code >= 500:
            delay = ALPACA_RETRY_DELAYS[min(attempt, len(ALPACA_RETRY_DELAYS) - 1)]
            time.sleep(delay)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return {}  # unreachable


# ---------------------------------------------------------------------------
# Universe / Asset tasks
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=10)
def fetch_universe() -> pd.DataFrame:
    """
    Pull all active, tradable US equities from Alpaca get_all_assets.

    Returns DataFrame with columns: symbol, shortable, easy_to_borrow, first_listed.
    Note: Alpaca's asset object does not expose an IPO/listing date; we use the
    first available daily bar date (fetched separately) as the proxy. The `first_listed`
    column is therefore populated downstream when bars are loaded.
    """
    logger = get_run_logger()
    logger.info("Fetching universe from Alpaca get_all_assets...")

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
        # Apply SYMBOL_REMAP canonicalization.
        sym = SYMBOL_REMAP.get(sym, sym)
        rows.append({
            "symbol": sym,
            "name": a.get("name", ""),  # live company name, for meta name-reconciliation
            "shortable": bool(a.get("shortable", False)),
            "easy_to_borrow": bool(a.get("easy_to_borrow", False)),
        })

    df = pd.DataFrame(rows).drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    logger.info(f"Universe: {len(df)} active tradable US equities")
    return df


@task(retries=2, retry_delay_seconds=10)
def persist_universe_snapshot(
    universe: pd.DataFrame,
    as_of: date,
    aws_credentials: AwsCredentials,
) -> str:
    """Write universe snapshot to S3 as parquet for audit trail."""
    logger = get_run_logger()
    key = f"{UNIVERSE_SNAPSHOT_PREFIX}/as_of={as_of.isoformat()}/universe.parquet"

    snap = universe.copy()
    snap["as_of_date"] = as_of

    buf = io.BytesIO()
    snap.to_parquet(buf, compression="snappy", index=False)
    s3 = aws_credentials.get_boto3_session().client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/x-parquet",
    )
    logger.info(f"Universe snapshot: s3://{S3_BUCKET}/{key}")
    return key


# ---------------------------------------------------------------------------
# Daily bar fetch — multi-symbol batched
# ---------------------------------------------------------------------------

def _fetch_multi_bars_page(
    symbols: list[str],
    start_str: str,
    end_str: str,
) -> dict[str, list[dict]]:
    """
    Fetch split-adjusted daily bars for up to ALPACA_BATCH_SIZE symbols in one call.

    Returns dict {symbol -> [bar_dict, ...]}.
    Handles pagination internally.
    """
    url = f"{ALPACA_BASE}/bars"
    base_params: dict = {
        "symbols": ",".join(symbols),
        "timeframe": "1Day",
        "start": start_str,
        "end": end_str,
        "adjustment": "split",
        "feed": "sip",
        "limit": 10000,
    }
    all_bars: dict[str, list] = {}
    page_token: Optional[str] = None

    while True:
        params = dict(base_params)
        if page_token:
            params["page_token"] = page_token
        j = _get(url, params)
        for sym, bars in (j.get("bars") or {}).items():
            all_bars.setdefault(sym, []).extend(bars)
        page_token = j.get("next_page_token")
        if not page_token:
            break

    return all_bars


def _bars_to_df(raw_bars: list[dict], symbol: str) -> pd.DataFrame:
    """Convert Alpaca bar dicts to a tidy DataFrame with date-string column `d`."""
    if not raw_bars:
        return pd.DataFrame(columns=["d", "close", "volume"])
    df = pd.DataFrame(raw_bars)
    # Alpaca daily bar fields: t, o, h, l, c, v, vw, n
    df["d"] = pd.to_datetime(df["t"], utc=True).dt.tz_convert("America/New_York").dt.date.astype(str)
    df = df.rename(columns={"c": "close", "v": "volume"})
    df = df[["d", "close", "volume"]].sort_values("d").reset_index(drop=True)
    return df


@task(retries=2, retry_delay_seconds=30)
def fetch_daily_bars_batch(
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    """
    Fetch split-adjusted daily bars for a batch of symbols (≤ ALPACA_BATCH_SIZE).

    Returns {symbol -> DataFrame(d, close, volume)}.
    """
    logger = get_run_logger()
    start_str = f"{start_date.isoformat()}T00:00:00Z"
    end_str = f"{end_date.isoformat()}T23:59:59Z"

    raw = _fetch_multi_bars_page(symbols, start_str, end_str)
    result: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        bars = raw.get(sym, [])
        result[sym] = _bars_to_df(bars, sym)

    got = sum(1 for df in result.values() if not df.empty)
    logger.info(f"Bars fetched for {got}/{len(symbols)} symbols in batch")
    return result


# ---------------------------------------------------------------------------
# Bar fetch orchestration — full universe, batched
# ---------------------------------------------------------------------------

def fetch_all_daily_bars(
    symbols: list[str],
    start_date: date,
    end_date: date,
) -> dict[str, pd.DataFrame]:
    """
    Fetch daily bars for the full symbol list in ALPACA_BATCH_SIZE batches.

    Respects rate-limit headroom between batches. Returns {symbol -> DataFrame}.
    This is called from within the flow; each batch is a retryable Prefect task.
    """
    logger = get_run_logger()
    logger.info(f"Fetching daily bars for {len(symbols)} symbols from {start_date} to {end_date}...")
    logger.info(f"  Batch size: {ALPACA_BATCH_SIZE} | Est. batches: {len(symbols) // ALPACA_BATCH_SIZE + 1}")

    all_bars: dict[str, pd.DataFrame] = {}
    batches = [symbols[i:i + ALPACA_BATCH_SIZE] for i in range(0, len(symbols), ALPACA_BATCH_SIZE)]

    for i, batch in enumerate(batches):
        if i > 0:
            time.sleep(ALPACA_RATE_DELAY)
        batch_result = fetch_daily_bars_batch(batch, start_date, end_date)
        all_bars.update(batch_result)
        if (i + 1) % 10 == 0 or (i + 1) == len(batches):
            logger.info(f"  Batch {i+1}/{len(batches)} done ({len(all_bars)} symbols accumulated)")

    return all_bars


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

def compute_eligibility(
    universe: pd.DataFrame,
    bars: dict[str, pd.DataFrame],
    as_of: date,
) -> pd.DataFrame:
    """
    Filter universe to eligible tickers: first bar ≥ 90 calendar days before as_of.

    Returns DataFrame with columns from universe plus `first_bar_date` and `eligible`.
    """
    rows = []
    cutoff = as_of - timedelta(days=MIN_LISTING_DAYS)

    for _, row in universe.iterrows():
        sym = row["symbol"]
        df = bars.get(sym, pd.DataFrame())
        if df.empty:
            first_bar = None
            eligible = False
        else:
            first_bar = pd.to_datetime(df["d"].min()).date()
            eligible = first_bar <= cutoff

        rows.append({
            **row.to_dict(),
            "first_bar_date": first_bar,
            "eligible": eligible,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Beta / R² computation — vectorized
# ---------------------------------------------------------------------------

def _compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Add pct_change `ret` and `dollar_vol` columns to a bars DataFrame."""
    out = df.copy()
    out["d"] = pd.to_datetime(out["d"]).dt.date
    out = out.sort_values("d").reset_index(drop=True)
    out["ret"] = out["close"].pct_change()
    out["dollar_vol"] = out["close"] * out["volume"]
    return out.dropna(subset=["ret"])


def beta_r2_pair(
    stock_ret: pd.DataFrame,
    etf_ret: pd.DataFrame,
    as_of: date,
    lookback: int = LOOKBACK_TRADING_DAYS,
) -> tuple[float, float, int]:
    """
    OLS beta + R² of stock daily returns on ETF daily returns.

    Window: trailing `lookback` trading days with `d <= as_of` (inclusive).
    `as_of` is the last close date to include, i.e. the close whose bars are
    already available when this function is called.

    Callers set `as_of` to the last completed close, making `as_of_date` in
    the output parquet an accurate label for "close through which betas were
    computed." This differs from lib.py::beta_r2 which takes `as_of_date` as
    the event/pick date and ends the window at `as_of_date - 1`; the parity
    harness compensates by passing `prior_trading_day(pick_date)` as `as_of`.

    Returns (beta, r2, n_obs). Returns (nan, nan, n) if n < MIN_N_OBS or zero variance.
    """
    end = pd.Timestamp(as_of).date()  # inclusive: window uses d <= as_of
    s = stock_ret[stock_ret["d"] <= end].tail(lookback)
    e = etf_ret[etf_ret["d"] <= end].tail(lookback)
    j = pd.merge(s[["d", "ret"]], e[["d", "ret"]], on="d", suffixes=("_s", "_e")).dropna()
    n = len(j)
    if n < MIN_N_OBS or j["ret_e"].var() == 0:
        return (np.nan, np.nan, n)
    cov = np.cov(j["ret_s"], j["ret_e"])[0, 1]
    beta = cov / np.var(j["ret_e"], ddof=1)
    r = np.corrcoef(j["ret_s"], j["ret_e"])[0, 1]
    return (float(beta), float(r * r), n)


def compute_adv(df: pd.DataFrame, as_of: date, n_days: int = ADV_LOOKBACK_DAYS) -> float:
    """Trailing-N-day $ADV with `d <= as_of` (inclusive). `as_of` is the last close to include."""
    end = pd.Timestamp(as_of).date()  # inclusive
    w = df[df["d"] <= end].tail(n_days)
    return float(w["dollar_vol"].mean()) if len(w) else np.nan


def compute_hedge_map(
    eligible: pd.DataFrame,
    all_bars: dict[str, pd.DataFrame],
    etf_meta: dict[str, dict],  # {etf_sym -> {shortable, easy_to_borrow}}
    as_of: date,
    effective_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Compute beta/R² for every eligible stock × every ETF candidate, then select top-3.

    Args:
        as_of: Last close date whose bars are used for beta/ADV computation.
               In normal operation this equals the most recent completed session.
               When bars are lagged (holiday / settlement delay), the caller passes
               the actual last settled close here while supplying the correct
               effective_date explicitly.
        effective_date: The upcoming session this partition is for. Defaults to
               _next_trading_day(as_of). The caller must pass this explicitly when
               as_of != the session that determines effective_date (bar-lag case).

    etf_meta carries shortability flags (from universe/asset pull for ETF symbols).
    Returns the hedge_map DataFrame (schema-conforming).
    """
    if effective_date is None:
        effective_date = _next_trading_day(as_of)

    # Pre-compute returns for all ETF candidates; skip those with no bars.
    etf_rets: dict[str, pd.DataFrame] = {}
    etf_advs: dict[str, float] = {}
    for etf in ETF_CANDIDATES:
        df = all_bars.get(etf, pd.DataFrame())
        if df.empty:
            continue
        r = _compute_returns(df)
        etf_rets[etf] = r
        etf_advs[etf] = compute_adv(r, as_of)

    rows: list[dict] = []
    elig_syms = eligible[eligible["eligible"]]["symbol"].tolist()

    for sym in elig_syms:
        df = all_bars.get(sym, pd.DataFrame())
        if df.empty:
            continue
        sret = _compute_returns(df)
        stock_adv = compute_adv(sret, as_of)

        cands: list[tuple[str, float, float, int]] = []
        for etf, eret in etf_rets.items():
            beta, r2, n_obs = beta_r2_pair(sret, eret, as_of)
            if np.isfinite(r2) and n_obs >= MIN_N_OBS:
                cands.append((etf, beta, r2, n_obs))

        if not cands:
            continue

        # Apply liquid + shortable screen: ETF must be liquid AND shortable at as_of.
        meta = etf_meta  # shortable/easy_to_borrow per ETF
        eligible_cands = [
            c for c in cands
            if etf_advs.get(c[0], 0) >= ADV_MIN_USD
            and meta.get(c[0], {}).get("shortable", False)
        ]
        if not eligible_cands:
            continue

        # Rank by R² descending; take top-3.
        eligible_cands.sort(key=lambda x: -x[2])
        top3 = eligible_cands[:TOP_N_HEDGES]

        for rank, (etf, beta, r2, n_obs) in enumerate(top3, 1):
            m = meta.get(etf, {})
            rows.append({
                "effective_date": effective_date,
                "as_of_date": as_of,
                "ticker": sym,
                "rank": rank,
                "hedge_etf": etf,
                "beta": beta,
                "r2": r2,
                "n_obs": n_obs,
                "etf_shortable": bool(m.get("shortable", False)),
                "etf_easy_to_borrow": bool(m.get("easy_to_borrow", False)),
                "etf_adv_usd_30d": etf_advs.get(etf, np.nan),
                "stock_adv_usd_30d": stock_adv,
                "selection_basis": "liquid_top_r2",
            })

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        # Return schema-conforming empty DataFrame.
        df_out = pd.DataFrame(columns=[
            "effective_date", "as_of_date", "ticker", "rank", "hedge_etf",
            "beta", "r2", "n_obs", "etf_shortable", "etf_easy_to_borrow",
            "etf_adv_usd_30d", "stock_adv_usd_30d", "selection_basis",
        ])
    return df_out


def _check_bar_availability(
    all_bars: dict[str, pd.DataFrame],
    as_of_dates: list[date],
    skip: bool = False,
) -> dict[date, date]:
    """
    Bar availability guard: verify that the expected close bars have settled by
    checking all sentinel ETFs (SPY, QQQ, IWM).

    Uses min() over sentinel last-bar dates so that a close is only considered
    settled when EVERY sentinel has it — the laggard, not the leader, determines
    availability. If any sentinel is missing entirely it is treated as not settled.

    KEY INVARIANT: effective_date must never rewind due to bar lag. The returned
    dict maps {run_as_of: beta_as_of} only for dates where beta_as_of < run_as_of;
    callers use run_as_of for effective_date and beta_as_of for the beta/ADV window.

    Args:
        all_bars: symbol → bar DataFrame (d column stored as str by _bars_to_df).
        as_of_dates: list of dates to check, most-recent-first.
        skip: when True (explicit --as-of override), returns {} immediately.

    Returns:
        {run_as_of: beta_as_of} for every as_of date whose bars haven't fully settled.
        Empty dict when all bars are current or skip=True.
    """
    import logging
    log = logging.getLogger(__name__)

    if skip:
        return {}

    # Collect the last available bar date for each sentinel.
    # min() requires ALL sentinels present; any missing sentinel is treated as not settled.
    sentinel_last: list[date] = []
    for etf in _SENTINEL_ETFS:
        df = all_bars.get(etf)
        if df is None or df.empty:
            # Sentinel missing — treat as fully lagged (use as_of_dates[-1] as floor).
            log.warning(f"Bar availability guard: sentinel {etf} has no bar data.")
            sentinel_last.append(date.min)
        else:
            sentinel_last.append(date.fromisoformat(df["d"].max()))

    # The close is settled only when the SLOWEST sentinel has it (min, not max).
    actual_last_bar: date = min(sentinel_last)
    if actual_last_bar == date.min:
        # At least one sentinel entirely absent — treat as not settled.
        actual_last_bar = _prior_trading_day(as_of_dates[0])

    overrides: dict[date, date] = {}
    for d in as_of_dates:
        if actual_last_bar < d:
            overrides[d] = actual_last_bar

    if overrides:
        log.warning(
            f"Bar availability guard: slowest sentinel last bar is {actual_last_bar}, "
            f"expected {as_of_dates[0]}. Beta window capped at {actual_last_bar} for "
            f"{len(overrides)} date(s); effective_date(s) unchanged (upcoming session). "
            "Likely cause: market holiday or bars not yet settled."
        )

    return overrides


def _next_trading_day(d: date) -> date:
    """First trading session after d. Uses exchange calendar when loaded."""
    nxt = d + timedelta(days=1)
    while not _is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


# ---------------------------------------------------------------------------
# S3 write helpers
# ---------------------------------------------------------------------------

def _latest_key(dated_key: str) -> str:
    """
    Derive the ``latest/`` S3 key from a dated partition key.

    Replaces the ``effective_date=YYYY-MM-DD/`` path segment with ``latest/``
    so consumers can read a stable, fixed-name key without date resolution.
    Raises ``ValueError`` if no ``effective_date=`` segment is found — this
    prevents silently writing to the wrong key on a malformed path.

    Examples::

        hedge_map/effective_date=2024-01-02/data.parquet
        → hedge_map/latest/data.parquet

        hedge_map/manifests/effective_date=2024-01-02/manifest.json
        → hedge_map/manifests/latest/manifest.json
    """
    parts = dated_key.split("/")
    new_parts = ["latest" if p.startswith("effective_date=") else p for p in parts]
    if new_parts == parts:
        raise ValueError(f"no effective_date= segment in key: {dated_key!r}")
    return "/".join(new_parts)


# ---------------------------------------------------------------------------
# S3 write tasks
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=10)
def write_hedge_map_to_s3(
    hedge_map: pd.DataFrame,
    aws_credentials: AwsCredentials,
    publish_latest: bool = True,
) -> str:
    """
    Write partitioned parquet to S3.

    Partition key: effective_date=YYYY-MM-DD/
    When ``publish_latest=True`` (default) also writes a byte-identical copy to the
    ``latest/`` key so consumers can read a stable address without date resolution.
    Pass ``publish_latest=False`` for backfill iterations that are not the newest
    effective_date — only the newest partition should claim the stable key.
    The returned URI is the dated partition.
    """
    logger = get_run_logger()
    if hedge_map.empty:
        raise ValueError("hedge_map is empty — nothing to write")

    effective_date = hedge_map["effective_date"].iloc[0]
    if hasattr(effective_date, "isoformat"):
        date_str = effective_date.isoformat()
    else:
        date_str = str(effective_date)

    key = f"{HEDGE_MAP_PREFIX}/effective_date={date_str}/data.parquet"

    # Cast columns to correct dtypes before writing.
    hm = hedge_map.copy()
    hm["effective_date"] = pd.to_datetime(hm["effective_date"]).dt.date
    hm["as_of_date"] = pd.to_datetime(hm["as_of_date"]).dt.date
    hm["ticker"] = hm["ticker"].astype(str)
    hm["rank"] = hm["rank"].astype(int)
    hm["hedge_etf"] = hm["hedge_etf"].astype(str)
    hm["beta"] = hm["beta"].astype(float)
    hm["r2"] = hm["r2"].astype(float)
    hm["n_obs"] = hm["n_obs"].astype(int)
    hm["etf_shortable"] = hm["etf_shortable"].astype(bool)
    hm["etf_easy_to_borrow"] = hm["etf_easy_to_borrow"].astype(bool)
    hm["etf_adv_usd_30d"] = hm["etf_adv_usd_30d"].astype(float)
    hm["stock_adv_usd_30d"] = hm["stock_adv_usd_30d"].astype(float)
    hm["selection_basis"] = hm["selection_basis"].astype(str)
    # v2 classification columns. classification_sector/industry may be None for
    # unclassified stocks — keep them as nullable object strings (no None→"None").
    hm["industry_source"] = hm["industry_source"].astype(str)
    hm["classification_sector"] = hm["classification_sector"].astype("string")
    hm["classification_industry"] = hm["classification_industry"].astype("string")
    hm["classification_source"] = hm["classification_source"].astype(str)

    buf = io.BytesIO()
    hm.to_parquet(buf, compression="snappy", index=False)

    s3 = aws_credentials.get_boto3_session().client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/x-parquet",
    )

    uri = f"s3://{S3_BUCKET}/{key}"
    logger.info(f"Hedge map written: {uri} ({len(hm)} rows)")

    if publish_latest:
        # Publish stable latest/ copy. Re-put the same in-memory bytes (requires only
        # s3:PutObject, no s3:GetObject; byte-identical to the dated object).
        latest = _latest_key(key)
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=latest,
                Body=buf.getvalue(),
                ContentType="application/x-parquet",
            )
        except Exception:
            logger.error(
                f"latest/ put failed for s3://{S3_BUCKET}/{latest}; dated partition "
                "is intact but latest/ may point at the prior run until the next "
                "successful run."
            )
            raise
        logger.info(f"Hedge map latest:  s3://{S3_BUCKET}/{latest}")

    return uri


@task(retries=2, retry_delay_seconds=10)
def write_run_manifest(
    as_of: date,
    effective_date: date,
    universe_size: int,
    eligible_count: int,
    covered_tickers: int,
    hedge_map_rows: int,
    aws_credentials: AwsCredentials,
    classification_snapshot_date: Optional[date] = None,
    coverage_stats: Optional[dict] = None,
    publish_latest: bool = True,
) -> str:
    """
    Write sidecar JSON manifest for this run (v2: + classification + crosswalk stats).

    When ``publish_latest=True`` (default) also writes a byte-identical copy to the
    ``latest/`` key. Pass ``publish_latest=False`` for backfill iterations that are
    not the newest effective_date. The returned URI is the dated partition.
    """
    logger = get_run_logger()

    # coverage_pct = fraction of eligible tickers with at least one valid hedge (rank=1 row).
    coverage_pct = (covered_tickers / eligible_count * 100.0
                    if eligible_count > 0 else 0.0)

    manifest = {
        "effective_date": effective_date.isoformat(),
        "as_of_date": as_of.isoformat(),
        "universe_size": universe_size,
        "eligible_count": eligible_count,
        "coverage_pct": round(coverage_pct, 2),
        "etf_set_version": ETF_SET_VERSION,
        "crosswalk_version": CROSSWALK_VERSION,
        "classification_snapshot_date": (
            classification_snapshot_date.isoformat()
            if classification_snapshot_date else None
        ),
        "crosswalk_coverage": coverage_stats or {},
        "row_count": hedge_map_rows,
        "run_utc": datetime.now(timezone.utc).isoformat(),
    }

    manifest_bytes = json.dumps(manifest, indent=2).encode()

    key = f"{HEDGE_MAP_PREFIX}/manifests/effective_date={effective_date.isoformat()}/manifest.json"
    s3 = aws_credentials.get_boto3_session().client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=manifest_bytes,
        ContentType="application/json",
    )

    uri = f"s3://{S3_BUCKET}/{key}"
    logger.info(f"Manifest written: {uri}")
    logger.info(json.dumps(manifest, indent=2))

    if publish_latest:
        # Publish stable latest/ copy. Re-put the same in-memory bytes (requires only
        # s3:PutObject, no s3:GetObject; byte-identical to the dated object).
        latest = _latest_key(key)
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=latest,
                Body=manifest_bytes,
                ContentType="application/json",
            )
        except Exception:
            logger.error(
                f"latest/ put failed for s3://{S3_BUCKET}/{latest}; dated partition "
                "is intact but latest/ may point at the prior run until the next "
                "successful run."
            )
            raise
        logger.info(f"Manifest latest:  s3://{S3_BUCKET}/{latest}")

    return uri


# ---------------------------------------------------------------------------
# Coverage stats (v2)
# ---------------------------------------------------------------------------

def compute_coverage_stats(
    hedge_map: pd.DataFrame,
    eligible_count: int,
    drop_reasons: Optional[dict] = None,
    eligible_syms: Optional[list] = None,
) -> dict:
    """
    Crosswalk coverage stats over the rank=1 rows (one per covered ticker).

    Reports, over rank=1 rows:
      industry_source     pure_play / sector_fallback
      selection_basis     heuristic_industry / heuristic_sector / spy_fallback
                          (spy_fallback = quality-gate beta fallback)
      classification_source  tiingo / sic / no_meta / isactive_dropped / name_mismatch
        — the last three are the DISTINCT ticker-reuse blast-radius buckets
          (kept separate, never lumped into one 'none').

    Plus `guard_drops` — the raw guard outcome over the ELIGIBLE universe
    (independent of whether the stock got a hedge row), so the reuse hazard is
    observable even for names that fell out on the SPY beta gate.
    """
    if hedge_map.empty:
        base = {"covered_tickers": 0, "eligible_count": eligible_count}
    else:
        r1 = hedge_map[hedge_map["rank"] == 1]
        covered = int(r1["ticker"].nunique())

        def _pct(n: int) -> float:
            return round(n / covered * 100.0, 2) if covered else 0.0

        src_counts = r1["industry_source"].value_counts().to_dict()
        basis_counts = r1["selection_basis"].value_counts().to_dict()
        cls_counts = r1["classification_source"].value_counts().to_dict()

        base = {
            "covered_tickers": covered,
            "eligible_count": eligible_count,
            "coverage_pct_of_eligible": (
                round(covered / eligible_count * 100.0, 2) if eligible_count else 0.0
            ),
            "industry_source": {
                "pure_play": int(src_counts.get("pure_play", 0)),
                "sector_fallback": int(src_counts.get("sector_fallback", 0)),
                "pure_play_pct": _pct(int(src_counts.get("pure_play", 0))),
            },
            "selection_basis": {
                "heuristic_industry": int(basis_counts.get("heuristic_industry", 0)),
                "heuristic_sector": int(basis_counts.get("heuristic_sector", 0)),
                "spy_fallback": int(basis_counts.get("spy_fallback", 0)),
            },
            "classification_source": {
                "tiingo": int(cls_counts.get("tiingo", 0)),
                "sic": int(cls_counts.get("sic", 0)),
                "no_meta": int(cls_counts.get("no_meta", 0)),
                "isactive_dropped": int(cls_counts.get("isactive_dropped", 0)),
                "name_mismatch": int(cls_counts.get("name_mismatch", 0)),
            },
        }

    # Raw guard blast radius over the eligible universe (independent of hedging).
    drop_reasons = drop_reasons or {}
    if eligible_syms is not None:
        elig_set = {str(s).upper() for s in eligible_syms}
        relevant = {t: r for t, r in drop_reasons.items() if t in elig_set}
    else:
        relevant = dict(drop_reasons)
    base["guard_drops"] = {
        "isactive_dropped": sum(1 for r in relevant.values() if r == "isactive_dropped"),
        "name_mismatch": sum(1 for r in relevant.values() if r == "name_mismatch"),
    }
    return base


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="Hedge Map ETL")
def hedge_map_flow(
    as_of_override: Optional[str] = None,
    subset_symbols: Optional[list[str]] = None,
    backfill_days: int = 0,
) -> dict:
    """
    Nightly hedge-map ETL. Produces top-3 hedge ETF candidates per eligible US equity.

    Args:
        as_of_override: ISO date string (YYYY-MM-DD). Defaults to prior trading day.
        subset_symbols: If provided, restrict stock universe to these symbols (for testing).
        backfill_days: If > 0, run for this many prior trading days ending at as_of
                       (useful for initial backfill; each day is a full compute pass).
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Starting Hedge Map ETL Flow")
    logger.info("=" * 60)

    # --- Credentials ---
    # Load Alpaca creds from Prefect Secret blocks (deployed path) with env-var fallback.
    # Must happen before any call to _alpaca_headers().
    logger.info("Loading Alpaca credentials (Prefect Secret blocks with env-var fallback)...")
    _init_alpaca_creds(from_prefect_blocks=True)

    # --- Determine as_of date (before calendar load so we know the full window) ---
    if as_of_override:
        # Snap to the latest trading session ≤ the requested date using weekday logic
        # (calendar not yet loaded). The calendar reload below re-checks with full
        # holiday awareness, but weekday snapping is sufficient for the range calculation.
        raw_as_of = date.fromisoformat(as_of_override)
        # Temporary weekday snap (no calendar yet); will be re-snapped after calendar load.
        as_of = raw_as_of
    else:
        # Scheduled default: today (run is post-close at 18:30 ET). Use weekday fallback
        # until the calendar is loaded; re-snapped below.
        as_of = date.today()

    # Load exchange calendar over the actual requested window:
    #   - cal_end   = raw_as_of + 7 days (cover next_trading_day look-ahead)
    #   - cal_start = raw_as_of - max(200, backfill_days * 2 + 160) calendar days
    #     so that a large backfill (e.g. backfill_days=250 ≈ 350 calendar days)
    #     doesn't walk out of the cached range and mis-classify sessions.
    #     Formula: backfill_days * 2 converts trading-days to calendar-days conservatively,
    #     + 130d bar-window buffer + 30d holiday headroom.
    cal_end = as_of + timedelta(days=7)
    cal_window_days = max(200, backfill_days * 2 + 160)
    cal_start = as_of - timedelta(days=cal_window_days)
    logger.info(f"Loading NYSE calendar {cal_start} → {cal_end}...")
    _init_trading_calendar(cal_start, cal_end)
    logger.info(f"  {len(_TRADING_DAYS)} trading sessions loaded")

    # Now re-snap as_of with full holiday awareness from the loaded calendar.
    if as_of_override:
        as_of = _latest_trading_day(raw_as_of)
        if as_of != raw_as_of:
            logger.warning(
                f"as_of_override {raw_as_of} is not a trading session; "
                f"snapped to {as_of}."
            )
    else:
        as_of = _latest_trading_day(date.today())

    logger.info("Loading AWS credentials from Prefect Cloud...")
    aws_credentials = AwsCredentials.load("aws-credentials-tim")

    logger.info(f"as_of_date: {as_of}  |  effective_date: {_next_trading_day(as_of)}")

    # Build list of as_of dates (single run or backfill).
    if backfill_days > 0:
        as_of_dates = _trading_days_before(as_of, backfill_days)
        logger.info(f"Backfill mode: {len(as_of_dates)} days from {as_of_dates[-1]} to {as_of_dates[0]}")
    else:
        as_of_dates = [as_of]

    # --- Universe ---
    universe = fetch_universe()
    if subset_symbols:
        universe = universe[universe["symbol"].isin(subset_symbols)].reset_index(drop=True)
        logger.info(f"Subset mode: {len(universe)} symbols")

    persist_universe_snapshot(universe, as_of_dates[0], aws_credentials)

    # --- Classification (Tiingo meta) ---
    # Lazy import to avoid the hedge_selection/hedge_classification → hedge_map_flow cycle.
    from hedge_classification import load_classification
    from hedge_selection import build_hedge_map as build_heuristic_hedge_map

    # Live Alpaca company names — used by the ticker-reuse guard to reconcile a
    # Tiingo meta record against the active company (rejects delisted predecessors).
    alpaca_names = {
        str(row["symbol"]).upper(): row.get("name", "")
        for _, row in universe.iterrows()
    }

    s3_client = aws_credentials.get_boto3_session().client("s3")
    try:
        classification, class_snapshot_date, drop_reasons = load_classification(
            s3_client, as_of=as_of_dates[0], alpaca_names=alpaca_names
        )
        n_reuse = sum(1 for r in drop_reasons.values()
                      if r in ("isactive_dropped", "name_mismatch"))
        logger.info(
            f"Loaded Tiingo classification: {len(classification)} tickers "
            f"(snapshot {class_snapshot_date}); guard dropped {n_reuse} reused-ticker rows"
        )
    except FileNotFoundError as exc:
        # No meta partition at all — degrade gracefully: every stock is
        # classification_source='no_meta' and hedges to SPY. Coverage stats make
        # this observable; we do NOT crash the nightly run over a missing meta.
        logger.error(
            f"No Tiingo meta partition found ({exc}); proceeding with empty "
            "classification (all stocks → SPY fallback)."
        )
        classification, class_snapshot_date, drop_reasons = {}, None, {}

    # The ETFs the heuristic crosswalk can emit (sector SPDRs + industry pure-plays + SPY).
    # We still resolve shortability for the full ETF_CANDIDATES set so the v1 helpers
    # and parity test keep working, but only the crosswalk ETFs need bars here.
    crosswalk_etfs = sorted(all_referenced_etfs())

    # Build ETF metadata dict (shortable/easy_to_borrow) for the crosswalk ETFs.
    # v2 only emits crosswalk ETFs, so we only need shortability for those — no
    # need to resolve all 57 v1 candidates (avoids ~27 extra individual fetches).
    # Start from the equity universe (ETFs are usually classified as us_equity on Alpaca).
    etf_meta_rows = universe[universe["symbol"].isin(crosswalk_etfs)]
    etf_meta: dict[str, dict] = {
        row["symbol"]: {"shortable": row["shortable"], "easy_to_borrow": row["easy_to_borrow"]}
        for _, row in etf_meta_rows.iterrows()
    }
    # For any ETF not captured above, fetch its asset record individually so we don't
    # silently default it to non-shortable. Alpaca may classify some ETFs differently
    # (e.g., different asset_class) so they'd be absent from the us_equity universe.
    missing_etfs = [e for e in crosswalk_etfs if e not in etf_meta]
    if missing_etfs:
        logger.info(f"Fetching asset records for {len(missing_etfs)} ETFs not in universe: {missing_etfs}")
    for etf in missing_etfs:
        try:
            r = requests.get(
                f"https://api.alpaca.markets/v2/assets/{etf}",
                headers=_alpaca_headers(),
                timeout=30,
            )
            if r.status_code == 200:
                a = r.json()
                etf_meta[etf] = {
                    "shortable": bool(a.get("shortable", False)),
                    "easy_to_borrow": bool(a.get("easy_to_borrow", False)),
                }
                logger.info(f"  {etf}: shortable={etf_meta[etf]['shortable']}")
            else:
                logger.warning(f"  {etf}: asset fetch returned {r.status_code}, defaulting to non-shortable")
                etf_meta[etf] = {"shortable": False, "easy_to_borrow": False}
        except Exception as exc:
            logger.warning(f"  {etf}: asset fetch failed ({exc}), defaulting to non-shortable")
            etf_meta[etf] = {"shortable": False, "easy_to_borrow": False}

    # --- Bars ---
    # Fetch bars through as_of_dates[0] (inclusive): as_of_date is the last close used
    # in beta/ADV computation, so that close's bar must be present.
    # 60 trading days ≈ 87 calendar days; ADV window adds ~43 more; 130d total provides
    # headroom for holiday-heavy periods (Dec, Nov) without being wasteful.
    earliest_as_of = as_of_dates[-1]  # oldest date in backfill
    bar_end = as_of_dates[0]           # inclusive: include the as_of close
    bar_start = earliest_as_of - timedelta(days=130)  # ~130 calendar days of buffer

    # v2 only needs bars for the crosswalk ETFs (sector SPDRs + industry pure-plays
    # + SPY), not all 57. Sentinel ETFs (SPY/QQQ/IWM) are added for the bar-lag guard.
    all_symbols = list(universe["symbol"].tolist()) + crosswalk_etfs + _SENTINEL_ETFS
    all_symbols = list(dict.fromkeys(all_symbols))  # dedupe, preserve order

    logger.info(f"Fetching bars for {len(all_symbols)} symbols from {bar_start} to {bar_end}...")
    all_bars = fetch_all_daily_bars(all_symbols, bar_start, bar_end)

    logger.info(f"Bars loaded: {sum(1 for df in all_bars.values() if not df.empty)}/{len(all_symbols)} symbols")

    # --- Bar availability guard ---
    # Only fires on the scheduled default path (not --as-of override).
    beta_as_of_overrides = _check_bar_availability(all_bars, as_of_dates, as_of_override is not None)

    # --- Process each as_of date ---
    # Compute the newest effective_date across all runs upfront so we can gate
    # publish_latest correctly: only the newest partition should claim latest/.
    # Using max() is robust to any future ordering change in as_of_dates.
    max_effective_date = max(_next_trading_day(d) for d in as_of_dates)

    results: list[dict] = []
    for run_as_of in as_of_dates:
        # beta_as_of: the last settled close used for betas/ADV. Normally equals run_as_of.
        # When the bar-lag guard fires, it's the most recent available bar date.
        beta_as_of = beta_as_of_overrides.get(run_as_of, run_as_of)
        # effective_date: the session this partition is FOR. Always derived from run_as_of
        # (the intended date), never from beta_as_of, so bar lag never delays the partition.
        run_effective_date = _next_trading_day(run_as_of)

        if beta_as_of != run_as_of:
            logger.info(
                f"Computing hedge map for as_of={run_as_of} "
                f"(beta_as_of={beta_as_of}, effective_date={run_effective_date})..."
            )
        else:
            logger.info(f"Computing hedge map for as_of={run_as_of}...")

        elig_df = compute_eligibility(universe, all_bars, beta_as_of)
        eligible_count = int(elig_df["eligible"].sum())
        logger.info(f"  Eligible: {eligible_count}/{len(universe)}")

        elig_syms = elig_df[elig_df["eligible"]]["symbol"].tolist()
        hedge_map = build_heuristic_hedge_map(
            elig_df, all_bars, etf_meta, classification,
            as_of=beta_as_of,
            effective_date=run_effective_date,
            drop_reasons=drop_reasons,
        )
        covered = hedge_map[hedge_map["rank"] == 1]["ticker"].nunique()
        cov_stats = compute_coverage_stats(
            hedge_map, eligible_count, drop_reasons=drop_reasons, eligible_syms=elig_syms
        )
        logger.info(f"  Covered tickers (rank=1): {covered}")
        logger.info(f"  Coverage stats: {json.dumps(cov_stats)}")

        if not hedge_map.empty:
            publish_latest = (run_effective_date == max_effective_date)
            s3_uri = write_hedge_map_to_s3(hedge_map, aws_credentials,
                                           publish_latest=publish_latest)
            manifest_uri = write_run_manifest(
                as_of=beta_as_of,
                effective_date=run_effective_date,
                universe_size=len(universe),
                eligible_count=eligible_count,
                covered_tickers=covered,
                hedge_map_rows=len(hedge_map),
                aws_credentials=aws_credentials,
                classification_snapshot_date=class_snapshot_date,
                coverage_stats=cov_stats,
                publish_latest=publish_latest,
            )
            results.append({
                "as_of": beta_as_of.isoformat(),
                "effective_date": run_effective_date.isoformat(),
                "universe_size": len(universe),
                "eligible_count": eligible_count,
                "covered_tickers": covered,
                "hedge_map_rows": len(hedge_map),
                "coverage_pct": round(covered / eligible_count * 100, 2) if eligible_count > 0 else 0,
                "s3_uri": s3_uri,
                "manifest_uri": manifest_uri,
            })
        else:
            if subset_symbols:
                # Subset/test run: lenient — log and continue so partial test runs complete.
                logger.warning(f"  No hedge map rows for as_of={run_as_of} (subset run; continuing)")
                results.append({"as_of": run_as_of.isoformat(), "error": "empty_hedge_map"})
            else:
                # Full-universe scheduled run: an empty map means data/metadata outage.
                # Fail loudly so alerting fires — silently producing no hedge is worse.
                raise RuntimeError(
                    f"build_hedge_map returned zero rows for as_of={run_as_of} "
                    f"(eligible={eligible_count}). This indicates a data or metadata "
                    "outage; fix the root cause before the next run."
                )

    logger.info("=" * 60)
    logger.info("Hedge Map ETL Flow completed")
    for r in results:
        logger.info(f"  {r}")
    logger.info("=" * 60)

    # Return summary of the most recent run for inspection.
    return results[-1] if results else {}


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _latest_trading_day(d: date) -> date:
    """
    Most recent trading session <= d. Uses exchange calendar when loaded.

    For the scheduled-run default: a run at 18:30 ET on trading day X returns X
    so that effective_date = next_trading_day(X) = the upcoming session.
    On a market holiday, walks back to the preceding session.
    """
    candidate = d
    while not _is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def _prior_trading_day(d: date) -> date:
    """Most recent trading session strictly before d. Uses exchange calendar when loaded."""
    candidate = d - timedelta(days=1)
    while not _is_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def _trading_days_before(end: date, n: int) -> list[date]:
    """n trading sessions ending at end, most-recent-first. Uses exchange calendar when loaded."""
    days: list[date] = []
    candidate = end
    while len(days) < n:
        if _is_trading_day(candidate):
            days.append(candidate)
        candidate -= timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Nightly hedge-map ETL")
    ap.add_argument("--as-of", dest="as_of", help="as_of date YYYY-MM-DD (default: prior trading day)")
    ap.add_argument("--subset", help="Comma-separated symbol list (for testing)")
    ap.add_argument("--backfill-days", dest="backfill_days", type=int, default=0,
                    help="Run for N historical as_of dates ending at --as-of")
    args = ap.parse_args()

    subset = [s.strip() for s in args.subset.split(",")] if args.subset else None

    result = hedge_map_flow(
        as_of_override=args.as_of,
        subset_symbols=subset,
        backfill_days=args.backfill_days,
    )
    print("\nResult:", json.dumps(result, indent=2, default=str))
