"""
Nightly Hedge-Map ETL — Part A of MF Drift Hedge Overlay

Produces a partitioned parquet on S3 that maps every eligible US equity ticker
to its top-3 beta-weighted hedge ETFs (by R²) for the next trading session.

S3 output (canonical path — the apex team needs this):
  s3://mh-guess-data/hedge_map/effective_date=YYYY-MM-DD/data.parquet
  s3://mh-guess-data/hedge_map/manifests/effective_date=YYYY-MM-DD/manifest.json

Sidecar manifest fields: effective_date, as_of_date, universe_size,
  eligible_count, coverage_pct, etf_set_version, row_count.

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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

S3_BUCKET = "mh-guess-data"
HEDGE_MAP_PREFIX = "hedge_map"
UNIVERSE_SNAPSHOT_PREFIX = "hedge_map/universe_snapshots"

# ETF candidate set version tag — bump when the research list changes.
ETF_SET_VERSION = "v1-57etf-20260624"

# The research 57 ETFs (from etf_candidates.csv, in canonical order).
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
) -> pd.DataFrame:
    """
    Compute beta/R² for every eligible stock × every ETF candidate, then select top-3.

    etf_meta carries shortability flags (from universe/asset pull for ETF symbols).
    Returns the hedge_map DataFrame (schema-conforming).
    """
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

        effective_date = _next_trading_day(as_of)

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


def _next_trading_day(d: date) -> date:
    """First trading session after d. Uses exchange calendar when loaded."""
    nxt = d + timedelta(days=1)
    while not _is_trading_day(nxt):
        nxt += timedelta(days=1)
    return nxt


# ---------------------------------------------------------------------------
# S3 write tasks
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=10)
def write_hedge_map_to_s3(
    hedge_map: pd.DataFrame,
    aws_credentials: AwsCredentials,
) -> str:
    """
    Write partitioned parquet to S3.

    Partition key: effective_date=YYYY-MM-DD/
    Returns the S3 URI of the written file.
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
) -> str:
    """Write sidecar JSON manifest for this run."""
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
        "row_count": hedge_map_rows,
        "run_utc": datetime.now(timezone.utc).isoformat(),
    }

    key = f"{HEDGE_MAP_PREFIX}/manifests/effective_date={effective_date.isoformat()}/manifest.json"
    s3 = aws_credentials.get_boto3_session().client("s3")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )

    uri = f"s3://{S3_BUCKET}/{key}"
    logger.info(f"Manifest written: {uri}")
    logger.info(json.dumps(manifest, indent=2))
    return uri


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

    # Load exchange calendar so all trading-day helpers are holiday-aware.
    # Covers 200 calendar days back (beta/ADV window) + 7 days forward (next session).
    cal_start = date.today() - timedelta(days=200)
    cal_end = date.today() + timedelta(days=7)
    logger.info(f"Loading NYSE calendar {cal_start} → {cal_end}...")
    _init_trading_calendar(cal_start, cal_end)
    logger.info(f"  {len(_TRADING_DAYS)} trading sessions loaded")

    logger.info("Loading AWS credentials from Prefect Cloud...")
    aws_credentials = AwsCredentials.load("aws-credentials-tim")

    # --- Determine as_of date ---
    if as_of_override:
        as_of = date.fromisoformat(as_of_override)
    else:
        # For a scheduled run at 18:30 ET (after today's close), as_of = today so that
        # effective_date = next_trading_day(today) = the upcoming session.
        # _latest_trading_day() uses the exchange calendar so holidays are handled correctly.
        as_of = _latest_trading_day(date.today())
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

    # Build ETF metadata dict (shortable/easy_to_borrow for each candidate ETF).
    # Start from the equity universe (ETFs are usually classified as us_equity on Alpaca).
    etf_meta_rows = universe[universe["symbol"].isin(ETF_CANDIDATES)]
    etf_meta: dict[str, dict] = {
        row["symbol"]: {"shortable": row["shortable"], "easy_to_borrow": row["easy_to_borrow"]}
        for _, row in etf_meta_rows.iterrows()
    }
    # For any ETF not captured above, fetch its asset record individually so we don't
    # silently default it to non-shortable. Alpaca may classify some ETFs differently
    # (e.g., different asset_class) so they'd be absent from the us_equity universe.
    missing_etfs = [e for e in ETF_CANDIDATES if e not in etf_meta]
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

    all_symbols = list(universe["symbol"].tolist()) + ETF_CANDIDATES
    all_symbols = list(dict.fromkeys(all_symbols))  # dedupe, preserve order

    logger.info(f"Fetching bars for {len(all_symbols)} symbols from {bar_start} to {bar_end}...")
    all_bars = fetch_all_daily_bars(all_symbols, bar_start, bar_end)

    logger.info(f"Bars loaded: {sum(1 for df in all_bars.values() if not df.empty)}/{len(all_symbols)} symbols")

    # --- Bar availability guard ---
    # Verify that as_of's close bars have settled by checking sentinel ETFs.
    # Fires when the scheduled default is used (not when as_of_override is set),
    # guarding against: market holidays, very early runs, or bar-settlement delay.
    if not as_of_override:
        sentinel_max = [
            all_bars[e]["d"].max()
            for e in _SENTINEL_ETFS
            if e in all_bars and not all_bars[e].empty
        ]
        if sentinel_max:
            actual_last_bar: date = max(sentinel_max)
            if actual_last_bar < as_of_dates[0]:
                logger.warning(
                    f"Bar availability guard: latest settled bar is {actual_last_bar}, "
                    f"expected {as_of_dates[0]}. Adjusting as_of → {actual_last_bar}. "
                    "Likely cause: market holiday or bars not yet settled."
                )
                as_of_dates = (
                    _trading_days_before(actual_last_bar, backfill_days)
                    if backfill_days > 0
                    else [actual_last_bar]
                )

    # --- Process each as_of date ---
    results: list[dict] = []
    for run_as_of in as_of_dates:
        logger.info(f"Computing hedge map for as_of={run_as_of}...")

        elig_df = compute_eligibility(universe, all_bars, run_as_of)
        eligible_count = int(elig_df["eligible"].sum())
        logger.info(f"  Eligible: {eligible_count}/{len(universe)}")

        hedge_map = compute_hedge_map(elig_df, all_bars, etf_meta, run_as_of)
        covered = hedge_map[hedge_map["rank"] == 1]["ticker"].nunique()
        logger.info(f"  Covered tickers (rank=1): {covered}")

        if not hedge_map.empty:
            s3_uri = write_hedge_map_to_s3(hedge_map, aws_credentials)
            manifest_uri = write_run_manifest(
                as_of=run_as_of,
                effective_date=_next_trading_day(run_as_of),
                universe_size=len(universe),
                eligible_count=eligible_count,
                covered_tickers=covered,
                hedge_map_rows=len(hedge_map),
                aws_credentials=aws_credentials,
            )
            results.append({
                "as_of": run_as_of.isoformat(),
                "effective_date": _next_trading_day(run_as_of).isoformat(),
                "universe_size": len(universe),
                "eligible_count": eligible_count,
                "covered_tickers": covered,
                "hedge_map_rows": len(hedge_map),
                "coverage_pct": round(covered / eligible_count * 100, 2) if eligible_count > 0 else 0,
                "s3_uri": s3_uri,
                "manifest_uri": manifest_uri,
            })
        else:
            logger.warning(f"  No hedge map rows produced for as_of={run_as_of}")
            results.append({"as_of": run_as_of.isoformat(), "error": "empty_hedge_map"})

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
