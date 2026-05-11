"""
APEX Volatility Table Pipeline

Computes daily per-minute volatility estimates for each symbol in the APEX
universe and writes the result to S3 as a single parquet file.

Output: s3://apex-market-data-raw-220464759930/derived/volatility/{YYYY-MM-DD}/vol_table.parquet

Schedule: 6 PM ET weekdays (after market close).
"""

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from datetime import datetime, timedelta, timezone, date
import requests
import time
import math
import io

import pandas as pd
from ruamel.yaml import YAML

from shared import load_credentials

APEX_BUCKET = "apex-market-data-raw-220464759930"
SYMBOLS_URL = "https://raw.githubusercontent.com/mh-guess/apex/main/symbols.yaml"
TIINGO_BASE = "https://api.tiingo.com/tiingo/daily"
LOOKBACK_CALENDAR_DAYS = 180
MIN_HISTORY_DAYS = 90
TRADING_DAYS_PER_YEAR = 252
TRADING_MINUTES_PER_DAY = 390
RATE_LIMIT_DELAY = 2


@task(retries=2, retry_delay_seconds=5)
def fetch_symbols_from_github(github_token: str) -> list[str]:
    """Fetch the APEX symbol list from the mh-guess/apex repo on GitHub."""
    logger = get_run_logger()
    headers = {'Authorization': f'token {github_token}'}
    response = requests.get(SYMBOLS_URL, headers=headers)
    response.raise_for_status()

    yaml = YAML(typ='safe')
    symbols = yaml.load(response.text)

    if not isinstance(symbols, list):
        raise ValueError(f"Expected a list in symbols.yaml, got {type(symbols).__name__}")

    symbols = [str(s).upper() for s in symbols]
    logger.info(f"Loaded {len(symbols)} symbols from GitHub: {', '.join(symbols)}")
    return symbols


@task(retries=3, retry_delay_seconds=10)
def fetch_eod_prices(symbol: str, api_token: str) -> list:
    """Fetch daily OHLCV data from Tiingo for the lookback window."""
    logger = get_run_logger()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_CALENDAR_DAYS)

    url = f"{TIINGO_BASE}/{symbol}/prices"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token {api_token}',
    }
    params = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} trading days for {symbol}")
    return data


def compute_volatility_row(symbol: str, ohlcv: list | None, computed_at: datetime) -> dict:
    """Compute volatility metrics for a single ticker."""
    base = {
        'symbol': symbol,
        'daily_vol': None,
        'daily_vol_annualized': None,
        'per_minute_vol': None,
        'lookback_days': 0,
        'data_start_date': None,
        'data_end_date': None,
        'short_history': False,
        'no_data': True,
        'computed_at': computed_at,
    }

    if not ohlcv:
        return base

    sorted_data = sorted(ohlcv, key=lambda r: r['date'])
    closes = [(r['date'], r['adjClose']) for r in sorted_data if r.get('adjClose') is not None]

    if len(closes) < 2:
        return base

    dates, prices = zip(*closes)
    log_returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))]

    daily_vol = pd.Series(log_returns).std(ddof=1)
    daily_vol_annualized = daily_vol * math.sqrt(TRADING_DAYS_PER_YEAR)
    per_minute_vol = daily_vol / math.sqrt(TRADING_MINUTES_PER_DAY)

    lookback_days = len(closes)
    start_str = dates[0][:10] if isinstance(dates[0], str) else str(dates[0])[:10]
    end_str = dates[-1][:10] if isinstance(dates[-1], str) else str(dates[-1])[:10]

    return {
        'symbol': symbol,
        'daily_vol': daily_vol,
        'daily_vol_annualized': daily_vol_annualized,
        'per_minute_vol': per_minute_vol,
        'lookback_days': lookback_days,
        'data_start_date': date.fromisoformat(start_str),
        'data_end_date': date.fromisoformat(end_str),
        'short_history': lookback_days < MIN_HISTORY_DAYS,
        'no_data': False,
        'computed_at': computed_at,
    }


def build_dataframe(rows: list[dict]) -> pd.DataFrame:
    """Build a DataFrame from volatility row dicts with correct dtypes."""
    df = pd.DataFrame(rows)
    df['symbol'] = df['symbol'].astype(str)
    df['daily_vol'] = df['daily_vol'].astype('float64')
    df['daily_vol_annualized'] = df['daily_vol_annualized'].astype('float64')
    df['per_minute_vol'] = df['per_minute_vol'].astype('float64')
    df['lookback_days'] = df['lookback_days'].astype('Int32')
    df['short_history'] = df['short_history'].astype(bool)
    df['no_data'] = df['no_data'].astype(bool)
    df['computed_at'] = pd.to_datetime(df['computed_at'], utc=True)
    return df


@flow(name="APEX Volatility Table")
def vol_table_flow():
    """
    Daily pipeline: compute per-minute volatility for every APEX symbol
    and write the result table to S3 as parquet.
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Starting APEX Volatility Table Flow")
    logger.info("=" * 60)

    api_token, aws_credentials = load_credentials()

    logger.info("Loading GitHub PAT from Prefect Cloud...")
    github_token = Secret.load("github-pat-apex").get()

    symbols = fetch_symbols_from_github(github_token)

    computed_at = datetime.now(timezone.utc)
    rows = []
    failed = 0

    for symbol in symbols:
        try:
            ohlcv = fetch_eod_prices(symbol, api_token)
        except Exception as e:
            logger.warning(f"Failed to fetch data for {symbol}: {e}")
            ohlcv = None
            failed += 1

        row = compute_volatility_row(symbol, ohlcv, computed_at)
        rows.append(row)
        time.sleep(RATE_LIMIT_DELAY)

    if all(r['no_data'] for r in rows):
        raise RuntimeError("All symbols returned no data — aborting")

    df = build_dataframe(rows)

    buf = io.BytesIO()
    df.to_parquet(buf, compression='snappy', index=False)
    parquet_bytes = buf.getvalue()

    date_partition = computed_at.strftime('%Y-%m-%d')
    s3_key = f"derived/volatility/{date_partition}/vol_table.parquet"

    s3_client = aws_credentials.get_boto3_session().client('s3')
    latest_key = "derived/volatility/vol_table_latest.parquet"
    for key in [s3_key, latest_key]:
        s3_client.put_object(
            Bucket=APEX_BUCKET,
            Key=key,
            Body=parquet_bytes,
            ContentType='application/x-parquet',
        )

    no_data_count = sum(1 for r in rows if r['no_data'])
    short_count = sum(1 for r in rows if r['short_history'])

    logger.info("=" * 60)
    logger.info("APEX Volatility Table Flow completed")
    logger.info(f"Symbols processed: {len(symbols)}")
    logger.info(f"  OK: {len(symbols) - no_data_count}")
    logger.info(f"  No data: {no_data_count}")
    logger.info(f"  Short history (<{MIN_HISTORY_DAYS}d): {short_count}")
    if failed > 0:
        logger.warning(f"  API failures (included as no_data): {failed}")
    logger.info(f"Output: s3://{APEX_BUCKET}/{s3_key}")
    logger.info(f"Latest: s3://{APEX_BUCKET}/{latest_key}")
    logger.info(f"File size: {len(parquet_bytes):,} bytes")
    logger.info("=" * 60)

    return s3_key


if __name__ == "__main__":
    vol_table_flow()
