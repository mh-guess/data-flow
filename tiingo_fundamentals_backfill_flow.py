"""
Tiingo Fundamentals Historical Backfill Pipeline

Fetches historical fundamental metrics and financial statements with year-level
partitioning. Includes rate limiting for Tiingo API (50 req/hour free tier).

S3 Structure:
  tiingo/json/fundamentals/daily/load_type=retro/year={YYYY}/{ticker}.json
  tiingo/json/fundamentals/statements/as_reported=true/load_type=retro/year={YYYY}/{ticker}.json
  tiingo/json/fundamentals/statements/as_reported=false/load_type=retro/year={YYYY}/{ticker}.json
"""

from prefect import flow, task, get_run_logger
from datetime import datetime
import requests
import time
from typing import Optional, List

from shared import (
    S3_BUCKET_NAME, TICKERS_S3_KEY,
    fetch_tickers_from_s3, load_credentials, upload_json_to_s3,
)

TIINGO_FUNDAMENTALS_BASE = "https://api.tiingo.com/tiingo/fundamentals"
RATE_LIMIT_DELAY_SECONDS = 3


def _tiingo_headers(api_token: str) -> dict:
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Token {api_token}',
    }


@task(retries=3, retry_delay_seconds=10)
def fetch_fundamentals_daily_year(ticker: str, year: int, api_token: str) -> list:
    """Fetch daily fundamental metrics for a full year."""
    logger = get_run_logger()
    logger.info(f"Fetching {year} daily fundamentals for {ticker}...")

    response = requests.get(
        f"{TIINGO_FUNDAMENTALS_BASE}/{ticker}/daily",
        headers=_tiingo_headers(api_token),
        params={
            'startDate': f"{year}-01-01",
            'endDate': f"{year}-12-31",
        },
    )
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} daily fundamental records for {ticker} in {year}")
    return data


@task(retries=3, retry_delay_seconds=10)
def fetch_statements_year(ticker: str, year: int, api_token: str, as_reported: bool) -> list:
    """Fetch financial statements for a full year."""
    logger = get_run_logger()
    label = "as-reported" if as_reported else "restated"
    logger.info(f"Fetching {year} statements ({label}) for {ticker}...")

    response = requests.get(
        f"{TIINGO_FUNDAMENTALS_BASE}/{ticker}/statements",
        headers=_tiingo_headers(api_token),
        params={
            'startDate': f"{year}-01-01",
            'endDate': f"{year}-12-31",
            'asReported': str(as_reported).lower(),
        },
    )
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} statement records ({label}) for {ticker} in {year}")
    return data


@flow(name="Tiingo Fundamentals Backfill")
def tiingo_fundamentals_backfill_flow(
    start_year: int,
    end_year: int,
    tickers: Optional[List[str]] = None,
):
    """
    Backfill historical fundamentals data with year-level partitioning.

    For each ticker and year, fetches:
    - Daily fundamental metrics
    - Financial statements (asReported=true and asReported=false)

    Args:
        start_year: Start year for backfill (inclusive), e.g., 2020
        end_year: End year for backfill (inclusive), e.g., 2024
        tickers: Optional list of tickers. If None, fetches from S3.
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Starting Tiingo Fundamentals Backfill")
    logger.info(f"Years: {start_year} to {end_year}")
    logger.info("=" * 60)

    api_token, aws_credentials = load_credentials()

    if tickers is None:
        logger.info("No tickers provided, fetching from S3...")
        tickers = fetch_tickers_from_s3(S3_BUCKET_NAME, TICKERS_S3_KEY, aws_credentials)
    else:
        logger.info(f"Using provided tickers: {', '.join(tickers)}")

    years = list(range(start_year, end_year + 1))
    # 3 API calls per ticker-year (1 daily + 2 statement variants)
    total_ops = len(tickers) * len(years) * 3
    logger.info(
        f"Backfill plan: {len(tickers)} tickers × {len(years)} years × 3 endpoints = {total_ops} API calls"
    )
    logger.info(f"Rate limit delay: {RATE_LIMIT_DELAY_SECONDS}s between calls")

    uploaded_keys = []

    for ticker in tickers:
        for year in years:
            # Daily fundamentals
            daily_data = fetch_fundamentals_daily_year(ticker, year, api_token)
            key = upload_json_to_s3(
                daily_data,
                f"tiingo/json/fundamentals/daily/load_type=retro/year={year}/{ticker}.json",
                S3_BUCKET_NAME, aws_credentials,
            )
            uploaded_keys.append(key)
            time.sleep(RATE_LIMIT_DELAY_SECONDS)

            # Statements - both variants
            for as_reported in [True, False]:
                variant = "true" if as_reported else "false"
                stmts = fetch_statements_year(ticker, year, api_token, as_reported)
                key = upload_json_to_s3(
                    stmts,
                    f"tiingo/json/fundamentals/statements/as_reported={variant}/load_type=retro/year={year}/{ticker}.json",
                    S3_BUCKET_NAME, aws_credentials,
                )
                uploaded_keys.append(key)
                time.sleep(RATE_LIMIT_DELAY_SECONDS)

    logger.info("=" * 60)
    logger.info("Fundamentals Backfill completed successfully!")
    logger.info(f"Total files uploaded: {len(uploaded_keys)}")
    logger.info(f"Years processed: {start_year}-{end_year}")
    logger.info(f"Tickers processed: {', '.join(tickers)}")
    logger.info(f"Data location: s3://{S3_BUCKET_NAME}/tiingo/json/fundamentals/")
    logger.info("=" * 60)

    return {
        "uploaded_keys": uploaded_keys,
        "total_files": len(uploaded_keys),
        "years": years,
        "tickers": tickers,
    }


if __name__ == "__main__":
    tiingo_fundamentals_backfill_flow(start_year=2020, end_year=2024)
