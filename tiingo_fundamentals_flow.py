"""
Tiingo Fundamentals Daily Pipeline

Fetches daily fundamental metrics, financial statements, definitions, and company
metadata from Tiingo Fundamentals API and loads to S3.

S3 Structure:
  tiingo/json/fundamentals/daily/load_type=daily/date={YYYY-MM-DD}/{ticker}.json
  tiingo/json/fundamentals/statements/as_reported=true/load_type=daily/date={YYYY-MM-DD}/{ticker}.json
  tiingo/json/fundamentals/statements/as_reported=false/load_type=daily/date={YYYY-MM-DD}/{ticker}.json
  tiingo/json/fundamentals/definitions/date={YYYY-MM-DD}/definitions.json
  tiingo/json/fundamentals/meta/date={YYYY-MM-DD}/meta.json
"""

from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
import requests

from shared import (
    S3_BUCKET_NAME, TICKERS_S3_KEY,
    fetch_tickers_from_s3, load_credentials, upload_json_to_s3,
)

TIINGO_FUNDAMENTALS_BASE = "https://api.tiingo.com/tiingo/fundamentals"


def _tiingo_headers(api_token: str) -> dict:
    return {
        'Content-Type': 'application/json',
        'Authorization': f'Token {api_token}',
    }


@task(retries=3, retry_delay_seconds=10)
def fetch_definitions(api_token: str) -> list:
    """Fetch all available fundamental metric definitions."""
    logger = get_run_logger()
    logger.info("Fetching fundamentals definitions...")

    response = requests.get(
        f"{TIINGO_FUNDAMENTALS_BASE}/definitions",
        headers=_tiingo_headers(api_token),
    )
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} metric definitions")
    return data


@task(retries=3, retry_delay_seconds=10)
def fetch_meta(api_token: str, tickers: list) -> list:
    """Fetch company metadata for all tickers in a single call."""
    logger = get_run_logger()
    tickers_str = ",".join(tickers)
    logger.info(f"Fetching fundamentals meta for {len(tickers)} tickers...")

    response = requests.get(
        f"{TIINGO_FUNDAMENTALS_BASE}/meta",
        headers=_tiingo_headers(api_token),
        params={'tickers': tickers_str},
    )
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched meta for {len(data)} companies")
    return data


@task(retries=3, retry_delay_seconds=10)
def fetch_fundamentals_daily(ticker: str, api_token: str) -> list:
    """Fetch daily fundamental metrics (P/E, market cap, etc.) for last 30 days."""
    logger = get_run_logger()
    logger.info(f"Fetching daily fundamentals for {ticker}...")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    response = requests.get(
        f"{TIINGO_FUNDAMENTALS_BASE}/{ticker}/daily",
        headers=_tiingo_headers(api_token),
        params={
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
        },
    )
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} daily fundamental records for {ticker}")
    return data


@task(retries=3, retry_delay_seconds=10)
def fetch_statements(ticker: str, api_token: str, as_reported: bool) -> list:
    """Fetch financial statements (balance sheet, income, cash flow)."""
    logger = get_run_logger()
    label = "as-reported" if as_reported else "restated"
    logger.info(f"Fetching statements ({label}) for {ticker}...")

    response = requests.get(
        f"{TIINGO_FUNDAMENTALS_BASE}/{ticker}/statements",
        headers=_tiingo_headers(api_token),
        params={'asReported': str(as_reported).lower()},
    )
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} statement records ({label}) for {ticker}")
    return data


@flow(name="Tiingo Fundamentals Daily")
def tiingo_fundamentals_flow():
    """
    Daily pipeline fetching all four fundamentals endpoints:
    definitions, meta, daily metrics, and statements (both asReported variants).
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Starting Tiingo Fundamentals Daily Flow")
    logger.info("=" * 60)

    api_token, aws_credentials = load_credentials()
    tickers = fetch_tickers_from_s3(S3_BUCKET_NAME, TICKERS_S3_KEY, aws_credentials)
    date_partition = datetime.now().strftime('%Y-%m-%d')

    uploaded_keys = []

    # 1. Definitions (single bulk call)
    definitions = fetch_definitions(api_token)
    key = upload_json_to_s3(
        definitions,
        f"tiingo/json/fundamentals/definitions/date={date_partition}/definitions.json",
        S3_BUCKET_NAME, aws_credentials,
    )
    uploaded_keys.append(key)

    # 2. Meta (single bulk call with all tickers)
    meta = fetch_meta(api_token, tickers)
    key = upload_json_to_s3(
        meta,
        f"tiingo/json/fundamentals/meta/date={date_partition}/meta.json",
        S3_BUCKET_NAME, aws_credentials,
    )
    uploaded_keys.append(key)

    # 3. Per-ticker: daily metrics + statements (both variants)
    for ticker in tickers:
        daily_data = fetch_fundamentals_daily(ticker, api_token)
        key = upload_json_to_s3(
            daily_data,
            f"tiingo/json/fundamentals/daily/load_type=daily/date={date_partition}/{ticker}.json",
            S3_BUCKET_NAME, aws_credentials,
        )
        uploaded_keys.append(key)

        for as_reported in [True, False]:
            variant = "true" if as_reported else "false"
            stmts = fetch_statements(ticker, api_token, as_reported)
            key = upload_json_to_s3(
                stmts,
                f"tiingo/json/fundamentals/statements/as_reported={variant}/load_type=daily/date={date_partition}/{ticker}.json",
                S3_BUCKET_NAME, aws_credentials,
            )
            uploaded_keys.append(key)

    logger.info("=" * 60)
    logger.info("Fundamentals Daily Flow completed successfully!")
    logger.info(f"Uploaded {len(uploaded_keys)} files to S3:")
    for key in uploaded_keys:
        logger.info(f"  - s3://{S3_BUCKET_NAME}/{key}")
    logger.info("=" * 60)

    return uploaded_keys


if __name__ == "__main__":
    tiingo_fundamentals_flow()
