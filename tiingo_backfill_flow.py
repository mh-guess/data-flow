"""
Tiingo Historical Data Backfill Pipeline

Fetches historical price data from Tiingo API and loads to S3 with year-level partitioning.
Uses type-partitioned structure for efficient querying and processing.

S3 Structure: s3://mh-guess-data/tiingo/json/load_type=retro/year={YYYY}/{ticker}.json

Each file contains all trading days for that ticker in that year (~252 records).
This minimizes API calls: 1 call per ticker per year instead of 1 per day.
"""

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from datetime import datetime
import requests
import json
from typing import Optional, List


# Configuration
S3_BUCKET_NAME = "mh-guess-data"
TICKERS_S3_KEY = "adhoc/tickers.txt"


@task(retries=2, retry_delay_seconds=5)
def fetch_tickers_from_s3(bucket_name: str, s3_key: str, aws_credentials: AwsCredentials) -> list:
    """
    Fetch ticker list from S3 file.

    Args:
        bucket_name: S3 bucket name
        s3_key: S3 key for the tickers file
        aws_credentials: AWS credentials from Prefect Cloud

    Returns:
        List of ticker symbols
    """
    logger = get_run_logger()
    logger.info(f"Fetching tickers from s3://{bucket_name}/{s3_key}...")

    # Get S3 client using AWS credentials from Prefect Cloud
    s3_client = aws_credentials.get_boto3_session().client('s3')

    # Fetch the tickers file
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    tickers_content = response['Body'].read().decode('utf-8')

    # Parse tickers (one per line, strip whitespace)
    tickers = [line.strip() for line in tickers_content.strip().split('\n') if line.strip()]

    logger.info(f"Loaded {len(tickers)} tickers: {', '.join(tickers)}")
    return tickers


@task(retries=3, retry_delay_seconds=10)
def fetch_year_data(ticker: str, year: int, api_token: str) -> dict:
    """
    Fetch all daily price data for a ticker for a specific year from Tiingo API.

    Args:
        ticker: Stock ticker symbol
        year: Year to fetch (e.g., 2020)
        api_token: Tiingo API token

    Returns:
        Dictionary with ticker, year, and data
    """
    logger = get_run_logger()
    logger.info(f"Fetching {year} data for {ticker}...")

    # Tiingo API endpoint for daily prices
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token {api_token}'
    }
    params = {
        'startDate': start_date,
        'endDate': end_date
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} records for {ticker} in {year}")

    return {
        "ticker": ticker,
        "year": year,
        "data": data,
        "record_count": len(data),
        "fetched_at": datetime.now().isoformat()
    }


@task(retries=2, retry_delay_seconds=5)
def load_year_to_s3(
    ticker_year_data: dict,
    bucket_name: str,
    aws_credentials: AwsCredentials
) -> str:
    """
    Load year-level historical data to S3 with type=retro partition.

    Args:
        ticker_year_data: Dictionary with ticker, year, and data
        bucket_name: S3 bucket name
        aws_credentials: AWS credentials from Prefect Cloud

    Returns:
        S3 key of uploaded file
    """
    logger = get_run_logger()
    ticker = ticker_year_data["ticker"]
    year = ticker_year_data["year"]
    data = ticker_year_data["data"]

    # Create S3 key: tiingo/json/load_type=retro/year={YYYY}/{ticker}.json
    s3_key = f"tiingo/json/load_type=retro/year={year}/{ticker}.json"

    logger.info(f"Loading {ticker} {year} data ({len(data)} records) to S3...")

    # Get S3 client using AWS credentials from Prefect Cloud
    s3_client = aws_credentials.get_boto3_session().client('s3')

    # Save raw data as-is (compact JSON, no pretty formatting)
    json_data = json.dumps(data)

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json_data,
        ContentType='application/json'
    )

    logger.info(f"Uploaded to s3://{bucket_name}/{s3_key}")
    return s3_key


@flow(name="Tiingo Historical Backfill")
def tiingo_backfill_flow(
    start_year: int,
    end_year: int,
    tickers: Optional[List[str]] = None
):
    """
    Backfill historical Tiingo data with year-level partitioning.

    This flow fetches historical price data and saves it to S3 with:
    - Type partition: load_type=retro
    - Year partition: year={YYYY}
    - One file per ticker per year

    Args:
        start_year: Start year for backfill (inclusive), e.g., 2020
        end_year: End year for backfill (inclusive), e.g., 2024
        tickers: Optional list of tickers. If None, fetches from S3.

    Example:
        # Backfill 2020-2024 for all tickers in S3
        tiingo_backfill_flow(start_year=2020, end_year=2024)

        # Backfill specific tickers
        tiingo_backfill_flow(start_year=2020, end_year=2024, tickers=["AAPL", "TSLA"])
    """
    logger = get_run_logger()
    logger.info("="*60)
    logger.info("Starting Tiingo Historical Backfill")
    logger.info(f"Years: {start_year} to {end_year}")
    logger.info("="*60)

    # Load credentials from Prefect Cloud Blocks
    logger.info("Loading Tiingo API token from Prefect Cloud...")
    tiingo_token_block = Secret.load("tiingo-api-token")
    api_token = tiingo_token_block.get()

    logger.info("Loading AWS credentials from Prefect Cloud...")
    aws_credentials = AwsCredentials.load("aws-credentials-tim")

    # Fetch tickers
    if tickers is None:
        logger.info("No tickers provided, fetching from S3...")
        tickers = fetch_tickers_from_s3(S3_BUCKET_NAME, TICKERS_S3_KEY, aws_credentials)
    else:
        logger.info(f"Using provided tickers: {', '.join(tickers)}")

    # Calculate total operations
    years = list(range(start_year, end_year + 1))
    total_ops = len(tickers) * len(years)
    logger.info(f"Backfill plan: {len(tickers)} tickers × {len(years)} years = {total_ops} API calls")

    # Track results
    uploaded_keys = []

    # Process each ticker and year
    for ticker in tickers:
        for year in years:
            # Fetch year data
            year_data = fetch_year_data(ticker, year, api_token)

            # Load to S3
            s3_key = load_year_to_s3(year_data, S3_BUCKET_NAME, aws_credentials)
            uploaded_keys.append(s3_key)

    logger.info("="*60)
    logger.info("Backfill completed successfully!")
    logger.info(f"Total files uploaded: {len(uploaded_keys)}")
    logger.info(f"Years processed: {start_year}-{end_year}")
    logger.info(f"Tickers processed: {', '.join(tickers)}")
    logger.info(f"Data location: s3://{S3_BUCKET_NAME}/tiingo/json/load_type=retro/")
    logger.info("="*60)

    return {
        "uploaded_keys": uploaded_keys,
        "total_files": len(uploaded_keys),
        "years": years,
        "tickers": tickers
    }


if __name__ == "__main__":
    # Example: Backfill 2020-2024 for all tickers
    tiingo_backfill_flow(start_year=2020, end_year=2024)
