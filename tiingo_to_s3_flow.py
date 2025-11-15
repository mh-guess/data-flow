"""
Tiingo to S3 Daily ETL Pipeline

Fetches daily price data from Tiingo API and loads it to AWS S3.
Uses Prefect Cloud Blocks for secure credential management.

S3 Structure: s3://mh-guess-data/tiingo/json/load_type=daily/date={YYYY-MM-DD}/{ticker}.json

This is the incremental daily pipeline. For historical backfills, see tiingo_backfill_flow.py.
"""

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
from datetime import datetime, timedelta
import requests
import json


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
def extract_ticker_data(ticker: str, api_token: str) -> dict:
    """
    Extract daily price data from Tiingo API for a single ticker.

    Args:
        ticker: Stock ticker symbol
        api_token: Tiingo API token

    Returns:
        Dictionary with ticker data
    """
    logger = get_run_logger()
    logger.info(f"Extracting data for {ticker}...")

    # Tiingo API endpoint for daily prices
    # Get last 30 days of data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)

    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Token {api_token}'
    }
    params = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d')
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Extracted {len(data)} records for {ticker}")

    return {
        "ticker": ticker,
        "data": data,
        "extracted_at": datetime.now().isoformat()
    }


@task
def extract_all_tickers(tickers: list, api_token: str) -> list:
    """
    Extract data for all tickers.

    Args:
        tickers: List of ticker symbols
        api_token: Tiingo API token

    Returns:
        List of ticker data dictionaries
    """
    all_data = []
    for ticker in tickers:
        ticker_data = extract_ticker_data(ticker, api_token)
        all_data.append(ticker_data)

    return all_data


@task
def transform_data(ticker_data_list: list) -> dict:
    """
    Transform extracted data into a structured format.

    Args:
        ticker_data_list: List of ticker data dictionaries

    Returns:
        Transformed data dictionary
    """
    logger = get_run_logger()
    logger.info("Transforming data...")

    # Extract ticker symbols from the data
    tickers = [td["ticker"] for td in ticker_data_list]

    transformed = {
        "metadata": {
            "extracted_at": datetime.now().isoformat(),
            "tickers": tickers,
            "record_count": sum(len(td["data"]) for td in ticker_data_list)
        },
        "tickers": {}
    }

    for ticker_data in ticker_data_list:
        ticker = ticker_data["ticker"]
        transformed["tickers"][ticker] = ticker_data["data"]

    logger.info(f"Transformed data for {len(ticker_data_list)} tickers")
    return transformed


@task(retries=2, retry_delay_seconds=5)
def load_to_s3(ticker_data_list: list, bucket_name: str, aws_credentials: AwsCredentials) -> list:
    """
    Load raw Tiingo data to AWS S3, partitioned by date.
    Saves data exactly as received from Tiingo API without any transformation.

    Args:
        ticker_data_list: List of raw ticker data from Tiingo API
        bucket_name: S3 bucket name
        aws_credentials: AWS credentials from Prefect Cloud

    Returns:
        List of S3 keys for uploaded files
    """
    logger = get_run_logger()
    logger.info(f"Loading data to S3 bucket: {bucket_name}...")

    # Generate date partition (YYYY-MM-DD format)
    current_date = datetime.now()
    date_partition = current_date.strftime('%Y-%m-%d')

    # Get S3 client using AWS credentials from Prefect Cloud
    s3_client = aws_credentials.get_boto3_session().client('s3')
    uploaded_keys = []

    # Save each ticker's raw data separately with type and date partitioning
    for ticker_data in ticker_data_list:
        ticker = ticker_data["ticker"]
        raw_data = ticker_data["data"]  # Raw data from Tiingo API

        # Create S3 key: tiingo/json/load_type=daily/date={YYYY-MM-DD}/{ticker}.json
        s3_key = f"tiingo/json/load_type=daily/date={date_partition}/{ticker}.json"

        # Save raw data as-is (compact JSON, no pretty formatting)
        json_data = json.dumps(raw_data)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )

        uploaded_keys.append(s3_key)
        logger.info(f"Uploaded {ticker} raw data ({len(raw_data)} records) to s3://{bucket_name}/{s3_key}")

    logger.info(f"Successfully loaded {len(uploaded_keys)} files to S3")
    return uploaded_keys


@flow(name="Tiingo to S3 ETL")
def tiingo_to_s3_flow():
    """
    Main ETL flow that orchestrates data extraction from Tiingo
    and loading to S3.
    """
    logger = get_run_logger()
    logger.info("="*60)
    logger.info("Starting Tiingo to S3 ETL Flow")
    logger.info("="*60)

    # Load credentials from Prefect Cloud Blocks
    logger.info("Loading Tiingo API token from Prefect Cloud...")
    tiingo_token_block = Secret.load("tiingo-api-token")
    api_token = tiingo_token_block.get()

    logger.info("Loading AWS credentials from Prefect Cloud...")
    aws_credentials = AwsCredentials.load("aws-credentials-tim")

    # Fetch tickers from S3
    tickers = fetch_tickers_from_s3(S3_BUCKET_NAME, TICKERS_S3_KEY, aws_credentials)

    # Extract
    raw_data = extract_all_tickers(tickers, api_token)

    # Load raw data directly to S3 (no transformation)
    s3_keys = load_to_s3(raw_data, S3_BUCKET_NAME, aws_credentials)

    logger.info("="*60)
    logger.info(f"Flow completed successfully!")
    logger.info(f"Uploaded {len(s3_keys)} files to S3:")
    for key in s3_keys:
        logger.info(f"  - s3://{S3_BUCKET_NAME}/{key}")
    logger.info("="*60)

    return s3_keys


if __name__ == "__main__":
    # Run the flow
    tiingo_to_s3_flow()
