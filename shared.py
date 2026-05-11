"""
Shared utilities for Tiingo ETL pipelines.

Common tasks and helpers used across daily, backfill, and fundamentals flows.
"""

from prefect import task, get_run_logger
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
import json

S3_BUCKET_NAME = "mh-guess-data"
TICKERS_S3_KEY = "adhoc/tickers.txt"


def load_credentials():
    """Load Tiingo API token and AWS credentials from Prefect Cloud blocks."""
    logger = get_run_logger()
    logger.info("Loading Tiingo API token from Prefect Cloud...")
    tiingo_token_block = Secret.load("tiingo-api-token")
    api_token = tiingo_token_block.get()

    logger.info("Loading AWS credentials from Prefect Cloud...")
    aws_credentials = AwsCredentials.load("aws-credentials-tim")

    return api_token, aws_credentials


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

    s3_client = aws_credentials.get_boto3_session().client('s3')

    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    tickers_content = response['Body'].read().decode('utf-8')

    tickers = [line.strip() for line in tickers_content.strip().split('\n') if line.strip()]

    logger.info(f"Loaded {len(tickers)} tickers: {', '.join(tickers)}")
    return tickers


@task(retries=2, retry_delay_seconds=5)
def upload_json_to_s3(data, s3_key: str, bucket_name: str, aws_credentials: AwsCredentials) -> str:
    """Upload raw data as compact JSON to S3."""
    logger = get_run_logger()
    s3_client = aws_credentials.get_boto3_session().client('s3')
    json_data = json.dumps(data)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json_data,
        ContentType='application/json'
    )
    logger.info(f"Uploaded to s3://{bucket_name}/{s3_key}")
    return s3_key
