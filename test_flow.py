"""
Test flow to verify managed work pool environment.

Validates that all imports, credentials, and S3 access work
without touching production data.
"""

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_aws import AwsCredentials
import requests
import json


@task(retries=1, retry_delay_seconds=5)
def test_s3_access(aws_credentials: AwsCredentials) -> str:
    logger = get_run_logger()
    s3_client = aws_credentials.get_boto3_session().client('s3')
    response = s3_client.get_object(Bucket="mh-guess-data", Key="adhoc/tickers.txt")
    tickers = response['Body'].read().decode('utf-8').strip().split('\n')
    logger.info(f"S3 access OK: loaded {len(tickers)} tickers")
    return f"{len(tickers)} tickers"


@task(retries=1, retry_delay_seconds=5)
def test_tiingo_access(api_token: str) -> str:
    logger = get_run_logger()
    response = requests.get(
        "https://api.tiingo.com/tiingo/fundamentals/definitions",
        headers={'Authorization': f'Token {api_token}', 'Content-Type': 'application/json'},
    )
    response.raise_for_status()
    data = response.json()
    logger.info(f"Tiingo API OK: {len(data)} definitions returned")
    return f"{len(data)} definitions"


@flow(name="Environment Test")
def test_flow():
    logger = get_run_logger()
    logger.info("Testing managed work pool environment...")

    tiingo_token = Secret.load("tiingo-api-token").get()
    aws_credentials = AwsCredentials.load("aws-credentials-tim")

    s3_result = test_s3_access(aws_credentials)
    tiingo_result = test_tiingo_access(tiingo_token)

    logger.info(f"All checks passed: S3={s3_result}, Tiingo={tiingo_result}")
    return {"s3": s3_result, "tiingo": tiingo_result}


if __name__ == "__main__":
    test_flow()
