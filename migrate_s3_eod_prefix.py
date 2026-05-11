"""
One-time S3 migration: move EOD price data under price_eod/ prefix.

Copies objects from:
  tiingo/json/load_type=daily/...  -> tiingo/json/price_eod/load_type=daily/...
  tiingo/json/load_type=retro/...  -> tiingo/json/price_eod/load_type=retro/...

Does NOT delete originals. Manual cleanup after verification.

Usage: python migrate_s3_eod_prefix.py [--dry-run]
"""

import boto3
import sys

S3_BUCKET_NAME = "mh-guess-data"
OLD_PREFIX = "tiingo/json/load_type="


def list_objects(s3_client, bucket_name: str, prefix: str) -> list:
    """List all S3 objects under the given prefix, handling pagination."""
    all_keys = []
    continuation_token = None

    while True:
        kwargs = {'Bucket': bucket_name, 'Prefix': prefix}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**kwargs)

        if 'Contents' in response:
            for obj in response['Contents']:
                all_keys.append(obj['Key'])

        if response.get('IsTruncated'):
            continuation_token = response['NextContinuationToken']
        else:
            break

    return all_keys


def migrate_eod_prefix(dry_run: bool = False):
    """Copy existing EOD data from tiingo/json/load_type=* to tiingo/json/price_eod/load_type=*."""
    print("=" * 60)
    print("S3 Migration: EOD data -> price_eod/ prefix")
    print(f"Dry run: {dry_run}")
    print("=" * 60)

    s3_client = boto3.client('s3')

    source_keys = list_objects(s3_client, S3_BUCKET_NAME, OLD_PREFIX)
    print(f"Found {len(source_keys)} objects under s3://{S3_BUCKET_NAME}/{OLD_PREFIX}")

    if not source_keys:
        print("No objects found to migrate.")
        return

    copied = []
    for source_key in source_keys:
        suffix = source_key[len("tiingo/json/"):]
        dest_key = f"tiingo/json/price_eod/{suffix}"

        if dry_run:
            print(f"  [DRY RUN] {source_key} -> {dest_key}")
        else:
            s3_client.copy_object(
                Bucket=S3_BUCKET_NAME,
                CopySource={'Bucket': S3_BUCKET_NAME, 'Key': source_key},
                Key=dest_key,
            )
            copied.append(dest_key)
            print(f"  Copied: {source_key} -> {dest_key}")

    print("=" * 60)
    if dry_run:
        print(f"Dry run complete. {len(source_keys)} objects would be copied.")
    else:
        print(f"Migration complete. {len(copied)} objects copied.")
    print("Originals NOT deleted. Verify new paths, then clean up manually.")
    print("=" * 60)


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    migrate_eod_prefix(dry_run=dry_run)
