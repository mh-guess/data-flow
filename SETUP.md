# Prefect Cloud Setup Guide

This guide explains how to configure credentials in Prefect Cloud for the Tiingo to S3 ETL pipeline.

## Prerequisites

- Prefect Cloud account (free at https://app.prefect.cloud)
- Tiingo API token
- AWS Access Key ID and Secret Access Key with S3 permissions

## Step 1: Login to Prefect Cloud

```bash
prefect cloud login
```

Follow the prompts to authenticate with your Prefect Cloud account.

## Step 2: Create AWS Credentials Block

You can create the AWS credentials block either via the UI or CLI.

### Option A: Using Prefect Cloud UI (Recommended)

1. Go to https://app.prefect.cloud
2. Navigate to **Blocks** in the left sidebar
3. Click **+** (Add Block)
4. Search for and select **AWS Credentials**
5. Fill in the form:
   - **Block Name**: `aws-credentials`
   - **AWS Access Key ID**: Your AWS access key
   - **AWS Secret Access Key**: Your AWS secret key
   - **Region Name** (optional): Your preferred AWS region (e.g., `us-east-1`)
6. Click **Create**

### Option B: Using Python Script

Create a file named `create_aws_block.py`:

```python
from prefect_aws import AwsCredentials

# Create AWS credentials block
aws_credentials = AwsCredentials(
    aws_access_key_id="YOUR_ACCESS_KEY_ID",
    aws_secret_access_key="YOUR_SECRET_ACCESS_KEY",
    region_name="us-east-1"  # Optional: change to your region
)

# Save to Prefect Cloud
aws_credentials.save("aws-credentials", overwrite=True)
print("AWS credentials block created successfully!")
```

Run it:
```bash
python create_aws_block.py
```

## Step 3: Verify Tiingo API Token Block

You mentioned you already have a Secret block named `tiingo-api-token`. If you need to create or update it:

### Using Prefect Cloud UI

1. Go to https://app.prefect.cloud
2. Navigate to **Blocks**
3. Click **+** (Add Block)
4. Select **Secret**
5. Fill in:
   - **Block Name**: `tiingo-api-token`
   - **Value**: Your Tiingo API token
6. Click **Create**

### Using Python Script

```python
from prefect.blocks.system import Secret

# Create Secret block
secret = Secret(value="YOUR_TIINGO_API_TOKEN")
secret.save("tiingo-api-token", overwrite=True)
print("Tiingo API token block created successfully!")
```

## Step 4: Verify Your Blocks

List all blocks to confirm they're created:

```bash
prefect block ls
```

You should see:
- `aws-credentials` (AwsCredentials)
- `tiingo-api-token` (Secret)

## Step 5: Run the Pipeline

Now you can run the pipeline without needing to set environment variables:

```bash
python tiingo_to_s3_flow.py
```

The pipeline will automatically load credentials from Prefect Cloud!

## Troubleshooting

### Block not found error
- Ensure you're logged into Prefect Cloud: `prefect cloud login`
- Verify block names match exactly (case-sensitive)
- Check blocks exist in UI: https://app.prefect.cloud

### AWS permission errors
- Verify your AWS credentials have S3 permissions
- Ensure the bucket `mh-guess-data` exists or your credentials can create it
- Check the region matches where your bucket is located

### Tiingo API errors
- Verify your API token is valid at https://www.tiingo.com
- Check you haven't exceeded API rate limits
