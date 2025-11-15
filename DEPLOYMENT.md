# Deployment Guide

This guide explains how to manage Prefect deployments as Infrastructure as Code using `prefect.yaml`.

## Overview

The `prefect.yaml` file defines our deployment configuration including:
- Flow entrypoint and metadata
- Schedule (6 PM weekdays, Pacific Time)
- GitHub repository configuration
- Python dependencies
- Tags and versioning

## Deploying with prefect.yaml

### Method 1: Using Prefect CLI (Recommended for IaC)

```bash
# Login to Prefect Cloud
prefect cloud login

# Deploy using prefect.yaml
prefect deploy --all

# Or deploy a specific deployment
prefect deploy -n tiingo-to-s3-production
```

This method:
- ✅ Uses version-controlled configuration
- ✅ Tracks all changes in Git
- ✅ Ensures consistency across environments
- ✅ Supports multiple deployments in one file

### Method 2: Using prefect-cloud CLI (Quick deployments)

```bash
# Quick deployment (what we used initially)
uvx prefect-cloud deploy tiingo_to_s3_flow.py:tiingo_to_s3_flow \
  --from mh-guess/data-flow \
  --with-requirements requirements.txt
```

This method:
- ⚠️ Creates deployments imperatively
- ⚠️ Configuration not tracked in Git
- ✅ Faster for one-off changes

## Updating Deployments

When you need to change the deployment (schedule, dependencies, etc.):

1. **Update `prefect.yaml`** with your changes
2. **Commit to Git**: `git add prefect.yaml && git commit -m "Update deployment config"`
3. **Push**: `git push`
4. **Redeploy**: `prefect deploy --all`

## Schedule Configuration

Current schedule in `prefect.yaml`:
```yaml
schedules:
  - cron: "0 18 * * 1-5"
    timezone: "America/Los_Angeles"
    active: true
```

- **Time**: 6:00 PM Pacific Time
- **Days**: Monday through Friday
- **Timezone**: America/Los_Angeles (handles PST/PDT automatically)

## Version Control Benefits

By using `prefect.yaml`, you get:
- 📝 Full deployment history in Git
- 🔄 Easy rollbacks to previous configurations
- 👥 Team collaboration on deployment changes
- 🔍 Clear audit trail of what changed and when
- 🚀 Reproducible deployments across environments

## Manual Flow Runs

To manually trigger a flow run:

```bash
# Using prefect-cloud CLI
uvx prefect-cloud run tiingo_to_s3_flow/tiingo_to_s3_flow

# Or using Prefect CLI
prefect deployment run tiingo-to-s3-production
```

## Removing Schedule

To unschedule but keep the deployment:

```bash
uvx prefect-cloud schedule tiingo_to_s3_flow/tiingo_to_s3_flow none
```

Or update `prefect.yaml` to set `active: false` and redeploy.
