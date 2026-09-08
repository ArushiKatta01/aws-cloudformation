"""
csv_transform_job.py
---------------------
AWS Glue **Python Shell** job for the CloudFormation CSV workflow.

Why this looks different from a typical Glue script:
Python Shell jobs run plain Python on a single small machine (0.0625 or 1
DPU) - there is NO Spark cluster behind them. That means:
  - no `pyspark`, no `SparkContext` / `GlueContext`
  - no `awsglue.transforms` (DropNullFields, etc.) or DynamicFrames
  - no distributed processing - everything happens in one Python process
Only plain Python plus a small set of AWS-preinstalled libraries are
available: boto3, pandas, numpy, and a few others. This script uses only
boto3 (to talk to S3) and pandas (to read/transform/write CSV).

What it does:
1. Lists every .csv object under s3://<SRC_BUCKET>/<SRC_PREFIX>.
2. Reads each one into a pandas DataFrame and concatenates them.
3. Transforms: drops columns that are 100% null, adds a processed_at
   timestamp column.
4. Writes a single combined CSV to s3://<DEST_BUCKET>/<DEST_PREFIX>.

This script is uploaded to:
    s3://<UploadBucketName>/scripts/csv_transform_job.py
and referenced by the Glue Job resource's ScriptLocation property in
template.yaml (Command.Name = "pythonshell").
"""

import sys
import io
from datetime import datetime, timezone

import boto3
import pandas as pd

# NOTE: awsglue.utils getResolvedOptions has been removed intentionally.
# Workflows pass arguments in a raw format that frequently breaks Glue's native
# internal argparse parser. This custom block resolves arguments cleanly instead.

# ---------------------------------------------------------------------------
# Zero-Dependency Argument Resolution
# Completely bypasses getResolvedOptions to prevent internal argparse crashes
# ---------------------------------------------------------------------------
args = {}

# 1. Harvest standard style parameters (--KEY value)
for i in range(1, len(sys.argv) - 1):
    if sys.argv[i].startswith('--'):
        key = sys.argv[i].lstrip('-')
        args[key] = sys.argv[i+1]

# 2. Harvest non-prefixed key-value pairings passed by Workflows
for i in range(1, len(sys.argv)):
    if sys.argv[i] in ["JOB_NAME", "SRC_BUCKET", "SRC_PREFIX", "DEST_BUCKET", "DEST_PREFIX"]:
        if i + 1 < len(sys.argv):
            args[sys.argv[i]] = sys.argv[i+1]

# 3. Apply safe defaults if structural keys are missing
if "JOB_NAME" not in args:
    args["JOB_NAME"] = "glue-job-run"

s3 = boto3.client("s3")


def list_csv_keys(bucket: str, prefix: str) -> list:
    """Return every .csv object key under a bucket/prefix (paginated)."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                keys.append(key)
    return keys


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Download one CSV object and load it into a pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def main():
    src_bucket = args.get("SRC_BUCKET")
    src_prefix = args.get("SRC_PREFIX")
    dest_bucket = args.get("DEST_BUCKET")
    dest_prefix = args.get("DEST_PREFIX")

    # Raise clear error traces if arguments are missing completely
    if not all([src_bucket, src_prefix, dest_bucket, dest_prefix]):
        print(f"ERROR: Missing required arguments. Extracted arguments: {args}")
        sys.exit(1)

    print(f"Looking for CSV files under s3://{src_bucket}/{src_prefix}")
    keys = list_csv_keys(src_bucket, src_prefix)

    if not keys:
        print("No CSV files found - nothing to process. Exiting.")
        return

    frames = []
    for key in keys:
        print(f"Reading s3://{src_bucket}/{key}")
        frames.append(read_csv_from_s3(src_bucket, key))

    df = pd.concat(frames, ignore_index=True)
    print(f"Combined {len(keys)} file(s) into {len(df)} rows.")

    # ---- Transform -------------------------------------------------------
    # Drop any column that is null in every single row.
    df = df.dropna(axis=1, how="all")
    # Stamp every row with the UTC time this run processed it.
    df["processed_at"] = datetime.now(timezone.utc).isoformat()

    # ---- Write output ------------------------------------------------------
    out_buffer = io.StringIO()
    df.to_csv(out_buffer, index=False)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_key = f"{dest_prefix.rstrip('/')}/transformed_{timestamp}.csv"

    s3.put_object(
        Bucket=dest_bucket,
        Key=out_key,
        Body=out_buffer.getvalue().encode("utf-8"),
    )
    print(f"Wrote output to s3://{dest_bucket}/{out_key}")


if __name__ == "__main__":
    main()
