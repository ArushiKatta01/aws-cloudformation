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
  - no job bookmarks (`--job-bookmark-option` is a Spark-job feature), so
    this script tracks "already processed" files itself (see below)
Only plain Python plus a small set of AWS-preinstalled libraries are
available: boto3, pandas, numpy, and a few others. This script uses boto3
(to talk to S3), pandas (to read/transform/write CSV), and
awsglue.utils.getResolvedOptions (pure stdlib under the hood, no PySpark
dependency, so it works fine in Python Shell too).

What it does:
1. Lists every .csv object under s3://<SRC_BUCKET>/<SRC_PREFIX>, keyed by
   ETag.
2. Loads a manifest (s3://<DEST_BUCKET>/<DEST_PREFIX>/_manifest.json) that
   records the ETag of every source key already processed, and skips
   anything unchanged since the last run - so re-running the job doesn't
   reprocess (or re-combine) files it has already handled.
3. For each new/changed CSV: drops columns that are 100% null, adds a
   processed_at timestamp, and writes it to s3://<DEST_BUCKET>/<DEST_PREFIX>/
   as its own output file. Files are NOT concatenated across sources -
   different source CSVs can have entirely different columns, and combining
   them would pad every mismatched column with nulls.
4. Updates the manifest with the newly processed keys' ETags.

This script is uploaded to:
    s3://<UploadBucketName>/scripts/csv_transform_job.py
and referenced by the Glue Job resource's ScriptLocation property in
template.yaml (Command.Name = "pythonshell").
"""

import io
import json
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
from awsglue.utils import getResolvedOptions

# getResolvedOptions must be given every "--key" that actually appears on
# the job's command line, not just the ones this script cares about. Glue
# passes through ALL of DefaultArguments (template.yaml sets --job-language
# and --TempDir too) plus --JOB_NAME, and getResolvedOptions uses getopt
# under the hood - any argument present in sys.argv but missing from this
# list makes getopt raise "option not recognized" and crash. That crash
# (not some general unreliability in Glue's parser) was the real issue.
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "job-language",
        "TempDir",
        "SRC_BUCKET",
        "SRC_PREFIX",
        "DEST_BUCKET",
        "DEST_PREFIX",
    ],
)

s3 = boto3.client("s3")


def list_csv_objects(bucket: str, prefix: str) -> dict:
    """Return {key: etag} for every .csv object under bucket/prefix."""
    objects = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".csv"):
                objects[key] = obj["ETag"]
    return objects


def load_manifest(bucket: str, manifest_key: str) -> dict:
    """Return {source_key: etag} for files processed by previous runs."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=manifest_key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return {}


def save_manifest(bucket: str, manifest_key: str, manifest: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest).encode("utf-8"),
    )


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Download one CSV object and load it into a pandas DataFrame."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Drop any column that is null in every single row.
    df = df.dropna(axis=1, how="all")
    # Stamp every row with the UTC time this run processed it.
    df["processed_at"] = datetime.now(timezone.utc).isoformat()
    return df


def output_key_for(dest_prefix: str, source_key: str) -> str:
    stem = source_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{dest_prefix.rstrip('/')}/{stem}_transformed_{timestamp}.csv"


def main():
    src_bucket = args["SRC_BUCKET"]
    src_prefix = args["SRC_PREFIX"]
    dest_bucket = args["DEST_BUCKET"]
    dest_prefix = args["DEST_PREFIX"]

    manifest_key = f"{dest_prefix.rstrip('/')}/_manifest.json"
    manifest = load_manifest(dest_bucket, manifest_key)

    print(f"Looking for CSV files under s3://{src_bucket}/{src_prefix}")
    current = list_csv_objects(src_bucket, src_prefix)

    new_keys = [key for key, etag in current.items() if manifest.get(key) != etag]

    if not new_keys:
        print("No new or changed CSV files since the last run - nothing to process.")
        return

    for key in new_keys:
        print(f"Processing s3://{src_bucket}/{key}")
        df = transform(read_csv_from_s3(src_bucket, key))

        out_buffer = io.StringIO()
        df.to_csv(out_buffer, index=False)
        out_key = output_key_for(dest_prefix, key)
        s3.put_object(
            Bucket=dest_bucket,
            Key=out_key,
            Body=out_buffer.getvalue().encode("utf-8"),
        )
        print(f"Wrote output to s3://{dest_bucket}/{out_key}")

        manifest[key] = current[key]

    save_manifest(dest_bucket, manifest_key, manifest)
    print(f"Updated manifest with {len(new_keys)} newly processed file(s).")


if __name__ == "__main__":
    main()
