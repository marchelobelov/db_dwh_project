"""S3 (Yandex Object Storage) reader built from the Airflow connection.

We build a boto3 client straight from the `s3_avia_ru` connection so it works
regardless of which provider package is installed. Credentials come from the
connection (login/password or the extra), the endpoint defaults to the Yandex
endpoint in pipeline.yaml if the connection doesn't specify one.
"""
import gzip
import io

import boto3
import pandas as pd
from airflow.hooks.base import BaseHook

from .config import S3_CONN_ID, S3_BUCKET, S3_SOURCE_PREFIX, S3_ENDPOINT_URL, S3_REGION
from .logging_utils import get_logger

log = get_logger(__name__)


def get_s3_client():
    conn = BaseHook.get_connection(S3_CONN_ID)
    extra = conn.extra_dejson or {}
    access_key = conn.login or extra.get("aws_access_key_id")
    secret_key = conn.password or extra.get("aws_secret_access_key")
    endpoint = (extra.get("endpoint_url") or extra.get("host")
                or extra.get("endpoint") or S3_ENDPOINT_URL)
    region = extra.get("region_name") or extra.get("region") or S3_REGION
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def list_partition_keys(date_str: str, client=None) -> list[str]:
    """List flights_us_data/<date>/*.csv(.gz) object keys for a single day."""
    client = client or get_s3_client()
    prefix = f"{S3_SOURCE_PREFIX}/{date_str}/"
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv.gz") or key.endswith(".csv"):
                keys.append(key)
    log.info("Found %d source files under s3://%s/%s", len(keys), S3_BUCKET, prefix)
    return keys


def read_csv_object(key: str, client=None) -> pd.DataFrame:
    """Read one (optionally gzipped) CSV object into a string DataFrame."""
    client = client or get_s3_client()
    body = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz"):
        body = gzip.decompress(body)
    return pd.read_csv(io.BytesIO(body), dtype=str, low_memory=False)


def read_partition(date_str: str) -> pd.DataFrame:
    """Read and concatenate every file in the day partition.

    Adds a `source_file` column; returns an empty DataFrame if the partition is
    absent.
    """
    client = get_s3_client()
    keys = list_partition_keys(date_str, client=client)
    frames = []
    for key in keys:
        df = read_csv_object(key, client=client)
        df["source_file"] = key
        frames.append(df)
        log.info("Read %d rows from %s", len(df), key)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
