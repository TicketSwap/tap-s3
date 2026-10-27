"""Serve the test bucket from a moto S3 mock instead of real AWS."""

from __future__ import annotations

import datetime as dt
import io
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws

BUCKET = "my-bucket"
PREFIX = "exports/2026-08-01/"
REGION = "eu-west-1"

_aws = mock_aws()


def _parquet_bytes() -> bytes:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["a", "b", None], pa.string()),
            "amount": pa.array([1.5, 2.0, 3.25], pa.float64()),
            "is_active": pa.array([True, False, True], pa.bool_()),
            "created_at": pa.array(
                [dt.datetime(2026, 8, 1, hour, tzinfo=dt.UTC) for hour in range(3)],
                pa.timestamp("us", tz="UTC"),
            ),
            "tags": pa.array([["x"], [], ["y", "z"]], pa.list_(pa.string())),
            "details": pa.array(
                [{"source": "web"}, {"source": "app"}, {"source": None}],
                pa.struct([("source", pa.string())]),
            ),
        },
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def pytest_configure() -> None:
    """Start the S3 mock before collection, which already discovers the stream schema."""
    os.environ.pop("AWS_PROFILE", None)
    os.environ.update(
        {
            "AWS_ACCESS_KEY_ID": "testing",
            "AWS_SECRET_ACCESS_KEY": "testing",
            "AWS_DEFAULT_REGION": REGION,
        },
    )
    _aws.start()
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}part-0.parquet", Body=_parquet_bytes())
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}_SUCCESS", Body=b"")


def pytest_unconfigure() -> None:
    """Stop the S3 mock."""
    _aws.stop()
