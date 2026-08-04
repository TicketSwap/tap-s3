"""Stream type classes for tap-s3."""

from __future__ import annotations

from tap_s3.client import S3Stream


class ParquetFolderStream(S3Stream):
    """Records read from every Parquet file in the configured S3 bucket/prefix."""

    name = "records"
    primary_keys: tuple[str, ...] = ()
    replication_key = None
