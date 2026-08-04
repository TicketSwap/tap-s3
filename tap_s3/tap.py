"""S3 tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_s3 import streams

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapS3(Tap):
    """Singer tap for reading a folder of Parquet files from S3."""

    name = "tap-s3"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "bucket",
            th.StringType(nullable=False),
            required=True,
            title="S3 Bucket",
            description="The S3 bucket containing the Parquet files to extract",
        ),
        th.Property(
            "prefix",
            th.StringType(nullable=False),
            default="",
            title="S3 Prefix",
            description="Folder (key prefix) within the bucket to read Parquet files from",
        ),
        th.Property(
            "aws_profile",
            th.StringType(nullable=True),
            title="AWS Profile",
            description=(
                "Named AWS profile to use, e.g. one you've already logged into via "
                "`aws sso login --profile <name>`. Leave unset to use the default "
                "AWS credential chain."
            ),
        ),
        th.Property(
            "aws_region",
            th.StringType(nullable=True),
            title="AWS Region",
            description="AWS region of the bucket. Leave unset to use your AWS config's default",
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.S3Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ParquetFolderStream(self),
        ]


if __name__ == "__main__":
    TapS3.cli()
