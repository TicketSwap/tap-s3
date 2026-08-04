"""Tests standard tap features using the built-in SDK tests library.

NOTE: These tests require a real, already-authenticated AWS session (e.g. via
`aws sso login`) with read access to the bucket/prefix below, since the tap
lists and reads real Parquet objects from S3 rather than mocked data.
"""

from singer_sdk.testing import get_tap_test_class

from tap_s3.tap import TapS3

SAMPLE_CONFIG = {
    "bucket": "my-bucket",
    "prefix": "exports/2026-08-01/",
}


# Run standard built-in tap tests from the SDK:
TestTapS3 = get_tap_test_class(
    tap_class=TapS3,
    config=SAMPLE_CONFIG,
)
