"""Tests standard tap features using the built-in SDK tests library.

The bucket is a moto S3 mock set up in conftest.py, so no AWS access is needed.
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
