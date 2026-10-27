"""Custom client handling, including S3Stream base class."""

from __future__ import annotations

import io
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from singer_sdk import typing as th
from singer_sdk.streams import Stream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from singer_sdk.helpers.types import Context


_SCALAR_ARROW_TYPES: tuple[tuple[Callable[[pa.DataType], bool], dict[str, Any]], ...] = (
    (pa.types.is_boolean, th.BooleanType.type_dict),
    (pa.types.is_integer, th.IntegerType.type_dict),
    (pa.types.is_floating, th.NumberType.type_dict),
    (pa.types.is_decimal, th.NumberType.type_dict),
    (pa.types.is_temporal, th.DateTimeType.type_dict),
)


def _struct_to_jsonschema(arrow_type: pa.StructType) -> dict[str, Any]:
    """Convert a PyArrow struct type into a JSON Schema object definition.

    Args:
        arrow_type: The PyArrow struct type to convert.

    Returns:
        A JSON Schema object type dictionary with one property per struct field.
    """
    return th.ObjectType(
        *(
            th.Property(
                field.name,
                th.CustomType(_arrow_type_to_jsonschema(field.type)),
                nullable=field.nullable,
            )
            for field in arrow_type
        ),
    ).type_dict


def _arrow_type_to_jsonschema(arrow_type: pa.DataType) -> dict[str, Any]:
    """Convert a PyArrow data type into a JSON Schema type definition.

    Args:
        arrow_type: The PyArrow data type to convert.

    Returns:
        A JSON Schema type dictionary describing `arrow_type`.
    """
    if pa.types.is_struct(arrow_type):
        return _struct_to_jsonschema(arrow_type)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return th.ArrayType(
            th.CustomType(_arrow_type_to_jsonschema(arrow_type.value_type)),
        ).type_dict
    for is_match, type_dict in _SCALAR_ARROW_TYPES:
        if is_match(arrow_type):
            return type_dict
    # Fall back to string for anything else (binary, etc.).
    return th.StringType.type_dict


class _S3RangeReader(io.RawIOBase):
    """A minimal seekable, read-only file object backed by S3 `Range` GETs.

    Lets PyArrow read just the Parquet footer (a HEAD request plus one or two
    small ranged GETs) instead of downloading the whole object, which is what
    `ParquetFile` needs to resolve `schema_arrow` cheaply.
    """

    def __init__(self, s3_client: Any, bucket: str, key: str) -> None:  # noqa: ANN401
        """Initialize the reader and look up the object's size.

        Args:
            s3_client: A boto3 S3 client.
            bucket: The S3 bucket containing the object.
            key: The S3 key of the object to read.
        """
        super().__init__()
        self._s3_client = s3_client
        self._bucket = bucket
        self._key = key
        self._pos = 0
        self._size: int = s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]

    @override
    def readable(self) -> bool:
        """Whether this stream supports reading.

        Returns:
            Always `True`.
        """
        return True

    @override
    def seekable(self) -> bool:
        """Whether this stream supports seeking.

        Returns:
            Always `True`.
        """
        return True

    @override
    def tell(self) -> int:
        """Return the current stream position.

        Returns:
            The current byte offset into the object.
        """
        return self._pos

    @override
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the stream position.

        Args:
            offset: Byte offset, relative to `whence`.
            whence: One of `io.SEEK_SET`, `io.SEEK_CUR`, or `io.SEEK_END`.

        Returns:
            The new absolute stream position.
        """
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._size + offset
        return self._pos

    @override
    def read(self, size: int = -1) -> bytes:
        """Read up to `size` bytes starting at the current position.

        Args:
            size: Maximum number of bytes to read. Negative reads to EOF.

        Returns:
            The bytes read, via a single `GetObject` call with a `Range` header.
        """
        if self._pos >= self._size:
            return b""
        end = self._size - 1 if size < 0 else min(self._pos + size, self._size) - 1
        response = self._s3_client.get_object(
            Bucket=self._bucket,
            Key=self._key,
            Range=f"bytes={self._pos}-{end}",
        )
        data = response["Body"].read()
        self._pos += len(data)
        return data


class S3Stream(Stream):
    """Base stream for reading a folder of Parquet files from S3.

    Authenticates with the local AWS credential chain (e.g. an `aws sso login`
    session), lists every `.parquet` object under the configured bucket/prefix, and
    yields their rows as records. The schema is inferred from the footer metadata
    of the first Parquet file found, without downloading its full contents.
    """

    @cached_property
    def s3_client(self) -> Any:  # noqa: ANN401
        """A boto3 S3 client, authenticated via the local AWS credential chain.

        Returns:
            A boto3 S3 client for the configured (or default) profile and region.
        """
        session = boto3.Session(profile_name=self.config.get("aws_profile"))
        return session.client("s3", region_name=self.config.get("aws_region"))

    @property
    def bucket(self) -> str:
        """The configured S3 bucket name.

        Returns:
            The S3 bucket to read Parquet files from.
        """
        return self.config["bucket"]

    @property
    def prefix(self) -> str:
        """The configured S3 key prefix (folder) to read Parquet files from.

        Returns:
            The S3 key prefix, with any leading slash stripped.
        """
        return self.config.get("prefix", "").lstrip("/")

    def get_parquet_keys(self) -> Iterable[str]:
        """List the keys of all `.parquet` objects under the configured prefix.

        Yields:
            S3 object keys ending in `.parquet`.
        """
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    yield obj["Key"]

    def get_parquet_metadata(self, key: str) -> pq.ParquetFile:
        """Open a Parquet file's footer metadata without downloading its contents.

        Args:
            key: The S3 object key of the Parquet file to inspect.

        Returns:
            A `ParquetFile` whose metadata/schema has been read via a HEAD request
            plus one or two small ranged GETs, not a download of the full object.
        """
        return pq.ParquetFile(_S3RangeReader(self.s3_client, self.bucket, key))

    def iter_parquet_batches(self, key: str) -> Iterable[pa.RecordBatch]:
        """Download a single Parquet object from S3 and stream its row batches.

        Args:
            key: The S3 object key of the Parquet file to read.

        Yields:
            One PyArrow `RecordBatch` per row group in the file.
        """
        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        buffer = io.BytesIO(response["Body"].read())
        yield from pq.ParquetFile(buffer).iter_batches()

    @property
    @override
    def schema(self) -> dict:
        """Infer the JSON schema from the first Parquet file's footer metadata.

        Returns:
            JSON Schema dictionary for this stream.

        Raises:
            RuntimeError: If no `.parquet` files are found under the prefix.
        """
        if self._schema is None:
            first_key = next(iter(self.get_parquet_keys()), None)
            if first_key is None:
                msg = f"No .parquet files found in s3://{self.bucket}/{self.prefix}"
                raise RuntimeError(msg)
            self.logger.info("Inferring schema from s3://%s/%s", self.bucket, first_key)
            arrow_schema = self.get_parquet_metadata(first_key).schema_arrow
            self._schema = th.PropertiesList(
                *(
                    th.Property(
                        field.name,
                        th.CustomType(_arrow_type_to_jsonschema(field.type)),
                        nullable=field.nullable,
                    )
                    for field in arrow_schema
                ),
            ).to_dict()
        return self._schema

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Read every Parquet file under the configured prefix and yield its rows.

        Args:
            context: Stream partition or context dictionary (unused).

        Yields:
            One dictionary per row, across every Parquet file in the folder.
        """
        keys = list(self.get_parquet_keys())
        self.logger.info(
            "Found %d Parquet file(s) under s3://%s/%s",
            len(keys),
            self.bucket,
            self.prefix,
        )
        for index, key in enumerate(keys, start=1):
            self.logger.info("Reading file %d/%d: s3://%s/%s", index, len(keys), self.bucket, key)
            for batch in self.iter_parquet_batches(key):
                yield from batch.to_pylist()
