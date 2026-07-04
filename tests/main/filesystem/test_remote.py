from dataclasses import dataclass
from urllib.parse import urlparse

import pytest

from omniload.source.filesystem.error import UnsupportedEndpointError
from omniload.source.filesystem.format.registry import supported_file_format_message
from omniload.source.filesystem.router import (
    determine_endpoint,
    parse_endpoint,
    parse_uri,
    split_format_hint,
)


@dataclass
class URITestCase:
    uri: str
    table: str
    expect_bucket: str
    expect_glob: str


test_cases: list[URITestCase] = [
    URITestCase("s3://", "bucket/file", "bucket", "file"),
    URITestCase("s3://bucket", "file", "bucket", "file"),
    URITestCase("s3://bucket/file", "", "bucket", "file"),
    URITestCase("s3://primary", "s3://secondary/file", "primary", "file"),
    URITestCase(
        "s3://primary", "s3://secondary/path/to/file", "primary", "path/to/file"
    ),
    URITestCase("s3://primary", "path/to/file", "primary", "path/to/file"),
    URITestCase("s3://", "s3://secondary/path/to/file", "secondary", "path/to/file"),
    URITestCase("s3://", "s3://bucket/file", "bucket", "file"),
]


@pytest.mark.parametrize("test_case", test_cases)
def test_parse_uri(test_case: URITestCase):
    uri = urlparse(test_case.uri)
    (bucket, glob) = parse_uri(uri, test_case.table)
    assert bucket == test_case.expect_bucket
    assert glob == test_case.expect_glob


@pytest.mark.parametrize(
    ("path", "endpoint"),
    [
        ("data.csv", "read_csv"),
        ("data.csv.gz", "read_csv"),
        ("data.jsonl", "read_jsonl"),
        ("data.jsonl.gz", "read_jsonl"),
        ("data.parquet", "read_parquet"),
    ],
)
def test_parse_endpoint(path: str, endpoint: str):
    assert parse_endpoint(path) == endpoint


@pytest.mark.parametrize(
    ("table", "path", "endpoint"),
    [
        ("bucket/path/no-extension#csv", "path/no-extension", "read_csv"),
        (
            "bucket/path/no-extension#csv_headless",
            "path/no-extension",
            "read_csv_headless",
        ),
        ("bucket/path/no-extension#jsonl", "path/no-extension", "read_jsonl"),
        ("bucket/path/no-extension#parquet", "path/no-extension", "read_parquet"),
    ],
)
def test_determine_endpoint_format_hint(table: str, path: str, endpoint: str):
    assert determine_endpoint(table, path) == endpoint


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("path/file.csv#csv", ("path/file.csv", "csv")),
        ("path/no-extension#csv_headless", ("path/no-extension", "csv_headless")),
        # literal '#' in the path: trailing segment is not a known format
        ("path/vendor#1/data.csv", ("path/vendor#1/data.csv", None)),
        ("path/data#unknown", ("path/data#unknown", None)),
        ("path/file.csv", ("path/file.csv", None)),
    ],
)
def test_split_format_hint(table: str, expected: tuple[str, str | None]):
    assert split_format_hint(table) == expected


@pytest.mark.parametrize(
    ("table", "path", "endpoint"),
    [
        # a literal '#' in the path must not be mistaken for a format hint;
        # the extension drives the reader instead
        ("bucket/vendor#1/data.csv", "vendor#1/data.csv", "read_csv"),
        ("bucket/weird#thing.jsonl", "weird#thing.jsonl", "read_jsonl"),
    ],
)
def test_determine_endpoint_literal_hash_in_path(table: str, path: str, endpoint: str):
    assert determine_endpoint(table, path) == endpoint


def test_parse_endpoint_rejects_unsupported_format():
    with pytest.raises(UnsupportedEndpointError, match="Unsupported file format: bin"):
        parse_endpoint("data.bin")


def test_supported_file_format_message():
    assert (
        supported_file_format_message("S3")
        == "S3 Source only supports file formats: csv, csv_headless, jsonl, parquet"
    )
