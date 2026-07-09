from dataclasses import dataclass
from urllib.parse import urlparse

import pytest

from omniload.source.filesystem.error import UnsupportedEndpointError
from omniload.source.filesystem.format.registry import supported_file_format_message
from omniload.source.filesystem.router import (
    determine_endpoint,
    parse_endpoint,
    parse_fragment,
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
    """Parsing a source URI splits it into the expected bucket and file glob."""
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
        ("data.bson", "read_bson"),
        ("data.bson.gz", "read_bson"),
    ],
)
def test_parse_endpoint(path: str, endpoint: str):
    """A file extension maps to the expected reader name."""
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
        ("bucket/path/no-extension#bson", "path/no-extension", "read_bson"),
    ],
)
def test_determine_endpoint_format_hint(table: str, path: str, endpoint: str):
    """An explicit `#format` hint selects the reader, overriding the extension."""
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
    """Splitting a table spec yields the path and any trailing format hint."""
    assert split_format_hint(table) == expected


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        # no fragment
        ("file.csv", ("file.csv", None, {})),
        # bare format hint preserved (unchanged from split_format_hint)
        ("file#csv", ("file", "csv", {})),
        ("path/no-extension#csv_headless", ("path/no-extension", "csv_headless", {})),
        # a single named hint
        ("book.xlsx#sheet=foo", ("book.xlsx", None, {"sheet": "foo"})),
        # multiple named hints
        (
            "book.xlsx#sheet=foo&header=0",
            ("book.xlsx", None, {"sheet": "foo", "header": "0"}),
        ),
        # format hint and named hint coexist in one fragment
        ("feed.dat#csv&sheet=foo", ("feed.dat", "csv", {"sheet": "foo"})),
        # empty value is kept (reader decides if "" means unset)
        ("book.xlsx#sheet=", ("book.xlsx", None, {"sheet": ""})),
        # '=' in the value: parse_qsl partitions on the first '=', not split
        ("book.xlsx#x=a=b", ("book.xlsx", None, {"x": "a=b"})),
        # percent-decoding of values
        ("book.xlsx#sheet=My%20Sheet", ("book.xlsx", None, {"sheet": "My Sheet"})),
        ("book.xlsx#sheet=R%26D", ("book.xlsx", None, {"sheet": "R&D"})),
        # duplicate key: last wins
        ("book.xlsx#sheet=foo&sheet=bar", ("book.xlsx", None, {"sheet": "bar"})),
        # trailing '&' is harmless separator noise
        ("book.xlsx#sheet=foo&", ("book.xlsx", None, {"sheet": "foo"})),
        # mixed valid hint + invalid bare token -> whole '#...' stays literal
        ("book.xlsx#sheet=foo&bad", ("book.xlsx#sheet=foo&bad", None, {})),
        # duplicate/conflicting bare formats -> literal
        ("feed.dat#csv&parquet", ("feed.dat#csv&parquet", None, {})),
        # literal '#' in a path (trailing segment is neither hint nor format)
        ("/feeds/vendor#1/data.csv", ("/feeds/vendor#1/data.csv", None, {})),
        ("path/data#unknown", ("path/data#unknown", None, {})),
        # a bare trailing '#' interprets to nothing -> kept literal
        ("file.csv#", ("file.csv#", None, {})),
        # %23 forces a literal '#' that would otherwise look like a hint fragment
        ("book.xlsx%23sheet=foo", ("book.xlsx%23sheet=foo", None, {})),
    ],
)
def test_parse_fragment(spec: str, expected: tuple[str, str | None, dict[str, str]]):
    """Parsing a spec fragment yields the path, format hint, and named hints."""
    assert parse_fragment(spec) == expected


@pytest.mark.parametrize(
    "spec",
    [
        "path/file.csv#csv",
        "path/no-extension#csv_headless",
        "path/vendor#1/data.csv",
        "path/data#unknown",
        "path/file.csv",
    ],
)
def test_split_format_hint_matches_parse_fragment(spec: str):
    """split_format_hint stays a faithful (path, format) projection of parse_fragment."""
    path, fmt, _ = parse_fragment(spec)
    assert split_format_hint(spec) == (path, fmt)


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
    """A literal `#` in a path is not treated as a format hint."""
    assert determine_endpoint(table, path) == endpoint


def test_parse_endpoint_rejects_unsupported_format():
    """An unknown extension raises UnsupportedEndpointError."""
    with pytest.raises(UnsupportedEndpointError, match="Unsupported file format: bin"):
        parse_endpoint("data.bin")


def test_supported_file_format_message():
    """The supported-formats message lists the base formats in order."""
    # The base formats are always advertised, in order. Iterable-extra formats (msgpack, ...)
    # are appended only when their decoder is installed, so assert the stable base prefix.
    assert supported_file_format_message("S3").startswith(
        "S3 Source only supports file formats: csv, csv_headless, jsonl, parquet, bson"
    )
