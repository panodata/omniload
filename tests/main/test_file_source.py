import os
import sys
from unittest.mock import patch

import pytest

from omniload.core.factory import SourceDestinationFactory
from omniload.error import MissingValueError
from omniload.source.filesystem.api import LocalFilesystemSource
from omniload.util.endpoint import supported_file_format_message

CWD = os.getcwd()


def capture_reader_args(uri: str, table: str = "", **kwargs) -> dict:
    """Run ``dlt_source`` with the shared reader stubbed out.

    Returns the ``(bucket_url, file_glob, reader_name)`` the parser computed, so we can
    assert the file:// URI semantics without touching the filesystem.
    """
    captured: dict = {}

    def fake_reader(bucket_url, fs, file_glob, reader_name, column_types):
        captured.update(
            bucket_url=bucket_url,
            file_glob=file_glob,
            reader_name=reader_name,
            column_types=column_types,
        )
        return "SENTINEL"

    with patch("omniload.source.filesystem.adapter.resource_for_reader", fake_reader):
        result = LocalFilesystemSource().dlt_source(uri, table, **kwargs)

    assert result == "SENTINEL"
    return captured


def test_factory_dispatches_file_scheme_to_local_source():
    factory = SourceDestinationFactory(
        "file://omniload/testdata/create_replace.csv", "duckdb:///tmp/x.duckdb"
    )
    assert isinstance(factory.get_source(), LocalFilesystemSource)


# (uri, table, expected bucket_url, expected file_glob, expected reader) - one row per
# form in the Decision table so a future refactor that reintroduces the netloc trap
# (urlparse dropping "omniload" as a host) fails CI.
uri_cases = [
    # relative, path in URI (reporter's exact form): resolves against cwd
    (
        "file://omniload/testdata/create_replace.csv",
        "create_replace",
        f"file://{CWD}/omniload/testdata",
        "create_replace.csv",
        "read_csv",
    ),
    # absolute, path in URI
    (
        "file:///data/x.jsonl",
        "",
        "file:///data",
        "x.jsonl",
        "read_jsonl",
    ),
    # blob/sftp-style split: empty URI path, path supplied via --source-table
    (
        "file://",
        "path/x.parquet",
        f"file://{CWD}/path",
        "x.parquet",
        "read_parquet",
    ),
    # glob in URI
    (
        "file://data/*.csv",
        "",
        f"file://{CWD}/data",
        "*.csv",
        "read_csv",
    ),
    # recursive glob: split at the first glob segment, keep ** in the glob
    (
        "file://data/**/*.csv",
        "",
        f"file://{CWD}/data",
        "**/*.csv",
        "read_csv",
    ),
    # #format hint on an extensionless file
    (
        "file://feed.dat#csv",
        "",
        f"file://{CWD}",
        "feed.dat",
        "read_csv",
    ),
    # literal '#' in the path (suffix is not a known format) stays part of the path
    (
        "file://vendor#1/data.csv",
        "",
        f"file://{CWD}/vendor#1",
        "data.csv",
        "read_csv",
    ),
]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="file:// source is POSIX-first; these cases pin '/'-separated bucket URLs "
    "(os.path.abspath yields backslash drive paths on Windows). Windows support is a "
    "documented follow-up, see the file:// docs page.",
)
@pytest.mark.parametrize(
    ("uri", "table", "bucket_url", "file_glob", "reader"),
    uri_cases,
    ids=[c[0] for c in uri_cases],
)
def test_uri_parsing(uri, table, bucket_url, file_glob, reader):
    captured = capture_reader_args(uri, table)
    assert captured["bucket_url"] == bucket_url
    assert captured["file_glob"] == file_glob
    assert captured["reader_name"] == reader


def test_split_form_matches_path_in_uri():
    """--source-uri file:// + --source-table x.csv == file://x.csv."""
    via_split = capture_reader_args("file://", "omniload/testdata/create_replace.csv")
    via_uri = capture_reader_args("file://omniload/testdata/create_replace.csv", "")
    assert via_split["bucket_url"] == via_uri["bucket_url"]
    assert via_split["file_glob"] == via_uri["file_glob"]


def test_column_types_are_forwarded():
    captured = capture_reader_args(
        "file://feed.dat#csv_headless", "", column_types={"a": {}, "b": {}}
    )
    assert captured["reader_name"] == "read_csv_headless"
    assert captured["column_types"] == {"a": {}, "b": {}}


@pytest.mark.parametrize(
    "uri", ["file://data.bin", "file://data", "file://archive.zip"]
)
def test_unsupported_extension_reports_supported_formats(uri):
    with pytest.raises(ValueError) as exc:
        capture_reader_args(uri)
    assert str(exc.value) == supported_file_format_message("Local file")


def test_requested_incremental_key_is_rejected():
    # run_ingest nulls incremental_key before calling us (handles_incrementality True),
    # so the rejection must key off requested_incremental_key, not incremental_key.
    with pytest.raises(ValueError, match="incrementality on its own"):
        LocalFilesystemSource().dlt_source(
            "file://omniload/testdata/create_replace.csv",
            "",
            incremental_key=None,
            requested_incremental_key="date",
        )


def test_handles_incrementality_is_true():
    assert LocalFilesystemSource().handles_incrementality() is True


def test_empty_path_raises():
    with pytest.raises(MissingValueError):
        LocalFilesystemSource().dlt_source("file://", "")
