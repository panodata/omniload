import os
from unittest.mock import patch

import pytest

from omniload.core.factory import SourceDestinationFactory
from omniload.error import MissingValueError
from omniload.source.filesystem.api import (
    LocalFilesystemSource,
    _is_absolute_local,
    _url_path_to_local,
)
from omniload.util.endpoint import supported_file_format_message

# Normalized so the relative-form expectations hold on Windows too (os.getcwd() there
# returns a backslash drive path).
CWD = os.getcwd().replace(os.sep, "/")


def capture_reader_args(uri: str, table: str = "", **kwargs) -> dict:
    """Run ``dlt_source`` with the shared reader stubbed out.

    Returns the ``(bucket_url, file_glob, reader_name)`` the parser computed, so we can
    assert the file:// URI semantics without touching the filesystem.
    """
    captured: dict = {}

    def fake_reader(request):
        captured.update(
            bucket_url=request.bucket_url,
            file_glob=request.file_glob,
            reader_name=request.reader_name,
            column_types=request.column_types,
            table=request.table,
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


# Absolute forms: deterministic regardless of cwd/OS, so they run on every lane
# (Linux, macOS, and the Windows CI matrix) and pin the drive-letter / UNC handling.
absolute_cases = [
    # POSIX absolute
    ("file:///data/x.jsonl", "", "/data", "x.jsonl", "read_jsonl"),
    # Windows drive letter, three-slash and two-slash both resolve to the drive
    ("file:///C:/data/x.csv", "", "C:/data", "x.csv", "read_csv"),
    ("file://C:/data/x.csv", "", "C:/data", "x.csv", "read_csv"),
    # UNC, four-slash and backslash forms
    (
        "file:////server/share/x.parquet",
        "",
        "//server/share",
        "x.parquet",
        "read_parquet",
    ),
    (r"file://\\server\share\x.csv", "", "//server/share", "x.csv", "read_csv"),
    # recursive glob under a drive and a UNC share
    ("file:///C:/logs/**/*.csv", "", "C:/logs", "**/*.csv", "read_csv"),
    (
        "file:////server/share/**/*.jsonl",
        "",
        "//server/share",
        "**/*.jsonl",
        "read_jsonl",
    ),
    # literal '#' in a drive path stays part of the path
    ("file://C:/vendor#1/data.csv", "", "C:/vendor#1", "data.csv", "read_csv"),
]

# Relative forms: resolve against the working directory. Expected values are cwd-derived
# (still cross-platform via the normalized CWD) and guard against the netloc trap, a
# refactor that drops "omniload" as a host would fail these.
relative_cases = [
    (
        "file://omniload/testdata/create_replace.csv",
        "create_replace",
        f"{CWD}/omniload/testdata",
        "create_replace.csv",
        "read_csv",
    ),
    # blob/sftp-style split: empty URI path, path supplied via --source-table
    ("file://", "path/x.parquet", f"{CWD}/path", "x.parquet", "read_parquet"),
    # glob in URI
    ("file://data/*.csv", "", f"{CWD}/data", "*.csv", "read_csv"),
    # recursive glob: split at the first glob segment, keep ** in the glob
    ("file://data/**/*.csv", "", f"{CWD}/data", "**/*.csv", "read_csv"),
    # #format hint on an extensionless file
    ("file://feed.dat#csv", "", CWD, "feed.dat", "read_csv"),
    # literal '#' in the path (suffix is not a known format) stays part of the path
    ("file://vendor#1/data.csv", "", f"{CWD}/vendor#1", "data.csv", "read_csv"),
]

uri_cases = absolute_cases + relative_cases


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


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("dir/x.csv", "dir/x.csv"),  # relative, unchanged
        ("/abs/x.csv", "/abs/x.csv"),  # POSIX absolute, unchanged
        ("/C:/data/x.csv", "C:/data/x.csv"),  # file:///C:/ -> drop leading slash
        ("C:/data/x.csv", "C:/data/x.csv"),  # bare drive, unchanged
        ("//server/share/x.csv", "//server/share/x.csv"),  # UNC, unchanged
        (r"\\server\share\x.csv", "//server/share/x.csv"),  # backslash UNC -> forward
        (r"C:\data\x.csv", "C:/data/x.csv"),  # backslash drive -> forward
    ],
)
def test_url_path_to_local(spec, expected):
    assert _url_path_to_local(spec) == expected


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("dir/x.csv", False),
        ("x.csv", False),
        ("/abs", True),
        ("//server/share", True),
        ("C:/data", True),
    ],
)
def test_is_absolute_local(path, expected):
    assert _is_absolute_local(path) is expected


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


def test_reads_a_real_local_file(tmp_path):
    """End-to-end read through fsspec (no stub) so the Windows CI lane exercises the real
    drive-letter path. tmp_path is a POSIX path on Linux/macOS and a ``C:\\...`` drive
    path on Windows, so ``file://<tmp_path>/x.csv`` covers both."""
    csv = tmp_path / "people.csv"
    csv.write_text("name,age\nAlice,30\nBob,25\nCarol,41\n")

    resource = LocalFilesystemSource().dlt_source(f"file://{csv}", "")

    names = sorted(row["name"] for row in resource)
    assert names == ["Alice", "Bob", "Carol"]
