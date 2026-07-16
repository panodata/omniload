import os
import re

import pytest

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.target.local import LocalFilesystemDestination
from dlt_filesystem.target.model import DEFAULT_DATASET_NAME
from dlt_filesystem.target.util import _resolve_output_target

# Normalized so the relative-form expectations hold on Windows too (os.getcwd() there
# returns a backslash drive path), matching the source-side test.
CWD = os.getcwd().replace(os.sep, "/")


# Output path + format resolution. Absolute forms are cwd/OS-independent; relative forms
# resolve against the working directory. Format comes from the extension unless a #hint
# overrides it, mirroring the file:// source.
resolve_cases = [
    ("file:///data/out.csv", "/data/out.csv", "csv"),
    ("file:///data/out.jsonl", "/data/out.jsonl", "jsonl"),
    ("file:///data/out.parquet", "/data/out.parquet", "parquet"),
    # Windows drive and UNC, same handling as the source
    ("file:///C:/data/out.csv", "C:/data/out.csv", "csv"),
    ("file://C:/data/out.parquet", "C:/data/out.parquet", "parquet"),
    ("file:////server/share/out.jsonl", "//server/share/out.jsonl", "jsonl"),
    # #format hint wins over (or supplies) the format; the path keeps the hinted-off name
    ("file:///data/out.dat#jsonl", "/data/out.dat", "jsonl"),
    ("file:///data/feed#csv", "/data/feed", "csv"),
    # literal '#' in the path (suffix is not a known format) stays part of the path,
    # so the format falls back to the extension
    ("file:///data/v#1/out.csv", "/data/v#1/out.csv", "csv"),
    # relative path resolves against the working directory
    ("file://out.parquet", f"{CWD}/out.parquet", "parquet"),
    ("file://sub/out.csv", f"{CWD}/sub/out.csv", "csv"),
]


@pytest.mark.parametrize(
    ("uri", "path", "fmt"), resolve_cases, ids=[c[0] for c in resolve_cases]
)
def test_resolve_output_target(uri, path, fmt):
    assert _resolve_output_target(uri) == (path, fmt)


@pytest.mark.parametrize(
    "uri",
    ["file:///data/out.txt", "file:///data/out", "file:///data/archive.zip"],
)
def test_unsupported_format_reports_supported_formats(uri):
    with pytest.raises(
        ValueError, match="only supports file formats: csv, jsonl, parquet"
    ):
        _resolve_output_target(uri)


@pytest.mark.parametrize("uri", ["file:///data/out.bson", "file:///data/out.dat#bson"])
def test_bson_destination_is_rejected(uri):
    """BSON is read-only. Adding ``bson`` to ``FORMAT_TO_READER`` lights up the read
    path, but the destination's ``WRITE_FORMATS`` is a separate tuple, so a ``.bson``
    output (or an explicit ``#bson`` hint) is still rejected. This pins that contract
    and documents the harmless edge that a destination ``#bson`` suffix now reads as an
    (unsupported) format hint rather than part of the filename.
    """
    with pytest.raises(
        ValueError, match="only supports file formats: csv, jsonl, parquet"
    ):
        _resolve_output_target(uri)


@pytest.mark.parametrize("uri", ["file://", "file://#csv", "file://   "])
def test_empty_path_raises(uri):
    with pytest.raises(MissingConnectorOption):
        _resolve_output_target(uri)


@pytest.mark.parametrize(
    "uri",
    ["file://data/*.csv", "file://out-?.jsonl", "file://logs/[abc].parquet"],
)
def test_glob_in_destination_path_is_rejected(uri):
    with pytest.raises(ValueError, match="globs"):
        _resolve_output_target(uri)


@pytest.mark.parametrize("uri", ["file://out.csv", "out.csv"])
def test_dlt_run_params_requires_uri(uri):
    destination = LocalFilesystemDestination().dlt_run_params(uri, "")
    assert destination["dataset_name"] == DEFAULT_DATASET_NAME
    assert destination["table_name"] == "out"


@pytest.mark.parametrize("table", ["justtable", "a.b.c"])
def test_dlt_run_params_requires_two_part_table(table):
    with pytest.raises(ValueError, match=re.escape("<schema>.<table>")):
        LocalFilesystemDestination().dlt_run_params("file://out.csv", table)
