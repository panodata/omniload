import csv
import io
import json
import os
import re

import pytest

from omniload.core.factory import SourceDestinationFactory
from omniload.error import MissingValueError
from omniload.target.filesystem.api import (
    LocalFilesystemDestination,
)
from omniload.target.filesystem.util import _resolve_output_target
from tests.util import invoke_ingest_command

# Normalized so the relative-form expectations hold on Windows too (os.getcwd() there
# returns a backslash drive path), matching the source-side test.
CWD = os.getcwd().replace(os.sep, "/")

PEOPLE = "name,age\nAlice,30\nBob,25\nCarol,41\n"


def test_factory_dispatches_file_scheme_to_local_destination():
    factory = SourceDestinationFactory("file://in.csv", "file://out.jsonl")
    assert isinstance(factory.get_destination(), LocalFilesystemDestination)


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


@pytest.mark.parametrize("uri", ["file://", "file://#csv", "file://   "])
def test_empty_path_raises(uri):
    with pytest.raises(MissingValueError):
        _resolve_output_target(uri)


@pytest.mark.parametrize(
    "uri",
    ["file://data/*.csv", "file://out-?.jsonl", "file://logs/[abc].parquet"],
)
def test_glob_in_destination_path_is_rejected(uri):
    with pytest.raises(ValueError, match="globs"):
        _resolve_output_target(uri)


@pytest.mark.parametrize("table", ["justtable", "a.b.c", ""])
def test_dlt_run_params_requires_two_part_table(table):
    with pytest.raises(ValueError, match=re.escape("<schema>.<table>")):
        LocalFilesystemDestination().dlt_run_params("file://out.csv", table)


def _write_source_files(directory):
    (directory / "people.csv").write_text(PEOPLE)
    with (directory / "people.jsonl").open("w") as f:
        for row in csv.DictReader(io.StringIO(PEOPLE)):
            f.write(json.dumps(row) + "\n")
    return directory


@pytest.mark.parametrize("out_format", ["csv", "jsonl", "parquet"])
def test_file_to_file_round_trip(tmp_path, out_format):
    """file:// source -> file:// destination end-to-end (no Docker, no DB).

    Reads a real local CSV and writes a clean single file in each output format, then
    reads it back and asserts the three rows survive without dlt's `_dlt_*` bookkeeping
    columns. Runs in the fast unit lane, same as the source's real-read test.
    """
    _write_source_files(tmp_path)
    out_path = tmp_path / f"out.{out_format}"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"file://{out_path}",
        "public.people",
    )
    assert result.exit_code == 0, result.output
    assert out_path.exists()

    rows = _read_back(out_path, out_format)
    assert [r["name"] for r in rows] == ["Alice", "Bob", "Carol"]
    assert all(not key.startswith("_dlt_") for r in rows for key in r)
    assert str(next(r["age"] for r in rows if r["name"] == "Bob")) == "25"


def test_format_hint_drives_writer_end_to_end(tmp_path):
    """A #format hint on an extensionless destination selects the writer end-to-end."""
    _write_source_files(tmp_path)
    out_path = tmp_path / "feed.dat"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"file://{out_path}#jsonl",
        "public.people",
    )
    assert result.exit_code == 0, result.output
    assert out_path.exists()
    assert [r["name"] for r in _read_back(out_path, "jsonl")] == [
        "Alice",
        "Bob",
        "Carol",
    ]


def test_unsupported_destination_format_fails(tmp_path):
    """An unsupported output extension aborts the ingest instead of writing garbage."""
    _write_source_files(tmp_path)
    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"file://{tmp_path / 'out.txt'}",
        "public.people",
        print_output=False,
    )
    assert result.exit_code != 0
    assert not (tmp_path / "out.txt").exists()


@pytest.mark.parametrize("out_format", ["csv", "jsonl", "parquet"])
def test_column_missing_from_first_row_survives(tmp_path, out_format):
    """A column absent from the first row must not be dropped from the output.

    dlt omits null keys per row, so the first row here carries only (id, name) while a
    later row adds `note`. Guards the writers' union-of-keys against schema inference
    (e.g. pa.Table.from_pylist) that would look at the first row only.
    """
    (tmp_path / "in.csv").write_text("id,name,note\n1,alice,\n2,bob,hi\n")
    out_path = tmp_path / f"out.{out_format}"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'in.csv'}",
        "rows",
        f"file://{out_path}",
        "public.rows",
    )
    assert result.exit_code == 0, result.output

    rows = _read_back(out_path, out_format)
    assert any("note" in row for row in rows)
    note = next(row["note"] for row in rows if str(row.get("id")) == "2")
    assert note == "hi"


def test_nested_destination_dir_is_created(tmp_path):
    """A destination path with non-existent parent directories is created on write."""
    _write_source_files(tmp_path)
    out_path = tmp_path / "nested" / "deeper" / "out.csv"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"file://{out_path}",
        "public.people",
    )
    assert result.exit_code == 0, result.output
    assert out_path.exists()
    assert len(_read_back(out_path, "csv")) == 3


@pytest.mark.parametrize("out_format", ["csv", "jsonl", "parquet"])
def test_empty_source_writes_a_file_without_crashing(tmp_path, out_format):
    """A header-only source (zero data rows) still produces an output file."""
    (tmp_path / "empty.csv").write_text("name,age\n")
    out_path = tmp_path / f"out.{out_format}"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'empty.csv'}",
        "empty",
        f"file://{out_path}",
        "public.empty",
    )
    assert result.exit_code == 0, result.output
    assert out_path.exists()
    assert _read_back(out_path, out_format) == []


def _read_back(path, out_format):
    if out_format == "csv":
        with open(path, newline="") as f:
            return list(csv.DictReader(f))
    if out_format == "jsonl":
        with open(path) as f:
            return [json.loads(line) for line in f if line.strip()]
    import pyarrow.parquet as pq

    return pq.read_table(path).to_pylist()
