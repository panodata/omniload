import csv
import io
import json

import pytest

from dlt_filesystem.target.local import LocalFilesystemDestination
from omniload import run_ingest
from omniload.core.factory import SourceDestinationFactory
from omniload.target.csv import CsvDestination
from tests.util import invoke_ingest_command


def test_factory_dispatches_file_scheme_to_local_destination():
    factory = SourceDestinationFactory("file://in.csv", "file://out.jsonl")
    assert isinstance(factory.get_destination(), LocalFilesystemDestination)


def test_factory_dispatches_csv_scheme_to_a_local_filesystem_destination():
    """``csv://`` is the same writer with the format pinned (#301), so a later change
    cannot reintroduce a separate staging and rewrite path without failing here."""
    factory = SourceDestinationFactory("file://in.csv", "csv://out.csv")
    destination = factory.get_destination()
    assert isinstance(destination, CsvDestination)
    assert isinstance(destination, LocalFilesystemDestination)


PEOPLE = "name,age\nAlice,30\nBob,25\nCarol,41\n"


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


@pytest.mark.parametrize("scheme", ["file", "csv"])
def test_nested_destination_dir_is_created(tmp_path, scheme):
    """A destination path with non-existent parent directories is created on write.

    One inherited `post_load()` serves both schemes, so running both is a guard against
    a future `CsvDestination` override rather than two implementations under test. It is
    here because the replaced CSV destination did carry its own `os.makedirs` call, and
    both doc pages still promise the behaviour.
    """
    _write_source_files(tmp_path)
    out_path = tmp_path / "nested" / "deeper" / f"out-{scheme}.csv"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"{scheme}://{out_path}",
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


# --- csv:// compatibility destination (#301) ---


def test_csv_and_file_destinations_write_the_same_file(tmp_path):
    """The two spellings are one writer, so the same load produces the same output."""
    _write_source_files(tmp_path)
    outputs = {}
    for scheme in ("file", "csv"):
        out_path = tmp_path / f"out-{scheme}.csv"
        result = invoke_ingest_command(
            f"file://{tmp_path / 'people.csv'}",
            "people",
            f"{scheme}://{out_path}",
            "public.people",
        )
        assert result.exit_code == 0, result.output
        outputs[scheme] = out_path.read_text()

    assert outputs["csv"] == outputs["file"]


def test_csv_destination_writes_every_rotated_load_file(tmp_path):
    """dlt may split one load across several staged files. The replaced implementation
    rewrote the *first* file it found and still exited zero, so the rest of the rows
    were silently dropped. ``loader_file_size=1`` forces that split; all three ids must
    survive, which is what fails if the shared post-load ever reads one file again."""
    (tmp_path / "in.csv").write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")
    out_path = tmp_path / "out.csv"

    run_ingest(
        source_uri=f"file://{tmp_path / 'in.csv'}",
        dest_uri=f"csv://{out_path}",
        source_table="rows",
        dest_table="public.rows",
        loader_file_size=1,
        progress="log",
    )

    rows = _read_back(out_path, "csv")
    assert sorted(row["id"] for row in rows) == ["1", "2", "3"]
    assert sorted(row["name"] for row in rows) == ["Alice", "Bob", "Carol"]


@pytest.mark.parametrize(
    "dest",
    [
        "out.jsonl",
        "out.parquet",
        "out.dat#jsonl",
        "out.dat#parquet",
        # `json` is a registered read format, so it names a format even though the write
        # side cannot produce it. Naming it is an error rather than a CSV file wearing a
        # `.json` extension.
        "out.json",
        "out.dat#csv_duckdb",
    ],
)
def test_csv_destination_rejects_a_non_csv_output(tmp_path, dest):
    """The scheme names the format, so naming a different one is an error rather than a
    CSV file with a misleading extension. It is rejected before staging, so nothing is
    written."""
    _write_source_files(tmp_path)
    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"csv://{tmp_path / dest}",
        "public.people",
        print_output=False,
    )
    assert result.exit_code != 0
    assert not (tmp_path / dest.split("#")[0]).exists()


@pytest.mark.parametrize("name", ["report", "out.dat"])
def test_csv_destination_writes_a_path_with_no_csv_extension(tmp_path, name):
    """``csv://`` has always written CSV to whatever path it was given, extension or
    not. The shared resolver requires one; the pin keeps these spellings working."""
    _write_source_files(tmp_path)
    out_path = tmp_path / name

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"csv://{out_path}",
        "public.people",
    )
    assert result.exit_code == 0, result.output
    assert [r["name"] for r in _read_back(out_path, "csv")] == ["Alice", "Bob", "Carol"]
