import csv
import datetime
import decimal
import io
import json
import sys

import pytest

from dlt_filesystem.target.local import LocalFilesystemDestination
from dlt_filesystem.target.registry import WRITE_FORMATS
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


@pytest.mark.parametrize("out_format", WRITE_FORMATS)
def test_file_to_file_round_trip(tmp_path, out_format):
    """file:// source -> file:// destination end-to-end (no Docker, no DB).

    Reads a real local CSV and writes a clean single file in each output format, then
    reads it back and asserts the three rows survive without dlt's `_dlt_*` bookkeeping
    columns. Runs in the fast unit lane, same as the source's real-read test.
    """

    if out_format == "vortex" and sys.version_info < (3, 11):
        pytest.skip("Vortex files only supported on Python 3.11 and newer")

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


@pytest.mark.parametrize("out_format", WRITE_FORMATS)
def test_column_missing_from_first_row_survives(tmp_path, out_format):
    """A column absent from the first row must not be dropped from the output.

    dlt omits null keys per row, so the first row here carries only (id, name) while a
    later row adds `note`. Guards the writers' union-of-keys against schema inference
    (e.g. pa.Table.from_pylist) that would look at the first row only.
    """

    if out_format == "vortex" and sys.version_info < (3, 11):
        pytest.skip("Vortex files only supported on Python 3.11 and newer")

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


@pytest.mark.parametrize("out_format", WRITE_FORMATS)
def test_empty_source_writes_a_file_without_crashing(tmp_path, out_format):
    """A header-only source (zero data rows) still produces an output file."""

    if out_format == "vortex" and sys.version_info < (3, 11):
        pytest.skip("Vortex files only supported on Python 3.11 and newer")

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
    """Decode an output file with the format's own conventions, not omniload's reader.

    Deliberately independent of `src/`, so a writer and its reader agreeing on a broken
    encoding still fails here. `test_written_file_reads_back_through_its_own_reader`
    covers the other direction.
    """
    if out_format == "csv":
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    elif out_format == "jsonl":
        with open(path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    elif out_format == "json":
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    elif out_format in ("yaml", "yml"):
        import yaml

        # One document holding a list, so `safe_load` (not `safe_load_all`) is what
        # reads it; a `---` stream would come back as a generator of documents here
        # and fail the row assertions rather than passing quietly.
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    elif out_format in ("feather", "arrow", "ipc"):
        import pyarrow as pa

        return pa.ipc.open_file(path).read_all().to_pylist()
    elif out_format == "orc":
        import pyarrow.orc as po

        return po.read_table(path).to_pylist()
    elif out_format == "parquet":
        import pyarrow.parquet as pq

        return pq.read_table(path).to_pylist()
    elif out_format == "vortex":
        import vortex as vx  # ty: ignore[unresolved-import,unused-ignore-comment,unused-ignore-comment]

        return vx.open(str(path)).to_dataset().to_table().to_pylist()
    else:
        raise NotImplementedError(f"Unknown output format: {out_format}")


@pytest.mark.parametrize("out_format", WRITE_FORMATS)
def test_written_file_reads_back_through_its_own_reader(tmp_path, out_format):
    """Every registered writer emits something omniload can read back.

    A writer whose output its own reader rejects is a dead end that unit tests on either
    half would both pass, so this loads the written file back as a source. Non-ASCII
    values are in the fixture because the readers decode as UTF-8 unconditionally, so a
    locale-encoded writer fails here rather than in a user's export.
    """

    if out_format == "vortex" and sys.version_info < (3, 11):
        pytest.skip("Vortex files only supported on Python 3.11 and newer")

    (tmp_path / "in.csv").write_text(
        "name,city\nZoë,München\nBob,Ōtautahi\n", encoding="utf-8"
    )
    written = tmp_path / f"out.{out_format}"
    final = tmp_path / "roundtrip.jsonl"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'in.csv'}", "rows", f"file://{written}", "public.rows"
    )
    assert result.exit_code == 0, result.output

    result = invoke_ingest_command(
        f"file://{written}", "rows", f"file://{final}", "public.rows"
    )
    assert result.exit_code == 0, result.output

    rows = _read_back(final, "jsonl")
    assert sorted(row["name"] for row in rows) == ["Bob", "Zoë"]
    assert sorted(row["city"] for row in rows) == ["München", "Ōtautahi"]


def test_yaml_survives_a_forced_parquet_intermediate(tmp_path):
    """`--loader-file-format parquet` is the only load path that hands a writer native
    values instead of JSON-typed ones, and any typed source can reach it. A decimal
    column aborted the whole YAML export there before the writer learned to spell the
    types PyYAML's safe dumper refuses.

    duckdb rather than the CSV fixture the other cases use, because a CSV source cannot
    produce a decimal or a blob in the first place.
    """
    import duckdb

    source = tmp_path / "src.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute("CREATE TABLE t (id INTEGER, price DECIMAL(10,2), blob BLOB)")
    connection.execute("INSERT INTO t VALUES (1, 1.50, 'hi'::BLOB)")
    connection.close()

    out_path = tmp_path / "out.yaml"
    result = invoke_ingest_command(
        f"duckdb:///{source}",
        "main.t",
        f"file://{out_path}",
        "public.t",
        loader_file_format="parquet",
    )
    assert result.exit_code == 0, result.output

    rows = _read_back(out_path, "yaml")
    # The scale a float would drop, and the same string `write_json` writes for it.
    assert rows[0]["price"] == "1.50"
    assert rows[0]["blob"] == b"hi"


def _typed_feather_source(path):
    """A Feather file carrying one row of every type the format page tabulates."""
    import pyarrow as pa

    table = pa.table(
        {
            "i": pa.array([1]),
            "s": pa.array(["a"]),
            "date": pa.array([datetime.date(2020, 1, 1)]),
            "naive": pa.array([datetime.datetime(2020, 1, 2, 3, 4, 5)]),
            "time": pa.array([datetime.time(9, 30)]),
            "blob": pa.array([b"hi"]),
            "dec": pa.array([decimal.Decimal("3.14")], type=pa.decimal128(38, 2)),
            "lst": pa.array([[1, 2]]),
            "st": pa.array([{"n": 1}]),
            "nul": pa.array([None], type=pa.null()),
        }
    )
    with pa.ipc.new_file(str(path), table.schema) as writer:
        writer.write_table(table)
    return path


def _load_feather_to_feather(tmp_path, loader_file_format=None):
    source = _typed_feather_source(tmp_path / "in.feather")
    out_path = tmp_path / f"out-{loader_file_format or 'default'}.feather"
    kwargs = {"loader_file_format": loader_file_format} if loader_file_format else {}
    result = invoke_ingest_command(
        f"file://{source}", "rows", f"file://{out_path}", "public.rows", **kwargs
    )
    assert result.exit_code == 0, result.output
    import pyarrow as pa

    return pa.ipc.open_file(str(out_path)).read_all()


def test_default_staging_delivers_typed_columns_as_text(tmp_path):
    """What a load actually writes, which is not what the writer can hold.

    dlt stages gzip-JSONL by default, so every value reaches a writer already
    JSON-typed: a date, a timestamp, a time, a decimal and a blob all arrive as strings
    however capable the destination format is. Nested values survive, because JSON has
    them. This is the table on the Feather documentation page, and the reason that page
    separates "read and write" from "what a load delivers".
    """
    table = _load_feather_to_feather(tmp_path)

    types = {field.name: str(field.type) for field in table.schema}
    assert types["i"] == "int64"
    assert [types[name] for name in ("date", "naive", "time", "blob", "dec")] == [
        "string"
    ] * 5
    assert types["lst"] == "list<item: int64>"
    assert types["st"].startswith("struct<")

    row = table.to_pylist()[0]
    assert row["i"] == 1 and row["s"] == "a"
    assert row["date"] == "2020-01-01"
    assert row["naive"] == "2020-01-02T03:04:05+00:00"
    assert row["time"] == "09:30:00"
    assert row["dec"] == "3.14"
    assert row["blob"] == "aGk=", "bytes reach the writer base64-encoded"
    assert row["lst"] == [1, 2] and row["st"] == {"n": 1}


def test_parquet_staging_delivers_typed_columns_and_flattens_nesting(tmp_path):
    """The mirror image: the typed columns survive and the nested ones do not."""
    table = _load_feather_to_feather(tmp_path, loader_file_format="parquet")

    # The whole row and the whole schema, not a type predicate each: a predicate passes
    # on a null value, a wrong instant or a zero decimal, which is most of what could go
    # wrong here. dlt applies its own schema rather than the source's, so the naive
    # column arrives as UTC (asserted rather than inherited: this passes on a UTC+12
    # box, so it is dlt localizing rather than the platform) and the `decimal128(38, 2)`
    # is quantized to dlt's default scale of 9, from which PyArrow infers
    # `decimal128(10, 9)`. Both of those are the point rather than incidental.
    assert {field.name: str(field.type) for field in table.schema} == {
        "i": "int64",
        "s": "string",
        "date": "date32[day]",
        "naive": "timestamp[us, tz=UTC]",
        "time": "time64[us]",
        "blob": "binary",
        "dec": "decimal128(10, 9)",
        "lst": "string",
        "st": "string",
    }
    assert table.to_pylist()[0] == {
        "i": 1,
        "s": "a",
        "date": datetime.date(2020, 1, 1),
        "naive": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "time": datetime.time(9, 30),
        "blob": b"hi",
        "dec": decimal.Decimal("3.140000000"),
        "lst": "[1,2]",
        "st": '{"n":1}',
    }


@pytest.mark.parametrize("loader_file_format", [None, "parquet"])
def test_a_wholly_null_column_does_not_reach_the_output(tmp_path, loader_file_format):
    """dlt omits a null key per row, so a column null in every row has no keys at all.

    Asserted on both staging paths because it happens before the staging choice: the
    writer is never told the column existed, so no writer can reinstate it.
    """
    table = _load_feather_to_feather(tmp_path, loader_file_format)

    assert "nul" not in table.column_names
    assert "i" in table.column_names, "the rest of the row still arrives"


@pytest.mark.parametrize("loader_file_format", [None, "parquet"])
@pytest.mark.parametrize("unit", ["ns", "us"])
def test_a_duration_column_fails_the_load_before_any_writer(
    tmp_path, loader_file_format, unit
):
    """A duration cannot be loaded at all, whichever staging is chosen.

    dlt's extract step serializes rows as JSON and refuses a timedelta, so the run dies
    ahead of the staging format and no output is written. Pinned rather than left to be
    discovered, because the format page states it and because the failure is the same on
    a Parquet or ORC destination.
    """
    import pyarrow as pa

    source = tmp_path / "dur.feather"
    table = pa.table(
        {"id": pa.array([1]), "dur": pa.array([1234567890], type=pa.duration(unit))}
    )
    with pa.ipc.new_file(str(source), table.schema) as writer:
        writer.write_table(table)
    out_path = tmp_path / "out.feather"

    kwargs = {"loader_file_format": loader_file_format} if loader_file_format else {}
    result = invoke_ingest_command(
        f"file://{source}",
        "rows",
        f"file://{out_path}",
        "public.rows",
        print_output=False,
        **kwargs,
    )

    assert result.exit_code != 0
    assert "not JSON serializable" in str(result.output) + str(result.exception)
    assert not out_path.exists()


def test_orc_cannot_take_a_time_column_under_parquet_staging(tmp_path):
    """The one place the load table above stops describing an ORC destination.

    Parquet staging keeps a time column typed, and PyArrow will not write `time64` to
    ORC at all. The default staging has already turned it into text by the time the
    writer runs, so that direction loads. Both are asserted, because the exception is
    only meaningful against the case that works.
    """
    source = _typed_feather_source(tmp_path / "in.feather")

    ok = invoke_ingest_command(
        f"file://{source}", "rows", f"file://{tmp_path / 'default.orc'}", "public.rows"
    )
    assert ok.exit_code == 0, ok.output

    failed = invoke_ingest_command(
        f"file://{source}",
        "rows",
        f"file://{tmp_path / 'staged.orc'}",
        "public.rows",
        loader_file_format="parquet",
        print_output=False,
    )
    assert failed.exit_code != 0
    assert "time64" in str(failed.output) + str(failed.exception)
    # `write_orc` opens the path before PyArrow validates the schema, so the rejection
    # leaves an empty file behind rather than nothing. Asserted as it is rather than as
    # it should be: the writer predates this suite, and a 0-byte ORC file is refused on
    # read (`File size too small`), so the failure does not read as a successful export.
    assert (tmp_path / "staged.orc").stat().st_size == 0


@pytest.mark.parametrize("out_format", ["feather", "parquet"])
def test_nested_values_are_json_text_under_a_parquet_intermediate(tmp_path, out_format):
    """What a real load delivers for a nested column, which is not what the writer can hold.

    On the default staging path a list and a struct reach the writer as a list and a dict,
    and the columnar writers store them as Arrow types. Under `--loader-file-format parquet`
    dlt's own normalization serializes both to JSON text first, so the output column is a
    string and the nesting is gone before any writer is called.

    Pinned across two formats because it is dlt's staging rather than either writer, and
    documented on the format pages, which had claimed the forced-parquet path preserved
    source types.
    """
    (tmp_path / "in.jsonl").write_text(
        '{"id": 1, "tags": [1, 2], "meta": {"n": 1}}\n'
        '{"id": 2, "tags": [3], "meta": {"n": 2}}\n'
    )

    outputs = {}
    for label, loader_file_format in (("default", None), ("parquet", "parquet")):
        out_path = tmp_path / f"out-{label}.{out_format}"
        kwargs = (
            {"loader_file_format": loader_file_format} if loader_file_format else {}
        )
        result = invoke_ingest_command(
            f"file://{tmp_path / 'in.jsonl'}",
            "rows",
            f"file://{out_path}",
            "public.rows",
            **kwargs,
        )
        assert result.exit_code == 0, result.output
        outputs[label] = sorted(_read_back(out_path, out_format), key=lambda r: r["id"])

    assert outputs["default"][0]["tags"] == [1, 2]
    assert outputs["default"][0]["meta"] == {"n": 1}
    assert outputs["parquet"][0]["tags"] == "[1,2]"
    assert outputs["parquet"][0]["meta"] == '{"n":1}'


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
        # `json` is writable through `file://`, which is exactly why naming it here is
        # an error: the scheme pins CSV, so the alternative would be a CSV file wearing
        # a `.json` extension.
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


# --- the staging bucket's own naming (#333) ---


@pytest.mark.parametrize(
    "layout",
    [
        "{table_name}/data",  # no format in the name at all
        "{table_name}/data.csv",  # a format, and the wrong one
    ],
)
def test_staging_layout_ignores_ambient_filesystem_configuration(
    tmp_path, monkeypatch, layout
):
    """``post_load`` reads the staged files back by the format in their name, so the
    staging bucket names itself rather than inheriting a user's filesystem layout. It is
    a private temp directory the user never sees, and their layout would otherwise reach
    it: without ``{ext}`` the format leaves the name entirely, and a layout ending
    ``.csv`` puts a wrong one on a gzip-JSONL file, which reads back as mis-parsed rows
    rather than as an error."""
    monkeypatch.setenv("DESTINATION__FILESYSTEM__LAYOUT", layout)
    _write_source_files(tmp_path)
    out_path = tmp_path / "out.jsonl"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"file://{out_path}",
        "public.people",
    )

    assert result.exit_code == 0, result.output
    assert [r["name"] for r in _read_back(out_path, "jsonl")] == [
        "Alice",
        "Bob",
        "Carol",
    ]


@pytest.mark.parametrize("scheme", ["file", "csv"])
def test_destinations_read_back_a_csv_intermediate(tmp_path, scheme):
    """``--loader-file-format csv`` stages CSV instead of the default JSONL. dlt gzips
    both, so the staged file used to be read as JSONL and the load died on a raw JSON
    decode error. ``csv://`` inherits ``post_load``, so it is covered here too rather
    than assumed from the ``file://`` case."""
    _write_source_files(tmp_path)
    out_path = tmp_path / f"out-{scheme}.csv"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"{scheme}://{out_path}",
        "public.people",
        loader_file_format="csv",
    )

    assert result.exit_code == 0, result.output
    assert [r["name"] for r in _read_back(out_path, "csv")] == ["Alice", "Bob", "Carol"]


@pytest.mark.parametrize(
    "placeholders",
    [
        '{"ext": "csv"}',  # renames a gzip-JSONL file to a format it is not
        '{"table_name": "elsewhere"}',  # stages the load where post_load does not look
    ],
)
def test_staging_ignores_ambient_extra_placeholders(
    tmp_path, monkeypatch, placeholders
):
    """Pinning the layout is not enough on its own. dlt resolves ``extra_placeholders``
    from configuration too and applies its entries over the built-in ones, so an ``ext``
    or ``table_name`` entry rewrites a name the layout had already fixed."""
    monkeypatch.setenv("DESTINATION__FILESYSTEM__EXTRA_PLACEHOLDERS", placeholders)
    _write_source_files(tmp_path)
    out_path = tmp_path / "out.jsonl"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"file://{out_path}",
        "public.people",
    )

    assert result.exit_code == 0, result.output
    assert [r["name"] for r in _read_back(out_path, "jsonl")] == [
        "Alice",
        "Bob",
        "Carol",
    ]


@pytest.mark.parametrize("scheme", ["file", "csv"])
def test_reserved_dlt_table_name_is_refused(tmp_path, scheme):
    """dlt writes its own bookkeeping into the staging directory of a table in this
    namespace, and ``post_load`` reads every data file it finds there, so exporting one
    would interleave dlt's load record with the source's rows. ``csv://`` parses the
    destination table itself rather than inheriting the parser, so it has to refuse the
    namespace itself; it shares the ``post_load`` that does the reading."""
    _write_source_files(tmp_path)

    result = invoke_ingest_command(
        f"file://{tmp_path / 'people.csv'}",
        "people",
        f"{scheme}://{tmp_path / 'out.csv'}",
        "public._dlt_loads",
        print_output=False,
    )

    assert result.exit_code != 0
    assert "reserved by dlt" in str(result.output) + str(result.exception)


def _typed_duckdb_source(tmp_path):
    """A source carrying the types only the parquet intermediate delivers natively.

    A CSV fixture cannot produce a decimal, a blob or a time in the first place, which
    is why these two cases build a database instead.
    """
    import duckdb

    source = tmp_path / "src.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute(
        'CREATE TABLE t (id INTEGER, price DECIMAL(10,2), blob BLOB, "at" TIME)'
    )
    connection.execute("INSERT INTO t VALUES (1, 1.50, 'hi'::BLOB, '09:30:00')")
    connection.close()
    return source


def test_csv_survives_a_forced_parquet_intermediate(tmp_path):
    """`--loader-file-format parquet` is the only load path that hands a writer native
    values instead of JSON-typed ones, and CSV has no column type for binary at all.

    The replaced writer stringified whatever it was handed, so a blob column landed in
    the file as the Python repr `b'hi'`. It now carries the base64 string the JSON
    writers write for the same value, and the decimal keeps the scale a float drops.
    """
    out_path = tmp_path / "out.csv"
    result = invoke_ingest_command(
        f"duckdb:///{_typed_duckdb_source(tmp_path)}",
        "main.t",
        f"file://{out_path}",
        "public.t",
        loader_file_format="parquet",
    )
    assert result.exit_code == 0, result.output

    row = _read_back(out_path, "csv")[0]
    assert row["blob"] == "aGk="
    assert row["price"] == "1.50"
    assert row["at"].startswith("09:30:00")


def test_parquet_keeps_native_types_through_a_forced_parquet_intermediate(tmp_path):
    """The same load into the columnar format, where each of those types has a column
    type of its own: they survive as themselves rather than as text."""
    import datetime
    import decimal

    out_path = tmp_path / "out.parquet"
    result = invoke_ingest_command(
        f"duckdb:///{_typed_duckdb_source(tmp_path)}",
        "main.t",
        f"file://{out_path}",
        "public.t",
        loader_file_format="parquet",
    )
    assert result.exit_code == 0, result.output

    import pyarrow.parquet as pq

    row = _read_back(out_path, "parquet")[0]
    assert row["price"] == decimal.Decimal("1.50")
    assert row["blob"] == b"hi"
    assert row["at"] == datetime.time(9, 30)
    # `Decimal("1.50") == 1.5` is true, so the value assertion above passes for a double
    # column too. The column type is what says the scale survived the round trip.
    import pyarrow as pa

    price_type = pq.read_table(out_path).schema.field("price").type
    assert pa.types.is_decimal(price_type), price_type
    assert price_type.scale == 2


def test_nested_source_reaches_a_csv_export_as_json(tmp_path):
    """A nested document reaches a CSV export from any JSON-shaped source on the
    default load path, so this is the reachable half of what CSV cannot hold natively.

    Asserted end-to-end rather than on the writer alone, because it is dlt that decides
    whether a nested value arrives as a document or as a normalized child table.
    """
    (tmp_path / "in.jsonl").write_text(
        '{"id": 1, "meta": {"a": 1}, "tags": ["p", "q"]}\n', encoding="utf-8"
    )
    out_path = tmp_path / "out.csv"

    result = invoke_ingest_command(
        f"file://{tmp_path / 'in.jsonl'}",
        "rows",
        f"file://{out_path}",
        "public.rows",
    )
    assert result.exit_code == 0, result.output

    row = _read_back(out_path, "csv")[0]
    assert json.loads(row["meta"]) == {"a": 1}
    assert json.loads(row["tags"]) == ["p", "q"]
