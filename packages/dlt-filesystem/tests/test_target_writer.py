"""The writers themselves: what lands on disk, independent of the URI plumbing."""

import csv
import datetime
import decimal
import json
import math
import os
import subprocess
import sys

import pytest
import yaml

from dlt_filesystem.source.error import MissingDecoderError
from dlt_filesystem.target.registry import WRITE_FORMATS, writer_for_format

ROWS = [{"id": 1, "name": "Zoë"}, {"id": 2, "name": "Ōtautahi", "note": "late column"}]

# Writes each format from a child interpreter and reports the encoding that child's
# text handles would default to, so the parent can tell "the locale was forced" from
# "the platform ignored the request".
_CHILD = """
import sys, tempfile
from dlt_filesystem.target.registry import writer_for_format

# What `open()` in text mode would actually use, asked of a real handle rather than
# of the locale module: `locale.getencoding()` is 3.11+, and this package supports
# 3.10, where the equivalent spelling is `getpreferredencoding(False)`.
with tempfile.TemporaryFile("w") as probe:
    print(probe.encoding)

out_dir, rows = sys.argv[1], __import__("json").loads(sys.argv[2])
for file_format in sys.argv[3].split(","):
    writer_for_format(file_format)(f"{out_dir}/out.{file_format}", rows)
"""


def test_write_json_emits_one_array_document(tmp_path):
    """One document, not a concatenated stream: `read_json` parses the whole file as a
    single value and only falls back to line-delimited parsing when that fails."""
    path = tmp_path / "out.json"
    writer_for_format("json")(str(path), ROWS)

    assert json.loads(path.read_text(encoding="utf-8")) == ROWS


def test_write_json_of_no_rows_is_an_empty_array(tmp_path):
    path = tmp_path / "out.json"
    writer_for_format("json")(str(path), [])

    assert path.read_text(encoding="utf-8") == "[]"


def test_write_yaml_emits_one_sequence_document(tmp_path):
    """One document holding a list, not a `---`-separated document per row.

    Both shapes round-trip through `read_yaml`, which is why this asserts the document
    count rather than the rows: a stream would load the same records and still be a
    file no other writer here produces. Multi-row, because a single row cannot tell the
    two apart.
    """
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(str(path), ROWS)

    text = path.read_text(encoding="utf-8")
    documents = list(yaml.safe_load_all(text))
    assert len(documents) == 1
    assert documents[0] == ROWS
    assert "---" not in text


def test_write_yaml_keeps_the_column_order_of_the_row(tmp_path):
    """`sort_keys=False`, so an export reads in the order the load produced, the same
    promise `_column_union` makes for the writers that carry a header."""
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(str(path), [{"name": "Zoe", "id": 1, "age": 2}])

    assert path.read_text(encoding="utf-8").splitlines() == [
        "- name: Zoe",
        "  id: 1",
        "  age: 2",
    ]


def test_write_yaml_of_no_rows_is_an_empty_sequence(tmp_path):
    """An empty load writes an explicit empty document, not an empty file.

    Both load as zero rows here -- `_yaml_eager_decode` returns early on empty input --
    so this pins what the file *says* rather than what this reader does with it: `[]` is
    a document every YAML parser reads as an empty sequence, where an empty file is a
    parser-by-parser question.
    """
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(str(path), [])

    assert path.read_text(encoding="utf-8") == "[]\n"
    assert yaml.safe_load(path.read_text(encoding="utf-8")) == []


def test_write_yaml_spells_native_types_the_way_the_json_writers_do(tmp_path):
    """`--loader-file-format parquet` is the one path that hands a writer native values,
    and PyYAML's safe dumper raises on half of them. A `Decimal` writes as the string
    dlt's own serializer produces, keeping a scale a float would drop; the types YAML
    knows keep their native spelling, so a datetime reads back as a datetime.
    """
    path = tmp_path / "out.yaml"
    writer_for_format("yaml")(
        str(path),
        [
            {
                "price": decimal.Decimal("1.50"),
                "at": datetime.time(9, 30),
                "blob": b"hi",
                "ts": datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc),
            }
        ],
    )

    text = path.read_text(encoding="utf-8")
    assert "!!binary" in text, "bytes keep YAML's own binary tag rather than a repr"

    # Read with PyYAML, so these are claims about the file. Through omniload's own
    # reader the `!!binary` becomes the base64 string `'aGk='`, which is what `.json`
    # and `.jsonl` round-trip the same value to, by a different on-disk spelling.
    loaded = yaml.safe_load(text)[0]
    # dlt's spelling for the two types PyYAML would refuse, so a decimal keeps a scale
    # a float would drop and a time keeps its ISO form.
    assert loaded["price"] == "1.50"
    assert loaded["at"] == "09:30:00"
    # ... and the two it does know keep their native YAML types, so a datetime reads
    # back as a datetime rather than as text.
    assert loaded["ts"] == datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    assert loaded["blob"] == b"hi"


def test_write_yaml_without_pyyaml_names_the_install(tmp_path, monkeypatch):
    """The same install hint the reader gives, rather than a bare ImportError.

    PyYAML is an unconditional `dlt` dependency, so this branch is unreachable through
    a real install; it exists so the writer keeps the contract `yaml.md` states for the
    format as a whole.
    """
    monkeypatch.setitem(sys.modules, "yaml", None)

    with pytest.raises(MissingDecoderError) as exc:
        writer_for_format("yaml")(str(tmp_path / "out.yaml"), ROWS)

    assert "dlt-filesystem[iterable]" in str(exc.value)


def test_writers_emit_utf8_whatever_the_locale(tmp_path):
    """The readers decode as UTF-8 unconditionally (`json.loadb` accepts nothing else,
    Polars defaults to it), so a locale-encoded export would not read back on the
    machine that wrote it. A text handle's encoding is fixed at interpreter startup, so
    forcing a non-UTF-8 default needs a child process rather than a monkeypatch.
    """
    binary_formats = {"arrow", "feather", "ipc", "orc", "parquet", "vortex"}
    text_formats = [f for f in WRITE_FORMATS if f not in binary_formats]
    child = subprocess.run(  # noqa: S603  # trusted: sys.executable + a fixed code string
        [
            sys.executable,
            "-c",
            _CHILD,
            str(tmp_path),
            json.dumps(ROWS),
            ",".join(text_formats),
        ],
        env={
            **os.environ,
            "LC_ALL": "C",
            "PYTHONUTF8": "0",
            "PYTHONCOERCECLOCALE": "0",
        },
        capture_output=True,
        text=True,
    )
    assert child.returncode == 0, child.stderr
    if child.stdout.strip().lower().replace("-", "") in {"utf8", "cp65001"}:
        pytest.skip(f"platform kept a UTF-8 default: {child.stdout.strip()}")

    for file_format in text_formats:
        written = (tmp_path / f"out.{file_format}").read_bytes().decode("utf-8")
        assert "Zoë" in written and "Ōtautahi" in written, file_format


# --- the columnar writers, on Polars (#328) ---

# One row past Polars' default inference window, which is 100 rows.
PAST_THE_WINDOW = [{"id": i} for i in range(150)] + [{"id": 150, "late": "x"}]


def test_write_csv_keeps_a_column_that_first_appears_past_the_inference_window(
    tmp_path,
):
    """dlt omits null keys per row, so a column can first appear anywhere in a load.

    Polars infers a schema from the first 100 rows unless told otherwise, and this
    fixture puts the only row carrying `late` well past that, so a default-window
    inference drops the column from the header rather than failing.
    """
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), PAST_THE_WINDOW)

    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "id,late"
    assert lines[-1] == "150,x"


def test_write_parquet_keeps_a_column_that_first_appears_past_the_inference_window(
    tmp_path,
):
    """The same window, for the writer whose schema is the file's own."""
    import pyarrow.parquet as pq

    path = tmp_path / "out.parquet"
    writer_for_format("parquet")(str(path), PAST_THE_WINDOW)

    table = pq.read_table(path)
    assert table.column_names == ["id", "late"]
    assert table.to_pylist()[-1] == {"id": 150, "late": "x"}


def test_write_csv_keeps_crlf_line_endings(tmp_path):
    """What this destination has always written -- `csv.DictWriter` defaults to CRLF,
    as does RFC 4180 -- where Polars defaults to LF. Asserted on the bytes, because a
    text-mode read translates the ending away and would pass either way."""
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), ROWS)

    written = path.read_bytes()
    assert written.startswith(b"id,name,note\r\n")
    assert written.endswith(b"\r\n")
    assert b"\n" not in written.replace(b"\r\n", b"")


def test_write_csv_writes_a_nested_value_as_json(tmp_path):
    """A nested document reaches a CSV export from any JSON-shaped source, and Polars
    refuses a struct or list column outright. It is encoded rather than left to abort,
    and as JSON rather than as the `str()` of a Python object: the replaced writer wrote
    `{'a': 1}`, which is valid neither as JSON nor as anything a reader parses.
    """
    path = tmp_path / "out.csv"
    writer_for_format("csv")(
        str(path), [{"id": 1, "meta": {"a": 1, "b": "x"}, "tags": ["p", "q"]}]
    )

    row = next(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    assert json.loads(row["meta"]) == {"a": 1, "b": "x"}
    assert json.loads(row["tags"]) == ["p", "q"]


def test_write_csv_spells_bytes_the_way_the_json_writers_do(tmp_path):
    """`--loader-file-format parquet` hands a writer native `bytes`, and CSV has no
    column type for them. The replaced writer wrote the Python repr `b'hi'`; this
    writes the base64 string `.jsonl` writes for the same value, so one column reads
    the same whichever format the export names.
    """
    rows = [{"blob": b"hi"}]
    csv_path, jsonl_path = tmp_path / "out.csv", tmp_path / "out.jsonl"
    writer_for_format("csv")(str(csv_path), rows)
    writer_for_format("jsonl")(str(jsonl_path), rows)

    cell = next(csv.DictReader(csv_path.read_text(encoding="utf-8").splitlines()))
    assert cell["blob"] == json.loads(jsonl_path.read_text(encoding="utf-8"))["blob"]
    assert cell["blob"] == "aGk="


def test_write_parquet_keeps_snappy_compression(tmp_path):
    """PyArrow's default and this destination's output to date. Polars defaults to
    Zstd, which is smaller but is a codec a consumer either supports or fails on, so
    the migration pins the one already being written."""
    import pyarrow.parquet as pq

    path = tmp_path / "out.parquet"
    writer_for_format("parquet")(str(path), ROWS)

    metadata = pq.ParquetFile(path).metadata
    codecs = {
        metadata.row_group(group).column(column).compression
        for group in range(metadata.num_row_groups)
        for column in range(metadata.num_columns)
    }
    assert codecs == {"SNAPPY"}


def test_write_csv_of_no_rows_writes_the_header_line_it_always_has(tmp_path):
    """An empty load writes one empty line, which is what `csv.DictWriter` produced for
    a header of no columns. Asserted on the bytes because both an empty file and this
    one read back as zero rows, so the end-to-end case cannot tell them apart."""
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), [])

    assert path.read_bytes() == b"\r\n"


def test_write_csv_keeps_float_values_through_a_spelling_change(tmp_path):
    """Polars spells some floats differently from `str()`: `1e-05` writes as `0.00001`,
    `1e-07` as `1e-7`, and a NaN as `NaN`. The spelling is cosmetic and the value is
    not, so this pins the values rather than the text a Polars release chooses.
    """
    values = [1e-5, 1e-7, 1e20, 1e308, 5e-324, 0.1, -0.0, float("inf")]
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), [{"a": value} for value in values])

    read_back = [
        float(row["a"])
        for row in csv.DictReader(path.read_text(encoding="utf-8").splitlines())
    ]
    assert read_back == values
    # `-0.0 == 0.0`, so the sign is asserted rather than left to the comparison above.
    assert math.copysign(1, read_back[values.index(-0.0)]) == -1
    # NaN compares unequal to itself, so it is asserted separately rather than left out.
    nan_path = tmp_path / "nan.csv"
    writer_for_format("csv")(str(nan_path), [{"a": float("nan")}])
    assert math.isnan(
        float(
            next(csv.DictReader(nan_path.read_text(encoding="utf-8").splitlines()))["a"]
        )
    )


def test_write_csv_spells_a_nested_value_before_polars_types_the_column(tmp_path):
    """Polars types a column across the whole load, so spelling a nested value read back
    out of a frame would not spell the value dlt produced.

    Two rows carrying different keys come back with each other's keys as null; a list of
    one integer past f64's exact range, beside a list of one float, comes back rounded.
    dlt keeps a nested column as a single JSON column rather than splitting it into
    variants, so both are reachable on the default load path.
    """
    path = tmp_path / "out.csv"
    writer_for_format("csv")(
        str(path),
        [
            {"meta": {"a": 1}, "tags": [9007199254740993]},
            {"meta": {"b": 2}, "tags": [0.5]},
        ],
    )

    rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    assert [json.loads(row["meta"]) for row in rows] == [{"a": 1}, {"b": 2}]
    assert [json.loads(row["tags"]) for row in rows] == [[9007199254740993], [0.5]]


def test_write_csv_spells_a_list_of_several_types(tmp_path):
    """The third way a typed column changes a nested value: Polars cannot build a series
    from a list holding a string, an integer and a boolean at all, so a row dlt happily
    produces would abort the export rather than lose precision quietly."""
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), [{"tags": ["p", 1, True]}])

    row = next(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    tags = json.loads(row["tags"])
    assert tags == ["p", 1, True]
    # `True == 1` in Python, so the values alone do not say the boolean stayed one.
    assert [type(tag) for tag in tags] == [str, int, bool]


def test_write_csv_keeps_a_record_whose_only_column_is_null(tmp_path):
    """A record with every field null is a blank line in a one-column file, and a blank
    line is not a record to most readers, so the row is lost on the way back in. The csv
    module wrote a quoted empty field here; Polars leaves a null bare, so the file is
    written quoted. dlt omits a null key rather than writing it, which is what makes a
    one-column load produce an empty record in the first place.

    Quoted rather than filled: filling the null would mean casting the column to text,
    and a date or a float is spelled differently by a cast than by the CSV writer, so
    the one file that happened to carry a null would read differently from every other.
    """
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), [{"name": "a"}, {}, {"name": "b"}])

    assert path.read_bytes() == b'"name"\r\n"a"\r\n""\r\n"b"\r\n'
    assert len(list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))) == 3


def test_write_csv_of_rows_that_carry_no_columns_at_all(tmp_path):
    """Every row null across a one-column load leaves rows with no keys, and Polars
    refuses a frame with height and no width. The file says zero rows, which is what the
    replaced writer's blank lines also read back as."""
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), [{}, {}])

    assert path.read_bytes() == b"\r\n"


def test_write_csv_writes_a_decimal_wider_than_polars_holds(tmp_path):
    """Polars stops at 128-bit decimals where PyArrow reached for a 256-bit one, so a
    `DECIMAL(50,2)` column that the replaced writer exported would abort this one. A
    decimal is spelled by dlt's serializer for that reason, which keeps every digit and
    leaves an ordinary decimal reading exactly as it did."""
    path = tmp_path / "out.csv"
    wide = decimal.Decimal("1234567890123456789012345678901234567890.12")
    writer_for_format("csv")(
        str(path), [{"wide": wide, "ordinary": decimal.Decimal("1.50")}]
    )

    row = next(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    assert decimal.Decimal(row["wide"]) == wide
    assert row["ordinary"] == "1.50"


def test_write_csv_keeps_native_spelling_in_a_one_column_file_with_a_null(tmp_path):
    """The quoting above must not reach the values themselves: a datetime is spelled by
    the CSV writer either way, not by a cast to text, so a file that carries a null
    reads the same as one that does not."""
    when = datetime.datetime(2020, 1, 1, 12, 0)
    with_null = tmp_path / "with_null.csv"
    without_null = tmp_path / "without_null.csv"
    writer_for_format("csv")(str(with_null), [{"at": when}, {}])
    writer_for_format("csv")(str(without_null), [{"at": when}])

    written = with_null.read_text(encoding="utf-8")
    assert "2020-01-01T12:00:00.000000" in written
    assert (
        next(csv.DictReader(written.splitlines()))["at"]
        == next(csv.DictReader(without_null.read_text(encoding="utf-8").splitlines()))[
            "at"
        ]
    )


# Loads whose numbers Polars would write rounded, and which PyArrow refused outright.
# dlt splits a scalar column of two types into variants but keeps a nested one whole,
# so a list or a struct field is how this arrives in practice.
ROUNDED_BY_A_DOUBLE = {
    "a bare column": [{"v": 9007199254740993}, {"v": 0.5}],
    "a list": [{"v": [9007199254740993]}, {"v": [0.5]}],
    "one struct field": [{"v": {"n": 9007199254740993}}, {"v": {"n": 0.5}}],
    # Widened to lists of floats within the first row, then to lists of strings by the
    # second, so the column's final type says nothing about the rounding on the way.
    "a list widened twice": [{"v": [[9007199254740993], [0.5]]}, {"v": [["s"]]}],
    # A Decimal is a number a double cannot hold either, and it is not an int.
    "a decimal": [{"v": decimal.Decimal("9007199254740993")}, {"v": 0.5}],
}


@pytest.mark.parametrize("rows", ROUNDED_BY_A_DOUBLE.values(), ids=ROUNDED_BY_A_DOUBLE)
def test_writers_refuse_to_round_a_number_a_double_cannot_hold(tmp_path, rows):
    """PyArrow refused a column holding both a large number and a float, rather than
    widening it and writing the number rounded. Polars widens silently, so the values
    are compared against the frame before it is written."""
    with pytest.raises(ValueError, match="'v'"):
        writer_for_format("parquet")(str(tmp_path / "out.parquet"), rows)


# The same shapes, where nothing is rounded. A check that reads the column as a whole
# rather than value by value refuses these too, which would be worse than the bug.
EXACT_ALONGSIDE_A_FLOAT = {
    "separate struct fields": [{"v": {"n": 9007199254740993, "ratio": 0.5}}],
    "separate list entries": [{"v": [{"n": 9007199254740993}, {"ratio": 0.5}]}],
    "a large integer alone": [{"v": 9007199254740993}, {"v": 1}],
    "a float column with no large number": [{"v": 0.1}, {"v": 0.5}],
    "a large integer beside text": [{"v": 9007199254740993}, {"v": "x"}],
    # The same double widening as the refused case above, with the float in its own
    # row: Polars goes straight to text and writes every digit, so nothing is rounded
    # and the check has to tell the two apart by the values, not by the type.
    "a list widened twice, exactly": [
        {"v": [[9007199254740993]]},
        {"v": [[0.5]]},
        {"v": [["s"]]},
    ],
}


@pytest.mark.parametrize(
    "rows", EXACT_ALONGSIDE_A_FLOAT.values(), ids=EXACT_ALONGSIDE_A_FLOAT
)
def test_the_rounding_check_leaves_an_exact_load_alone(tmp_path, rows):
    """Every value here survives as itself, so the write goes through."""
    import pyarrow.parquet as pq

    path = tmp_path / "out.parquet"
    writer_for_format("parquet")(str(path), rows)

    assert len(pq.read_table(path).to_pylist()) == len(rows)


def test_the_rounding_check_writes_a_large_integer_as_itself(tmp_path):
    """The check is about the widening, not about the size: a large integer in a column
    with no float in it is exact as an int64 and reads back digit for digit."""
    import pyarrow.parquet as pq

    path = tmp_path / "out.parquet"
    writer_for_format("parquet")(str(path), [{"id": 9007199254740993}, {"id": 1}])

    assert pq.read_table(path).to_pylist() == [{"id": 9007199254740993}, {"id": 1}]


def test_csv_writes_the_same_load_exactly_because_it_spells_first(tmp_path):
    """CSV spells a nested value before Polars types the column, so the load the
    Parquet writer refuses is written exactly rather than refused."""
    path = tmp_path / "out.csv"
    writer_for_format("csv")(str(path), [{"tags": [9007199254740993]}, {"tags": [0.5]}])

    rows = list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    assert [json.loads(row["tags"]) for row in rows] == [[9007199254740993], [0.5]]
