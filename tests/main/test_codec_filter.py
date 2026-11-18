"""Interval normalization: `timedelta` values and Arrow `duration` columns load as text."""

import hashlib
from datetime import timedelta

import duckdb
import numpy as np
import pandas as pd
import pendulum
import pyarrow as pa
import pyarrow.parquet
import pytest

from omniload.codec.filter import cast_timedelta, format_interval
from tests.util import invoke_ingest_command

US = 1_000
SECOND = 1_000_000_000
DAY = 86_400 * SECOND

EXAMPLES = [
    (timedelta(days=1, hours=2, minutes=3, seconds=4.5), "1 day 02:03:04.5"),
    (timedelta(hours=-3), "-03:00:00"),
    (-timedelta(days=1, hours=3), "-1 days -03:00:00"),
    (timedelta(days=-1), "-1 days"),
    (timedelta(0), "00:00:00"),
    (timedelta(days=2), "2 days"),
    (timedelta(days=428), "428 days"),
    (timedelta(microseconds=1), "00:00:00.000001"),
    (timedelta(microseconds=-1), "-00:00:00.000001"),
    (timedelta(hours=23, minutes=59, seconds=59), "23:59:59"),
    (timedelta(seconds=0.25), "00:00:00.25"),
]


@pytest.mark.parametrize(("value", "text"), EXAMPLES, ids=[t for _, t in EXAMPLES])
def test_a_timedelta_row_value_becomes_interval_text(value, text):
    assert cast_timedelta({"d": value}) == {"d": text}


@pytest.mark.parametrize(("value", "text"), EXAMPLES, ids=[t for _, t in EXAMPLES])
def test_duckdb_reads_the_text_back_as_the_same_interval(value, text):
    with duckdb.connect() as connection:
        parsed = connection.execute("SELECT CAST(? AS INTERVAL)", [text]).fetchone()
    assert parsed == (value,)


def test_the_extremes_format_without_overflow():
    assert cast_timedelta({"d": timedelta.max})["d"] == "999999999 days 23:59:59.999999"
    assert cast_timedelta({"d": timedelta.min})["d"] == "-999999999 days"


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (pd.Timedelta(1500, "ns"), "00:00:00.0000015"),
        (pd.Timedelta(-1, "ns"), "-00:00:00.000000001"),
        (pd.Timedelta(-1500, "ns"), "-00:00:00.0000015"),
        (pd.Timedelta("1 days 00:00:00.000001500"), "1 day 00:00:00.0000015"),
        (pd.Timedelta("1 days 02:03:04.5"), "1 day 02:03:04.5"),
    ],
)
def test_a_pandas_timedelta_keeps_its_nanoseconds(value, text):
    assert cast_timedelta({"d": value}) == {"d": text}


@pytest.mark.parametrize("unit", ["s", "ms"])
@pytest.mark.parametrize("count", [2**63 - 1, -(2**63 - 1)])
def test_a_coarse_pandas_timedelta_past_the_nanosecond_range_is_exact(unit, count):
    # pandas keeps second and millisecond resolution, so such a value can hold more
    # than an int64 count of nanoseconds; it formats like the Arrow duration does.
    value = pd.Timedelta(np.timedelta64(count, unit))
    expected = format_interval(count * {"s": SECOND, "ms": 1_000_000}[unit])

    assert cast_timedelta({"d": value}) == {"d": expected}


def test_duckdb_reads_nanosecond_text_to_the_microsecond():
    with duckdb.connect() as connection:
        parsed = connection.execute(
            "SELECT CAST('1 day 00:00:00.0000015' AS INTERVAL)"
        ).fetchone()
    assert parsed == (timedelta(days=1, microseconds=1),)


def test_a_timedelta_subclass_is_converted():
    class DriverTimeDelta(timedelta):
        pass

    assert cast_timedelta({"d": DriverTimeDelta(seconds=90)}) == {"d": "00:01:30"}


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (pendulum.duration(hours=-3), "-03:00:00"),
        (pendulum.duration(days=-1, hours=-3), "-1 days -03:00:00"),
        (pendulum.duration(days=1, hours=2, seconds=4.5), "1 day 02:00:04.5"),
    ],
)
def test_a_subclass_that_overrides_the_fields_is_read_from_timedelta(value, text):
    # pendulum's `Duration` reports `.seconds` signed, not normalized to a day.
    assert cast_timedelta({"d": value}) == {"d": text}


def test_other_values_in_the_row_are_left_alone():
    row = {"id": 1, "d": timedelta(minutes=5), "note": None, "nested": [timedelta(1)]}
    nested = row["nested"]

    result = cast_timedelta(row)

    assert result is row
    assert result == {"id": 1, "d": "00:05:00", "note": None, "nested": nested}


@pytest.mark.parametrize("item", ["text", 42, None, b"raw"])
def test_items_that_are_neither_rows_nor_arrow_pass_through(item):
    assert cast_timedelta(item) is item


@pytest.mark.parametrize(
    ("unit", "count", "text"),
    [
        ("s", 93_784, "1 day 02:03:04"),
        ("ms", -10_800_000, "-03:00:00"),
        ("us", 1, "00:00:00.000001"),
        ("ns", 1_500, "00:00:00.0000015"),
        ("s", 2**63 - 1, format_interval((2**63 - 1) * SECOND)),
        ("ns", -(2**63), format_interval(-(2**63))),
    ],
)
@pytest.mark.parametrize("container", [pa.table, pa.record_batch])
def test_an_arrow_duration_column_becomes_text(container, unit, count, text):
    item = container(
        {"id": pa.array([1, 2]), "d": pa.array([count, None], type=pa.duration(unit))}
    )

    result = cast_timedelta(item)

    assert type(result) is type(item)
    assert result.schema.field("d").type == pa.string()
    assert result.column("d").to_pylist() == [text, None]
    assert result.column("id").to_pylist() == [1, 2]


def test_the_largest_second_duration_is_exact():
    assert format_interval((2**63 - 1) * SECOND) == "106751991167300 days 15:30:07"


def test_the_arrow_rebuild_keeps_field_and_schema_metadata():
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False, metadata={"k": "id"}),
            pa.field("d", pa.duration("us"), nullable=False, metadata={"k": "d"}),
        ],
        metadata={"dlt.null_columns": "[]"},
    )
    table = pa.table({"id": [1], "d": pa.array([5], pa.duration("us"))}, schema=schema)

    result = cast_timedelta(table)

    assert result.schema.metadata == schema.metadata
    assert result.schema.field("id") == schema.field("id")
    duration = result.schema.field("d")
    assert (duration.type, duration.nullable, duration.metadata) == (
        pa.string(),
        False,
        {b"k": b"d"},
    )


def test_a_chunked_duration_column_keeps_its_chunks():
    durations = pa.chunked_array(
        [
            pa.array([1, 2], pa.duration("s")),
            pa.array([], pa.duration("s")),
            pa.array([3, 4, 5], pa.duration("s")).slice(1),
        ]
    )
    ids = pa.chunked_array([[1, 2], [], [4, 5]])
    table = pa.table({"id": ids, "d": durations})

    result = cast_timedelta(table)

    assert [len(chunk) for chunk in result.column("d").chunks] == [2, 0, 2]
    assert result.column("d").to_pylist() == [
        "00:00:01",
        "00:00:02",
        "00:00:04",
        "00:00:05",
    ]
    assert result.column("id").equals(ids)
    assert [len(chunk) for chunk in result.column("id").chunks] == [2, 0, 2]


def test_an_arrow_item_without_durations_is_returned_as_is():
    table = pa.table({"id": [1], "t": pa.array([1], pa.time64("us"))})
    assert cast_timedelta(table) is table


# End to end through the CLI. DuckDB hands an INTERVAL over as a `timedelta`, which
# the sqlalchemy backend yields in dict rows and the pyarrow backend as `duration[us]`.


@pytest.fixture
def interval_source(tmp_path):
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA raw")
        connection.execute("CREATE TABLE raw.t (id INTEGER, d INTERVAL, note TEXT)")
        connection.execute(
            """
            INSERT INTO raw.t VALUES
                (1, INTERVAL '1 day 2 hours 3 minutes 4.5 seconds', 'a'),
                (2, NULL, 'b'),
                (3, INTERVAL '-3 hours', 'c'),
                (4, INTERVAL '-1 day -3 hours', 'd')
            """
        )
    return f"duckdb:///{path}"


def _rows(dest, query):
    with duckdb.connect(str(dest), read_only=True) as connection:
        return connection.execute(query).fetchall()


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_an_interval_column_loads_as_text(interval_source, tmp_path, backend):
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        interval_source,
        "raw.t",
        f"duckdb:///{dest}",
        "main.t",
        sql_backend=backend,
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    assert _rows(dest, "SELECT id, d, note FROM main.t ORDER BY id") == [
        (1, "1 day 02:03:04.5", "a"),
        (2, None, "b"),
        (3, "-03:00:00", "c"),
        (4, "-1 days -03:00:00", "d"),
    ]
    assert _rows(dest, "SELECT DISTINCT typeof(d) FROM main.t") == [("VARCHAR",)]


def test_a_masked_interval_column_keeps_its_hash(interval_source, tmp_path):
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        interval_source,
        "raw.t",
        f"duckdb:///{dest}",
        "main.t",
        sql_backend="sqlalchemy",
        mask=["d:sha256"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    value = timedelta(days=1, hours=2, minutes=3, seconds=4.5)
    expected = hashlib.sha256(str(value).encode()).hexdigest()
    assert _rows(dest, "SELECT d FROM main.t WHERE id = 1") == [(expected,)]


# Masks that do not apply to an interval and hand it back as it is.
INTERVAL_PASS_THROUGH_MASKS = [
    "round",
    "noise",
    "range",
    "date_shift",
    "year_only",
    "month_year",
]


@pytest.mark.parametrize("algorithm", INTERVAL_PASS_THROUGH_MASKS)
@pytest.mark.parametrize("path", ["sqlalchemy", "pyarrow", "record-batches"])
def test_a_mask_that_leaves_an_interval_as_it_is_fails_the_run(
    interval_source, tmp_path, algorithm, path
):
    # The interval would otherwise become text after masking and load in clear.
    if path == "record-batches":
        source = tmp_path / "source.parquet"
        pa.parquet.write_table(
            pa.table(
                {
                    "id": [1, 2],
                    "d": pa.array(
                        [timedelta(days=1, hours=2, minutes=3, seconds=4.5), None],
                        type=pa.duration("us"),
                    ),
                }
            ),
            source,
        )
        options = {"source_uri": f"file://{source}#use_pyarrow=true", "table": "rows"}
    else:
        options = {"source_uri": interval_source, "table": "raw.t", "sql_backend": path}
    dest = tmp_path / "warehouse.duckdb"

    source_uri = options.pop("source_uri")
    table = options.pop("table")
    result = invoke_ingest_command(
        source_uri,
        table,
        f"duckdb:///{dest}",
        "main.t",
        mask=[f"d:{algorithm}"],
        print_output=False,
        **options,
    )

    assert result.exit_code != 0, result.output
    assert "would load it unmasked" in repr(result.exception)


@pytest.mark.parametrize("algorithm", ["date_shift", "year_only", "month_year"])
def test_a_date_mask_does_not_read_a_short_interval_as_a_time(algorithm):
    # `1:00:00` parses as one o'clock today, which `date_shift` would load as a
    # timestamp carrying the interval's hours.
    from omniload.codec.masking import create_masking_mapper

    mapper = create_masking_mapper([f"d:{algorithm}"])
    items = [
        {"d": timedelta(hours=1)},
        pa.record_batch({"d": pa.array([timedelta(hours=1)], pa.duration("us"))}),
    ]
    for item in items:
        with pytest.raises(ValueError, match="would load it unmasked"):
            mapper(item)


def test_an_arrow_file_with_a_duration_column_loads(tmp_path):
    source = tmp_path / "source.arrow"
    table = pa.table(
        {
            "id": pa.array([1, 2]),
            "d": pa.array([93_784_500_000_000, None], type=pa.duration("ns")),
        }
    )
    with pa.OSFile(str(source), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"mmap://{source}",
        "whatever",
        f"duckdb:///{dest}",
        "main.t",
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    assert _rows(dest, "SELECT id, d FROM main.t ORDER BY id") == [
        (1, "1 day 02:03:04.5"),
        (2, None),
    ]


def test_a_masked_duration_column_in_record_batches_is_hashed(tmp_path):
    # `use_pyarrow` makes the Parquet reader yield RecordBatch items, which the mask
    # has to reach before the duration column is turned into loadable text.
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table({"id": [1], "d": pa.array([3_600], type=pa.duration("s"))}), source
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=["d:sha256"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    # The mask reads the column's Python value, as the dict-row path does.
    expected = hashlib.sha256(str(timedelta(hours=1)).encode()).hexdigest()
    assert _rows(dest, "SELECT d FROM main.t") == [(expected,)]


def test_an_unmasked_duration_beside_a_masked_column_loads_as_text(tmp_path):
    source = tmp_path / "source.parquet"
    pa.parquet.write_table(
        pa.table({"id": [1], "d": pa.array([3_600], type=pa.duration("s"))}), source
    )
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        f"file://{source}#use_pyarrow=true",
        "rows",
        f"duckdb:///{dest}",
        "main.t",
        mask=["id:sha256"],
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    expected = hashlib.sha256(b"1").hexdigest()
    assert _rows(dest, "SELECT id, d FROM main.t") == [(expected, "01:00:00")]
