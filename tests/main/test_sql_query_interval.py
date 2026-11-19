"""A `query:` table binds `:interval_start` / `:interval_end` from the incremental
state, so a resumed run reads only the rows past the stored cursor."""

from datetime import datetime

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.engine import Engine

from omniload.main import app


@pytest.fixture
def bound_parameters():
    """Record the parameters of every source statement that reads table `t`."""
    seen = []

    def record(conn, cursor, statement, parameters, context, executemany):
        if "FROM t" in statement:
            seen.append(dict(context.compiled_parameters[0]))

    event.listen(Engine, "before_cursor_execute", record)
    yield seen
    event.remove(Engine, "before_cursor_execute", record)


@pytest.fixture
def source(tmp_path):
    database = tmp_path / "source.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE t (id INTEGER, ts TIMESTAMP)")
    return database


def add_rows(source, *days):
    with duckdb.connect(str(source)) as connection:
        for day in days:
            connection.execute(
                "INSERT INTO t VALUES (?, ?)", [day, datetime(2024, 1, day)]
            )


def ingest(source, destination, query, *extra):
    app(
        [
            "ingest",
            "--source-uri",
            f"duckdb:///{source}",
            "--source-table",
            f"query:{query}",
            "--dest-uri",
            f"duckdb:///{destination}",
            "--dest-table",
            "out.q",
            "--incremental-key",
            "ts",
            "--incremental-strategy",
            "append",
            "--progress",
            "log",
            *extra,
        ],
        standalone_mode=False,
    )


def loaded_ids(destination):
    with duckdb.connect(str(destination)) as connection:
        return [
            row[0]
            for row in connection.sql("SELECT id FROM out.q ORDER BY id").fetchall()
        ]


def test_resumed_run_binds_the_stored_cursor(source, tmp_path, bound_parameters):
    destination = tmp_path / "warehouse.duckdb"
    query = "SELECT id, ts FROM t WHERE ts >= :interval_start"
    add_rows(source, 1, 2, 3)
    ingest(source, destination, query)
    assert bound_parameters[-1]["interval_start"] == datetime(1, 1, 1)

    add_rows(source, 4)
    ingest(source, destination, query)
    assert bound_parameters[-1]["interval_start"] == datetime(2024, 1, 3)
    assert loaded_ids(destination) == [1, 2, 3, 4]


def test_resumed_run_binds_interval_end_open_and_cursor_start(
    source, tmp_path, bound_parameters
):
    destination = tmp_path / "warehouse.duckdb"
    query = "SELECT id, ts FROM t WHERE ts >= :interval_start AND ts < :interval_end"
    add_rows(source, 1, 2)
    ingest(source, destination, query)
    add_rows(source, 3)
    ingest(source, destination, query)
    assert bound_parameters[-1] == {
        "interval_start": datetime(2024, 1, 2),
        "interval_end": datetime.max,
    }
    assert loaded_ids(destination) == [1, 2, 3]


def test_stored_cursor_outranks_interval_start_on_resumed_run(
    source, tmp_path, bound_parameters
):
    """As for a table, --interval-start only seeds the first run."""
    destination = tmp_path / "warehouse.duckdb"
    query = "SELECT id, ts FROM t WHERE ts >= :interval_start"
    add_rows(source, 1, 2, 3)
    ingest(source, destination, query, "--interval-start", "2024-01-02")
    assert bound_parameters[-1]["interval_start"] == datetime(2024, 1, 2)
    add_rows(source, 4)
    ingest(source, destination, query, "--interval-start", "2024-01-01")
    assert bound_parameters[-1]["interval_start"] == datetime(2024, 1, 3)
    assert loaded_ids(destination) == [2, 3, 4]


def test_interval_window_binds_both_bounds_on_resumed_run(
    source, tmp_path, bound_parameters
):
    """A start and end together window the run, past the stored cursor. dlt keeps
    no boundary state for a window, so `append` loads the rows in it again."""
    destination = tmp_path / "warehouse.duckdb"
    query = "SELECT id, ts FROM t WHERE ts >= :interval_start AND ts <= :interval_end"
    add_rows(source, 1, 2, 3, 4)
    ingest(source, destination, query)
    ingest(
        source,
        destination,
        query,
        "--interval-start",
        "2024-01-01",
        "--interval-end",
        "2024-01-02",
    )
    assert bound_parameters[-1] == {
        "interval_start": datetime(2024, 1, 1),
        "interval_end": datetime(2024, 1, 2),
    }
    assert loaded_ids(destination) == [1, 1, 2, 2, 3, 4]


def test_query_without_placeholders_resumes(source, tmp_path, bound_parameters):
    destination = tmp_path / "warehouse.duckdb"
    query = "SELECT id, ts FROM t"
    add_rows(source, 1, 2)
    ingest(source, destination, query)
    add_rows(source, 3)
    ingest(source, destination, query)
    assert bound_parameters[-1] == {}
    assert loaded_ids(destination) == [1, 2, 3]


@pytest.mark.parametrize(
    ("comparison", "expected"),
    [(">=", [1, 2, 3, 4, 5]), (">", [1, 2, 3, 4])],
    ids=["inclusive", "exclusive"],
)
def test_resumed_run_reads_new_rows_at_the_stored_cursor(
    source, tmp_path, comparison, expected
):
    """The stored cursor is inclusive: a row added at it loads only through `>=`,
    and the rows the last run already loaded there are not loaded twice."""
    destination = tmp_path / "warehouse.duckdb"
    query = f"SELECT id, ts FROM t WHERE ts {comparison} :interval_start"
    add_rows(source, 1, 2, 3)
    ingest(source, destination, query)
    with duckdb.connect(str(source)) as connection:
        connection.execute("INSERT INTO t VALUES (5, ?)", [datetime(2024, 1, 3)])
    add_rows(source, 4)
    ingest(source, destination, query)
    assert loaded_ids(destination) == expected
