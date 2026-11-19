"""Custom SQL queries must not change the loader used by subsequent SQL sources."""

import sqlite3
from unittest.mock import Mock

import dlt.sources.sql_database as sql_database
import duckdb
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from omniload.core.router import SqlSourceRouter
from omniload.error import ValidationError
from omniload.main import app


@pytest.fixture
def sql_source(tmp_path, monkeypatch):
    # Contain a regression after the assertions, never between the two loads.
    monkeypatch.setattr(sql_database, "table_rows", sql_database.table_rows)
    database = tmp_path / "source.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE t (id INTEGER, v TEXT)")
        connection.executemany("INSERT INTO t VALUES (?, ?)", [(1, "one"), (2, "two")])
    return f"sqlite:///{database}"


def test_query_then_plain_table_in_one_process(sql_source, tmp_path):
    destination = tmp_path / "warehouse.duckdb"
    original = sql_database.table_rows
    for selector, table in [
        ("query:SELECT id, v FROM t WHERE id = 2", "queried"),
        ("main.t", "plain"),
    ]:
        app(
            [
                "ingest",
                "--source-uri",
                sql_source,
                "--source-table",
                selector,
                "--dest-uri",
                f"duckdb:///{destination}",
                "--dest-table",
                f"out.{table}",
                "--progress",
                "log",
            ],
            standalone_mode=False,
        )

    with duckdb.connect(str(destination)) as connection:
        assert connection.sql("SELECT id, v FROM out.queried").fetchall() == [
            (2, "two")
        ]
        assert connection.sql("SELECT id, v FROM out.plain ORDER BY id").fetchall() == [
            (1, "one"),
            (2, "two"),
        ]
    assert sql_database.table_rows is original


def test_query_resources_capture_their_own_loaders(sql_source):
    original = sql_database.table_rows
    router = SqlSourceRouter()
    first = router.dlt_source(
        sql_source,
        "query:SELECT id, v FROM t WHERE id = :interval_start",
        interval_start=1,
        page_size=100,
    )
    assert sql_database.table_rows is original
    second = router.dlt_source(
        sql_source,
        "query:SELECT id, v FROM t WHERE id = :interval_start",
        interval_start=2,
        page_size=100,
    )
    assert sql_database.table_rows is original
    assert list(second) == [{"id": 2, "v": "two"}]
    assert list(first) == [{"id": 1, "v": "one"}]
    assert list(router.dlt_source(sql_source, "main.t", page_size=100)) == [
        {"id": 1, "v": "one"},
        {"id": 2, "v": "two"},
    ]


def test_query_builder_failure_restores_the_existing_loader(sql_source, monkeypatch):
    existing = Mock(wraps=sql_database.table_rows)
    monkeypatch.setattr(sql_database, "table_rows", existing)
    error = RuntimeError("builder failed")

    def broken_builder(**kwargs):
        assert sql_database.table_rows is not existing
        raise error

    with pytest.raises(RuntimeError) as raised:
        SqlSourceRouter(table_builder=broken_builder).dlt_source(
            sql_source, "query:SELECT id, v FROM t"
        )
    assert raised.value is error
    assert sql_database.table_rows is existing


def test_query_credentials_failure_does_not_change_loader(sql_source, monkeypatch):
    original = sql_database.table_rows
    error = ValueError("credentials failed")

    def broken_credentials(*args, **kwargs):
        assert sql_database.table_rows is original
        raise error

    monkeypatch.setattr(
        "omniload.core.router.ConnectionStringCredentials", broken_credentials
    )
    with pytest.raises(ValueError) as raised:
        SqlSourceRouter().dlt_source(sql_source, "query:SELECT id, v FROM t")
    assert raised.value is error
    assert sql_database.table_rows is original


def _ingest_query_incremental(source, destination, cursor):
    app(
        [
            "ingest",
            "--source-uri",
            f"duckdb:///{source}",
            "--source-table",
            "query:SELECT * FROM t",
            "--dest-uri",
            f"duckdb:///{destination}",
            "--dest-table",
            "out.q",
            "--incremental-key",
            cursor,
            "--incremental-strategy",
            "append",
            "--progress",
            "log",
        ],
        standalone_mode=False,
    )


@pytest.mark.parametrize(
    "cursor",
    ["amount", "ratio", "code", "id"],
    ids=["decimal", "double", "varchar", "bigint"],
)
def test_query_incremental_resumes_from_stored_cursor(tmp_path, cursor):
    """The second run restores the cursor value the first run stored, whatever its
    Python type, and loads only the rows past it."""
    source = tmp_path / "source.duckdb"
    destination = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "CREATE TABLE t (id BIGINT, amount DECIMAL(10, 2), ratio DOUBLE, "
            "code VARCHAR)"
        )

    def add_rows(ids):
        with duckdb.connect(str(source)) as connection:
            for i in ids:
                connection.execute(
                    "INSERT INTO t VALUES (?, ?, ?, ?)",
                    [i, i * 1.5, i * 0.5, f"k{i:03d}"],
                )

    add_rows([1, 2, 3])
    _ingest_query_incremental(source, destination, cursor)
    add_rows([4, 5])
    _ingest_query_incremental(source, destination, cursor)

    with duckdb.connect(str(destination)) as connection:
        assert connection.sql("SELECT id FROM out.q ORDER BY id").fetchall() == [
            (1,),
            (2,),
            (3,),
            (4,),
            (5,),
        ]


def test_query_incremental_unsupported_cursor_type_names_it(tmp_path):
    source = tmp_path / "source.duckdb"
    destination = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(source)) as connection:
        connection.execute("CREATE TABLE t AS SELECT 1 AS id, true AS flag")
    _ingest_query_incremental(source, destination, "flag")

    with pytest.raises(PipelineStepFailed) as raised:
        _ingest_query_incremental(source, destination, "flag")
    cause = raised.value.__cause__
    while cause is not None and not isinstance(cause, ValidationError):
        cause = cause.__cause__
    assert isinstance(cause, ValidationError)
    assert "cursor column 'flag' has type bool" in str(cause)
