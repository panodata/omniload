import sqlite3

import duckdb
import pytest
from dlt.common.pipeline import LoadInfo
from sqlalchemy.exc import NoSuchTableError

from omniload import ValidationError, run_ingest


def _make_sqlite_source(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE widgets (id INTEGER, name TEXT)")
    conn.executemany(
        "INSERT INTO widgets VALUES (?, ?)",
        [(1, "alpha"), (2, "beta"), (3, "gamma")],
    )
    conn.commit()
    conn.close()


def test_run_ingest_sqlite_to_duckdb(tmp_path):
    """The documented Python entry point loads rows and returns a LoadInfo."""
    src = tmp_path / "source.db"
    # DuckDB's catalog is the db file stem; keep it distinct from the "out"
    # schema/dataset name to avoid an ambiguous-reference binder error.
    dest = tmp_path / "warehouse.duckdb"
    _make_sqlite_source(str(src))

    info = run_ingest(
        source_uri=f"sqlite:///{src}",
        dest_uri=f"duckdb:///{dest}",
        source_table="main.widgets",
        dest_table="out.widgets",
        progress="log",
    )

    assert isinstance(info, LoadInfo)

    con = duckdb.connect(str(dest))
    rows = con.sql("select id, name from out.widgets order by id asc").fetchall()
    con.close()
    assert rows == [(1, "alpha"), (2, "beta"), (3, "gamma")]


def test_run_ingest_accepts_string_enums(tmp_path):
    """Enum parameters accept their CLI string value (here, 'merge')."""
    src = tmp_path / "source.db"
    # DuckDB's catalog is the db file stem; keep it distinct from the "out"
    # schema/dataset name to avoid an ambiguous-reference binder error.
    dest = tmp_path / "warehouse.duckdb"
    _make_sqlite_source(str(src))

    info = run_ingest(
        source_uri=f"sqlite:///{src}",
        dest_uri=f"duckdb:///{dest}",
        source_table="main.widgets",
        dest_table="out.widgets",
        incremental_strategy="merge",
        primary_key=["id"],
        progress="log",
    )

    assert isinstance(info, LoadInfo)

    con = duckdb.connect(str(dest))
    count = con.sql("select count(*) from out.widgets").fetchall()[0][0]
    con.close()
    assert count == 3


def test_run_ingest_dry_run_returns_none_and_writes_nothing(tmp_path):
    """dry_run short-circuits before the load: returns None, writes no table."""
    src = tmp_path / "source.db"
    # DuckDB's catalog is the db file stem; keep it distinct from the "out"
    # schema/dataset name to avoid an ambiguous-reference binder error.
    dest = tmp_path / "warehouse.duckdb"
    _make_sqlite_source(str(src))

    result = run_ingest(
        source_uri=f"sqlite:///{src}",
        dest_uri=f"duckdb:///{dest}",
        source_table="main.widgets",
        dest_table="out.widgets",
        dry_run=True,
    )

    assert result is None

    con = duckdb.connect(str(dest))
    tables = con.execute(
        "select table_name from information_schema.tables where table_schema = 'out'"
    ).fetchall()
    con.close()
    assert tables == []


def test_run_ingest_invalid_source_table_raises_validation_error(tmp_path):
    """A library exception (not a typer abort) surfaces on a bad table spec."""
    # DuckDB's catalog is the db file stem; keep it distinct from the "out"
    # schema/dataset name to avoid an ambiguous-reference binder error.
    dest = tmp_path / "warehouse.duckdb"
    with pytest.raises(ValidationError):
        run_ingest(
            source_uri="sqlite:///does-not-matter.db",
            dest_uri=f"duckdb:///{dest}",
            source_table="widgets",  # missing schema, and no dest_table given
        )


def test_run_ingest_without_tables_source_table_does_not_exist(tmp_path):
    """For streaming pipeline elements, should support invocation without table option."""
    # As a consequence, expect an SQLAlchemy `NoSuchTableError`.
    dest = tmp_path / "warehouse.duckdb"
    with pytest.raises(NoSuchTableError):
        run_ingest(
            source_uri="sqlite:///does-not-matter.db",
            dest_uri=f"duckdb://{dest}",
        )


def test_run_ingest_without_tables_invalid_destination_table(tmp_path):
    """When invoking without destination table, fail on destinations that need it."""
    dest = tmp_path / "warehouse.duckdb"
    with pytest.raises(ValueError) as excinfo:
        run_ingest(
            source_uri="csv://omniload/testdata/create_replace.csv",
            dest_uri=f"duckdb://{dest}",
        )
    assert excinfo.match("Table name must be in the format <schema>.<table>")
