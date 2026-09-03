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
            source_uri="file://tests/assets/create_replace.csv",
            dest_uri=f"duckdb://{dest}",
        )
    assert excinfo.match(
        "The 'duckdb' table name must be in the format <schema>.<table>"
    )


def test_dotted_elasticsearch_index_defaults_through_without_component_counting():
    result = run_ingest(
        source_uri="elasticsearch://localhost:9200",
        dest_uri="elasticsearch://localhost:9200",
        source_table="filebeat-2026.03.15",
        dry_run=True,
    )

    assert result is None


@pytest.mark.parametrize(
    "source_uri,source_table",
    [
        pytest.param(
            "gsheets://?credentials_base64=e30=",
            "spreadsheet_id.'Q3.2026'!A1:D5",
            id="sheets-a1-range-with-a-dotted-sheet-name",
        ),
        pytest.param(
            "mongodb://localhost:27017",
            "mydb.audit.2026",
            id="mongodb-dotted-collection",
        ),
        pytest.param(
            "elasticsearch://localhost:9200",
            "filebeat-2026.03.15",
            id="elasticsearch-dated-index",
        ),
    ],
)
def test_defaulted_dest_table_refuses_a_name_the_destination_would_resplit(
    source_uri, source_table, tmp_path
):
    """A source that keeps dots whole cannot default into a destination that splits them.

    Both spellings name two things at the source (a spreadsheet plus an A1 range, a
    database plus a dotted collection). A SQL destination reads three, so defaulting
    would load into a schema and table the source never named.
    """
    dest = tmp_path / "warehouse.duckdb"
    with pytest.raises(ValidationError) as excinfo:
        run_ingest(
            source_uri=source_uri,
            dest_uri=f"duckdb:///{dest}",
            source_table=source_table,
            dry_run=True,
        )

    assert excinfo.match("Cannot default the destination table")
    assert excinfo.match("reads it as 3 components")

    # A destination that retargets its connection from the catalog is the sharper
    # case: without the guard this one opens a different database entirely.
    with pytest.raises(ValidationError) as excinfo:
        run_ingest(
            source_uri=source_uri,
            dest_uri="postgres://user:pw@host/mydb",
            source_table=source_table,
            dry_run=True,
        )

    assert excinfo.match("Cannot default the destination table")


def test_defaulted_dest_table_passes_a_two_component_name_through(tmp_path):
    """The guard is about re-splitting, not about dots: two components default as before."""
    dest = tmp_path / "warehouse.duckdb"
    result = run_ingest(
        source_uri="mongodb://localhost:27017",
        dest_uri=f"duckdb:///{dest}",
        source_table="mydb.events",
        dry_run=True,
    )

    assert result is None


def test_defaulted_dest_table_counts_quoted_components_not_dots(tmp_path):
    """A dot inside a quoted identifier is one component, so the name still defaults."""
    dest = tmp_path / "warehouse.duckdb"
    result = run_ingest(
        source_uri="file://tests/assets/create_replace.csv",
        dest_uri=f"duckdb:///{dest}",
        source_table='public."order.items"',
        dry_run=True,
    )

    assert result is None
