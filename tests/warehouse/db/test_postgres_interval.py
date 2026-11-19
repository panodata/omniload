"""PostgreSQL `interval` columns load as interval text, on both SQL backends."""

from datetime import timedelta

import duckdb
import pytest
import sqlalchemy

from tests.util import invoke_ingest_command
from tests.util.common import get_random_string
from tests.warehouse.manager import registry

# Source literal, the text the load writes. psycopg2 counts a year as 365 days, a
# month as 30 and a day as 24 hours before omniload sees the value, so calendar
# units arrive folded.
ROWS = [
    (1, "1 day 02:03:04.5", "1 day 02:03:04.5"),
    (2, "-3 hours", "-03:00:00"),
    (3, "-1 day -3 hours", "-1 days -03:00:00"),
    (4, "1 year 2 mons 3 days", "428 days"),
    (5, "0.000001 seconds", "00:00:00.000001"),
    (6, None, None),
    (7, "1 day -1 hour", "23:00:00"),
]


@pytest.fixture
def interval_table():
    assert registry.postgresql is not None
    source_uri = registry.postgresql.start()
    schema = f"testschema_interval_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema}.input (id INTEGER PRIMARY KEY, d INTERVAL)"
        )
        for row_id, literal, _ in ROWS:
            conn.exec_driver_sql(
                f"INSERT INTO {schema}.input VALUES (%s, CAST(%s AS INTERVAL))",
                (row_id, literal),
            )
    yield engine, source_uri, schema
    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA {schema} CASCADE")
    engine.dispose()


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_postgres_interval_loads_as_text(interval_table, tmp_path, backend):
    engine, source_uri, schema = interval_table
    dest = tmp_path / "warehouse.duckdb"

    result = invoke_ingest_command(
        source_uri,
        f"{schema}.input",
        f"duckdb:///{dest}",
        "main.output",
        sql_backend=backend,
        print_output=False,
    )

    assert result.exit_code == 0, result.output
    with duckdb.connect(str(dest), read_only=True) as connection:
        loaded = connection.execute(
            "SELECT id, d, CAST(d AS INTERVAL) FROM main.output ORDER BY id"
        ).fetchall()
    with engine.connect() as conn:
        source = dict(
            conn.exec_driver_sql(f"SELECT id, d FROM {schema}.input").fetchall()
        )

    assert [(row_id, text) for row_id, text, _ in loaded] == [
        (row_id, text) for row_id, _, text in ROWS
    ]
    # DuckDB reads each string back as the value psycopg2 handed over.
    assert {row_id: value for row_id, _, value in loaded} == source


def test_postgres_reads_the_text_back_as_it_writes_it(interval_table):
    engine, _, _ = interval_table
    texts = [text for _, _, text in ROWS if text is not None]
    with engine.connect() as conn:
        conn.exec_driver_sql("SET intervalstyle = 'postgres'")
        for text in texts:
            echoed = conn.exec_driver_sql(
                "SELECT CAST(CAST(%s AS INTERVAL) AS TEXT)", (text,)
            ).scalar()
            assert echoed == text
        # Nanosecond digits round to PostgreSQL's microseconds, carrying if need be.
        rounded = conn.exec_driver_sql(
            "SELECT CAST('00:00:00.0000015' AS INTERVAL), "
            "CAST('-1 days -23:59:59.9999995' AS INTERVAL)"
        ).one()
    assert tuple(rounded) == (timedelta(microseconds=2), timedelta(days=-2))
