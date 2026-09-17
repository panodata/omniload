"""The ``adbcbridge`` SQL backend against real databases.

Needs an ODBC driver manager and psqlodbc (``odbc`` marker), so it runs in the
Docker matrix lane only where those are installed. The PostgreSQL driver is
found by its registered name ``PostgreSQL Unicode``, or by the path in
``OMNILOAD_ODBC_DRIVER_POSTGRESQL``.
"""

import os
import shutil
import subprocess

import pytest
import sqlalchemy

from omniload import run_ingest
from tests.util import invoke_ingest_command
from tests.util.common import get_random_string
from tests.util.db import get_query_result
from tests.warehouse.manager import registry

CRATEDB_IMAGE = "docker.io/crate/crate:5.10.11"

pytestmark = pytest.mark.odbc


def _registered_odbc_drivers() -> set[str]:
    odbcinst = shutil.which("odbcinst")
    if not odbcinst:
        return set()
    out = subprocess.run(  # noqa: S603
        [odbcinst, "-q", "-d"], capture_output=True, text=True, check=False
    ).stdout
    return {line.strip("[]") for line in out.splitlines() if line.startswith("[")}


def _psqlodbc_available() -> bool:
    return bool(os.environ.get("OMNILOAD_ODBC_DRIVER_POSTGRESQL")) or (
        "PostgreSQL Unicode" in _registered_odbc_drivers()
    )


def _msodbcsql_available() -> bool:
    return bool(os.environ.get("OMNILOAD_ODBC_DRIVER_MSSQL")) or (
        "ODBC Driver 18 for SQL Server" in _registered_odbc_drivers()
    )


needs_psqlodbc = pytest.mark.skipif(
    not _psqlodbc_available(), reason="psqlodbc is not installed"
)


def _postgresql_uri() -> str:
    assert registry.postgresql is not None
    return registry.postgresql.start()


def _duckdb_uri() -> str:
    assert registry.duckdb_destination is not None
    return registry.duckdb_destination.start()


def _sample_table(engine, schema):
    """A table with the column types the backend has to get right."""
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
        conn.exec_driver_sql(
            f"""
            CREATE TABLE {schema}.input (
                id INTEGER PRIMARY KEY,
                val DOUBLE PRECISION,
                name VARCHAR(40),
                amount NUMERIC(10, 2),
                flag BOOLEAN,
                day DATE,
                seen TIMESTAMP(0),
                seen_tz TIMESTAMP(3) WITH TIME ZONE,
                updated_at TIMESTAMP
            )"""
        )
        conn.exec_driver_sql(
            f"""
            INSERT INTO {schema}.input VALUES
              (1, 1.5, 'héllo wörld', 12.34, true, '2024-02-29', '2024-02-29 13:45:10',
               '2024-02-29 15:45:10.123+02:00', '2022-01-01 00:00:00'),
              (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2022-01-02 00:00:00'),
              (3, 3.25, 'plain', 0.01, false, '1999-12-31', '1999-12-31 23:59:59',
               '1999-12-31 23:59:59.999+00', '2022-01-03 00:00:00')
            """
        )


def _rows(uri, query):
    return get_query_result(uri, query)


@needs_psqlodbc
@pytest.mark.parametrize("dest", [registry.duckdb_destination], ids=["duckdb"])
def test_postgresql_reads_the_same_rows_as_the_pyarrow_backend(dest, tmp_path):
    """Differential: the ODBC route yields what the SQLAlchemy/Arrow route yields."""
    source_uri = _postgresql_uri()
    dest_uri = dest.start()
    schema = f"adbcbridge_diff_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    _sample_table(engine, schema)
    # The adbcBridge pin (0.1.3) exists because psqlodbc rendered `timestamptz`
    # in the session's zone and an earlier release took that as UTC.  Give every
    # new session a non-UTC default so that regression is what this test sees;
    # the `+02:00` instant in the sample row is 13:45:10.123 UTC either way.
    dbname = sqlalchemy.engine.make_url(source_uri).database
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"ALTER DATABASE \"{dbname}\" SET timezone TO 'America/New_York'"
        )
    engine.dispose()
    try:
        _differential(source_uri, dest_uri, schema, tmp_path)
    finally:
        engine = sqlalchemy.create_engine(source_uri)
        with engine.begin() as conn:
            conn.exec_driver_sql(f'ALTER DATABASE "{dbname}" RESET timezone')
        engine.dispose()


def _differential(source_uri, dest_uri, schema, tmp_path):
    for backend in ("pyarrow", "adbcbridge"):
        run_ingest(
            source_uri=source_uri,
            source_table=f"{schema}.input",
            dest_uri=dest_uri,
            dest_table=f"{schema}.out_{backend}",
            sql_backend=backend,
            progress="log",
            pipelines_dir=str(tmp_path / backend),
        )

    query = "select id, val, name, amount, flag, day, seen, seen_tz, updated_at from {} order by id"
    arrow_rows = _rows(dest_uri, query.format(f"{schema}.out_pyarrow"))
    odbc_rows = _rows(dest_uri, query.format(f"{schema}.out_adbcbridge"))
    assert len(odbc_rows) == 3
    assert odbc_rows == arrow_rows
    assert odbc_rows[0][2] == "héllo wörld"
    # The instant, not a session-zone wall clock: 15:45:10.123+02:00 is
    # 13:45:10.123Z.  Read as epoch milliseconds so the check does not depend on
    # the zone DuckDB renders `timestamptz` in either.
    assert _rows(
        dest_uri, f"select epoch_ms(seen_tz) from {schema}.out_adbcbridge where id = 1"
    ) == [(1709214310123,)]


@needs_psqlodbc
def test_postgresql_incremental_append_through_the_cli(tmp_path):
    source_uri = _postgresql_uri()
    dest_uri = _duckdb_uri()
    schema = f"adbcbridge_append_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    _sample_table(engine, schema)

    def run():
        res = invoke_ingest_command(
            source_uri,
            f"{schema}.input",
            dest_uri,
            f"{schema}.output",
            "append",
            "updated_at",
            sql_backend="adbcbridge",
        )
        assert res.exit_code == 0, res.stderr

    def output():
        return _rows(dest_uri, f"select id, name from {schema}.output order by id")

    run()
    assert [r[0] for r in output()] == [1, 2, 3]

    # A second run with nothing new appends nothing.
    run()
    assert [r[0] for r in output()] == [1, 2, 3]

    # A row past the cursor arrives; an older one does not.
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"INSERT INTO {schema}.input VALUES"
            " (4, 4.0, 'four', 4.00, true, '2024-01-04', '2024-01-04 00:00:00',"
            " '2024-01-04 00:00:00+00', '2022-01-04 00:00:00'),"
            " (0, 0.0, 'late', 0.00, false, '2021-01-01', '2021-01-01 00:00:00',"
            " '2021-01-01 00:00:00+00', '2021-12-31 00:00:00')"
        )
    engine.dispose()
    run()
    assert [r[0] for r in output()] == [1, 2, 3, 4]


@needs_psqlodbc
def test_postgresql_merge_with_primary_key(tmp_path):
    source_uri = _postgresql_uri()
    dest_uri = _duckdb_uri()
    schema = f"adbcbridge_merge_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    _sample_table(engine, schema)

    def run():
        run_ingest(
            source_uri=source_uri,
            source_table=f"{schema}.input",
            dest_uri=dest_uri,
            dest_table=f"{schema}.output",
            incremental_strategy="merge",
            incremental_key="updated_at",
            primary_key=["id"],
            sql_backend="adbcbridge",
            progress="log",
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    run()
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"UPDATE {schema}.input SET name = 'renamed', updated_at = '2022-02-01' WHERE id = 3"
        )
    engine.dispose()
    run()
    rows = _rows(dest_uri, f"select id, name from {schema}.output order by id")
    assert rows == [(1, "héllo wörld"), (2, None), (3, "renamed")]


@needs_psqlodbc
def test_postgresql_small_pages_and_a_limit(tmp_path):
    """page_size drives the Arrow batch size; sql_limit reaches the compiled query."""
    source_uri = _postgresql_uri()
    dest_uri = _duckdb_uri()
    schema = f"adbcbridge_pages_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema}.input AS SELECT i AS id, md5(i::text) AS name"
            " FROM generate_series(1, 1000) i"
        )
    engine.dispose()

    run_ingest(
        source_uri=source_uri,
        source_table=f"{schema}.input",
        dest_uri=dest_uri,
        dest_table=f"{schema}.paged",
        sql_backend="adbcbridge",
        page_size=7,
        progress="log",
        pipelines_dir=str(tmp_path / "paged"),
    )
    assert _rows(dest_uri, f"select count(*), max(id) from {schema}.paged") == [
        (1000, 1000)
    ]

    run_ingest(
        source_uri=source_uri,
        source_table=f"{schema}.input",
        dest_uri=dest_uri,
        dest_table=f"{schema}.limited",
        sql_backend="adbcbridge",
        sql_limit=25,
        progress="log",
        pipelines_dir=str(tmp_path / "limited"),
    )
    assert _rows(dest_uri, f"select count(*) from {schema}.limited") == [(25,)]


def test_postgresql_custom_query_still_uses_sqlalchemy():
    """A `query:` source is row-oriented by design, whatever backend is named.

    Run in a subprocess: the `query:` path installs its synthetic `table_rows`
    into dlt's module for the life of the process (see `SqlSourceRouter`), and
    a later plain-table load in the same interpreter would pick it up.
    """
    source_uri = _postgresql_uri()
    dest_uri = _duckdb_uri()
    schema = f"adbcbridge_query_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    _sample_table(engine, schema)
    engine.dispose()
    res = invoke_ingest_command(
        source_uri,
        f"query:select id, name from {schema}.input where id > 1",
        dest_uri,
        f"{schema}.q",
        sql_backend="adbcbridge",
        run_in_subprocess=True,
    )
    assert res.exit_code == 0, res.stderr
    assert _rows(dest_uri, f"select id from {schema}.q order by id") == [(2,), (3,)]


@pytest.fixture(scope="module")
def cratedb():
    """CrateDB with both its HTTP (4200) and PostgreSQL wire (5432) ports mapped."""
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    container = (
        DockerContainer(CRATEDB_IMAGE)
        .with_exposed_ports(4200, 5432)
        .with_command("-Cdiscovery.type=single-node")
    )
    with container:
        wait_for_logs(container, "started", timeout=120)
        host = container.get_container_host_ip()
        yield {
            "http": f"crate://crate@{host}:{container.get_exposed_port(4200)}/?ssl=false",
            "pg_port": container.get_exposed_port(5432),
            "host": host,
        }


@needs_psqlodbc
def test_cratedb_over_the_postgresql_wire(cratedb, monkeypatch, tmp_path):
    """CrateDB: the driver's ODBC route reads where the native ADBC path stops."""
    dest_uri = _duckdb_uri()
    engine = sqlalchemy.create_engine(cratedb["http"])
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TABLE doc.adbcbridge_input (id INTEGER PRIMARY KEY, val DOUBLE,"
            " name TEXT, updated_at TIMESTAMP)"
        )
        conn.exec_driver_sql(
            "INSERT INTO doc.adbcbridge_input VALUES (1, 1.5, 'héllo wörld', '2022-01-01'),"
            " (2, NULL, NULL, '2022-01-02'), (3, 3.25, 'plain', '2022-01-03')"
        )
        conn.exec_driver_sql("REFRESH TABLE doc.adbcbridge_input")
    engine.dispose()

    driver = os.environ.get("OMNILOAD_ODBC_DRIVER_POSTGRESQL", "PostgreSQL Unicode")
    odbc_uri = (
        f"Driver={{{driver}}};Server={cratedb['host']};Port={cratedb['pg_port']};"
        "Database=doc;Uid=crate;"
    )
    # The container maps the wire-protocol port somewhere random, so the ODBC
    # string is given explicitly, as the docs show for a non-default port.
    monkeypatch.setenv("OMNILOAD_SQL_ODBC_URI", odbc_uri)
    res = invoke_ingest_command(
        cratedb["http"],
        "doc.adbcbridge_input",
        dest_uri,
        "crate.output",
        "append",
        "updated_at",
        sql_backend="adbcbridge",
    )
    assert res.exit_code == 0, res.stderr
    rows = _rows(dest_uri, "select id, val, name from crate.output order by id")
    assert rows == [(1, 1.5, "héllo wörld"), (2, None, None), (3, 3.25, "plain")]


@pytest.mark.skipif(
    registry.mssql is None or not _msodbcsql_available(),
    reason="SQL Server container or its ODBC driver not available",
)
def test_mssql_reads_the_same_rows_as_the_sqlalchemy_backend(tmp_path):
    assert registry.mssql is not None
    source_uri = registry.mssql.start()
    dest_uri = _duckdb_uri()
    schema = f"adbcbridge_{get_random_string(5)}"
    engine = sqlalchemy.create_engine(source_uri)
    with engine.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
        conn.exec_driver_sql(
            f"CREATE TABLE {schema}.input (id INT PRIMARY KEY, val FLOAT, name NVARCHAR(40),"
            " amount DECIMAL(10, 2), flag BIT, day DATE, seen DATETIME2(3), updated_at DATETIME2)"
        )
        conn.exec_driver_sql(
            f"INSERT INTO {schema}.input VALUES"
            " (1, 1.5, N'héllo wörld', 12.34, 1, '2024-02-29', '2024-02-29 13:45:10.123', '2022-01-01'),"
            " (2, NULL, NULL, NULL, NULL, NULL, NULL, '2022-01-02'),"
            " (3, 3.25, N'plain', 0.01, 0, '1999-12-31', '1999-12-31 23:59:59.999', '2022-01-03')"
        )
    engine.dispose()
    for backend in ("sqlalchemy", "adbcbridge"):
        run_ingest(
            source_uri=source_uri,
            source_table=f"{schema}.input",
            dest_uri=dest_uri,
            dest_table=f"{schema}.out_{backend}",
            sql_backend=backend,
            progress="log",
            pipelines_dir=str(tmp_path / backend),
        )
    query = (
        "select id, val, name, amount, flag, day, seen, updated_at from {} order by id"
    )
    assert _rows(dest_uri, query.format(f"{schema}.out_adbcbridge")) == _rows(
        dest_uri, query.format(f"{schema}.out_sqlalchemy")
    )
