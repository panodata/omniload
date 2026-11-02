"""Exasol to DuckDB round trips against Exasol's ``docker-db`` image.

Opt-in: set ``OMNILOAD_TEST_EXASOL=1``. The image is 12 GB unpacked and must run
privileged, so nothing here builds a container object, or imports the dialect,
without the variable. With it set, a missing ``sqlalchemy-exasol`` or a server that
never answers fails the run instead of skipping it.

The module starts one container and shares it, so run it without xdist::

    OMNILOAD_TEST_EXASOL=1 pytest tests/warehouse/db/test_exasol.py -n0

Under xdist each worker that collects a test would start its own container.
"""

import contextlib
import os
import ssl
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

if not os.environ.get("OMNILOAD_TEST_EXASOL"):
    pytest.skip(
        "OMNILOAD_TEST_EXASOL is not set (needs privileged Docker and the 12 GB "
        "exasol/docker-db image)",
        allow_module_level=True,
    )

# pyexasol 3 sends a usage event on every connection unless this is set, and its
# telemetry client reads the setting once, so it has to precede the first connection.
os.environ.setdefault("EXASOL_TELEMETRY_DISABLE", "1")

import docker  # noqa: E402
import docker.errors  # noqa: E402
import duckdb  # noqa: E402
import pyexasol  # noqa: E402
import sqlalchemy as sa  # noqa: E402
import sqlalchemy_exasol  # noqa: E402, F401
from testcontainers.core.container import DockerContainer  # noqa: E402

import omniload.core.router as router  # noqa: E402
from tests.util import invoke_ingest_command  # noqa: E402
from tests.warehouse.manager import EXASOL_IMAGE  # noqa: E402

PORT = 8563
READY_TIMEOUT = 300
PASSWORD = "exasol"  # noqa: S105 - docker-db default administrator password
READER = "omniload_reader"
READER_PASSWORD = "Reader-Password-1"  # noqa: S105 - fixture value
BACKENDS = ["sqlalchemy", "pyarrow"]
UTC = timezone.utc


def utc(*args):
    return datetime(*args, tzinfo=UTC)


@pytest.fixture(scope="module")
def exasol():
    """Start docker-db once for the module and return its host port."""
    if int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1")) > 1:
        pytest.fail("tests/warehouse/db/test_exasol.py must run with -n0")

    name = f"omniload-test-exasol-{uuid.uuid4().hex[:12]}"
    container = (
        DockerContainer(EXASOL_IMAGE)
        .with_name(name)
        .with_exposed_ports(PORT)
        .with_kwargs(
            privileged=True,
            mem_limit="4g",
            memswap_limit="4g",
            nano_cpus=2_000_000_000,
            stop_signal="SIGTERM",
        )
    )
    try:
        container.start()
        port = int(container.get_exposed_port(PORT))
        deadline = time.monotonic() + READY_TIMEOUT
        while True:
            try:
                connect(port).close()
                break
            except Exception as exc:
                if time.monotonic() > deadline:
                    pytest.fail(f"Exasol did not accept connections: {exc!r}")
                time.sleep(2)
        seed(port)
        yield port
    finally:
        # By name, so a container that was created but failed to start goes too.
        with (
            contextlib.closing(docker.from_env()) as client,
            contextlib.suppress(docker.errors.NotFound),
        ):
            client.containers.get(name).remove(force=True, v=True)


def connect(port, user="sys", password=PASSWORD):
    return pyexasol.connect(
        dsn=f"127.0.0.1:{port}",
        user=user,
        password=password,
        websocket_sslopt={"cert_reqs": ssl.CERT_NONE},
    )


def writer(port):
    """Return a connection whose session runs in UTC, so literals mean UTC."""
    connection = connect(port)
    connection.execute("ALTER SESSION SET TIME_ZONE = 'UTC'")
    return connection


def seed(port):
    with writer(port) as connection:
        connection.execute("CREATE SCHEMA probe")
        connection.execute(
            """
            CREATE TABLE probe.t_types (
                id DECIMAL(18, 0) PRIMARY KEY,
                c_bool BOOLEAN,
                c_char CHAR(5),
                c_varchar VARCHAR(100),
                c_dec92 DECIMAL(9, 2),
                c_dec364 DECIMAL(36, 4),
                c_dec180 DECIMAL(18, 0),
                c_int INTEGER,
                c_bigint BIGINT,
                c_double DOUBLE,
                c_date DATE,
                c_ts TIMESTAMP,
                c_ts9 TIMESTAMP(9),
                c_tsltz TIMESTAMP WITH LOCAL TIME ZONE,
                c_iym INTERVAL YEAR TO MONTH,
                c_geom GEOMETRY,
                c_hash HASHTYPE(16 BYTE)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO probe.t_types VALUES (
                1, TRUE, 'ab', 'héllo ✓', 1234567.89,
                12345678901234567890123456789012.3456, 123456789012345678,
                2147483647, 9223372036854775807, 3.5, DATE '2026-01-31',
                TIMESTAMP '2026-01-31 12:34:56.123456',
                TIMESTAMP '2026-01-31 12:34:56.123456789',
                TIMESTAMP '2026-01-31 12:00:00',
                INTERVAL '5-3' YEAR TO MONTH, 'POINT (1 2)',
                '0123456789abcdef0123456789abcdef'
            )
            """
        )
        connection.execute(
            "CREATE TABLE probe.t_interval (id DECIMAL(18, 0), c_ids INTERVAL DAY TO SECOND)"
        )
        connection.execute(
            "INSERT INTO probe.t_interval VALUES (1, INTERVAL '2 12:30:15.500' DAY TO SECOND)"
        )
        connection.execute(
            'CREATE TABLE probe."MixedCase" (id DECIMAL(18, 0), v VARCHAR(10))'
        )
        connection.execute(
            "INSERT INTO probe.\"MixedCase\" VALUES (1, 'one'), (2, 'two')"
        )
        connection.execute(f'CREATE USER {READER} IDENTIFIED BY "{READER_PASSWORD}"')
        connection.execute(f"GRANT CREATE SESSION TO {READER}")
        connection.execute(f"GRANT SELECT ON SCHEMA probe TO {READER}")


def execute(port, *statements):
    with writer(port) as connection:
        for statement in statements:
            connection.execute(statement)


def source_uri(port, user="sys", password=PASSWORD):
    return (
        f"exa+websocket://{user}:{password}@127.0.0.1:{port}"
        "?SSLCertificate=SSL_VERIFY_NONE"
    )


def ingest(port, table, dest, dest_table, **kwargs):
    result = invoke_ingest_command(
        kwargs.pop("uri", source_uri(port)),
        table,
        f"duckdb:///{dest}",
        dest_table,
        **kwargs,
    )
    assert result.exit_code == 0, result.stdout
    return result


def rows(dest, sql):
    with duckdb.connect(str(dest), read_only=True) as connection:
        return connection.sql(sql).fetchall()


def new_table(port, columns, values):
    """Create a fresh table, so tests that mutate their source stay independent."""
    name = f"probe.t_{uuid.uuid4().hex[:10]}"
    execute(
        port,
        f"CREATE TABLE {name} ({columns})",
        f"INSERT INTO {name} VALUES {values}",
    )
    return name


def without_session_hook(monkeypatch):
    """Read in the server's zone, as omniload did before the UTC session."""
    monkeypatch.setattr(router, "_exasol_engine_adapter", lambda engine: engine)


@pytest.mark.parametrize("backend", BACKENDS)
def test_types_round_trip(exasol, tmp_path, backend):
    dest = tmp_path / "warehouse.duckdb"
    ingest(exasol, "probe.t_types", dest, "out.t_types", sql_backend=backend)

    columns = dict(
        rows(dest, "SELECT column_name, column_type FROM (DESCRIBE out.t_types)")
    )
    assert {k: v for k, v in columns.items() if not k.startswith("_dlt")} == {
        "id": "BIGINT",
        "c_bool": "BOOLEAN",
        "c_char": "VARCHAR",
        "c_varchar": "VARCHAR",
        "c_dec92": "DECIMAL(9,2)",
        "c_dec364": "DECIMAL(36,4)",
        "c_dec180": "BIGINT",
        "c_int": "BIGINT",
        "c_bigint": "DECIMAL(36,0)",
        "c_double": "DOUBLE",
        "c_date": "DATE",
        "c_ts": "TIMESTAMP",
        "c_ts9": "TIMESTAMP",
        "c_tsltz": "TIMESTAMP WITH TIME ZONE",
        "c_iym": "VARCHAR",
        "c_geom": "VARCHAR",
        "c_hash": "VARCHAR",
    }
    assert rows(
        dest,
        "SELECT id, c_bool, c_char, c_varchar, c_dec92, c_dec364, c_dec180, c_int, "
        "c_bigint, c_double, c_date, c_ts, c_ts9, c_tsltz, c_iym, c_geom, c_hash "
        "FROM out.t_types",
    ) == [
        (
            1,
            True,
            "ab   ",
            "héllo ✓",
            Decimal("1234567.89"),
            Decimal("12345678901234567890123456789012.3456"),
            123456789012345678,
            2147483647,
            Decimal("9223372036854775807"),
            3.5,
            datetime(2026, 1, 31).date(),
            # TIMESTAMP keeps milliseconds; TIMESTAMP(9) arrives at microseconds.
            datetime(2026, 1, 31, 12, 34, 56, 123000),
            datetime(2026, 1, 31, 12, 34, 56, 123456),
            utc(2026, 1, 31, 12, 0),
            "+05-03",
            "POINT (1 2)",
            "0123456789abcdef0123456789abcdef",
        )
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_tsltz_instant_depends_on_the_utc_session(
    exasol, tmp_path, backend, monkeypatch
):
    # Reader sessions inherit the database zone, not the writer's session setting,
    # so the control below only means something while that zone is not UTC.
    plain = sa.create_engine(source_uri(exasol))
    try:
        with plain.connect() as connection:
            zone = connection.exec_driver_sql("SELECT SESSIONTIMEZONE").scalar()
    finally:
        plain.dispose()
    assert zone.upper() not in ("UTC", "UNIVERSAL", "GMT", "ETC/UTC")

    dest = tmp_path / "warehouse.duckdb"
    query = "SELECT id, c_tsltz FROM out.t ORDER BY id"
    ingest(exasol, "probe.t_types", dest, "out.t", sql_backend=backend)
    assert rows(dest, query) == [(1, utc(2026, 1, 31, 12, 0))]

    without_session_hook(monkeypatch)
    dest = tmp_path / "unhooked.duckdb"
    ingest(exasol, "probe.t_types", dest, "out.t", sql_backend=backend)
    assert rows(dest, query) != [(1, utc(2026, 1, 31, 12, 0))]
    assert len(rows(dest, query)) == 1


@pytest.mark.parametrize("table", ["probe.t_types", "PROBE.T_TYPES"])
def test_unquoted_identifiers_in_either_case(exasol, tmp_path, table):
    dest = tmp_path / "warehouse.duckdb"
    ingest(exasol, table, dest, "out.t")
    assert rows(dest, "SELECT id, c_varchar FROM out.t") == [(1, "héllo ✓")]


@pytest.mark.parametrize("table", ["probe.MixedCase", 'probe."MixedCase"'])
def test_mixed_case_identifier(exasol, tmp_path, table):
    dest = tmp_path / "warehouse.duckdb"
    ingest(exasol, table, dest, "out.t")
    assert rows(dest, "SELECT id, v FROM out.t ORDER BY id") == [(1, "one"), (2, "two")]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("cursor", ["ts", "tsltz"])
def test_append_loads_only_new_rows(exasol, tmp_path, backend, cursor):
    table = new_table(
        exasol,
        "id DECIMAL(18, 0), ts TIMESTAMP, tsltz TIMESTAMP WITH LOCAL TIME ZONE",
        "(1, '2026-01-31 10:00:00', '2026-01-31 10:00:00'), "
        "(2, '2026-01-31 11:00:00', '2026-01-31 11:00:00')",
    )
    dest = tmp_path / "warehouse.duckdb"
    options = {"inc_strategy": "append", "inc_key": cursor, "sql_backend": backend}

    ingest(exasol, table, dest, "out.t", **options)
    execute(
        exasol,
        f"INSERT INTO {table} VALUES (3, '2026-01-31 12:00:00', '2026-01-31 12:00:00')",
    )
    ingest(exasol, table, dest, "out.t", **options)

    # dlt's closed range re-reads the row at the last cursor value and drops it as a
    # duplicate, so a second copy of any row means the cursor did not resume.
    assert rows(dest, "SELECT id, tsltz FROM out.t ORDER BY id") == [
        (1, utc(2026, 1, 31, 10)),
        (2, utc(2026, 1, 31, 11)),
        (3, utc(2026, 1, 31, 12)),
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_merge_updates_in_place(exasol, tmp_path, backend):
    table = new_table(
        exasol,
        "id DECIMAL(18, 0) PRIMARY KEY, v VARCHAR(10), tsltz TIMESTAMP WITH LOCAL TIME ZONE",
        "(1, 'a', '2026-01-31 10:00:00'), (2, 'b', '2026-01-31 11:00:00')",
    )
    dest = tmp_path / "warehouse.duckdb"
    options = {"inc_strategy": "merge", "primary_key": ["id"], "sql_backend": backend}

    ingest(exasol, table, dest, "out.t", **options)
    execute(
        exasol,
        f"UPDATE {table} SET v = 'b2', tsltz = '2026-01-31 13:00:00' WHERE id = 2",
        f"INSERT INTO {table} VALUES (5, 'e', '2026-01-31 14:00:00')",
    )
    ingest(exasol, table, dest, "out.t", **options)

    assert rows(dest, "SELECT id, v, tsltz FROM out.t ORDER BY id") == [
        (1, "a", utc(2026, 1, 31, 10)),
        (2, "b2", utc(2026, 1, 31, 13)),
        (5, "e", utc(2026, 1, 31, 14)),
    ]


WINDOW_ROWS = (
    "(1, '2026-01-31 11:00:00'), (2, '2026-01-31 12:00:00'), "
    "(3, '2026-01-31 13:00:00'), (4, '2026-01-31 14:00:00')"
)
# 11:30Z to 13:00Z, written at +01:00: rows 2 and 3 only.
WINDOW = {
    "interval_start": "2026-01-31T12:30:00+01:00",
    "interval_end": "2026-01-31T14:00:00+01:00",
}


@pytest.mark.parametrize("backend", BACKENDS)
def test_aware_window_on_a_table(exasol, tmp_path, backend):
    table = new_table(
        exasol, "id DECIMAL(18, 0), tsltz TIMESTAMP WITH LOCAL TIME ZONE", WINDOW_ROWS
    )
    dest = tmp_path / "warehouse.duckdb"
    ingest(exasol, table, dest, "out.t", inc_key="tsltz", sql_backend=backend, **WINDOW)
    assert rows(dest, "SELECT id, tsltz FROM out.t ORDER BY id") == [
        (2, utc(2026, 1, 31, 12)),
        (3, utc(2026, 1, 31, 13)),
    ]


def test_aware_window_on_a_query(exasol, tmp_path):
    table = new_table(
        exasol, "id DECIMAL(18, 0), tsltz TIMESTAMP WITH LOCAL TIME ZONE", WINDOW_ROWS
    )
    dest = tmp_path / "warehouse.duckdb"
    ingest(
        exasol,
        f"query:SELECT id, tsltz FROM {table} "
        "WHERE tsltz >= :interval_start AND tsltz <= :interval_end",
        dest,
        "out.t",
        **WINDOW,
    )
    assert rows(dest, "SELECT id, tsltz FROM out.t ORDER BY id") == [
        (2, utc(2026, 1, 31, 12)),
        (3, utc(2026, 1, 31, 13)),
    ]


@pytest.mark.parametrize("backend", BACKENDS)
def test_aware_start_resumes_from_the_cursor(exasol, tmp_path, backend):
    table = new_table(
        exasol, "id DECIMAL(18, 0), tsltz TIMESTAMP WITH LOCAL TIME ZONE", WINDOW_ROWS
    )
    dest = tmp_path / "warehouse.duckdb"
    options = {
        "inc_strategy": "append",
        "inc_key": "tsltz",
        "sql_backend": backend,
        "interval_start": WINDOW["interval_start"],
    }

    ingest(exasol, table, dest, "out.t", **options)
    execute(
        exasol,
        f"INSERT INTO {table} VALUES "
        "(5, '2026-01-31 10:00:00'), (6, '2026-01-31 15:00:00')",
    )
    ingest(exasol, table, dest, "out.t", **options)

    assert rows(dest, "SELECT id, tsltz FROM out.t ORDER BY id") == [
        (2, utc(2026, 1, 31, 12)),
        (3, utc(2026, 1, 31, 13)),
        (4, utc(2026, 1, 31, 14)),
        (6, utc(2026, 1, 31, 15)),
    ]


def test_interval_day_to_second_workarounds(exasol, tmp_path):
    dest = tmp_path / "warehouse.duckdb"
    ingest(
        exasol,
        "query:SELECT id, CAST(c_ids AS VARCHAR(40)) AS c_ids FROM probe.t_interval",
        dest,
        "out.cast",
    )
    assert rows(dest, "SELECT id, c_ids FROM out.cast") == [(1, "+02 12:30:15.500")]

    ingest(
        exasol, "probe.t_interval", dest, "out.excluded", sql_exclude_columns="c_ids"
    )
    columns = rows(dest, "SELECT column_name FROM (DESCRIBE out.excluded)")
    assert [name for (name,) in columns if not name.startswith("_dlt")] == ["id"]
    assert rows(dest, "SELECT id FROM out.excluded") == [(1,)]


def test_query_predicates_are_evaluated_in_utc(exasol, tmp_path, monkeypatch):
    # Plain TIMESTAMP rows half an hour either side of the current UTC wall time.
    # CURRENT_TIMESTAMP follows the session zone, so the later row falls before it
    # only in a session ahead of UTC.
    table = f"probe.t_{uuid.uuid4().hex[:10]}"
    execute(
        exasol,
        f"CREATE TABLE {table} (id DECIMAL(18, 0), ts TIMESTAMP)",
        f"INSERT INTO {table} SELECT 1, ADD_MINUTES(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), -30)",
        f"INSERT INTO {table} SELECT 2, ADD_MINUTES(CAST(CURRENT_TIMESTAMP AS TIMESTAMP), 30)",
    )
    query = (
        f"query:SELECT id FROM {table} WHERE ts < CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"
    )

    dest = tmp_path / "warehouse.duckdb"
    ingest(exasol, query, dest, "out.t")
    assert rows(dest, "SELECT id FROM out.t ORDER BY id") == [(1,)]

    without_session_hook(monkeypatch)
    dest = tmp_path / "unhooked.duckdb"
    ingest(exasol, query, dest, "out.t")
    assert rows(dest, "SELECT id FROM out.t ORDER BY id") == [(1,), (2,)]


def test_read_only_user(exasol, tmp_path):
    dest = tmp_path / "warehouse.duckdb"
    ingest(
        exasol,
        "probe.t_types",
        dest,
        "out.t",
        uri=source_uri(exasol, READER, READER_PASSWORD),
    )
    assert rows(dest, "SELECT id, c_tsltz FROM out.t") == [(1, utc(2026, 1, 31, 12))]
