"""Exasol routing through the SQL router, with no Exasol server or dialect.

The UTC session listener is exercised on a SQLite engine whose DBAPI connections
record the Exasol session statement instead of running it, so these tests also run
where `sqlalchemy-exasol` is not installed. The round trip against a real server is
``tests/warehouse/db/test_exasol.py``.
"""

import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import dlt
import pytest
import sqlalchemy as sa
from sqlalchemy.pool import QueuePool

from omniload.core.router import SqlSourceRouter, _exasol_utc_session
from omniload.error import ValidationError

EXASOL_URI = "exa+websocket://sys:secret@exasol.example:8563"
# Written out rather than imported, so a change to the statement fails these tests.
UTC_SESSION = "ALTER SESSION SET TIME_ZONE = 'UTC'"
AWARE_START = datetime(2026, 1, 31, 12, 30, tzinfo=timezone(timedelta(hours=1)))
AWARE_END = datetime(2026, 1, 31, 14, 0, tzinfo=timezone(timedelta(hours=1)))


def build(uri, table, **kwargs):
    """Run the router with a capturing table builder and return its arguments."""
    captured: dict = {}

    def table_builder(**builder_kwargs):
        captured.update(builder_kwargs)
        return dlt.resource([], name="captured")

    SqlSourceRouter(table_builder=table_builder).dlt_source(uri, table, **kwargs)
    return captured


class RecordingCursor:
    """Record the Exasol session statement, pass everything else to SQLite."""

    def __init__(self, cursor, log, fail):
        self._cursor = cursor
        self._log = log
        self._fail = fail

    def execute(self, statement, *args):
        if statement == UTC_SESSION:
            self._log.append(statement)
            if self._fail:
                raise sqlite3.OperationalError("insufficient privileges")
            return self
        return self._cursor.execute(statement, *args)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class RecordingConnection:
    def __init__(self, log, fail):
        self._connection = sqlite3.connect(":memory:", check_same_thread=False)
        self._log = log
        self._fail = fail
        self.closed = False

    def cursor(self, *args, **kwargs):
        return RecordingCursor(
            self._connection.cursor(*args, **kwargs), self._log, self._fail
        )

    def close(self):
        self.closed = True
        self._connection.close()

    def __getattr__(self, name):
        return getattr(self._connection, name)


def recording_engine(fail=False):
    """Return a pooled SQLite engine, its statement log and its connections."""
    log: list = []
    connections: list = []

    def creator():
        connection = RecordingConnection(log, fail)
        connections.append(connection)
        return connection

    engine = sa.create_engine(
        "sqlite://", creator=creator, poolclass=QueuePool, pool_size=2
    )
    return engine, log, connections


def exasol_callback():
    callback = build(EXASOL_URI, "probe.t")["engine_adapter_callback"]
    assert callback is not None
    return callback


def test_exasol_table_name_takes_schema_and_table():
    captured = build(EXASOL_URI, "probe.t_types")
    assert (captured["schema"], captured["table"]) == ("probe", "t_types")

    captured = build(EXASOL_URI, 'probe."MixedCase"')
    assert (captured["schema"], captured["table"]) == ("probe", "MixedCase")


@pytest.mark.parametrize("uri", ["exa://sys:secret@host:8563", EXASOL_URI])
def test_exasol_three_part_name_is_refused_with_exasol_hint(uri):
    with pytest.raises(ValidationError) as exc_info:
        build(uri, "db.probe.t")

    message = str(exc_info.value)
    assert "Exasol has no catalogs; use schema.table." in message
    assert "catalog or database in --source-uri" not in message


@pytest.mark.parametrize("uri", ["exa://sys:secret@host:8563", EXASOL_URI])
def test_session_listener_is_attached_for_exasol(uri):
    callback = build(uri, "probe.t")["engine_adapter_callback"]
    engine, log, _ = recording_engine()

    assert callback(engine) is engine
    with engine.connect() as connection:
        assert connection.exec_driver_sql("SELECT 1").scalar() == 1
    assert log == [UTC_SESSION]


@pytest.mark.parametrize(
    "uri, table",
    [
        ("postgresql://user:secret@host/db", "public.t"),
        # The check is on the dialect name, not a prefix of the scheme.
        ("exasolx://user:secret@host:8563", "query:SELECT 1"),
    ],
)
def test_session_listener_is_absent_for_other_schemes(uri, table):
    assert build(uri, table)["engine_adapter_callback"] is None


def test_exasol_engine_turns_off_pyexasol_telemetry(monkeypatch):
    telemetry = pytest.importorskip("exasol.telemetry.client")
    from exasol.telemetry.client import config, setup

    # Start from a process where telemetry has not been set up and nothing in the
    # environment turns it off, so only the engine callback can.
    monkeypatch.setattr(config, "_config", None)
    monkeypatch.delenv(config.ENV_DISABLE, raising=False)
    monkeypatch.delenv(config.ENV_CI, raising=False)
    started: list = []
    monkeypatch.setattr(setup.worker, "start_worker", lambda: started.append(True))
    monkeypatch.setattr(setup, "setup_exit_handlers", lambda: None)

    engine, log, _ = recording_engine()
    assert exasol_callback()(engine) is engine

    # What pyexasol 3 calls on every new connection.
    telemetry.track("PYEXA", "0.0.0", "connect")
    assert not config.was_enabled()
    assert started == []

    # The session listener is still in place.
    with engine.connect():
        pass
    assert log == [UTC_SESSION]


def test_exasol_engine_without_telemetry_client(monkeypatch):
    # pyexasol before 3 ships no telemetry client; the engine is prepared anyway.
    monkeypatch.setitem(sys.modules, "exasol.telemetry", None)
    engine, log, _ = recording_engine()

    assert exasol_callback()(engine) is engine
    with engine.connect():
        pass
    assert log == [UTC_SESSION]


def test_session_statement_runs_once_per_new_connection():
    callback = exasol_callback()
    engine, log, connections = recording_engine()

    # Applying the callback twice must not register a second listener.
    callback(callback(engine))

    with engine.connect():
        pass
    with engine.connect():
        pass
    assert len(connections) == 1
    assert log == [UTC_SESSION]

    with engine.connect(), engine.connect():
        pass
    assert len(connections) == 2
    assert log == [UTC_SESSION] * 2

    engine.dispose()
    with engine.connect():
        pass
    assert len(connections) == 3
    assert log == [UTC_SESSION] * 3


def test_session_listener_stays_on_its_own_engine():
    callback = exasol_callback()
    first, first_log, _ = recording_engine()
    second, second_log, _ = recording_engine()

    callback(first)
    with second.connect():
        pass
    assert second_log == []

    with first.connect():
        pass
    assert first_log == [UTC_SESSION]
    assert second_log == []


def test_failed_session_statement_fails_the_connection():
    callback = exasol_callback()
    engine, log, connections = recording_engine(fail=True)
    callback(engine)

    with pytest.raises(RuntimeError) as exc_info:
        engine.connect()

    message = str(exc_info.value)
    assert UTC_SESSION in message
    assert "insufficient privileges" in message
    assert log == [UTC_SESSION]
    assert [connection.closed for connection in connections] == [True]


def test_failed_session_statement_closes_the_connection_itself():
    # Older SQLAlchemy releases leave a connection whose connect event failed open.
    connection = RecordingConnection([], fail=True)

    with pytest.raises(RuntimeError, match="insufficient privileges"):
        _exasol_utc_session(connection, None)
    assert connection.closed


@pytest.mark.parametrize("backend", ["adbcbridge", "connectorx", "pandas"])
def test_backends_not_known_to_read_through_the_engine_are_refused(backend):
    with pytest.raises(ValidationError, match=f"--sql-backend {backend}"):
        build(EXASOL_URI, "probe.t", sql_backend=backend)


@pytest.mark.parametrize("backend", ["sqlalchemy", "pyarrow"])
def test_backends_reading_through_the_engine_are_accepted(backend):
    assert build(EXASOL_URI, "probe.t", sql_backend=backend)["backend"] == backend


def test_query_sources_read_through_the_engine_whatever_the_backend():
    captured = build(EXASOL_URI, "query:SELECT 1", sql_backend="adbcbridge")
    assert captured["backend"] == "sqlalchemy"


def test_aware_bounds_reach_the_incremental_as_naive_utc():
    captured = build(
        EXASOL_URI,
        "probe.t",
        incremental_key="ts",
        interval_start=AWARE_START,
        interval_end=AWARE_END,
    )

    incremental = captured["incremental"]
    assert incremental.initial_value == datetime(2026, 1, 31, 11, 30)
    assert incremental.initial_value.tzinfo is None
    assert incremental.end_value == datetime(2026, 1, 31, 13, 0)
    assert incremental.end_value.tzinfo is None


def test_aware_bounds_reach_query_parameters_as_naive_utc():
    captured = build(
        EXASOL_URI,
        "query:SELECT * FROM probe.t WHERE ts >= :interval_start "
        "AND ts < :interval_end",
        interval_start=AWARE_START,
        interval_end=AWARE_END,
    )

    clause = captured["query_adapter_callback"](None, None)
    start = clause._bindparams["interval_start"].value
    end = clause._bindparams["interval_end"].value
    assert (start, start.tzinfo) == (datetime(2026, 1, 31, 11, 30), None)
    assert (end, end.tzinfo) == (datetime(2026, 1, 31, 13, 0), None)


def test_naive_bounds_pass_through_unchanged():
    start = datetime(2026, 1, 31, 12, 30)
    captured = build(EXASOL_URI, "probe.t", incremental_key="ts", interval_start=start)
    assert captured["incremental"].initial_value is start


def test_aware_bounds_are_untouched_for_other_sql_sources():
    captured = build(
        "postgresql://user:secret@host/db",
        "public.t",
        incremental_key="ts",
        interval_start=AWARE_START,
    )
    assert captured["incremental"].initial_value is AWARE_START
