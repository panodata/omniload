"""The ``adbcbridge`` SQL backend: option plumbing and ODBC string derivation.

Docker-free. The loader is exercised here with ``adbcbridge`` stubbed, and against
real databases in ``tests/warehouse/db/test_adbcbridge.py``.
"""

import sys
import types

import pyarrow as pa
import pytest
import sqlalchemy
from dlt.common.schema.typing import TColumnSchema
from dlt.sources.sql_database.helpers import get_table_loader_class

from omniload import ValidationError, run_ingest
from omniload.core.router import SqlSourceRouter
from omniload.model import SqlBackend
from omniload.source.sql_database.adbcbridge import (
    BACKEND_NAME,
    AdbcBridgeTableLoader,
    odbc_uri_from_url,
    register,
)


def test_backend_is_a_cli_choice():
    assert SqlBackend("adbcbridge") is SqlBackend.adbcbridge


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        (
            "postgresql://user:secret@db.example.org:5433/shop?sslmode=require",
            "Driver={PostgreSQL Unicode};Server=db.example.org;Port=5433;"
            "Database=shop;Uid=user;SSLmode=require;Pwd=secret;",
        ),
        (
            "postgres://user@localhost/shop",
            "Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=shop;Uid=user;",
        ),
        # CrateDB URIs name the HTTP port; the ODBC route is the PostgreSQL wire
        # protocol on 5432.
        (
            "crate://crate@localhost:4200/",
            "Driver={PostgreSQL Unicode};Server=localhost;Port=5432;Database=doc;Uid=crate;",
        ),
        (
            "crate://admin:pw@cluster.example.net:15432/",
            "Driver={PostgreSQL Unicode};Server=cluster.example.net;Port=15432;"
            "Database=doc;Uid=admin;Pwd=pw;",
        ),
        (
            "mssql://sa:pw@sql.example.org:1433/master"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
            "Driver={ODBC Driver 18 for SQL Server};Server=sql.example.org,1433;"
            "Database=master;Uid=sa;Pwd=pw;TrustServerCertificate=yes;",
        ),
        (
            "mysql+pymysql://root:pw@localhost:3307/shop",
            "Driver={MySQL ODBC 9.4 Unicode Driver};Server=localhost;Port=3307;"
            "Database=shop;User=root;Password=pw;",
        ),
    ],
)
def test_odbc_uri_is_derived_from_the_source_uri(uri, expected):
    assert odbc_uri_from_url(uri) == expected


def test_driver_name_can_be_overridden_per_family(monkeypatch):
    monkeypatch.setenv("OMNILOAD_ODBC_DRIVER_POSTGRESQL", "/opt/odbc/psqlodbcw.so")
    assert odbc_uri_from_url("postgresql://u@h/d").startswith(
        "Driver={/opt/odbc/psqlodbcw.so};"
    )


def test_a_value_carrying_the_separator_is_braced():
    uri = odbc_uri_from_url("postgresql://u:p%3Bq@h/d")
    assert "Pwd={p;q};" in uri


def test_an_unknown_scheme_asks_for_an_explicit_odbc_uri():
    with pytest.raises(ValidationError, match="--sql-odbc-uri"):
        odbc_uri_from_url("sqlite:///some.db")


def test_register_is_idempotent_and_reaches_dlt():
    register()
    register()
    assert get_table_loader_class(BACKEND_NAME) is AdbcBridgeTableLoader


def test_router_registers_the_backend_and_forwards_the_odbc_uri():
    seen = {}

    def sql_table(**kwargs):
        seen.update(kwargs)

        class _Res:
            max_table_nesting = None

        return _Res()

    router = SqlSourceRouter(table_builder=sql_table)
    router.dlt_source(
        uri="postgresql://u:p@localhost/db",
        table="public.t",
        sql_backend="adbcbridge",
        sql_odbc_uri="Driver={x};Server=h;",
    )
    assert seen["backend"] == "adbcbridge"
    assert seen["backend_kwargs"] == {"odbc_uri": "Driver={x};Server=h;"}
    assert get_table_loader_class(BACKEND_NAME) is AdbcBridgeTableLoader


def test_router_leaves_backend_kwargs_alone_without_an_override():
    seen = {}

    def sql_table(**kwargs):
        seen.update(kwargs)

        class _Res:
            max_table_nesting = None

        return _Res()

    SqlSourceRouter(table_builder=sql_table).dlt_source(
        uri="postgresql://u:p@localhost/db", table="public.t", sql_backend="adbcbridge"
    )
    assert seen["backend_kwargs"] is None


def test_scd2_rejects_the_backend_like_the_other_arrow_backends(tmp_path):
    with pytest.raises(ValidationError, match="adbcbridge"):
        run_ingest(
            source_uri=f"sqlite:///{tmp_path}/src.db",
            dest_uri=f"duckdb:///{tmp_path}/dest.duckdb",
            source_table="main.t",
            dest_table="main.t",
            incremental_strategy="scd2",
            primary_key=["id"],
            sql_backend="adbcbridge",
            pipelines_dir=str(tmp_path / "pipelines"),
        )


# --- the loader itself, with adbcbridge stubbed -----------------------------


class _FakeCursor:
    def __init__(self, batches):
        self._batches = batches

    def execute(self, query):
        self.query = query

    def fetch_record_batch(self):
        yield from self._batches

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConnection:
    def __init__(self, batches):
        self._batches = batches

    def cursor(self):
        return _FakeCursor(self._batches)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _loader(engine, chunk_size):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table("t", metadata, sqlalchemy.Column("id", sqlalchemy.Integer))
    columns: dict[str, TColumnSchema] = {"id": {"name": "id", "data_type": "bigint"}}
    return AdbcBridgeTableLoader(
        engine, "sqlalchemy", table, columns, chunk_size=chunk_size
    )


def _stub_adbcbridge(monkeypatch, connect):
    module = types.ModuleType("adbcbridge")
    module.__dict__["connect"] = connect
    monkeypatch.setitem(sys.modules, "adbcbridge", module)


def test_page_size_reaches_the_driver_as_the_batch_size(monkeypatch):
    """`--page-size` is the Arrow batch size adbcBridge is asked for, and the
    loader hands every batch through unchanged."""
    seen = {}

    def connect(**kwargs):
        seen.update(kwargs)
        size = kwargs["batch_size"]
        rows = 1000
        return _FakeConnection(
            [
                pa.record_batch({"id": pa.array(range(start, min(start + size, rows)))})
                for start in range(0, rows, size)
            ]
        )

    _stub_adbcbridge(monkeypatch, connect)
    engine = sqlalchemy.create_engine("sqlite://")
    batches = list(_loader(engine, chunk_size=7).load_rows({"odbc_uri": "Driver={x};"}))

    assert seen["batch_size"] == 7
    assert seen["uri"] == "Driver={x};"
    assert len(batches) == 143  # 1000 rows in batches of 7
    assert {b.num_rows for b in batches[:-1]} == {7}
    assert batches[-1].num_rows == 1000 - 142 * 7
    assert sum(b.num_rows for b in batches) == 1000


def test_a_missing_odbc_driver_names_the_override_knobs(monkeypatch):
    """The most likely first-run failure gets a pointer, not a raw driver error."""

    def connect(**kwargs):
        raise RuntimeError(
            "[unixODBC][Driver Manager]Data source name not found and no default "
            "driver specified (IM002)"
        )

    _stub_adbcbridge(monkeypatch, connect)
    engine = sqlalchemy.create_engine("postgresql://u:p@localhost/db")
    with pytest.raises(
        ValidationError, match="OMNILOAD_ODBC_DRIVER_POSTGRESQL"
    ) as info:
        list(_loader(engine, chunk_size=100).load_rows())
    assert "--sql-odbc-uri" in str(info.value)
    assert "IM002" in str(info.value)


def test_other_connection_errors_pass_through_unchanged(monkeypatch):
    def connect(**kwargs):
        raise RuntimeError("FATAL: password authentication failed")

    _stub_adbcbridge(monkeypatch, connect)
    engine = sqlalchemy.create_engine("postgresql://u:p@localhost/db")
    with pytest.raises(RuntimeError, match="password authentication"):
        list(_loader(engine, chunk_size=100).load_rows())
