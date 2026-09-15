"""The ``adbcbridge`` SQL backend: option plumbing and ODBC string derivation.

Docker-free. The loader itself is exercised against real databases in
``tests/warehouse/db/test_adbcbridge.py``.
"""

import pytest
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
