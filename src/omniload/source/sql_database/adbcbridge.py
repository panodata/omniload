"""SQL source backend that reads through adbcBridge (ADBC over ODBC).

Registered with dlt as the ``adbcbridge`` table-loader backend, next to
``sqlalchemy``, ``pyarrow`` and ``connectorx``. SQLAlchemy still reflects the
table and compiles the query, incremental cursor and all; adbcBridge then runs
the compiled SQL through the database's ODBC driver and yields Arrow record
batches, so ``append`` and ``merge`` loads keep their usual state handling.

The ODBC connection string is derived from the source URI for the database
families whose ODBC drivers omniload's container image carries (PostgreSQL,
Microsoft SQL Server), plus CrateDB over its PostgreSQL wire protocol and
MySQL. Any other database, or a non-standard driver registration, is reached
by giving the ODBC connection string explicitly with ``--sql-odbc-uri``.
"""

from __future__ import annotations

import os
import typing as t
from collections.abc import Iterator

from dlt.sources.sql_database.helpers import (
    BaseTableLoader,
    register_table_loader_backend,
)
from sqlalchemy.engine import URL, make_url

from omniload.error import ValidationError

BACKEND_NAME = "adbcbridge"

#: Registered ODBC driver name per database family, overridable with
#: ``OMNILOAD_ODBC_DRIVER_<FAMILY>`` (a registered name or a path to the driver
#: library). The defaults are the names Debian's ``odbc-postgresql`` and
#: Microsoft's ``msodbcsql18`` packages register, which is what the omniload
#: container image installs.
DEFAULT_DRIVERS: dict[str, str] = {
    "postgresql": "PostgreSQL Unicode",
    "cratedb": "PostgreSQL Unicode",
    "mssql": "ODBC Driver 18 for SQL Server",
    "mysql": "MySQL ODBC 9.4 Unicode Driver",
}

#: CrateDB's PostgreSQL wire protocol listens on 5432; ``crate://`` URIs name
#: the HTTP port (4200) instead, so a CrateDB URI with the HTTP port, or no
#: port at all, maps to the wire-protocol port.
CRATEDB_HTTP_PORT = 4200
CRATEDB_PG_PORT = 5432


def _family(url: URL) -> str:
    backend = url.get_backend_name()
    if backend in ("postgresql", "postgres"):
        return "postgresql"
    if backend == "crate":
        return "cratedb"
    if backend == "mssql":
        return "mssql"
    if backend in ("mysql", "mariadb"):
        return "mysql"
    raise ValidationError(
        f"The '{BACKEND_NAME}' SQL backend cannot derive an ODBC connection string "
        f"from a '{backend}://' URI. Pass the ODBC connection string explicitly with "
        "'--sql-odbc-uri' (for example 'Driver=<name or path>;Server=...;')."
    )


def _driver(family: str, url: URL) -> str:
    override = os.environ.get(f"OMNILOAD_ODBC_DRIVER_{family.upper()}")
    if override:
        return override
    if family == "mssql":
        # The SQLAlchemy mssql URI carries the ODBC driver name already
        # (`?driver=ODBC+Driver+18+for+SQL+Server`), as omniload documents.
        driver = url.query.get("driver")
        if driver:
            return driver if isinstance(driver, str) else driver[0]
    return DEFAULT_DRIVERS[family]


def _keyword(key: str, value: t.Any) -> str:
    text = str(value)
    if ";" in text or "}" in text:
        # ODBC's quoting rule for a value carrying the separator.
        text = "{" + text + "}"
    return f"{key}={text};"


def odbc_uri_from_url(url: URL | str) -> str:
    """Derive an ODBC connection string from a SQLAlchemy URL.

    Mirrors the connection strings adbcBridge's compatibility matrix verifies
    for each family: ``Server``/``Port``/``Database``/``Uid``/``Pwd`` for
    psqlodbc (PostgreSQL and CrateDB), ``Server=host,port`` for the SQL Server
    driver, ``User``/``Password`` for MySQL Connector/ODBC. Query parameters of
    an ``mssql://`` URI other than ``driver`` and ``authentication`` are passed
    through (``TrustServerCertificate=yes``, ``Encrypt=no``, ...); a
    PostgreSQL ``sslmode`` becomes psqlodbc's ``SSLmode``.
    """
    url = make_url(url) if isinstance(url, str) else url
    family = _family(url)
    driver = _driver(family, url)
    host = url.host or "localhost"
    parts = ["Driver={" + driver + "};"]
    if family == "mssql":
        port = url.port or 1433
        parts.append(_keyword("Server", f"{host},{port}"))
        if url.database:
            parts.append(_keyword("Database", url.database))
        if url.username:
            parts.append(_keyword("Uid", url.username))
        if url.password:
            parts.append(_keyword("Pwd", url.password))
        for key, value in url.query.items():
            if key.lower() in ("driver", "authentication"):
                continue
            parts.append(_keyword(key, value if isinstance(value, str) else value[0]))
        return "".join(parts)
    if family == "mysql":
        parts.append(_keyword("Server", host))
        parts.append(_keyword("Port", url.port or 3306))
        if url.database:
            parts.append(_keyword("Database", url.database))
        if url.username:
            parts.append(_keyword("User", url.username))
        if url.password:
            parts.append(_keyword("Password", url.password))
        return "".join(parts)
    # psqlodbc: PostgreSQL itself, and CrateDB over the PostgreSQL wire protocol.
    parts.append(_keyword("Server", host))
    if family == "cratedb":
        port = url.port
        if port is None or port == CRATEDB_HTTP_PORT:
            port = CRATEDB_PG_PORT
        parts.append(_keyword("Port", port))
        parts.append(_keyword("Database", url.database or "doc"))
        parts.append(_keyword("Uid", url.username or "crate"))
    else:
        parts.append(_keyword("Port", url.port or 5432))
        if url.database:
            parts.append(_keyword("Database", url.database))
        if url.username:
            parts.append(_keyword("Uid", url.username))
        sslmode = url.query.get("sslmode")
        if sslmode:
            parts.append(
                _keyword("SSLmode", sslmode if isinstance(sslmode, str) else sslmode[0])
            )
    if url.password:
        parts.append(_keyword("Pwd", url.password))
    return "".join(parts)


_registered = False


def register() -> None:
    """Register the ``adbcbridge`` backend with dlt (idempotent).

    Imports adbcbridge lazily, so the optional dependency is only required when
    the backend is actually selected.
    """
    global _registered
    try:
        import adbcbridge  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised without the extra
        raise ValidationError(
            "The 'adbcbridge' SQL backend needs the adbcbridge package and an ODBC "
            "driver manager: pip install 'omniload[adbcbridge]'"
        ) from exc
    if not _registered:
        register_table_loader_backend(BACKEND_NAME, AdbcBridgeTableLoader)
        _registered = True


class AdbcBridgeTableLoader(BaseTableLoader):
    """dlt table loader that executes the compiled query through adbcBridge."""

    def load_rows(
        self, backend_kwargs: dict[str, t.Any] | None = None
    ) -> Iterator[t.Any]:
        import adbcbridge

        kwargs = dict(backend_kwargs or {})
        odbc_uri = kwargs.pop("odbc_uri", None) or odbc_uri_from_url(self.engine.url)
        query = self.compile_query(self.make_query())
        options: dict[str, t.Any] = {}
        if self.chunk_size:
            options["batch_size"] = int(self.chunk_size)
        options.update(kwargs)
        with adbcbridge.connect(uri=odbc_uri, autocommit=True, **options) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                yield from cur.fetch_record_batch()
