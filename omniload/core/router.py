import base64
import json
import os
import sys
import tempfile
from datetime import date, datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Type,
    TypeAlias,
    Union,
)
from urllib.parse import parse_qs, urlencode, urlparse

import pendulum
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.extract import Incremental
from dlt.extract import Incremental as dlt_incremental
from dlt.sources.sql_database import BaseTableLoader

from omniload.core.model import TableDefinition, table_string_to_dataclass


class SqlSourceRouter:
    table_builder: Callable

    def __init__(self, table_builder=None) -> None:
        if table_builder is None:
            from dlt.sources.sql_database import sql_table

            table_builder = sql_table

        self.table_builder = table_builder

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        table_fields = TableDefinition(dataset="custom", table="custom")
        if not table.startswith("query:"):
            if uri.startswith("spanner://"):
                table_fields = TableDefinition(dataset="", table=table)
            else:
                table_fields = table_string_to_dataclass(table)

        incremental = None
        if kwargs.get("incremental_key"):
            start_value = kwargs.get("interval_start")
            end_value = kwargs.get("interval_end")
            incremental = dlt_incremental(
                kwargs.get("incremental_key", ""),
                initial_value=start_value,
                end_value=end_value,
                range_end="closed",
                range_start="closed",
            )

        engine_adapter_callback = None

        if uri.startswith("md://") or uri.startswith("motherduck://"):
            parsed_uri = urlparse(uri)
            query_params = parse_qs(parsed_uri.query)
            # Convert md:// URI to duckdb:///md: format
            db_path = parsed_uri.path or parsed_uri.netloc
            db_path = db_path.lstrip("/")

            token = query_params.get("token", [""])[0]
            if not token:
                raise ValueError("Token is required for MotherDuck connection")
            uri = f"duckdb:///md:{db_path}?motherduck_token={token}"

        if uri.startswith("mysql://"):
            uri = uri.replace("mysql://", "mysql+pymysql://")

        # Handle Databricks OAuth M2M authentication
        if uri.startswith("databricks://"):
            parsed_uri = urlparse(uri)
            query_params = parse_qs(parsed_uri.query)
            client_id = query_params.get("client_id", [None])[0]
            client_secret = query_params.get("client_secret", [None])[0]

            if client_id and client_secret:
                from omniload.util.auth import get_databricks_oauth_token

                server_hostname = parsed_uri.hostname
                if not server_hostname:
                    raise ValueError("Databricks URI must include a server hostname")

                # Exchange client credentials for access token
                access_token = get_databricks_oauth_token(
                    server_hostname, client_id, client_secret
                )

                # Remove client_id and client_secret from query params
                filtered_params = {
                    k: v
                    for k, v in query_params.items()
                    if k not in ("client_id", "client_secret")
                }

                # Rebuild URI preserving all components (port, path, etc.)
                uri = parsed_uri._replace(
                    netloc=f"token:{access_token}@{parsed_uri.netloc.split('@')[-1]}",
                    query=urlencode(filtered_params, doseq=True)
                    if filtered_params
                    else "",
                ).geturl()

        # Monkey patch cx_Oracle to use oracledb (thin mode, no client libraries required)
        if uri.startswith("oracle+") or uri.startswith("oracle://"):
            try:
                import oracledb  # type: ignore[import-not-found]

                # SQLAlchemy's cx_oracle dialect checks for version >= 5.2
                # oracledb has a different versioning scheme, so we need to patch it
                oracledb.version.__version__ = "8.3.0"  # ty: ignore[invalid-assignment]
                sys.modules["cx_Oracle"] = oracledb
            except ImportError:
                # oracledb not installed, will fail later with a clear error
                pass

        # Process Snowflake private key authentication
        if uri.startswith("snowflake://"):
            parsed_uri = urlparse(uri)
            query_params = parse_qs(parsed_uri.query)

            if "private_key" in query_params:
                from dlt.common.libs.cryptography import decode_private_key

                private_key = query_params["private_key"][0]
                passphrase = query_params.get("private_key_passphrase", [None])[0]
                decoded_key = decode_private_key(private_key, passphrase)

                query_params["private_key"] = [base64.b64encode(decoded_key).decode()]
                if "private_key_passphrase" in query_params:
                    del query_params["private_key_passphrase"]

                # Rebuild URI
                uri = parsed_uri._replace(
                    query=urlencode(query_params, doseq=True)
                ).geturl()

        # clickhouse://<username>:<password>@<host>:<port>?secure=<secure>
        if uri.startswith("clickhouse://"):
            parsed_uri = urlparse(uri)

            query_params = parse_qs(parsed_uri.query)

            if "http_port" in query_params:
                del query_params["http_port"]

            if "secure" not in query_params:
                query_params["secure"] = ["1"]

            uri = parsed_uri._replace(
                scheme="clickhouse+native",
                query=urlencode(query_params, doseq=True),
            ).geturl()

        if uri.startswith("db2://"):
            uri = uri.replace("db2://", "db2+ibm_db://")

        if uri.startswith("spanner://"):
            parsed_uri = urlparse(uri)
            query_params = parse_qs(parsed_uri.query)

            project_id_param = query_params.get("project_id")
            instance_id_param = query_params.get("instance_id")
            database_param = query_params.get("database")

            cred_path = query_params.get("credentials_path")
            cred_base64 = query_params.get("credentials_base64")

            if not project_id_param or not instance_id_param or not database_param:
                raise ValueError(
                    "project_id, instance_id and database are required in the URI to get data from Google Spanner"
                )

            project_id = project_id_param[0]
            instance_id = instance_id_param[0]
            database = database_param[0]

            if not cred_path and not cred_base64:
                raise ValueError(
                    "credentials_path or credentials_base64 is required in the URI to get data from Google Sheets"
                )
            if cred_path:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path[0]
            elif cred_base64:
                credentials = json.loads(
                    base64.b64decode(cred_base64[0]).decode("utf-8")
                )
                temp = tempfile.NamedTemporaryFile(
                    mode="w", delete=False, suffix=".json"
                )
                json.dump(credentials, temp)
                temp.close()
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp.name

            uri = f"spanner+spanner:///projects/{project_id}/instances/{instance_id}/databases/{database}"

            def eng_callback(engine):
                return engine.execution_options(read_only=True)

            engine_adapter_callback = eng_callback

        if uri.startswith("cratedb://"):
            # CrateDB's SQLAlchemy dialect uses the `crate://` protocol scheme,
            # but we would like to harmonize on `cratedb://` across the board.
            # https://github.com/bruin-data/bruin/issues/1640

            # Rewrite parameter `sslmode` to `ssl`.
            parsed_uri = urlparse(uri)
            query_params = parse_qs(parsed_uri.query)
            if "sslmode" in query_params:
                sslmode: str = query_params.get("sslmode", [""])[0]
                del query_params["sslmode"]
                ssl = "false"
                if sslmode.lower() in [
                    "require",
                    "verify-ca",
                    "verify-full",
                ]:
                    ssl = "true"
                query_params["ssl"] = [ssl]

            # Rebuild URI
            uri = parsed_uri._replace(
                scheme="crate", query=urlencode(query_params, doseq=True)
            ).geturl()

        from dlt.common.libs.sql_alchemy import (
            Engine,
            MetaData,
        )
        from dlt.sources.sql_database.schema_types import (
            ReflectionLevel,
            SelectAny,
            Table,
            TTypeAdapter,
        )
        from sqlalchemy import Column
        from sqlalchemy import types as sa

        from omniload.codec.filter import table_adapter_exclude_columns
        from omniload.source.sql_database.callbacks import (
            chained_query_adapter_callback,
            custom_query_variable_subsitution,
            limit_callback,
            type_adapter_callback,
        )

        query_adapters = []
        if kwargs.get("sql_limit"):
            query_adapters.append(
                limit_callback(kwargs["sql_limit"], kwargs.get("incremental_key"))
            )

        defer_table_reflect = False
        sql_backend = kwargs.get("sql_backend", "sqlalchemy")
        if table.startswith("query:"):
            if kwargs.get("sql_limit"):
                raise ValueError(
                    "sql_limit is not supported for custom queries, please apply the limit in the query instead"
                )

            sql_backend = "sqlalchemy"
            defer_table_reflect = True
            query_value = table.split(":", 1)[1]

            TableBackend: TypeAlias = Literal[
                "sqlalchemy", "pyarrow", "pandas", "connectorx"
            ]
            TQueryAdapter: TypeAlias = Callable[[SelectAny, Table], SelectAny]
            import dlt
            from dlt.common.typing import TDataItem

            # this is a very hacky version of the table_rows function. it is built this way to go around the dlt's table loader.
            # I didn't want to write a full fledged sqlalchemy source for now, and wanted to benefit from the existing stuff to begin with.
            # this is by no means a production ready solution, but it works for now.
            # the core idea behind this implementation is to create a mock table instance with the columns that are absolutely necessary for the incremental load to work.
            # the table loader will then use the query adapter callback to apply the actual query and load the rows.
            def table_rows(
                engine: Engine,
                table: Union[Table, str],
                metadata: MetaData,
                chunk_size: int,
                backend: TableBackend,
                incremental: Optional[Incremental[Any]] = None,
                reflection_level: ReflectionLevel = "minimal",
                table_adapter_callback: Optional[Callable[[Table], None]] = None,
                backend_kwargs: Dict[str, Any] = None,  # type: ignore
                type_adapter_callback: Optional[TTypeAdapter] = None,
                included_columns: Optional[List[str]] = None,
                excluded_columns: Optional[
                    List[str]
                ] = None,  # Added for dlt 1.16.0 compatibility
                query_adapter_callback: Optional[TQueryAdapter] = None,
                resolve_foreign_keys: bool = False,
                table_loader_class: Optional[Type[BaseTableLoader]] = None,
            ) -> Iterator[TDataItem]:
                hints = {
                    "columns": [],
                }
                cols = []

                if incremental:
                    switchDict = {
                        int: sa.INTEGER,
                        datetime: sa.TIMESTAMP,
                        date: sa.DATE,
                        pendulum.Date: sa.DATE,
                        pendulum.DateTime: sa.TIMESTAMP,
                    }

                    if incremental.last_value is not None:
                        cols.append(
                            Column(
                                incremental.cursor_path,
                                switchDict[type(incremental.last_value)],
                            )
                        )
                    else:
                        cols.append(Column(incremental.cursor_path, sa.TIMESTAMP))

                table = Table(
                    "query_result",
                    metadata,
                    *cols,
                )

                from dlt.sources.sql_database.helpers import TableLoader

                loader = TableLoader(
                    engine,
                    backend,
                    table,
                    hints["columns"],  # type: ignore
                    incremental=incremental,
                    chunk_size=chunk_size,
                    query_adapter_callback=query_adapter_callback,
                )
                try:
                    yield from loader.load_rows(backend_kwargs)
                finally:
                    if getattr(engine, "may_dispose_after_use", False):
                        engine.dispose()

            dlt.sources.sql_database.table_rows = table_rows  # type: ignore

            # override the query adapters, the only one we want is the one here in the case of custom queries
            query_adapters = [custom_query_variable_subsitution(query_value, kwargs)]

        credentials = ConnectionStringCredentials(uri)
        if uri.startswith("mssql://"):
            parsed_uri = urlparse(uri)
            params = parse_qs(parsed_uri.query)
            params = {k.lower(): v for k, v in params.items()}
            if params.get("authentication") == ["ActiveDirectoryAccessToken"]:
                import pyodbc
                from sqlalchemy import create_engine

                from omniload.target.mssql import MSSQL_COPT_SS_ACCESS_TOKEN
                from omniload.util.auth import serialize_azure_token
                from omniload.util.time import handle_datetimeoffset

                cfg = {
                    "DRIVER": params.get("driver", ["ODBC Driver 18 for SQL Server"])[
                        0
                    ],
                    "SERVER": f"{parsed_uri.hostname},{parsed_uri.port or 1433}",
                    "DATABASE": parsed_uri.path.lstrip("/"),
                }
                for k, v in params.items():
                    if k.lower() not in ["driver", "authentication", "connect_timeout"]:
                        cfg[k.upper()] = v[0]

                token = serialize_azure_token(parsed_uri.password)
                dsn = ";".join([f"{k}={v}" for k, v in cfg.items()])

                def creator():
                    connection = pyodbc.connect(
                        dsn,
                        autocommit=True,
                        timeout=kwargs.get("connect_timeout", 30),
                        attrs_before={
                            MSSQL_COPT_SS_ACCESS_TOKEN: token,
                        },
                    )
                    connection.add_output_converter(-155, handle_datetimeoffset)
                    return connection

                credentials = create_engine(
                    "mssql+pyodbc://",
                    creator=creator,
                )

        builder_res = self.table_builder(
            credentials=credentials,
            schema=table_fields.dataset,
            table=table_fields.table,
            incremental=incremental,
            backend=sql_backend,
            chunk_size=kwargs.get("page_size", None),
            reflection_level=kwargs.get("sql_reflection_level", None),
            query_adapter_callback=chained_query_adapter_callback(query_adapters),
            type_adapter_callback=type_adapter_callback,
            table_adapter_callback=table_adapter_exclude_columns(
                kwargs.get("sql_exclude_columns", [])
            ),
            defer_table_reflect=defer_table_reflect,
            engine_adapter_callback=engine_adapter_callback,
        )

        builder_res.max_table_nesting = 0

        return builder_res
