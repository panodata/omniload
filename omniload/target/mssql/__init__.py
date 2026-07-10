import dlt
from dlt import Schema
from dlt.common.destination import DestinationCapabilitiesContext

from omniload.target.model import GenericSqlDestination
from omniload.util.auth import serialize_azure_token
from omniload.util.time import handle_datetimeoffset

# MSSQL_COPT_SS_ACCESS_TOKEN is a connection attribute used to pass
# an Azure Active Directory access token to the SQL Server ODBC driver.
MSSQL_COPT_SS_ACCESS_TOKEN = 1256


def build_mssql_dest():
    """Build a dlt MSSQL destination class with Azure token authentication support."""
    # https://github.com/bruin-data/ingestr/issues/293

    from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration
    from dlt.destinations.impl.mssql.mssql import (
        HINT_TO_MSSQL_ATTR,
        MsSqlJobClient,
    )
    from dlt.destinations.impl.mssql.sql_client import (
        PyOdbcMsSqlClient,
    )

    class OdbcMsSqlClient(PyOdbcMsSqlClient):
        """pyodbc client that can authenticate with an Azure access token."""

        SKIP_CREDENTIALS = {"PWD", "AUTHENTICATION", "UID"}

        def open_connection(self):
            """Open a standard MSSQL connection or an Azure-token connection."""
            cfg = self.credentials.get_odbc_dsn_dict()
            if (
                cfg.get("AUTHENTICATION", "").strip().lower()
                != "activedirectoryaccesstoken"
            ):
                return super().open_connection()

            import pyodbc

            dsn = ";".join(
                [f"{k}={v}" for k, v in cfg.items() if k not in self.SKIP_CREDENTIALS]
            )

            self._conn = pyodbc.connect(
                dsn,
                timeout=self.credentials.connect_timeout,
                attrs_before={
                    MSSQL_COPT_SS_ACCESS_TOKEN: serialize_azure_token(cfg["PWD"]),
                },
            )

            # https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
            self._conn.add_output_converter(-155, handle_datetimeoffset)
            self._conn.autocommit = True
            return self._conn

    class MsSqlClient(MsSqlJobClient):
        """dlt MSSQL job client wired to the custom pyodbc SQL client."""

        def __init__(
            self,
            schema: Schema,
            config: MsSqlClientConfiguration,
            capabilities: DestinationCapabilitiesContext,
        ) -> None:
            """Initialize the job client with the custom ODBC SQL client."""
            sql_client = OdbcMsSqlClient(
                config.normalize_dataset_name(schema),
                config.normalize_staging_dataset_name(schema),
                config.credentials,
                capabilities,
            )
            super(MsSqlJobClient, self).__init__(schema, config, sql_client)
            self.config: MsSqlClientConfiguration = config
            self.sql_client = sql_client
            self.active_hints = HINT_TO_MSSQL_ATTR if self.config.create_indexes else {}
            self.type_mapper = capabilities.get_type_mapper()

    class MsSqlDestImpl(dlt.destinations.mssql):
        """dlt MSSQL destination implementation using the custom client class."""

        @property
        def client_class(self):
            """Return the MSSQL job client implementation for this destination."""
            return MsSqlClient

    return MsSqlDestImpl


class MsSQLDestination(GenericSqlDestination):
    """Destination adapter for SQL Server and Azure SQL targets."""

    def dlt_dest(self, uri: str, **kwargs):
        """Build the dlt MSSQL destination for the given connection URI."""
        cls = build_mssql_dest()
        return cls(credentials=uri, **kwargs)
