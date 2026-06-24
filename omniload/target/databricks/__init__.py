from urllib.parse import parse_qs, urlparse

import dlt

from omniload.target.model import GenericSqlDestination
from omniload.util.auth import get_databricks_oauth_token


class DatabricksDestination(GenericSqlDestination):
    def dlt_dest(self, uri: str, **kwargs):
        p = urlparse(uri)
        q = parse_qs(p.query)
        server_hostname = p.hostname
        http_path = q.get("http_path", [None])[0]
        catalog = q.get("catalog", [None])[0]

        if not server_hostname:
            raise ValueError("Databricks URI must include a server hostname")

        # Check for OAuth M2M credentials (client_id and client_secret)
        client_id = q.get("client_id", [None])[0]
        client_secret = q.get("client_secret", [None])[0]

        access_token: str
        if client_id and client_secret:
            # OAuth M2M authentication: exchange client credentials for access token
            access_token = get_databricks_oauth_token(
                server_hostname, client_id, client_secret
            )
        else:
            # Traditional token-based authentication
            if not p.password:
                raise ValueError(
                    "Databricks URI must include an access token or client_id/client_secret"
                )
            access_token = p.password

        creds = {
            "access_token": access_token,
            "server_hostname": server_hostname,
            "http_path": http_path,
            "catalog": catalog,
        }

        return dlt.destinations.databricks(
            credentials=creds,
            **kwargs,
        )

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        p = urlparse(uri)
        q = parse_qs(p.query)
        uri_schema = q.get("schema", [None])[0]

        table_fields = table.split(".")

        # If table is in schema.table format, use that (overrides URI schema)
        if len(table_fields) == 2:
            return {
                "dataset_name": table_fields[0],
                "table_name": table_fields[1],
            }

        # If table is just a table name, use schema from URI
        if len(table_fields) == 1 and uri_schema:
            return {
                "dataset_name": uri_schema,
                "table_name": table_fields[0],
            }

        raise ValueError(
            "Table name must be in the format <schema>.<table>, or specify schema in the URI"
        )
