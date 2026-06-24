from urllib.parse import parse_qs, urlparse

import dlt


class ClickhouseDestination:
    def dlt_dest(self, uri: str, **kwargs):
        parsed_uri = urlparse(uri)

        if "dest_table" in kwargs:
            table = kwargs["dest_table"]
            database = table.split(".")[0]
        else:
            database = parsed_uri.path.lstrip("/")

        username = parsed_uri.username
        if not username:
            raise ValueError(
                "A username is required to connect to the ClickHouse database."
            )

        password = parsed_uri.password
        if not password:
            raise ValueError(
                "A password is required to authenticate with the ClickHouse database."
            )

        host = parsed_uri.hostname
        if not host:
            raise ValueError(
                "The hostname or IP address of the ClickHouse server is required to establish a connection."
            )

        port = parsed_uri.port
        if not port:
            raise ValueError(
                "The TCP port of the ClickHouse server is required to establish a connection."
            )

        query_params = parse_qs(parsed_uri.query)
        secure = int(query_params["secure"][0]) if "secure" in query_params else 1

        default_http_port = 8443 if secure == 1 else 8123
        http_port = (
            int(query_params["http_port"][0])
            if "http_port" in query_params
            else default_http_port
        )

        if secure not in (0, 1):
            raise ValueError(
                "Invalid value for secure. Set to `1` for a secure HTTPS connection or `0` for a non-secure HTTP connection."
            )

        from dlt.destinations.impl.clickhouse.configuration import ClickHouseCredentials

        credentials = ClickHouseCredentials(
            {
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "database": database,
                "http_port": http_port,
                "secure": secure,
            }
        )
        return dlt.destinations.clickhouse(credentials=credentials)

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")
        return {
            "table_name": table_fields[-1],
        }

    def post_load(self):
        pass

    @staticmethod
    def engine_settings(uri: str) -> dict[str, str]:
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        return {
            key[len("engine.") :]: query_params[key][0]
            for key in query_params
            if key.startswith("engine.")
        }

    @staticmethod
    def engine_type(uri: str) -> str | None:
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        values = query_params.get("engine")
        if values:
            return values[0]
        return None
