from typing import Any, Dict
from urllib.parse import urlparse

from omniload.error import MissingValueError
from omniload.util.endpoint import UnsupportedEndpointError, parse_endpoint


class SFTPSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        host = parsed_uri.hostname
        if not host:
            raise MissingValueError("host", "SFTP URI")
        port = parsed_uri.port or 22
        username = parsed_uri.username
        password = parsed_uri.password

        params: Dict[str, Any] = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "look_for_keys": False,
            "allow_agent": False,
        }

        import fsspec

        try:
            fs = fsspec.filesystem("sftp", **params)
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect or authenticate to sftp server {host}:{port}. Error: {e}"
            )
        bucket_url = f"sftp://{host}:{port}"

        if table.startswith("/"):
            file_glob = table
        else:
            file_glob = f"/{table}"

        try:
            endpoint = parse_endpoint(table)
        except UnsupportedEndpointError:
            raise ValueError(
                "SFTP Source only supports specific formats files: csv, jsonl, parquet"
            )
        except Exception as e:
            raise ValueError(f"Failed to parse endpoint from path: {table}") from e

        from omniload.source.filesystem.adapter import readers

        dlt_source_resource = readers(bucket_url, fs, file_glob)
        return dlt_source_resource.with_resources(endpoint)
