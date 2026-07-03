from typing import Any, Dict
from urllib.parse import urlparse

from omniload.error import MissingValueError
from omniload.util.endpoint import (
    UnsupportedEndpointError,
    determine_endpoint,
    split_format_hint,
    supported_file_format_message,
)


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

        table_path, _ = split_format_hint(table)
        if table_path.startswith("/"):
            file_glob = table_path
        else:
            file_glob = f"/{table_path}"

        try:
            endpoint = determine_endpoint(table, file_glob)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("SFTP"))
        except Exception as e:
            raise ValueError(f"Failed to parse endpoint from path: {table}") from e

        from omniload.source.filesystem.adapter import (
            ReaderResourceRequest,
            resource_for_reader,
        )

        return resource_for_reader(
            ReaderResourceRequest(
                bucket_url=bucket_url,
                credentials=fs,
                file_glob=file_glob,
                reader_name=endpoint,
                column_types=kwargs.get("column_types"),
                table=table,
            )
        )
