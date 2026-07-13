import base64
import json
from typing import Any, Dict
from urllib.parse import parse_qs, urlparse

from omniload.error import InvalidBlobTableError, MissingValueError
from omniload.source.filesystem.base import FilesystemSource
from omniload.source.filesystem.error import UnsupportedEndpointError
from omniload.source.filesystem.format.registry import supported_file_format_message
from omniload.source.filesystem.router import (
    blob_hints,
    determine_endpoint,
    parse_fragment,
    parse_uri,
)
from omniload.util.auth import AzureBlobAuth, parse_azure_blob_auth


class GCSSource(FilesystemSource):
    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "GCS takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("GCS")

        bucket_url = f"gs://{bucket_name}"

        credentials_path = params.get("credentials_path")
        credentials_base64 = params.get("credentials_base64")
        credentials_available = any(
            map(
                lambda x: x is not None,
                [credentials_path, credentials_base64],
            )
        )
        if credentials_available is False:
            raise MissingValueError("credentials_path or credentials_base64", "GCS")

        credentials = None
        if credentials_path:
            credentials = credentials_path[0]
        else:
            credentials = json.loads(base64.b64decode(credentials_base64[0]).decode())  # type: ignore

        # There's a compatiblity issue between google-auth, dlt and gcsfs
        # that makes it difficult to use google.oauth2.service_account.Credentials
        # (The RECOMMENDED way of passing service account credentials)
        # directly with gcsfs. As a workaround, we construct the GCSFileSystem
        # and pass it directly to filesystem.readers.
        import gcsfs

        fs = gcsfs.GCSFileSystem(
            token=credentials,
        )

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("GCS"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from omniload.source.filesystem.adapter import resource_for_reader
        from omniload.source.filesystem.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


class S3Source(FilesystemSource):
    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "S3 takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)
        access_key_id = source_fields.get("access_key_id")
        if not access_key_id:
            raise ValueError("access_key_id is required to connect to S3")

        secret_access_key = source_fields.get("secret_access_key")
        if not secret_access_key:
            raise ValueError("secret_access_key is required to connect to S3")

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("S3")

        bucket_url = f"s3://{bucket_name}/"

        import s3fs

        endpoint_url = source_fields.get("endpoint_url")
        fs_kwargs: dict = {
            "key": access_key_id[0],
            "secret": secret_access_key[0],
        }
        if endpoint_url:
            fs_kwargs["endpoint_url"] = endpoint_url[0]

        fs = s3fs.S3FileSystem(**fs_kwargs)

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("S3"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from omniload.source.filesystem.adapter import resource_for_reader
        from omniload.source.filesystem.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


def _azure_fs(auth: AzureBlobAuth):
    """Build an ``adlfs.AzureBlobFileSystem`` from resolved Azure auth params.

    The ingestr-style short names already match adlfs kwargs, so they pass
    straight through; only the supplied ones are forwarded. ``adlfs`` is
    imported lazily so the CLI ``--help`` and every non-Azure path never load
    the Azure SDK (matching the s3fs/gcsfs deferred-import convention).
    """
    import adlfs

    kwargs = {"account_name": auth.account_name}
    if auth.account_key is not None:
        kwargs["account_key"] = auth.account_key
    if auth.sas_token is not None:
        kwargs["sas_token"] = auth.sas_token
    if auth.tenant_id is not None:
        kwargs["tenant_id"] = auth.tenant_id
    if auth.client_id is not None:
        kwargs["client_id"] = auth.client_id
    if auth.client_secret is not None:
        kwargs["client_secret"] = auth.client_secret
    if auth.account_host is not None:
        kwargs["account_host"] = auth.account_host

    # adlfs annotates its credential params as `str` (defaulting to None) and
    # mixes in non-str params (blocksize: int, ...), so ty can't check a
    # conditional str-kwargs splat against the signature. The kwargs are all
    # valid adlfs credential arguments by construction.
    return adlfs.AzureBlobFileSystem(**kwargs)  # ty: ignore[invalid-argument-type]


class AzureSource(FilesystemSource):
    """Azure Blob Storage / ADLS Gen2 source (``az://``, ``adls://``, ``abfss://``).

    adlfs serves both Blob and ADLS Gen2 through one ``AzureBlobFileSystem`` and
    the ``az://`` bucket-url scheme, so every Azure user-scheme reads through the
    same ``az://`` backend; the ``adls://`` / ``abfss://`` schemes are registry
    aliases onto this class.
    """

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Azure takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        auth = parse_azure_blob_auth(params)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("Azure")

        bucket_url = f"az://{bucket_name}"

        fs = _azure_fs(auth)

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("Azure"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from omniload.source.filesystem.adapter import resource_for_reader
        from omniload.source.filesystem.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


class SFTPSource(FilesystemSource):
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
            ) from e
        bucket_url = f"sftp://{host}:{port}"

        table_path, _, hints = parse_fragment(table)
        if table_path.startswith("/"):
            file_glob = table_path
        else:
            file_glob = f"/{table_path}"

        try:
            endpoint = determine_endpoint(table, file_glob)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("SFTP")) from None
        except Exception as e:
            raise ValueError(f"Failed to parse endpoint from path: {table}") from e

        from omniload.source.filesystem.adapter import resource_for_reader
        from omniload.source.filesystem.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=file_glob,
                reader_name=endpoint,
                hints=hints,
                column_types=kwargs.get("column_types"),
            )
        )
