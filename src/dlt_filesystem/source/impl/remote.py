from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Optional, Type
from urllib.parse import parse_qs, urlparse

from fsspec import AbstractFileSystem

from dlt_filesystem.error import InvalidBlobTableError, MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.error import UnsupportedEndpointError
from dlt_filesystem.source.format.registry import supported_file_format_message
from dlt_filesystem.source.model import strip_run_options
from dlt_filesystem.source.router import (
    blob_hints,
    determine_endpoint,
    parse_uri,
    source_selects_single_file,
)
from dlt_filesystem.util.auth import (
    azure_arrow_filesystem_kwargs,
    gcs_filesystem_kwargs,
    parse_azure_blob_auth,
    s3_arrow_filesystem_kwargs,
)
from dlt_filesystem.util.fsspec import ReadIntoArrowFSWrapper

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


class _S3CompatibleArrowFSWrapper(ReadIntoArrowFSWrapper):
    """Keep S3-compatible object keys intact while stripping their URI scheme."""

    protocol = "s3"

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        prefix = f"{cls.protocol}://"
        if path.startswith(prefix):
            return path[len(prefix) :]
        return super()._strip_protocol(path)


class _R2ArrowFSWrapper(_S3CompatibleArrowFSWrapper):
    """Expose an Arrow S3 client through R2's public URI scheme."""

    protocol = "r2"


class _AzureArrowFSWrapper(ReadIntoArrowFSWrapper):
    """Keep Azure blob names intact while stripping their URI scheme.

    Arrow addresses a blob as ``container/name`` with the storage account as the
    filesystem root, so stripping is a prefix removal. The generic wrapper reads
    the rest of the path as a URL instead, and a blob name may contain ``?`` or
    ``#`` literally.
    """

    protocol = "az"
    #: Every Azure user-scheme is a registry alias onto one client, so all three
    #: reach this wrapper even though discovery composes ``az://`` URLs.
    schemes = ("az", "adls", "abfss")

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        for scheme in cls.schemes:
            prefix = f"{scheme}://"
            if path.startswith(prefix):
                return path[len(prefix) :]
        return super()._strip_protocol(path)


class GCSSource(FilesystemSource):
    """dlt source for Google Cloud Storage"""

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        """Return GCSFileSystem class"""
        # There's a compatibility issue between google-auth, dlt and gcsfs
        # that makes it difficult to use google.oauth2.service_account.Credentials
        # (The RECOMMENDED way of passing service account credentials)
        # directly with gcsfs. As a workaround, we construct the GCSFileSystem
        # and pass it directly to filesystem.readers.
        from gcsfs import GCSFileSystem

        return GCSFileSystem

    def dlt_source(
        self,
        uri: str,
        table: str,
        *,
        filesystem_incremental: bool = False,
        column_types: Optional[Dict[str, Any]] = None,
        reader_hints: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("GCS")

        bucket_url = f"gs://{bucket_name}"

        # gcsfs takes the caller's own keyword arguments as the baseline it merges the
        # URI parameters into, and this connector merges the query string wholesale, so
        # both carriers get filtered before they reach the constructor.
        fs = self.fs_class(**gcs_filesystem_kwargs(strip_run_options(params), kwargs))

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("GCS")) from None
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.core import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace="gcs",
                filesystem_incremental=filesystem_incremental,
                require_file_match=source_selects_single_file(uri, table),
                hints={**(reader_hints or {}), **blob_hints(parsed_uri, table)},
                column_types=column_types,
            )
        )


class S3CompatibleSource(FilesystemSource):
    """Access S3 and compatible filesystems through ``pyarrow.fs``."""

    @property
    @abstractmethod
    def fs_name(self) -> str:
        raise NotImplementedError("Need to implement abstract property")

    @property
    def fs_class(self) -> Type[Any]:
        from pyarrow.fs import S3FileSystem

        return S3FileSystem

    @property
    def fs_protocol(self) -> str:
        return "s3"

    def _filesystem(self, fs_kwargs: dict[str, Any]) -> AbstractFileSystem:
        arrow_fs = self.fs_class(**fs_kwargs)
        wrapper = (
            _R2ArrowFSWrapper
            if self.fs_protocol == "r2"
            else _S3CompatibleArrowFSWrapper
        )
        return wrapper(arrow_fs)

    def dlt_source(
        self,
        uri: str,
        table: str,
        *,
        filesystem_incremental: bool = False,
        column_types: Optional[Dict[str, Any]] = None,
        reader_hints: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)
        fs_kwargs = s3_arrow_filesystem_kwargs(source_fields, self.fs_name)
        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError(self.fs_name)

        bucket_url = f"{self.fs_protocol}://{bucket_name}/"

        endpoint_url = source_fields.get("endpoint_url")

        fs = self._filesystem(fs_kwargs)

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message(self.fs_name)) from None
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.core import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace=f"s3:{self.endpoint_namespace(endpoint_url[0] if endpoint_url else None, 'aws')}",
                filesystem_incremental=filesystem_incremental,
                require_file_match=source_selects_single_file(uri, table),
                hints={**(reader_hints or {}), **blob_hints(parsed_uri, table)},
                column_types=column_types,
            )
        )


class S3Source(S3CompatibleSource):
    @property
    def fs_name(self) -> str:
        return "S3"


class AzureSource(FilesystemSource):
    """Azure Blob Storage / ADLS Gen2 source (``az://``, ``adls://``, ``abfss://``).

    Reads through ``pyarrow.fs.AzureFileSystem``, which serves both Blob and
    ADLS Gen2 from one client and detects a hierarchical namespace itself, so
    every Azure user-scheme reads through the same ``az://`` backend; the
    ``adls://`` / ``abfss://`` schemes are registry aliases onto this class.
    """

    @property
    def fs_class(self) -> Type[Any]:
        from pyarrow.fs import AzureFileSystem

        return AzureFileSystem

    def _filesystem(self, fs_kwargs: dict[str, Any]) -> AbstractFileSystem:
        """Wrap the native client, which does not speak the fsspec contract."""
        return _AzureArrowFSWrapper(self.fs_class(**fs_kwargs))

    def dlt_source(
        self,
        uri: str,
        table: str,
        *,
        filesystem_incremental: bool = False,
        column_types: Optional[Dict[str, Any]] = None,
        reader_hints: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        auth = parse_azure_blob_auth(params)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("Azure")

        bucket_url = f"az://{bucket_name}"

        kwargs.update(azure_arrow_filesystem_kwargs(auth))
        fs = self._filesystem(kwargs)

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("Azure")) from None
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.core import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace=(
                    f"azure:{(auth.account_name or '').lower()}:"
                    f"{self.endpoint_namespace(auth.account_host, 'azure-public')}"
                ),
                filesystem_incremental=filesystem_incremental,
                require_file_match=source_selects_single_file(uri, table),
                hints={**(reader_hints or {}), **blob_hints(parsed_uri, table)},
                column_types=column_types,
            )
        )


class SFTPSource(FilesystemSource):
    """Access files on SFTP servers."""

    def dlt_source(
        self,
        uri: str,
        table: str,
        *,
        filesystem_incremental: bool = False,
        column_types: Optional[Dict[str, Any]] = None,
        reader_hints: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        parsed_uri = urlparse(uri)
        host = parsed_uri.hostname
        if not host:
            raise MissingConnectorOption("host", "SFTP")
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

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("SFTP")

        bucket_url = f"sftp://{host}:{port}"

        try:
            endpoint = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("SFTP")) from None
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.core import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace=(f"sftp:{host.lower()}:{port}:{username or ''}"),
                filesystem_incremental=filesystem_incremental,
                require_file_match=source_selects_single_file(uri, table),
                hints={**(reader_hints or {}), **blob_hints(parsed_uri, table)},
                column_types=column_types,
            )
        )
