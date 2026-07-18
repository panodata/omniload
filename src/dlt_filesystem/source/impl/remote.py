import base64
import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Type
from urllib.parse import parse_qs, urlparse

from dlt_filesystem.error import InvalidBlobTableError, MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.error import UnsupportedEndpointError
from dlt_filesystem.source.format.registry import supported_file_format_message
from dlt_filesystem.source.router import (
    blob_hints,
    determine_endpoint,
    parse_fragment,
    parse_uri,
)
from dlt_filesystem.util.auth import AzureBlobAuth, parse_azure_blob_auth

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem
    from pyarrow.fs import HadoopFileSystem


def _endpoint_namespace(endpoint: str | None, default: str) -> str:
    """Return a normalized endpoint identity without credentials or query values."""
    if not endpoint:
        return default

    parsed = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
    host = parsed.hostname
    if not host:
        return default

    host = host.lower()
    if ":" in host:
        host = f"[{host}]"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"

    return f"{host}{parsed.path.rstrip('/')}"


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
            map(  # noqa: C417
                lambda x: x is not None,
                [credentials_path, credentials_base64],
            )
        )
        if credentials_available is False:
            raise MissingConnectorOption(
                "credentials_path or credentials_base64", "GCS"
            )

        credentials = None
        if credentials_path:
            credentials = credentials_path[0]
        else:
            credentials = json.loads(base64.b64decode(credentials_base64[0]).decode())  # type: ignore

        # There's a compatibility issue between google-auth, dlt and gcsfs
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

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace="gcs",
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


class S3CompatibleSource(FilesystemSource):
    """
    Access S3 and compatible filesystems.

    TODO: Forward more parameters than `access_key_id` and `secret_access_key`
          (key/secret/endpoint_url) only, like `region`.
    """

    @property
    @abstractmethod
    def fs_name(self) -> str:
        raise NotImplementedError("Need to implement abstract property")

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        import s3fs

        return s3fs.S3FileSystem

    @property
    def fs_protocol(self) -> str:
        return self.fs_class.protocol[0]

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                f"{self.fs_name} takes care of incrementality on its own, "
                f"you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)
        access_key_id = source_fields.get("access_key_id")
        if not access_key_id:
            raise MissingConnectorOption("access_key_id", self.fs_name)

        secret_access_key = source_fields.get("secret_access_key")
        if not secret_access_key:
            raise MissingConnectorOption("secret_access_key", self.fs_name)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError(self.fs_name)

        bucket_url = f"{self.fs_protocol}://{bucket_name}/"

        endpoint_url = source_fields.get("endpoint_url")
        fs_kwargs: dict = {
            "key": access_key_id[0],
            "secret": secret_access_key[0],
        }
        if endpoint_url:
            fs_kwargs["endpoint_url"] = endpoint_url[0]

        fs = self.fs_class(**fs_kwargs)

        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message(self.fs_name))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace=f"s3:{_endpoint_namespace(endpoint_url[0] if endpoint_url else None, 'aws')}",
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


class S3Source(S3CompatibleSource):
    @property
    def fs_name(self) -> str:
        return "S3"


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

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace=(
                    f"azure:{auth.account_name.lower()}:"
                    f"{_endpoint_namespace(auth.account_host, 'azure-public')}"
                ),
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


class SFTPSource(FilesystemSource):
    def dlt_source(self, uri: str, table: str, **kwargs):
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

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=file_glob,
                reader_name=endpoint,
                storage_namespace=(f"sftp:{host.lower()}:{port}:{username or ''}"),
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=hints,
                column_types=kwargs.get("column_types"),
            )
        )


class HDFSSource(FilesystemSource):
    """
    Provide access to HDFS via Arrow.
    https://arrow.apache.org/docs/python/generated/pyarrow.fs.HadoopFileSystem.html
    """

    @property
    def fs_class(self) -> Type["HadoopFileSystem"]:
        from pyarrow.fs import HadoopFileSystem

        return HadoopFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "HDFS takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("HDFS")

        parsed_fields = parse_qs(parsed_uri.query)
        fs_kwargs: Dict[str, Any] = {
            key: value[0] for key, value in parsed_fields.items()
        }
        # Rename `block_size` to `default_block_size`.
        if "block_size" in fs_kwargs:
            fs_kwargs["default_block_size"] = fs_kwargs["block_size"]
            del fs_kwargs["block_size"]
        # Cast values to `int`.
        for field_name in ["port", "replication", "buffer_size", "default_block_size"]:
            if field_name in fs_kwargs:
                fs_kwargs[field_name] = int(fs_kwargs[field_name])
        # Cast values to `dict`.
        for field_name in ["extra_conf"]:
            if field_name in fs_kwargs:
                fs_kwargs[field_name] = json.loads(fs_kwargs[field_name])

        fs = self.fs_class(**kwargs)

        host = parsed_uri.hostname
        port = parsed_uri.port or 8020
        bucket_url = f"hdfs://{host}:{port}"
        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("HDFS"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

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


class R2Source(S3CompatibleSource):
    """
    Provide access to Cloudflare R2, compatible with Amazon S3.
    https://github.com/panodata/omniload/issues/163
    """

    @property
    def fs_name(self) -> str:
        return "R2"

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        import s3fs

        class R2FileSystem(s3fs.S3FileSystem):
            protocol = "r2"

        return R2FileSystem


class OSSSource(FilesystemSource):
    """
    Object Storage Service (OSS)
    https://www.alibabacloud.com/en/product/object-storage-service
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        import ossfs

        return ossfs.OSSFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: It looks like this is generic code that could be refactored already
        #       if it's common amongst different implementations. Breaking out the
        if kwargs.get("incremental_key"):
            raise ValueError(
                "OSS takes care of incrementality on its own, you should not provide incremental_key"
            )

        parsed_uri = urlparse(uri)
        parsed_fields = parse_qs(parsed_uri.query)

        # TODO: BaseOSSFileSystem accepts `endpoint`, `key`, `secret`, `token`,
        #       with xor key/secret vs. token. Let's give those parameter bunches
        #       a real data model and validate it using Pydantic or other such
        #       frameworks in the future. Maybe `dataclasses` or `attrs` is enough?
        fs_kwargs: Dict[str, Any] = {
            key: value[0] for key, value in parsed_fields.items()
        }

        # TODO: BaseOSSFileSystem accepts `default_cache_type` and `default_block_size`.
        #       I don't know why they are using the `default_` prefix. With an advanced
        #       parameter model, let's rename them automatically so users can use shorter
        #       names on input and output URIs.
        if "cache_type" in fs_kwargs:
            fs_kwargs["default_cache_type"] = fs_kwargs["cache_type"]
            del fs_kwargs["cache_type"]
        if "block_size" in fs_kwargs:
            fs_kwargs["default_block_size"] = fs_kwargs["block_size"]
            del fs_kwargs["block_size"]

        # TODO: BaseOSSFileSystem accepts `default_block_size` as `int`.
        #       Currently, only a single type conversion needs to be applied,
        #       so let's do it manually here for now. In the future, it will be
        #       sweet to let the parameter data model machinery handle it.
        if "default_block_size" in fs_kwargs:
            fs_kwargs["default_block_size"] = int(fs_kwargs["default_block_size"])

        # TODO: BaseOSSFileSystem accepts `default_cache_type` as a `str` type with
        #       a choice of different values. The default value is `readahead`, and
        #       setting `none` is possible. For all other values, the inline
        #       documentation refers to the `fsspec` documentation. Let's harvest
        #       relevant details and add them to the parameter data model.
        # No demo implementation here.

        # TODO: It looks like this is generic code that could be refactored already
        #       if it's common amongst different implementations. Breaking out the
        #       reference to the relevant filesystem implementation itself per
        #       `fs_class` already contributed to a better situation than before.
        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("OSS")

        bucket_url = f"oss://{bucket_name}/"

        fs = self.fs_class(**fs_kwargs)

        # TODO: Naming things: Rename `determine_endpoint` to `find_reader`.
        # TODO: Refactoring: Break out reader finding and fragments of the
        #       filesystem initialization into common routines.
        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("OSS"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

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


class OCISource(FilesystemSource):
    """
    Oracle Cloud Infrastructure Object Storage (OCI)

    https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm
    https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        import ocifs

        return ocifs.OCIFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        if kwargs.get("incremental_key"):
            raise ValueError(
                "OCI takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Decode URL.
        parsed_uri = urlparse(uri)
        parsed_fields = parse_qs(parsed_uri.query)

        # Decode query arguments.
        fs_kwargs: Dict[str, Any] = {
            key: value[0] for key, value in parsed_fields.items()
        }

        # TODO: OCIFileSystem accepts dict-typed `config`, `config_kwargs`,
        #       and `oci_additional_kwargs`. Convey this using JSON.
        for field_name in ["config", "config_kwargs", "oci_additional_kwargs"]:
            if field_name in fs_kwargs:
                fs_kwargs[field_name] = json.loads(fs_kwargs[field_name])

        # TODO: OCIFileSystem accepts `default_block_size`.
        #       I don't know why they are using the `default_` prefix. With an advanced
        #       parameter model, let's rename them automatically so users can use shorter
        #       names on input and output URIs.
        if "block_size" in fs_kwargs:
            fs_kwargs["default_block_size"] = fs_kwargs["block_size"]
            del fs_kwargs["block_size"]

        # TODO: OCIFileSystem accepts `default_block_size` as `int`.
        #       Currently, only a single type conversion needs to be applied,
        #       so let's do it manually here for now. In the future, it will be
        #       sweet to let the parameter data model machinery handle it.
        if "default_block_size" in fs_kwargs:
            fs_kwargs["default_block_size"] = int(fs_kwargs["default_block_size"])

        # TODO: It looks like this is generic code that could be refactored already
        #       if it's common amongst different implementations. Breaking out the
        #       reference to the relevant filesystem implementation itself per
        #       `fs_class` already contributed to a better situation than before.
        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("OCI")

        bucket_url = f"oci://{bucket_name}/"

        fs = self.fs_class(**fs_kwargs)

        # TODO: Naming things: Rename `determine_endpoint` to `find_reader`.
        # TODO: Refactoring: Break out reader finding and fragments of the
        #       filesystem initialization into common routines.
        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("OCI"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

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


class DropboxSource(FilesystemSource):
    """
    Access files on Dropbox.

    https://github.com/fsspec/dropboxdrivefs
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        import dropboxdrivefs

        return dropboxdrivefs.DropboxDriveFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Dropbox takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Decode URL.
        parsed_uri = urlparse(uri)
        parsed_fields = parse_qs(parsed_uri.query)

        # Decode query arguments.
        fs_kwargs: Dict[str, Any] = {
            key: value[0] for key, value in parsed_fields.items()
        }

        # TODO: It looks like this is generic code that could be refactored already
        #       if it's common amongst different implementations. Breaking out the
        #       reference to the relevant filesystem implementation itself per
        #       `fs_class` already contributed to a better situation than before.
        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("Dropbox")

        bucket_url = f"dropbox://{bucket_name}/"

        fs = self.fs_class(**fs_kwargs)

        # TODO: Naming things: Rename `determine_endpoint` to `find_reader`.
        # TODO: Refactoring: Break out reader finding and fragments of the
        #       filesystem initialization into common routines.
        try:
            endpoint: str = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("Dropbox"))
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from path: {path_to_file}"
            ) from e

        from dlt_filesystem.source.adapter import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                hints=blob_hints(parsed_uri, table),
                # TODO: Can `column_types` be looped into reader hints instead?
                column_types=kwargs.get("column_types"),
            )
        )
