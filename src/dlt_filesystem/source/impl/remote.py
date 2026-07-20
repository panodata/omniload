import base64
import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Type
from urllib.parse import parse_qs, urlparse

from sqlalchemy.util import asbool

from dlt_filesystem.error import InvalidBlobTableError, MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.error import UnsupportedEndpointError
from dlt_filesystem.source.format.registry import supported_file_format_message
from dlt_filesystem.source.model import FilesystemLocator
from dlt_filesystem.source.router import (
    blob_hints,
    determine_endpoint,
    parse_fragment,
    parse_uri,
)
from dlt_filesystem.source.util import (
    apply_alias,
    cast_to_bool,
    cast_to_dict,
    cast_to_int,
)
from dlt_filesystem.util.auth import AzureBlobAuth, parse_azure_blob_auth

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem


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
        if isinstance(self.fs_class.protocol, (list, tuple)):
            return self.fs_class.protocol[0]
        return self.fs_class.protocol

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
                    f"azure:{auth.account_name.lower()}:"
                    f"{_endpoint_namespace(auth.account_host, 'azure-public')}"
                ),
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=blob_hints(parsed_uri, table),
                column_types=kwargs.get("column_types"),
            )
        )


class FTPSource(FilesystemSource):
    """Access files on FTP servers."""

    def dlt_source(self, uri: str, table: str, **kwargs):

        from fsspec.implementations.ftp import FTPFileSystem

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="FTP", fs_class=FTPFileSystem, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks).
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "FTP")
        fs_kwargs["port"] = fs_kwargs.get("port", 21)
        # Cast values to `int`.
        cast_to_int(fs_kwargs, ["block_size", "port", "timeout"])
        # Type casting for special parameters.
        if "tls" in fs_kwargs:
            try:
                fs_kwargs["tls"] = asbool(fs_kwargs["tls"])
            except ValueError:
                pass

        # Create filesystem wrapper.
        fs = FTPFileSystem(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"ftp://{fs_kwargs['host']}:{fs_kwargs['port']}"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class SFTPSource(FilesystemSource):
    """Access files on SFTP servers."""

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

        bucket_name, path_to_file = parse_uri(parsed_uri, table)
        if not bucket_name or not path_to_file:
            raise InvalidBlobTableError("WebDAV")

        bucket_url = f"sftp://{host}:{port}"

        _, _, hints = parse_fragment(table)

        try:
            endpoint = determine_endpoint(table, path_to_file)
        except UnsupportedEndpointError:
            raise ValueError(supported_file_format_message("SFTP")) from None
        except Exception as e:
            raise ValueError(f"Failed to parse endpoint from path: {table}") from e

        from dlt_filesystem.source.core import resource_for_reader
        from dlt_filesystem.source.model import FilesystemReference

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=bucket_url,
                file_glob=path_to_file,
                reader_name=endpoint,
                storage_namespace=(f"sftp:{host.lower()}:{port}:{username or ''}"),
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=hints,
                column_types=kwargs.get("column_types"),
            )
        )


class HDFSSource(FilesystemSource):
    """
    Access files on HDFS via Arrow.
    https://arrow.apache.org/docs/python/generated/pyarrow.fs.HadoopFileSystem.html
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from fsspec.implementations.arrow import HadoopFileSystem

        return HadoopFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "HDFS takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="HDFS", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks).
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "FTP")
        fs_kwargs["port"] = fs_kwargs.get("port", 8020)
        apply_alias(fs_kwargs, "block_size", "default_block_size")
        cast_to_int(
            fs_kwargs, ["port", "replication", "buffer_size", "default_block_size"]
        )
        cast_to_dict(fs_kwargs, ["extra_conf"])

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"hdfs://{fs_kwargs['host']}:{fs_kwargs['port']}"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class R2Source(S3CompatibleSource):
    """
    Access files on Cloudflare R2.

    R2 is compatible with Amazon S3.
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
    Access files on Alibaba Cloud Object Storage Service (OSS).
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

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="OSS", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks).
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        apply_alias(fs_kwargs, "block_size", "default_block_size")
        apply_alias(fs_kwargs, "cache_type", "default_cache_type")
        cast_to_int(fs_kwargs, ["default_block_size"])

        # TODO: BaseOSSFileSystem accepts `default_cache_type` as a `str` type with
        #       a choice of different values. The default value is `readahead`, and
        #       setting `none` is possible. For all other values, the inline
        #       documentation refers to the `fsspec` documentation. Let's harvest
        #       relevant details and add them to the parameter data model.
        # No demo implementation here.

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"oss://{locator.bucket_name}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class OCISource(FilesystemSource):
    """
    Access files on Oracle Cloud Infrastructure Object Storage (OCI).

    https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm
    https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from ocifs import OCIFileSystem

        OCIFileSystem._get_kwargs_from_urls = self._get_kwargs_from_urls  # ty: ignore[invalid-assignment]

        return OCIFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        if kwargs.get("incremental_key"):
            raise ValueError(
                "OCI takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="OCI", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        # {"default_block_size": int, "config": dict, "config_kwargs": dict, "oci_additional_kwargs": dict}
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)

        # Decode dict-typed `config`, `config_kwargs`, `oci_additional_kwargs` from JSON.
        cast_to_dict(fs_kwargs, ["config", "config_kwargs", "oci_additional_kwargs"])
        # The `default_` prefix seems unnecessary. Let's make it optional by using an alias.
        apply_alias(fs_kwargs, "block_size", "default_block_size")
        # Convert to integers.
        cast_to_int(fs_kwargs, ["default_block_size"])

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"oci://{locator.bucket_name}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class DropboxSource(FilesystemSource):
    """
    Access files on Dropbox.

    https://github.com/fsspec/dropboxdrivefs
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from dropboxdrivefs import DropboxDriveFileSystem

        DropboxDriveFileSystem._get_kwargs_from_urls = self._get_kwargs_from_urls  # ty: ignore[invalid-assignment]

        return DropboxDriveFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Dropbox takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="Dropbox", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"dropbox://{locator.bucket_name}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class WebdavSource(FilesystemSource):
    """
    Access files on WebDAV.

    https://skshetry.github.io/webdav4/
    https://en.wikipedia.org/wiki/WebDAV
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from webdav4.fsspec import WebdavFileSystem

        WebdavFileSystem._get_kwargs_from_urls = self._get_kwargs_from_urls  # ty: ignore[invalid-assignment]

        return WebdavFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "WebDAV takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="WebDAV", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        auth = None
        if "username" in fs_kwargs:
            auth = (fs_kwargs["username"], fs_kwargs.get("password"))

        # Create filesystem wrapper.
        bucket_url = f"webdav://{locator.bucket_name}/"
        fs = self.fs_class(bucket_url, auth=auth)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class SharePointOneDriveSource(FilesystemSource):
    """
    Access files on Microsoft SharePoint and OneDrive.

    https://github.com/acsone/msgraphfs
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from msgraphfs import MSGDriveFS

        MSGDriveFS._get_kwargs_from_urls = self._get_kwargs_from_urls  # ty: ignore[invalid-assignment]

        return MSGDriveFS

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "MSSharePointOneDrive takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="MSSharePointOneDrive", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        cast_to_dict(fs_kwargs, ["oauth2_client_params"])
        cast_to_bool(fs_kwargs, ["use_recycle_bin"])

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"msgd://{locator.bucket_name}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)


class SMBSource(FilesystemSource):
    """
    Access files on Microsoft Windows Server Shares.

    https://en.wikipedia.org/wiki/Server_Message_Block
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from fsspec.implementations.smb import SMBFileSystem

        return SMBFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "SMB takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="SMB", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        cast_to_int(
            fs_kwargs,
            [
                "port",
                "timeout",
                "register_session_retries",
                "register_session_retry_wait",
                "register_session_retry_factor",
            ],
        )
        cast_to_bool(fs_kwargs, ["encrypt", "auto_mkdir"])

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"smb://{locator.baseurl}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)
