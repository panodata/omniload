import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type, Union
from urllib.parse import parse_qs

from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.storages import FilesystemConfiguration
from dlt.common.storages.configuration import FileSystemCredentials
from fsspec import AbstractFileSystem

from dlt_filesystem.source.util import shrink_qs_dict


@configspec
class FilesystemConfigurationResource(FilesystemConfiguration):
    credentials: Optional[Union[FileSystemCredentials, AbstractFileSystem]] = None
    file_glob: Optional[str] = "*"
    files_per_page: int = 100
    extract_content: bool = False

    @resolve_type("credentials")
    def resolve_credentials_type(self) -> Type[CredentialsConfiguration]:
        # use known credentials or empty credentials for unknown protocol
        return Union[  # ty: ignore[invalid-return-type]
            self.PROTOCOL_CREDENTIALS.get(self.protocol)  # ty: ignore[invalid-type-form]
            or Optional[CredentialsConfiguration],
            AbstractFileSystem,
        ]


@dataclass
class FilesystemOptions:
    """Bundle filesystem options from URL."""

    options: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)

    @property
    def fs_kwargs(self) -> Dict[str, Any]:
        response = deepcopy(self.options)
        response.update(self.params)
        return response


@dataclass
class FilesystemLocator:
    """A full filesystem information locator."""

    name: str
    fs_class: Type[AbstractFileSystem]
    uri: str
    path: str
    baseurl: Optional[str] = None
    bucket_name: Optional[str] = None
    options: FilesystemOptions = field(default_factory=FilesystemOptions)

    def __post_init__(self):
        self.read_options()

    @property
    def hints(self) -> Dict[str, Any]:
        """
        Destructure reader or writer hints from URL fragment.

        Let's use the omniload approach of decoding
        URL fragments, because it handles a few edge cases, also taking
        the URL path into consideration.
        """
        from urllib.parse import urlparse

        from dlt_filesystem.source.router import blob_hints

        parsed_uri = urlparse(self.uri)
        return blob_hints(parsed_uri, self.path)

    def read_options(self) -> "FilesystemLocator":
        """
        Destructure input URL as a baseline for fsspec kwargs.

        Let's use the fsspec approach of decoding
        URIs, based on `fsspec.utils.infer_storage_options`.
        Invoking `_get_kwargs_from_urls` should happen before instantiation of the
        class; incoming paths then should be amended to strip the options in methods.
        """
        options = self.fs_class._get_kwargs_from_urls(self.uri)

        # URL query parameters.
        params = shrink_qs_dict(parse_qs(options.pop("url_query", "")))

        # Reader or writer hints.
        self.options = FilesystemOptions(
            options=options,
            params=params,
        )
        return self


@dataclass
class FilesystemReference:
    """
    Bundle the arguments needed by `resource_for_reader` to build a resource.

    Args:
        fs (AbstractFilesystem): fsspec filesystem instance.
        bucket_url (str): The url to the bucket.
        file_glob (str): The filter to apply to the files in glob format.
        reader_name (str): The name of the reader resource to build, e.g. `read_csv`.
        storage_namespace (str): Secret-free identity for the storage service or
            endpoint. Defaults to ``filesystem`` for callers that do not need to
            distinguish transports. The bucket URL and glob are added separately
            when deriving the incremental resource-state key.
        filesystem_incremental (bool): Whether to filter files using their
            modification time and persistent dlt resource state.
        hints (dict[str, str]): Free-form per-URI reader hints parsed from the
            `#key=value` fragment (e.g. `{"sheet_name": "ticker-symbols"}`). The
            key a reader looks up is that reader's contract; no reader consumes
            hints yet, so this is currently populated but unread.
        column_types (dict[str, Any], optional): Column name to type mapping, e.g. used by `read_csv_headless`.
    """

    fs: AbstractFileSystem
    bucket_url: str
    file_glob: str
    reader_name: str
    storage_namespace: str = "filesystem"
    filesystem_incremental: bool = False
    hints: dict[str, str] = field(default_factory=dict)
    column_types: Optional[dict[str, Any]] = None

    @property
    def incremental_resource_name(self) -> str:
        """Return a stable, secret-free resource name for this file selection."""
        identity = json.dumps(
            [self.storage_namespace, self.bucket_url, self.file_glob],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        return f"filesystem_{digest}"
