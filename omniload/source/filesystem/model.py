from dataclasses import dataclass, field
from typing import Any, Optional, Type, Union

from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.storages import FilesystemConfiguration
from dlt.common.storages.configuration import FileSystemCredentials
from fsspec import AbstractFileSystem


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
class FilesystemReference:
    """Bundle the arguments needed by `resource_for_reader` to build a resource.

    Args:
        fs (AbstractFilesystem): fsspec filesystem instance.
        bucket_url (str): The url to the bucket.
        file_glob (str): The filter to apply to the files in glob format.
        reader_name (str): The name of the reader resource to build, e.g. `read_csv`.
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
    hints: dict[str, str] = field(default_factory=dict)
    column_types: Optional[dict[str, Any]] = None
