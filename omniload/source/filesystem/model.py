from typing import Optional, Type, Union

from dlt.common.configuration import configspec, resolve_type
from dlt.common.configuration.specs import CredentialsConfiguration
from dlt.common.storages import FilesystemConfiguration
from dlt.common.storages.configuration import FileSystemCredentials
from fsspec import AbstractFileSystem

DEFAULT_CHUNK_SIZE = 5_000


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
