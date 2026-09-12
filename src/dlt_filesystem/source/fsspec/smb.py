from typing import Any, Dict, Optional, Type

from fsspec import AbstractFileSystem

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator, ResourceOptions
from dlt_filesystem.util.python import cast_to_bool, cast_to_int


class SMBSource(FilesystemSource):
    """
    Access files on Microsoft Windows Server Shares.

    https://en.wikipedia.org/wiki/Server_Message_Block
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from fsspec.implementations.smb import SMBFileSystem

        return SMBFileSystem

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

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="SMB", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        resource_options = ResourceOptions(
            filesystem_incremental=filesystem_incremental,
            column_types=column_types,
            reader_hints=reader_hints,
        )
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

        # Sanity checks.
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "SMB")

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator, options=resource_options)
