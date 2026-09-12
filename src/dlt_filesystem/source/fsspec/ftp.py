from typing import Any, Dict, Optional

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator, ResourceOptions
from dlt_filesystem.util.python import asbool, cast_to_int


class FTPSource(FilesystemSource):
    """Access files on FTP servers."""

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

        from fsspec.implementations.ftp import FTPFileSystem

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="FTP", fs_class=FTPFileSystem, uri=uri, path=table, default_port=21
        )

        # Decode individual options (type casting, default values, sanity checks).
        resource_options = ResourceOptions(
            filesystem_incremental=filesystem_incremental,
            column_types=column_types,
            reader_hints=reader_hints,
        )
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        fs_kwargs["port"] = fs_kwargs.get("port", locator.default_port)
        # Cast values to `int`.
        cast_to_int(fs_kwargs, ["block_size", "port", "timeout"])
        # Type casting for special parameters.
        if "tls" in fs_kwargs:
            try:
                fs_kwargs["tls"] = asbool(fs_kwargs["tls"])
            except ValueError:
                pass

        # Sanity checks.
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "FTP")

        # Create filesystem and dlt resource wrapper.
        fs = FTPFileSystem(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator, options=resource_options)
