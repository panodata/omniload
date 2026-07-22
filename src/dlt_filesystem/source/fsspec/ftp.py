from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator
from dlt_filesystem.util.python import asbool, cast_to_int


class FTPSource(FilesystemSource):
    """Access files on FTP servers."""

    def dlt_source(self, uri: str, table: str, **kwargs):

        from fsspec.implementations.ftp import FTPFileSystem

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="FTP", fs_class=FTPFileSystem, uri=uri, path=table, default_port=21
        )

        # Decode individual options (type casting, default values, sanity checks).
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "FTP")
        fs_kwargs["port"] = fs_kwargs.get("port", locator.default_port)
        # Cast values to `int`.
        cast_to_int(fs_kwargs, ["block_size", "port", "timeout"])
        # Type casting for special parameters.
        if "tls" in fs_kwargs:
            try:
                fs_kwargs["tls"] = asbool(fs_kwargs["tls"])
            except ValueError:
                pass

        # Create filesystem and dlt resource wrapper.
        fs = FTPFileSystem(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator)
