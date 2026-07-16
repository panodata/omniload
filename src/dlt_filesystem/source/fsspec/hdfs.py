from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator
from dlt_filesystem.util.python import apply_alias, cast_to_dict, cast_to_int


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
            name="HDFS", fs_class=self.fs_class, uri=uri, path=table, default_port=8020
        )

        # Decode individual options (type casting, default values, sanity checks).
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "HDFS")
        fs_kwargs["port"] = fs_kwargs.get("port", locator.default_port)
        apply_alias(fs_kwargs, "block_size", "default_block_size")
        cast_to_int(
            fs_kwargs, ["port", "replication", "buffer_size", "default_block_size"]
        )
        cast_to_dict(fs_kwargs, ["extra_conf"])

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator)
