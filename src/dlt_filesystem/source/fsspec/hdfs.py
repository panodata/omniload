from typing import Any, Dict, Optional, Type

from fsspec import AbstractFileSystem
from fsspec.implementations.arrow import HadoopFileSystem

from dlt_filesystem.error import MissingConnectorOption
from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator, ResourceOptions
from dlt_filesystem.util.fsspec import ReadIntoArrowFSMixin
from dlt_filesystem.util.python import apply_alias, cast_to_dict, cast_to_int


class ReadIntoHadoopFileSystem(ReadIntoArrowFSMixin, HadoopFileSystem):
    """HDFS through Arrow, with `readinto` on its read handles.

    Defined at module scope so the class stays addressable by import path: fsspec
    filesystems are picklable by design, and a class built inside a function is not.
    Importing `fsspec.implementations.arrow` costs nothing here, it defers pyarrow to
    the constructor.
    """


class HDFSSource(FilesystemSource):
    """
    Access files on HDFS via Arrow.
    https://arrow.apache.org/docs/python/generated/pyarrow.fs.HadoopFileSystem.html
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        return ReadIntoHadoopFileSystem

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
            name="HDFS", fs_class=self.fs_class, uri=uri, path=table, default_port=8020
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
        apply_alias(fs_kwargs, "block_size", "default_block_size")
        cast_to_int(
            fs_kwargs, ["port", "replication", "buffer_size", "default_block_size"]
        )
        cast_to_dict(fs_kwargs, ["extra_conf"])

        # Sanity checks.
        if "host" not in fs_kwargs or not fs_kwargs["host"]:
            raise MissingConnectorOption("host", "HDFS")

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator, options=resource_options)
