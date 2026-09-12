from typing import Any, Dict, Optional, Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator, ResourceOptions
from dlt_filesystem.util.python import (
    cast_to_bool,
    cast_to_int,
    cast_to_list,
)


class DatabricksSource(FilesystemSource):
    """
    Access files on Databricks Unity Catalog Volumes, Workspace files, and Legacy DBFS (Databricks File System).
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from fsspec_databricks import DatabricksFileSystem

        return DatabricksFileSystem

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
            name="Databricks",
            fs_class=self.fs_class,
            uri=uri,
            path=table,
            accept_no_bucket_name=True,
            accept_no_host_name=True,
        )

        # Decode individual options (type casting, default values, sanity checks).
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
                "debug_truncate_bytes",
                "volume_fs_max_read_concurrency",
                "volume_fs_min_read_block_size",
                "volume_fs_max_read_block_size",
                "volume_fs_max_write_concurrency",
                "volume_fs_min_write_block_size",
                "volume_fs_max_write_block_size",
                "volume_min_multipart_upload_size",
                "volume_fs_connection_pool_size",
            ],
        )
        cast_to_list(fs_kwargs, ["scopes"])
        cast_to_bool(
            fs_kwargs,
            [
                "debug_headers",
                "use_local_fs_in_workspace",
                "verbose_debug_log",
            ],
        )

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator, options=resource_options)
