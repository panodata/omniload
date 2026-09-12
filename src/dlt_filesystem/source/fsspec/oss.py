from typing import Any, Dict, Optional, Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator, ResourceOptions
from dlt_filesystem.util.python import apply_alias, cast_to_int


class OSSSource(FilesystemSource):
    """
    Access files on Alibaba Cloud Object Storage Service (OSS).
    https://www.alibabacloud.com/en/product/object-storage-service
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        import ossfs

        return ossfs.OSSFileSystem

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
            name="OSS", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks).
        resource_options = ResourceOptions(
            filesystem_incremental=filesystem_incremental,
            column_types=column_types,
            reader_hints=reader_hints,
        )
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

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator, options=resource_options)
