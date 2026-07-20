from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator
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

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: It looks like this is generic code that could be refactored already
        #       if it's common amongst different implementations. Breaking out the
        if kwargs.get("incremental_key"):
            raise ValueError(
                "OSS takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="OSS", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks).
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

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"oss://{locator.bucket_name}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)
