from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator
from dlt_filesystem.util.python import apply_alias, cast_to_dict, cast_to_int


class OCISource(FilesystemSource):
    """
    Access files on Oracle Cloud Infrastructure Object Storage (OCI).

    https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm
    https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from ocifs import OCIFileSystem

        return OCIFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        if kwargs.get("incremental_key"):
            raise ValueError(
                "OCI takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="OCI", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        # {"default_block_size": int, "config": dict, "config_kwargs": dict, "oci_additional_kwargs": dict}
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)

        # Decode dict-typed `config`, `config_kwargs`, `oci_additional_kwargs` from JSON.
        cast_to_dict(fs_kwargs, ["config", "config_kwargs", "oci_additional_kwargs"])
        # The `default_` prefix seems unnecessary. Let's make it optional by using an alias.
        apply_alias(fs_kwargs, "block_size", "default_block_size")
        # Convert to integers.
        cast_to_int(fs_kwargs, ["default_block_size"])

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator)
