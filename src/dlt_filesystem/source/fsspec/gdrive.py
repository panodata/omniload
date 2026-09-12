from typing import Any, Dict, Optional, Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator, ResourceOptions
from dlt_filesystem.util.python import apply_alias, cast_to_bool, cast_to_dict


class GoogleDriveSource(FilesystemSource):
    """
    Access files on Google Drive.
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from gdrive_fsspec import GoogleDriveFileSystem

        return GoogleDriveFileSystem

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
            name="GoogleDrive",
            fs_class=self.fs_class,
            uri=uri,
            path=table,
        )

        # Decode individual options (type casting, default values, sanity checks).
        resource_options = ResourceOptions(
            filesystem_incremental=filesystem_incremental,
            column_types=column_types,
            reader_hints=reader_hints,
        )
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        apply_alias(fs_kwargs, "auth", "auth_kwargs")
        cast_to_dict(fs_kwargs, ["creds", "auth_kwargs"])
        if "auth_kwargs" in fs_kwargs:
            cast_to_bool(fs_kwargs["auth_kwargs"], ["use_local_webserver"])

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator, options=resource_options)
