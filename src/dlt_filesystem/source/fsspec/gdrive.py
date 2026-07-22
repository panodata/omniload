from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator
from dlt_filesystem.util.python import apply_alias, cast_to_bool, cast_to_dict


class GoogleDriveSource(FilesystemSource):
    """
    Access files on Google Drive.
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from gdrive_fsspec import GoogleDriveFileSystem

        return GoogleDriveFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: It looks like this is generic code that could be refactored already
        #       if it's common amongst different implementations.
        if kwargs.get("incremental_key"):
            raise ValueError(
                "GoogleDrive takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="GoogleDrive",
            fs_class=self.fs_class,
            uri=uri,
            path=table,
        )

        # Decode individual options (type casting, default values, sanity checks).
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        apply_alias(fs_kwargs, "auth", "auth_kwargs")
        cast_to_dict(fs_kwargs, ["creds", "auth_kwargs"])
        cast_to_bool(fs_kwargs, ["use_local_webserver"])

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(**fs_kwargs)
        return infer_resource(fs=fs, locator=locator)
