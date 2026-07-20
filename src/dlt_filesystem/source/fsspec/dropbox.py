from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator


class DropboxSource(FilesystemSource):
    """
    Access files on Dropbox.

    https://github.com/fsspec/dropboxdrivefs
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from dropboxdrivefs import DropboxDriveFileSystem

        DropboxDriveFileSystem._get_kwargs_from_urls = self._get_kwargs_from_urls  # ty: ignore[invalid-assignment]

        return DropboxDriveFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Dropbox takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="Dropbox", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)

        # Create filesystem wrapper.
        fs = self.fs_class(**fs_kwargs)

        # Attach canonical URL form. It is currently required, but why?
        # TODO: Review why the URL must be partly reconstructed
        #       across the board of all filesystem wrappers?
        bucket_url = f"dropbox://{locator.bucket_name}/"
        locator.baseurl = bucket_url

        return infer_resource(fs=fs, locator=locator)
