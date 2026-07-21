from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator


class WebdavSource(FilesystemSource):
    """
    Access files on WebDAV.

    https://skshetry.github.io/webdav4/
    https://en.wikipedia.org/wiki/WebDAV
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from webdav4.fsspec import WebdavFileSystem

        WebdavFileSystem._get_kwargs_from_urls = self._get_kwargs_from_urls  # ty: ignore[invalid-assignment]

        return WebdavFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "WebDAV takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="WebDAV", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual options (type casting, default values, sanity checks). Schema:
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        auth = None
        if "username" in fs_kwargs:
            auth = (fs_kwargs["username"], fs_kwargs.get("password"))

        # Create filesystem wrapper.
        fs = self.fs_class(base_url=uri, auth=auth)

        # Attach canonical URL form. It is currently required, but why?
        locator.baseurl = uri

        return infer_resource(fs=fs, locator=locator)
