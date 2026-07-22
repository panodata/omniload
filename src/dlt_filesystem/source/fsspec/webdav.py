from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import infer_resource
from dlt_filesystem.source.model import FilesystemLocator
from dlt_filesystem.util.python import asbool, cast_to_bool, cast_to_dict, cast_to_int


class WebdavSource(FilesystemSource):
    """
    Access files on WebDAV.

    https://skshetry.github.io/webdav4/
    https://en.wikipedia.org/wiki/WebDAV
    """

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        from webdav4.fsspec import WebdavFileSystem

        return WebdavFileSystem

    def dlt_source(self, uri: str, table: str, **kwargs):

        # This adjustment is specific to WebDAV.
        # omniload uses the `https+webdav://`, but fsspec uses `https://`.
        uri = uri.replace("+webdav", "").replace("+dav", "")

        # TODO: Is this applicable for Dropbox and friends at all?
        if kwargs.get("incremental_key"):
            raise ValueError(
                "WebDAV takes care of incrementality on its own, you should not provide incremental_key"
            )

        # Bundle essential information to infer filesystem wrapper.
        locator = FilesystemLocator(
            name="WebDAV", fs_class=self.fs_class, uri=uri, path=table
        )

        # Decode individual arguments.
        fs_kwargs = locator.options.fs_kwargs
        fs_kwargs.update(kwargs)
        cast_to_bool(fs_kwargs, ["retry", "trust_env"])
        cast_to_dict(fs_kwargs, ["headers", "cookies", "proxies"])
        cast_to_int(fs_kwargs, ["chunk_size", "max_redirects"])
        if "verify" in fs_kwargs:
            try:
                fs_kwargs["verify"] = asbool(fs_kwargs["verify"])
            except ValueError:
                pass

        # Extract authentication credentials.
        auth = None
        if "username" in fs_kwargs:
            auth = (fs_kwargs.pop("username"), fs_kwargs.pop("password", None))

        # Downstream implementation does not accept this kwarg.
        fs_kwargs.pop("host", None)

        # Create filesystem and dlt resource wrapper.
        fs = self.fs_class(base_url=uri, auth=auth, **fs_kwargs)
        return infer_resource(fs=fs, locator=locator)
