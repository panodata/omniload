from typing import Type

from fsspec import AbstractFileSystem

from dlt_filesystem.source.impl.remote import S3CompatibleSource


class R2Source(S3CompatibleSource):
    """
    Access files on Cloudflare R2.

    R2 is compatible with Amazon S3.
    https://github.com/panodata/omniload/issues/163
    """

    @property
    def fs_name(self) -> str:
        return "R2"

    @property
    def fs_class(self) -> Type["AbstractFileSystem"]:
        if hasattr(self, "_r2_fs_class"):
            return self._r2_fs_class
        import s3fs

        class R2FileSystem(s3fs.S3FileSystem):
            protocol = "r2"

        self._r2_fs_class = R2FileSystem
        return self._r2_fs_class
