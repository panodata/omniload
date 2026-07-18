from dlt_filesystem.source.impl.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import (
    AzureSource,
    DropboxSource,
    GCSSource,
    HDFSSource,
    OCISource,
    OSSSource,
    R2Source,
    S3Source,
    SFTPSource,
    WebdavSource,
)

__all__ = [
    "AzureSource",
    "DropboxSource",
    "GCSSource",
    "HDFSSource",
    "LocalFilesystemSource",
    "OCISource",
    "OSSSource",
    "R2Source",
    "S3Source",
    "SFTPSource",
    "WebdavSource",
]
