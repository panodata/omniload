from dlt_filesystem.source.impl.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import (
    AzureSource,
    DropboxSource,
    FTPSource,
    GCSSource,
    HDFSSource,
    OCISource,
    OSSSource,
    R2Source,
    S3Source,
    SFTPSource,
    SharePointOneDriveSource,
    WebdavSource,
)

__all__ = [
    "AzureSource",
    "DropboxSource",
    "FTPSource",
    "GCSSource",
    "HDFSSource",
    "LocalFilesystemSource",
    "OCISource",
    "OSSSource",
    "R2Source",
    "S3Source",
    "SFTPSource",
    "SharePointOneDriveSource",
    "WebdavSource",
]
