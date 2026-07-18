from dlt_filesystem.source.impl.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import (
    AzureSource,
    GCSSource,
    HDFSSource,
    OCISource,
    OSSSource,
    R2Source,
    S3Source,
    SFTPSource,
)

__all__ = [
    "AzureSource",
    "GCSSource",
    "HDFSSource",
    "LocalFilesystemSource",
    "OCISource",
    "OSSSource",
    "R2Source",
    "S3Source",
    "SFTPSource",
]
