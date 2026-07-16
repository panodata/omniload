from omniload.source.filesystem.impl.local import LocalFilesystemSource
from omniload.source.filesystem.impl.remote import (
    AzureSource,
    GCSSource,
    S3Source,
    SFTPSource,
)

__all__ = [
    "AzureSource",
    "GCSSource",
    "LocalFilesystemSource",
    "S3Source",
    "SFTPSource",
]
