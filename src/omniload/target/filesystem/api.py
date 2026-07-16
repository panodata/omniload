from omniload.target.filesystem.local import LocalFilesystemDestination
from omniload.target.filesystem.remote import (
    AzureDestination,
    GCSDestination,
    S3Destination,
)

__all__ = [
    "AzureDestination",
    "GCSDestination",
    "LocalFilesystemDestination",
    "S3Destination",
]
