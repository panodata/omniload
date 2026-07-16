from dlt_filesystem.target.local import LocalFilesystemDestination
from dlt_filesystem.target.remote import (
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
