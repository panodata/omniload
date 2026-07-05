from omniload.target.filesystem.local import LocalFilesystemDestination
from omniload.target.filesystem.remote import GCSDestination, S3Destination

__all__ = [
    "GCSDestination",
    "LocalFilesystemDestination",
    "S3Destination",
]
