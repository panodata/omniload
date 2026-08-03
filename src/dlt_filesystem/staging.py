"""Run-scoped materialization of remote objects for random-access consumers."""

from __future__ import annotations

import logging
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

from dlt_filesystem.util.auth import (
    azure_blob_filesystem_kwargs,
    gcs_filesystem_kwargs,
    parse_azure_blob_auth,
    s3_filesystem_kwargs,
)

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

logger = logging.getLogger(__name__)

# Source schemes that can be staged, mapped onto the backend that serves them.
# The alias schemes are the same ones the matching sources accept, so a URI that
# reads a file also stages an object.
STAGING_BACKENDS: dict[str, str] = {
    "abfss": "az",
    "adls": "az",
    "az": "az",
    "gs": "gs",
    "r2": "s3",
    "s3": "s3",
}
COPY_CHUNK_SIZE = 8 * 1024 * 1024


class RemoteObjectNotFoundError(FileNotFoundError):
    """A remote object selected for staging is missing or is not a file."""

    def __init__(self, location: str):
        super().__init__(f"Remote object not found: {location}")


class RemoteObjectSizeError(OSError):
    """A staged copy does not match the size reported before download."""


@dataclass(frozen=True)
class RemoteObject:
    """One object on a remote filesystem, with the storage options that reach it."""

    backend: str
    path: str
    safe_location: str
    storage_options: dict[str, list[str]] = field(repr=False)

    @classmethod
    def from_uri(cls, uri: str) -> RemoteObject:
        """Parse a filesystem URI that names one object.

        The URI is the same one the matching source reads files from: the path
        addresses the object and the query carries the storage options, so
        ``s3://bucket/path/object?access_key_id=...`` needs no second encoding
        layer.
        """
        parsed = urlsplit(uri)
        scheme = parsed.scheme.lower()
        backend = STAGING_BACKENDS.get(scheme)
        if backend is None:
            supported = ", ".join(sorted(STAGING_BACKENDS))
            raise ValueError(
                f"Unsupported storage scheme {scheme!r} for object staging; "
                f"expected one of: {supported}."
            )
        if parsed.username is not None or parsed.password is not None:
            raise ValueError(
                "Storage credentials must be supplied as URI query parameters, "
                "not embedded in the object URI."
            )
        if parsed.fragment:
            raise ValueError(
                "The object URI must not carry a '#' fragment; it names one "
                "object rather than selecting a reader."
            )
        if not parsed.netloc or not parsed.path.lstrip("/"):
            raise ValueError(
                "The object URI must identify one object as "
                "'<scheme>://<bucket-or-container>/<object>'."
            )

        safe_location = urlunsplit((scheme, parsed.netloc, parsed.path, "", ""))
        path = f"{unquote(parsed.netloc)}{unquote(parsed.path)}"
        return cls(
            backend=backend,
            path=path,
            safe_location=safe_location,
            storage_options=parse_qs(parsed.query),
        )


def _filesystem_for(remote: RemoteObject) -> AbstractFileSystem:
    """Construct the same authorized fsspec client as the matching source."""
    if remote.backend == "s3":
        from s3fs import S3FileSystem

        return S3FileSystem(**s3_filesystem_kwargs(remote.storage_options))
    if remote.backend == "gs":
        from gcsfs import GCSFileSystem

        return GCSFileSystem(**gcs_filesystem_kwargs(remote.storage_options))
    if remote.backend == "az":
        from adlfs import AzureBlobFileSystem

        auth = parse_azure_blob_auth(remote.storage_options)
        return AzureBlobFileSystem(**azure_blob_filesystem_kwargs(auth))
    raise AssertionError(f"Unhandled staging backend: {remote.backend}")


def _expected_size(info: dict[str, Any]) -> int | None:
    """Return fsspec's normalized object size when it is available."""
    value = info.get("size")
    if isinstance(value, int) and value >= 0:
        return value
    return None


@contextmanager
def materialize_remote_object(
    remote: RemoteObject,
    *,
    filename: str,
    staging_root: str | Path | None = None,
) -> Iterator[Path]:
    """Download one remote object and remove its run-scoped copy on every exit."""
    root = Path(staging_root) if staging_root is not None else None
    if root is not None:
        root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="omniload-staged-object-", dir=root
    ) as staging_dir:
        local_path = Path(staging_dir) / filename
        filesystem = _filesystem_for(remote)

        try:
            info = filesystem.info(remote.path)
        except FileNotFoundError as exc:
            raise RemoteObjectNotFoundError(remote.safe_location) from exc
        if info.get("type") == "directory":
            raise RemoteObjectNotFoundError(remote.safe_location)

        expected_size = _expected_size(info)
        if expected_size is not None:
            available = shutil.disk_usage(staging_dir).free
            if expected_size > available:
                raise OSError(
                    f"Not enough local disk space to stage {remote.safe_location}: "
                    f"need {expected_size} bytes, have {available} bytes."
                )

        logger.info(
            "Staging remote object %s (%s bytes)",
            remote.safe_location,
            expected_size if expected_size is not None else "unknown",
        )

        downloaded = 0
        try:
            with (
                filesystem.open(remote.path, "rb") as source,
                local_path.open("wb") as destination,
            ):
                while chunk := source.read(COPY_CHUNK_SIZE):
                    downloaded += len(chunk)
                    destination.write(chunk)
        except FileNotFoundError as exc:
            raise RemoteObjectNotFoundError(remote.safe_location) from exc

        if expected_size is not None and downloaded != expected_size:
            raise RemoteObjectSizeError(
                f"Remote object size changed while staging {remote.safe_location}: "
                f"expected {expected_size} bytes, downloaded {downloaded} bytes."
            )

        logger.info(
            "Staged remote object %s (%d bytes)",
            remote.safe_location,
            downloaded,
        )
        yield local_path
