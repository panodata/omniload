"""Source connector for ``rsync+ssh://`` and ``rsync://`` (daemon, TCP 873).

The remote path/glob is supplied via ``--source-table``; when empty the URI
path is the fallback.  A ``#format`` / ``#key=value`` fragment on the table
selects the reader and passes reader hints, as with ``file://``.
"""

from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import resource_for_reader
from dlt_filesystem.source.error import UnsupportedEndpointError
from dlt_filesystem.source.format.registry import supported_file_format_message
from dlt_filesystem.source.model import FilesystemReference
from dlt_filesystem.source.router import determine_endpoint, parse_fragment
from omniload.source.rsync.command import CommandRunner, SubprocessRunner
from omniload.source.rsync.config import RsyncConfig, query_params
from omniload.source.rsync.error import InvalidRsyncUriError
from omniload.source.rsync.stager import FileSelection, RsyncStager
from omniload.source.rsync.transport import transport_for


class RsyncSource(FilesystemSource):
    """Stage remote files via oc-rsync, then read through the filesystem reader.

    ``runner`` accepts a test double; production uses :class:`SubprocessRunner`.
    """

    def __init__(self, runner: Optional[CommandRunner] = None) -> None:
        self._runner: CommandRunner = runner or SubprocessRunner()

    def dlt_source(self, uri: str, table: str, **kwargs):
        """Stage remote files via oc-rsync and return a dlt reader resource."""
        # Reject row-level incremental keys; this source uses file mtime.
        if kwargs.get("requested_incremental_key"):
            raise ValueError(
                "The rsync source manages incrementality through file "
                "modification time; you should not provide incremental_key"
            )

        params = query_params(uri)
        transport = transport_for(uri, params)
        config = RsyncConfig.from_params(params)

        remote_path, _, hints = parse_fragment(self._resolve_remote_path(uri, table))
        endpoint = self._resolve_endpoint(table, remote_path)
        selection = FileSelection.from_remote_path(remote_path)

        staging_dir = RsyncStager(transport, config, self._runner).stage(selection)

        from fsspec.implementations.arrow import ArrowFSWrapper
        from pyarrow.fs import LocalFileSystem

        fs = ArrowFSWrapper(LocalFileSystem())

        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=staging_dir,
                file_glob=selection.pattern,
                reader_name=endpoint,
                storage_namespace=transport.storage_namespace(),
                filesystem_incremental=kwargs.get("filesystem_incremental", False),
                hints=hints,
                column_types=kwargs.get("column_types"),
            )
        )

    @staticmethod
    def _resolve_remote_path(uri: str, table: str) -> str:
        """Return the remote path/glob, preferring the table over the URI path."""
        spec = table.strip()
        if spec:
            return spec
        path = urlparse(uri).path.strip()
        if path:
            return path
        raise InvalidRsyncUriError(
            "No remote path supplied; pass it via --source-table "
            "(e.g. 'module/path/*.csv') or in the source URI path"
        )

    @staticmethod
    def _resolve_endpoint(table: str, remote_path: str) -> str:
        """Resolve the reader for the selection, or raise a formats message."""
        try:
            return determine_endpoint(table, remote_path)
        except (UnsupportedEndpointError, ValueError):
            raise ValueError(supported_file_format_message("rsync")) from None
