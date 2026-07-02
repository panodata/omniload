import os
from typing import Tuple

from omniload.error import MissingValueError
from omniload.util.endpoint import (
    UnsupportedEndpointError,
    determine_endpoint,
    split_format_hint,
    supported_file_format_message,
)

_GLOB_CHARS = "*?["


class LocalFilesystemSource:
    """Read local CSV / JSONL / Parquet files through the shared filesystem readers.

    Everything after ``file://`` is treated as a filesystem path, never an RFC-8089
    host, matching how ``csv://`` and ``mmap://`` already work. This keeps the
    reporter's two-slash relative form (``file://dir/x.csv`` -> ``<cwd>/dir/x.csv``)
    working, while ``file:///abs/x.csv`` resolves to an absolute path. See #106 for
    the URI-semantics discussion.
    """

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        # The shared filesystem adapter deliberately disables per-file incremental
        # loading, so a user-supplied incremental key can't be honoured. run_ingest()
        # nulls kwargs["incremental_key"] before calling us (because
        # handles_incrementality() is True), so check requested_incremental_key, which
        # preserves what the user actually asked for.
        if kwargs.get("requested_incremental_key"):
            raise ValueError(
                "Local file source takes care of incrementality on its own, "
                "you should not provide incremental_key"
            )

        # Everything after file:// is the path spec; fall back to the source table for
        # the blob/sftp-style split form (--source-uri file:// --source-table x.csv).
        spec = uri.split("://", 1)[1] if "://" in uri else uri
        spec = spec.strip()
        if not spec:
            spec = table.strip()
        if not spec:
            raise MissingValueError("path", "file URI")

        # Strip a trailing #format hint before splitting into dir/glob, so
        # file://feed.dat#csv globs feed.dat (not feed.dat#csv). Literal '#' in a path
        # is preserved by split_format_hint when the suffix isn't a known format.
        path, _ = split_format_hint(spec)

        # Resolve the reader from the original hinted string (determine_endpoint re-runs
        # split_format_hint internally and falls back to the file extension). It wraps an
        # unrecognized extension into a plain ValueError, so catch both that and the
        # underlying UnsupportedEndpointError and surface the supported-format list.
        try:
            endpoint = determine_endpoint(spec, path)
        except (UnsupportedEndpointError, ValueError):
            raise ValueError(supported_file_format_message("Local file")) from None

        abspath = os.path.abspath(path)
        directory, file_glob = self._split_dir_glob(abspath)

        import fsspec

        fs = fsspec.filesystem("file")

        from omniload.source.filesystem.adapter import resource_for_reader

        return resource_for_reader(
            f"file://{directory}", fs, file_glob, endpoint, kwargs.get("column_types")
        )

    @staticmethod
    def _split_dir_glob(abspath: str) -> Tuple[str, str]:
        """Split an absolute path into a (directory, glob) pair for the readers adapter.

        The readers treat the bucket_url as a directory and the third argument as a
        filename/glob relative to it. When the path contains a glob pattern, split at
        the first path segment that carries a glob char so a recursive pattern like
        ``/abs/data/**/*.csv`` becomes ``("/abs/data", "**/*.csv")``. A plain file
        becomes ``(dirname, basename)``.
        """
        parts = abspath.split(os.sep)
        for i, part in enumerate(parts):
            if any(c in part for c in _GLOB_CHARS):
                directory = os.sep.join(parts[:i]) or os.sep
                file_glob = os.sep.join(parts[i:])
                return directory, file_glob
        return os.path.dirname(abspath), os.path.basename(abspath)
