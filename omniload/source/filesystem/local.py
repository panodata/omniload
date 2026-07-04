import os
from typing import Tuple

from omniload.error import MissingValueError
from omniload.source.filesystem.error import UnsupportedEndpointError
from omniload.source.filesystem.format.registry import supported_file_format_message
from omniload.source.filesystem.router import (
    determine_endpoint,
    split_format_hint,
)

_GLOB_CHARS = "*?["


def _is_windows_drive(path: str) -> bool:
    """True for a path that starts with a Windows drive letter, e.g. ``C:/data``."""
    return len(path) >= 2 and path[0].isalpha() and path[1] == ":"


def _url_path_to_local(spec: str) -> str:
    """Turn the path portion of a ``file://`` URL into a slash-normalized local path.

    Everything after ``file://`` is treated as a path, never a host, so the two-slash
    relative form (``file://dir/x.csv``) keeps working. On top of that this recognizes
    the platform-independent absolute forms:

    - ``//server/share/...`` (from ``file:////server/share/...`` or backslash UNC input)
      -> a UNC path, kept as-is.
    - ``/C:/...`` (from ``file:///C:/...``) -> the leading slash before the drive letter
      is dropped, giving ``C:/...``.

    Backslashes are normalized to forward slashes so Windows separators survive the trip
    through fsspec (a literal backslash in a POSIX filename is not supported, matching the
    other filesystem sources).
    """
    s = spec.replace("\\", "/")
    if s.startswith("//"):
        return s  # UNC: //server/share/...
    if len(s) >= 3 and s[0] == "/" and _is_windows_drive(s[1:]):
        return s[1:]  # /C:/... -> C:/...
    return s


def _is_absolute_local(path: str) -> bool:
    """True for POSIX-absolute, UNC, or Windows drive-letter paths (any OS)."""
    return path.startswith("/") or _is_windows_drive(path)


class LocalFilesystemSource:
    """Read local CSV / JSONL / Parquet files through the shared filesystem readers.

    Everything after ``file://`` is treated as a filesystem path, never an RFC-8089
    host, matching how ``csv://`` and ``mmap://`` already work. This keeps the
    reporter's two-slash relative form (``file://dir/x.csv`` -> ``<cwd>/dir/x.csv``)
    working. Absolute forms are recognized across platforms:

    - ``file:///abs/x.csv`` -> ``/abs/x.csv`` (POSIX)
    - ``file:///C:/x.csv`` or ``file://C:/x.csv`` -> ``C:/x.csv`` (Windows drive)
    - ``file:////server/share/x.csv`` or ``file://\\\\server\\share\\x.csv``
      -> ``//server/share/x.csv`` (UNC)

    See #106 for the URI-semantics discussion.
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

        local = _url_path_to_local(path)
        if not _is_absolute_local(local):
            # Relative to the working directory; normalize separators so the split below
            # and dlt's path handling are slash-delimited on Windows too.
            local = os.path.abspath(local).replace(os.sep, "/")

        directory, file_glob = self._split_dir_glob(local)

        import fsspec

        fs = fsspec.filesystem("file")

        from omniload.source.filesystem.adapter import resource_for_reader

        # Pass the plain absolute directory (not a hand-built file:// URL). dlt's
        # glob_files routes a local path through make_file_url/make_local_path, which is
        # documented to handle POSIX, Windows drive-letter and UNC paths correctly, so we
        # inherit that instead of reconstructing a file:// URL ourselves (a naive
        # "file://" + "C:/dir" parses the drive as a URL host and reads nothing).
        return resource_for_reader(
            directory, fs, file_glob, endpoint, kwargs.get("column_types")
        )

    @staticmethod
    def _split_dir_glob(path: str) -> Tuple[str, str]:
        """Split a slash-normalized path into a (directory, glob) pair for the readers.

        The readers treat the bucket_url as a directory and the third argument as a
        filename/glob relative to it. When the path contains a glob pattern, split at
        the first segment that carries a glob char so a recursive pattern like
        ``/abs/data/**/*.csv`` becomes ``("/abs/data", "**/*.csv")``. A plain file
        becomes ``(dirname, basename)``. Splitting is always on ``/`` (separators were
        normalized upstream), so UNC (``//server/share``) and drive (``C:/…``) paths
        split correctly on any platform.
        """
        parts = path.split("/")
        for i, part in enumerate(parts):
            if any(c in part for c in _GLOB_CHARS):
                directory = "/".join(parts[:i]) or "/"
                file_glob = "/".join(parts[i:])
                return directory, file_glob
        directory, _, basename = path.rpartition("/")
        return (directory or "/"), basename
