import os

from omniload.error import MissingValueError
from omniload.source.filesystem.base import FilesystemSource
from omniload.source.filesystem.error import UnsupportedEndpointError
from omniload.source.filesystem.format.registry import supported_file_format_message
from omniload.source.filesystem.impl.util import (
    _is_absolute_local,
    _split_dir_glob,
    _url_path_to_local,
)
from omniload.source.filesystem.router import (
    determine_endpoint,
    parse_fragment,
)


class LocalFilesystemSource(FilesystemSource):
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

        # Strip the trailing #fragment (format hint and/or #key=value reader
        # hints) before splitting into dir/glob, so file://feed.dat#csv and
        # file://book.xlsx#sheet_name=foo glob the bare path. Literal '#' in a path is
        # preserved by parse_fragment when the fragment isn't a valid directive.
        path, _, hints = parse_fragment(spec)

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

        directory, file_glob = _split_dir_glob(local)

        import fsspec

        fs = fsspec.filesystem("file")

        from omniload.source.filesystem.adapter import resource_for_reader
        from omniload.source.filesystem.model import FilesystemReference

        # Pass the plain absolute directory (not a hand-built file:// URL). dlt's
        # glob_files routes a local path through make_file_url/make_local_path, which is
        # documented to handle POSIX, Windows drive-letter and UNC paths correctly, so we
        # inherit that instead of reconstructing a file:// URL ourselves (a naive
        # "file://" + "C:/dir" parses the drive as a URL host and reads nothing).
        return resource_for_reader(
            FilesystemReference(
                fs=fs,
                bucket_url=directory,
                file_glob=file_glob,
                reader_name=endpoint,
                hints=hints,
                column_types=kwargs.get("column_types"),
            )
        )
