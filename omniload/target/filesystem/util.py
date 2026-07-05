import os

from omniload.error import MissingValueError
from omniload.source.filesystem.format.registry import FORMAT_TO_READER
from omniload.source.filesystem.impl.util import _is_absolute_local, _url_path_to_local
from omniload.target.filesystem.registry import (
    WRITE_FORMATS,
    supported_write_format_message,
)

# Globbing is a read-only feature; a write target must name exactly one output file.
_GLOB_CHARS = "*?["


def _split_format_hint(spec: str) -> tuple[str, str | None]:
    """Split ``path#format`` into ``(path, format)``.

    A trailing ``#segment`` is an explicit format hint only when ``segment`` is a
    recognized format; otherwise the ``#`` is part of the path (so a literal ``#`` in a
    filename keeps working). Mirrors ``source.filesystem.router.split_format_hint`` but is
    reimplemented here to avoid importing that module's heavy dlt/fsspec dependencies just
    for a string split.
    """
    head, sep, suffix = spec.rpartition("#")
    if sep and suffix in FORMAT_TO_READER:
        return head, suffix
    return spec, None


def _resolve_output_target(dest_uri: str) -> tuple[str, str]:
    """Resolve a ``file://`` destination URI into ``(local_path, format)``.

    Format precedence matches the source: an explicit ``#format`` hint wins, otherwise it
    is inferred from the file extension. Unsupported/absent formats raise a ``ValueError``
    listing the supported set.
    """
    spec = dest_uri.split("://", 1)[1] if "://" in dest_uri else dest_uri
    spec = spec.strip()
    if not spec:
        raise MissingValueError("path", "file URI")

    path, hint = _split_format_hint(spec)
    if not path:
        # e.g. file://#csv, where the whole spec was consumed by the format hint.
        raise MissingValueError("path", "file URI")
    if any(char in path for char in _GLOB_CHARS):
        raise ValueError(
            "file:// destinations must name a single output file; "
            "globs (*, ?, []) are only supported when reading"
        )

    if hint:
        file_format = hint
    elif "." in path:
        file_format = path.rsplit(".", 1)[-1].lower()
    else:
        file_format = None
    if file_format not in WRITE_FORMATS:
        raise ValueError(supported_write_format_message(file_format))

    local = _url_path_to_local(path)
    if not _is_absolute_local(local):
        # Relative to the working directory; normalize separators so Windows drive paths
        # come back slash-delimited, matching the source side.
        local = os.path.abspath(local).replace(os.sep, "/")
    return local, file_format


def _strip_dlt_columns(row: dict) -> dict:
    """Drop dlt's bookkeeping columns (``_dlt_id``, ``_dlt_load_id``, ...)."""
    return {key: value for key, value in row.items() if not key.startswith("_dlt_")}
