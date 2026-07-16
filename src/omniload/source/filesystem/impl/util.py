from typing import Tuple

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
