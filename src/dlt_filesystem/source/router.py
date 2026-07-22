from typing import Any, Dict, Optional, Tuple, TypeAlias, Union
from urllib.parse import ParseResult, parse_qsl, urlparse

import dlt
from dlt.common.configuration import with_config
from dlt.common.storages import FilesystemConfiguration, fsspec_filesystem
from dlt.common.storages.configuration import FileSystemCredentials
from dlt.extract import DltResource
from fsspec import AbstractFileSystem

from dlt_filesystem.source.format.registry import (
    FORMAT_TO_READER,
    reader_for_format,
)

BucketName: TypeAlias = str
FileGlob: TypeAlias = str


def parse_uri(uri: Union[ParseResult, str], table: str) -> Tuple[BucketName, FileGlob]:
    """
    parse the URI of a blob storage and
    return the bucket name and the file glob.

    Supports the following Forms:
    - uri: "gs://"
      table: "bucket-name/file-glob"
    - uri: "gs://uri-bucket-name" (uri-bucket-name is preferred)
      table: "gs://table-bucket-name/file-glob"
    - uri: "gs://"
      table: "gs://bucket-name/file-glob"
    - uri: gs://bucket-name/file-glob
      table: None
    - uri: "gs://bucket-name"
      table: "file-glob"

    The first form is the preferred method. Other forms are supported but discouraged.
    """

    if isinstance(uri, str):
        uri = urlparse(uri)

    table = table.strip()
    host = uri.netloc.strip()

    # Form: scheme://bucket-name/file-glob
    # Note: This form was previously slated for deprecation.
    if table == "" or uri.path.strip() != "":
        return host, uri.path.lstrip("/")

    table_uri = urlparse(table)

    if host != "":
        return host, table_uri.path.lstrip("/")

    if table_uri.hostname:
        return table_uri.hostname, table_uri.path.lstrip("/")

    parts = table_uri.path.lstrip("/").split("/", maxsplit=1)
    if len(parts) != 2:
        return "", parts[0]

    return parts[0], parts[1]


def parse_endpoint(path: str) -> str:
    """
    Parse the endpoint kind from the URI.

    kind is a file format. one of [csv, jsonl, parquet]
    """
    file_extension = path.split(".")[-1]
    if file_extension == "gz":
        file_extension = path.split(".")[-2]
    return reader_for_format(file_extension)


def parse_fragment(spec: str) -> Tuple[str, Optional[str], Dict[str, str]]:
    """Split a filesystem-source spec into ``(path, format_hint, hints)``.

    The trailing ``#fragment`` (everything after the *last* ``#``) is a per-URI
    reader-directive channel. It may carry, in any combination:

    - a bare **format hint** -- a single known-format token (``#csv``); and/or
    - **named hints** -- ``key=value`` pairs (``#sheet_name=foo&header=0``), parsed
      with :func:`urllib.parse.parse_qsl`. That means ``=``-partition semantics
      (``#x=a=b`` -> ``{"x": "a=b"}``, not a split), percent-decoding of keys
      and values (``#sheet_name=My%20Sheet`` -> ``My Sheet``; and, form-encoding
      style, a literal ``+`` decodes to a space), and empty values are kept
      (``#sheet_name=`` -> ``{"sheet_name": ""}``; a reader decides whether that means
      "unset"). Duplicate keys are last-wins.

    Grammar of the fragment (``&``-separated segments):

    - at most one bare (no ``=``) segment, which must be a known format;
    - any number of ``key=value`` segments;
    - empty segments (from a trailing/doubled ``&``) are ignored.

    **All-or-nothing.** If any segment is neither a ``key=value`` pair nor a
    single known-format token -- an unknown bare token (``#sheet_name=foo&bad``), a
    second/duplicate bare format (``#csv&parquet``) -- the whole ``#...`` is
    treated as a *literal part of the path* and returned unchanged as
    ``(spec, None, {})``. A malformed tail never silently drops a valid hint,
    and a typo never gets silently absorbed as one. A fragment that interprets
    to nothing (a bare trailing ``#``) is likewise left literal.

    This preserves literal ``#`` in a path (``/feeds/vendor#1/data.csv``). The
    one narrowed case is a trailing segment that looks exactly like
    ``key=value`` (``vendor#x=y`` as the final segment), which now parses as a
    hint; percent-encode the ``#`` as ``%23`` to force it literal.
    """
    path, sep, fragment = spec.rpartition("#")
    if not sep:
        return spec, None, {}

    format_hint: Optional[str] = None
    hints: Dict[str, str] = {}
    for segment in fragment.split("&"):
        if segment == "":
            # Empty separator noise (trailing or doubled '&'); harmless.
            continue
        if "=" in segment:
            for key, value in parse_qsl(segment, keep_blank_values=True):
                hints[key] = value
        elif segment in FORMAT_TO_READER and format_hint is None:
            format_hint = segment
        else:
            # Unknown bare token, or a second/duplicate bare format: the '#' is
            # a literal part of the path, not a fragment.
            return spec, None, {}

    if format_hint is None and not hints:
        # Nothing to interpret (e.g. a bare trailing '#'); keep it literal.
        return spec, None, {}

    return path, format_hint, hints


def split_format_hint(table: str) -> Tuple[str, Optional[str]]:
    """Split a table spec into ``(path, format_hint)``.

    Thin wrapper over :func:`parse_fragment` that drops the named hints, so
    every existing caller keeps its ``(path, format_hint)`` contract. A trailing
    ``#segment`` is an explicit format hint only when ``segment`` is a
    recognized format; otherwise the ``#`` stays part of the path (literal
    ``#`` in file paths like ``/feeds/vendor#1/data.csv`` keep working).
    """
    path, format_hint, _ = parse_fragment(table)
    return path, format_hint


def blob_hints(parsed_uri: ParseResult, table: str) -> Dict[str, str]:
    """Extract ``#key=value`` reader hints for a blob (S3/GCS) source URI.

    Mirrors :func:`parse_uri`'s carrier choice so hints always track the file
    that is actually loaded. In the recommended form the fragment rides
    ``--source-table`` (``s3://?...`` + table ``bucket/book.xlsx#sheet_name=foo``); in
    the deprecated URI-path form (``s3://bucket/book.xlsx#sheet_name=foo``)
    :func:`urllib.parse.urlparse` strips it into ``parsed_uri.fragment``.
    ``parse_uri`` uses the URI-path form whenever ``parsed_uri.path`` is set (or
    ``table`` is empty), so the fragment is read from there in that case and only
    falls back to ``table`` for the pure table form -- otherwise a contradictory
    ``s3://bucket/a.csv#x`` + ``--source-table b.csv#y`` would attach ``b``'s
    hints to the ``a`` file that ``parse_uri`` actually loads. The bucket and
    glob still come from :func:`parse_uri`.
    """
    if parsed_uri.path.strip() or not table.strip():
        if parsed_uri.fragment:
            _, _, hints = parse_fragment(f"{parsed_uri.path}#{parsed_uri.fragment}")
            return hints
        return {}
    _, _, hints = parse_fragment(table)
    return hints


def determine_endpoint(table: str, path: str) -> str:
    """
    Find the designated reader method from either `table` or URL `path` component.
    """

    _, file_format = split_format_hint(table)
    if file_format is not None:
        return reader_for_format(file_format)

    try:
        return parse_endpoint(table)
    except Exception:
        try:
            return parse_endpoint(path)
        except Exception as e:
            raise ValueError(
                f"Failed to parse endpoint from table '{table}' or path '{path}'"
            ) from e


def fsspec_from_resource(filesystem_instance: DltResource) -> AbstractFileSystem:
    """Extract authorized fsspec client from a filesystem resource"""

    @with_config(
        spec=FilesystemConfiguration,
        sections=("sources", filesystem_instance.section, filesystem_instance.name),
    )
    def _get_fsspec(
        bucket_url: str, credentials: Optional[FileSystemCredentials]
    ) -> AbstractFileSystem:
        kwargs: Dict[str, Any] = {}
        if credentials is not None:
            kwargs["credentials"] = credentials
        return fsspec_filesystem(bucket_url, **kwargs)[0]

    return _get_fsspec(
        filesystem_instance.explicit_args.get("bucket_url", dlt.config.value),
        filesystem_instance.explicit_args.get("credentials", dlt.secrets.value),
    )
