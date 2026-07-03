import warnings
from typing import Optional, Tuple, TypeAlias
from urllib.parse import ParseResult, urlparse

BucketName: TypeAlias = str
FileGlob: TypeAlias = str


class UnsupportedEndpointError(Exception):
    pass


FORMAT_TO_READER: dict[str, str] = {
    "csv": "read_csv",
    "csv_headless": "read_csv_headless",
    "jsonl": "read_jsonl",
    "parquet": "read_parquet",
    "xlsx": "read_excel",
}
SUPPORTED_FILE_FORMATS = tuple(FORMAT_TO_READER)
SUPPORTED_FILE_FORMATS_TEXT = ", ".join(SUPPORTED_FILE_FORMATS)


def reader_for_format(file_format: str) -> str:
    try:
        return FORMAT_TO_READER[file_format]
    except KeyError as e:
        raise UnsupportedEndpointError(f"Unsupported file format: {file_format}") from e


def supported_file_format_message(source_name: str) -> str:
    return f"{source_name} Source only supports file formats: {SUPPORTED_FILE_FORMATS_TEXT}"


def parse_uri(uri: ParseResult, table: str) -> Tuple[BucketName, FileGlob]:
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

    table = table.strip()
    host = uri.netloc.strip()

    if table == "" or uri.path.strip() != "":
        warnings.warn(
            f"Using the form '{uri.scheme}://bucket-name/file-glob' is deprecated and will be removed in future versions.",
            DeprecationWarning,
            stacklevel=2,
        )
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


def split_format_hint(table: str) -> Tuple[str, Optional[str]]:
    """Split a table spec into ``(path, format_hint)``.

    A trailing ``#segment`` is treated as an explicit format hint only when
    ``segment`` is a recognized format. Otherwise the ``#`` is part of the
    path and the hint is ``None``, so literal ``#`` characters in file paths
    keep working (e.g. ``/feeds/vendor#1/data.csv``).
    """
    path, sep, suffix = table.rpartition("#")
    if sep and suffix in FORMAT_TO_READER:
        return path, suffix
    return table, None


def determine_endpoint(table: str, path: str) -> str:
    """
    determines the endpoint/method to use for reading data from a blob source
    """

    _, file_format = split_format_hint(table)
    if file_format is not None:
        return reader_for_format(file_format)

    try:
        return parse_endpoint(path)
    except Exception as e:
        raise ValueError(f"Failed to parse endpoint from path: {path}") from e
