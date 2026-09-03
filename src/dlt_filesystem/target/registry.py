# csv_headless is a read-only concept (parsing a header-less CSV);
# writing always emits a header, so the write side supports the
# plain-format subset of FORMAT_TO_READER.
from typing import Callable

from dlt_filesystem.target.writer import write_csv, write_jsonl, write_parquet

FORMAT_TO_WRITER: dict[str, Callable[[str, list[dict]], None]] = {
    "csv": write_csv,
    "jsonl": write_jsonl,
    "parquet": write_parquet,
}

WRITE_FORMATS = ("csv", "jsonl", "parquet")
WRITE_FORMATS_TEXT = ", ".join(WRITE_FORMATS)


def writer_for_format(file_format: str) -> Callable[[str, list[dict]], None]:
    try:
        return FORMAT_TO_WRITER[file_format]
    except KeyError as e:
        raise NotImplementedError(f"Unsupported file format: {file_format}") from e


def supported_write_format_message(file_format: str | None = None) -> str:
    got = f" (got '{file_format}')" if file_format else ""
    return (
        f"Local file Destination only supports file formats: {WRITE_FORMATS_TEXT}{got}"
    )


def pinned_write_format_message(pinned_format: str, file_format: str) -> str:
    """Build the error for a destination whose scheme already names its format."""
    return (
        f"A '{pinned_format}' destination only writes {pinned_format} files, and this "
        f"one names '{file_format}'. Use a 'file://' destination for other formats."
    )
