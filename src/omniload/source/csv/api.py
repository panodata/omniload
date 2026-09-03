"""``csv://``: a CSV-only compatibility spelling of the local ``file://`` source.

The scheme predates the filesystem family and is kept so existing commands resolve.
It routes through the shared local reader, so ``csv://x.csv`` and ``file://x.csv``
load identically, and the only behaviour this module adds is the format restriction:
the scheme names a format, so it reads CSV and nothing else. See #301.
"""

from typing import Optional

from dlt_filesystem.source.format.registry import reader_for_format
from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from omniload.error import ValidationError

# Every CSV-family reader the shared registry exposes. The scheme restricts the file
# format, not the parsing route, so the header-less and DuckDB CSV readers stay
# reachable through ``#csv_headless`` and ``#csv_duckdb``.
CSV_FORMATS = ("csv", "csv_headless", "csv_duckdb")
CSV_READERS = frozenset(reader_for_format(file_format) for file_format in CSV_FORMATS)


class LocalCsvSource(LocalFilesystemSource):
    """Read local CSV files through the shared filesystem readers.

    Everything but the format restriction is inherited: path grammar (split form,
    relative and absolute paths, Windows drive and UNC paths), globs, gzip, ``#format``
    and ``#key=value`` reader hints, column typing, and file-level incrementality by
    modification time. Row-level ``--incremental-key`` is rejected here exactly as it is
    on ``file://``; the old string-versus-datetime cursor is gone.
    """

    def validate_reader(self, reader_name: Optional[str]) -> None:
        """Reject any selection that resolves to a reader outside the CSV family."""
        if reader_name in CSV_READERS:
            return
        resolved = (
            f"resolves to the '{reader_name}' reader"
            if reader_name
            else "names no known file format"
        )
        raise ValidationError(
            f"The 'csv' source only reads CSV ({', '.join(CSV_FORMATS)}); this "
            f"selection {resolved}. Use a 'file://' source to read other formats."
        )
