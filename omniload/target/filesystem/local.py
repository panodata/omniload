"""Write CSV / JSONL / Parquet files to the local filesystem via ``file://``.

This is the write-side twin of ``omniload.source.filesystem`` ``LocalFilesystemSource``
(the ``file://`` source). It mirrors that URI grammar exactly: everything after
``file://`` is a filesystem path (never an RFC-8089 host), relative paths resolve
against the working directory, and the output format is taken from the file
extension or an explicit ``#format`` hint. See #106 for the URI-semantics discussion
and #143 for the destination request.

dlt's filesystem destination writes a directory layout
(``{dataset}/{table}/{load_id}.{ext}``), not a single named file, so this class
loads into a temp directory and, in ``post_load()``, reads the produced data back
through the format-agnostic ``load_dlt_file`` helper and re-emits it as one clean file
at the requested path (dropping dlt's bookkeeping ``_dlt_*`` columns). This is the
same shape as ``omniload.target.csv`` ``CsvDestination``, generalized from CSV-only to
CSV / JSONL / Parquet.
"""

import os
import shutil
import tempfile

from omniload.error import MissingValueError
from omniload.source.filesystem.format.registry import FORMAT_TO_READER
from omniload.source.filesystem.impl.util import (
    _is_absolute_local,
    _url_path_to_local,
)
from omniload.util.loader import load_dlt_file

# csv_headless is a read-only concept (parsing a header-less CSV); writing always emits a
# header, so the write side supports the plain-format subset of FORMAT_TO_READER.
WRITE_FORMATS = ("csv", "jsonl", "parquet")
WRITE_FORMATS_TEXT = ", ".join(WRITE_FORMATS)

# Globbing is a read-only feature; a write target must name exactly one output file.
_GLOB_CHARS = "*?["


def _supported_write_format_message(file_format: str | None = None) -> str:
    got = f" (got '{file_format}')" if file_format else ""
    return (
        f"Local file Destination only supports file formats: {WRITE_FORMATS_TEXT}{got}"
    )


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
        raise ValueError(_supported_write_format_message(file_format))

    local = _url_path_to_local(path)
    if not _is_absolute_local(local):
        # Relative to the working directory; normalize separators so Windows drive paths
        # come back slash-delimited, matching the source side.
        local = os.path.abspath(local).replace(os.sep, "/")
    return local, file_format


def _strip_dlt_columns(row: dict) -> dict:
    """Drop dlt's bookkeeping columns (``_dlt_id``, ``_dlt_load_id``, ...)."""
    return {key: value for key, value in row.items() if not key.startswith("_dlt_")}


def _write_csv(path: str, rows: list[dict]) -> None:
    import csv

    # Union of keys in first-seen order: dlt omits null keys per row, so a later row can
    # carry a column the first row lacked. First-seen order preserves the source column
    # order (rather than sorting), which is what an export is expected to look like.
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, restval="")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: str, rows: list[dict]) -> None:
    # dlt's json handles datetime/Decimal/etc. that dlt may have produced; stdlib json
    # would choke on them.
    from dlt.common import json

    with open(path, "w") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _write_parquet(path: str, rows: list[dict]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Union of keys, same reasoning as _write_csv: dlt omits null keys per row, and
    # pa.Table.from_pylist infers the schema from the first row only, so a column that
    # first appears in a later row would be silently dropped. Build explicit columns
    # (missing values become None) so every row contributes its full key set.
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    columns = {name: [row.get(name) for row in rows] for name in fieldnames}
    pq.write_table(pa.table(columns), path)


_WRITERS = {
    "csv": _write_csv,
    "jsonl": _write_jsonl,
    "parquet": _write_parquet,
}


class LocalFilesystemDestination:
    """Write a single local CSV / JSONL / Parquet file addressed by ``file://``.

    Usage mirrors the ``file://`` source: ``--dest-uri file://<path>[#format]``. The
    ``--dest-table`` value must be ``<dataset>.<table>``; it only names dlt's intermediate
    layout, the output file is the URI path.
    """

    output_path: str
    output_format: str
    temp_path: str
    dataset_name: str
    table_name: str

    def dlt_dest(self, uri: str, **kwargs):
        from pathlib import Path

        import dlt.destinations

        self.output_path, self.output_format = _resolve_output_target(uri)
        self.temp_path = tempfile.mkdtemp()
        # dlt writes its layout under this temp bucket; post_load() reassembles the single
        # output file from it. Its own loader_file_format is irrelevant, load_dlt_file
        # reads whatever dlt produced (gzip-jsonl by default, or csv/parquet). Path.as_uri()
        # gives an RFC-correct file:// URL on every platform (file:///tmp/x on POSIX,
        # file:///C:/... on Windows), avoiding the drive-as-host trap of a naive
        # "file://" + path.
        return dlt.destinations.filesystem(bucket_url=Path(self.temp_path).as_uri())

    def dlt_run_params(self, uri: str, table: str, **kwargs) -> dict:
        table_fields = table.split(".")
        if len(table_fields) != 2:
            raise ValueError("Table name must be in the format <schema>.<table>")

        self.dataset_name, self.table_name = table_fields
        return {
            "dataset_name": self.dataset_name,
            "table_name": self.table_name,
        }

    def post_load(self) -> None:
        table_dir = os.path.join(self.temp_path, self.dataset_name, self.table_name)
        try:
            # The whole load is materialized here before writing. dlt omits null keys per
            # row in its intermediate files, so csv output needs the column union across
            # all rows (and parquet needs the full table); streaming those correctly would
            # reintroduce the per-row re-header logic this codebase is moving away from.
            # Buffered is the simple, correct v1; streaming is a follow-up if it matters.
            rows: list[dict] = []
            if os.path.isdir(table_dir):
                # A load may be split across several data files; read them all, in a
                # stable order, so nothing is dropped. Only the target table's data files
                # live here, dlt keeps its bookkeeping tables in sibling directories.
                for name in sorted(os.listdir(table_dir)):
                    data_file = os.path.join(table_dir, name)
                    if os.path.isfile(data_file):
                        rows.extend(
                            _strip_dlt_columns(row) for row in load_dlt_file(data_file)
                        )

            out_dir = os.path.dirname(self.output_path)
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)

            _WRITERS[self.output_format](self.output_path, rows)
        finally:
            # Always clear the temp bucket, even if reading or writing failed partway.
            shutil.rmtree(self.temp_path, ignore_errors=True)
