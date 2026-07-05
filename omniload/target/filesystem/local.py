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

from omniload.target.filesystem.registry import writer_for_format
from omniload.target.filesystem.util import _resolve_output_target, _strip_dlt_columns
from omniload.util.loader import load_dlt_file


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

            writer_for_format(self.output_format)(self.output_path, rows)
        finally:
            # Always clear the temp bucket, even if reading or writing failed partway.
            shutil.rmtree(self.temp_path, ignore_errors=True)
