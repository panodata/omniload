"""Test the MessagePack filesystem reader: the generic iterabledata harness + msgpack.

Mock-only unit lane (no Docker, no credentials): msgpack files are written to ``tmp_path``
and read back through ``LocalFilesystemSource``, which exercises the same ``read_msgpack``
transformer the S3/GCS/SFTP sources use. The suite covers: the stream bridge (incl. the
non-seekable spool), extended-type normalization, chunk-boundary flushing, the pinned import
path, the missing-decoder message, and that the decoder stays out of ``sys.modules`` on the
import path.

Skipped entirely when the optional ``iterable`` extra isn't installed.
"""

import base64
import json

import duckdb
import pytest

from dlt_filesystem.testing.writer import write_msgpack
from omniload import run_ingest

pytest.importorskip("iterable.datatypes.msgpack")
msgpack = pytest.importorskip("msgpack")


def test_msgpack_loads_into_duckdb_via_parquet_loader(tmp_path):
    """A msgpack load into duckdb auto-selects the parquet loader (file sources aren't
    JSON_RETURNING_SOURCES), so bytes/nested handling must survive the parquet writer."""
    doc = {
        "id": 1,
        "blob": b"\x00\x01\x02",
        "nested": {"inner": "deep", "tags": ["x", "y"]},
    }
    src = write_msgpack(tmp_path / "docs.msgpack", [doc])
    dest = tmp_path / "warehouse.duckdb"

    run_ingest(
        source_uri=f"file://{src}",
        dest_uri=f"duckdb:///{dest}",
        source_table="",
        dest_table="out.docs",
        progress="log",
    )

    con = duckdb.connect(str(dest))
    try:
        blob, nested = con.sql("select blob, nested from out.docs").fetchall()[0]
    finally:
        con.close()

    assert base64.b64decode(blob) == b"\x00\x01\x02"
    # nested lands as a JSON column; parse and assert the exact structure survives.
    assert json.loads(nested) == {"inner": "deep", "tags": ["x", "y"]}
