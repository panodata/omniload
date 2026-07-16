"""BSON filesystem reader: decoding, extended-type normalization, and the
parquet-loader destination path.

Mock-only unit lane (no Docker, no credentials): BSON files are written to ``tmp_path``
and read back through ``LocalFilesystemSource``, which exercises the same
``_read_bson`` transformer the S3/GCS/SFTP sources use. The duckdb test covers the
parquet loader auto-selected for ``PARQUET_SUPPORTED_DESTINATIONS`` so ``Binary`` and
nested docs are proven to survive the parquet writer, not just the insert path.
"""

import base64

import duckdb
from bson import Binary, ObjectId
from bson.code import Code
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.max_key import MaxKey
from bson.min_key import MinKey

from dlt_filesystem.testing.writer import write_bson
from omniload import run_ingest

OID = "507f1f77bcf86cd799439011"
OID2 = "507f1f77bcf86cd799439012"


# --- parquet-loader destination path (duckdb is in PARQUET_SUPPORTED_DESTINATIONS) ---


def test_bson_loads_into_duckdb_via_parquet_loader(tmp_path):
    """A BSON load into duckdb auto-selects the parquet loader (file sources aren't
    JSON_RETURNING_SOURCES), so Binary/nested handling must survive the parquet writer.

    With ``max_table_nesting=0`` (consistent with the filesystem family) the nested doc
    lands as a single complex/JSON column rather than being shredded into child tables,
    and its content is preserved intact, evidence the ``0`` choice does not mangle
    nested BSON (PLAN Decisions §2).
    """
    doc = {
        "_id": ObjectId(OID),
        "amt": Decimal128("3.14"),
        "blob": Binary(b"\x00\x01\x02"),
        "nested": {"inner": ObjectId(OID2), "tags": ["x", "y"]},
    }
    src = write_bson(tmp_path / "docs.bson", [doc])
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
        blob, amt, nested = con.sql(
            "select blob, amt, nested from out.docs"
        ).fetchall()[0]
    finally:
        con.close()

    assert base64.b64decode(blob) == b"\x00\x01\x02"
    assert str(amt) == "3.14"
    # nested survives as a JSON/struct column; its values are intact and normalized.
    assert OID2 in str(nested)
    assert "x" in str(nested) and "y" in str(nested)


def test_exotic_bson_types_load_into_duckdb_without_crashing(tmp_path):
    """DBRef/MinKey/MaxKey/Code are not JSON-serializable as raw BSON objects; without
    normalization a dump containing one crashes the load (`Type is not JSON
    serializable`). Assert such a document loads and the values round-trip as their
    Extended-JSON-shaped forms."""
    doc = {
        "_id": ObjectId(OID),
        "ref": DBRef("users", ObjectId(OID2)),
        "lo": MinKey(),
        "hi": MaxKey(),
        "fn": Code("function(){return 1}"),
    }
    src = write_bson(tmp_path / "exotic.bson", [doc])
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
        ref, lo, hi, fn = con.sql("select ref, lo, hi, fn from out.docs").fetchall()[0]
    finally:
        con.close()

    assert OID2 in str(ref) and "users" in str(ref)
    assert "minKey" in str(lo)
    assert "maxKey" in str(hi)
    assert fn == "function(){return 1}"
