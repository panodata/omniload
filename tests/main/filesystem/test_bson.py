"""BSON filesystem reader: decoding, extended-type normalization, and the
parquet-loader destination path.

Mock-only unit lane (no Docker, no credentials): BSON files are written to ``tmp_path``
and read back through ``LocalFilesystemSource``, which exercises the same
``_read_bson`` transformer the S3/GCS/SFTP sources use. The duckdb test covers the
parquet loader auto-selected for ``PARQUET_SUPPORTED_DESTINATIONS`` so ``Binary`` and
nested docs are proven to survive the parquet writer, not just the insert path.
"""

import base64
import datetime
import gzip
from typing import Any

import bson
import duckdb
import pytest
from bson import Binary, ObjectId, Regex, Timestamp
from bson.code import Code
from bson.dbref import DBRef
from bson.decimal128 import Decimal128
from bson.max_key import MaxKey
from bson.min_key import MinKey
from dlt.common.utils import map_nested_values_in_place

from omniload import run_ingest
from omniload.source.filesystem.api import LocalFilesystemSource
from omniload.source.filesystem.format.bson_codec import convert_bson_objs

OID = "507f1f77bcf86cd799439011"
OID2 = "507f1f77bcf86cd799439012"


def _write_bson(path, docs):
    """Write BSON documents concatenated into a single file (the on-disk mongodump form)."""
    with open(path, "wb") as f:
        for doc in docs:
            f.write(bson.encode(doc))
    return path


def _read_via_source(path):
    """Read a local BSON file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))


# --- convert_bson_objs: one leaf value at a time (what map_nested_values_in_place calls) ---


def test_objectid_becomes_str():
    assert convert_bson_objs(ObjectId(OID)) == OID


def test_decimal128_becomes_str():
    assert convert_bson_objs(Decimal128("3.14")) == "3.14"


def test_binary_becomes_base64_str():
    result = convert_bson_objs(Binary(b"\x00\x01\x02"))
    assert isinstance(result, str)
    assert base64.b64decode(result) == b"\x00\x01\x02"


def test_raw_bytes_becomes_base64_str():
    # Binary subclasses bytes; a plain bytes leaf takes the same base64 branch.
    result = convert_bson_objs(b"hello")
    assert isinstance(result, str)
    assert base64.b64decode(result) == b"hello"


def test_datetime_becomes_pendulum_utc():
    result = convert_bson_objs(datetime.datetime(2020, 1, 2, 3, 4, 5))
    assert isinstance(result, datetime.datetime)
    assert result.utcoffset() == datetime.timedelta(0)
    assert (result.year, result.month, result.day) == (2020, 1, 2)


def test_timestamp_becomes_pendulum_utc_datetime():
    # Timestamp(1600000000, 1) -> 2020-09-13T12:26:40Z
    result = convert_bson_objs(Timestamp(1600000000, 1))
    assert isinstance(result, datetime.datetime)
    assert result.utcoffset() == datetime.timedelta(0)
    assert result.year == 2020


def test_regex_becomes_pattern_str():
    assert convert_bson_objs(Regex("^abc", "i")) == "^abc"


def test_regex_with_pcre_only_syntax_does_not_crash():
    # A legal BSON/PCRE pattern that Python's re cannot compile (named group PCRE-style).
    # The reader must return the raw pattern string, not try to compile it.
    pattern = r"(?<year>\d{4})"
    assert convert_bson_objs(Regex(pattern, "")) == pattern


def test_dbref_becomes_extended_json_with_normalized_id():
    oid = ObjectId(OID)
    assert convert_bson_objs(DBRef("users", oid)) == {"$ref": "users", "$id": OID}
    # database is included only when present
    assert convert_bson_objs(DBRef("users", oid, "mydb")) == {
        "$ref": "users",
        "$id": OID,
        "$db": "mydb",
    }


def test_minkey_and_maxkey_become_extended_json():
    assert convert_bson_objs(MinKey()) == {"$minKey": 1}
    assert convert_bson_objs(MaxKey()) == {"$maxKey": 1}


def test_code_without_scope_becomes_str():
    assert convert_bson_objs(Code("function(){return 1}")) == "function(){return 1}"


def test_code_with_scope_becomes_extended_json_with_normalized_scope():
    result = convert_bson_objs(Code("function(){return x}", {"x": ObjectId(OID)}))
    assert result == {"$code": "function(){return x}", "$scope": {"x": OID}}


@pytest.mark.parametrize("value", [1, 1.5, "text", None, True])
def test_plain_values_pass_through(value):
    assert convert_bson_objs(value) is value


def test_nested_dict_and_list_are_normalized_recursively():
    # dict[str, Any] because map_nested_values_in_place is typed to return its input type
    # (it mutates in place); the leaves are converted to str at runtime but the checker
    # would otherwise still see the pre-conversion ObjectId/Binary/Decimal128 types.
    doc: dict[str, Any] = {
        "_id": ObjectId(OID),
        "nested": {"inner": ObjectId(OID2), "blob": Binary(b"AB")},
        "items": [Binary(b"x"), 1, {"deep": Decimal128("2.5")}],
    }
    result = map_nested_values_in_place(convert_bson_objs, doc)
    assert result["_id"] == OID
    assert result["nested"]["inner"] == OID2
    assert base64.b64decode(result["nested"]["blob"]) == b"AB"
    assert base64.b64decode(result["items"][0]) == b"x"
    assert result["items"][1] == 1
    assert result["items"][2]["deep"] == "2.5"


# --- reader end-to-end (fsspec, no Docker) ---


def test_reads_single_bson_document(tmp_path):
    path = _write_bson(tmp_path / "one.bson", [{"_id": ObjectId(OID), "name": "alice"}])
    rows = _read_via_source(path)
    assert len(rows) == 1
    assert rows[0]["_id"] == OID
    assert rows[0]["name"] == "alice"


def test_reads_multiple_bson_documents(tmp_path):
    docs = [{"_id": i, "name": name} for i, name in enumerate(["a", "b", "c"], start=1)]
    path = _write_bson(tmp_path / "many.bson", docs)
    rows = _read_via_source(path)
    assert [r["_id"] for r in rows] == [1, 2, 3]
    assert sorted(r["name"] for r in rows) == ["a", "b", "c"]


def test_reads_gzipped_bson(tmp_path):
    """.bson.gz is decompressed by fsspec (encoding=gzip) and routed to read_bson
    (parse_endpoint strips the trailing .gz)."""
    path = tmp_path / "data.bson.gz"
    with gzip.open(path, "wb") as f:
        f.write(bson.encode({"_id": ObjectId(OID), "name": "gzipped"}))
    rows = _read_via_source(path)
    assert len(rows) == 1
    assert rows[0]["_id"] == OID
    assert rows[0]["name"] == "gzipped"


def test_reader_normalizes_binary_and_nested(tmp_path):
    doc = {
        "_id": ObjectId(OID),
        "blob": Binary(b"\x00\x01\x02"),
        "nested": {"inner": ObjectId(OID2), "tags": ["x", "y"]},
    }
    path = _write_bson(tmp_path / "rich.bson", [doc])
    row = _read_via_source(path)[0]
    assert base64.b64decode(row["blob"]) == b"\x00\x01\x02"
    assert row["nested"]["inner"] == OID2
    assert row["nested"]["tags"] == ["x", "y"]


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
    src = _write_bson(tmp_path / "docs.bson", [doc])
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
    src = _write_bson(tmp_path / "exotic.bson", [doc])
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
