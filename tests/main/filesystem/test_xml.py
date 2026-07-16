"""Test the XML filesystem reader on the generic iterabledata harness.

Mock-only unit lane (no Docker, no credentials): XML files are written to ``tmp_path`` and read
back through ``LocalFilesystemSource``. Covers: the mandatory ``#tagname`` reader hint threaded
through all three harness seams, the row-element-to-record convention (``@attr`` / ``#text`` /
repeated-children-as-list / namespaces), and the parser-safety contract -- an XXE / entity-bomb /
external-DTD / bad-encoding input is neutralized, never leaked or expanded.

XML is parsed with a hardened ``lxml`` config directly (iterabledata's XML parser resolves
entities and can't be locked down through its API), so it needs only ``lxml``, which ships in
omniload's ``iterable`` extra.
"""

import json

import duckdb
import pytest

from dlt_filesystem.testing.writer import write_xml
from omniload import run_ingest

lxml_etree = pytest.importorskip("lxml.etree")


def test_xml_loads_into_duckdb_via_parquet_loader(tmp_path):
    """An xml load into duckdb auto-selects the parquet loader, so attributes / repeated-as-list
    / nested shapes must survive the parquet writer."""
    src = write_xml(
        tmp_path / "docs.xml",
        '<data><item id="1"><name>foo</name><tag>a</tag><tag>b</tag>'
        "<nested><deep>1</deep></nested></item>"
        '<item id="2"><name>bar</name></item></data>',
    )
    dest = tmp_path / "warehouse.duckdb"

    run_ingest(
        source_uri=f"file://{src}#tagname=item",
        dest_uri=f"duckdb:///{dest}",
        source_table="",
        dest_table="out.docs",
        progress="log",
    )

    con = duckdb.connect(str(dest))
    try:
        # dlt normalizes the `@id` attribute key to the column `aid`.
        name, tag, nested = con.sql(
            "select name, tag, nested from out.docs where aid = '1'"
        ).fetchall()[0]
    finally:
        con.close()

    assert name == "foo"
    # max_table_nesting=0 lands the repeated tags and nested element as JSON columns.
    assert json.loads(tag) == ["a", "b"]
    assert json.loads(nested) == {"deep": "1"}
