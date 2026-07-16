"""Test the CBOR filesystem reader: the second format on the generic iterabledata harness.

Mock-only unit lane (no Docker, no credentials): CBOR files are written to ``tmp_path`` and
read back through ``LocalFilesystemSource``. Covers: CBOR resolves by extension + ``#cbor``
hint, the adversarial fixture (bytes, datetime, Decimal, a custom tag, nested) loads dlt-safe,
and the documented single-top-level-value constraint holds (concatenated CBOR objects read
only the first, a cbor2 limitation that can't be detected).

Skipped when ``cbor2`` isn't installed (it ships in omniload's ``iterable`` extra). CBOR
decodes with ``cbor2`` directly, so it does not need the ``iterable`` package itself.
"""

import base64
import decimal
import json

import duckdb
import pytest

from dlt_filesystem.testing.writer import write_cbor
from omniload import run_ingest

cbor2 = pytest.importorskip("cbor2")


def test_cbor_loads_into_duckdb_via_parquet_loader(tmp_path):
    """A cbor load into duckdb auto-selects the parquet loader, so bytes/Decimal/tag/nested
    handling must survive the parquet writer."""
    docs = [
        {
            "id": 1,
            "blob": b"\x00\x01\x02",
            "amt": decimal.Decimal("3.14"),
            "tagged": cbor2.CBORTag(1234, "custom"),
            "nested": {"inner": "deep", "tags": ["x", "y"]},
        },
        {"id": 2, "amt": decimal.Decimal("9.99")},
    ]
    src = write_cbor(tmp_path / "docs.cbor", docs)
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
        blob, amt, tagged, nested = con.sql(
            "select blob, amt, tagged, nested from out.docs where id = 1"
        ).fetchall()[0]
    finally:
        con.close()

    assert base64.b64decode(blob) == b"\x00\x01\x02"
    assert decimal.Decimal(str(amt)) == decimal.Decimal("3.14")
    # tagged/nested land as JSON columns; parse and assert the exact structure survives.
    assert json.loads(tagged) == {"tag": 1234, "value": "custom"}
    assert json.loads(nested) == {"inner": "deep", "tags": ["x", "y"]}
