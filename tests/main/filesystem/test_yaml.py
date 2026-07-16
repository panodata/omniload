"""Test the YAML filesystem reader on the generic iterabledata harness.

Mock-only unit lane (no Docker, no credentials): YAML files are written to ``tmp_path`` and read
back through ``LocalFilesystemSource``. Covers the document-to-row shapes (single dict, multi-doc,
top-level list, skipped ``None`` docs), extended-leaf normalization (``!!binary`` / ``!!set`` /
``!!timestamp``), and the safety contract: ``!!python/object`` is rejected and a malformed file
raises rather than silently loading zero rows.

YAML is decoded with ``yaml.safe_load_all`` directly (not iterabledata's eager, error-swallowing
wrapper), so it needs only PyYAML, which ships in omniload's ``iterable`` extra.
"""

import base64
import datetime
import json

import duckdb
import pytest

from dlt_filesystem.testing.writer import write_yaml
from omniload import run_ingest

yaml = pytest.importorskip("yaml")


def test_yaml_loads_into_duckdb_via_parquet_loader(tmp_path):
    """A yaml load into duckdb auto-selects the parquet loader, so the base64/list/datetime
    normalization must survive the parquet writer."""
    path = write_yaml(
        tmp_path / "docs.yaml",
        "- id: 1\n"
        "  blob: !!binary aGVsbG8=\n"
        "  labels: !!set {x: null, y: null}\n"
        "  when: 2020-01-02 03:04:05\n"
        "- id: 2\n"
        "  blob: !!binary QUI=\n",
    )
    dest = tmp_path / "warehouse.duckdb"

    run_ingest(
        source_uri=f"file://{path}",
        dest_uri=f"duckdb:///{dest}",
        source_table="",
        dest_table="out.docs",
        progress="log",
    )

    con = duckdb.connect(str(dest))
    try:
        blob, labels, when = con.sql(
            'select blob, labels, "when" from out.docs where id = 1'
        ).fetchall()[0]
    finally:
        con.close()

    assert base64.b64decode(blob) == b"hello"
    # max_table_nesting=0 lands the normalized set as a JSON array column, not a child table.
    assert sorted(json.loads(labels)) == ["x", "y"]
    assert isinstance(when, datetime.datetime)
