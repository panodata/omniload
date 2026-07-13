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
import importlib.util
import json

import duckdb
import pytest

yaml = pytest.importorskip("yaml")

from omniload import run_ingest  # noqa: E402
from omniload.source.filesystem.api import LocalFilesystemSource  # noqa: E402
from omniload.source.filesystem.error import MissingDecoderError  # noqa: E402
from omniload.source.filesystem.format.iterable_codec import (  # noqa: E402
    FORMAT_TO_ITERABLE,
)
from omniload.source.filesystem.format.readers import read_yaml  # noqa: E402
from tests.main.filesystem._iterable_helpers import (  # noqa: E402
    FileItemStub,
    NonSeekableItem,
)


def _write_yaml(path, text):
    """Write raw YAML ``text`` to ``path``."""
    with open(path, "w") as f:
        f.write(text)
    return path


def _read_via_source(path):
    """Read a local YAML file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))


# --- document -> row shapes --------------------------------------------------


def test_single_mapping_is_one_row(tmp_path):
    """A single YAML document that is a mapping loads as exactly one row."""
    path = _write_yaml(tmp_path / "one.yaml", "id: 1\nname: alice\n")
    assert _read_via_source(path) == [{"id": 1, "name": "alice"}]


def test_multi_document_yields_one_row_each(tmp_path):
    """`---`-separated documents each become one row."""
    path = _write_yaml(tmp_path / "multi.yaml", "id: 1\n---\nid: 2\n---\nid: 3\n")
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_top_level_list_expands_to_one_row_per_element(tmp_path):
    """A document that is a top-level sequence expands to one row per element (not one nested
    row), so a plain list of records loads naturally."""
    path = _write_yaml(tmp_path / "list.yaml", "- id: 1\n- id: 2\n- id: 3\n")
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_mixed_list_and_mapping_documents(tmp_path):
    """A file mixing a list document and a mapping document flattens both into rows."""
    path = _write_yaml(tmp_path / "mixed.yaml", "- id: 1\n- id: 2\n---\nid: 3\n")
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]


def test_none_documents_are_skipped(tmp_path):
    """A ``---``-only / empty document parses to ``None`` and carries no row, so it is skipped
    rather than emitting an empty record."""
    path = _write_yaml(tmp_path / "nones.yaml", "id: 1\n---\n---\nid: 2\n")
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2]


def test_empty_file_yields_no_rows(tmp_path):
    """An empty (or whitespace-only) file loads as zero rows, matching the other readers."""
    path = _write_yaml(tmp_path / "empty.yaml", "   \n")
    assert _read_via_source(path) == []


def test_extension_and_format_hint_both_resolve(tmp_path):
    """A `.yaml` extension, a `.yml` extension, and an explicit `#yaml` hint all resolve."""
    body = "- id: 1\n- id: 2\n"
    assert len(_read_via_source(_write_yaml(tmp_path / "by_ext.yaml", body))) == 2
    assert len(_read_via_source(_write_yaml(tmp_path / "by_ext.yml", body))) == 2
    hint_path = _write_yaml(tmp_path / "feed.dat", body)
    rows = list(LocalFilesystemSource().dlt_source(f"file://{hint_path}#yaml", ""))
    assert len(rows) == 2


# --- extended-leaf normalization ---------------------------------------------


def test_extended_leaves_are_normalized(tmp_path):
    """`!!binary` -> base64 str, `!!set` -> list, `!!timestamp` -> datetime/date; nested too."""
    path = _write_yaml(
        tmp_path / "ext.yaml",
        "blob: !!binary aGVsbG8=\n"
        "labels: !!set {a: null, b: null}\n"
        "when: 2020-01-02 03:04:05\n"
        "day: 2020-01-02\n"
        "nested:\n  inner: !!binary QUI=\n",
    )
    row = _read_via_source(path)[0]
    assert base64.b64decode(row["blob"]) == b"hello"
    assert sorted(row["labels"]) == ["a", "b"]
    assert isinstance(row["when"], datetime.datetime)
    assert row["day"] == datetime.date(2020, 1, 2)
    assert base64.b64decode(row["nested"]["inner"]) == b"AB"


def test_reads_multiple_files_flushes_each_remainder(tmp_path):
    """Multi-file glob: every record across files loads (each file's remainder is flushed)."""
    _write_yaml(tmp_path / "a.yaml", "- id: 1\n- id: 2\n- id: 3\n")
    _write_yaml(tmp_path / "b.yaml", "- id: 4\n- id: 5\n")
    rows = list(LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.yaml", ""))
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]


# --- safety ------------------------------------------------------------------


def test_python_object_tag_is_rejected(tmp_path):
    """`safe_load_all` refuses a `!!python/object` tag (arbitrary-object construction), so a
    hostile document raises instead of executing. Driven through the reader for the precise
    exception (the source path wraps it in dlt's ResourceExtractionError)."""
    path = _write_yaml(
        tmp_path / "evil.yaml", "!!python/object/apply:os.system ['echo pwned']\n"
    )
    with pytest.raises(yaml.YAMLError):
        list(read_yaml(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]


def test_malformed_yaml_raises_instead_of_silently_dropping_rows(tmp_path):
    """A malformed document must raise, not load as zero rows (iterabledata's YAML wrapper
    swallows the error; safe_load_all surfaces it)."""
    path = _write_yaml(tmp_path / "bad.yaml", "id: [1, 2\nname: broken\n")
    with pytest.raises(yaml.YAMLError):
        list(read_yaml(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]


def test_anchors_and_aliases_resolve(tmp_path):
    """A normal anchor/alias reference resolves (safe_load supports them); only code-execution
    tags are blocked."""
    path = _write_yaml(tmp_path / "anchor.yaml", "a: &v 7\nb: *v\n")
    assert _read_via_source(path) == [{"a": 7, "b": 7}]


def test_recursive_alias_raises_cleanly(tmp_path):
    """A self-recursive alias makes a cyclic structure that safe_load accepts but that cannot be
    flattened into a finite record; normalization hits Python's recursion limit and raises a
    bounded RecursionError rather than hanging or emitting corrupt data. Pathological input,
    pinned here as an unsupported-but-safe boundary (not a code-execution or data-leak hole)."""
    path = _write_yaml(tmp_path / "cyclic.yaml", "a: &a [*a]\n")
    with pytest.raises(RecursionError):
        list(read_yaml(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]


# --- chunking contract (drives read_yaml directly with a small chunksize) ----


def test_chunks_at_boundary_and_flushes_partial(tmp_path):
    """Records are yielded in chunksize-sized chunks, with the partial final chunk flushed."""
    body = "".join(f"- id: {i}\n" for i in range(5))
    path = _write_yaml(tmp_path / "five.yaml", body)
    chunks = list(read_yaml(iter([FileItemStub(path)]), chunksize=2))  # ty: ignore[invalid-argument-type]
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2, 3, 4]


def test_non_seekable_stream_is_spooled(tmp_path):
    """A non-seekable handle still loads (the harness reads the whole stream before decoding)."""
    data = b"- id: 0\n- id: 1\n- id: 2\n"
    from omniload.source.filesystem.format.iterable_codec import read_via_iterable

    chunks = list(
        read_via_iterable(
            iter([NonSeekableItem(data)]),  # ty: ignore[invalid-argument-type]
            file_format="yaml",
            chunksize=1000,
        )
    )
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2]


# --- registry / import-path / error UX ---------------------------------------


def test_yaml_uses_a_direct_decoder_not_iterabledata():
    """YAML is read by decoding with PyYAML directly (safe_load_all), so the registry entry
    carries an eager_decoder and no class_path."""
    spec = FORMAT_TO_ITERABLE["yaml"]
    assert spec.eager_decoder is not None
    assert spec.class_path is None
    assert spec.decoder_dist == "yaml"


def test_missing_decoder_raises_typed_install_hint(tmp_path, monkeypatch):
    """With PyYAML unavailable, the reader raises a typed error naming the exact `pip install`,
    not a bare ImportError."""
    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the yaml decoder as absent, delegating other lookups to the real finder."""
        if name == "yaml":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    path = _write_yaml(tmp_path / "x.yaml", "id: 1\n")
    with pytest.raises(MissingDecoderError) as exc:
        list(read_yaml(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]
    message = str(exc.value)
    assert "yaml" in message
    assert "omniload[iterable]" in message


def test_yaml_advertised_only_when_decoder_installed(monkeypatch):
    """A base install must not advertise yaml as supported when PyYAML is absent, even though
    the format stays routable."""
    from omniload.source.filesystem.format.registry import advertised_file_formats

    assert "yaml" in advertised_file_formats()

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the yaml decoder as absent, delegating other lookups to the real finder."""
        if name == "yaml":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    assert "yaml" not in advertised_file_formats()


# --- parquet-loader destination path (duckdb is in PARQUET_SUPPORTED_DESTINATIONS) ---


def test_yaml_loads_into_duckdb_via_parquet_loader(tmp_path):
    """A yaml load into duckdb auto-selects the parquet loader, so the base64/list/datetime
    normalization must survive the parquet writer."""
    path = _write_yaml(
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
