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
import datetime
import decimal
import importlib.util
import json

import duckdb
import pytest

cbor2 = pytest.importorskip("cbor2")

from omniload import run_ingest  # noqa: E402
from omniload.source.filesystem.api import LocalFilesystemSource  # noqa: E402
from omniload.source.filesystem.error import MissingDecoderError  # noqa: E402
from omniload.source.filesystem.format.iterable_codec import (  # noqa: E402
    FORMAT_TO_ITERABLE,
    read_via_iterable,
)
from omniload.source.filesystem.format.readers import read_cbor  # noqa: E402
from tests.main.filesystem._iterable_helpers import (  # noqa: E402
    FileItemStub,
    NonSeekableItem,
)


def _write_cbor(path, value):
    """Write a single top-level CBOR value (an array of records, or one record)."""
    with open(path, "wb") as f:
        f.write(cbor2.dumps(value))
    return path


def _read_via_source(path):
    """Read a local cbor file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))


# --- end-to-end reader (fsspec, no Docker) ---


def test_reads_single_top_level_object(tmp_path):
    """A single top-level CBOR map loads as one record."""
    path = _write_cbor(tmp_path / "one.cbor", {"id": 1, "name": "alice"})
    assert _read_via_source(path) == [{"id": 1, "name": "alice"}]


def test_reads_top_level_array_all_rows(tmp_path):
    """The supported shape: a single top-level array yields one row per element."""
    docs = [{"id": i, "name": n} for i, n in enumerate(["a", "b", "c"], start=1)]
    path = _write_cbor(tmp_path / "arr.cbor", docs)
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert sorted(r["name"] for r in rows) == ["a", "b", "c"]


def test_concatenated_objects_read_only_the_first(tmp_path):
    """Documented limitation: a file that concatenates several top-level CBOR objects is read
    only up to the first; cbor2 cannot stream them and it can't be detected. The fix is to
    write a top-level array (see test above)."""
    path = tmp_path / "concat.cbor"
    with open(path, "wb") as f:
        for doc in [{"id": 1}, {"id": 2}, {"id": 3}]:
            f.write(cbor2.dumps(doc))
    rows = _read_via_source(path)
    assert rows == [{"id": 1}]


def test_extension_and_format_hint_both_resolve(tmp_path):
    """A `.cbor` extension and an explicit `#cbor` hint both resolve to the reader."""
    docs = [{"id": 1}, {"id": 2}]
    ext_path = _write_cbor(tmp_path / "by_ext.cbor", docs)
    assert len(_read_via_source(ext_path)) == 2
    hint_path = _write_cbor(tmp_path / "feed.dat", docs)
    rows = list(LocalFilesystemSource().dlt_source(f"file://{hint_path}#cbor", ""))
    assert len(rows) == 2


def test_adversarial_values_are_normalized(tmp_path):
    """Adversarial record: raw bytes, a tz-aware datetime, a Decimal, an unknown CBOR tag, and
    a nested map with a nested bytes value. bytes -> base64, an unknown tag -> {"tag","value"};
    datetime and Decimal are already dlt-safe and pass through."""
    doc = {
        "blob": b"\x00\x01\x02",
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "amt": decimal.Decimal("3.14"),
        "tagged": cbor2.CBORTag(1234, "custom"),
        "nested": {"inner": b"AB", "tags": ["x", "y"]},
    }
    path = _write_cbor(tmp_path / "adv.cbor", [doc])
    row = _read_via_source(path)[0]
    assert base64.b64decode(row["blob"]) == b"\x00\x01\x02"
    assert isinstance(row["when"], datetime.datetime)
    assert row["when"].utcoffset() == datetime.timedelta(0)
    assert row["amt"] == decimal.Decimal("3.14")
    assert row["tagged"] == {"tag": 1234, "value": "custom"}
    assert base64.b64decode(row["nested"]["inner"]) == b"AB"
    assert row["nested"]["tags"] == ["x", "y"]


def test_reads_multiple_files_flushes_each_remainder(tmp_path):
    """Multi-file glob: each file is a top-level array; all records across files load."""
    _write_cbor(tmp_path / "a.cbor", [{"id": 1}, {"id": 2}, {"id": 3}])
    _write_cbor(tmp_path / "b.cbor", [{"id": 4}, {"id": 5}])
    rows = list(LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.cbor", ""))
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]


# --- chunking contract (drives read_cbor directly with a small chunksize) ---


def test_chunks_at_boundary_and_flushes_partial(tmp_path):
    """Records are yielded in chunksize-sized chunks, with the partial final chunk flushed."""
    path = _write_cbor(tmp_path / "five.cbor", [{"id": i} for i in range(5)])
    chunks = list(read_cbor(iter([FileItemStub(path)]), chunksize=2))  # ty: ignore[invalid-argument-type]
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2, 3, 4]


def test_exact_chunksize_multiple_yields_no_empty_trailing_chunk(tmp_path):
    """A record count that is an exact multiple of chunksize emits no trailing empty chunk."""
    path = _write_cbor(tmp_path / "four.cbor", [{"id": i} for i in range(4)])
    chunks = list(read_cbor(iter([FileItemStub(path)]), chunksize=2))  # ty: ignore[invalid-argument-type]
    assert [len(chunk) for chunk in chunks] == [2, 2]
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2, 3]


def test_non_seekable_stream_is_spooled(tmp_path):
    """A non-seekable handle is spooled into a BytesIO so CBORIterable's seek(0) reset works
    (it otherwise silently yields nothing). The central stream-bridge claim, for CBOR."""
    data = cbor2.dumps([{"id": i} for i in range(3)])
    chunks = list(
        read_via_iterable(
            iter([NonSeekableItem(data)]),  # ty: ignore[invalid-argument-type]
            file_format="cbor",
            chunksize=1000,
        )
    )
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2]


# --- registry / import-path / error UX ---


def test_cbor_uses_a_direct_decoder_not_iterabledata():
    """CBOR is read by decoding with cbor2 directly (not iterabledata's error-swallowing
    CBORIterable), so the registry entry carries an eager_decoder and no class_path."""
    spec = FORMAT_TO_ITERABLE["cbor"]
    assert spec.eager_decoder is not None
    assert spec.class_path is None
    assert spec.decoder_dist == "cbor2"


def test_truncated_cbor_raises_instead_of_silently_dropping_rows(tmp_path):
    """A corrupt/truncated CBOR file must raise, not load as zero rows (silent data loss).
    iterabledata's CBORIterable swallows the decode error; decoding via cbor2 surfaces it.
    Driven through the reader directly for the precise exception (the source path wraps it in
    dlt's ResourceExtractionError)."""
    full = cbor2.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
    path = tmp_path / "truncated.cbor"
    path.write_bytes(full[: len(full) // 2])  # cut mid-stream
    with pytest.raises(cbor2.CBORDecodeError):
        list(read_cbor(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]


def test_empty_cbor_file_yields_no_rows(tmp_path):
    """An empty file is not corrupt; it loads as zero rows (matching the other readers), so the
    truncation guard must not fire on it."""
    path = tmp_path / "empty.cbor"
    path.write_bytes(b"")
    assert _read_via_source(path) == []


def test_missing_decoder_raises_typed_install_hint(tmp_path, monkeypatch):
    """With the cbor2 decoder unavailable, the reader raises a typed error naming the exact
    `pip install`, not a bare ImportError."""
    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the cbor2 decoder as absent, delegating other lookups to the real finder."""
        if name == "cbor2":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    path = _write_cbor(tmp_path / "x.cbor", [{"id": 1}])
    with pytest.raises(MissingDecoderError) as exc:
        list(read_cbor(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]
    message = str(exc.value)
    assert "cbor2" in message
    assert "omniload[iterable]" in message


def test_cbor_advertised_only_when_decoder_installed(monkeypatch):
    """A base install must not advertise cbor as supported when its decoder is absent, even
    though the format stays routable."""
    from omniload.source.filesystem.format.registry import advertised_file_formats

    assert "cbor" in advertised_file_formats()

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the cbor2 decoder as absent, delegating other lookups to the real finder."""
        if name == "cbor2":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    assert "cbor" not in advertised_file_formats()


# --- parquet-loader destination path (duckdb is in PARQUET_SUPPORTED_DESTINATIONS) ---


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
    src = _write_cbor(tmp_path / "docs.cbor", docs)
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
