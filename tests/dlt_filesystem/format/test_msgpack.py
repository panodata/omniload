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
import datetime
import importlib.util
import subprocess
import sys

import pytest

from dlt_filesystem.source.error import MissingDecoderError
from dlt_filesystem.source.format.iterable_codec import (
    FORMAT_TO_ITERABLE,
    _load_iterable_class,
    read_via_iterable,
)
from dlt_filesystem.source.format.readers import read_msgpack
from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.testing.stub import (
    FileItemStub,
    NonSeekableItem,
)
from dlt_filesystem.testing.writer import write_msgpack

pytest.importorskip("iterable.datatypes.msgpack")
msgpack = pytest.importorskip("msgpack")


def _read_via_source(path):
    """Read a local msgpack file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))


# --- end-to-end reader (fsspec, no Docker) ---


def test_reads_single_msgpack_record(tmp_path):
    """A single-record msgpack file loads that one record end-to-end."""
    path = write_msgpack(tmp_path / "one.msgpack", [{"id": 1, "name": "alice"}])
    rows = _read_via_source(path)
    assert rows == [{"id": 1, "name": "alice"}]


def test_reads_multiple_msgpack_records(tmp_path):
    """A multi-record msgpack file loads every record end-to-end."""
    docs = [{"id": i, "name": n} for i, n in enumerate(["a", "b", "c"], start=1)]
    path = write_msgpack(tmp_path / "many.msgpack", docs)
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert sorted(r["name"] for r in rows) == ["a", "b", "c"]


def test_extension_and_format_hint_both_resolve(tmp_path):
    """A `.msgpack` extension and an explicit `#msgpack` hint both resolve to the reader."""
    docs = [{"id": 1}, {"id": 2}]
    # extension detection
    ext_path = write_msgpack(tmp_path / "by_ext.msgpack", docs)
    assert len(_read_via_source(ext_path)) == 2
    # explicit #msgpack hint on a non-standard extension
    hint_path = write_msgpack(tmp_path / "feed.dat", docs)
    rows = list(LocalFilesystemSource().dlt_source(f"file://{hint_path}#msgpack", ""))
    assert len(rows) == 2


def test_adversarial_values_are_normalized(tmp_path):
    """Adversarial record: raw bytes, a Timestamp extension, and a nested map with a nested
    bytes value. All become dlt-safe, the same normalization the BSON reader applies."""
    doc = {
        "blob": b"\x00\x01\x02",
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "nested": {"inner": b"AB", "tags": ["x", "y"]},
    }
    path = write_msgpack(tmp_path / "adv.msgpack", [doc], datetime=True)
    row = _read_via_source(path)[0]
    assert base64.b64decode(row["blob"]) == b"\x00\x01\x02"
    assert isinstance(row["when"], datetime.datetime)
    assert row["when"].utcoffset() == datetime.timedelta(0)
    assert (row["when"].year, row["when"].month, row["when"].day) == (2020, 1, 2)
    assert base64.b64decode(row["nested"]["inner"]) == b"AB"
    assert row["nested"]["tags"] == ["x", "y"]


def test_custom_ext_type_is_dlt_safe(tmp_path):
    """A custom msgpack extension (non-Timestamp) decodes to ``msgpack.ExtType``, which is a
    namedtuple, so ``map_nested_values_in_place`` turns it into ``{"code", "data"}`` and the
    normalizer base64s its ``data``. Nothing non-serializable reaches dlt."""
    path = write_msgpack(tmp_path / "ext.msgpack", [{"x": msgpack.ExtType(7, b"raw")}])
    row = _read_via_source(path)[0]
    assert row["x"] == {"code": 7, "data": base64.b64encode(b"raw").decode("ascii")}


def test_reads_multiple_files_flushes_each_remainder(tmp_path):
    """A multi-file glob must flush each file's partial final chunk before the next file,
    so no records are dropped at a file boundary."""
    write_msgpack(tmp_path / "a.msgpack", [{"id": 1}, {"id": 2}, {"id": 3}])
    write_msgpack(tmp_path / "b.msgpack", [{"id": 4}, {"id": 5}])
    rows = list(LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.msgpack", ""))
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]


# --- chunking contract (drives read_msgpack directly with a small chunksize) ---


def test_chunks_at_boundary_and_flushes_partial(tmp_path):
    """Records are yielded in chunksize-sized chunks, with the partial final chunk flushed."""
    path = write_msgpack(tmp_path / "five.msgpack", [{"id": i} for i in range(5)])
    chunks = list(read_msgpack(iter([FileItemStub(path)]), chunksize=2))  # ty: ignore[invalid-argument-type]
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2, 3, 4]


def test_exact_chunksize_multiple_yields_no_empty_trailing_chunk(tmp_path):
    """A file whose record count is an exact multiple of chunksize must not emit a trailing
    empty chunk (the read_bulk()==[] EOF sentinel)."""
    path = write_msgpack(tmp_path / "four.msgpack", [{"id": i} for i in range(4)])
    chunks = list(read_msgpack(iter([FileItemStub(path)]), chunksize=2))  # ty: ignore[invalid-argument-type]
    assert [len(chunk) for chunk in chunks] == [2, 2]
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2, 3]


def test_non_seekable_stream_is_spooled(tmp_path):
    """A non-seekable handle (remote pipe / some compressed streams) is spooled into a
    BytesIO so iterabledata's seek(0) reset works. The central stream-bridge claim."""
    data = b"".join(msgpack.packb({"id": i}, use_bin_type=True) for i in range(3))
    chunks = list(
        read_via_iterable(
            iter([NonSeekableItem(data)]),  # ty: ignore[invalid-argument-type]
            file_format="msgpack",
            chunksize=1000,
        )
    )
    assert [doc["id"] for chunk in chunks for doc in chunk] == [0, 1, 2]


# --- registry / import-path / error UX ---


def test_msgpack_import_path_resolves_to_the_class():
    """The `iterable.datatypes.*` module naming is non-obvious (msgpack -> msgpack,
    bson -> bsonf) and could shift; pin it with a direct import-path check."""
    spec = FORMAT_TO_ITERABLE["msgpack"]
    assert spec.class_path == "iterable.datatypes.msgpack.MessagePackIterable"
    klass = _load_iterable_class(spec)
    assert klass.__name__ == "MessagePackIterable"


def test_missing_decoder_raises_typed_install_hint(tmp_path, monkeypatch):
    """With the msgpack decoder unavailable, the reader raises a typed error naming the
    exact `pip install`, not a bare ImportError."""
    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the msgpack decoder as absent, delegating other lookups to the real finder."""
        if name == "msgpack":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    path = write_msgpack(tmp_path / "x.msgpack", [{"id": 1}])
    with pytest.raises(MissingDecoderError) as exc:
        list(read_msgpack(iter([FileItemStub(path)]), chunksize=10))  # ty: ignore[invalid-argument-type]
    message = str(exc.value)
    assert "msgpack" in message
    assert "omniload[iterable]" in message


def test_msgpack_advertised_only_when_decoder_installed(monkeypatch):
    """A base install must not advertise msgpack as supported when its decoder is absent,
    even though the format stays routable. With the decoder present it is listed."""
    from dlt_filesystem.source.format.registry import advertised_file_formats

    assert "msgpack" in advertised_file_formats()

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        """Report the msgpack decoder as absent, delegating other lookups to the real finder."""
        if name == "msgpack":
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    assert "msgpack" not in advertised_file_formats()


def test_import_path_keeps_decoder_out_of_sys_modules():
    """Importing the filesystem reader path must not pull `iterable` or `msgpack` (they are
    lazy-imported only when a msgpack file is actually read). Runs in a subprocess so an
    earlier test in this module that imported them can't mask the result."""
    code = (
        "import sys\n"
        "import dlt_filesystem.source.adapter\n"
        "decoders = ('iterable', 'msgpack', 'cbor2')\n"
        "leaked = sorted(m for m in sys.modules if m.split('.')[0] in decoders)\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run(  # noqa: S603  # trusted: sys.executable + a fixed code string
        [sys.executable, "-c", code], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
