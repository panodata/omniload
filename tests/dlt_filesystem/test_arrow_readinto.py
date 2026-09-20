"""Arrow-backed filesystems hand out read handles that fill a caller's buffer.

fsspec's ``ArrowFile`` mirrors a fixed list of methods from the pyarrow stream it wraps,
and ``readinto`` is not on it. Nothing in a default install notices, because the stdlib
gzip reader only ever calls ``read()``. fsspec picks isal's ``IGzipFile`` as its ``gzip``
codec whenever ``isal`` is importable, which any dependency pulling ``xopen`` arranges on
x86-64 and AArch64, and that reader decompresses through ``readinto``: every ``.gz`` file
read through ``file://``, ``s3://``, ``az://``, ``hdfs://`` or ``rsync://`` then failed
with
``AttributeError: 'ArrowFile' object has no attribute 'readinto'``.

The tests below pin the handle contract itself, so they hold whether or not isal is
installed in the environment running them.
"""

import ast
import gzip
import io
import pathlib
import pickle

import fsspec.compression
import pytest
from pyarrow.fs import LocalFileSystem

from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.source.impl.remote import (
    _AzureArrowFSWrapper,
    _R2ArrowFSWrapper,
    _S3CompatibleArrowFSWrapper,
)
from dlt_filesystem.util.fsspec import ReadIntoArrowFSMixin, ReadIntoArrowFSWrapper


def _buffering_gzip(fileobj, **kwargs):
    """Stand in for isal's ``IGzipFile``, which reads its source through ``readinto``.

    ``io.BufferedReader`` is the same demand in the stdlib: it fills its own buffer, so a
    source without ``readinto`` raises before a byte is decompressed.
    """
    return gzip.GzipFile(fileobj=io.BufferedReader(fileobj), **kwargs)


@pytest.fixture
def readinto_gzip_codec(monkeypatch):
    """Register a gzip codec that decompresses through ``readinto``."""
    monkeypatch.setitem(fsspec.compression.compr, "gzip", _buffering_gzip)


def test_arrow_handle_fills_a_caller_supplied_buffer(tmp_path):
    """The read handle implements ``readinto``, not just ``read``."""
    path = tmp_path / "payload.bin"
    path.write_bytes(b"0123456789")

    fs = ReadIntoArrowFSWrapper(LocalFileSystem())
    with fs.open(str(path), "rb") as handle:
        buffer = bytearray(4)
        assert handle.readinto(buffer) == 4
        assert bytes(buffer) == b"0123"


def test_arrow_handle_fills_a_buffer_on_a_non_seekable_stream(tmp_path):
    """`cat_file` and `get_file` open non-seekable streams, which mirror the same way."""
    path = tmp_path / "payload.bin"
    path.write_bytes(b"0123456789")

    fs = ReadIntoArrowFSWrapper(LocalFileSystem())
    with fs.open(str(path), "rb", seekable=False) as handle:
        buffer = bytearray(4)
        assert handle.readinto(buffer) == 4
        assert bytes(buffer) == b"0123"


def test_gzipped_file_loads_under_a_readinto_based_codec(tmp_path, readinto_gzip_codec):
    """A `.gz` file reads end to end when the gzip codec demands ``readinto``."""
    path = tmp_path / "data.json.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write('[{"id": 1}, {"id": 2}]')

    rows = list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))

    assert [row["id"] for row in rows] == [1, 2]


@pytest.mark.parametrize(
    "wrapper",
    [_S3CompatibleArrowFSWrapper, _R2ArrowFSWrapper, _AzureArrowFSWrapper],
)
def test_every_blob_wrapper_carries_the_shim(wrapper):
    """S3, R2 and Azure read through Arrow too, so each inherits the handle contract."""
    assert issubclass(wrapper, ReadIntoArrowFSMixin)


def test_write_handles_are_left_alone(tmp_path):
    """A write handle is not a read handle, and the shim does not pretend otherwise."""
    path = tmp_path / "written.bin"

    fs = ReadIntoArrowFSWrapper(LocalFileSystem())
    with fs.open(str(path), "wb") as handle:
        handle.write(b"payload")

    assert path.read_bytes() == b"payload"


def _bare_arrow_constructions(module: pathlib.Path) -> list[int]:
    """Return the lines where `module` instantiates fsspec's unshimmed wrapper.

    Read as syntax rather than as text, so a call split across lines or written through
    an import alias counts, while the name in a comment, a docstring or a `class` base
    list does not. Subclassing the wrapper is how the shim itself is built.
    """
    tree = ast.parse(module.read_text(encoding="utf-8"))
    names = {"ArrowFSWrapper"} | {
        alias.asname
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if alias.name == "ArrowFSWrapper" and alias.asname
    }

    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id in names)
            or (isinstance(node.func, ast.Attribute) and node.func.attr in names)
        )
    ]


def test_every_arrow_filesystem_in_the_tree_carries_the_shim():
    """No source builds a bare `ArrowFSWrapper`, whichever package it lives in.

    The gap is one line at a construction site, and the sites sit in both distributed
    packages, so a new connector reintroduces it silently. Pinning the construction
    itself catches that where a per-scheme test cannot.
    """
    src = pathlib.Path(__file__).parents[2] / "src"

    offenders = [
        f"{module.relative_to(src)}:{line}"
        for module in sorted(src.rglob("*.py"))
        for line in _bare_arrow_constructions(module)
    ]

    assert offenders == []


def test_hdfs_reads_through_the_shim():
    """HDFS is Arrow-backed as well, and its class survives a round trip through pickle.

    fsspec filesystems are picklable by design, which a class defined inside a function
    would quietly break.
    """
    from dlt_filesystem.source.fsspec.hdfs import HDFSSource

    fs_class = HDFSSource().fs_class

    assert issubclass(fs_class, ReadIntoArrowFSMixin)
    # Pickling a class stores it by import path and verifies the lookup resolves back to
    # the same object, so this fails on a class defined inside a function.
    assert pickle.dumps(fs_class)
