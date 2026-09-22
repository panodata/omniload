import datetime
import decimal
import gzip
import importlib.util
import re

import pyarrow as pa
import pytest
from dlt.extract.exceptions import ResourceExtractionError
from fsspec.implementations.memory import MemoryFileSystem

from dlt_filesystem.source.adapter import readers
from dlt_filesystem.source.format.readers import read_vortex
from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.target.registry import writer_for_format
from dlt_filesystem.target.writer import write_vortex
from dlt_filesystem.testing.stub import FileItemStub

if importlib.util.find_spec("vortex") is None:
    pytest.skip(
        "Needs the vortex extra (vortex-data, Python 3.11+)", allow_module_level=True
    )


def _read_via_source(path, suffix=""):
    """Read a local Vortex file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}{suffix}", ""))


def _read_via_memory_filesystem(name, payload, tmp_path):
    """Read one object through a real `memory://` listing, the remote staging path.

    Built from the listing rather than by hand, because the listing is what sets the
    `encoding` field that decides whether `open(compression="auto")` decompresses.
    """
    filesystem = MemoryFileSystem()
    remote_path = f"/{tmp_path.name}/{name}"
    filesystem.pipe_file(remote_path, payload)
    try:
        source = readers(
            f"memory://{tmp_path.name}", filesystem, file_glob=name
        ).with_resources("read_vortex")
        return list(source)
    finally:
        filesystem.rm(remote_path)


# --- end-to-end reader (fsspec, no Docker) ---


def test_read_single_row(tmp_path):
    path = tmp_path / "one.vortex"
    write_vortex(str(path), [{"id": 1, "name": "alice"}])
    assert _read_via_source(path) == [{"id": 1, "name": "alice"}]


def test_read_multiple_rows(tmp_path):
    path = tmp_path / "arr.vortex"
    docs = [{"id": i, "name": n} for i, n in enumerate(["a", "b", "c"], start=1)]
    write_vortex(str(path), docs)
    rows = _read_via_source(path)
    assert [row["id"] for row in rows] == [1, 2, 3]
    assert sorted(row["name"] for row in rows) == ["a", "b", "c"]


def test_read_sparse_rows(tmp_path):
    """A column missing from a row is written as a nullable column, and loads as null."""
    path = tmp_path / "sparse.vortex"
    write_vortex(str(path), [{"id": 1, "note": "here"}, {"id": 2}])
    assert _read_via_source(path) == [
        {"id": 1, "note": "here"},
        {"id": 2, "note": None},
    ]


def test_the_extension_resolves_to_the_reader(tmp_path):
    path = tmp_path / "by_ext.vortex"
    write_vortex(str(path), [{"id": 1}, {"id": 2}])
    assert len(_read_via_source(path)) == 2


def test_the_format_hint_resolves_to_the_reader(tmp_path):
    """A bare `#vortex`, not `#format=vortex`: a `key=value` fragment segment is a named
    reader argument, and only a bare token is matched against the format map."""
    path = tmp_path / "feed.dat"
    write_vortex(str(path), [{"id": 1}, {"id": 2}])
    assert len(_read_via_source(path, "#vortex")) == 2


def test_read_multiple_files(tmp_path):
    write_vortex(str(tmp_path / "a.vortex"), [{"id": 1}, {"id": 2}, {"id": 3}])
    write_vortex(str(tmp_path / "b.vortex"), [{"id": 4}, {"id": 5}])
    rows = list(LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.vortex", ""))
    assert sorted(row["id"] for row in rows) == [1, 2, 3, 4, 5]


def test_read_remote_filesystem(tmp_path):
    path = tmp_path / "remote.vortex"
    rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    write_vortex(str(path), rows)
    assert _read_via_memory_filesystem(path.name, path.read_bytes(), tmp_path) == rows


def test_read_gzipped_local_file(tmp_path):
    """A `.vortex.gz` routes to this reader, so it has to be decompressed on the way."""
    plain = tmp_path / "plain.vortex"
    write_vortex(str(plain), [{"id": 1}, {"id": 2}])
    gzipped = tmp_path / "events.vortex.gz"
    gzipped.write_bytes(gzip.compress(plain.read_bytes()))
    assert _read_via_source(gzipped) == [{"id": 1}, {"id": 2}]


def test_read_gzipped_remote_file(tmp_path):
    plain = tmp_path / "plain.vortex"
    write_vortex(str(plain), [{"id": 1}, {"id": 2}])
    payload = gzip.compress(plain.read_bytes())
    assert _read_via_memory_filesystem("r.vortex.gz", payload, tmp_path) == [
        {"id": 1},
        {"id": 2},
    ]


# --- reader hints ---


def test_read_with_chunksize(tmp_path):
    path = tmp_path / "data.vortex"
    write_vortex(str(path), [{"id": i} for i in range(5)])
    chunks = list(
        read_vortex(iter([FileItemStub(path)]), chunksize=2)  # ty: ignore[invalid-argument-type]
    )
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [row["id"] for chunk in chunks for row in chunk] == list(range(5))


def test_read_with_chunksize_flushes_each_file_remainder(tmp_path):
    """A file's short final chunk is yielded before the next file starts."""
    first, second = tmp_path / "a.vortex", tmp_path / "b.vortex"
    write_vortex(str(first), [{"id": 1}, {"id": 2}, {"id": 3}])
    write_vortex(str(second), [{"id": 4}, {"id": 5}])
    chunks = list(
        read_vortex(iter([FileItemStub(first), FileItemStub(second)]), chunksize=2)  # ty: ignore[invalid-argument-type]
    )
    assert [[row["id"] for row in chunk] for chunk in chunks] == [[1, 2], [3], [4, 5]]


def test_read_chunks_never_exceed_chunksize(tmp_path):
    """Vortex's own batching is a hint; the reader caps every chunk itself."""
    path = tmp_path / "many.vortex"
    write_vortex(str(path), [{"id": i} for i in range(50_000)])
    chunks = list(
        read_vortex(iter([FileItemStub(path)]), chunksize=3000)  # ty: ignore[invalid-argument-type]
    )
    assert max(len(chunk) for chunk in chunks) <= 3000
    assert sum(len(chunk) for chunk in chunks) == 50_000


def test_read_an_item_without_a_path_raises(tmp_path):
    """An item the reader cannot open fails loudly, rather than re-reading the file before it."""
    path = tmp_path / "one.vortex"
    write_vortex(str(path), [{"id": 1}])
    rows = []
    with pytest.raises(AttributeError):
        for chunk in read_vortex(iter([FileItemStub(path), object()])):  # ty: ignore[invalid-argument-type]
            rows.extend(chunk)
    assert rows == [{"id": 1}]


def test_read_with_chunksize_invalid(tmp_path):
    """A hint arrives as a string, so the cast is the validation.

    Without it a literal slicing loop fails on a string, and a negative step yields
    nothing at all -- a silent empty read that looks like an empty source.
    """
    path = tmp_path / "one.vortex"
    write_vortex(str(path), [{"id": 1}])
    with pytest.raises(ResourceExtractionError) as excinfo:
        _read_via_source(path, "#chunksize=foo")
    assert excinfo.match("chunksize must be an integer, not foo")


@pytest.mark.parametrize("chunksize", [0, -1])
def test_read_with_non_positive_chunksize(tmp_path, chunksize):
    path = tmp_path / "one.vortex"
    write_vortex(str(path), [{"id": 1}])
    with pytest.raises(ResourceExtractionError) as excinfo:
        _read_via_source(path, f"#chunksize={chunksize}")
    assert excinfo.match(f"chunksize must be greater than zero, not {chunksize}")


def test_read_with_invalid_option(tmp_path):
    """An unknown hint raises rather than being swallowed by a `**kwargs` catch-all."""
    path = tmp_path / "one.vortex"
    write_vortex(str(path), [{"id": 1}])
    with pytest.raises(TypeError) as excinfo:
        _read_via_source(path, "#invalid=true")
    assert excinfo.match(
        re.escape("read_vortex(): got an unexpected keyword argument 'invalid'")
    )


@pytest.mark.parametrize(
    "tz",
    [
        datetime.timezone.utc,
        datetime.timezone(datetime.timedelta(hours=12)),
        datetime.timezone(datetime.timedelta(hours=-5, minutes=-30)),
    ],
    ids=["utc", "fixed-offset-east", "fixed-offset-west"],
)
def test_read_adversarial_values_are_normalized(tmp_path, tz):
    """A timezone-aware datetime keeps its instant, and a Decimal passes through.

    A fixed offset is the case that matters: Vortex looks a timezone up by name, has
    none for `+12:00`, and panics, so the writer stores the same instant in UTC.
    """
    doc = {
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=tz),
        "amt": decimal.Decimal("3.14"),
    }
    path = tmp_path / "adv.vortex"
    write_vortex(str(path), [doc])
    row = _read_via_source(path)[0]
    assert row["when"] == doc["when"]
    assert row["when"].utcoffset() == datetime.timedelta(0)
    assert row["amt"] == decimal.Decimal("3.14")


def test_write_fixed_offset_leaves_the_callers_rows_alone(tmp_path):
    when = datetime.datetime(
        2020, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=12))
    )
    rows = [{"when": when, "nested": {"at": [when]}}]
    write_vortex(str(tmp_path / "out.vortex"), rows)
    assert rows == [{"when": when, "nested": {"at": [when]}}]
    assert rows[0]["when"].tzinfo is when.tzinfo


def test_write_preserves_sparse_rows_and_column_order(tmp_path):
    import vortex as vx

    ROWS = [
        {"id": 1, "name": "Zoë"},
        {"id": 2, "name": "Ōtautahi", "note": "late column"},
    ]

    path = tmp_path / "out.vortex"
    writer_for_format("vortex")(str(path), ROWS)

    table = vx.open(str(path)).to_dataset().to_table()
    assert table.column_names == ["id", "name", "note"]
    assert table.to_pylist() == [
        {"id": 1, "name": "Zoë", "note": None},
        {"id": 2, "name": "Ōtautahi", "note": "late column"},
    ]


def test_write_no_rows_success(tmp_path):
    """Empty rows are written as a zero-column Vortex table."""
    import vortex as vx

    path = tmp_path / "empty.vortex"
    writer_for_format("vortex")(str(path), [])

    table = vx.open(str(path)).to_dataset().to_table()
    assert table.column_names == []
    assert table.to_pylist() == []


def test_write_nonempty_fieldless_rows_success(tmp_path):
    """Nonempty fieldless rows can be represented in Vortex."""
    import vortex as vx

    path = tmp_path / "fieldless.vortex"
    writer_for_format("vortex")(str(path), [{}, {}])

    table = vx.open(str(path)).to_dataset().to_table()
    assert table.column_names == []
    assert table.to_pylist() == [{}, {}]


def test_read_empty_file_raises_resource_extraction_error(tmp_path):
    """An empty Vortex file raises ResourceExtractionError."""
    path = tmp_path / "empty.vortex"
    path.write_bytes(b"")
    with pytest.raises(ResourceExtractionError) as excinfo:
        _read_via_source(path)
    assert excinfo.match("Initial read must be at least")


def test_read_damaged_file_raises_resource_extraction_error(tmp_path):
    """A file that is not Vortex at all raises ResourceExtractionError."""
    path = tmp_path / "damaged.vortex"
    path.write_bytes(b"not vortex at all")
    with pytest.raises(ResourceExtractionError) as excinfo:
        _read_via_source(path)
    assert excinfo.match("Malformed file, invalid magic bytes")


def test_write_u64_too_large(tmp_path):
    """Raise an exception with too large integers."""
    path = tmp_path / "int-too-large.vortex"
    with pytest.raises(OverflowError, match="too large"):
        write_vortex(str(path), [{"n": 2**64 - 1}])


def test_write_nested(tmp_path):
    """dlt keeps a nested column as one JSON column, so this is how the value arrives."""
    import vortex as vx

    path = tmp_path / "nested.vortex"
    write_vortex(str(path), [{"s": {"n": 42}}])

    table = vx.open(str(path)).to_dataset().to_table()
    assert table.schema.field("s").type == pa.struct([("n", pa.int64())])
    assert table.to_pylist() == [{"s": {"n": 42}}]


def test_write_extended_types(tmp_path):
    """What the writer puts on disk for the types the row path carries.

    Measured rather than asserted from the docs, and pinned because a vortex-data change
    would otherwise move these silently. Microsecond precision is kept for times and
    timestamps, a Decimal keeps its own precision and scale, and bytes land as the
    `binary_view` Arrow type. A `timedelta` is not writable: vortex-data has no array
    encoding for an Arrow duration.
    """
    import vortex as vx

    path = tmp_path / "all-types.vortex"
    row = {
        "t": datetime.time(0, 0, 0, 123456),
        "ts": datetime.datetime(2020, 1, 2, 3, 4, 5, 123456),
        "dec": decimal.Decimal("3.14"),
        "blob": b"hi",
    }
    write_vortex(str(path), [row])

    table = vx.open(str(path)).to_dataset().to_table()
    assert table.schema == pa.schema(
        [
            ("t", pa.time64("us")),
            ("ts", pa.timestamp("us")),
            ("dec", pa.decimal128(3, 2)),
            ("blob", pa.binary_view()),
        ]
    )
    assert table.to_pylist() == [row]
