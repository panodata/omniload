import datetime
import decimal
import gzip
import importlib.util
import re
import zoneinfo

import dateutil.tz
import pendulum
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


def _system_zone(name):
    """A zone only the system tz database loads (Vortex does not carry it), or None."""
    try:
        return zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError):
        return None


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


def test_read_chunks_never_exceed_chunksize(tmp_path, monkeypatch):
    """The reader caps every chunk itself, whatever batch size the scan returns.

    vortex-data 0.86 honours `batch_size` exactly, so the scan is made to return one
    oversized batch here; otherwise the cap would never be exercised.
    """
    import vortex as vx

    path = tmp_path / "data.vortex"
    write_vortex(str(path), [{"id": 0}])
    oversized = pa.RecordBatch.from_pylist([{"id": i} for i in range(7)])

    class OneBigBatch:
        def scan(self, *args, **kwargs):
            class Batches:
                def to_arrow(self):
                    return iter([oversized])

            return Batches()

    monkeypatch.setattr(vx, "open", lambda *a, **k: OneBigBatch())
    chunks = list(
        read_vortex(iter([FileItemStub(path)]), chunksize=3)  # ty: ignore[invalid-argument-type]
    )
    assert [len(chunk) for chunk in chunks] == [3, 3, 1]
    assert [row["id"] for chunk in chunks for row in chunk] == list(range(7))


def test_read_passes_chunksize_to_the_scan(tmp_path, monkeypatch):
    """The scan batches at `chunksize` itself, so the slice loop is a cap, not the
    mechanism."""
    import vortex as vx

    path = tmp_path / "data.vortex"
    write_vortex(str(path), [{"id": i} for i in range(5)])
    requested = []
    real_open = vx.open

    class Spy:
        def __init__(self, inner):
            self._inner = inner

        def scan(self, *args, **kwargs):
            requested.append(kwargs.get("batch_size"))
            return self._inner.scan(*args, **kwargs)

    monkeypatch.setattr(vx, "open", lambda *a, **k: Spy(real_open(*a, **k)))
    list(read_vortex(iter([FileItemStub(path)]), chunksize=2))  # ty: ignore[invalid-argument-type]
    assert requested == [2]


def test_read_local_file_in_place(tmp_path, monkeypatch):
    """A plain local file is opened where it is, with no staged copy."""
    import tempfile

    path = tmp_path / "local.vortex"
    write_vortex(str(path), [{"id": 1}])

    def no_staging(*args, **kwargs):
        raise AssertionError("a plain local file was staged")

    monkeypatch.setattr(tempfile, "TemporaryDirectory", no_staging)
    assert _read_via_source(path) == [{"id": 1}]


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
        dateutil.tz.tzoffset(None, 12 * 3600),
        pendulum.FixedTimezone(12 * 3600),
        dateutil.tz.tzoffset("CUSTOM", 12 * 3600),
        dateutil.tz.tzoffset("America/New_York", 12 * 3600),
        pytest.param(
            _system_zone("right/UTC"),
            marks=pytest.mark.skipif(
                _system_zone("right/UTC") is None,
                reason="no system tz database with right/ zones",
            ),
            id="system-only-zone",
        ),
        zoneinfo.ZoneInfo("Pacific/Auckland"),
    ],
    ids=[
        "utc",
        "fixed-offset-east",
        "fixed-offset-west",
        "dateutil-offset",
        "pendulum-offset",
        "custom-named-offset",
        "offset-labelled-as-a-zone",
        "system-only-zone",
        "named-zone",
    ],
)
def test_read_adversarial_values_are_normalized(tmp_path, tz):
    """A timezone-aware datetime keeps its instant, and a Decimal passes through.

    A fixed offset is the case that matters: Vortex looks a timezone up by name, has
    none for `+12:00`, a custom name or a system-only zone such as `right/UTC`, and
    panics, so the writer stores the same instant in UTC. A named zone keeps its zone.
    """
    doc = {
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=tz),
        "amt": decimal.Decimal("3.14"),
    }
    path = tmp_path / "adv.vortex"
    write_vortex(str(path), [doc])
    row = _read_via_source(path)[0]
    assert row["when"] == doc["when"]
    if isinstance(tz, zoneinfo.ZoneInfo) and tz.key in zoneinfo.available_timezones():
        assert str(row["when"].tzinfo) == tz.key
    else:
        assert row["when"].utcoffset() == datetime.timedelta(0)
    assert row["amt"] == decimal.Decimal("3.14")


def test_write_fixed_offset_leaves_the_callers_rows_alone(tmp_path):
    when = datetime.datetime(
        2020, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=12))
    )
    rows = [{"when": when, "nested": {"at": [when], "pair": (when, 1)}}]
    write_vortex(str(tmp_path / "out.vortex"), rows)
    assert rows == [{"when": when, "nested": {"at": [when], "pair": (when, 1)}}]
    assert rows[0]["when"].tzinfo is when.tzinfo


def test_a_zone_the_host_knows_but_vortex_does_not_is_written_as_utc(
    tmp_path, monkeypatch
):
    """The host's zone list and Vortex's compiled-in one can disagree: a zone newer than
    Vortex's copy is on the list and panics on write. `right/UTC` stands in for one,
    listed here as available, since Vortex rejects it the same way."""
    import dlt_filesystem.target.writer as writer

    zone = _system_zone("right/UTC")
    if zone is None:
        pytest.skip("no system tz database with right/ zones")
    listed = writer._available_zone_names() | {"right/UTC"}
    monkeypatch.setattr(writer, "_available_zone_names", lambda: listed)
    writer._vortex_writes_zone.cache_clear()

    when = datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=zone)
    path = tmp_path / "unknown-zone.vortex"
    write_vortex(str(path), [{"when": when}])
    row = _read_via_source(path)[0]
    assert row["when"] == when
    assert row["when"].utcoffset() == datetime.timedelta(0)


def test_fixed_offsets_are_converted_inside_nested_values():
    from dlt_filesystem.target.writer import _utc_unnamed_zones

    east = datetime.timezone(datetime.timedelta(hours=12))
    when = datetime.datetime(2020, 1, 2, tzinfo=east)
    converted = _utc_unnamed_zones([{"a": {"b": [when]}, "c": (when,)}])
    assert converted[0]["a"]["b"][0].tzinfo is datetime.timezone.utc
    assert converted[0]["c"][0].tzinfo is datetime.timezone.utc
    assert converted[0]["a"]["b"][0] == when


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
