import datetime
import decimal
import json
import re
from pathlib import Path

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from dlt_filesystem.source.format.readers import read_orc
from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.target.local import LocalFilesystemDestination
from dlt_filesystem.target.registry import writer_for_format
from dlt_filesystem.target.writer import write_jsonl
from dlt_filesystem.testing.stub import FileItemStub
from dlt_filesystem.testing.writer import write_orc


def _read_via_source(path):
    """Read a local ORC file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))


# --- end-to-end reader (fsspec, no Docker) ---


@pytest.mark.xfail(
    raises=ResourceExtractionError,
    reason="PyArrow only handles ORC files with a top-level struct",
    strict=True,
)
def test_read_external_apache_timestamp_fixture():
    """Read an ORC file from https://github.com/apache/orc/tree/main/examples.

    FIXME: pyarrow.lib.ArrowNotImplementedError: Only ORC files with a top-level struct can be handled
    """
    path = "tests/assets/TestOrcFile.testTimestamp.orc"
    data = _read_via_source(path)
    assert len(data) == 3
    assert isinstance(data[0]["TIMESTAMP"], datetime.datetime), (
        "TIMESTAMP should be a datetime"
    )


def test_read_single_row(tmp_path):
    """A single-row ORC file loads as one record."""
    path = write_orc(tmp_path / "one.orc", [{"id": 1, "name": "alice"}])
    assert _read_via_source(path) == [{"id": 1, "name": "alice"}]


def test_read_multiple_rows(tmp_path):
    """An ORC row set yields one record per row."""
    docs = [{"id": i, "name": n} for i, n in enumerate(["a", "b", "c"], start=1)]
    path = write_orc(tmp_path / "arr.orc", docs)
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert sorted(r["name"] for r in rows) == ["a", "b", "c"]


def test_read_extension_and_format_hint_both_resolve(tmp_path):
    """A `.orc` extension and an explicit `#orc` hint both resolve to the reader."""
    docs = [{"id": 1}, {"id": 2}, {"id": 3}]
    ext_path = write_orc(tmp_path / "by_ext.orc", docs)
    assert len(_read_via_source(ext_path)) == 3
    hint_path = write_orc(tmp_path / "feed.dat", docs)
    rows = list(LocalFilesystemSource().dlt_source(f"file://{hint_path}#orc", ""))
    assert len(rows) == 3


def test_read_with_columns_single(tmp_path):
    """Read with column filtering: Use a single column."""
    data = [{"id": 1, "name": "alice", "age": 44}]
    path = write_orc(tmp_path / "one.orc", data)
    rows = list(LocalFilesystemSource().dlt_source(f"file://{path}#columns=name", ""))
    assert rows == [{"name": "alice"}]


def test_read_with_columns_json(tmp_path):
    """Read with column filtering: Use multiple columns."""
    data = [{"id": 1, "name": "alice", "age": 44}]
    path = write_orc(tmp_path / "one.orc", data)
    columns = json.dumps(["id", "name"])
    rows = list(
        LocalFilesystemSource().dlt_source(f"file://{path}#columns={columns}", "")
    )
    assert rows == [{"id": 1, "name": "alice"}]


def test_read_with_columns_unknown(tmp_path):
    """Read with column filtering: Use an unknown column."""
    data = [{"id": 1, "name": "alice", "age": 44}]
    path = write_orc(tmp_path / "one.orc", data)
    with pytest.raises(ResourceExtractionError) as excinfo:
        list(LocalFilesystemSource().dlt_source(f"file://{path}#columns=unknown", ""))
    assert excinfo.match(
        "Invalid column selected unknown. Valid names are age, id, name"
    )


def test_read_with_chunksize_success(tmp_path):
    """Rows are yielded at each chunksize boundary and in a final partial chunk."""
    data = [{"id": i} for i in range(5)]
    path = write_orc(tmp_path / "data.orc", data)
    chunks = list(
        read_orc(iter([FileItemStub(path)]), chunksize=2)  # ty: ignore[invalid-argument-type]
    )
    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [row["id"] for chunk in chunks for row in chunk] == list(range(5))


def test_read_with_chunksize_invalid(tmp_path):
    """Read with invalid chunksize option value."""
    data = [{"id": 1, "name": "alice", "age": 44}]
    path = write_orc(tmp_path / "one.orc", data)
    with pytest.raises(ResourceExtractionError) as excinfo:
        list(LocalFilesystemSource().dlt_source(f"file://{path}#chunksize=foo", ""))
    assert excinfo.match("chunksize must be an integer, not foo")


@pytest.mark.parametrize("chunksize", [0, -1])
def test_read_with_non_positive_chunksize(tmp_path, chunksize):
    """Reject zero and negative chunksize option values."""
    path = write_orc(tmp_path / "one.orc", [{"id": 1}])
    with pytest.raises(ResourceExtractionError) as excinfo:
        list(
            LocalFilesystemSource().dlt_source(
                f"file://{path}#chunksize={chunksize}", ""
            )
        )
    assert excinfo.match(f"chunksize must be greater than zero, not {chunksize}")


def test_read_with_invalid_option(tmp_path):
    """Read with invalid option."""
    data = [{"id": 1, "name": "alice", "age": 44}]
    path = write_orc(tmp_path / "one.orc", data)
    with pytest.raises(TypeError) as excinfo:
        list(LocalFilesystemSource().dlt_source(f"file://{path}#invalid=true", ""))
    assert excinfo.match(
        re.escape("read_orc(): got an unexpected keyword argument 'invalid'")
    )


def test_read_adversarial_values_are_normalized(tmp_path):
    """A timezone-aware datetime and Decimal pass through unchanged."""
    doc = {
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "amt": decimal.Decimal("3.14"),
    }
    path = write_orc(tmp_path / "adv.orc", [doc])
    row = _read_via_source(path)[0]
    assert isinstance(row["when"], datetime.datetime)
    assert row["when"].utcoffset() == datetime.timedelta(0)
    assert row["amt"] == decimal.Decimal("3.14")


def test_read_multiple_files_flushes_each_remainder(tmp_path):
    """A multi-file glob loads all rows across files."""
    write_orc(tmp_path / "a.orc", [{"id": 1}, {"id": 2}, {"id": 3}])
    write_orc(tmp_path / "b.orc", [{"id": 4}, {"id": 5}])
    rows = list(LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.orc", ""))
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]


# --- registry / import-path / error UX ---


def test_read_empty_orc_file_raises_resource_extraction_error(tmp_path):
    """An empty ORC file raises ResourceExtractionError."""
    path = tmp_path / "empty.orc"
    path.write_bytes(b"")
    with pytest.raises(ResourceExtractionError) as excinfo:
        _read_via_source(path)
    assert excinfo.match("File size too small")


def test_write_preserves_sparse_rows_and_column_order(tmp_path):
    from pyarrow import orc

    ROWS = [
        {"id": 1, "name": "Zoë"},
        {"id": 2, "name": "Ōtautahi", "note": "late column"},
    ]

    path = tmp_path / "out.orc"
    writer_for_format("orc")(str(path), ROWS)

    table = orc.ORCFile(path).read()
    assert table.column_names == ["id", "name", "note"]
    assert table.to_pylist() == [
        {"id": 1, "name": "Zoë", "note": None},
        {"id": 2, "name": "Ōtautahi", "note": "late column"},
    ]


def test_write_of_no_rows_is_valid(tmp_path):
    from pyarrow import orc

    path = tmp_path / "empty.orc"
    writer_for_format("orc")(str(path), [])

    assert orc.ORCFile(path).read().num_rows == 0


def test_write_rejects_nonempty_fieldless_rows(tmp_path):
    """Nonempty fieldless rows cannot be represented in ORC."""
    output = tmp_path / "fieldless.orc"

    with pytest.raises(ValueError, match="requires at least one column"):
        writer_for_format("orc")(str(output), [{}, {}])

    assert not output.exists()


def test_write_destination_round_trips_without_dlt_columns(tmp_path):
    destination = LocalFilesystemDestination()
    output_path = tmp_path / "out.orc"
    destination.dlt_dest(f"file://{output_path}")
    destination.dataset_name, destination.table_name = "public", "rows"
    table_dir = Path(destination.temp_path) / "public" / "rows"
    table_dir.mkdir(parents=True)
    rows = [
        {"id": 1, "name": "alice", "_dlt_id": "internal"},
        {"id": 2, "name": "bob", "note": "later", "_dlt_load_id": "internal"},
    ]
    write_jsonl(str(table_dir / "load.jsonl"), rows)
    destination.post_load()

    assert list(LocalFilesystemSource().dlt_source(f"file://{output_path}", "")) == [
        {"id": 1, "name": "alice", "note": None},
        {"id": 2, "name": "bob", "note": "later"},
    ]
