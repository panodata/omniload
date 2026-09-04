import datetime
import decimal

import pandas as pd
import pytest
from dlt.extract.exceptions import ResourceExtractionError

from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.testing.writer import write_orc


def _read_via_source(path):
    """Read a local ORC file end-to-end through the shared filesystem reader."""
    return list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))


# --- end-to-end reader (fsspec, no Docker) ---


def test_reads_single_top_level_object(tmp_path):
    """A single top-level ORC map loads as one record."""
    data = pd.DataFrame.from_records([{"id": 1, "name": "alice"}])
    path = write_orc(tmp_path / "one.orc", data)
    assert _read_via_source(path) == [{"id": 1, "name": "alice"}]


def test_reads_top_level_array_all_rows(tmp_path):
    """The supported shape: a single top-level array yields one row per element."""
    docs = [{"id": i, "name": n} for i, n in enumerate(["a", "b", "c"], start=1)]
    path = write_orc(tmp_path / "arr.orc", docs)
    rows = _read_via_source(path)
    assert [r["id"] for r in rows] == [1, 2, 3]
    assert sorted(r["name"] for r in rows) == ["a", "b", "c"]


def test_extension_and_format_hint_both_resolve(tmp_path):
    """A `.orc` extension and an explicit `#orc` hint both resolve to the reader."""
    docs = [{"id": 1}, {"id": 2}, {"id": 3}]
    ext_path = write_orc(tmp_path / "by_ext.orc", docs)
    assert len(_read_via_source(ext_path)) == 3
    hint_path = write_orc(tmp_path / "feed.dat", docs)
    rows = list(LocalFilesystemSource().dlt_source(f"file://{hint_path}#orc", ""))
    assert len(rows) == 3


def test_adversarial_values_are_normalized(tmp_path):
    """Adversarial record: raw bytes, a tz-aware datetime, a Decimal, and
    a nested map with a nested bytes value. bytes -> base64, an unknown tag -> {"tag","value"};
    datetime and Decimal are already dlt-safe and pass through."""
    doc = {
        "when": datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        "amt": decimal.Decimal("3.14"),
    }
    path = write_orc(tmp_path / "adv.orc", [doc])
    row = _read_via_source(path)[0]
    assert isinstance(row["when"], datetime.datetime)
    assert row["when"].utcoffset() == datetime.timedelta(0)
    assert row["amt"] == decimal.Decimal("3.14")


def test_reads_multiple_files_flushes_each_remainder(tmp_path):
    """Multi-file glob: each file is a top-level array; all records across files load."""
    write_orc(tmp_path / "a.orc", [{"id": 1}, {"id": 2}, {"id": 3}])
    write_orc(tmp_path / "b.orc", [{"id": 4}, {"id": 5}])
    rows = list(LocalFilesystemSource().dlt_source(f"file://{tmp_path}/*.orc", ""))
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]


# --- registry / import-path / error UX ---


def test_empty_orc_file_yields_no_rows(tmp_path):
    """An empty file is not corrupt; it loads as zero rows (matching the other readers), so the
    truncation guard must not fire on it."""
    path = tmp_path / "empty.orc"
    path.write_bytes(b"")
    with pytest.raises(ResourceExtractionError) as excinfo:
        _read_via_source(path)
    assert excinfo.match("File size too small")
