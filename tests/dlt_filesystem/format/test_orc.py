import pandas as pd

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
