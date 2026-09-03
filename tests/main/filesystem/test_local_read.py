"""``file://`` dispatch, and the ``csv://`` compatibility scheme that shares its reader.

``csv://`` is a CSV-only spelling of the local filesystem source (#301). Everything but
the format restriction is inherited, so most of what is asserted here is *parity*: the
two schemes must resolve the same reader arguments and produce the same records. The
rest pins the boundary the compatibility scheme adds.
"""

import gzip
from unittest.mock import patch

import pytest

from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from dlt_filesystem.source.model import FilesystemReference
from omniload.core.factory import SourceDestinationFactory
from omniload.error import ValidationError
from omniload.source.csv.api import LocalCsvSource


def capture_reader_args(source, uri: str, table: str = "", **kwargs) -> dict:
    """Run ``dlt_source`` with the shared reader stubbed out.

    Returns the ``FilesystemReference`` the parser computed, so the two schemes can be
    compared without touching the filesystem. ``fs`` is reduced to its type: it is a
    fresh wrapper instance per call and compares by identity. The dlt_filesystem suite
    has its own single-source copy of this; the two test packages stay
    import-independent.
    """
    captured: dict = {}

    def fake_reader(ref: FilesystemReference):
        captured.update(ref.__dict__)
        captured["fs"] = type(ref.fs)
        return "SENTINEL"

    with patch("dlt_filesystem.source.core.resource_for_reader", fake_reader):
        assert source.dlt_source(uri, table, **kwargs) == "SENTINEL"
    return captured


# A numeric column, a float column, a null, and a row whose fields are all empty. The
# standalone reader dropped that row and yielded every value as a string; the shared
# reader infers types and keeps the row as all-nulls.
MIXED = "name,age,score,note\nAlice,30,1.5,hi\n,,,\nBob,25,,\n"


def test_factory_dispatches_file_scheme_to_local_source():
    factory = SourceDestinationFactory(
        "file://tests/assets/create_replace.csv", "duckdb:///tmp/x.duckdb"
    )
    assert isinstance(factory.get_source(), LocalFilesystemSource)


def test_factory_dispatches_csv_scheme_to_a_local_filesystem_source():
    """The compatibility scheme keeps its own class, so the CSV-only rule stays
    visible and testable, but it is a ``LocalFilesystemSource``: a later change cannot
    reintroduce a separate reader without failing here."""
    factory = SourceDestinationFactory(
        "csv://tests/assets/create_replace.csv", "duckdb:///tmp/x.duckdb"
    )
    source = factory.get_source()
    assert isinstance(source, LocalCsvSource)
    assert isinstance(source, LocalFilesystemSource)


# Path grammar the compatibility scheme inherits wholesale. Each entry is a (uri, table)
# spec written without its scheme, run through both spellings.
parity_specs = [
    ("tests/assets/create_replace.csv", ""),
    # blob/sftp-style split form: empty URI path, path supplied via --source-table
    ("", "tests/assets/create_replace.csv"),
    ("/data/x.csv", ""),
    ("data/*.csv", ""),
    ("data/**/*.csv", ""),
    ("data/x.csv.gz", ""),
    ("feed.dat#csv", ""),
    ("feed.dat#csv_headless", ""),
    ("feed.dat#csv_duckdb", ""),
    ("vendor#1/data.csv", ""),
]


@pytest.mark.parametrize(
    ("spec", "table"), parity_specs, ids=[f"{s or '<split>'}" for s, _ in parity_specs]
)
def test_csv_scheme_resolves_the_same_reader_arguments_as_file(spec, table):
    via_file = capture_reader_args(LocalFilesystemSource(), f"file://{spec}", table)
    via_csv = capture_reader_args(LocalCsvSource(), f"csv://{spec}", table)
    assert via_csv == via_file


def test_csv_and_file_read_identical_records_and_types(tmp_path):
    """Numbers, nulls and a fully empty row come back the same through both schemes."""
    path = tmp_path / "mixed.csv"
    path.write_text(MIXED)

    via_file = list(LocalFilesystemSource().dlt_source(f"file://{path}", ""))
    via_csv = list(LocalCsvSource().dlt_source(f"csv://{path}", ""))

    assert via_csv == via_file
    assert [{k: type(v) for k, v in row.items()} for row in via_csv] == [
        {k: type(v) for k, v in row.items()} for row in via_file
    ]
    # Inference and empty-row preservation, stated rather than only compared: `age` is an
    # int, not the '30' the standalone reader yielded, and the all-empty row survives.
    assert via_csv[0] == {"name": "Alice", "age": 30, "score": 1.5, "note": "hi"}
    assert via_csv[1] == {"name": None, "age": None, "score": None, "note": None}
    assert len(via_csv) == 3


def test_csv_reads_a_gzipped_file(tmp_path):
    """`.csv.gz` is a path feature of the shared reader, not a format the scheme bars."""
    path = tmp_path / "mixed.csv.gz"
    with gzip.open(path, "wt") as handle:
        handle.write(MIXED)

    rows = list(LocalCsvSource().dlt_source(f"csv://{path}", ""))
    assert [row["name"] for row in rows] == ["Alice", None, "Bob"]


def test_csv_glob_reads_every_matching_file(tmp_path):
    """A glob widens the path selection, not the format, so it stays a CSV selection."""
    (tmp_path / "a.csv").write_text("name\nAlice\n")
    (tmp_path / "b.csv").write_text("name\nBob\n")

    rows = list(LocalCsvSource().dlt_source(f"csv://{tmp_path}/*.csv", ""))
    assert sorted(row["name"] for row in rows) == ["Alice", "Bob"]


# Selections that resolve outside the CSV family. Both probes must reject each of them
# before any reader I/O: `produces_multiple_tables` runs first in `run_ingest`, so a
# check only in `dlt_source` would let a workbook fail as a plural-table mismatch.
non_csv_specs = [
    "a.jsonl",
    "a.dat#jsonl",
    "a.parquet",
    "a.dat#parquet",
    "book.xlsx",
    "data/*.jsonl",
    "a.json",
    "a.bin",
]


@pytest.mark.parametrize("spec", non_csv_specs)
@pytest.mark.parametrize("probe", ["dlt_source", "produces_multiple_tables"])
def test_non_csv_selections_are_rejected_by_both_probes(spec, probe):
    with pytest.raises(ValidationError, match="only reads CSV"):
        getattr(LocalCsvSource(), probe)(f"csv://{spec}", "")


def test_a_workbook_names_the_csv_restriction_not_the_table_count():
    """`produces_multiple_tables` swallows endpoint errors to answer False. The CSV
    restriction is raised outside that block, so `csv://book.xlsx` reports the real
    problem rather than a plural-worksheet destination mismatch."""
    with pytest.raises(ValidationError) as excinfo:
        LocalCsvSource().produces_multiple_tables("csv://book.xlsx", "")
    assert "read_excel" in str(excinfo.value)


def test_file_scheme_still_accepts_every_reader():
    """The hook's default permits everything, so `file://` is unchanged."""
    source = LocalFilesystemSource()
    assert source.produces_multiple_tables("file://a.jsonl", "") is False
    assert capture_reader_args(source, "file://a.jsonl")["reader_name"] == "read_jsonl"


def test_csv_rejects_a_row_level_incremental_key():
    """The standalone reader compared `DictReader` strings against CLI-parsed datetimes.
    The shared source has no row cursor at all and says so, identically for both
    spellings."""
    with pytest.raises(ValueError, match="incrementality on its own"):
        LocalCsvSource().dlt_source(
            "csv://tests/assets/create_replace.csv",
            "",
            incremental_key=None,
            requested_incremental_key="date",
        )


def test_csv_inherits_the_filesystem_disposition_contract():
    """Which is what makes an unqualified rerun append rather than replace, and what
    lets an explicit `--incremental-strategy replace` through."""
    source = LocalCsvSource()
    assert source.handles_incrementality() is True
    assert source.honours_run_disposition() is True
    assert source.supports_filesystem_incremental() is True
