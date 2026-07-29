from unittest.mock import patch

import pytest

from dlt_filesystem.source.model import FilesystemReference
from omniload.source.rsync.api import RsyncSource
from omniload.source.rsync.error import (
    InvalidRsyncUriError,
    UnsupportedTransportError,
)
from tests.rsync.support import RecordingRunner


def capture_reader_args(uri, table="", runner=None, **kwargs):
    """Call ``dlt_source`` with a stubbed reader and return the captured
    :class:`FilesystemReference` fields.
    """
    captured = {}

    def fake_reader(ref: FilesystemReference):
        captured.update(ref.__dict__)
        return "SENTINEL"

    with patch("omniload.source.rsync.api.resource_for_reader", fake_reader):
        result = RsyncSource(runner=runner or RecordingRunner()).dlt_source(
            uri, table, **kwargs
        )

    assert result == "SENTINEL"
    return captured


# --- contract --------------------------------------------------------------


def test_reports_filesystem_incrementality_contract():
    source = RsyncSource(runner=RecordingRunner())
    assert source.handles_incrementality() is True
    assert source.honours_run_disposition() is True
    assert source.supports_filesystem_incremental() is True


def test_rejects_row_level_incremental_key():
    with pytest.raises(ValueError, match="modification time"):
        RsyncSource(runner=RecordingRunner()).dlt_source(
            "rsync://host/mod/x.csv",
            "mod/x.csv",
            requested_incremental_key="updated_at",
        )


def test_unsupported_scheme_raises():
    with pytest.raises(UnsupportedTransportError):
        RsyncSource(runner=RecordingRunner()).dlt_source("ftp://host/x", "x.csv")


def test_missing_remote_path_raises():
    with pytest.raises(InvalidRsyncUriError):
        RsyncSource(runner=RecordingRunner()).dlt_source("rsync://host", "")


def test_unsupported_format_raises_supported_message():
    with pytest.raises(ValueError, match="only supports file formats"):
        capture_reader_args("rsync://host/mod", "mod/data.weird")


# --- planning (reader reference) ------------------------------------------


def test_daemon_plan_sets_reader_glob_namespace_and_endpoint():
    ref = capture_reader_args("rsync://user@host/mod", "mod/exports/**/*.csv")
    assert ref["file_glob"] == "**/*.csv"
    assert ref["reader_name"] == "read_csv"
    assert ref["storage_namespace"] == "rsync:host:873:user"
    assert ref["bucket_url"].endswith(tuple("0123456789abcdef"))


def test_ssh_plan_uses_table_path_and_ssh_namespace():
    ref = capture_reader_args("rsync+ssh://deploy@host", "/srv/data/*.jsonl")
    assert ref["file_glob"] == "*.jsonl"
    assert ref["reader_name"] == "read_jsonl"
    assert ref["storage_namespace"] == "rsync+ssh:host:22:deploy"


def test_format_hint_fragment_selects_reader():
    ref = capture_reader_args("rsync://host/mod", "mod/feed#csv")
    assert ref["reader_name"] == "read_csv"


def test_reader_hints_fragment_is_forwarded():
    ref = capture_reader_args("rsync://host/mod", "mod/book.xlsx#sheet_name=Sheet1")
    assert ref["hints"] == {"sheet_name": "Sheet1"}
    assert ref["reader_name"] == "read_excel"


def test_remote_path_falls_back_to_uri_path_when_table_empty():
    ref = capture_reader_args("rsync+ssh://host/srv/data/*.csv", "")
    assert ref["file_glob"] == "*.csv"
    assert ref["reader_name"] == "read_csv"


def test_filesystem_incremental_flag_is_threaded():
    ref = capture_reader_args(
        "rsync://host/mod", "mod/*.csv", filesystem_incremental=True
    )
    assert ref["filesystem_incremental"] is True


def test_column_types_are_threaded():
    columns = {"id": "bigint"}
    ref = capture_reader_args(
        "rsync://host/mod", "mod/data#csv_headless", column_types=columns
    )
    assert ref["column_types"] == columns
    assert ref["reader_name"] == "read_csv_headless"


# --- end-to-end read (offline, real reader) --------------------------------


def test_end_to_end_reads_staged_csv():
    csv = "id,name\n1,alice\n2,bob\n"
    runner = RecordingRunner(files={"people.csv": csv})

    source = RsyncSource(runner=runner)
    rows = list(source.dlt_source("rsync://user@host/data", "data/people.csv"))

    assert runner.calls == 1
    assert rows == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ]


def test_end_to_end_recursive_glob_reads_nested_files():
    runner = RecordingRunner(
        files={
            "2024/jan.csv": "id,v\n1,10\n",
            "2024/feb.csv": "id,v\n2,20\n",
        }
    )
    source = RsyncSource(runner=runner)
    rows = list(source.dlt_source("rsync+ssh://host", "/exports/**/*.csv"))

    assert sorted(row["id"] for row in rows) == [1, 2]
    assert "-r" in runner.argv


def test_end_to_end_uses_only_matching_files_via_reader_glob():
    runner = RecordingRunner(
        files={
            "keep.csv": "id\n1\n",
            "skip.txt": "not,a,match\n",
        }
    )
    source = RsyncSource(runner=runner)
    rows = list(source.dlt_source("rsync://host/mod", "mod/*.csv"))
    assert rows == [{"id": 1}]
