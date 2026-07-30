from pathlib import Path

import duckdb
import pytest
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_filesystem.source.error import NoFilesFoundError
from omniload import run_ingest
from tests.util.common import has_exception


def _load(source: str | Path, dest: Path, **kwargs):
    return run_ingest(
        source_uri=f"file://{source}",
        source_table="people",
        dest_uri=f"duckdb:///{dest}",
        dest_table="out.people",
        progress="log",
        **kwargs,
    )


def _table_exists(dest: Path) -> bool:
    with duckdb.connect(str(dest)) as db:
        row = db.execute(
            """
            select count(*)
            from information_schema.tables
            where table_schema = 'out' and table_name = 'people'
            """
        ).fetchone()
    assert row is not None
    return row[0] == 1


def test_missing_concrete_file_fails_and_names_selection(tmp_path):
    source = tmp_path / "missing.csv"

    with pytest.raises(PipelineStepFailed) as exc_info:
        _load(source, tmp_path / "warehouse.duckdb")

    assert has_exception(exc_info, NoFilesFoundError)
    assert source.parent.as_uri() in str(exc_info.value)
    assert "missing.csv" in str(exc_info.value)


def test_unmatched_glob_succeeds_without_creating_a_table(tmp_path):
    dest = tmp_path / "warehouse.duckdb"

    result = _load(tmp_path / "*.csv", dest)

    assert result is not None
    assert not _table_exists(dest)


def test_header_only_file_succeeds_without_creating_a_table(tmp_path):
    source = tmp_path / "header-only.csv"
    source.write_text("name\n")
    dest = tmp_path / "warehouse.duckdb"

    result = _load(source, dest)

    assert result is not None
    assert not _table_exists(dest)


def test_directory_named_like_a_file_fails(tmp_path):
    source = tmp_path / "directory.csv"
    source.mkdir()

    with pytest.raises(PipelineStepFailed) as exc_info:
        _load(source, tmp_path / "warehouse.duckdb")

    assert has_exception(exc_info, NoFilesFoundError)


def test_missing_extensionless_file_with_format_hint_fails(tmp_path):
    source = f"{tmp_path / 'missing'}#csv"

    with pytest.raises(PipelineStepFailed) as exc_info:
        _load(source, tmp_path / "warehouse.duckdb")

    assert has_exception(exc_info, NoFilesFoundError)


@pytest.mark.parametrize(
    "source",
    [
        "baz?.csv",
        "vendor#1/*.csv",
        "da?ta/y=1/*.csv",
    ],
)
def test_unmatched_ambiguous_glob_stays_empty_safe(tmp_path, source):
    result = _load(tmp_path / source, tmp_path / "warehouse.duckdb")

    assert result is not None
