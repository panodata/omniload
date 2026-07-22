import os
import tempfile
from types import SimpleNamespace

import dlt
import duckdb
import pytest
from dlt.sources.filesystem import FileItemDict
from fsspec.implementations.arrow import ArrowFSWrapper
from pyarrow.fs import LocalFileSystem

from dlt_filesystem.source.base import FilesystemSource
from dlt_filesystem.source.core import resource_for_reader
from dlt_filesystem.source.model import FilesystemReference
from omniload import ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory


def _write_csv(path, name: str, mtime: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"name\n{name}\n")
    os.utime(path, (mtime, mtime))


def _load(source, dest, *, dest_uri: str | None = None, **kwargs):
    return run_ingest(
        source_uri=f"file://{source}",
        dest_uri=dest_uri or f"duckdb:///{dest}",
        source_table="people",
        dest_table="out.people",
        filesystem_incremental=True,
        progress="log",
        **kwargs,
    )


def _names(dest) -> list[str]:
    con = duckdb.connect(str(dest))
    try:
        return [
            row[0]
            for row in con.sql("select name from out.people order by name").fetchall()
        ]
    finally:
        con.close()


def _record_file_opens(monkeypatch) -> list[str]:
    opened: list[str] = []
    original_open = FileItemDict.open

    def recording_open(self, *args, **kwargs):
        opened.append(str(self["file_url"]).rsplit("/", 1)[-1])
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(FileItemDict, "open", recording_open)
    return opened


def test_unchanged_files_are_not_opened_and_one_newer_file_is_loaded(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)
    opened = _record_file_opens(monkeypatch)

    _load(source / "*.csv", dest, incremental_strategy="append")
    assert opened == ["a.csv"]
    assert _names(dest) == ["Alice"]

    opened.clear()
    _load(source / "*.csv", dest)
    assert opened == []
    assert _names(dest) == ["Alice"]

    _write_csv(source / "b.csv", "Bob", 1_700_000_100)
    opened.clear()
    _load(source / "*.csv", dest)
    assert opened == ["b.csv"]
    assert _names(dest) == ["Alice", "Bob"]


def test_closed_boundary_uses_file_url_hashes_for_equal_mtimes(tmp_path, monkeypatch):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    boundary = 1_700_000_000
    _write_csv(source / "a.csv", "Alice", boundary)
    _write_csv(source / "b.csv", "Bob", boundary)
    opened = _record_file_opens(monkeypatch)

    _load(source / "*.csv", dest)
    assert sorted(opened) == ["a.csv", "b.csv"]

    _write_csv(source / "c.csv", "Carol", boundary)
    opened.clear()
    _load(source / "*.csv", dest)

    assert opened == ["c.csv"]
    assert _names(dest) == ["Alice", "Bob", "Carol"]


def test_older_backfill_waits_for_full_refresh(tmp_path, monkeypatch):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    _write_csv(source / "new.csv", "New", 1_700_000_100)
    opened = _record_file_opens(monkeypatch)

    _load(source / "*.csv", dest)
    _write_csv(source / "backfill.csv", "Backfill", 1_700_000_000)

    opened.clear()
    _load(source / "*.csv", dest)
    assert opened == []
    assert _names(dest) == ["New"]

    opened.clear()
    _load(source / "*.csv", dest, full_refresh=True)
    assert sorted(opened) == ["backfill.csv", "new.csv"]
    assert _names(dest) == ["Backfill", "New"]


def test_different_globs_have_isolated_cursors_for_the_same_table(tmp_path):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    _write_csv(source / "recent" / "new.csv", "New", 1_700_000_100)
    _write_csv(source / "archive" / "old.csv", "Old", 1_700_000_000)

    _load(source / "recent" / "*.csv", dest)
    _load(source / "archive" / "*.csv", dest)

    assert _names(dest) == ["New", "Old"]


class _NamespacedFilesystemSource(FilesystemSource):
    def __init__(self, directory, namespace):
        self.directory = directory
        self.namespace = namespace

    def dlt_source(self, uri, table, **kwargs):
        return resource_for_reader(
            FilesystemReference(
                fs=ArrowFSWrapper(LocalFileSystem()),
                bucket_url=str(self.directory),
                file_glob="*.csv",
                reader_name="read_csv",
                storage_namespace=self.namespace,
                filesystem_incremental=kwargs["filesystem_incremental"],
            )
        )


class _LocalStateOnlyDestination:
    def __init__(self):
        def discard(items, table) -> None:
            pass

        self.destination = dlt.destination(
            discard,
            name="local_state_only",
            loader_file_format="typed-jsonl",
        )

    def dlt_dest(self, uri, **kwargs):
        return self.destination

    def dlt_run_params(self, uri, table, **kwargs):
        dataset_name, table_name = table.split(".", 1)
        return {"dataset_name": dataset_name, "table_name": table_name}

    def post_load(self) -> None:
        pass


def test_different_storage_namespaces_have_isolated_cursors(tmp_path, monkeypatch):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    recent = source / "recent.csv"
    _write_csv(recent, "New", 1_700_000_100)
    active_source = [_NamespacedFilesystemSource(source, "store-a")]
    monkeypatch.setattr(
        SourceDestinationFactory,
        "get_source",
        lambda self: active_source[0],
    )

    _load(source / "*.csv", dest)

    recent.unlink()
    _write_csv(source / "backfill.csv", "Old", 1_700_000_000)
    active_source[0] = _NamespacedFilesystemSource(source, "store-b")
    _load(source / "*.csv", dest)

    assert _names(dest) == ["New", "Old"]


def test_explicit_pipelines_dir_persists_cursor(tmp_path, monkeypatch):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    pipelines_dir = tmp_path / "pipelines"
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)
    opened = _record_file_opens(monkeypatch)

    _load(source / "*.csv", dest, pipelines_dir=str(pipelines_dir))
    opened.clear()
    _load(source / "*.csv", dest, pipelines_dir=str(pipelines_dir))

    assert opened == []
    assert _names(dest) == ["Alice"]
    assert pipelines_dir.is_dir()


def test_temp_pipeline_dir_requires_destination_state_sync(tmp_path, monkeypatch):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    temp_pipelines_dir = tmp_path / "temporary-pipeline"
    temp_pipelines_dir.mkdir()
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)

    monkeypatch.setattr(
        tempfile, "mkdtemp", lambda *args, **kwargs: str(temp_pipelines_dir)
    )
    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kwargs: SimpleNamespace(
            destination=SimpleNamespace(client_class=object)
        ),
    )

    with pytest.raises(ValidationError, match="Set '--pipelines-dir'"):
        _load(source / "*.csv", dest)

    assert not temp_pipelines_dir.exists()


def test_temp_pipeline_dir_is_removed_when_load_fails(tmp_path, monkeypatch):
    from dlt.common.destination.client import WithStateSync

    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    temp_pipelines_dir = tmp_path / "temporary-pipeline"
    temp_pipelines_dir.mkdir()
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)

    monkeypatch.setattr(
        tempfile, "mkdtemp", lambda *args, **kwargs: str(temp_pipelines_dir)
    )

    def fail_load(*args, **kwargs):
        raise RuntimeError("load failed")

    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kwargs: SimpleNamespace(
            destination=SimpleNamespace(client_class=WithStateSync),
            run=fail_load,
        ),
    )

    with pytest.raises(RuntimeError, match="load failed"):
        _load(source / "*.csv", dest)

    assert not temp_pipelines_dir.exists()


def test_temp_pipeline_dir_is_removed_on_early_validation_error(tmp_path, monkeypatch):
    temp_pipelines_dir = tmp_path / "temporary-pipeline"
    temp_pipelines_dir.mkdir()

    monkeypatch.setattr(
        tempfile, "mkdtemp", lambda *args, **kwargs: str(temp_pipelines_dir)
    )

    with pytest.raises(
        ValidationError, match="cannot be combined with '--yield-limit'"
    ):
        run_ingest(
            source_uri=f"sqlite:///{tmp_path / 'source.db'}",
            dest_uri=f"duckdb:///{tmp_path / 'warehouse.duckdb'}",
            source_table="main.people",
            dest_table="out.people",
            incremental_strategy="scd2",
            yield_limit=1,
            progress="log",
        )

    assert not temp_pipelines_dir.exists()


def test_persistent_pipeline_dir_allows_destination_without_state_sync(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    pipelines_dir = tmp_path / "pipelines"
    pipelines_dir.mkdir()
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)

    monkeypatch.setattr(
        dlt,
        "pipeline",
        lambda **kwargs: SimpleNamespace(
            destination=SimpleNamespace(client_class=object)
        ),
    )

    assert (
        _load(
            source / "*.csv",
            dest,
            pipelines_dir=str(pipelines_dir),
            dry_run=True,
        )
        is None
    )
    assert pipelines_dir.is_dir()


def test_persistent_pipeline_dir_carries_cursor_without_destination_state_sync(
    tmp_path, monkeypatch
):
    source = tmp_path / "source"
    pipelines_dir = tmp_path / "pipelines"
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)
    opened = _record_file_opens(monkeypatch)
    destination = _LocalStateOnlyDestination()
    monkeypatch.setattr(
        SourceDestinationFactory,
        "get_destination",
        lambda self: destination,
    )

    kwargs = {
        "dest_uri": "local-state-only://sink",
        "pipelines_dir": str(pipelines_dir),
    }
    _load(source / "*.csv", tmp_path / "unused", **kwargs)
    assert opened == ["a.csv"]

    opened.clear()
    _load(source / "*.csv", tmp_path / "unused", **kwargs)
    assert opened == []

    _write_csv(source / "b.csv", "Bob", 1_700_000_100)
    _load(source / "*.csv", tmp_path / "unused", **kwargs)
    assert opened == ["b.csv"]


def test_replace_and_non_filesystem_sources_are_rejected(tmp_path):
    source = tmp_path / "source"
    dest = tmp_path / "warehouse.duckdb"
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)

    with pytest.raises(ValidationError, match="requires append loading"):
        _load(source / "*.csv", dest, incremental_strategy="replace")

    with pytest.raises(ValidationError, match="only supported by filesystem"):
        run_ingest(
            source_uri="csv://tests/assets/create_replace.csv",
            dest_uri=f"duckdb:///{dest}",
            source_table="people",
            dest_table="out.people",
            filesystem_incremental=True,
            progress="log",
        )


@pytest.mark.parametrize("dest_scheme", ["csv", "file"])
def test_single_file_destinations_are_rejected(tmp_path, dest_scheme):
    source = tmp_path / "source"
    _write_csv(source / "a.csv", "Alice", 1_700_000_000)

    with pytest.raises(ValidationError, match="cannot write to"):
        _load(
            source / "*.csv",
            tmp_path / "unused",
            dest_uri=f"{dest_scheme}://{tmp_path / 'output.csv'}",
        )
