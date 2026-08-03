import sqlite3
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import urlencode

import duckdb
import pytest
from fsspec.implementations.memory import MemoryFileSystem
from sqlalchemy.engine import make_url

from dlt_filesystem.staging import RemoteObject
from omniload import run_ingest
from omniload.source.sql_database.remote import (
    _database_uri,
    dry_run_database_uri,
    parse_remote_database_uri,
    resolve_database_engine,
)


class _IsolatedMemoryFileSystem(MemoryFileSystem):
    """Keep remote-database fixtures out of fsspec's shared memory store."""

    store: ClassVar[dict[str, Any]] = {}

    def __init__(self) -> None:
        super().__init__()
        self.pseudo_dirs = [""]


def _remote_uri(object_name: str = "database.duckdb") -> str:
    query = urlencode(
        {
            "access_key_id": "access",
            "secret_access_key": "secret",
        }
    )
    return f"s3://bucket/path/{object_name}?{query}"


def test_parse_remote_database_uri_reads_the_object_uri_as_written():
    query = urlencode(
        {
            "access_key_id": "+/=&?",
            "secret_access_key": "secret+/=&?",
        }
    )

    remote = parse_remote_database_uri(
        f"s3://bucket/reports/data%3Fpart%3D1.duckdb?{query}"
    )

    assert remote is not None
    assert remote.backend == "s3"
    assert remote.path == "bucket/reports/data?part=1.duckdb"
    assert remote.safe_location == "s3://bucket/reports/data%3Fpart%3D1.duckdb"
    assert remote.storage_options == {
        "access_key_id": ["+/=&?"],
        "secret_access_key": ["secret+/=&?"],
    }
    assert "+/=&?" not in repr(remote)
    assert "secret+/=&?" not in repr(remote)


@pytest.mark.parametrize(
    ("uri", "backend"),
    [
        ("s3://bucket/events.duckdb", "s3"),
        ("r2://bucket/events.ddb", "s3"),
        ("gs://bucket/events.sqlite", "gs"),
        ("az://container/events.sqlite3", "az"),
        ("adls://container/events.db", "az"),
        ("abfss://container/EVENTS.DuckDB", "az"),
    ],
)
def test_parse_remote_database_uri_covers_every_staging_scheme(uri, backend):
    remote = parse_remote_database_uri(uri)

    assert remote is not None
    assert remote.backend == backend


@pytest.mark.parametrize(
    "uri",
    [
        # Database schemes address their own files.
        "sqlite:///tmp/source.sqlite",
        "duckdb:///tmp/source.duckdb",
        "md://analytics?token=secret",
        "motherduck://analytics?token=secret",
        # Local files keep the local source.
        "file://path/to/source.sqlite",
        # Regular objects keep their filesystem source.
        "s3://bucket/path/events.csv",
        "gs://bucket/path/events.parquet",
        "s3://bucket/path/*.csv",
    ],
)
def test_parse_remote_database_uri_leaves_other_sources_alone(uri):
    assert parse_remote_database_uri(uri) is None


@pytest.mark.parametrize(
    ("uri", "message"),
    [
        ("s3://bucket/path/*.duckdb", "names one object"),
        ("s3://bucket/path/data[12].duckdb", "names one object"),
        ("s3://bucket/path/data.duckdb#csv", "must not carry a '#' fragment"),
        ("s3://access:secret@bucket/data.duckdb", "not embedded in the object URI"),
        ("s3:///data.duckdb", "must identify one object"),
    ],
)
def test_parse_remote_database_uri_rejects_ambiguous_selections(uri, message):
    with pytest.raises(ValueError, match=message):
        parse_remote_database_uri(uri)


def test_parse_remote_database_uri_keeps_an_encoded_wildcard_literal():
    remote = parse_remote_database_uri("s3://bucket/path/data%2A.duckdb")

    assert remote is not None
    assert remote.path == "bucket/path/data*.duckdb"


@pytest.mark.parametrize(
    "table",
    ["bucket/path/events.duckdb", "s3://bucket/path/events.sqlite"],
)
def test_parse_remote_database_uri_rejects_the_split_form(table):
    """The object path belongs on the URI: the table names a table inside it."""
    with pytest.raises(ValueError, match="Name a remote database on --source-uri"):
        parse_remote_database_uri("s3://?access_key_id=access", table)


def test_parse_remote_database_uri_leaves_the_split_form_for_files_alone():
    assert parse_remote_database_uri("s3://", "bucket/path/events.csv") is None


def test_resolve_database_engine_reads_the_file_header(tmp_path):
    sqlite_path = tmp_path / "opaque"
    with sqlite3.connect(sqlite_path) as db:
        db.execute("CREATE TABLE widgets (id INTEGER)")
    duckdb_path = tmp_path / "opaque.duckdb"
    with duckdb.connect(duckdb_path) as db:
        db.execute("CREATE TABLE widgets (id INTEGER)")

    # The extension of the remote object never overrides the header.
    assert resolve_database_engine(sqlite_path, "s3://bucket/a.duckdb") == "sqlite"
    assert resolve_database_engine(duckdb_path, "s3://bucket/a.sqlite") == "duckdb"


def test_resolve_database_engine_falls_back_only_for_an_empty_file(tmp_path):
    """A fresh, still-empty database has no header to read."""
    empty = tmp_path / "empty"
    empty.touch()

    assert resolve_database_engine(empty, "s3://bucket/fresh.sqlite") == "sqlite"
    assert resolve_database_engine(empty, "s3://bucket/fresh.ddb") == "duckdb"

    with pytest.raises(ValueError, match="the object is empty"):
        resolve_database_engine(empty, "s3://bucket/fresh.db")


@pytest.mark.parametrize(
    "location",
    ["s3://bucket/export.db", "s3://bucket/export.sqlite"],
)
def test_resolve_database_engine_reports_an_unidentifiable_file(tmp_path, location):
    """A non-empty file is judged by its header, never by its extension."""
    not_a_database = tmp_path / "opaque"
    not_a_database.write_bytes(b"id,name\n1,alpha\n")

    with pytest.raises(ValueError, match="matches neither SQLite nor DuckDB"):
        resolve_database_engine(not_a_database, location)


def test_database_uri_rejects_question_mark_in_staged_path(tmp_path):
    with pytest.raises(ValueError, match="must not contain '\\?'"):
        _database_uri("sqlite", tmp_path / "staging ? root" / "database")


def test_run_ingest_keeps_staged_database_through_pipeline_and_cleans_it_up(
    tmp_path, mocker
):
    filesystem = _IsolatedMemoryFileSystem()
    filesystem.store.clear()
    filesystem.pipe_file("bucket/path/database.duckdb", b"\x00" * 8 + b"DUCK" + b"rest")
    mocker.patch("dlt_filesystem.staging._filesystem_for", return_value=filesystem)
    staged_paths = []

    def fake_run(jr, pipelines_dir, *, is_pipelines_dir_temp):
        staged_url = make_url(jr.source_uri)
        assert staged_url.drivername == "duckdb"
        staged_path = staged_url.database
        assert staged_path is not None
        staged_paths.append(staged_path)
        with open(staged_path, "rb") as staged:
            assert staged.read() == b"\x00" * 8 + b"DUCK" + b"rest"
        return "loaded"

    mocker.patch("omniload.api._run_ingest", side_effect=fake_run)

    result = run_ingest(
        source_uri=_remote_uri(),
        dest_uri="duckdb:///destination.duckdb",
        source_table="main.widgets",
        dest_table="out.widgets",
        remote_database_staging_root=str(tmp_path),
    )

    assert result == "loaded"
    assert len(staged_paths) == 1
    assert not Path(staged_paths[0]).exists()
    assert list(tmp_path.iterdir()) == []
    filesystem.store.clear()


def test_run_ingest_remote_sqlite_to_duckdb_end_to_end(tmp_path, mocker):
    source_path = tmp_path / "source.sqlite"
    with sqlite3.connect(source_path) as db:
        db.execute("CREATE TABLE widgets (id INTEGER, name TEXT)")
        db.executemany(
            "INSERT INTO widgets VALUES (?, ?)",
            [(1, "alpha"), (2, "beta"), (3, "gamma")],
        )

    filesystem = _IsolatedMemoryFileSystem()
    filesystem.store.clear()
    filesystem.pipe_file("bucket/path/source.sqlite", source_path.read_bytes())
    mocker.patch("dlt_filesystem.staging._filesystem_for", return_value=filesystem)
    staging_root = tmp_path / "staging #% café"
    destination = tmp_path / "destination.duckdb"

    result = run_ingest(
        source_uri=_remote_uri("source.sqlite"),
        dest_uri=f"duckdb:///{destination}",
        source_table="main.widgets",
        dest_table="out.widgets",
        remote_database_staging_root=str(staging_root),
        progress="log",
    )

    assert result is not None
    with duckdb.connect(destination) as db:
        rows = db.sql("select id, name from out.widgets order by id").fetchall()
    assert rows == [(1, "alpha"), (2, "beta"), (3, "gamma")]
    assert list(staging_root.iterdir()) == []
    filesystem.store.clear()


def test_run_ingest_cleans_staged_database_up_when_pipeline_fails(tmp_path, mocker):
    filesystem = _IsolatedMemoryFileSystem()
    filesystem.store.clear()
    filesystem.pipe_file("bucket/path/database.duckdb", b"\x00" * 8 + b"DUCK")
    mocker.patch("dlt_filesystem.staging._filesystem_for", return_value=filesystem)

    def fail_run(jr, pipelines_dir, *, is_pipelines_dir_temp):
        staged_path = make_url(jr.source_uri).database
        assert staged_path is not None
        assert Path(staged_path).exists()
        raise RuntimeError("pipeline failed")

    mocker.patch("omniload.api._run_ingest", side_effect=fail_run)

    with pytest.raises(RuntimeError, match="pipeline failed"):
        run_ingest(
            source_uri=_remote_uri(),
            dest_uri="duckdb:///destination.duckdb",
            source_table="main.widgets",
            dest_table="out.widgets",
            remote_database_staging_root=str(tmp_path),
        )

    assert list(tmp_path.iterdir()) == []
    filesystem.store.clear()


@pytest.mark.parametrize(
    "source_uri",
    [
        "sqlite:///local.sqlite",
        "duckdb:///local.duckdb",
        "md://analytics?token=secret",
        "s3://bucket/path/events.csv?access_key_id=access&secret_access_key=secret",
    ],
)
def test_run_ingest_other_sources_do_not_construct_stager(source_uri, mocker):
    stage = mocker.patch("omniload.source.sql_database.remote.stage_remote_database")
    mocker.patch("omniload.api._run_ingest", return_value="loaded")

    assert (
        run_ingest(
            source_uri=source_uri,
            dest_uri="duckdb:///destination.duckdb",
            source_table="main.widgets",
            dest_table="out.widgets",
        )
        == "loaded"
    )
    stage.assert_not_called()


def test_run_ingest_dry_run_does_not_download_remote_database(tmp_path, mocker):
    filesystem = mocker.patch("dlt_filesystem.staging._filesystem_for")

    assert (
        run_ingest(
            source_uri=_remote_uri(),
            dest_uri=f"duckdb:///{tmp_path / 'destination.duckdb'}",
            source_table="main.widgets",
            dest_table="out.widgets",
            dry_run=True,
            progress="log",
        )
        is None
    )
    filesystem.assert_not_called()


def test_run_ingest_dry_run_validates_the_sql_source(tmp_path, mocker):
    """A dry run must validate the source the real run uses, not the storage one."""
    captured = []
    mocker.patch(
        "omniload.api._run_ingest",
        side_effect=lambda jr, *args, **kwargs: captured.append(jr.source_uri),
    )

    run_ingest(
        source_uri=_remote_uri(),
        dest_uri=f"duckdb:///{tmp_path / 'destination.duckdb'}",
        source_table="main.widgets",
        dest_table="out.widgets",
        dry_run=True,
    )

    assert captured == ["duckdb:///bucket/path/database.duckdb"]


@pytest.mark.parametrize(
    ("object_name", "expected"),
    [
        ("events.duckdb", "duckdb:///bucket/path/events.duckdb"),
        ("events.sqlite3", "sqlite:///bucket/path/events.sqlite3"),
        # Nothing connects during a dry run, so an ambiguous name may resolve
        # to either engine.
        ("events.db", "sqlite:///bucket/path/events.db"),
    ],
)
def test_dry_run_database_uri_names_a_sql_source(object_name, expected):
    remote = parse_remote_database_uri(_remote_uri(object_name))

    assert remote is not None
    assert dry_run_database_uri(remote) == expected


def test_remote_object_rejects_embedded_credentials():
    with pytest.raises(ValueError, match="not embedded in the object URI"):
        RemoteObject.from_uri("s3://access:secret@bucket/database.duckdb")
