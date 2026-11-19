"""Destination capabilities are checked through a real, resolved dlt pipeline."""

import sqlite3
from unittest.mock import MagicMock

import duckdb
import pytest

from omniload import ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory
from omniload.target.sqlite import SqliteDestination


@pytest.fixture
def source_db(tmp_path):
    path = tmp_path / "source.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO people VALUES (1, 'Alice')")
    return path


def _ingest(source_db, dest_uri, **kwargs):
    kwargs.setdefault("dest_table", "people")
    kwargs.setdefault("incremental_strategy", "replace")
    return run_ingest(
        source_uri=f"sqlite:///{source_db}",
        source_table="main.people",
        dest_uri=dest_uri,
        progress="log",
        **kwargs,
    )


def _capabilities(monkeypatch, **overrides):
    """Override capabilities on a real destination, retaining pipeline resolution."""
    destination = SqliteDestination()
    original = destination.dlt_dest

    def dlt_dest(uri, **kwargs):
        resolved = original(uri, **kwargs)
        resolved.caps_params.update(overrides)
        return resolved

    monkeypatch.setattr(destination, "dlt_dest", dlt_dest)
    monkeypatch.setattr(
        SourceDestinationFactory, "get_destination", lambda self: destination
    )


@pytest.mark.parametrize("dry_run", [False, True])
def test_mongodb_refuses_jsonl_before_constructing_the_source(
    source_db, monkeypatch, dry_run
):
    source = SourceDestinationFactory(
        f"sqlite:///{source_db}", "mongodb://localhost:1/db"
    ).get_source()
    construct = MagicMock(side_effect=AssertionError("source must not be constructed"))
    monkeypatch.setattr(source, "dlt_source", construct)
    monkeypatch.setattr(SourceDestinationFactory, "get_source", lambda self: source)

    with pytest.raises(
        ValidationError, match="Loader file format jsonl is not supported"
    ):
        _ingest(
            source_db,
            "mongodb://localhost:1/db",
            loader_file_format="jsonl",
            dry_run=dry_run,
        )

    construct.assert_not_called()


def test_mongodb_format_suggestions_use_cli_values(source_db):
    with pytest.raises(ValidationError) as exc:
        _ingest(
            source_db,
            "mongodb://localhost:1/db",
            loader_file_format="jsonl",
            dry_run=True,
        )
    assert str(exc.value) == (
        "Loader file format jsonl is not supported by the destination. "
        "Supported loader file formats: parquet."
    )


@pytest.mark.parametrize("loader_file_format", [None, "parquet"])
def test_mongodb_loads_rows_with_a_resolved_destination(
    tmp_path, monkeypatch, loader_file_format
):
    """Keep the factory, pipeline, loader file and MongoDB sink; fake only the server."""
    source = tmp_path / "people.csv"
    source.write_text("id,name\n1,Alice\n")
    client = MagicMock()
    client.__enter__.return_value = client
    monkeypatch.setattr("pymongo.MongoClient", lambda uri: client)

    run_ingest(
        source_uri=f"csv://{source}",
        source_table="people",
        dest_uri="mongodb://localhost:1/db",
        dest_table="people",
        incremental_strategy="replace",
        loader_file_format=loader_file_format,
        progress="log",
    )

    collection = client["db"]["people"]
    collection.delete_many.assert_called_once_with({})
    documents = [
        document
        for call in collection.insert_many.call_args_list
        for document in call.args[0]
    ]
    assert [(doc["id"], doc["name"]) for doc in documents] == [(1, "Alice")]


@pytest.mark.parametrize("loader_file_format", [None, "parquet"])
def test_athena_accepts_its_staging_format(source_db, loader_file_format):
    _ingest(
        source_db,
        "athena://?bucket=s3://example-bucket&access_key_id=testing"
        "&secret_access_key=testing&region_name=us-east-1",
        dest_table="analytics.people",
        loader_file_format=loader_file_format,
        dry_run=True,
    )


@pytest.mark.parametrize("dry_run", [False, True])
def test_athena_refuses_jsonl_with_staging_format_suggestions(source_db, dry_run):
    with pytest.raises(ValidationError) as exc:
        _ingest(
            source_db,
            "athena://?bucket=s3://example-bucket&access_key_id=testing"
            "&secret_access_key=testing&region_name=us-east-1",
            dest_table="analytics.people",
            loader_file_format="jsonl",
            dry_run=dry_run,
        )
    assert str(exc.value) == (
        "Loader file format jsonl is not supported by the destination. "
        "Supported loader file formats: parquet."
    )


def test_direct_loading_does_not_use_staging_formats(source_db, tmp_path, monkeypatch):
    _capabilities(monkeypatch, supported_staging_file_formats=["insert_values"])
    with pytest.raises(ValidationError) as exc:
        _ingest(
            source_db,
            f"sqlite:///{tmp_path / 'dest.db'}",
            loader_file_format="insert_values",
            dry_run=True,
        )
    assert str(exc.value) == (
        "Loader file format insert_values is not supported by the destination. "
        "Supported loader file formats: parquet."
    )


def test_an_instantiated_destination_accepts_its_loader_format(source_db, tmp_path):
    dest = tmp_path / "dest.duckdb"
    _ingest(
        source_db,
        f"duckdb:///{dest}",
        dest_table="out.people",
        loader_file_format="parquet",
    )
    with duckdb.connect(str(dest)) as conn:
        assert conn.sql("SELECT id, name FROM out.people").fetchall() == [(1, "Alice")]


@pytest.mark.parametrize("dry_run", [False, True])
def test_an_instantiated_destination_refuses_an_unsupported_loader_format(
    source_db, tmp_path, dry_run
):
    with pytest.raises(
        ValidationError, match="Loader file format insert_values is not supported"
    ):
        _ingest(
            source_db,
            f"sqlite:///{tmp_path / 'dest.db'}",
            loader_file_format="insert_values",
            dry_run=dry_run,
        )


@pytest.mark.parametrize(
    ("formats", "message"),
    [
        (
            ["typed-jsonl", "model", "parquet", "csv", "future-format"],
            "Supported loader file formats: csv, parquet.",
        ),
        (
            ["typed-jsonl", "model", "future-format"],
            "It has no supported loader file format exposed by '--loader-file-format'.",
        ),
    ],
)
def test_loader_format_suggestions_omit_dlt_only_names(
    source_db, tmp_path, monkeypatch, formats, message
):
    _capabilities(monkeypatch, supported_loader_file_formats=formats)
    with pytest.raises(ValidationError) as exc:
        _ingest(
            source_db,
            f"sqlite:///{tmp_path / 'dest.db'}",
            loader_file_format="insert_values",
            dry_run=True,
        )
    assert str(exc.value) == (
        "Loader file format insert_values is not supported by the destination. "
        + message
    )


@pytest.mark.parametrize(
    ("strategies", "selected", "message"),
    [
        (
            ["delete-insert", "scd2", "upsert", "insert-only", "future-strategy"],
            None,
            "Supported incremental strategies for this table: delete+insert, merge, scd2, upsert.",
        ),
        (
            ["delete-insert", "scd2"],
            ["scd2"],
            "Supported incremental strategies for this table: scd2.",
        ),
        (
            ["upsert", "delete-insert", "scd2"],
            None,
            "Supported incremental strategies for this table: merge, scd2, upsert.",
        ),
        (
            ["insert-only"],
            None,
            "Supported incremental strategies for this table: merge.",
        ),
        (
            ["delete-insert", "future-strategy"],
            ["future-strategy"],
            "It has no supported incremental strategy exposed by '--incremental-strategy' for this table.",
        ),
        (["delete-insert", "scd2"], [], "It has no merge strategy for it."),
        ([], None, "It has no merge strategy for it."),
    ],
)
def test_incremental_strategy_suggestions_use_selected_cli_values(
    source_db, tmp_path, monkeypatch, strategies, selected, message
):
    def selector(available, *, table_schema):
        assert available == strategies
        assert table_schema == {"table_format": None}
        return selected

    _capabilities(
        monkeypatch,
        supported_merge_strategies=strategies,
        merge_strategies_selector=selector if selected is not None else None,
    )
    with pytest.raises(ValidationError) as exc:
        _ingest(
            source_db,
            f"sqlite:///{tmp_path / 'dest.db'}",
            incremental_strategy="cdc",
            dry_run=True,
        )
    assert str(exc.value) == (
        "The 'sqlite' destination does not support incremental strategy 'cdc' for this table. "
        + message
    )
