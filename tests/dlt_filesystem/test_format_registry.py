import inspect
from typing import Any
from unittest.mock import patch

import dlt
import fsspec
import pytest

from dlt_filesystem.source.adapter import filesystem, readers
from dlt_filesystem.source.format.readers import read_csv
from dlt_filesystem.source.format.registry import (
    FORMAT_TO_READER,
    READER_REGISTRATIONS,
    ReaderRegistration,
    _build_format_map,
)

EXPECTED_FORMAT_TO_READER = {
    "csv": "read_csv",
    "csv_headless": "read_csv_headless",
    "json": "read_json",
    "jsonl": "read_jsonl",
    "ods": "read_ods",
    "orc": "read_orc",
    "parquet": "read_parquet",
    "bson": "read_bson",
    "xlsx": "read_excel",
    "csv_duckdb": "read_csv_duckdb",
    "cbor": "read_cbor",
    "msgpack": "read_msgpack",
    "xml": "read_xml",
    "yaml": "read_yaml",
    "yml": "read_yaml",
}
EXPECTED_READER_NAMES = (
    "read_csv",
    "read_csv_headless",
    "read_excel",
    "read_ods",
    "read_json",
    "read_jsonl",
    "read_bson",
    "read_msgpack",
    "read_cbor",
    "read_xml",
    "read_yaml",
    "read_parquet",
    "read_csv_duckdb",
    "read_orc",
)


def _reader_source():
    return readers("memory://bucket", fsspec.filesystem("memory"), file_glob="*.none")


def test_format_routes_are_explicit_and_ordered():
    """Routing changes must update the expected public format map deliberately."""
    assert list(FORMAT_TO_READER.items()) == list(EXPECTED_FORMAT_TO_READER.items())


def test_registry_and_transformers_agree_in_both_directions():
    registered_names = {
        registration.reader_name for registration in READER_REGISTRATIONS
    }
    transformer_names = set(_reader_source().resources)

    assert transformer_names - registered_names == set()
    assert registered_names - transformer_names == set()
    assert tuple(_reader_source().resources) == EXPECTED_READER_NAMES


def test_generated_read_csv_matches_literal_transformer_metadata():
    """Loop construction must preserve dlt metadata from the former literal wiring."""
    fs = fsspec.filesystem("memory")
    source = readers("memory://bucket", fs, file_glob="*.none")
    generated = source.resources["read_csv"]
    literal = filesystem("memory://bucket", fs, file_glob="*.none") | dlt.transformer(
        name="read_csv", max_table_nesting=0
    )(read_csv)
    generated_dynamic: Any = generated
    literal_dynamic: Any = literal

    assert generated.name == literal.name == "read_csv"
    assert generated.table_name == literal.table_name == "read_csv"
    assert generated.section == literal.section == "readers"
    assert generated.max_table_nesting == literal.max_table_nesting == 0
    assert generated._hints == literal._hints
    assert inspect.signature(generated) == inspect.signature(literal)
    assert inspect.signature(generated_dynamic._pipe.gen) == inspect.signature(
        literal_dynamic._pipe.gen
    )
    assert (
        generated_dynamic.__SPEC__.__module__,
        generated_dynamic.__SPEC__.__qualname__,
        generated_dynamic.__SPEC__.__annotations__,
        inspect.signature(generated_dynamic.__SPEC__),
    ) == (
        literal_dynamic.__SPEC__.__module__,
        literal_dynamic.__SPEC__.__qualname__,
        literal_dynamic.__SPEC__.__annotations__,
        inspect.signature(literal_dynamic.__SPEC__),
    )
    assert tuple(source.with_resources("read_csv").selected_resources) == ("read_csv",)


def test_duplicate_format_keys_are_rejected_during_map_construction():
    registrations = (
        ReaderRegistration("read_first", ("csv",), transformer_order=0),
        ReaderRegistration("read_second", ("csv",), transformer_order=1),
    )

    with pytest.raises(ValueError, match="Duplicate file format registration: csv"):
        _build_format_map(registrations)


def test_unresolvable_reader_name_is_rejected_when_source_builds():
    registration = ReaderRegistration("read_missing", ("missing",), transformer_order=0)

    with (
        patch("dlt_filesystem.source.adapter.READER_REGISTRATIONS", (registration,)),
        pytest.raises(
            ValueError, match="Reader function 'read_missing' is not defined"
        ),
    ):
        _reader_source()
