import inspect
from typing import Any
from unittest.mock import patch

import dlt
import fsspec
import pytest

from dlt_filesystem.source.adapter import filesystem, readers
from dlt_filesystem.source.format.readers import read_csv
from dlt_filesystem.source.format.registry import (
    ADVERTISED_FILE_FORMATS,
    BASE_READER_REGISTRATIONS,
    FORMAT_TO_READER,
    READER_REGISTRATIONS,
    ReaderRegistration,
    _advertised_formats,
    _build_format_map,
    advertised_file_formats,
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
    # One reader under all three extensions Feather V2 travels under.
    "feather": "read_feather",
    "arrow": "read_feather",
    "ipc": "read_feather",
    "avro": "read_avro",
    "vortex": "read_vortex",
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
    "read_feather",
    "read_avro",
    "read_vortex",
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


def test_an_alias_routes_without_joining_the_advertised_set():
    """Both halves of the split, on one synthetic registration.

    Every key routes, so a second extension for a format resolves; only the first is
    named in the "supported formats" message, so the message enumerates formats rather
    than the extensions they answer to.
    """
    registrations = (
        ReaderRegistration("read_thing", ("thing", "thingy"), transformer_order=0),
    )

    assert _build_format_map(registrations) == {
        "thing": "read_thing",
        "thingy": "read_thing",
    }
    assert _advertised_formats(registrations) == ("thing",)


def test_no_registered_alias_is_advertised():
    """The property the synthetic case above pins, asserted on the real registry.

    Non-vacuity is asserted first: with no alias registered anywhere this would pass
    against any implementation, including one that advertises every routing key.
    """
    aliases = {
        key
        for registration in READER_REGISTRATIONS
        for key in registration.format_keys[1:]
    }
    assert aliases, "no registration carries an alias, so this guard proves nothing"
    assert aliases.isdisjoint(advertised_file_formats())
    assert aliases < set(FORMAT_TO_READER), "an alias must still route"


def test_the_advertised_base_set_is_one_entry_per_base_reader():
    assert len(ADVERTISED_FILE_FORMATS) == len(BASE_READER_REGISTRATIONS)
    assert set(ADVERTISED_FILE_FORMATS) <= set(FORMAT_TO_READER)


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


def test_every_registered_reader_has_a_typing_stub_entry():
    """`ReadersSource` is a hand-written `TYPE_CHECKING` stub, so nothing ties it to the
    registry at runtime. A reader missing from it has no signature for a type checker,
    which reads it as an attribute that does not exist."""
    import ast

    from dlt_filesystem.source.format import readers as readers_module

    tree = ast.parse(inspect.getsource(readers_module))
    stub = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == "ReadersSource"
    )
    stubbed = {node.name for node in stub.body if isinstance(node, ast.FunctionDef)}
    registered = {registration.reader_name for registration in READER_REGISTRATIONS}
    assert registered - stubbed == set()


def _without_vortex(monkeypatch):
    """Make `vortex` unimportable and invisible to `find_spec`, as on Python 3.10 or an
    install without the extra."""
    import importlib.util
    import sys

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        if name == "vortex" or name.startswith("vortex."):
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    for name in [m for m in sys.modules if m == "vortex" or m.startswith("vortex.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setitem(sys.modules, "vortex", None)


def test_vortex_is_advertised_only_when_installed(monkeypatch):
    """The format stays routable without its package, so the reader can raise the
    install hint, but a supported-formats message must not claim it."""
    _without_vortex(monkeypatch)
    assert "vortex" not in advertised_file_formats()
    assert FORMAT_TO_READER["vortex"] == "read_vortex"
    assert "vortex" in ADVERTISED_FILE_FORMATS


def test_vortex_reader_without_the_extra_names_the_install(monkeypatch):
    from dlt_filesystem.source.error import MissingDecoderError
    from dlt_filesystem.source.format.readers import read_vortex

    _without_vortex(monkeypatch)
    with pytest.raises(
        MissingDecoderError, match=r"pip install 'dlt-filesystem\[vortex\]'"
    ):
        list(read_vortex(iter([])))
