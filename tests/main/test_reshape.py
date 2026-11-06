"""Docker-free unit tests for the reshape codec engine dispatch (issue #112 harness).

The end-to-end behaviour of each lane is covered by the integration tests in
``tests/warehouse/db/test_sample_airbnb.py``; these check only ``create_reshape_mapper``'s
spec parsing, the per-row vs batch classification, and the error contract the CLI relies on
(spec errors are ``ValueError``, which ``omniload/api.py`` converts to ``ValidationError``).
"""

import pytest

from omniload.codec.reshape import ReshapeMapper, create_reshape_mapper


def test_callable_passes_through_as_per_row():
    def reshape(doc):
        return doc

    mapper = create_reshape_mapper(reshape)
    assert isinstance(mapper, ReshapeMapper)
    assert mapper.batch is False
    assert mapper.fn is reshape


def test_python_engine_is_per_row():
    mapper = create_reshape_mapper("python:tests.warehouse.db.sample_airbnb:reshape")
    assert mapper.batch is False
    assert callable(mapper.fn)


def test_polars_engine_is_batch():
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")
    mapper = create_reshape_mapper("polars:select:_id,name")
    assert mapper.batch is True
    assert callable(mapper.fn)


def test_jq_engine_is_per_row():
    pytest.importorskip("tikray")
    mapper = create_reshape_mapper("jq:{_id: ._id}")
    assert mapper.batch is False
    assert callable(mapper.fn)


def test_missing_engine_separator_raises_valueerror():
    with pytest.raises(ValueError):
        create_reshape_mapper("just-a-string-no-colon")


def test_unknown_engine_raises_valueerror():
    with pytest.raises(ValueError):
        create_reshape_mapper("nope:whatever")


def test_empty_polars_spec_raises_valueerror():
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")
    # The CLI relies on this being a ValueError (api.py wraps it as ValidationError).
    with pytest.raises(ValueError):
        create_reshape_mapper("polars:")


def test_python_spec_without_callable_part_raises_valueerror():
    with pytest.raises(ValueError):
        create_reshape_mapper("python:no_callable_after_module")


def test_missing_attribute_raises_valueerror_not_attributeerror():
    """A named callable that is not there is a spec error, not a traceback.

    `api.py` converts only ValueError and ImportError into ValidationError, so an
    AttributeError escaping here reaches the CLI as a stack trace.
    """
    with pytest.raises(ValueError):
        create_reshape_mapper("python:builtins:not_present")


def test_extra_colon_in_python_spec_raises_valueerror():
    """`partition` puts the tail in the attribute name, which must not raise."""
    with pytest.raises(ValueError):
        create_reshape_mapper("python:builtins:print:extra")


def test_polars_recipe_registration_is_idempotent():
    """Building a second mapper must not trip macropipe's duplicate-name guard."""
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")

    create_reshape_mapper("polars:select:a")
    create_reshape_mapper("polars:select:a")


def test_a_foreign_recipe_of_the_same_name_is_reported():
    """A name already held by someone else is named, not silently accepted.

    Skipping on "already present" is what makes registration idempotent, so the
    owner has to be checked too: otherwise a third party occupying the name would
    silently take over the reshape.
    """
    pytest.importorskip("macropipe")
    pytest.importorskip("polars")

    from macropipe.registry import (  # ty: ignore[import-not-found, unused-ignore-comment, unused-ignore-comment]
        Registry,
    )

    def cast_number(lazy_frame, column_names):  # pragma: no cover - never called
        raise AssertionError("the foreign recipe must not be invoked")

    cast_number.__module__ = "somebody.else"
    # Snapshot both names, not just the injected one: registration walks the pair
    # in order, so the raise on `cast_number` leaves `geojson_point_flatten`
    # behind, and a test that fails must not hand the next one a mutated registry.
    names = ("geojson_point_flatten", "cast_number")
    saved = {name: Registry.r.get(name) for name in names}
    Registry.r["cast_number"] = cast_number
    try:
        with pytest.raises(ValueError, match="somebody.else"):
            create_reshape_mapper("polars:select:a")
    finally:
        for name, original in saved.items():
            if original is None:
                Registry.r.pop(name, None)
            else:
                Registry.r[name] = original


def test_as_yield_map_rowifies_an_arrow_batch():
    """A per-row mapper must survive an item that is an Arrow table, not a dict.

    dlt's `MapItem` enumerates a Python list and passes anything else through
    whole, so a source on the default `pyarrow` SQL backend (or `mmap`) hands the
    mapper a `pyarrow.Table`. Without rowification the mapper sees one Table where
    it expects one document.
    """
    pa = pytest.importorskip("pyarrow")

    seen = []

    def per_row(doc):
        seen.append(type(doc).__name__)
        return {"id": doc["id"], "doubled": doc["id"] * 2}

    mapper = create_reshape_mapper(per_row)
    table = pa.table({"id": [1, 2, 3]})

    out = list(mapper.as_yield_map()(table))

    assert seen == ["dict", "dict", "dict"]
    assert out == [
        {"id": 1, "doubled": 2},
        {"id": 2, "doubled": 4},
        {"id": 3, "doubled": 6},
    ]


def test_as_yield_map_passes_a_plain_document_through():
    """The dict path stays one document in, one out."""
    mapper = create_reshape_mapper(lambda doc: {"id": doc["id"]})

    assert list(mapper.as_yield_map()({"id": 7, "drop": "me"})) == [{"id": 7}]
