"""Unit tests for the ``invoke_ingest_command`` test helper.

Docker-free. Proves the CLI arg builder emits ``--source-table`` / ``--dest-table``
only when supplied (keyed on ``is None``, not truthiness), that legacy call shapes
still produce a byte-identical arg list (backward compatibility), and that the
sentinel keeps ``dest_uri`` required while both tables are omittable.
"""

import pytest

from tests.util import _MISSING, build_ingest_args, invoke_ingest_command


def test_both_tables_present():
    args = build_ingest_args(
        "sqlite:///src.db",
        "sqlite:///dest.db",
        source_table="a.src",
        dest_table="b.dst",
    )
    assert args == [
        "ingest",
        "--source-uri",
        "sqlite:///src.db",
        "--source-table",
        "a.src",
        "--dest-uri",
        "sqlite:///dest.db",
        "--dest-table",
        "b.dst",
    ]


def test_source_table_only():
    args = build_ingest_args(
        "sqlite:///src.db",
        "sqlite:///dest.db",
        source_table="a.src",
    )
    assert "--source-table" in args
    assert "--dest-table" not in args
    assert args == [
        "ingest",
        "--source-uri",
        "sqlite:///src.db",
        "--source-table",
        "a.src",
        "--dest-uri",
        "sqlite:///dest.db",
    ]


def test_dest_table_only():
    args = build_ingest_args(
        "sqlite:///src.db",
        "sqlite:///dest.db",
        dest_table="b.dst",
    )
    assert "--source-table" not in args
    assert "--dest-table" in args
    assert args == [
        "ingest",
        "--source-uri",
        "sqlite:///src.db",
        "--dest-uri",
        "sqlite:///dest.db",
        "--dest-table",
        "b.dst",
    ]


def test_neither_table():
    args = build_ingest_args("sqlite:///src.db", "sqlite:///dest.db")
    assert "--source-table" not in args
    assert "--dest-table" not in args
    assert args == [
        "ingest",
        "--source-uri",
        "sqlite:///src.db",
        "--dest-uri",
        "sqlite:///dest.db",
    ]


def test_empty_string_table_is_forwarded():
    """An explicit empty-string table is forwarded (omission keys on ``is None``,
    not truthiness). A truthiness guard would silently drop ``--source-table ""``."""
    args = build_ingest_args(
        "sqlite:///src.db",
        "sqlite:///dest.db",
        source_table="",
        dest_table="",
    )
    assert args == [
        "ingest",
        "--source-uri",
        "sqlite:///src.db",
        "--source-table",
        "",
        "--dest-uri",
        "sqlite:///dest.db",
        "--dest-table",
        "",
    ]


def test_exact_list_legacy_call_with_positional_inc_args():
    """Byte-identical backward-compat proof for a legacy call that also passes
    ``inc_key`` / ``inc_strategy`` (mirrors the 6 such call sites, e.g.
    tests/warehouse/db/test_arrow.py). The old hard-coded literal emitted the
    table flags inline and ``--incremental-strategy`` before ``--incremental-key``;
    the builder must reproduce that exactly."""
    args = build_ingest_args(
        "mmap:///src.arrow",
        "postgresql://dest",
        source_table="whatever",
        dest_table="myschema.output",
        inc_strategy="delete+insert",
        inc_key="id",
    )
    assert args == [
        "ingest",
        "--source-uri",
        "mmap:///src.arrow",
        "--source-table",
        "whatever",
        "--dest-uri",
        "postgresql://dest",
        "--dest-table",
        "myschema.output",
        "--incremental-strategy",
        "delete+insert",
        "--incremental-key",
        "id",
    ]


def test_missing_dest_uri_raises_type_error():
    """``dest_uri`` stays required via the sentinel; omitting it is a caller bug."""
    assert build_ingest_args.__module__  # builder importable
    with pytest.raises(TypeError):
        invoke_ingest_command("sqlite:///src.db", "a.src")


def test_sentinel_is_distinct_object():
    """The sentinel must not collide with any real ``dest_uri`` value."""
    assert _MISSING is not None
    assert _MISSING != ""


def test_omission_shapes_do_not_raise_through_helper():
    """Both omission shapes reach the CLI runner without the helper signature or
    arg builder raising. ``print_output=False`` makes the helper swallow the
    (expected) non-zero exit from the intentionally invalid scheme, so this stays
    Docker-free and only exercises signature + arg-building, not a real ingest."""
    # Omit dest_table (positional short form).
    res = invoke_ingest_command(
        "no-such-scheme://src",
        "a.src",
        "no-such-scheme://dest",
        print_output=False,
    )
    assert hasattr(res, "exit_code")

    # Omit source_table (dest_uri via keyword).
    res = invoke_ingest_command(
        "no-such-scheme://src",
        dest_uri="no-such-scheme://dest",
        print_output=False,
    )
    assert hasattr(res, "exit_code")
