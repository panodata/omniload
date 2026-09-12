"""What `omniload.api` sends a source's `dlt_source`, by declared vocabulary.

The package-level property (`dlt_source` consumes exactly its declared names and
passes everything else straight to the connector) lives in
`tests/dlt_filesystem/test_source_option_ownership.py`, driven by calling
`dlt_source` directly. This file is the other half: what `run_ingest` itself
sends, which is what actually enforces that boundary in a real pipeline.

A source that declares `consumed_run_options()` (the filesystem family, plus
anything that inherits `FilesystemSource`) receives only the subset it named.
A source that does not (SQL sources, most SaaS connectors) keeps receiving every
one of omniload's fifteen run options, unfiltered -- the behaviour before this
inversion, preserved for the ~90 sources that never opted out of it.
"""

import sqlite3
from unittest import mock

import duckdb
import pytest

from dlt_filesystem.source.fsspec.local import LocalFilesystemSource
from omniload import run_ingest
from omniload.api import RUN_OPTION_KEYS, _reject_unconsumed_incremental_key
from omniload.core.router import SqlSourceRouter


def _make_sqlite_source(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE widgets (id INTEGER, name TEXT)")
    conn.execute("INSERT INTO widgets VALUES (1, 'alpha')")
    conn.commit()
    conn.close()


def _spy_dlt_source(cls) -> tuple[list[dict], mock._patch]:
    """Wrap (not replace) `cls.dlt_source` to record the keywords it receives.

    Delegates to the real implementation so the pipeline it feeds still runs to
    completion; a source that raised on unexpected input would otherwise mask
    the very keyword set this is trying to observe.
    """
    calls: list[dict] = []
    original = cls.dlt_source

    def wrapper(self, uri, table, **kwargs):
        calls.append(dict(kwargs))
        return original(self, uri, table, **kwargs)

    return calls, mock.patch.object(cls, "dlt_source", wrapper)


class _StubSource:
    """A minimal stand-in with a controllable `consumed_run_options()`."""

    def __init__(self, consumed: frozenset) -> None:
        self._consumed = consumed

    def consumed_run_options(self) -> frozenset:
        return self._consumed


def test_reject_fires_for_a_declarer_that_does_not_consume_the_key():
    """The common case: a source names its vocabulary and it excludes both
    incremental-key spellings, so a requested one is rejected."""
    source = _StubSource(frozenset({"filesystem_incremental", "column_types"}))
    with pytest.raises(ValueError, match="you should not provide incremental_key"):
        _reject_unconsumed_incremental_key(source, "file", "some_column")


def test_reject_is_silent_for_a_declarer_that_does_consume_the_key():
    """The hook's contract is "names I accept", not "I own incrementality": a
    source that names one of the incremental-key spellings in its own declared
    vocabulary must not be rejected for receiving it."""
    source = _StubSource(frozenset({"requested_incremental_key"}))
    _reject_unconsumed_incremental_key(source, "hypothetical", "some_column")


def test_reject_is_silent_for_a_source_that_declares_nothing():
    """A source with no `consumed_run_options()` at all (the ~90 majority) is
    untouched here; it keeps whatever guard it carries itself, if any."""
    _reject_unconsumed_incremental_key(object(), "sqlite", "some_column")


def test_filesystem_source_receives_only_its_declared_options(tmp_path):
    """`LocalFilesystemSource` declares `{filesystem_incremental, column_types}`
    (inherited from `FilesystemSource`), so that is exactly what it should see --
    none of the other thirteen names in omniload's own run vocabulary."""
    path = tmp_path / "data.csv"
    path.write_text("id,name\n1,alpha\n2,beta\n")
    dest = tmp_path / "warehouse.duckdb"

    calls, patcher = _spy_dlt_source(LocalFilesystemSource)
    with patcher:
        run_ingest(
            source_uri=f"file://{path}",
            dest_uri=f"duckdb:///{dest}",
            source_table="",
            dest_table="out.data",
            progress="log",
        )

    assert len(calls) == 1
    received = set(calls[0])
    assert received == {"filesystem_incremental", "column_types"}
    assert received.isdisjoint(RUN_OPTION_KEYS - received)


def test_sql_source_still_receives_every_run_option(tmp_path):
    """`SqlSourceRouter` declares no `consumed_run_options()`, so it keeps the
    pre-inversion behaviour: every one of the fifteen names, unfiltered. This is
    the majority case (~90 sources); the filesystem family is the exception."""
    src = tmp_path / "source.db"
    dest = tmp_path / "warehouse.duckdb"
    _make_sqlite_source(str(src))

    assert getattr(SqlSourceRouter, "consumed_run_options", None) is None

    calls, patcher = _spy_dlt_source(SqlSourceRouter)
    with patcher:
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="main.widgets",
            dest_table="out.widgets",
            progress="log",
        )

    assert len(calls) == 1
    assert set(calls[0]) == set(RUN_OPTION_KEYS)

    con = duckdb.connect(str(dest))
    try:
        rows = con.sql("select id, name from out.widgets order by id asc").fetchall()
    finally:
        con.close()
    assert rows == [(1, "alpha")]
