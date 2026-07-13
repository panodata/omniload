"""Resolution of ``--incremental-strategy`` / the run-level write disposition for
filesystem-family sources (issue #188).

Filesystem sources manage their own incrementality, so omniload's per-key logic is
disabled and the default is append-on-rerun (documented in the incremental-loading
guide). This pins the follow-on: an *explicit* ``--incremental-strategy`` append/replace
is now honoured for them, while the default and every non-filesystem source stay exactly
as they were.

Mock-only unit lane (no Docker, no credentials): real ``file://`` / ``sqlite://`` sources
load into a real embedded duckdb, and behaviour is proven by row counts across two runs
(append accumulates, replace resets). The one source without a public local driver, a
non-filesystem ``handles_incrementality`` source, is a small fake.
"""

import sqlite3

import dlt
import duckdb
import pytest

from omniload import ValidationError, run_ingest
from omniload.core.factory import SourceDestinationFactory

PEOPLE = "name,age\nAlice,30\nBob,25\nCarol,41\n"


def _write_people(tmp_path):
    src = tmp_path / "people.csv"
    src.write_text(PEOPLE)
    return src


def _make_sqlite(path):
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE widgets (id INTEGER, name TEXT)")
    conn.executemany(
        "INSERT INTO widgets VALUES (?, ?)",
        [(1, "alpha"), (2, "beta"), (3, "gamma")],
    )
    conn.commit()
    conn.close()


def _count(dest, table):
    con = duckdb.connect(str(dest))
    try:
        return con.sql(f"select count(*) from {table}").fetchall()[0][0]
    finally:
        con.close()


# --- filesystem source: default append, explicit append/replace ---


def test_filesystem_default_appends_on_rerun(tmp_path):
    """No --incremental-strategy: the file source appends a second copy on re-run."""
    src = _write_people(tmp_path)
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        run_ingest(
            source_uri=f"file://{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="people",
            dest_table="out.people",
            progress="log",
        )
    assert _count(dest, "out.people") == 6


def test_filesystem_explicit_append_accumulates(tmp_path):
    """Explicit append matches the default behaviour (accumulates), now stated intent."""
    src = _write_people(tmp_path)
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        run_ingest(
            source_uri=f"file://{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="people",
            dest_table="out.people",
            incremental_strategy="append",
            progress="log",
        )
    assert _count(dest, "out.people") == 6


def test_filesystem_explicit_replace_resets_on_rerun(tmp_path):
    """Explicit replace resets the destination instead of appending (issue #188)."""
    src = _write_people(tmp_path)
    dest = tmp_path / "wh.duckdb"

    # A default run first populates the table.
    run_ingest(
        source_uri=f"file://{src}",
        dest_uri=f"duckdb:///{dest}",
        source_table="people",
        dest_table="out.people",
        progress="log",
    )
    assert _count(dest, "out.people") == 3

    # An explicit replace resets rather than appending a second copy.
    run_ingest(
        source_uri=f"file://{src}",
        dest_uri=f"duckdb:///{dest}",
        source_table="people",
        dest_table="out.people",
        incremental_strategy="replace",
        progress="log",
    )
    assert _count(dest, "out.people") == 3


def test_filesystem_explicit_none_appends(tmp_path):
    """Explicit 'none' on a filesystem source falls back to the append default."""
    src = _write_people(tmp_path)
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        run_ingest(
            source_uri=f"file://{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="people",
            dest_table="out.people",
            incremental_strategy="none",
            progress="log",
        )
    assert _count(dest, "out.people") == 6


@pytest.mark.parametrize("strategy", ["merge", "scd2", "delete+insert"])
def test_filesystem_key_strategies_are_rejected(tmp_path, strategy):
    """Key-dependent strategies error clearly instead of silently appending: filesystem
    sources can't supply the incremental/merge key these need."""
    src = _write_people(tmp_path)
    dest = tmp_path / "wh.duckdb"
    with pytest.raises(ValidationError, match="filesystem sources do not expose"):
        run_ingest(
            source_uri=f"file://{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="people",
            dest_table="out.people",
            incremental_strategy=strategy,
            progress="log",
        )


# --- regression guard: a non-filesystem handles_incrementality source ---


class _ManagedSource:
    """A ``handles_incrementality`` source that sets its own resource-level disposition,
    like the SaaS/streaming sources. It deliberately omits ``honours_run_disposition``, so
    a run-level write disposition must NOT override it (getattr defaults to False)."""

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri, table, **kwargs):
        @dlt.resource(name="rows")
        def rows():
            yield from [{"id": 1}, {"id": 2}, {"id": 3}]

        return rows()


def test_managed_source_ignores_explicit_run_disposition(tmp_path, monkeypatch):
    """Explicit replace on a non-filesystem handles_incrementality source is still forced
    to none, so the source keeps appending (row count grows) rather than resetting."""
    dest = tmp_path / "wh.duckdb"
    monkeypatch.setattr(
        SourceDestinationFactory, "get_source", lambda self: _ManagedSource()
    )
    for _ in range(2):
        run_ingest(
            source_uri="file://placeholder.csv",
            dest_uri=f"duckdb:///{dest}",
            source_table="rows",
            dest_table="out.rows",
            incremental_strategy="replace",
            progress="log",
        )
    # 6, not 3: replace was NOT honoured, so the resource-level disposition (append) stood.
    assert _count(dest, "out.rows") == 6


# --- non-filesystem sources: unchanged default (replace) and explicit replace ---


def test_sql_source_default_is_replace(tmp_path):
    """A non-filesystem source with no --incremental-strategy still defaults to replace,
    so a re-run resets rather than accumulating."""
    src = tmp_path / "src.db"
    _make_sqlite(src)
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="main.widgets",
            dest_table="out.widgets",
            progress="log",
        )
    assert _count(dest, "out.widgets") == 3


def test_sql_source_explicit_replace_still_replaces(tmp_path):
    """Explicit replace on an ordinary SQL source is unchanged (resets on re-run)."""
    src = tmp_path / "src.db"
    _make_sqlite(src)
    dest = tmp_path / "wh.duckdb"
    for _ in range(2):
        run_ingest(
            source_uri=f"sqlite:///{src}",
            dest_uri=f"duckdb:///{dest}",
            source_table="main.widgets",
            dest_table="out.widgets",
            incremental_strategy="replace",
            progress="log",
        )
    assert _count(dest, "out.widgets") == 3
