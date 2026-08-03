"""Route filesystem URIs that name a SQLite or DuckDB file into the SQL source."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Iterator, Optional
from urllib.parse import unquote, urlsplit

from dlt_filesystem.source.impl.util import has_glob_magic
from dlt_filesystem.staging import (
    STAGING_BACKENDS,
    RemoteObject,
    materialize_remote_object,
)

# File extensions that name a file-based database, mapped onto the engine they
# imply. `.db` is used by both engines, so it carries no implication and is
# resolved from the file header alone.
DATABASE_EXTENSIONS: dict[str, Optional[str]] = {
    ".db": None,
    ".ddb": "duckdb",
    ".duckdb": "duckdb",
    ".sqlite": "sqlite",
    ".sqlite3": "sqlite",
}

# Both engines start their files with a fixed marker: SQLite at offset 0, DuckDB
# behind an 8-byte checksum. Reading them identifies the engine of a staged file
# whatever its extension says.
SQLITE_MAGIC = b"SQLite format 3\x00"
DUCKDB_MAGIC = b"DUCK"
DUCKDB_MAGIC_OFFSET = 8
DATABASE_HEADER_SIZE = 16


def _names_database(carrier: str) -> bool:
    """Return whether a URI or table carrier ends in a database extension."""
    path = unquote(urlsplit(carrier).path)
    return PurePosixPath(path).suffix.lower() in DATABASE_EXTENSIONS


def parse_remote_database_uri(uri: str, table: str = "") -> Optional[RemoteObject]:
    """Return the remote object for a filesystem URI naming a database file.

    ``s3://analytics/snapshots/events.duckdb`` reads as the object URI it is:
    the same carrier the filesystem sources take, with credentials in its query.
    Every other URI returns ``None`` and keeps its own source, including a
    filesystem URI that selects a regular file.
    """
    parsed = urlsplit(uri)
    if parsed.scheme.lower() not in STAGING_BACKENDS:
        return None

    if not _names_database(parsed.path):
        # The filesystem sources also accept the object path on `--source-table`.
        # A database cannot use that form, because the table names a table inside
        # the database, so say which carrier takes the object.
        if _names_database(table):
            raise ValueError(
                "Name a remote database on --source-uri, as "
                "'s3://bucket/path/events.duckdb'. --source-table selects a table "
                "inside the database, so it cannot also carry the object path."
            )
        return None
    # Glob syntax is written literally, so the still-encoded path is what decides:
    # a percent-encoded `?` is part of an object name, not a wildcard.
    if has_glob_magic(parsed.path):
        raise ValueError(
            "A database source names one object; wildcards select a file set "
            "that no single database connection can open."
        )

    return RemoteObject.from_uri(uri)


def resolve_database_engine(local_path: Path, location: str) -> str:
    """Identify the engine of a staged database file.

    The file header decides, so a mislabeled object still loads. Only an empty
    file has no header to read, and it falls back to an unambiguous extension so
    a freshly created, still-empty database stays loadable.
    """
    with local_path.open("rb") as database:
        header = database.read(DATABASE_HEADER_SIZE)

    if header.startswith(SQLITE_MAGIC):
        return "sqlite"
    if header[DUCKDB_MAGIC_OFFSET : DUCKDB_MAGIC_OFFSET + len(DUCKDB_MAGIC)] == (
        DUCKDB_MAGIC
    ):
        return "duckdb"
    if header:
        raise ValueError(
            f"Cannot identify the database engine of {location}: its header "
            f"matches neither SQLite nor DuckDB."
        )

    engine = _engine_from_extension(location)
    if engine is None:
        raise ValueError(
            f"Cannot identify the database engine of {location}: the object is "
            f"empty, and its extension names both SQLite and DuckDB."
        )
    return engine


def _engine_from_extension(location: str) -> Optional[str]:
    """Return the engine a location's extension implies, if it implies one."""
    suffix = PurePosixPath(unquote(urlsplit(location).path)).suffix.lower()
    return DATABASE_EXTENSIONS.get(suffix)


def dry_run_database_uri(remote: RemoteObject) -> str:
    """Return the SQL URI a dry run validates in place of the object URI.

    A dry run stops before extraction, so the object is never downloaded and its
    engine cannot be read. Only the scheme matters here: it routes validation
    through the SQL source the real run uses, rather than the filesystem source
    the URI came from. Nothing connects, so an extension that names both engines
    resolves to either one.
    """
    engine = _engine_from_extension(remote.safe_location) or "sqlite"
    return f"{engine}:///{remote.path}"


def _database_uri(engine: str, path: Path) -> str:
    """Build an absolute SQLAlchemy file URI, including on Windows."""
    normalized = path.resolve().as_posix()
    if "?" in normalized:
        raise ValueError(
            "Remote database staging paths must not contain '?', which SQLAlchemy "
            "treats as the start of connection query parameters."
        )
    return f"{engine}:///{normalized}"


@contextmanager
def stage_remote_database(
    remote: RemoteObject,
    *,
    staging_root: str | Path | None = None,
) -> Iterator[str]:
    """Yield a local SQL URI backed by a run-scoped copy of the remote database."""
    with materialize_remote_object(
        remote,
        filename="database",
        staging_root=staging_root,
    ) as local_path:
        engine = resolve_database_engine(local_path, remote.safe_location)
        yield _database_uri(engine, local_path)
