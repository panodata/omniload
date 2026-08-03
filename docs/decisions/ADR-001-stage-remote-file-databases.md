# ADR-001: Stage remote file databases before SQL ingestion

**Status**: Proposed
**Date**: 2026-08-03

## Context

SQLite and DuckDB require random access to a database file through their
existing SQLAlchemy integrations. Object-storage streams do not provide a
shared database interface that both engines can open directly.

Remote database access needs the same URI and lifecycle semantics across
supported storage transports. Storage credentials must remain separate from
logged object locations, and the database file must remain available while dlt
constructs and consumes its lazy SQL resource.

## Decision

We materialize remote SQLite and DuckDB source files into a run-scoped local
temporary directory before invoking the existing SQL source path.

A remote database is addressed by its plain object URI, the same carrier the
filesystem sources read files from, with storage options as its query
parameters. A filesystem source URI whose object name carries a database
extension routes to the SQL source rather than to a format reader, so
`--source-table` keeps selecting a table inside the database. Transport support
covers the storage schemes that share the repository's existing filesystem
credential parsing and emulator coverage: `s3://`, `r2://`, `gs://`, `az://`,
`adls://`, and `abfss://`.

The engine is resolved from the staged file's header, with an unambiguous
extension as the fallback, because `.db` names both engines and an object's name
is not evidence of its contents.

The staging operation checks the remote object's reported size and available
disk space, streams the object while counting bytes, and keeps the local copy
alive through the complete ingestion run. It removes the copy on normal
completion and exception exits. The staged database is source-only, and local
changes are never written back to object storage.

## Alternatives considered

- **Carry the object URI in a `location` query parameter of the SQL URI**
  (`duckdb:///?location=<encoded-object-uri>&<storage-options>`): rejected
  because it makes the caller encode a URI inside a URI for a location the
  storage scheme already names unambiguously.
- **Stack storage schemes inside the SQL URI**: rejected because nested URI
  authorities, query strings, and credential ownership are ambiguous to
  standard URL parsers.
- **Decode database files through a filesystem reader**: rejected because the
  SQL source already owns reflection, chunking, type mapping, and table
  selection. A reader would reimplement them one file format at a time.
- **Use DuckDB `httpfs` or `ATTACH` as the primary path**: rejected because it
  does not provide the same mechanism for SQLite. Engine-specific fast paths
  can be added later without changing the public URI grammar.
- **Expose object storage through an engine-specific VFS**: rejected because
  there is no shared, maintained random-access interface for both engines and
  all three launch transports.
- **Keep a persistent local cache**: rejected because cache invalidation,
  credential boundaries, and stale-object behavior require a separate policy.
  Run-scoped staging has explicit ownership and cleanup.
- **Write staged database changes back remotely**: rejected because concurrent
  writers and atomic replacement require consistency guarantees that a source
  ingestion does not need.

## Consequences

- Remote file databases reuse the established SQL reflection, extraction, and
  loading path.
- Each run downloads the whole object and requires enough local disk for the
  database plus any engine sidecar files.
- Normal success and failure paths remove staged data. A process terminated
  without cleanup may leave a temporary directory when a persistent staging
  parent is configured.
- Credentials remain storage options and are excluded from safe object
  locations and object representations.
- A database extension on a storage URI is reserved: an object named `.db`,
  `.ddb`, `.duckdb`, `.sqlite`, or `.sqlite3` is never offered to a format
  reader, and one that holds neither engine reports that instead of a format
  error.
- Additional transports need a credential adapter and integration coverage. A
  transport that builds its filesystem client inside its own source class needs
  that construction lifted out before it can stage objects.
- A future engine-specific fast path must preserve the same source-only behavior
  and public URI contract.
