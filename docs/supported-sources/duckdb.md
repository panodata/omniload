# DuckDB

DuckDB is an in-memory database designed to be fast and reliable.

omniload supports DuckDB as both a source and destination.

## URI format

The URI format for DuckDB is as follows:

```text
duckdb:///<database-file>
```

URI parameters:

- `database-file`: the path to the DuckDB database file

The same URI structure can be used both for sources and destinations. See the
[DuckDB SQLAlchemy dialect][duckdb-sqlalchemy] for details.

## Remote source files

A DuckDB source file that lives in [Amazon S3](s3.md), [Cloudflare R2](r2.md),
[Azure Blob Storage](azure-storage.md), or
[Google Cloud Storage](google-cloud-storage.md) is addressed by its object URI,
with the storage credentials as query parameters:

```bash
omniload ingest \
  --source-uri 's3://analytics/snapshots/events.duckdb?access_key_id=ACCESS&secret_access_key=SECRET' \
  --source-table 'main.events' \
  --dest-uri 'duckdb:///local.duckdb' \
  --dest-table 'raw.events'
```

Percent-encode every query value that contains reserved characters such as `+`,
`/`, `=`, `&`, or `?`.

The object is staged locally for the duration of the run, so remote databases
are sources only. See {ref}`database-files` for the whole contract.

[duckdb-sqlalchemy]: https://github.com/Mause/duckdb_engine
