# SQLite

SQLite is a C-language library that implements a small, fast, self-contained,
high-reliability, full-featured SQL database engine.

omniload supports SQLite as a source and a destination.

## URI format

The URI format for SQLite is as follows:

```text
sqlite:///<database-file>
```

URI parameters:

- `database-file`: the path to the SQLite database file.

The same URI structure can be used both for sources and destinations. See the
[SQLite SQLAlchemy dialect][sqlite-sqlalchemy] for details.

## Remote source files

An SQLite source file that lives in [Amazon S3](s3.md), [Cloudflare R2](r2.md),
[Azure Blob Storage](azure-storage.md), or
[Google Cloud Storage](google-cloud-storage.md) is addressed by its object URI,
with the storage credentials as query parameters:

```bash
omniload ingest \
  --source-uri 's3://analytics/snapshots/events.sqlite?access_key_id=ACCESS&secret_access_key=SECRET' \
  --source-table 'main.events' \
  --dest-uri 'duckdb:///local.duckdb' \
  --dest-table 'raw.events'
```

Percent-encode every query value that contains reserved characters such as `+`,
`/`, `=`, `&`, or `?`.

The object is staged locally for the duration of the run, so remote databases
are sources only. See {ref}`database-files` for the whole contract.

[sqlite-sqlalchemy]: https://docs.sqlalchemy.org/en/20/dialects/sqlite.html
