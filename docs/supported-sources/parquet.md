(parquet)=

# Parquet

`omniload` reads [Apache Parquet] files. Parquet is a column-oriented binary
format for structured data. It stores a schema with the data and supports
nested values, typed columns, compression, and row groups.

Parquet is available for read operations on every supported filesystem source.
Parquet is also available for write operations through the local `file://`
destination.

## Installation

Parquet support is included in the base `omniload` installation. It uses
[`pyarrow`] to read and write Parquet files.

```sh
pip install omniload
```

Do not install an optional extra to use Parquet.

## Where it works

Every source that uses the shared filesystem readers can read Parquet:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...
- HTTP and HTTPS URLs: {ref}`http`

The source determines the storage connection and authentication. Parquet adds
no storage-specific configuration.

`omniload` selects the Parquet reader in one of these cases:

- The filename ends in `.parquet`.
- The filename ends in `.parquet.gz`.
- The source path has an explicit `#parquet` {ref}`format hint <format-hint>`.

For example, use `#parquet` when the file has no `.parquet` extension:

```text
file://data/events.bin#parquet
```

Gzipped Parquet files are decompressed before `pyarrow` reads them:

```text
s3://my-bucket/events/2026-09-01.parquet.gz
```

See {ref}`filesystem` for format detection, glob patterns, compression, and
incremental file selection.

## Reading behavior

`omniload` reads Parquet files with `pyarrow.parquet.ParquetFile`. The reader
returns rows in batches. The default batch size is 10 rows.

The reader converts each Arrow batch to Python dictionaries before it passes
the rows to the loader. The Parquet schema controls the decoded column types.

Parquet is not a streaming format over a remote transport. `pyarrow` reads the
file footer and can request the complete data section. A large remote Parquet
file can therefore require substantial memory and network transfer even when
the reader returns rows in batches.

If the Parquet file is corrupt, truncated, encrypted without the required
configuration, or uses an unsupported codec, `pyarrow` raises an error during
the load. Validate files upstream when a partial or failed load is not
acceptable.

## Examples

### Load a local Parquet file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://data/events.parquet' \
    --source-table 'events' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

When the URI already contains the file path, `--source-table` does not select a
table inside the Parquet file. The destination table is set by `--dest-table`.

### Load a Parquet file from S3 into DuckDB

```sh
omniload ingest \
    --source-uri 's3://my-bucket?access_key_id=YOUR_ACCESS_KEY&secret_access_key=YOUR_SECRET_KEY' \
    --source-table 'events/2026-09-01.parquet' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

Use the documentation for the selected filesystem source to configure
authentication and source paths.

### Read a file with a non-standard extension

```sh
omniload ingest \
    --source-uri 'file://data/events.data#parquet' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.events'
```

The `#parquet` fragment is not part of the filename. It instructs `omniload`
to use the Parquet reader.

## Write a local Parquet file

Use a `file://` destination path that ends in `.parquet`, or use `#parquet`:

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/app' \
    --source-table 'public.events' \
    --dest-uri 'file://export/events.parquet' \
    --dest-table 'public.events'
```

The `file://` destination writes one Parquet file at the requested path. It
creates missing parent directories and overwrites an existing output file.

The destination removes dlt bookkeeping columns before it writes the file. It
collects all loaded rows before it writes the Parquet table. This makes a
single-file output reliable, but it is not suitable for data that cannot fit in
memory.

The `file://` destination supports only `csv`, `jsonl`, and `parquet` output.
It does not accept glob patterns. See {ref}`file-destination` for the complete
URI and destination-table rules.

## Parquet files and Parquet loader files

The Parquet source format is independent of the Parquet loader format that
`omniload` can select for some warehouse destinations. A Parquet source
controls how `omniload` reads input files. A Parquet loader controls how
`omniload` stages rows for a destination.

You do not need to set a loader option to read a Parquet source file.

[Apache Parquet]: https://parquet.apache.org/
[`pyarrow`]: https://arrow.apache.org/docs/python/
