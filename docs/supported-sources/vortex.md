(vortex)=

# Vortex

`omniload` reads the highly performant [Vortex] file format. Vortex is an
extensible columnar data format considered faster than Parquet.

Vortex is supported for reads on shared filesystem sources and for writes
through the local `file://` destination.

## Installation

Vortex support ships in the optional `vortex` extra, which needs Python 3.11 or
newer, so it is not part of the base install:

```sh
pip install 'omniload[vortex]'
```

The `full` extra includes it. If a Vortex file is read or written without the
extra installed, `omniload` fails with an error naming the extra to install.

## Where it works

Vortex is available on every source that uses the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

Remote reads use the source's existing fsspec handle. They use its existing
authentication. No separate Vortex storage configuration is required.

A file is read as Vortex when its extension is `.vortex` (optionally
`.vortex.gz`). You can also append the `#vortex` {ref}`format hint <format-hint>`
to a file with a different extension. Gzipped files are decompressed
automatically.

The Vortex library opens files by path only, so a remote or gzipped file is
first copied to a local temporary file, which needs enough free disk space to
hold it. The file is then scanned in batches, and `#chunksize=` sets how many
rows are handed downstream at a time.

For details about format selection, see {ref}`file-format-routing`.

## Examples

### Load a local Vortex file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://events/day.vortex' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load a Vortex file from S3

Use `#vortex` if the object name does not end in `.vortex`.

```sh
omniload ingest \
    --source-uri 's3://' \
    --source-table 'my_bucket/events/day.data#vortex' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load multiple Vortex files

Use a glob to load rows from all matching Vortex files.

```sh
omniload ingest \
    --source-uri 'file://events/*.vortex' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Write a source table to a local Vortex file

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/db' \
    --source-table 'public.events' \
    --dest-uri 'file://export/events.vortex' \
    --dest-table 'public.events'
```

Vortex output is available through the local `file://` destination. Columns
that are absent from an individual source row are written as null values. A
timestamp carrying a fixed UTC offset, such as `+12:00`, is written as the same
instant in UTC.

:::{note}
dlt stages the rows between the source and the writer, and the staging format
decides what the writer receives: a load to a local `file://` destination
writes a timestamp or a decimal as text unless `--loader-file-format parquet`
is passed. See {ref}`file-load-types`.
:::


[Vortex]: https://vortex.dev/
