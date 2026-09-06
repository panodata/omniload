(orc)=

# ORC

`omniload` reads [Apache ORC][ORC] (Optimized Row Columnar) files. ORC is a
columnar format for analytical data. An ORC file has a schema and contains zero
or more rows.

ORC is currently supported for read operations only.

## Where it works

ORC is available on every source that uses the shared file readers:

- Local files: {ref}`file`
- Remote files: {ref}`s3`, {ref}`gcs`, {ref}`azure-storage`, {ref}`sftp`, ...

Remote reads use the source's existing fsspec handle. They use its existing
authentication. No separate ORC storage configuration is required.

A file is read as ORC when its extension is `.orc`, optionally followed by
`.gz`. You can also append the `#orc` {ref}`format hint <format-hint>` to a
file with a different extension. `omniload` decompresses gzipped files
automatically.

For details about format selection, see {ref}`file-format-routing`.

## Examples

### Load a local ORC file into DuckDB

```sh
omniload ingest \
    --source-uri 'file://events/day.orc' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load an ORC file from S3

Use `#orc` if the object name does not end in `.orc`.

```sh
omniload ingest \
    --source-uri 's3://' \
    --source-table 'my_bucket/events/day.data#orc' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

### Load multiple ORC files

Use a glob to load rows from all matching ORC files.

```sh
omniload ingest \
    --source-uri 'file://events/*.orc' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

## Extended-type handling

The reader uses PyArrow's `ORCFile.read_stripe()` and converts each record batch
to Python rows. Large stripes are sliced into batches according to the
`chunksize` format hint.

Common ORC types such as strings, integers, floating-point values, booleans,
dates, timestamps, decimals, lists, maps, and structs pass through the
PyArrow conversion. UTC timestamp values remain timezone-aware.
Decimal values remain decimals.

ORC `TIMESTAMP` values have no time zone. The reader returns them as
timezone-naive datetime values.

ORC `TIMESTAMP_INSTANT` values represent fixed instants and remain
timezone-aware. The PyArrow conversion returns these values with
time-zone information when the source file provides it.

[ORC]: https://orc.apache.org/
