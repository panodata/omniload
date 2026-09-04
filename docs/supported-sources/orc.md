(orc)=

# ORC

`omniload` reads [Apache ORC][ORC] (Optimized Row Columnar) files. ORC is a
columnar format for analytical data. An ORC file has a schema and contains zero
or more rows.

ORC is currently supported for read operations only.

## Installation

ORC support is included in the base installation. The reader uses
[`pandas.read_orc`][pandas-read-orc] with PyArrow.

```sh
pip install omniload
```

`omniload` uses PyArrow-backed pandas dtypes by default. You can override this
with a reader hint when your workflow requires a different pandas dtype backend.

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

## Data types

The reader converts each ORC file to a pandas DataFrame. It then loads the
DataFrame rows into the destination.

Common ORC types such as strings, integers, floating-point values, booleans,
dates, timestamps, decimals, lists, maps, and structs pass through the
PyArrow and pandas conversion. UTC timestamp values remain timezone-aware.
Decimal values remain decimals.

The destination can impose additional type restrictions. For example, a
JSON-based destination cannot serialize every value that an ORC file can
contain. Use a SQL or Parquet destination when the source uses decimals,
timestamps, or nested values.

## Limits and errors

The reader loads one ORC file into a pandas DataFrame before it yields that
file's rows. For large data sets, split the input into multiple ORC files.

An empty or malformed ORC file does not load rows. PyArrow reports an error
when it cannot read the file. Validate files before loading if a partial load
would cause problems.

[ORC]: https://orc.apache.org/
[pandas-read-orc]: https://pandas.pydata.org/docs/reference/api/pandas.read_orc.html
