# Local files

The `file://` source reads local files (CSV, JSONL, Parquet) through the same
readers used by the S3, GCS and SFTP sources. Any file format those sources
support is supported here too, along with globbing, gzip decompression and
`#format` hints.

`omniload` supports local files as both a data source and a destination. See
[Using `file://` as a destination](#using-file-as-a-destination) below for writing.

## URI Format

Everything after `file://` is treated as a filesystem path. Relative paths
resolve against the current working directory; an extra leading slash gives an
absolute path.

```text
file://<path>
```

| Form | Example | Resolves to |
| :--- | :--- | :--- |
| Relative path | `file://data/users.csv` | `<cwd>/data/users.csv` |
| Absolute path (POSIX) | `file:///srv/data/users.jsonl` | `/srv/data/users.jsonl` |
| Windows drive | `file:///C:/data/users.csv` (or `file://C:/data/users.csv`) | `C:\data\users.csv` |
| Windows UNC | `file:////server/share/users.csv` | `\\server\share\users.csv` |
| Path via `--source-table` | `--source-uri file:// --source-table data/users.parquet` | `<cwd>/data/users.parquet` |
| Glob | `file://data/*.csv` | all matching files in `<cwd>/data` |
| Format hint | `file://feed.dat#csv` | `feed.dat` read as CSV |

The file format is inferred from the extension (`.csv`, `.jsonl`, `.parquet`,
optionally `.gz`) or from an explicit [format hint](#file-type-hinting).

:::{tip}
`file://` intentionally treats the first path segment as part of the path, not
as an RFC-8089 host. This is what makes the two-slash form `file://data/x.csv`
(relative to the working directory) work, matching how `csv://` already behaves.
Use the three-slash form `file:///abs/x.csv` for absolute paths.
:::

:::{note}
Windows paths are supported: `file:///C:/data/x.csv` (or `file://C:/data/x.csv`)
reads the drive path `C:\data\x.csv`, and `file:////server/share/x.csv` reads the
UNC path `\\server\share\x.csv`. Backslash input (`file://\\server\share\x.csv`)
is accepted as well.
:::

## Example: Loading a local CSV into DuckDB

```sh
omniload ingest \
    --source-uri 'file://data/users.csv' \
    --source-table 'users' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.users'
```

The `--source-table` value is only used as the path when the URI path is empty
(the split form above); otherwise it is ignored, and the destination table is
controlled by `--dest-table`.

## Supported formats

The same set the blob sources support:

- `#csv` - comma-separated values with a header row
- `#csv_headless` - CSV without a header row (see below)
- `#jsonl` - line-delimited JSON
- `#parquet` - Parquet

## File glob patterns

The path may contain a glob pattern to load multiple files at once. The split
into directory and pattern happens at the first segment containing a glob
character (`*`, `?`, `[`), so recursive patterns work:

| Pattern | Description |
| :--- | :--- |
| `file://data/*.csv` | All CSV files at the top level of `<cwd>/data`. |
| `file://data/**/*.jsonl` | All JSONL files under `<cwd>/data`, recursively. |
| `file:///srv/logs/**/*.csv.gz` | All gzipped CSV files under `/srv/logs`, recursively. |

## Compressed files

Gzipped files (`.gz`) are detected and decompressed automatically, so
`file://data/events.csv.gz` loads without any extra configuration.

## File type hinting

If a file is correctly encoded but has a non-standard extension, append a
`#format` fragment to tell `omniload` how to read it:

```sh
omniload ingest \
    --source-uri 'file://data/event-data#jsonl' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

A literal `#` in a path is preserved when the trailing segment is not one of the
known formats, so `file://data/vendor#1/report.csv` reads the file at
`data/vendor#1/report.csv` as CSV.

### CSV files without headers

For CSV files without a header row, use the `#csv_headless` hint and optionally
supply column names with `--columns`:

```sh
omniload ingest \
    --source-uri 'file://data/raw-data.csv#csv_headless' \
    --source-table 'raw' \
    --columns "id:bigint,name:text,value:double" \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.raw_data'
```

Without column names, columns are auto-named `unknown_col_0`, `unknown_col_1`,
and so on.

## Using `file://` as a destination

`file://` also writes local files. The output format is taken from the
destination file extension (`.csv`, `.jsonl`, `.parquet`) or from an explicit
`#format` hint, exactly like the source side. The written file drops dlt's
internal bookkeeping columns, so it round-trips cleanly.

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/db' \
    --source-table 'public.users' \
    --dest-uri 'file://export/users.parquet' \
    --dest-table 'public.users'
```

| Destination URI | Output |
| :--- | :--- |
| `file://out.csv` | CSV written to `<cwd>/out.csv` |
| `file:///srv/out.jsonl` | JSONL written to `/srv/out.jsonl` |
| `file://export/users.parquet` | Parquet written to `<cwd>/export/users.parquet` |
| `file://feed.dat#csv` | CSV written to `<cwd>/feed.dat` |

The path grammar is identical to the source (relative-to-cwd, absolute,
Windows drive and UNC forms all resolve the same way). Supported output formats
are `csv`, `jsonl` and `parquet`; any other extension (or none) is rejected with
the supported-format list. `--dest-table` must be `<dataset>.<table>`; it only
names the intermediate layout, the output file is the URI path.

Parent directories in the destination path are created if they don't exist, and
an existing file at the destination is overwritten. Globs are a read-only feature
and are not supported when writing.

## Relationship to `csv://`

The [`csv://`](csv.md) scheme still exists and is unchanged: it reads and writes
a single local CSV file. `file://` is the broader local path, covering JSONL and
Parquet as well as CSV, plus (on read) globbing and gzip decompression. Prefer
`file://` for local files; use `csv://` only when you specifically want the
standalone CSV reader.
