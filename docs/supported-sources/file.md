# Filesystem

The `file://` source reads local files (BSON, CSV, JSONL, Parquet) through the same
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

The file format is inferred from the extension (`.bson`, `.csv`, `.jsonl`,
`.parquet`, optionally `.gz`) or from an explicit [format hint](#file-type-hinting).

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

- `#bson` - BSON (MongoDB dump format), read-only. See [BSON](bson.md).
- `#csv` - comma-separated values with a header row
- `#csv_headless` - CSV without a header row (see below)
- `#jsonl` - line-delimited JSON
- `#parquet` - Parquet

BSON is a read format only; the write side below supports `csv`, `jsonl` and `parquet`.

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

## Reader hints

The URI fragment is also a general **reader-hint channel**: besides a `#format`
token it can carry `#key=value` pairs that a reader may use to parametrize how a
file is read (for example, a future spreadsheet reader could take a sheet name).
A format hint and named hints coexist in one fragment, `&`-separated:

```text
file://quotes.dat#csv&sheet=daily&header=0
```

The named-hint grammar:

- Values are percent-decoded, so `#sheet=My%20Sheet` gives the value `My Sheet`
  and `#sheet=R%26D` gives `R&D`.
- Only the first `=` splits key from value, so a value may itself contain `=`
  (`#range=A1=B2` gives `range` = `A1=B2`).
- An empty value is preserved (`#sheet=` gives `sheet` = `""`); a reader decides
  whether that means "unset".
- Duplicate keys take the last value (`#sheet=a&sheet=b` gives `b`).
- If any segment of the fragment is neither a `key=value` pair nor a single
  known format, the whole `#...` is treated as a literal part of the path, so a
  real `#` in a filename keeps working. Percent-encode a literal `#` as `%23`
  when a trailing `path#key=value` would otherwise be read as a fragment.

:::{note}
Reader hints are a forward-looking channel. The built-in BSON, CSV, JSONL and
Parquet readers take no hints today, so a `#key=value` pair is parsed and
carried but has no effect on them yet. Only the `#format` token changes current
read behavior. The same channel is available on the [S3](s3.md),
[Google Cloud Storage](google-cloud-storage.md) and [SFTP](sftp.md) sources.
:::

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
