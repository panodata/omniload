# CSV

`csv://` reads and writes a single local CSV file. It is a CSV-only spelling of
the {ref}`file:// <file>` connector: same readers, same writer, same path
grammar, same rerun behaviour. Prefer `file://` for new work, and reach for
`csv://` only to keep an existing command working.

## URI format

```text
csv://path/to/file.csv
```

Everything after `csv://` is a filesystem path, resolved exactly as
{ref}`file:// <file>` resolves it: relative to the working directory, or
absolute with an extra leading slash. Globs, gzipped files, split form
(`--source-uri csv:// --source-table data/users.csv`), Windows drive and UNC
paths, and {ref}`format hints <format-hint>` all behave the same way.

```sh
omniload ingest \
    --source-uri 'csv://data/users.csv' \
    --source-table 'users' \
    --dest-uri 'duckdb:///local.duckdb' \
    --dest-table 'public.users'
```

## CSV only

The scheme names the file format, so it accepts the CSV family of readers and
nothing else: `csv`, `csv_headless` and `csv_duckdb`.

| URI | Result |
| :--- | :--- |
| `csv://data/*.csv` | Reads every matching CSV. A glob widens the path, not the format. |
| `csv://feed.dat#csv_headless` | Reads a header-less CSV. |
| `csv://events.csv.gz` | Reads a gzipped CSV. |
| `csv://data.jsonl` | Rejected. Use `file://data.jsonl`. |
| `csv://feed.dat#parquet` | Rejected. Use `file://feed.dat#parquet`. |
| `csv://book.xlsx` | Rejected. Use `file://book.xlsx`. |

The same restriction applies when writing: `csv://out.jsonl` and
`csv://out.dat#parquet` are rejected before the load starts. A path carrying no
recognised format at all (`csv://report`, `csv://out.dat`) writes CSV, because
the scheme already names the format.

## Destination connector

```sh
omniload ingest \
    --source-uri 'postgres://user:password@host:5432/db' \
    --source-table 'public.users' \
    --dest-uri 'csv://export/users.csv' \
    --dest-table 'public.users'
```

`--dest-table` must be `<schema>.<table>`; it names only dlt's intermediate
layout, while the output file is the URI path. Parent directories are created
if they don't exist, and an existing file is overwritten. The output drops
dlt's internal bookkeeping columns.

:::{note}
Rows are written in a deterministic order, but not necessarily source order.
:::

## Behaviour changes

This release moves `csv://` onto the `file://` reader and writer, which changes
six things:

- **Values are typed.** The reader infers column types, so a numeric column
  arrives as a number and `true`/`false` as a boolean, where the old reader
  yielded every value as a string. ISO date strings stay strings.
- **Empty rows are preserved.** A row whose fields are all empty is loaded as a
  row of nulls instead of being dropped.
- **A rerun appends.** With no `--incremental-strategy`, a second load of the
  same file adds a second copy rather than replacing the first. Pass
  `--incremental-strategy replace` for the old behaviour.
- **`merge`, `delete+insert` and `scd2` are rejected.** They need an
  incremental or merge key, which the shared reader does not expose. Use a
  source that does, or `--full-refresh` to reset the destination.
- **`--incremental-key` is rejected**, with the same error `file://` gives. The
  row cursor it drove compared raw strings against parsed datetimes and crashed
  on an interval; it is gone rather than ported. File-level selection by
  modification time is available instead, through `--filesystem-incremental`.
- **The whole load is written.** A load dlt splits across several files used to
  write only the first of them and still exit zero. Every row now reaches the
  output file.
