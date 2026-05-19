(filesystem)=

# Filesystem

omniload supports reading and writing different file formats from various
local and remote filesystem types.

Filesystem handlers support globbing, gzip decompression, as well as format
and reader hints.

(file-formats)=

## Supported formats

The file format is inferred from the file extension (with optional `.gz`
suffix), or by using a {ref}`format hint <format-hint>` if your resource
URI does not include file extensions.

| Format           | Description                                     | Extensions | Format hint   | Read | Write |
|:-----------------|:------------------------------------------------|:-----------|:--------------|:-----|:------|
| {ref}`bson`      | Binary JSON (MongoDB dump format)               | .bson      | #bson         | ✅   | ❌    |
| {ref}`cbor`      | Concise Binary Object Representation (RFC 8949) | .cbor      | #cbor         | ✅   | ❌    |
| [CSV]            | Comma-separated values with a header row        | .csv       | #csv          | ✅   | ✅    |
| [CSV] (headless) | Comma-separated values without a header row     | .csv       | #csv_headless | ✅   | ❌    |
| [JSONL]          | Newline-delimited JSON                          | .jsonl     | #jsonl        | ✅   | ✅    |
| {ref}`msgpack`   | Efficient binary serialization format           | .msgpack   | #msgpack      | ✅   | ❌    |
| {ref}`ods`       | OpenDocument spreadsheet format                 | .ods       | #ods          | ✅   | ❌    |
| [Parquet]        | Apache Parquet format                           | .parquet   | #parquet      | ✅   | ✅    |
| {ref}`xlsx`      | Excel spreadsheet format                        | .xlsx      | #xlsx         | ✅   | ❌    |

:::{note}
Supported formats for write operations are currently CSV, JSONL, and Parquet.
:::

(filesystem-types)=

## Supported filesystems

| Name                 | Description                             | Protocol scheme |
|:---------------------|:----------------------------------------|:----------------|
| {ref}`Local <file>`  | Local and mounted filesystems           | file://         |
| [Amazon S3]          | S3 and compatible filesystems           | s3://           |
| [Google GCS]         | Google Cloud Storage                    | gs://           |
| [Azure Blob Storage] | Azure Blob Storage                      | az://           |
| [SFTP]               | Simple File Transfer Protocol (RFC 913) | sftp://         |


:::{note}
`omniload` supports read and write operations on both local and remote filesystems.
See {ref}`file:// destination <file-destination>` for write support.
:::

(format-hint)=
(format-hints)=

## Format hints

If a file is correctly encoded but has a non-standard extension, append a
`#format` fragment to tell `omniload` how to read it:

```sh
omniload ingest \
    --source-uri 'file://data/event-data#jsonl' \
    --source-table 'events' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events'
```

If the format hint is not one of the known formats, the path is preserved 1:1,
so `file://data/vendor#1/report.csv` reads the file
at `data/vendor#1/report.csv` as CSV.

:::{rubric} Example: CSV files without headers
:::

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

(reader-hints)=

## Reader hints

The URI fragment is also a channel to convey **reader hints**: Besides a `#format`
token, it can carry `#key=value` pairs that a reader may use to parametrize how a
file is read. For example, the spreadsheet reader takes a `sheet_name` parameter.
A format hint and named hints can coexist in one fragment, `&`-separated.

```text
file://quotes.dat#xlsx&sheet_name=daily&header=0
```

The named-hint grammar:

- Values are percent-decoded, so `#sheet_name=My%20Sheet` gives the value `My Sheet`
  and `#sheet_name=R%26D` gives `R&D`.
- Only the first `=` splits key from value, so a value may itself contain `=`
  (`#range=A1=B2` gives `range` = `A1=B2`).
- An empty value is preserved (`#sheet_name=` gives `sheet_name` = `""`);
  a reader decides whether that means "unset".
- Duplicate keys take the last value (`#sheet_name=a&sheet_name=b` gives `b`).
- If any segment of the fragment is neither a `key=value` pair nor a single
  known format, the whole `#...` is treated as a literal part of the path, so a
  real `#` in a filename keeps working. Percent-encode a literal `#` as `%23`
  when a trailing `path#key=value` would otherwise be read as a fragment.

:::{note}
Reader hints can be used to forward additional parameters as `key=value` pairs
to the underlying pipeline element implementation.
For example, CSV and Excel readers forward corresponding parameters to the
[polars.read_csv] and [polars.read_excel] functions.
:::


[Amazon S3]: https://aws.amazon.com/
[Azure Blob Storage]: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction
[CSV]: https://en.wikipedia.org/wiki/Comma-separated_values
[Google GCS]: https://cloud.google.com/storage
[JSONL]: https://en.wikipedia.org/wiki/JSON_streaming#JSONL
[Parquet]: https://en.wikipedia.org/wiki/Apache_Parquet
[polars.read_csv]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html
[polars.read_excel]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_excel.html
[SFTP]: https://en.wikipedia.org/wiki/File_Transfer_Protocol#Simple_File_Transfer_Protocol
