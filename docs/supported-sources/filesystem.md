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
| {ref}`xml`       | XML format                                      | .xml       | #xml          | ✅   | ❌    |
| {ref}`yaml`      | YAML format                                     | .yaml      | #yaml         | ✅   | ❌    |

:::{note}
Supported formats for write operations are currently CSV, JSONL, and Parquet.
:::

(filesystem-types)=

## Supported filesystems

| Name                      | Description                                                  | Protocol scheme          | Read | Write |
|:--------------------------|:-------------------------------------------------------------|:-------------------------|:-----|:------|
| {ref}`Local files <file>` | Files on local and mounted filesystems                       | file://                  | ✅   | ✅    |
| Azure Storage             | {ref}`azure-blob-storage` and {ref}`azure-data-lake-storage` | az://, adls://, abfss:// | ✅   | ✅    |
| {ref}`s3`                 | Amazon S3 and compatible filesystems                         | s3://                    | ✅   | ✅    |
| {ref}`gcs`                | Google Cloud Storage                                         | gs://                    | ✅   | ✅    |
| {ref}`hdfs`               | Hadoop distributed file system                               | hdfs://                  | ✅   | ❌    |
| {ref}`oss`                | Alibaba Object Storage Service (OSS)                         | oss://                   | ✅   | ❌    |
| {ref}`r2`                 | Cloudflare R2                                                | r2://                    | ✅   | ❌    |
| {ref}`sftp`               | Simple File Transfer Protocol (RFC 913)                      | sftp://                  | ✅   | ✅    |

:::{note}
`omniload` supports read and write operations on both local and remote filesystems.
For some filesystems, write support has not been unlocked yet, but we expect it to
land during the upcoming releases.
:::

## Incremental file selection

Filesystem sources append every matching file again on a normal re-run. Add
`--filesystem-incremental` to keep an mtime cursor and append rows only from new
or newly modified files:

```sh
omniload ingest \
    --source-uri 'file:///data/events/*.jsonl' \
    --dest-uri duckdb:///local.duckdb \
    --dest-table 'public.events' \
    --filesystem-incremental
```

This mode is opt-in and append-only. Its cursor needs durable dlt pipeline state:
the default temporary pipeline directory works when the destination supports
state sync, while other destinations require a stable `--pipelines-dir`. Files
at the current maximum modification time are deduplicated by their URL, and an
older-mtime backfill requires `--full-refresh` to reset the cursor.

The single-file `csv://` and `file://` destinations are rejected because they
replace their output using only the rows selected for the current run.

See {ref}`Filesystem sources <incremental-loading-filesystem>` for the state,
boundary, source-identity, and reset details.

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

(reader-hint)=
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
For example, CSV and {ref}`xlsx` readers forward corresponding parameters to
the [polars.read_csv] and [polars.read_excel] functions, and the {ref}`xml`
reader requires a `#tagname=<row-tag>` hint to define the repeated element
that represents one record / row.
:::

(file-format-routing)=

## File format routing

omniload reads each file format through the best available path rather than
one generic reader. This section explains how that routing works, so the
individual per-format pages (BSON, CBOR, MessagePack, XML, YAML) can stay
focused on how to use each format.

In general, omniload builds mostly upon the excellent fsspec, polars and
iterabledata packages for local and remote filesystem access and format
decoding.

| Format              | Library                 | Description                        |
|:--------------------|:------------------------|:-----------------------------------|
| CSV, JSONL, Parquet | `polars` / `pyarrow`    | Built-ins.                         |
| BSON                | Dedicated in-tree codec | Needs extended-type normalization. |
| CBOR                | `cbor`                  | Whole-file format.                 |
| MessagePack         | `iterabledata`          | Streamed record-by-record.         |
| ODS                 | `polars`                | Whole-file format.                 |
| XML                 | `lxml`                  | Whole-file parse, hardened.        |
| XLSX                | `polars`                | Whole-file format.                 |
| YAML                | `yaml`                  | Whole-file decode, safe.           |

omniload uses the [iterabledata] package for reading or decoding a few formats
not covered by native reader implementations. Install the `iterable` extra to
make them available to your environment.

```sh
pip install 'omniload[iterable]'
```

iterabledata exposes a uniform per-format class that yields record dicts from a
file object. When routing to iterabledata, omniload feeds it the source's fsspec
filesystem handle that is already authenticated, so remote filesystem access to
Amazon S3, Azure Blob Storage, Google Cloud Storage, or SFTP works transparently.

Where using iterabledata is not applicable, for example to enhance error handling,
or applying stronger security policies, omniload directly uses relevant low-level
decoder libraries.

## File format notes

### Performance

Where available, omniload uses Polars to read and decode files from local and
remote filesystems. Polars builds upon Apache Arrow and is written in Rust.
This guarantees robustness and speed.

### Whole-file decode

Some formats are whole-file rather than streaming. For those, omniload decodes
the bytes with the format's own library directly.

### Streaming

Records are pulled in batches until the reader signals end-of-file, and flushed
per file so a multi-file glob never drops a partial final chunk. iterabledata
rewinds the handle on construction, which fails on a non-seekable stream (a pipe,
some compressed or SFTP handles); such a stream is spooled into memory first,
while a seekable handle streams straight through.

### Type normalization

Binary formats carry types JSON does not implement, for example raw `bytes`,
timestamps, tagged values. The decoders hand some of those back as native
Python objects that a text or Parquet loader can not serialize. omniload
normalizes rows to portable values before handing data to the loader.

- `bytes` becomes a base64-encoded string, which is portable across text
  loaders and Parquet alike. This covers CBOR / MessagePack binary value
  types and YAML `!!binary` values.

- An unknown CBOR tag becomes a plain `{"tag": ..., "value": ...}` object
  rather than crashing the load.

- A MessagePack `Timestamp` extension becomes a UTC datetime.

- XML doesn't need any normalization: Its values are all strings or
  nested objects/lists, which every loader handles equally well.

- A YAML `!!set` type becomes a list.

Some values are made portable by the decoder itself rather than by omniload:
`cbor2` decodes the standard CBOR tags (datetime, big integers, decimals)
into native Python types directly.

Those load into Parquet and SQL destinations, but a native decimal cannot
be serialized to a JSONL *file* destination, so use a Parquet or SQL
destination for data that carries decimals. Nested maps and arrays are
handled recursively. The exact per-format mapping is on each format's own
documentation page under "Extended-type handling".

### Integrity and truncation

The read mechanism determines how a damaged file behaves, and it is worth knowing which
guarantee you get.

- **Whole-file decode (CBOR, XML, YAML)** raises on a corrupt or malformed file rather than
  loading partial data. CBOR additionally must be a *single* top-level value; files that
  concatenate several top-level objects are read only up to the first, a decoder limitation that
  cannot be detected at read time. XML additionally rejects an entity-expansion bomb and a
  mismatched encoding declaration.

- **Streaming formats (MessagePack)** carry no length prefix, so a truncated tail reads as a
  clean end-of-file: the partial trailing record, and anything after a mid-stream corruption,
  are dropped silently. Validate file integrity upstream if partial loads would be a problem.


[CSV]: https://en.wikipedia.org/wiki/Comma-separated_values
[iterabledata]: https://pypi.org/project/iterabledata/
[JSONL]: https://en.wikipedia.org/wiki/JSON_streaming#JSONL
[Parquet]: https://en.wikipedia.org/wiki/Apache_Parquet
[polars.read_csv]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html
[polars.read_excel]: https://docs.pola.rs/api/python/stable/reference/api/polars.read_excel.html
